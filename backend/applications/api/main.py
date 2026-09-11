from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
import os
from datetime import datetime, timedelta
import joblib
import json
import asyncio
import polars as pl

from .config import logger, MODELS_CONFIG, META_MODEL_PATH, META_FEATURES_PATH, DB_CONNECTION_STRING, CURRENT_SEASON
from .state import model_data
from .rate_limit import limiter
from .services.data_loader import refresh_db_data, refresh_app_state, load_historical_stats, load_depth_charts
from .services.etl import etl_trigger_wrapper, run_daily_etl_async, injury_refresh_wrapper
from .services.storylines import storylines_wrapper
from .routes import players, games, general, debug, tier_list, sleeper
from .routes.tier_list import load_persisted_rookies_into_profile, run_rookie_refresh

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    logger.info("Server startup sequence initiated")
    try:
        # 0. Schema migration (idempotent). `ADD COLUMN IF NOT EXISTS` is a no-op
        # after the first run; the backfill only touches NULL rows. Runs before
        # data load so df_profile sees the new columns.
        if DB_CONNECTION_STRING:
            try:
                import sys as _sys, os as _os
                _rag = _os.path.join(_os.path.dirname(__file__), "..", "..", "rag_data")
                if _rag not in _sys.path:
                    _sys.path.insert(0, _os.path.abspath(_rag))
                from sqlalchemy import create_engine as _create_engine
                from migrate_schema import run_migrations as _run_migrations, get_current_season as _gcs
                _engine = _create_engine(DB_CONNECTION_STRING)
                _run_migrations(_engine, int(_gcs()), log=lambda m: logger.info(f"[migration] {m}"))
                _engine.dispose()
            except Exception as e:
                logger.warning(f"Schema migration skipped: {e}")

        # 1. Load ML Models (best-effort: don't let xgboost/libomp issues block data load).
        model_data["models"] = {}
        for pos, paths in MODELS_CONFIG.items():
            if os.path.exists(paths['model']):
                try:
                    model_data["models"][pos] = {
                        "model": joblib.load(paths['model']),
                        "features": json.load(open(paths['features']))
                    }
                except Exception as e:
                    logger.warning(f"Skipped {pos} model load: {e}")

        if os.path.exists(META_MODEL_PATH):
            try:
                model_data["meta_models"] = joblib.load(META_MODEL_PATH)
                model_data["meta_features"] = json.load(open(META_FEATURES_PATH))
            except Exception as e:
                logger.warning(f"Skipped META model load: {e}")
            
        # 2. Initial Data Load (load data first, then determine current week)
        refresh_db_data()
        refresh_app_state()

        # 2b. Merge any previously-saved ESPN rookies (offline-tolerant restart).
        try:
            merged = load_persisted_rookies_into_profile()
            if merged:
                logger.info(f"Merged {merged} persisted rookies into df_profile")
        except Exception as e:
            logger.warning(f"Persisted rookie merge failed: {e}")

        # 2b-2. Load prior-season weekly stats (cached on disk after first fetch) so
        # /player/history can show last season's performance for veterans even when
        # the current-season DB is empty (offseason).
        try:
            asyncio.create_task(asyncio.to_thread(load_historical_stats))
        except Exception as e:
            logger.warning(f"Historical stats load schedule failed: {e}")

        # 2b-3. Load current-season depth charts so `is_starter` reflects the real
        # depth chart (pos_rank == 1) rather than a snap-count heuristic.
        try:
            asyncio.create_task(asyncio.to_thread(load_depth_charts))
        except Exception as e:
            logger.warning(f"Depth charts load schedule failed: {e}")

        # 2c. Fire-and-forget background refresh from ESPN so we have fresh rookies
        # without delaying startup. Only runs if the CSV is missing/stale.
        try:
            from os.path import exists, getmtime
            from time import time as _now
            from .routes.tier_list import _rookies_csv_path as _rkc

            from .config import CURRENT_SEASON as _SEASON

            csv_path = _rkc(int(_SEASON))
            is_stale = (not exists(csv_path)) or (_now() - getmtime(csv_path) > 23 * 3600)
            if is_stale:
                logger.info("Rookie cache missing/stale — scheduling background refresh")
                asyncio.create_task(asyncio.to_thread(run_rookie_refresh))
        except Exception as e:
            logger.warning(f"Could not schedule startup rookie refresh: {e}")

        # 3. Setup Scheduler
        scheduler = BackgroundScheduler()
        # Set for 6 AM
        scheduler.add_job(etl_trigger_wrapper, 'cron', hour=6, minute=0)
        scheduler.add_job(refresh_app_state, 'interval', hours=1)
        # Daily rookie refresh: 06:15 (just after ETL). Cheap (~10s), writes CSV.
        scheduler.add_job(run_rookie_refresh, 'cron', hour=6, minute=15, id='rookie_refresh')
        # Daily historical stats refresh (idempotent — reads cache if recent).
        scheduler.add_job(load_historical_stats, 'cron', hour=6, minute=20, id='historical_stats')
        # Daily depth chart refresh so `is_starter` tracks roster moves.
        scheduler.add_job(lambda: load_depth_charts(force=True), 'cron', hour=6, minute=25, id='depth_charts')
        # Injuries are the most time-sensitive field in the app and previously
        # only refreshed with the 06:00 ETL, so a player ruled out overnight
        # still rendered as "Active" the next morning. Pull them on their own
        # short cycle. Tune with INJURY_REFRESH_MINUTES (0 disables).
        injury_minutes = int(os.getenv('INJURY_REFRESH_MINUTES', '15'))
        if injury_minutes > 0:
            scheduler.add_job(
                injury_refresh_wrapper, 'interval', minutes=injury_minutes,
                id='injury_refresh', max_instances=1, coalesce=True,
            )
        # Player storylines: ESPN's league feed is capped at 50 articles and only
        # covers the last several hours, so polling it a few times a day is what
        # accumulates real per-player history. Tune with STORYLINE_REFRESH_HOURS.
        storyline_hours = float(os.getenv('STORYLINE_REFRESH_HOURS', '3'))
        if storyline_hours > 0:
            scheduler.add_job(
                storylines_wrapper, 'interval', hours=storyline_hours,
                id='storylines', max_instances=1, coalesce=True,
                next_run_time=datetime.now() + timedelta(seconds=45),
            )
        scheduler.start()
        logger.info(
            "Scheduler active: ETL 06:00, rookie refresh 06:15, historical stats 06:20, "
            "depth charts 06:25, app-state hourly, injuries every %s min, storylines every %s h.",
            injury_minutes if injury_minutes > 0 else "off",
            storyline_hours if storyline_hours > 0 else "off",
        )
        
        # Store scheduler in app state so we can shut it down
        app.state.scheduler = scheduler

        # Trigger ETL immediately at startup. Behavior controlled by RUN_ETL_ON_STARTUP.
        try:
            run_on_startup = os.getenv('RUN_ETL_ON_STARTUP', 'true').lower() in ('1', 'true', 'yes')
            if run_on_startup:
                # If DB appears empty (no player stats), run ETL synchronously on first start to ensure DB is populated.
                need_sync = True
                try:
                    if DB_CONNECTION_STRING:
                        # quick probe for target table
                        probe_q = f"SELECT count(1) as cnt FROM weekly_player_stats_{CURRENT_SEASON}"
                        probe_df = pl.read_database_uri(probe_q, DB_CONNECTION_STRING)
                        need_sync = (probe_df.row(0)[0] == 0)
                except Exception:
                    need_sync = True

                if need_sync:
                    logger.info("Running ETL synchronously at startup to ensure DB is populated.")
                    try:
                        # Wait up to 5 minutes for ETL to complete; fall back to async if it times out
                        # Note: restart_after=False to avoid infinite restart loop at startup
                        await asyncio.wait_for(run_daily_etl_async(restart_after=False), timeout=300)
                        logger.info("Startup ETL completed.")
                    except asyncio.TimeoutError:
                        logger.warning("Startup ETL timed out; falling back to background ETL.")
                        asyncio.create_task(run_daily_etl_async(restart_after=False))
                    except Exception as e:
                        logger.exception(f"Startup ETL failed (sync path): {e}")
                else:
                    # Non-blocking trigger when DB already has data (no restart needed)
                    asyncio.create_task(run_daily_etl_async(restart_after=False))
                    logger.info("Startup ETL triggered asynchronously (DB already populated).")
            else:
                logger.info("RUN_ETL_ON_STARTUP disabled; skipping startup ETL.")
        except Exception as e:
            logger.exception(f"Failed to trigger startup ETL: {e}")

    except Exception as e:
        logger.exception(f"Startup error: {e}")

    yield # --- SERVER IS RUNNING ---

    # --- SHUTDOWN ---
    logger.info("Server shutdown sequence initiated")
    if hasattr(app.state, "scheduler"):
        app.state.scheduler.shutdown()
    model_data.clear()

# --- INITIALIZE APP ---
app = FastAPI(lifespan=lifespan)

# Add Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Per-IP rate limiting. CORS is wide open (allow_origins=["*"]) with no auth,
# so nothing else stood between the public internet and unlimited calls into
# ETL triggers / external-fetch routes. Default is generous for normal read
# traffic; expensive routes set a tighter limit where they're defined.
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.include_router(players.router)
app.include_router(games.router)
app.include_router(general.router)
app.include_router(debug.router)
app.include_router(tier_list.router)
app.include_router(sleeper.router)
