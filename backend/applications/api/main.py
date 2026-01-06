from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import os
import joblib
import json
import asyncio
import polars as pl

from .config import logger, MODELS_CONFIG, META_MODEL_PATH, META_FEATURES_PATH, DB_CONNECTION_STRING, CURRENT_SEASON
from .state import model_data
from .services.data_loader import refresh_db_data, refresh_app_state
from .services.etl import etl_trigger_wrapper, run_daily_etl_async
from .routes import players, games, general, debug

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    logger.info("Server startup sequence initiated")
    try:
        # 1. Load ML Models
        model_data["models"] = {}
        for pos, paths in MODELS_CONFIG.items():
            if os.path.exists(paths['model']):
                model_data["models"][pos] = {
                    "model": joblib.load(paths['model']), 
                    "features": json.load(open(paths['features']))
                }
        
        if os.path.exists(META_MODEL_PATH):
            model_data["meta_models"] = joblib.load(META_MODEL_PATH)
            model_data["meta_features"] = json.load(open(META_FEATURES_PATH))
            
        # 2. Initial Data Load (load data first, then determine current week)
        refresh_db_data()
        refresh_app_state()
        
        # 3. Setup Scheduler
        scheduler = BackgroundScheduler()
        # Set for 6 AM
        scheduler.add_job(etl_trigger_wrapper, 'cron', hour=6, minute=0) 
        scheduler.add_job(refresh_app_state, 'interval', hours=1) 
        scheduler.start()
        logger.info("Scheduler active: ETL set for 06:00 daily.")
        
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
                        await asyncio.wait_for(run_daily_etl_async(), timeout=300)
                        logger.info("Startup ETL completed.")
                    except asyncio.TimeoutError:
                        logger.warning("Startup ETL timed out; falling back to background ETL.")
                        asyncio.create_task(run_daily_etl_async())
                    except Exception as e:
                        logger.exception(f"Startup ETL failed (sync path): {e}")
                else:
                    # Non-blocking trigger when DB already has data
                    asyncio.create_task(run_daily_etl_async())
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

app.include_router(players.router)
app.include_router(games.router)
app.include_router(general.router)
app.include_router(debug.router)
