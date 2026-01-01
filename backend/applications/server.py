from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import polars as pl
import joblib
import json
import sys
import os
import asyncio
import requests
import numpy as np
import subprocess
import traceback
from contextlib import asynccontextmanager
import nflreadpy as nfl
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from difflib import get_close_matches
import math
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("football-ai")

# --- 1. CONFIGURATION & SETUP ---
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:    
    logger.warning("DB_CONNECTION_STRING not found. Server will rely on local CSVs.")

# Paths
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
RAG_DIR = os.path.join(PROJECT_ROOT, 'rag_data')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'model_training', 'models')
ETL_SCRIPT_PATH = os.path.abspath(os.path.join(RAG_DIR, '05_etl_to_postgres.py'))

# --- 2. DYNAMIC SEASON LOGIC ---
def get_current_season():
    """Determines the active NFL season based on the current date."""
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

CURRENT_SEASON = get_current_season()
logger.info(f"Server initialized for NFL Season: {CURRENT_SEASON}")

# --- 3. CONSTANTS & MAPPINGS ---
MAE_VALUES = {'QB': 4.30, 'RB': 5.19, 'WR': 4.33, 'TE': 4.34}
META_MAE_VALUES = {'QB': 4.79, 'RB': 3.66, 'WR': 2.88, 'TE': 2.41}
WATCHLIST_FILE = os.path.join(RAG_DIR, 'watchlist.json')

TEAM_ABBR_MAP = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
}

MODELS_CONFIG = {
    'QB': {'model': os.path.join(MODEL_DIR, 'xgboost_QB_sliding_window_deviation_v1.joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_QB_sliding_window_deviation_v1.json')},
    'RB': {'model': os.path.join(MODEL_DIR, 'xgboost_RB_sliding_window_deviation_v1.joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_RB_sliding_window_deviation_v1.json')},
    'WR': {'model': os.path.join(MODEL_DIR, 'xgboost_WR_sliding_window_deviation_v1.joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_WR_sliding_window_deviation_v1.json')},
    'TE': {'model': os.path.join(MODEL_DIR, 'xgboost_TE_sliding_window_deviation_v1.joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_TE_sliding_window_deviation_v1.json')}
}
META_MODEL_PATH = os.path.join(MODEL_DIR, 'xgboost_META_model_v1.joblib')
META_FEATURES_PATH = os.path.join(MODEL_DIR, 'feature_names_META_model_v1.json')

model_data = {}

# --- 4. DATA LOADING HELPER FUNCTIONS ---
def enforce_types(df: pl.DataFrame) -> pl.DataFrame:
    """Ensures consistent data types for critical columns."""
    if df.is_empty(): return df
    cols = df.columns
    exprs = []
    
    # Force IDs to String
    if "player_id" in cols: exprs.append(pl.col("player_id").cast(pl.Utf8).str.strip_chars())
    
    # Force Week/Season to Int
    if "week" in cols: exprs.append(pl.col("week").fill_null(-1).cast(pl.Int64, strict=False))
    if "season" in cols: exprs.append(pl.col("season").fill_null(-1).cast(pl.Int64, strict=False))
    
    if exprs:
        try: return df.with_columns(exprs)
        except Exception: return df
    return df

def normalize_name(name):
    if not name: return ""
    return str(name).lower().replace(".", "").replace(" ", "").strip()

def get_team_abbr(raw_team):
    clean = str(raw_team).strip()
    return TEAM_ABBR_MAP.get(clean, clean)

def load_data_source(query: str, csv_filename: str, retries: int = 3, retry_delay: float = 1.0):
    """Try DB first with retries. By default the server runs in DB-only mode (no CSV fallback) unless ALLOW_CSV_FALLBACK is set to 'true'."""
    ALLOW_CSV_FALLBACK = os.getenv("ALLOW_CSV_FALLBACK", "false").lower() == "true"

    # Try DB with retries
    if DB_CONNECTION_STRING:
        attempt = 0
        while attempt < retries:
            try:
                df = pl.read_database_uri(query, DB_CONNECTION_STRING)
                logger.info(f"DB Load successful: {csv_filename} (attempt {attempt+1})")
                return enforce_types(df)
            except Exception as e:
                attempt += 1
                if attempt >= retries:
                    # If table missing show a specific hint
                    if "relation" in str(e).lower():
                        logger.warning(f"DB relation/table missing for {csv_filename}: {e}")
                    else:
                        logger.error(f"DB Load failed for {csv_filename} after {attempt} attempts: {e}")
                else:
                    time.sleep(retry_delay)

    # If CSV fallback is explicitly allowed, try it (development only)
    if ALLOW_CSV_FALLBACK:
        csv_path = os.path.join(RAG_DIR, csv_filename)
        if os.path.exists(csv_path):
            try:
                df = pl.read_csv(csv_path, ignore_errors=True)
                logger.info(f"CSV load successful (fallback): {csv_filename}")
                return enforce_types(df)
            except Exception as e:
                logger.error(f"CSV load failed: {csv_filename} - {e}")
        else:
            logger.warning(f"CSV fallback requested but file not found: {csv_filename}")
    logger.warning(f"Returning empty DataFrame for {csv_filename} (DB-only mode)")
    return pl.DataFrame()

def refresh_db_data():
    logger.info("Loading dataframes from DB/CVS sources...")
    sources = {
        "df_profile": ("SELECT * FROM player_profiles", f"player_profiles_{CURRENT_SEASON}.csv"),
        "df_schedule": ("SELECT * FROM schedule", f"schedule_{CURRENT_SEASON}.csv"),
        "df_player_stats": (f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv"),
        "df_snap_counts": (f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv"),
        "df_lines": ("SELECT * FROM bovada_game_lines", f"weekly_bovada_game_lines_{CURRENT_SEASON}.csv"),
        "df_props": ("SELECT * FROM bovada_player_props", f"weekly_bovada_player_props_{CURRENT_SEASON}.csv"),
        "df_injuries": (f"SELECT * FROM weekly_injuries_{CURRENT_SEASON}", f"weekly_injuries_{CURRENT_SEASON}.csv"),
        "df_features": (f"SELECT * FROM weekly_feature_set_{CURRENT_SEASON}", f"weekly_feature_set_{CURRENT_SEASON}.csv"),
    }
    
    for key, (query, csv) in sources.items():
        model_data[key] = load_data_source(query, csv)

    # If critical tables are empty, attempt an aggressive retry for player stats and snaps
    if ("df_player_stats" in model_data and model_data["df_player_stats"].is_empty()) or ("df_snap_counts" in model_data and model_data["df_snap_counts"].is_empty()):
        logger.warning("Critical tables empty after initial load — retrying DB loads for essential tables...")
        try:
            model_data["df_player_stats"] = load_data_source(f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for player_stats: {e}")
        try:
            model_data["df_snap_counts"] = load_data_source(f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for snap_counts: {e}")

    # --- BUILD INJURY MAP (ROBUST FIX) ---
    model_data["injury_map"] = {}
    model_data["gsis_to_sleeper"] = {}
    
    if "df_injuries" in model_data and not model_data["df_injuries"].is_empty():
        try:
            df = model_data["df_injuries"]
            
            # 1. Check if 'week' column exists (New Format)
            if "week" in df.columns:
                # Find the LATEST week available in the file
                max_wk = df.select(pl.col("week").max()).item()
                logger.info(f"Filtering injury map to latest week: {max_wk}")
                latest_report = df.filter(pl.col("week") == max_wk)
                
                rows = latest_report.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
            else:
                # Fallback for old CSVs without week column
                logger.warning("Injury CSV lacks 'week' column. Loading all rows (last write wins).")
                rows = df.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
                
        except Exception as e: 
            logger.exception(f"Injury map build error: {e}")

    try:
        players_df = nfl.load_ff_playerids()
        if "sleeper_id" in players_df.columns:
            map_df = players_df.drop_nulls(subset=['sleeper_id', 'gsis_id'])
            model_data["gsis_to_sleeper"] = dict(zip(map_df['gsis_id'].to_list(), map_df['sleeper_id'].cast(pl.Utf8).to_list()))
            model_data["sleeper_map"] = dict(zip(map_df['sleeper_id'].cast(pl.Utf8).to_list(), map_df['gsis_id'].to_list()))
    except Exception: pass

    logger.info("Data loaded into memory.")

def refresh_app_state():
    logger.info("Refreshing app state (scheduler) ...")
    try:
        base_week = nfl.get_current_week()
        if datetime.now().weekday() == 1: 
            model_data["current_nfl_week"] = base_week + 1
        else:
            model_data["current_nfl_week"] = base_week
        logger.info(f"Active NFL Week: {model_data['current_nfl_week']}")
    except Exception:
        model_data["current_nfl_week"] = 1

# --- Updated Non-Blocking ETL Function ---
async def run_daily_etl_async():
    """Executes the ETL script without blocking the main FastAPI event loop."""
    logger.info(f"Starting Daily ETL pipeline at {datetime.now()}...")
    if not os.path.exists(ETL_SCRIPT_PATH):
        logger.error(f"ETL script not found at: {ETL_SCRIPT_PATH}")
        return

    try:
        # Launch the process asynchronously
        process = await asyncio.create_subprocess_exec(
            sys.executable, ETL_SCRIPT_PATH,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("ETL process finished successfully")
            refresh_app_state()
            refresh_db_data()
        else:
            logger.error(f"ETL process exited with code {process.returncode}")
            if stderr: logger.error(f"STDERR: {stderr.decode()}")
    except Exception as e:
        logger.exception(f"Error during async ETL: {e}")

def etl_trigger_wrapper():
    """Bridge for APScheduler thread to async ETL."""
    try:
        asyncio.run(run_daily_etl_async())
    except Exception as e:
        logger.exception(f"Scheduler bridge error: {e}")

# --- LIFESPAN MANAGER ---

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
            
        # 2. Initial Data Load
        refresh_app_state()
        refresh_db_data()
        
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
# This MUST come after the lifespan function but before any @app.get decorators
app = FastAPI(lifespan=lifespan)

# Add Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
# --- 6. CORE LOGIC ---






def get_headshot_url(player_id: str):
    """Robust Headshot Locator"""
    try:
        if "df_profile" in model_data:
            row = model_data["df_profile"].filter(pl.col("player_id") == player_id)
            if not row.is_empty():
                url = row.row(0, named=True).get("headshot")
                if url and "http" in str(url): return url
    except: pass

    sleeper_id = model_data.get("gsis_to_sleeper", {}).get(player_id)
    if sleeper_id: return f"https://sleepercdn.com/content/nfl/players/{sleeper_id}.jpg"
    
    return "https://sleepercdn.com/images/v2/icons/player_default.webp"

def format_draft_info(year, number):
    if year and number and not np.isnan(number):
        return f"Pick {int(number)} ({int(year)})"
    return "Undrafted"

def calculate_fantasy_points(row):
    try:
        if row.get('y_fantasy_points_ppr') is not None: return float(row['y_fantasy_points_ppr'])
        p_yds = row.get('passing_yards') or 0
        p_tds = row.get('passing_touchdown') or 0
        r_yds = row.get('rushing_yards') or 0
        r_tds = row.get('rush_touchdown') or 0
        rec_yds = row.get('receiving_yards') or 0
        rec_tds = row.get('receiving_touchdown') or 0
        receptions = row.get('receptions') or 0
        ints = row.get('interceptions') or 0
        fumbles = row.get('fumbles_lost') or 0
        return ((p_yds * 0.04) + (p_tds * 4.0) + (r_yds * 0.1) + (r_tds * 6.0) + (rec_yds * 0.1) + (rec_tds * 6.0) + (receptions * 1.0) - (ints * 2.0) - (fumbles * 2.0))
    except: return 0.0

def get_injury_status_for_week(player_id: str, week: int, default="Active"):
    """
    Intelligent Injury Lookup.
    """
    if "df_injuries" not in model_data or model_data["df_injuries"].is_empty():
        logger.debug("Injury dataframe is missing or empty")
        return default

    df = model_data["df_injuries"]

    # Safety Check
    if "week" not in df.columns:
        logger.debug("Injury dataframe missing 'week' column; using fallback logic")
        # Fallback logic
        record = df.filter(pl.col("player_id") == str(player_id))
        if not record.is_empty():
            return record.row(0, named=True).get("injury_status", default)
        return default
    
    # Check Max Week in Memory
    max_wk = df.select(pl.col("week").max()).item()
    target_week = int(week)
    
    # If Future Week, use Max
    week_exists = not df.filter(pl.col("week") == int(week)).is_empty()
    if not week_exists:
        logger.debug(f"Week {week} not in injury data; falling back to latest known week {max_wk}")
        target_week = max_wk
    
    # Filter
    record = df.filter(
        (pl.col("week") == target_week) & 
        (pl.col("player_id") == str(player_id))
    )
    
    if not record.is_empty():
        status = record.row(0, named=True).get("injury_status", default)
        return status
    
    return default
def run_base_prediction(pid, pos, week):
    """
    Looks up features from DB. 
    1. RECENT FORM: Calculates average of the LAST 4 NON-ZERO GAMES.
    2. BASELINE CORRECTION: Uses that 4-game average as the starting point.
    3. LOGARITHMIC BOOST: 5.0 * ln(1 + deviation).
    4. USAGE VACUUM: Triggers on strict "Out/IR/Doubtful" status (Time-Aware).
    """
    # Initialize defaults
    features_dict = {}
    team = None
    # DEBUG: Report available dataframes and their columns (lightweight)
    try:
        logger.debug(f"Has dataframes: df_features={('df_features' in model_data)}, df_player_stats={('df_player_stats' in model_data)}, df_profile={('df_profile' in model_data)}")
        if 'df_features' in model_data:
            logger.debug(f"df_features columns: {model_data['df_features'].columns}")
        if 'df_player_stats' in model_data:
            logger.debug(f"df_player_stats columns: {model_data['df_player_stats'].columns}")
        if 'df_profile' in model_data:
            logger.debug(f"df_profile columns sample: {model_data['df_profile'].columns[:6]}")
    except Exception as e:
        logger.exception(f"Debug run info failure: {e}")
    
    # 1. Try to get features
    if "df_features" in model_data and not model_data["df_features"].is_empty() and 'player_id' in model_data['df_features'].columns and 'week' in model_data['df_features'].columns:
        player_features = model_data["df_features"].filter(
            (pl.col("player_id") == str(pid)) & (pl.col("week") == int(week))
        )
        if not player_features.is_empty():
            features_dict = player_features.row(0, named=True)
            team = features_dict.get('team') or features_dict.get('team_abbr')

    # 2. Fallback: Get Team/Pos from Profile
    if not team:
        prof = model_data["df_profile"].filter(pl.col("player_id") == str(pid))
        if not prof.is_empty():
            p_row = prof.row(0, named=True)
            team = p_row.get('team_abbr') or p_row.get('team')
        else:
            return None, "Player Not Found", None, 0.0

    if pos not in model_data["models"]: return None, "No Model", None, 0.0
    m_info = model_data["models"][pos]
    
    try:
        # --- 1. RECENT FORM (Last 4 Non-Zero Games) ---
        # Try to use in-memory player stats; if missing, fall back to direct DB query
        history_df = pl.DataFrame()
        if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty() and 'player_id' in model_data['df_player_stats'].columns:
            history_df = model_data['df_player_stats'].filter(
                (pl.col('player_id') == str(pid)) & 
                (pl.col('week') < int(week))
            )
        else:
            # Targeted DB lookup (player-level) as a robust fallback
            try:
                history_df = load_player_history_from_db(pid, week)
                if history_df is None: history_df = pl.DataFrame()
            except Exception as e:
                logger.warning(f"DB fallback failed for player history: {e}")
                history_df = pl.DataFrame()
        
        avg_recent_form = 0.0
        # If history_df is empty, as a last resort try a targeted DB load again
        if history_df.is_empty():
            try:
                history_db = load_player_history_from_db(pid, week)
                if history_db is not None and not history_db.is_empty():
                    history_df = history_db
            except Exception:
                pass

        if not history_df.is_empty():
            sorted_history = history_df.sort("week", descending=True)
            valid_pts = []
            for row in sorted_history.iter_rows(named=True):
                pts = calculate_fantasy_points(row)
                if pts > 0.0:
                    valid_pts.append(pts)
                if len(valid_pts) >= 4:
                    break
            
            if len(valid_pts) > 0:
                avg_recent_form = sum(valid_pts) / len(valid_pts)
            else:
                avg_recent_form = float(features_dict.get('player_season_avg_points', 0.0))
        else:
            avg_recent_form = float(features_dict.get('player_season_avg_points', 0.0))

        # --- 2. MODEL PREDICTION ---
        pred_dev = 0.0
        if features_dict:
            feats_input = {}
            for k in m_info["features"]:
                if k == 'player_season_avg_points':
                    feats_input[k] = [float(avg_recent_form)]
                else:
                    feats_input[k] = [float(features_dict.get(k) or 0.0)]
            
            try:
                pred_dev = m_info["model"].predict(pl.DataFrame(feats_input).to_numpy())[0]
            except: pred_dev = 0.0
        
        # --- 3. LOGARITHMIC BOOST (symmetric, sign-preserving) ---
        # Use log1p on absolute deviation to produce sharp increases for
        # small deviations and a tapering curve for large deviations.
        if pred_dev != 0:
            amplified_dev = math.copysign(5.0 * math.log1p(abs(pred_dev)), pred_dev)
        else:
            amplified_dev = 0.0
        
        # --- 4. BASELINE CORRECTION ---
        baseline = avg_recent_form

        # --- 5. USAGE VACUUM LOGIC (Robust Time-Aware) ---
        injury_boost = 0.0
        injury_statuses = ["IR", "Out", "Doubtful", "Inactive", "PUP"] 
        
        teammates = model_data["df_profile"].filter(
            (pl.col("team_abbr") == team) & (pl.col("position") == pos) & (pl.col("player_id") != pid)
        )
        
        for mate in teammates.iter_rows(named=True):
            mate_id = mate['player_id']

            # Check teammate injury status first; only injured teammates trigger usage boost logic (case-insensitive)
            status = str(get_injury_status_for_week(mate_id, week))
            status_norm = status.lower()
            if not any(s.lower() in status_norm for s in injury_statuses):
                continue  # skip teammates who are not injured

            # Fetch teammate historical stats to ensure they were a meaningful contributor
            mate_stats = pl.DataFrame()
            if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty() and 'player_id' in model_data['df_player_stats'].columns and 'week' in model_data['df_player_stats'].columns:
                mate_stats = model_data["df_player_stats"].filter(
                    (pl.col("player_id") == mate_id) & (pl.col("week") < int(week))
                )
            else:
                try:
                    q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{mate_id}' AND week < {int(week)} ORDER BY week DESC"
                    mate_stats = pl.read_database_uri(q, DB_CONNECTION_STRING)
                except Exception as e:
                    mate_stats = pl.DataFrame()

            if not mate_stats.is_empty():
                mate_pts = [calculate_fantasy_points(row) for row in mate_stats.to_dicts()]
                m_avg = sum(mate_pts) / len(mate_pts) if len(mate_pts) > 0 else 0

                m_snaps = 0.0
                if "df_snap_counts" in model_data:
                    mate_snaps_df = model_data["df_snap_counts"].filter(
                        (pl.col("player_id") == mate_id) & (pl.col("week") < int(week))
                    )
                    if not mate_snaps_df.is_empty():
                        try:
                            m_snaps = float(mate_snaps_df.select(pl.col("offense_pct")).mean().item())
                            # Normalize fraction -> percent
                            if m_snaps < 1.0: m_snaps *= 100
                            # Reject obviously invalid values
                            if math.isnan(m_snaps) or m_snaps < 0 or m_snaps > 200:
                                logger.debug(f"Unexpected mate_snaps value for {mate_id}: {m_snaps}; resetting to 0")
                                m_snaps = 0.0
                        except Exception as e:
                            logger.debug(f"Failed to compute mate_snaps for {mate_id}: {e}")
                            m_snaps = 0.0

                # Only apply boost if teammate was a consistent contributor (snaps and average points)
                # Loosen thresholds for RB/QB to catch more realistic vacancy cases (e.g., Kamara OUT should boost Saints RBs)
                if pos in ["RB", "QB"]:
                    if m_snaps >= 20 and m_avg >= 6:
                        injury_boost = 2.5
                        logger.info(f"Usage boost applied to {pid} due to teammate {mate_id} status {status} (m_avg={m_avg:.1f}, m_snaps={m_snaps:.1f})")
                        break
                else:
                    if m_snaps >= 20 and m_avg >= 8:
                        injury_boost = 1.5
                        logger.info(f"Usage boost applied to {pid} due to teammate {mate_id} status {status} (m_avg={m_avg:.1f}, m_snaps={m_snaps:.1f})")
                        break

        # --- 6. FINAL SCORE ---
        final_score = max(0.0, baseline + amplified_dev + injury_boost)
        is_boosted = injury_boost > 0
        
        return round(float(final_score), 2), is_boosted, features_dict, avg_recent_form
        
    except Exception as e:
        logger.exception(f"Prediction error for {pid}: {e}")
        return 0.0, False, features_dict, 0.0
def get_average_points_fallback(player_id, week):
    """Fallback calculation of average points if DB features are missing or malformed."""
    try:
        # Try in-memory first
        if 'df_player_stats' in model_data and not model_data['df_player_stats'].is_empty() and 'player_id' in model_data['df_player_stats'].columns:
            stats_history = model_data['df_player_stats'].filter((pl.col('player_id') == player_id) & (pl.col('week') < week))
        else:
            # Targeted DB fetch
            stats_history = load_player_history_from_db(player_id, week)
            if stats_history is None: return 0.0

        if not stats_history.is_empty():
            total_points, game_count = 0.0, 0
            for row in stats_history.iter_rows(named=True):
                pts = calculate_fantasy_points(row)
                if pts > 0 or row.get('offense_snaps', 0) > 0:
                    total_points += pts
                    game_count += 1
            if game_count > 0: return total_points / game_count
    except Exception as e:
        logger.warning(f"Average points fallback error: {e}")
    return 0.0


def load_player_history_from_db(player_id: str, week: int, limit: int = 12):
    """Load a player's recent history directly from DB (limited rows)."""
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT {int(limit)}"
        df = pl.read_database_uri(q, DB_CONNECTION_STRING)
        return enforce_types(df)
    except Exception as e:
        logger.warning(f"load_player_history_from_db error: {e}")
        return pl.DataFrame()

async def get_player_card(player_id: str, week: int):
    profile = model_data["df_profile"].filter(pl.col('player_id') == player_id)
    if profile.is_empty(): return None
    p_row = profile.row(0, named=True)
    
    pos = p_row['position']
    p_name = p_row['player_name']
    team = p_row.get('team_abbr') or p_row.get('team') or 'FA'

    # --- RUN PREDICTION ---
    l0_score, is_boosted, feats, rolling_avg_val = run_base_prediction(player_id, pos, week)
    
    # --- GET SEASON AVERAGE ---
    season_avg = 0.0
    if feats and isinstance(feats, dict) and feats.get("player_season_avg_points", 0) > 0:
        season_avg = feats["player_season_avg_points"]
    else:
        season_avg = get_average_points_fallback(player_id, week)

    if l0_score is None or l0_score == 0.0:
        # If prediction failed, try to compute rolling average directly from DB (robust fallback)
        if (not rolling_avg_val or rolling_avg_val == 0) and DB_CONNECTION_STRING:
            try:
                q = f"SELECT y_fantasy_points_ppr, passing_yards, rushing_yards, receiving_yards, receptions, passing_touchdown, rush_touchdown, receiving_touchdown, interceptions, fumbles_lost, week FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT 12"
                hist_df = pl.read_database_uri(q, DB_CONNECTION_STRING)
                if not hist_df.is_empty():
                    pts = []
                    for row in hist_df.iter_rows(named=True):
                        p = calculate_fantasy_points(row)
                        if p > 0: pts.append(p)
                        if len(pts) >= 4: break
                    if len(pts) > 0:
                        rolling_avg_val = sum(pts) / len(pts)
            except Exception as e:
                logger.warning(f"DB rolling average fallback failed for {player_id}: {e}")
        l0_score = season_avg if season_avg > 0 else rolling_avg_val

    meta_score = l0_score 

    # --- INJURY STATUS (Use Same Logic as Prediction) ---
    final_status = get_injury_status_for_week(player_id, week)

    # --- SNAP COUNT FALLBACK ---
    snap_pct, snap_count = 0.0, 0
    if feats and isinstance(feats, dict):
        snap_pct = float(feats.get('offense_pct', 0.0))
        snap_count = int(feats.get('offense_snaps', 0))

    if snap_count == 0:
        # Prefer in-memory snap history, otherwise query DB directly
        try:
            history_snaps = pl.DataFrame()
            if "df_snap_counts" in model_data and not model_data["df_snap_counts"].is_empty() and 'player_id' in model_data['df_snap_counts'].columns:
                history_snaps = model_data["df_snap_counts"].filter(
                    (pl.col("player_id") == player_id) & 
                    (pl.col("week") < int(week))
                ).sort("week", descending=True).head(1)
            else:
                # DB lookup for last snap counts
                q = f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT 1"
                history_snaps = pl.read_database_uri(q, DB_CONNECTION_STRING)

            if not history_snaps.is_empty():
                last_game = history_snaps.row(0, named=True)
                snap_count = int(last_game.get('offense_snaps', 0))
                snap_pct = float(last_game.get('offense_pct', 0.0))
        except Exception as e:
            logger.warning(f"Snap fallback failed for {player_id}: {e}")
            snap_count, snap_pct = snap_count, snap_pct


    if snap_pct < 1.0 and snap_pct > 0: snap_pct *= 100

    total_line = None 
    spread_val = None
    implied_total = None
    props_data = [] 
    prop_line = None
    prop_prob = None
    pass_td_line = None
    pass_td_prob = None
    anytime_td_prob = None

    try:
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines = model_data["df_lines"].filter(
                (pl.col("week") == int(week)) & 
                ((pl.col("home_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team) | 
                 (pl.col("away_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team))
            )
            if not lines.is_empty():
                row = lines.row(0, named=True)
                total_line = row.get("total_over")
                raw_spread = row.get("home_spread")
                
                # Calculate spread relative to player's team
                h_team = get_team_abbr(row.get("home_team"))
                p_team = get_team_abbr(team) # Ensure player team is also normalized
                
                if raw_spread is not None:
                    try:
                        s = float(raw_spread)
                        spread_val = s if h_team == p_team else -s
                        
                        # Calculate implied team total
                        if total_line:
                            t = float(total_line)
                            implied_total = (t / 2) - (spread_val / 2)
                    except: pass
    except Exception: pass

    try:
        if "df_props" in model_data and not model_data["df_props"].is_empty():
            week_props = model_data["df_props"].filter(pl.col("week") == int(week))
            p_norm = normalize_name(p_name)
            week_props = week_props.with_columns(
                pl.col("player_name").map_elements(normalize_name, return_dtype=pl.Utf8).alias("norm_name")
            )
            p_props = week_props.filter(pl.col("norm_name") == p_norm)

            if p_props.is_empty():
                all_names = week_props["player_name"].unique().to_list()
                matches = get_close_matches(p_name, all_names, n=1, cutoff=0.6)
                if matches:
                    p_props = week_props.filter(pl.col("player_name") == matches[0])

            if not p_props.is_empty():
                props_data = p_props.select(["prop_type", "line", "odds", "implied_prob"]).to_dicts()
                
                target_prop = None
                if pos == 'QB': target_prop = "Passing Yards"
                elif pos == 'RB': target_prop = "Rushing Yards"
                elif pos in ['WR', 'TE']: target_prop = "Receiving Yards"
                
                if target_prop:
                    # Case-insensitive search
                    main_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(target_prop.lower()))
                    
                    # If RB and Rushing Yards not found, try Rushing & Receiving Yards
                    if main_p.is_empty() and pos == 'RB':
                         main_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("rushing & receiving yards"))

                    if not main_p.is_empty():
                        # Sort to prefer exact match if possible (though contains is broad)
                        # Just take the first one for now
                        row = main_p.row(0, named=True)
                        prop_line = row['line']
                        prop_prob = row['implied_prob'] 
                
                if pos == 'QB':
                    td_pass = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("passing touchdowns"))
                    if not td_pass.is_empty():
                        row = td_pass.row(0, named=True)
                        pass_td_line = row['line']
                        pass_td_prob = row['implied_prob']

                td_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("anytime td"))
                if not td_p.is_empty():
                    anytime_td_prob = td_p.row(0, named=True)['implied_prob']

    except Exception as e: logger.exception(f"Props extraction error: {e}")

    opponent = feats.get('opponent_team') if (feats and isinstance(feats, dict)) else None

    # If we still don't have an opponent, try schedule (DB-backed) lookup
    if not opponent or opponent == "BYE":
        try:
            # Look into in-memory schedule first
            sched_df = model_data.get('df_schedule') if 'df_schedule' in model_data else pl.DataFrame()
            if sched_df is None or sched_df.is_empty():
                # fallback to DB schedule read (ensure DB-only behavior)
                try:
                    sched_df = pl.read_database_uri(f"SELECT * FROM schedule", DB_CONNECTION_STRING)
                except Exception:
                    sched_df = pl.DataFrame()

            if not sched_df.is_empty():
                found = False
                for r in sched_df.iter_rows(named=True):
                    try:
                        if int(r.get('week', -1)) != int(week):
                            continue
                    except Exception:
                        continue
                    h_abbr = get_team_abbr(r.get('home_team') or '')
                    a_abbr = get_team_abbr(r.get('away_team') or '')
                    # If schedule stores abbr directly, this still works
                    if h_abbr == team or a_abbr == team:
                        opponent = a_abbr if h_abbr == team else h_abbr
                        found = True
                        break
                if not found:
                    opponent = opponent or "BYE"
        except Exception as e:
            logger.warning(f"Opponent lookup failed: {e}")
            opponent = opponent or "BYE"

    return {
        "player_name": p_name,
        "player_id": player_id,
        "position": pos,
        "week": week,
        "team": team,
        "opponent": opponent,
        "draft_position": format_draft_info(p_row.get('draft_year'), p_row.get('draft_number')),
        "snap_count": snap_count,
        "snap_percentage": snap_pct,
        "overunder": float(total_line) if total_line else None,
        "spread": spread_val,
        "implied_total": round(implied_total, 1) if implied_total else None,
        "props": props_data,
        "prop_line": prop_line, 
        "prop_prob": prop_prob,
        "pass_td_line": pass_td_line, 
        "pass_td_prob": pass_td_prob, 
        "anytime_td_prob": anytime_td_prob,
        "image": get_headshot_url(player_id),
        "prediction": round(meta_score, 2),  
        "floor_prediction": round(meta_score * 0.8, 2),
        "average_points": round(season_avg, 1), 
        "rolling_4wk_avg": round(rolling_avg_val, 1), 
        "is_injury_boosted": is_boosted, 
        "injury_status": final_status, 
        "debug_err": None 
    }
async def get_team_roster_cards(team_abbr: str, week: int):
    composition = {"QB": 4, "RB": 8, "WR": 8, "TE": 5}
    roster_result = []
    
    ranked = pl.DataFrame()
    try:
        q = f"SELECT player_id, position FROM weekly_rankings WHERE week={week} AND team_abbr='{team_abbr}' ORDER BY predicted_points DESC"
        ranked = pl.read_database_uri(q, DB_CONNECTION_STRING)
    except: pass

    if ranked.is_empty():
        team_col = "team_abbr" if "team_abbr" in model_data["df_profile"].columns else "team"
        candidates = model_data["df_profile"].filter(
            (pl.col(team_col) == team_abbr) & 
            (pl.col("status") == "ACT")
        ).select(["player_id", "position"])
        ranked = candidates

    for pos, limit in composition.items():
        pos_candidates = ranked.filter(pl.col("position") == pos).head(limit)
        for row in pos_candidates.iter_rows(named=True):
            card = await get_player_card(row['player_id'], week)
            if card: roster_result.append(card)
            
    order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4}
    roster_result.sort(key=lambda x: order.get(x["position"], 99))
    return roster_result


# Developer helpers & debug endpoints (minimal)
def find_usage_boost_reason(player_id: str, week: int):
    """Returns details about whether a usage boost would be applied to this player and why.
    This mirrors the usage-boost check in `run_base_prediction` but returns diagnostic info.
    """
    try:
        profile = model_data.get("df_profile", pl.DataFrame()).filter(pl.col('player_id') == player_id)
        if profile.is_empty():
            return {"found": False, "error": "player profile not found"}
        p = profile.row(0, named=True)
        pos = p['position']
        team = p.get('team_abbr') or p.get('team') or 'FA'

        injury_statuses = [s.lower() for s in ["IR", "Out", "Doubtful", "Inactive", "PUP"]]

        teammates = model_data.get("df_profile", pl.DataFrame()).filter(
            (pl.col("team_abbr") == team) & (pl.col("position") == pos) & (pl.col("player_id") != player_id)
        )

        for mate in teammates.iter_rows(named=True):
            mate_id = mate['player_id']
            status = str(get_injury_status_for_week(mate_id, week)).lower()
            if not any(s in status for s in injury_statuses):
                continue

            # Gather mate stats
            try:
                mate_stats = pl.DataFrame()
                if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty():
                    mate_stats = model_data["df_player_stats"].filter((pl.col('player_id') == mate_id) & (pl.col('week') < int(week)))
                else:
                    q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{mate_id}' AND week < {int(week)} ORDER BY week DESC"
                    mate_stats = pl.read_database_uri(q, DB_CONNECTION_STRING)

                if mate_stats.is_empty():
                    continue

                mate_pts = [calculate_fantasy_points(r) for r in mate_stats.to_dicts()]
                m_avg = sum(mate_pts) / len(mate_pts) if len(mate_pts) > 0 else 0

                m_snaps = 0.0
                if "df_snap_counts" in model_data and not model_data["df_snap_counts"].is_empty():
                    sdf = model_data["df_snap_counts"].filter((pl.col('player_id') == mate_id) & (pl.col('week') < int(week)))
                    if not sdf.is_empty():
                        try:
                            m_snaps = float(sdf.select(pl.col('offense_pct')).mean().item())
                            if m_snaps < 1.0: m_snaps *= 100
                            if math.isnan(m_snaps) or m_snaps < 0 or m_snaps > 200:
                                m_snaps = 0.0
                        except:
                            m_snaps = 0.0

                if m_snaps >= 30 and m_avg >= 10:
                    return {"found": True, "boosted": True, "mate_id": mate_id, "mate_status": status, "mate_avg_points": m_avg, "mate_avg_snaps": m_snaps}

            except Exception as e:
                continue

        return {"found": True, "boosted": False}

    except Exception as e:
        return {"found": False, "error": str(e)}


@app.get('/debug/usage-boost/{player_id}/{week}')
async def debug_usage_boost(player_id: str, week: int):
    try:
        res = find_usage_boost_reason(player_id, week)
        return res
    except Exception as e:
        logger.exception(f"Debug usage-boost failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def fetch_sleeper_trends(trend_type: str, limit: int = 10, week: int = 1):
    if not model_data.get("sleeper_map"): refresh_app_state() 
    try:
        url = f"https://api.sleeper.app/v1/players/nfl/trending/{trend_type}?lookback_hours=24&limit={limit+10}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=3)
        if response.status_code != 200: return []
        data = response.json()
        cards = []
        for item in data:
            sleeper_id = str(item.get("player_id"))
            count = item.get("count", 0)
            our_id = model_data["sleeper_map"].get(sleeper_id)
            if our_id:
                card = await get_player_card(our_id, week)
                if card:
                    card["trending_count"] = count 
                    cards.append(card)
            if len(cards) >= limit: break
        return cards
    except: return []

# --- 7. ENDPOINTS ---
class PlayerRequest(BaseModel):
    player_name: str
    week: Optional[int] = None

class CompareRequest(BaseModel):
    player1_name: str
    player2_name: str
    week: Optional[int] = None

@app.get("/player/{player_id}")
async def get_player_by_id(player_id: str, week: Optional[int] = None):
    try:
        wk = week if week else model_data["current_nfl_week"]
        card = await get_player_card(player_id, wk)
        if not card: raise HTTPException(404, "Player not found")
        return card
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/current_week")
async def get_current_week(): return {"week": model_data.get("current_nfl_week", 1)}

@app.get("/health")
async def health_check():
    """Lightweight health check for orchestrators and load balancers.
    Returns: status, DB availability, models loaded, ETL script existence and current week."""
    status = {
        "status": "ok",
        "db_connection_string_set": bool(DB_CONNECTION_STRING),
        "models_loaded": len(model_data.get("models", {})),
        "meta_loaded": "meta_models" in model_data,
        "etl_script_exists": os.path.exists(ETL_SCRIPT_PATH),
        "current_week": model_data.get("current_nfl_week", None)
    }

    # Quick DB probe if connection string is configured
    if DB_CONNECTION_STRING:
        try:
            # Run a minimal probe query; some DB drivers may require a small table
            _ = pl.read_database_uri("SELECT 1", DB_CONNECTION_STRING)
            status["db_responding"] = True
        except Exception as e:
            status["db_responding"] = False
            status["db_error"] = str(e)
    else:
        status["db_responding"] = False

    return status

@app.get("/schedule/{week}")
async def get_schedule(week: int):
    try:
        if model_data["df_schedule"].is_empty(): return []
        
        max_week = model_data["df_schedule"]["week"].max()
        target_week = week if week <= max_week else max_week
        
        sched_df = model_data["df_schedule"].filter(pl.col("week") == int(target_week))
        games = sched_df.to_dicts()
        
        matched_count = 0
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines_df = model_data["df_lines"].filter(pl.col("week") == int(target_week))
            
            # Create a robust lookup map
            odds_map = {}
            for row in lines_df.iter_rows(named=True):
                h_abbr = get_team_abbr(row['home_team'])
                a_abbr = get_team_abbr(row['away_team'])
                odds_map[(h_abbr, a_abbr)] = row
                odds_map[(a_abbr, h_abbr)] = row
            
            for game in games:
                key = (game['home_team'], game['away_team'])
                match = odds_map.get(key)
                
                if match:
                    game['moneyline_home'] = match.get('home_ml')
                    game['moneyline_away'] = match.get('away_ml')
                    game['game_total'] = match.get('total_over')
                    matched_count += 1
                else:
                    game['moneyline_home'] = None
                    game['moneyline_away'] = None
                    game['game_total'] = None
        
        logger.info(f"Schedule (Wk {week}): Odds attached for {matched_count}/{len(games)} games.")
        return games

    except Exception as e:
        logger.exception(f"Schedule endpoint error: {e}")
        return []

@app.get("/matchup/{week}/{home_team}/{away_team}")
async def get_matchup_rosters(week: int, home_team: str, away_team: str):
    try:
        home_cards = await get_team_roster_cards(home_team, week)
        away_cards = await get_team_roster_cards(away_team, week)
        
        over_under, home_win, away_win, spread = None, None, None, None
        
        # Use the same approach as the schedule endpoint to fetch game odds
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines_df = model_data["df_lines"].filter(pl.col("week") == int(week))
            
            # Extract spread directly from DataFrame without iteration
            if not lines_df.is_empty():
                # Try filtering by both teams
                home_lines = lines_df.filter(
                    (pl.col("home_team") == home_team) & (pl.col("away_team") == away_team)
                )
                if not home_lines.is_empty():
                    row_dict = home_lines.row(0, named=True)
                    over_under = row_dict.get('total_over')
                    home_win = row_dict.get('home_ml_prob')
                    away_win = row_dict.get('away_ml_prob')
                    spread_val = row_dict.get('home_spread')
                    if spread_val is not None:
                        try:
                            spread = float(spread_val)
                        except (ValueError, TypeError):
                            spread = None

        return {
            "matchup": f"{away_team} @ {home_team}",
            "week": week,
            "over_under": over_under,
            "spread": spread,
            "home_win_prob": home_win,
            "away_win_prob": away_win,
            "home_roster": home_cards,
            "away_roster": away_cards
        }
    except Exception as e: 
        logger.exception(f"Matchup endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load matchup: {str(e)}")

@app.post("/predict")
async def predict(req: PlayerRequest):
    try:
        match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == req.player_name.lower())
        if match.is_empty(): raise HTTPException(404, "Player not found")
        pid = match.row(0, named=True)['player_id']
        wk = req.week if req.week else model_data["current_nfl_week"]
        return await get_player_card(pid, wk)
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/compare")
async def compare(req: CompareRequest):
    wk = req.week if req.week else model_data["current_nfl_week"]
    res = []
    for name in [req.player1_name, req.player2_name]:
        try:
            match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == name.lower())
            if not match.is_empty():
                pid = match.row(0, named=True)['player_id']
                res.append(await get_player_card(pid, wk))
            else:
                res.append({"error": f"Player {name} not found"})
        except:
            res.append({"error": "Lookup failed"})
    return {"week": wk, "comparison": res}



@app.get('/players/search')
async def search_players(q: str):
    if not q: return []
    try:
        expr = (pl.col('player_name').str.to_lowercase().str.contains(q.lower()) & (pl.col('position').is_in(['QB', 'RB', 'WR', 'TE'])))
        return model_data["df_profile"].filter(expr).select(['player_id', 'player_name', 'position', 'team_abbr', 'headshot', 'status']).head(20).to_dicts()
    except: return []

@app.get("/rankings/past/{week}")
async def get_trending_down(week: int):
    return await fetch_sleeper_trends("drop", limit=30, week=week)

@app.get("/rankings/future/{week}")
async def get_trending_up(week: int):
    return await fetch_sleeper_trends("add", limit=30, week=week)

@app.get("/player/history/{player_id}")
async def get_player_history(player_id: str):
    try:
        # Prefer in-memory data (loaded from CSV) for consistency; fallback to DB if empty
        df = model_data.get('df_player_stats', pl.DataFrame())
        if df.is_empty() and DB_CONNECTION_STRING:
            # Only try DB if in-memory is empty
            q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' ORDER BY week DESC"
            df = pl.read_database_uri(q, DB_CONNECTION_STRING)
        else:
            # Filter in-memory data
            df = df.filter(pl.col('player_id') == player_id).sort('week', descending=True)

        if df.is_empty():
            return []

        history = []
        player_snaps = model_data.get('df_snap_counts', pl.DataFrame())

        for row in df.iter_rows(named=True):
            wk = row.get('week')
            snap_count, snap_pct = 0, 0.0
            if not player_snaps.is_empty():
                s_row = player_snaps.filter(pl.col('week') == wk)
                if not s_row.is_empty():
                    s0 = s_row.row(0, named=True)
                    snap_count = int(s0.get('offense_snaps', 0))
                    snap_pct = float(s0.get('offense_pct', 0.0))

            history.append({
                "week": wk,
                "opponent": row.get('opponent_team') or "N/A",
                "points": round(float(calculate_fantasy_points(row)), 2),
                "passing_yds": int(row.get('passing_yards') or 0),
                "rushing_yds": int(row.get('rushing_yards') or 0),
                "receiving_yds": int(row.get('receiving_yards') or 0),
                "touchdowns": int((row.get('passing_touchdown') or 0) + (row.get('rush_touchdown') or 0) + (row.get('receiving_touchdown') or 0)),
                "snap_count": snap_count,
                "snap_percentage": snap_pct,
                "receptions": int(row.get('receptions') or 0),
                "targets": int(row.get('targets') or 0),
                "carries": int(row.get('rush_attempts') or 0)
            })
        return history
    except Exception as e:
        logger.exception(f"History endpoint failed for {player_id}: {e}")
        return []

# --- WATCHLIST ---
def load_wl(): return json.load(open(WATCHLIST_FILE)) if os.path.exists(WATCHLIST_FILE) else []
@app.get('/watchlist')
async def get_watchlist():
    ids = load_wl()
    if not ids: return []
    return model_data["df_profile"].filter(pl.col("player_id").is_in(ids)).select(['player_id', 'player_name', 'team_abbr', 'position']).to_dicts()
@app.post('/watchlist')
async def add_watchlist(item: dict):
    ids = load_wl()
    if item['player_id'] not in ids:
        ids.append(item['player_id'])
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids
@app.delete('/watchlist/{player_id}')
async def remove_watchlist(player_id: str):
    ids = load_wl()
    if player_id in ids:
        ids.remove(player_id)
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids

if __name__ == "__main__":
    import uvicorn
    # Ensure uvicorn imports the correct module path when running as a script
    uvicorn.run("applications.server:app", host="0.0.0.0", port=8000, reload=True)