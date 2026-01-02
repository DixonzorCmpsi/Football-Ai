import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# --- 1. CONFIGURATION & SETUP ---
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("football-ai")

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:    
    logger.warning("DB_CONNECTION_STRING not found. Server will rely on local CSVs.")

# Paths
# current_dir is backend/applications/api
current_dir = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT is backend/ (up 2 levels from api/)
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..', '..'))
RAG_DIR = os.path.join(PROJECT_ROOT, 'rag_data')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'model_training', 'models')
ETL_SCRIPT_PATH = os.path.abspath(os.path.join(RAG_DIR, '05_etl_to_postgres.py'))
WATCHLIST_FILE = os.path.join(RAG_DIR, 'watchlist.json')

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
