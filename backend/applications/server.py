# applications/server.py
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional
import polars as pl
import joblib
import json
import sys
import os
import numpy as np
import xgboost as xgb
from contextlib import asynccontextmanager
import nflreadpy as nfl
from rapidfuzz import process, fuzz
from sqlalchemy import create_engine, text
from dotenv import load_dotenv # Needed to load password

# --- Configuration ---
load_dotenv()


DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:    
    print("Error: DB_CONNECTION_STRING not found in environment variables.", file=sys.stderr)
    sys.exit(1)
# --- End DB Configuration ---

# --- Add dataPrep directory to Python path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
dataPrep_dir = os.path.abspath(os.path.join(current_dir, '..', 'dataPrep'))
if dataPrep_dir not in sys.path:
    sys.path.insert(0, dataPrep_dir)

# --- Import from the TimeSeries feature generator ---
try:
    from feature_generator_timeseries import generate_features_all
except ImportError as e:
    print(f"Error: Could not import generate_features_all: {e}", file=sys.stderr)
    sys.exit(1)

# --- Define Constants ---
CURRENT_SEASON = 2025
MAE_VALUES = {
    'QB': 4.30, 'RB': 5.19, 'WR': 4.33, 'TE': 4.34
}
META_MAE_VALUES = {
    'QB': 4.79, 'RB': 3.66, 'WR': 2.88, 'TE': 2.41
}
FUZZY_MATCH_THRESHOLD = 80

# --- File Paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
RAG_DIR = os.path.join(PROJECT_ROOT, 'rag_data')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'model_training', 'models')

# RAG Data paths (used for loading locally, but paths are ignored when reading from DB)
PROFILE_PATH = os.path.join(RAG_DIR, 'player_profiles.csv')
SCHEDULE_PATH = os.path.join(RAG_DIR, 'schedule_2025.csv')
PLAYER_STATS_PATH = os.path.join(RAG_DIR, 'weekly_player_stats_2025.csv')
DEFENSE_STATS_PATH = os.path.join(RAG_DIR, 'weekly_defense_stats_2025.csv')
OFFENSE_STATS_PATH = os.path.join(RAG_DIR, 'weekly_offense_stats_2025.csv')
SNAP_COUNTS_PATH = os.path.join(RAG_DIR, 'weekly_snap_counts_2025.csv')

# --- Model Paths (Dictionary) ---
MODELS_CONFIG = {
    'QB': {
        'model': os.path.join(MODEL_DIR, 'xgboost_QB_sliding_window_v1(TimeSeries).joblib'),
        'features': os.path.join(MODEL_DIR, 'feature_names_QB_sliding_window_v1(TimeSeries).json')
    },
    'RB': {
        'model': os.path.join(MODEL_DIR, 'xgboost_RB_sliding_window_v1(TimeSeries).joblib'),
        'features': os.path.join(MODEL_DIR, 'feature_names_RB_sliding_window_v1(TimeSeries).json')
    },
    'WR': {
        'model': os.path.join(MODEL_DIR, 'xgboost_WR_sliding_window_v1(TimeSeries).joblib'),
        'features': os.path.join(MODEL_DIR, 'feature_names_WR_sliding_window_v1(TimeSeries).json')
    },
    'TE': {
        'model': os.path.join(MODEL_DIR, 'xgboost_TE_sliding_window_v1(TimeSeries).joblib'),
        'features': os.path.join(MODEL_DIR, 'feature_names_TE_sliding_window_v1(TimeSeries).json')
    }
}
# --- Meta Model Paths ---
META_MODEL_PATH = os.path.join(MODEL_DIR, 'xgboost_META_model_v1.joblib')
META_FEATURES_PATH = os.path.join(MODEL_DIR, 'feature_names_META_model_v1.json')


model_data = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Loading data and models from PostgreSQL...")
    try:
        # 1. Load Data from DB (all pl.read_csv replaced)
        
        # Load necessary tables
        model_data["df_profile"] = pl.read_database_uri("SELECT * FROM player_profiles", DB_CONNECTION_STRING)
        model_data["df_schedule"] = pl.read_database_uri("SELECT * FROM schedule", DB_CONNECTION_STRING)
        model_data["df_player_stats"] = pl.read_database_uri("SELECT * FROM weekly_player_stats", DB_CONNECTION_STRING)
        model_data["df_defense"] = pl.read_database_uri("SELECT * FROM weekly_defense_stats", DB_CONNECTION_STRING)
        model_data["df_offense"] = pl.read_database_uri("SELECT * FROM weekly_offense_stats", DB_CONNECTION_STRING)
        model_data["df_snap_counts"] = pl.read_database_uri("SELECT * FROM weekly_snap_counts", DB_CONNECTION_STRING)
        
        # --- FIX: df_players_map loading is now removed as the generator doesn't use it ---
        print("All RAG data files loaded from DB.")

        # 2. Load Base (L0) Models
        model_data["models"] = {}
        for pos, paths in MODELS_CONFIG.items():
            if os.path.exists(paths['model']) and os.path.exists(paths['features']):
                print(f"Loading {pos} model from {paths['model']}...")
                model = joblib.load(paths['model'])
                with open(paths['features'], 'r') as f:
                    features = json.load(f)
                model_data["models"][pos] = {"model": model, "features": features}
                print(f"Loaded {pos} model and {len(features)} top features.")
            else:
                print(f"Warning: Model file missing for {pos}.")

        # 3. Load Meta (L1) Model
        print(f"Loading META models...")
        model_data["meta_models"] = joblib.load(META_MODEL_PATH)
        with open(META_FEATURES_PATH, 'r') as f:
            model_data["meta_features"] = json.load(f)
        print(f"Loaded {len(model_data['meta_models'])} meta-models.")

        # 4. Fuzzy Match List
        relevant_positions = ['QB', 'RB', 'WR', 'TE']
        filtered_profile = model_data["df_profile"].filter(pl.col('position').is_in(relevant_positions))
        model_data["player_name_id_list"] = list(zip(filtered_profile["player_name"], filtered_profile["player_id"]))
        
        # 5. Current Week
        try: model_data["current_nfl_week"] = nfl.get_current_week()
        except Exception: model_data["current_nfl_week"] = 10 
            
        print(f"Server Ready. Current NFL week is: {model_data['current_nfl_week']}")

    except Exception as e:
        print(f"FATAL ERROR during server startup: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    yield
    model_data.clear()

app = FastAPI(lifespan=lifespan)

# --- Pydantic Models ---
class PlayerRequest(BaseModel):
    player_name: str
    week: Optional[int] = None

class CompareRequest(BaseModel):
    player1_name: str
    player2_name: str
    week: Optional[int] = None

# --- Helper Functions ---

def find_player(player_name_input: str):
    """Finds player with exact (case-insensitive) match."""
    try:
        player_match = model_data["df_profile"].filter(
            pl.col('player_name').str.to_lowercase() == player_name_input.lower()
        )
        if player_match.is_empty():
            return None, None
        
        player_row = player_match.row(0, named=True)
        return player_row['player_name'], player_row['player_id']
    except Exception:
        return None, None

def predict_points(pos: str, features_dict: dict):
    """Selects correct model based on position and predicts."""
    if pos not in model_data["models"]:
        return None, f"No model loaded for position: {pos}"
    
    model_info = model_data["models"][pos]
    model = model_info["model"]
    trained_feature_names = model_info["features"]
    
    try:
        feature_data = {}
        for feature_name in trained_feature_names:
            val = features_dict.get(feature_name, 0.0) 
            feature_data[feature_name] = [val if val is not None and not np.isnan(val) else 0.0]
            
        df_pred = pl.DataFrame(feature_data).select(trained_feature_names)
        preds = model.predict(df_pred.to_numpy())
        return float(preds[0]), None
    except Exception as e:
        print(f"Error during prediction: {e}")
        import traceback
        traceback.print_exc()
        return None, f"Prediction Error: {e}"

def get_base_prediction(pid: str, pos: str, target_week: int):
    """Gets the Level 0 (Base) prediction for a single player."""
    
    # --- Generate Features (Only one call) ---
    # FIX: df_players_map argument is removed to avoid TypeError
    features, err = generate_features_all(
        pid, target_week,
        df_profile=model_data["df_profile"], 
        df_schedule=model_data["df_schedule"],
        df_player_stats=model_data["df_player_stats"], 
        df_defense=model_data["df_defense"],
        df_offense=model_data["df_offense"],
        df_snap_counts=model_data["df_snap_counts"]
    )
    if not features: return None, err or "Feature gen failed", None
    
    # --- Prediction ---
    l0_score, l0_err = predict_points(pos, features)
    
    # Return features along with score/error
    return l0_score, l0_err, features 


def find_teammate(team_abbr: str, pos: str, exclude_pid: str):
    """Finds the main player for a team/position."""
    candidates = model_data["df_profile"].filter(
        (pl.col('team_abbr') == team_abbr) &
        (pl.col('position') == pos) &
        (pl.col('status') == 'ACT') &
        (pl.col('player_id') != exclude_pid)
    )
    if candidates.is_empty(): return None, None
    return candidates.row(0, named=True)['player_name'], candidates.row(0, named=True)['player_id']

# --- Endpoints ---
# --- NEW: Ranking Endpoints ---
@app.get("/rankings/past/{week}")
async def get_past_rankings(week: int):
    """Returns top 10 performers (Actual vs Predicted) for a past week."""
    try:
        query = f"""
            SELECT player_name, position, team, opponent, predicted_points, actual_points 
            FROM weekly_rankings 
            WHERE week = {week} AND actual_points IS NOT NULL
            ORDER BY actual_points DESC 
            LIMIT 10
        """
        df = pl.read_database_uri(query, DB_CONNECTION_STRING)
        return df.to_dicts()
    except Exception as e:
        print(f"Error fetching rankings: {e}")
        return []

@app.get("/rankings/future/{week}")
async def get_future_rankings(week: int):
    """Returns top 10 projected performers for an upcoming week."""
    try:
        query = f"""
            SELECT player_name, position, team, opponent, predicted_points 
            FROM weekly_rankings 
            WHERE week = {week} 
            ORDER BY predicted_points DESC 
            LIMIT 10
        """
        df = pl.read_database_uri(query, DB_CONNECTION_STRING)
        return df.to_dicts()
    except Exception as e:
        print(f"Error fetching future rankings: {e}")
        return []

@app.get("/players/all")
async def get_all_players():
    """Returns a lightweight list of all players for Auto-Complete."""
    try:
        df = model_data["df_profile"].filter(
            pl.col('position').is_in(['QB', 'RB', 'WR', 'TE'])
        ).select(['player_name', 'position', 'team_abbr'])
        return df.to_dicts()
    except Exception as e:
        raise HTTPException(500, f"Error fetching player list: {e}")
    
@app.post("/predict")
async def predict_single_player(request: PlayerRequest):
    target_week = request.week if request.week is not None else model_data["current_nfl_week"]
    
    actual_name, pid = find_player(request.player_name)
    if not pid: raise HTTPException(404, f"Player '{request.player_name}' not found (exact match).")
    
    try:
        player_row = model_data["df_profile"].filter(pl.col('player_id') == pid).row(0, named=True)
        pos = player_row['position']
        team_abbr = player_row['team_abbr']
    except Exception as e: 
        print(f"Error extracting player position/team: {e}")
        pos = "UNK"; team_abbr = "UNK"
    
    if pos not in model_data["models"]:
        raise HTTPException(404, f"No model available for position: {pos}")

    # --- Step 1: Get L0 Prediction for the main player ---
    l0_score, l0_err, features = get_base_prediction(pid, pos, target_week)
    if l0_err: raise HTTPException(500, f"L0 prediction failed: {l0_err}")
    
    # --- Step 2: Get L0 Predictions for Teammates (Ecosystem) ---
    qb_name, qb_pid = find_teammate(team_abbr, 'QB', pid)
    if pos == 'QB': l0_pred_qb = l0_score
    elif qb_pid: l0_pred_qb, _, _ = get_base_prediction(qb_pid, 'QB', target_week)
    else: l0_pred_qb = 0.0

    rb_name, rb_pid = find_teammate(team_abbr, 'RB', pid)
    if pos == 'RB': l0_pred_rb = l0_score
    elif rb_pid: l0_pred_rb, _, _ = get_base_prediction(rb_pid, 'RB', target_week)
    else: l0_pred_rb = 0.0
        
    wr_name, wr_pid = find_teammate(team_abbr, 'WR', pid)
    if pos == 'WR': l0_pred_wr = l0_score
    elif wr_pid: l0_pred_wr, _, _ = get_base_prediction(wr_pid, 'WR', target_week)
    else: l0_pred_wr = 0.0

    te_name, te_pid = find_teammate(team_abbr, 'TE', pid)
    if pos == 'TE': l0_pred_te = l0_score
    elif te_pid: l0_pred_te, _, _ = get_base_prediction(te_pid, 'TE', target_week)
    else: l0_pred_te = 0.0
        
    l0_pred_qb = l0_pred_qb or 0.0
    l0_pred_rb = l0_pred_rb or 0.0
    l0_pred_wr = l0_pred_wr or 0.0
    l0_pred_te = l0_pred_te or 0.0
        
    # --- Step 3: Get L1 Meta-Model Prediction ---
    try:
        meta_model = model_data["meta_models"][pos]
        meta_features = model_data["meta_features"]
        
        meta_input_data = {
            'L0_pred_QB': [l0_pred_qb],
            'L0_pred_RB': [l0_pred_rb],
            'L0_pred_WR': [l0_pred_wr],
            'L0_pred_TE': [l0_pred_te]
        }
        
        meta_df = pl.DataFrame(meta_input_data).select(meta_features)
        meta_score = meta_model.predict(meta_df.to_numpy())[0]
        
    except Exception as e:
        print(f"Meta-model prediction failed: {e}")
        meta_score = None 

    # --- Step 4: Format Response ---
    l0_mae = MAE_VALUES.get(pos, 5.0)
    
    response = {
        "player_name": actual_name, "position": pos, "week": target_week,
        "opponent": features.get('opponent', 'N/A'),
        
        "position_specific_prediction": {
            "predicted_points": round(l0_score, 2),
            "expected_range": f"{max(0, l0_score-l0_mae):.2f} - {l0_score+l0_mae:.2f} (+/- {l0_mae:.2f})"
        }
    }
    
    if meta_score is not None:
        l1_mae = META_MAE_VALUES.get(pos, 5.0)
        response["ecosystem_aware_prediction"] = {
            "predicted_points": round(float(meta_score), 2),
            "expected_range": f"{max(0, meta_score-l1_mae):.2f} - {meta_score+l1_mae:.2f} (+/- {l1_mae:.2f})",
            "context_message": "This prediction is adjusted based on the predicted performance of the player's teammates."
        }
        
    print(f"Prediction successful for {actual_name}.")
    return response

@app.post("/compare")
async def compare_players(request: CompareRequest):
    target_week = request.week if request.week is not None else model_data["current_nfl_week"]
    results = []
    
    for input_name in [request.player1_name, request.player2_name]:
        try:
            # We can just call our /predict endpoint's logic
            response = await predict_single_player(PlayerRequest(player_name=input_name, week=target_week))
            results.append({"input_name": input_name, **response})
        except HTTPException as e:
            results.append({"input_name": input_name, "error": e.detail})
        except Exception as e:
            results.append({"input_name": input_name, "error": str(e)})
        
    return {"week": target_week, "comparison": results}

if __name__ == '__main__':
    import uvicorn
    print("Starting Multi-Model Time-Series Server (Exact Match)...")
    current_dir_main = os.path.dirname(os.path.abspath(__file__))
    dataPrep_dir_main = os.path.abspath(os.path.join(current_dir_main, '..', 'dataPrep'))
    rag_data_dir = os.path.abspath(os.path.join(current_dir_main, '..', 'rag_data'))
    
    uvicorn.run(
        "server:app", 
        host="127.0.0.1", 
        port=8000, 
        reload=True,
        reload_dirs=[current_dir_main, dataPrep_dir_main, rag_data_dir]
    )