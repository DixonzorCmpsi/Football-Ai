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
import nflreadpy as nfl # For getting current week

# --- Add dataPrep directory to Python path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
dataPrep_dir = os.path.abspath(os.path.join(current_dir, '..', 'dataPrep'))
if dataPrep_dir not in sys.path:
    sys.path.insert(0, dataPrep_dir)

# --- Import from feature_generator ---
try:
    from feature_generator import generate_features
except ImportError as e:
    print(f"Error: Could not import feature generation function: {e}", file=sys.stderr)
    print("Ensure 'feature_generator.py' exists in the '../dataPrep/' directory.", file=sys.stderr)
    sys.exit(1)

# --- Define Constants ---
CURRENT_SEASON = 2025
MAE_VALUE = 5.5

# --- File Paths (Absolute) ---
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
PROFILE_PATH = os.path.join(PROJECT_ROOT, 'rag_data', 'player_profiles.csv')
SCHEDULE_PATH = os.path.join(PROJECT_ROOT, 'rag_data', 'schedule_2025.csv')
PLAYER_STATS_PATH = os.path.join(PROJECT_ROOT, 'rag_data', 'weekly_player_stats_2025.csv')
DEFENSE_STATS_PATH = os.path.join(PROJECT_ROOT, 'rag_data', 'weekly_defense_stats_2025.csv')
MODEL_PATH = os.path.join(current_dir, 'tuned_xgboost_baseline(.56 mae).joblib')
FEATURE_NAMES_PATH = os.path.join(current_dir, 'feature_names.json')

# --- Global dictionary to hold loaded models/data ---
model_data = {}

# --- FastAPI Lifespan Event (Loads model/data at startup) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Loading data and model for server...")
    try:
        df_profile = pl.read_csv(PROFILE_PATH)
        model_data["df_profile"] = df_profile
        model_data["df_schedule"] = pl.read_csv(SCHEDULE_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        model_data["df_player_stats"] = pl.read_csv(PLAYER_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        model_data["df_defense"] = pl.read_csv(DEFENSE_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        print("Data files loaded.")

        # Removed fuzzy matching list creation

        try:
            model_data["current_nfl_week"] = nfl.get_current_week()
            print(f"Successfully determined current NFL week: {model_data['current_nfl_week']}")
        except Exception as week_e:
            print(f"Warning: Could not determine current week: {week_e}. Defaulting to Week 1.")
            model_data["current_nfl_week"] = 1

        model_data["model_object"] = joblib.load(MODEL_PATH)
        print("Model loaded.")
        with open(FEATURE_NAMES_PATH, 'r') as f:
            model_data["trained_feature_names"] = json.load(f)
        print(f"Loaded {len(model_data['trained_feature_names'])} feature names.")

    except Exception as e:
        print(f"FATAL ERROR during server startup: {e}", file=sys.stderr)
        sys.exit(1)
    
    yield
    
    print("Shutting down server.")
    model_data.clear()

# --- Initialize FastAPI App ---
app = FastAPI(lifespan=lifespan)

# --- Prediction Function (remains the same) ---
def predict_points(model, trained_feature_names, generated_features_dict):
    if not generated_features_dict: return None
    try:
        feature_data = {}
        for feature_name in trained_feature_names:
            feature_data[feature_name] = [generated_features_dict.get(feature_name, 0.0)]
        
        feature_df_pred = pl.DataFrame(feature_data).select(trained_feature_names)
        features_for_xgb = feature_df_pred.to_numpy()
        
    except Exception as e: 
        print(f"Error preparing features: {e}", file=sys.stderr)
        return None
    try:
        prediction = model.predict(features_for_xgb)
        return float(prediction[0])
    except Exception as e: 
        print(f"Error during prediction: {e}", file=sys.stderr)
        return None

# --- Pydantic Models (remains the same) ---
class PlayerRequest(BaseModel):
    player_name: str
    week: Optional[int] = None

class CompareRequest(BaseModel):
    player1_name: str
    player2_name: str
    week: Optional[int] = None

# --- REMOVED find_player HELPER FUNCTION ---

# --- API Endpoints (Reverted to simple filter) ---
@app.post("/predict")
async def predict_single_player(request: PlayerRequest):
    """Endpoint to predict fantasy points for a single player."""
    player_name_input = request.player_name
    target_week = request.week if request.week is not None else model_data.get("current_nfl_week")
    if target_week is None:
         raise HTTPException(status_code=500, detail="Server could not determine current week.")

    print(f"\nReceived prediction request for: '{player_name_input}', Week: {target_week}")

    # --- REVERTED LOOKUP LOGIC ---
    actual_name = None
    player_id_found = None
    player_team_abbr = "UNK"
    try:
        player_match = model_data["df_profile"].filter(
            pl.col('player_name').str.to_lowercase() == player_name_input.lower()
        )
        
        if player_match.is_empty():
            print(f"Player '{player_name_input}' not found (exact match required).")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                                detail=f"Player '{player_name_input}' not found. Requires exact, case-insensitive name.")
        
        if player_match.height > 1:
             print(f"Warning: Multiple players found for '{player_name_input}', using first match.")
             player_match = player_match.head(1) # Take the first row

        player_id_found = player_match['player_id'].item()
        actual_name = player_match['player_name'].item()
        player_team_abbr = player_match['team_abbr'].item()
        print(f"Found match: {actual_name} (ID: {player_id_found})")

    except Exception as e:
        print(f"Error during player lookup: {e}", file=sys.stderr)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error during player lookup")
    # --- END REVERTED LOGIC ---

    features_dict = generate_features(
        player_id_found, target_week, current_season=CURRENT_SEASON,
        df_profile=model_data["df_profile"],
        df_schedule=model_data["df_schedule"],
        df_player_stats=model_data["df_player_stats"],
        df_defense=model_data["df_defense"]
    )

    if not features_dict:
        print("Feature generation failed (e.g., Bye Week).")
        game_check = model_data["df_schedule"].filter(
            (pl.col('week') == target_week) &
            ((pl.col('home_team') == player_team_abbr) | (pl.col('away_team') == player_team_abbr))
         )
        reason = "Likely Bye Week" if game_check.is_empty() else "Missing historical data"
        return {"player_name": actual_name, "week": target_week, "prediction": None, "message": reason}

    predicted_score = predict_points(model_data["model_object"], model_data["trained_feature_names"], features_dict)

    if predicted_score is not None:
        lower_bound = max(0, predicted_score - MAE_VALUE)
        upper_bound = predicted_score + MAE_VALUE
        response = {
            "player_name": actual_name,
            "player_id": player_id_found,
            "position": features_dict.get('position', 'N/A'),
            "team": features_dict.get('team', 'N/A'),
            "opponent": features_dict.get('opponent', 'N/A'),
            "week": target_week,
            "predicted_points": round(predicted_score, 2),
            "expected_range": f"{lower_bound:.2f} - {upper_bound:.2f} (+/- {MAE_VALUE:.1f})"
        }
        print(f"Prediction successful: {predicted_score:.2f}")
        return response
    else:
        print("Prediction failed.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Prediction failed for this player")

@app.post("/compare")
async def compare_players(request: CompareRequest):
    """Endpoint to compare predictions for two players."""
    p1_name = request.player1_name
    p2_name = request.player2_name
    target_week = request.week if request.week is not None else model_data.get("current_nfl_week")
    if target_week is None:
         raise HTTPException(status_code=500, detail="Server could not determine current week.")

    print(f"\nReceived comparison request for: {p1_name} vs {p2_name}, Week: {target_week}")
    results = []

    for name in [p1_name, p2_name]:
        result_entry = {"player_name_input": name} 
        
        # --- REVERTED LOOKUP LOGIC ---
        actual_name, player_id = None, None
        player_team_abbr = "UNK"
        try:
            player_match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == name.lower())
            if not player_match.is_empty():
                 if player_match.height > 1:
                     player_match = player_match.head(1)
                 
                 player_id = player_match['player_id'].item()
                 actual_name = player_match['player_name'].item()
                 player_team_abbr = player_match['team_abbr'].item()
                 result_entry["player_name_matched"] = actual_name
        except Exception as e:
            print(f"Lookup error for {name}: {e}");
            result_entry["error"] = "Player lookup error"
            results.append(result_entry)
            continue

        if not player_id:
            result_entry["error"] = "Player not found (exact match required)"
            results.append(result_entry)
            continue
        # --- END REVERTED LOGIC ---

        features = generate_features(
            player_id, target_week, current_season=CURRENT_SEASON,
            df_profile=model_data["df_profile"], df_schedule=model_data["df_schedule"],
            df_player_stats=model_data["df_player_stats"], df_defense=model_data["df_defense"]
        )

        if not features:
            game_check = model_data["df_schedule"].filter(
                (pl.col('week') == target_week) &
                ((pl.col('home_team') == player_team_abbr) | (pl.col('away_team') == player_team_abbr))
            )
            reason = "Likely Bye Week" if game_check.is_empty() else "Missing historical data"
            result_entry["error"] = f"Could not generate features ({reason})"
            results.append(result_entry)
            continue

        prediction = predict_points(model_data["model_object"], model_data["trained_feature_names"], features)
        if prediction is not None:
             lower = max(0, prediction - MAE_VALUE)
             upper = prediction + MAE_VALUE
             result_entry.update({
                 "position": features.get('position', 'N/A'),
                 "team": features.get('team', 'N/A'),
                 "opponent": features.get('opponent', 'N/A'),
                 "predicted_points": round(prediction, 2),
                 "expected_range": f"{lower:.2f} - {upper:.2f}"
             })
        else:
             result_entry["error"] = "Prediction failed"
        
        results.append(result_entry)

    return {"week": target_week, "comparison": results}

# --- Run Server (using uvicorn) ---
if __name__ == '__main__':
    import uvicorn
    print("Starting Uvicorn server at http://127.0.0.1:8000")
    uvicorn.run(
        "server:app", 
        host="127.0.0.1", 
        port=8000, 
        reload=True,
        reload_dirs=[current_dir, dataPrep_dir]
    )