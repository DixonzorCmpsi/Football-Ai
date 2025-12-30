import polars as pl
import joblib
import json
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime

# --- Setup Paths ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, os.path.join(project_root, 'applications'))

try:
    from feature_generator_timeseries import generate_features_all
except ImportError:
    print("❌ Error: Could not find 'feature_generator_timeseries.py'.")
    print(f"   Checked path: {os.path.join(project_root, 'applications')}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()

def get_current_nfl_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_nfl_season()
print(f"Auto-detected NFL Season for Rankings: {SEASON}")

DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
MODEL_DIR = os.path.join(project_root, 'model_training', 'models')

# Ensure this matches your actual model filenames
MODELS_CONFIG = {
    'QB': {'model': 'xgboost_QB_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_QB_sliding_window_v1(TimeSeries).json'},
    'RB': {'model': 'xgboost_RB_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_RB_sliding_window_v1(TimeSeries).json'},
    'WR': {'model': 'xgboost_WR_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_WR_sliding_window_v1(TimeSeries).json'},
    'TE': {'model': 'xgboost_TE_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_TE_sliding_window_v1(TimeSeries).json'}
}

def main():
    print("--- Generating Weekly Rankings for ALL Players ---")
    
    if not DB_CONNECTION_STRING:
        print("❌ Error: DB_CONNECTION_STRING not found.")
        return

    # 1. Load Data (Robust Individual Loading)
    print(f"Loading data from Postgres for {SEASON}...")
    
    tables_to_load = {
        "player_profiles": f"SELECT * FROM player_profiles WHERE season = {SEASON}",
        "schedule": f"SELECT * FROM schedule WHERE season = {SEASON}",
        "weekly_player_stats": f"SELECT * FROM weekly_player_stats WHERE season = {SEASON}",
        "weekly_defense_stats": f"SELECT * FROM weekly_defense_stats WHERE season = {SEASON}",
        "weekly_offense_stats": f"SELECT * FROM weekly_offense_stats WHERE season = {SEASON}",
        "weekly_snap_counts": f"SELECT * FROM weekly_snap_counts WHERE season = {SEASON}"
    }
    
    data_frames = {}
    
    for name, query in tables_to_load.items():
        print(f"   - Loading {name}...", end=" ")
        try:
            df = pl.read_database_uri(query, DB_CONNECTION_STRING)
            if df.is_empty():
                print(f"⚠️ Warning: Table is empty.")
            else:
                print("✅")
            data_frames[name] = df
        except Exception as e:
            print(f"❌ FAILED.")
            print(f"     Error: {e}")
            print(f"     Hint: Does table '{name}' exist and have a 'season' column?")
            return # Critical failure, stop execution

    # Assign to variables for easy access
    df_profile = data_frames["player_profiles"]
    df_schedule = data_frames["schedule"]
    df_stats = data_frames["weekly_player_stats"]
    df_def = data_frames["weekly_defense_stats"]
    df_off = data_frames["weekly_offense_stats"]
    df_snaps = data_frames["weekly_snap_counts"]

    # 2. Load Models
    loaded_models = {}
    print("Loading AI Models...")
    for pos, files in MODELS_CONFIG.items():
        m_path = os.path.join(MODEL_DIR, files['model'])
        f_path = os.path.join(MODEL_DIR, files['feat'])
        if os.path.exists(m_path) and os.path.exists(f_path):
            try:
                loaded_models[pos] = {
                    "model": joblib.load(m_path),
                    "features": json.load(open(f_path))
                }
            except Exception as e:
                print(f"   ⚠️ Error loading {pos} model: {e}")
    
    if not loaded_models:
        print("   ⚠️ No models loaded. Skipping predictions.")
        return

    # 3. Determine Weeks to Rank
    last_completed_week = df_stats['week'].max()
    if last_completed_week is None: last_completed_week = 0
    
    # Predict for: [Last Week, Next Week]
    # (Last Week is re-run to provide "Actual vs Predicted" accuracy if games just finished)
    target_weeks = [last_completed_week, last_completed_week + 1]
    
    # Valid weeks only (1-18)
    target_weeks = [w for w in target_weeks if 1 <= w <= 18]
    if not target_weeks: target_weeks = [1] # Default to Week 1
    
    # Remove duplicates and sort
    target_weeks = sorted(list(set(target_weeks)))
    
    print(f"Generating rankings for Weeks: {target_weeks}")

    all_predictions = []
    
    # Get Active Players (QB, RB, WR, TE)
    active_players = df_profile.filter(
        pl.col('position').is_in(['QB', 'RB', 'WR', 'TE'])
    ).unique(subset=['player_id'])

    if active_players.is_empty():
        print("❌ No active players found in profiles.")
        return

    # 4. Generate Predictions Loop
    for week in target_weeks:
        print(f"   > Processing Week {week} ({len(active_players)} players)...")
        
        # Using tqdm correctly
        for row in tqdm(active_players.iter_rows(named=True), total=len(active_players), disable=True):
            pid = row['player_id']
            pos = row['position']
            name = row['player_name']
            
            injury_status = row.get('injury_status')
            is_injured = 1 if injury_status and injury_status.strip() and injury_status != 'Healthy' else 0
            
            if pos not in loaded_models: continue

            # --- Feature Generation ---
            feats, err = generate_features_all(
                pid, week, 
                df_profile=df_profile, df_schedule=df_schedule, df_player_stats=df_stats,
                df_defense=df_def, df_offense=df_off, df_snap_counts=df_snaps
            )
            
            if not feats: continue 

            # --- Prediction ---
            model = loaded_models[pos]['model']
            feature_names = loaded_models[pos]['features']
            
            try:
                # Ensure all features exist, default to 0.0
                feature_data = {name: [feats.get(name, 0.0)] for name in feature_names}
                X = pl.DataFrame(feature_data).select(feature_names).to_numpy()
                pred = float(model.predict(X)[0])
                
                # Get Actuals if available (for reference)
                actual = None
                if week <= last_completed_week:
                    actual_row = df_stats.filter((pl.col('player_id')==pid) & (pl.col('week')==week))
                    if not actual_row.is_empty():
                        actual = actual_row['y_fantasy_points_ppr'].item()

                all_predictions.append({
                    'player_id': pid,
                    'player_name': name,
                    'position': pos,
                    'team': feats.get('team', row.get('team_abbr')),
                    'opponent': feats.get('opponent'),
                    'season': SEASON,
                    'week': float(week), # Float for consistency
                    'predicted_points': round(pred, 2),
                    'actual_points': actual,
                    'is_injured': is_injured,
                    'injury_status': injury_status if injury_status else "Healthy"
                })

            except Exception: continue

    # 5. Save Results
    if all_predictions:
        print(f"Generated {len(all_predictions)} predictions.")
        
        # Enforce Schema for Database Compatibility
        schema_overrides = {
            "player_id": pl.Utf8,
            "player_name": pl.Utf8,
            "position": pl.Utf8,
            "team": pl.Utf8,
            "opponent": pl.Utf8,
            "season": pl.Int64,
            "week": pl.Float64,        
            "predicted_points": pl.Float64,
            "actual_points": pl.Float64,
            "is_injured": pl.Int64,
            "injury_status": pl.Utf8
        }
        
        try:
            df_final = pl.DataFrame(all_predictions, schema=schema_overrides)
        except Exception as e:
            print(f"⚠️ Strict schema failed ({e}), attempting inference...")
            df_final = pl.DataFrame(all_predictions, infer_schema_length=None)

        # Ranking Logic
        df_final = df_final.sort(['week', 'position', 'predicted_points'], descending=[False, False, True])
        
        # Add Rank Column
        df_final = df_final.with_columns(
            pl.col("predicted_points").rank(method="min", descending=True)
            .over(["week", "position"])
            .alias("position_rank")
        )
        
        # Output
        output_file = os.path.join(current_dir, 'weekly_rankings.csv')
        try:
            df_final.write_csv(output_file)
            print(f"✅ Rankings saved to {output_file}")
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")
    else:
        print("❌ No predictions generated. (Did features fail to generate?)")

if __name__ == "__main__":
    main()