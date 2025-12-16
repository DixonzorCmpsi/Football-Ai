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
    print("Error: Could not find feature_generator_timeseries.py")
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

MODELS_CONFIG = {
    'QB': {'model': 'xgboost_QB_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_QB_sliding_window_v1(TimeSeries).json'},
    'RB': {'model': 'xgboost_RB_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_RB_sliding_window_v1(TimeSeries).json'},
    'WR': {'model': 'xgboost_WR_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_WR_sliding_window_v1(TimeSeries).json'},
    'TE': {'model': 'xgboost_TE_sliding_window_v1(TimeSeries).joblib', 'feat': 'feature_names_TE_sliding_window_v1(TimeSeries).json'}
}

def get_db_engine():
    return create_engine(DB_CONNECTION_STRING)

def main():
    print("--- Generating Weekly Rankings for ALL Players ---")
    engine = get_db_engine()
    
    # 1. Load Data
    print("Loading data from Postgres...")
    try:
        df_profile = pl.read_database_uri("SELECT * FROM player_profiles", DB_CONNECTION_STRING)
        df_schedule = pl.read_database_uri("SELECT * FROM schedule", DB_CONNECTION_STRING)
        df_stats = pl.read_database_uri("SELECT * FROM weekly_player_stats", DB_CONNECTION_STRING)
        df_def = pl.read_database_uri("SELECT * FROM weekly_defense_stats", DB_CONNECTION_STRING)
        df_off = pl.read_database_uri("SELECT * FROM weekly_offense_stats", DB_CONNECTION_STRING)
        df_snaps = pl.read_database_uri("SELECT * FROM weekly_snap_counts", DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Error loading DB data: {e}")
        return

    # 2. Load Models
    loaded_models = {}
    for pos, files in MODELS_CONFIG.items():
        m_path = os.path.join(MODEL_DIR, files['model'])
        f_path = os.path.join(MODEL_DIR, files['feat'])
        if os.path.exists(m_path):
            print(f"Loading {pos} model...")
            loaded_models[pos] = {
                "model": joblib.load(m_path),
                "features": json.load(open(f_path))
            }
    
    # 3. Determine Weeks
    last_completed_week = df_stats['week'].max()
    if last_completed_week is None: last_completed_week = 0
    target_weeks = [last_completed_week, last_completed_week + 1]
    print(f"Generating rankings for Weeks: {target_weeks}")

    all_predictions = []
    active_players = df_profile.filter(pl.col('position').is_in(['QB', 'RB', 'WR', 'TE']))

    # 4. Generate Predictions
    for week in target_weeks:
        for row in tqdm(active_players.iter_rows(named=True), total=len(active_players), desc=f"Processing Week {week}"):
            pid = row['player_id']
            pos = row['position']
            name = row['player_name']
            
            injury_status = row.get('injury_status')
            is_injured = 1 if injury_status and injury_status.strip() else 0
            
            if pos not in loaded_models: continue

            feats, err = generate_features_all(
                pid, week, 
                df_profile=df_profile, df_schedule=df_schedule, df_player_stats=df_stats,
                df_defense=df_def, df_offense=df_off, df_snap_counts=df_snaps
            )
            
            if not feats: continue 

            model = loaded_models[pos]['model']
            feature_names = loaded_models[pos]['features']
            
            try:
                feature_data = {name: [feats.get(name, 0.0)] for name in feature_names}
                X = pl.DataFrame(feature_data).select(feature_names).to_numpy()
                pred = float(model.predict(X)[0])
                
                actual = None
                if week <= last_completed_week:
                    actual_row = df_stats.filter((pl.col('player_id')==pid) & (pl.col('week')==week))
                    if not actual_row.is_empty():
                        actual = actual_row['y_fantasy_points_ppr'].item()

                all_predictions.append({
                    'player_id': pid,
                    'player_name': name,
                    'position': pos,
                    'team': feats.get('team'),
                    'opponent': feats.get('opponent'),
                    'season': SEASON,
                    'week': week,
                    'predicted_points': round(pred, 2),
                    'actual_points': actual,
                    'is_injured': is_injured,
                    'injury_status': injury_status if injury_status else "Healthy"
                })

            except Exception: continue

    # 5. Save to CSV (Instead of DB)
    if all_predictions:
        print(f"Generated {len(all_predictions)} predictions.")
        
        # --- FIX: Define Strict Schema ---
        # Prevents crash when mixing None (null) and floats (0.0) in 'actual_points'
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
            print(f"⚠️ Strict schema failed ({e}), using inference with high limit...")
            df_final = pl.DataFrame(all_predictions, infer_schema_length=10000)

        # Sort and Rank
        df_final = df_final.sort(['week', 'position', 'predicted_points'], descending=[False, False, True])
        df_final = df_final.with_columns(
            pl.col("predicted_points").rank(method="min", descending=True)
            .over(["week", "position"])
            .alias("position_rank")
        )
        
        # Output to CSV
        output_file = os.path.join(current_dir, 'weekly_rankings.csv')
        try:
            df_final.write_csv(output_file)
            print(f"✅ Rankings saved to {output_file}")
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")
    else:
        print("No predictions generated.")

if __name__ == "__main__":
    main()