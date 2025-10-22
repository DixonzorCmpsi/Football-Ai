# dataPrep/rag-features.py
import polars as pl
from datetime import datetime
import sys
import os
import numpy as np

# --- (Imports and Path Setup remain the same) ---
# --- Add applications directory to Python path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
applications_dir = os.path.abspath(os.path.join(current_dir, '..', 'applications'))
if applications_dir not in sys.path:
    sys.path.insert(0, applications_dir)
try:
    from predict_fantasy_points import load_model_and_features, predict_points
except ImportError as e:
    print(f"Error: Could not import prediction functions: {e}", file=sys.stderr)
    sys.exit(1)

# Define file paths
PROFILE_PATH = '../rag_data/player_profiles.csv'
SCHEDULE_PATH = '../rag_data/schedule_2025.csv'
PLAYER_STATS_PATH = '../rag_data/weekly_player_stats_2025.csv'
DEFENSE_STATS_PATH = '../rag_data/weekly_defense_stats_2025.csv'
# OFFENSE_STATS_PATH = '../rag_data/weekly_offense_stats_2025.csv' # Not used currently

# Define rolling window sizes
PLAYER_ROLLING_WINDOW = 3
OPP_ROLLING_WINDOW = 4

# --- (calculate_rolling_avg function remains the same) ---
def calculate_rolling_avg(df: pl.DataFrame, col: str, window: int) -> float | None:
    if df is None or df.is_empty() or col not in df.columns or window <= 0: return 0.0
    try:
        avg = df.head(window).select(pl.col(col).mean()).item()
        return avg if avg is not None and not np.isnan(avg) else 0.0
    except (pl.ComputeError, TypeError): return 0.0
    except Exception as e: return 0.0

# --- Feature Generation Function ---
def generate_features(player_id: str, target_week: int, current_season: int = 2025):
    """Generates features, ensuring position is captured."""
    # print(f"\n--- Generating features for Player ID: {player_id}, Week: {target_week} ---") # Optional debug

    # --- 1. Load Data ---
    # ... (Loading logic remains the same) ...
    try:
        df_profile = pl.read_csv(PROFILE_PATH)
        df_schedule = pl.read_csv(SCHEDULE_PATH).with_columns(pl.col("week").cast(pl.Int64))
        df_player_stats = pl.read_csv(PLAYER_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64))
        df_defense = pl.read_csv(DEFENSE_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64))
    except Exception as e:
        print(f"Error loading data files: {e}", file=sys.stderr)
        return None


    # --- 2. Get Player Info (Add Debug for Position) ---
    player_info = df_profile.filter(pl.col('player_id') == player_id)
    if player_info.is_empty(): return None
    try:
        player_team = player_info.select('team_abbr').item()
        player_position = player_info.select('position').item() # Get position
        # --- Add Debug Print ---
        if player_position is None:
             print(f"[DEBUG Position] Position is None/Null for player {player_id} in player_profiles.csv")
        # else:
        #      print(f"[DEBUG Position] Found Position '{player_position}' for player {player_id}")
        # --- End Debug Print ---
        player_name = player_info.select('player_name').item()
        player_age = player_info.select(pl.col('age').fill_null(25)).item()
        player_years_exp = player_info.select(pl.col('years_exp').fill_null(0)).item() if 'years_exp' in player_info.columns else 0
        player_draft_ovr = player_info.select(pl.col('draft_number').fill_null(260)).item() if 'draft_number' in player_info.columns else 260
        player_status = player_info.select('injury_status').item()
    except Exception as e:
         print(f"Error extracting player info for {player_id}: {e}", file=sys.stderr)
         return None

    # --- 3. Get Opponent Info ---
    # ... (remains the same) ...
    game_info = df_schedule.filter(
        (pl.col('week') == target_week) &
        ((pl.col('home_team') == player_team) | (pl.col('away_team') == player_team))
    )
    if game_info.is_empty(): return None
    is_home = (game_info.select('home_team').item() == player_team)
    opponent_team = game_info.select('away_team').item() if is_home else game_info.select('home_team').item()


    # --- 4. Get Histories ---
    # ... (remains the same) ...
    player_history = df_player_stats.filter(
        (pl.col('player_id') == player_id) & (pl.col('week') < target_week)
    ).sort('week', descending=True)
    opponent_defense_history = df_defense.filter(
        (pl.col('team_abbr') == opponent_team) & (pl.col('week') < target_week)
    ).sort('week', descending=True)


    # --- 5. Calculate Features ---
    features = {}

    # Identifiers (include original position here for the final output table)
    features['player_id'] = player_id
    features['player_name'] = player_name
    features['position'] = player_position # Store original position string
    features['team'] = player_team
    features['target_week'] = target_week
    features['opponent'] = opponent_team
    features['season'] = current_season

    # Player Profile / Static
    features['age'] = player_age
    features['years_exp'] = player_years_exp
    features['draft_ovr'] = player_draft_ovr

    # Player Weekly Stats
    # ... (remains the same, ensures defaults are 0.0) ...
    last_game_stats = player_history.filter(pl.col('week') == target_week - 1) if target_week > 1 else pl.DataFrame()
    last_game_dict = last_game_stats.row(0, named=True) if not last_game_stats.is_empty() else {}
    features['offense_snaps'] = last_game_dict.get('offense_snaps', 0.0)
    features['offense_pct'] = last_game_dict.get('offense_pct', 0.0)
    features['touches'] = last_game_dict.get('touches', 0.0)
    features['targets'] = last_game_dict.get('targets', 0.0)
    features['receptions'] = last_game_dict.get('receptions', 0.0)
    features['rush_attempts'] = last_game_dict.get('rush_attempts', 0.0)
    features['pass_attempts'] = last_game_dict.get('pass_attempts', 0.0)
    features['receiving_yards'] = last_game_dict.get('receiving_yards', 0.0)
    features['rushing_yards'] = last_game_dict.get('rushing_yards', 0.0)
    features['yards_after_catch'] = last_game_dict.get('yards_after_catch', 0.0)
    features['passing_air_yards'] = last_game_dict.get('passing_air_yards', 0.0)
    features['receiving_air_yards'] = last_game_dict.get('receiving_air_yards', 0.0)
    features['adot'] = last_game_dict.get('adot', 0.0)
    features['yptouch'] = last_game_dict.get('yptouch', 0.0)
    features['passer_rating'] = last_game_dict.get('passer_rating', 0.0) if player_position == 'QB' else 0.0
    features['ypc'] = last_game_dict.get('ypc', 0.0)
    features['ypr'] = last_game_dict.get('ypr', 0.0)
    features['interception'] = last_game_dict.get('interception', 0.0)
    features['rush_attempts_redzone'] = last_game_dict.get('rush_attempts_redzone', 0.0)
    features['targets_redzone'] = last_game_dict.get('targets_redzone', 0.0)
    features['pass_attempts_redzone'] = last_game_dict.get('pass_attempts_redzone', 0.0)
    features['receiving_touchdown'] = last_game_dict.get('receiving_touchdown', 0.0)
    features['rush_touchdown'] = last_game_dict.get('rush_touchdown', 0.0)
    features['shotgun'] = last_game_dict.get('shotgun', 0.0)
    features['no_huddle'] = last_game_dict.get('no_huddle', 0.0)
    features['pass_pct'] = last_game_dict.get('pass_pct', 0.0) if player_position == 'QB' else 0.0
    features['total_off_yards'] = last_game_dict.get('total_off_yards', 0.0)


    # Player Rolling Averages
    prw = PLAYER_ROLLING_WINDOW
    # ... (remains the same) ...
    features[f'3_game_avg_offense_pct'] = calculate_rolling_avg(player_history, 'offense_pct', prw)
    features[f'3_game_avg_targets'] = calculate_rolling_avg(player_history, 'targets', prw)
    features[f'3_game_avg_receptions'] = calculate_rolling_avg(player_history, 'receptions', prw)
    features[f'3_game_avg_rush_attempts'] = calculate_rolling_avg(player_history, 'rush_attempts', prw)
    features[f'3_game_avg_receiving_yards'] = calculate_rolling_avg(player_history, 'receiving_yards', prw)
    features[f'3_game_avg_rushing_yards'] = calculate_rolling_avg(player_history, 'rushing_yards', prw)
    features[f'3_game_avg_y_fantasy_points_ppr'] = calculate_rolling_avg(player_history, 'y_fantasy_points_ppr', prw)


    # Opponent Defense Rolling Averages
    orw = OPP_ROLLING_WINDOW
    # ... (remains the same, use non-zero defaults) ...
    league_avg_pts_allowed = 22.0
    league_avg_pass_yds_allowed = 230.0
    league_avg_rush_yds_allowed = 115.0
    league_avg_sacks = 2.5
    league_avg_interceptions = 0.8
    features[f'rolling_avg_points_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'points_allowed', orw) or league_avg_pts_allowed
    features[f'rolling_avg_passing_yards_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'passing_yards_allowed', orw) or league_avg_pass_yds_allowed
    features[f'rolling_avg_rushing_yards_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'rushing_yards_allowed', orw) or league_avg_rush_yds_allowed
    features[f'rolling_avg_sack_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'def_sacks', orw) or league_avg_sacks
    features[f'rolling_avg_interception_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'def_interceptions', orw) or league_avg_interceptions


    # --- One-Hot Encode Position ---
    # Ensure player_position has a value, default to 'UNK' (Unknown) if None
    pos = player_position if player_position else 'UNK'
    features['position_QB'] = 1 if pos == 'QB' else 0
    features['position_RB'] = 1 if pos == 'RB' else 0
    features['position_TE'] = 1 if pos == 'TE' else 0
    features['position_WR'] = 1 if pos == 'WR' else 0
    # Add check for unexpected positions if needed
    if pos not in ['QB', 'RB', 'TE', 'WR', 'UNK']:
        print(f"Warning: Unexpected position '{pos}' found for player {player_id}.")


    # Binary Flags
    features['is_home'] = 1 if is_home else 0
    active_codes = ['ACT', 'A01']
    features['is_active'] = 1 if player_status in active_codes else 0


    # --- 6. Final Imputation (ensure all features are float/int) ---
    final_features = {}
    for k, v in features.items():
        # Ensure numeric types, handle potential string numbers if necessary
        try:
             # Convert Nones explicitly for numeric columns expected by model
             if v is None and k not in ['player_id', 'player_name', 'position', 'team', 'opponent']: # Keep identifiers as string/None
                 final_features[k] = 0.0
             # Convert NaN floats
             elif isinstance(v, float) and np.isnan(v):
                 final_features[k] = 0.0
             # Keep integers/floats as is
             elif isinstance(v, (int, float)):
                  final_features[k] = v
             # Keep identifiers
             elif k in ['player_id', 'player_name', 'position', 'team', 'opponent']:
                  final_features[k] = v if v is not None else "" # Use empty string for null identifiers if preferred
             else:
                  # Attempt conversion for anything else that should be numeric
                  final_features[k] = float(v) if v is not None else 0.0

        except (ValueError, TypeError):
             # If conversion fails for an expected numeric column, default to 0.0
             # print(f"Warning: Could not convert feature '{k}' value '{v}' to number. Defaulting to 0.0") # Optional Debug
             final_features[k] = 0.0


    return final_features

# --- Main Execution Block ---
if __name__ == "__main__":
    # --- Configuration ---
    target_prediction_week = 7 # Example: Set back to week 7 for testing
    model_path = '../model_training/models/tuned_xgboost_baseline(.56 mae).joblib'
    feature_names_path = '../model_training/models/feature_names.json'

    # --- Load Model Once ---
    model_object, trained_feature_names = load_model_and_features(model_path, feature_names_path)

    # --- Get Players ---
    try:
        all_profiles = pl.read_csv(PROFILE_PATH)
        active_codes = ['ACT', 'A01']
        players_to_predict = all_profiles.filter(
            pl.col('position').is_in(['QB', 'RB', 'WR', 'TE']) & # Filter relevant positions
            pl.col('injury_status').is_in(active_codes)
        )
        player_ids = players_to_predict['player_id'].unique().to_list()
        print(f"\nFound {len(player_ids)} unique active players [QB,RB,WR,TE] to generate predictions for Week {target_prediction_week}.")
    except Exception as e:
        print(f"Error loading or filtering player profiles: {e}", file=sys.stderr)
        player_ids = []

    # --- Loop, Generate Features, Predict ---
    predictions = []
    if not player_ids: sys.exit("No player IDs to process.")
    if not model_object: sys.exit("Model not loaded. Exiting.")

    print(f"\nGenerating predictions for {len(player_ids)} players...")
    for i, p_id in enumerate(player_ids):
        if (i + 1) % 50 == 0: print(f"  Processed {i+1}/{len(player_ids)} players...")

        features_dict = generate_features(p_id, target_prediction_week)

        if features_dict:
            # --- Add Debug for Position ---
            pos_val = features_dict.get('position')
            # if pos_val is None or pos_val == "":
            #      print(f"DEBUG: Position value is '{pos_val}' for player {features_dict.get('player_name')} ({p_id}) before prediction.")
            # --- End Debug ---

            prediction_entry = {
                'player_id': features_dict.get('player_id'),
                'player_name': features_dict.get('player_name'),
                'position': features_dict.get('position'), # Get original position
                'team': features_dict.get('team'),
                'target_week': features_dict.get('target_week'),
                'opponent_team': features_dict.get('opponent')
            }
            predicted_score = predict_points(model_object, trained_feature_names, features_dict)

            if predicted_score is not None:
                prediction_entry['predicted_points'] = round(predicted_score, 2)
            else:
                prediction_entry['predicted_points'] = None
            predictions.append(prediction_entry)


    # --- Display Results (with Schema) ---
    if predictions:
        print("\n--- Weekly Predictions ---")
        # Define schema explicitly to handle potential nulls in string columns
        schema = {
            "player_id": pl.Utf8,
            "player_name": pl.Utf8,
            "position": pl.Utf8, # Define as string
            "team": pl.Utf8,
            "target_week": pl.Int64,
            "opponent_team": pl.Utf8,
            "predicted_points": pl.Float64
        }
        predictions_df = pl.DataFrame(predictions, schema=schema)
        predictions_df = predictions_df.sort('predicted_points', descending=True, nulls_last=True)
        with pl.Config(tbl_rows=100, tbl_cols=10):
            print(predictions_df)
        # predictions_df.write_csv(f'week_{target_prediction_week}_predictions.csv')
    else:
        print("\nNo predictions were generated.")