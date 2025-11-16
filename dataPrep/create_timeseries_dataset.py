# dataPrep/create_timeseries_dataset.py
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
INPUT_FILE = Path("featured_dataset_QB.csv")
OUTPUT_FILE = Path("timeseries_training_data_QB.csv")
N_LAGS = 3 # Number of past games to use as features (e.g., 3 games)

# --- Define Features ---

# These are the "dynamic" stats from the current week that we want to
# use as "lagged" features (e.g., last week's stats, 2 weeks ago stats, etc.)
# We also include the opponent/context stats to lag them.
STATS_TO_LAG = [
    'offense_snaps', 'offense_pct', 'pass_attempts', 'passing_air_yards',
    'passer_rating', 'interception', 'pass_attempts_redzone', 'pass_pct',
    'total_off_yards', 'touches', 'rush_attempts', 'rushing_yards', 'ypc',
    'rush_attempts_redzone', 'rush_touchdown',
    'rolling_avg_points_allowed_4_weeks',
    'rolling_avg_passing_yards_allowed_4_weeks',
    'rolling_avg_rushing_yards_allowed_4_weeks',
    'rolling_avg_sack_4_weeks',
    'rolling_avg_interception_4_weeks',
    'rolling_avg_qb_hit_4_weeks',
    'rolling_avg_points_allowed_to_QB',
    'rolling_avg_points_allowed_to_RB',
    'rolling_avg_points_allowed_to_WR',
    'rolling_avg_points_allowed_to_TE',
    'y_fantasy_points_ppr' # We lag the *target* as a feature
]

# These are "static" (or current-week) features we will keep as-is
# They describe the *current* situation for the prediction week
STATIC_FEATURES_TO_KEEP = [
    'season', 'week', 'player_id', 'player_name', 'position', 'age', 
    'years_exp', 'team', 'opponent', 'draft_ovr'
    # Note: We are replacing the '3_game_avg' stats with explicit lags
]

# This is the value we are trying to predict
TARGET_VARIABLE = 'y_fantasy_points_ppr'


def create_lagged_features(df, features_to_lag, n_lags=3):
    """
    Creates lagged features for a list of columns, grouped by player.
    """
    print(f"Creating {n_lags} lag(s) for {len(features_to_lag)} features...")
    df_out = df.copy()
    # Sort to ensure lags are chronological
    df_out.sort_values(by=['player_id', 'season', 'week'], inplace=True)
    
    # Group by player so lags don't cross over
    g = df_out.groupby('player_id')
    
    new_lag_cols = []
    for col in tqdm(features_to_lag, desc="Lagging features"):
        for i in range(1, n_lags + 1):
            new_col_name = f"{col}_lag_{i}"
            new_lag_cols.append(new_col_name)
            df_out[new_col_name] = g[col].shift(i)
            
    return df_out, new_lag_cols

def main():
    """
    Main function to load, process, and save the time-series dataset.
    """
    print(f"--- Creating Time-Series (Lagged) Dataset ---")
    print(f"Loading QB-only data from: {INPUT_FILE}...")
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except FileNotFoundError:
        print(f"Error: File not found at {INPUT_FILE}.")
        return
    except Exception as e:
        print(f"Error loading file: {e}")
        return
        
    print(f"Loaded {len(df)} historical QB game rows.")

    # --- 1. Create Lagged Features ---
    # Find which stats to lag are actually present
    existing_stats_to_lag = [col for col in STATS_TO_LAG if col in df.columns]
    missing_lag_stats = set(STATS_TO_LAG) - set(existing_stats_to_lag)
    if missing_lag_stats:
        print(f"Warning: Cannot create lags for missing columns: {missing_lag_stats}")
        
    df_lagged, new_lag_cols = create_lagged_features(df, existing_stats_to_lag, n_lags=N_LAGS)

    # --- 2. Define Final Column Set ---
    # We keep static features, new lag features, and the target
    final_feature_columns = [col for col in STATIC_FEATURES_TO_KEEP if col in df_lagged.columns]
    final_feature_columns.extend(new_lag_cols)
    
    # Select the final set of features + the target variable
    final_df = df_lagged[final_feature_columns + [TARGET_VARIABLE]].copy()

    # --- 3. Clean Up Data ---
    print(f"Original shape before dropping NaNs: {final_df.shape}")
    # After shifting, the first N rows for each player will be NaN.
    # We must drop them as we can't make a prediction with no past data.
    final_df.dropna(inplace=True)
    print(f"New shape after dropping NaNs: {final_df.shape}")

    # Fill any other potential NaNs with 0 (e.g., from original data)
    final_df.fillna(0, inplace=True)
    
    # --- 4. Save the new dataset ---
    print(f"Saving time-series dataset to: {OUTPUT_FILE}...")
    final_df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")
    print(f"Final dataset has {final_df.shape[0]} rows and {final_df.shape[1]} columns.")
    print("\nFirst 5 rows of new data:")
    print(final_df.head())

if __name__ == "__main__":
    main()