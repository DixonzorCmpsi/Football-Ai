# dataPrep/create_timeseries_dataset_QB.py
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
INPUT_FILE = Path("featured_dataset.csv")
OUTPUT_FILE = Path("timeseries_training_data_QB.csv")
N_LAGS = 3 # Number of past games to use

# --- 1. PLAYER Stats to Lag (Dynamic History) ---
# These are the stats where we want to know: "How did he do last week? And the week before?"
# We will create _lag_1, _lag_2, etc. for these.
PLAYER_STATS_TO_LAG = [
    'offense_snaps', 'offense_pct', 
    'pass_attempts', 'passing_air_yards', 'passer_rating', 'interception', 'pass_attempts_redzone', 'pass_pct',
    'total_off_yards', 'touches', 'rush_attempts', 'rushing_yards', 'ypc', 'rush_attempts_redzone', 'rush_touchdown',
    'y_fantasy_points_ppr' # We lag the target itself (Autoregression)
]

# --- 2. CONTEXT Features (Keep As-Is for Current Week) ---
# These describe the *current* situation (Matchup, Age, etc.)
# We do NOT lag these, because we want to know who he is playing *this* week.
CONTEXT_FEATURES = [
    # Identifiers
    'season', 'week', 'player_id', 'player_name', 'position', 'team', 'opponent',
    
    # Profile
    'age', 'years_exp', 'draft_ovr', 'shotgun', 'no_huddle',
    
    # Opponent Matchup Stats (The defense he is facing THIS week)
    'rolling_avg_points_allowed_4_weeks',
    'rolling_avg_passing_yards_allowed_4_weeks',
    'rolling_avg_rushing_yards_allowed_4_weeks',
    'rolling_avg_sack_4_weeks',
    'rolling_avg_interception_4_weeks',
    'rolling_avg_qb_hit_4_weeks',
    'rolling_avg_points_allowed_to_QB',
    'rolling_avg_points_allowed_to_RB',
    'rolling_avg_points_allowed_to_WR',
    'rolling_avg_points_allowed_to_TE'
]

TARGET_VARIABLE = 'y_fantasy_points_ppr'

def create_lagged_features(df, features_to_lag, n_lags=3):
    """Creates lagged features grouped by player."""
    print(f"Creating {n_lags} lag(s) for {len(features_to_lag)} player stats...")
    df_out = df.copy()
    df_out.sort_values(by=['player_id', 'season', 'week'], inplace=True)
    
    g = df_out.groupby('player_id')
    
    new_lag_cols = []
    for col in tqdm(features_to_lag, desc="Lagging features"):
        for i in range(1, n_lags + 1):
            new_col_name = f"{col}_lag_{i}"
            new_lag_cols.append(new_col_name)
            # Shift(1) takes the previous row's value and puts it in the current row
            df_out[new_col_name] = g[col].shift(i)
            
    return df_out, new_lag_cols

def main():
    print(f"--- Creating Leak-Proof Time-Series Dataset ---")
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except Exception as e:
        print(f"Error: {e}")
        return
        
    print(f"Loaded {len(df)} rows.")

    # --- 1. Create Lags for Player Stats ---
    # Only lag columns that actually exist in the file
    actual_lag_cols = [c for c in PLAYER_STATS_TO_LAG if c in df.columns]
    df_lagged, new_lag_names = create_lagged_features(df, actual_lag_cols, n_lags=N_LAGS)

    # --- 2. Select Final Columns ---
    # Keep Context Features + New Lagged Features + Target
    # Drop the *original* current-week player stats (Prevention of Leakage)
    
    cols_to_keep = [c for c in CONTEXT_FEATURES if c in df_lagged.columns]
    cols_to_keep.extend(new_lag_names)
    cols_to_keep.append(TARGET_VARIABLE)
    
    final_df = df_lagged[cols_to_keep].copy()

    # --- 3. Clean Up ---
    print(f"Original shape: {final_df.shape}")
    
    # Drop rows with NaN in lagged columns (First N weeks of a player's career)
    # We cannot predict if we don't have history
    final_df.dropna(subset=new_lag_names, inplace=True)
    
    print(f"Shape after dropping initial weeks (no history): {final_df.shape}")
    
    # Fill any remaining NaNs (e.g. in static features) with 0
    final_df.fillna(0, inplace=True)
    
    # --- 4. Save ---
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved time-series dataset to: {OUTPUT_FILE}")
    print("Columns included:")
    print(final_df.columns.tolist())

if __name__ == "__main__":
    main()