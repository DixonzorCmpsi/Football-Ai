import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
INPUT_FILE = Path("featured_dataset_avg.csv")
OUTPUT_FILE = Path("timeseries_training_data_QB_avg.csv")
N_LAGS = 3 # Number of past games to use

# --- 1. PLAYER Stats to Lag (Dynamic History) ---
# "How did he do last week? And the week before?"
PLAYER_STATS_TO_LAG = [
    'offense_snaps', 'offense_pct', 
    'pass_attempts', 'passing_air_yards', 'passer_rating', 'interception', 'pass_attempts_redzone', 'pass_pct',
    'total_off_yards', 'touches', 'rush_attempts', 'rushing_yards', 'ypc', 'rush_attempts_redzone', 'rush_touchdown',
    'y_fantasy_points_ppr' # Autoregression target
]

# --- 2. CONTEXT Features (Current Week) ---
# "Who is he playing against? What is the game script?"
CONTEXT_FEATURES = [
    # Identifiers
    'season', 'week', 'player_id', 'player_name', 'position', 'team', 'opponent',
    
    # Profile
    'age', 'years_exp', 'draft_ovr', 'shotgun', 'no_huddle',
    
    # [NEW] Player Baseline (Crucial for Deviation Model)
    'player_season_avg_points',

    # --- Opponent DEFENSE (Rolling + Lags) ---
    'rolling_avg_points_allowed_4_weeks',
    'rolling_avg_passing_yards_allowed_4_weeks',
    'rolling_avg_rushing_yards_allowed_4_weeks',
    'rolling_avg_sack_4_weeks',
    'rolling_avg_interception_4_weeks',
    'rolling_avg_qb_hit_4_weeks',
    'rolling_avg_points_allowed_to_QB', # Position specific

    # [NEW] Explicit Opponent Defense Lags (L1, L2, L3)
    # For QBs, knowing if the opponent gave up 300+ yards last week is huge.
    'opp_def_points_allowed_lag_1', 'opp_def_points_allowed_lag_2', 'opp_def_points_allowed_lag_3',
    'opp_def_passing_yards_allowed_lag_1', 'opp_def_passing_yards_allowed_lag_2', 'opp_def_passing_yards_allowed_lag_3',
    'opp_def_interception_lag_1', 'opp_def_interception_lag_2', 'opp_def_interception_lag_3',
    'opp_def_sack_lag_1', 'opp_def_sack_lag_2', 'opp_def_sack_lag_3',

    # --- Opponent OFFENSE (Game Script) ---
    # High opponent offense = likely shootout = more passing volume for our QB.
    'opp_off_rolling_total_off_points_4_weeks',
    'opp_off_rolling_total_yards_4_weeks',
    'opp_off_rolling_passing_yards_4_weeks',
    'opp_off_rolling_rushing_yards_4_weeks',

    'opp_off_total_off_points_lag_1', 'opp_off_total_off_points_lag_2', 'opp_off_total_off_points_lag_3',
    'opp_off_total_yards_lag_1', 'opp_off_total_yards_lag_2', 'opp_off_total_yards_lag_3'
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
            df_out[new_col_name] = g[col].shift(i)
            
    return df_out, new_lag_cols

def main():
    print(f"--- Creating QB Time-Series Dataset ---")
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except Exception as e:
        print(f"Error: {e}")
        return
        
    print(f"Loaded {len(df)} rows.")

    # --- Filter for QBs ---
    if 'position' in df.columns:
        df = df[df['position'] == 'QB'].copy()
        print(f"Filtered to {len(df)} QB rows.")

    # --- 1. Create Lags for Player Stats ---
    actual_lag_cols = [c for c in PLAYER_STATS_TO_LAG if c in df.columns]
    df_lagged, new_lag_names = create_lagged_features(df, actual_lag_cols, n_lags=N_LAGS)

    # --- 2. Select Final Columns ---
    # Only keep context features that actually exist in the featured_dataset
    actual_context_cols = [c for c in CONTEXT_FEATURES if c in df_lagged.columns]
    
    cols_to_keep = actual_context_cols + new_lag_names
    cols_to_keep.append(TARGET_VARIABLE)
    
    final_df = df_lagged[cols_to_keep].copy()

    # --- 3. Clean Up ---
    print(f"Original shape: {final_df.shape}")
    
    # Drop rows with NaN in lagged columns (First N weeks of a player's career)
    final_df.dropna(subset=new_lag_names, inplace=True)
    final_df.fillna(0, inplace=True)
    
    print(f"Final shape for training: {final_df.shape}")
    
    # --- 4. Save ---
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved time-series dataset to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()