import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
INPUT_FILE = Path("featured_dataset_avg.csv")
OUTPUT_FILE = Path("timeseries_training_data_WR_avg.csv")
N_LAGS = 3

# --- 1. PLAYER Stats to Lag ---
# Dynamic history of the player's performance
PLAYER_STATS_TO_LAG = [
    'offense_snaps', 'offense_pct', 
    'targets', 'receptions', 'receiving_yards', 'yards_after_catch', 'receiving_air_yards', 
    'adot', 'ayptarget', 'ypr', 'targets_redzone', 'receptions_redzone', 'receiving_touchdown',
    'team_targets_share', 'team_receptions_share',
    'y_fantasy_points_ppr' # Autoregression target
]

# --- 2. CONTEXT Features (Current Week) ---
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
    'rolling_avg_sack_4_weeks',
    'rolling_avg_interception_4_weeks',
    'rolling_avg_points_allowed_to_WR',
    'rolling_avg_points_allowed_to_QB', # Helpful context for WRs

    # [NEW] Explicit Opponent Defense Lags (L1, L2, L3)
    # Knowing if the opponent gave up 300+ passing yards last week is key for WRs
    'opp_def_points_allowed_lag_1', 'opp_def_points_allowed_lag_2', 'opp_def_points_allowed_lag_3',
    'opp_def_passing_yards_allowed_lag_1', 'opp_def_passing_yards_allowed_lag_2', 'opp_def_passing_yards_allowed_lag_3',

    # --- Opponent OFFENSE (Game Script) ---
    # High opponent offense = likely shootout = more passing volume
    'opp_off_rolling_total_off_points_4_weeks',
    'opp_off_rolling_total_yards_4_weeks',
    'opp_off_rolling_passing_yards_4_weeks',
    'opp_off_rolling_rushing_yards_4_weeks',

    'opp_off_total_off_points_lag_1', 'opp_off_total_off_points_lag_2', 'opp_off_total_off_points_lag_3',
    'opp_off_total_yards_lag_1', 'opp_off_total_yards_lag_2', 'opp_off_total_yards_lag_3'
]

TARGET_VARIABLE = 'y_fantasy_points_ppr'

def create_lagged_features(df, features_to_lag, n_lags=3):
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
    print(f"--- Creating WR Time-Series Dataset ---")
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except Exception as e: print(f"Error: {e}"); return
        
    print(f"Loaded {len(df)} rows.")

    # --- Filter for WRs ---
    if 'position' in df.columns:
        df = df[df['position'] == 'WR'].copy()
        print(f"Filtered to {len(df)} WR rows.")

    # --- 1. Create Lags ---
    actual_lag_cols = [c for c in PLAYER_STATS_TO_LAG if c in df.columns]
    df_lagged, new_lag_names = create_lagged_features(df, actual_lag_cols, n_lags=N_LAGS)

    # --- 2. Select Final Columns ---
    # Only keep context features that actually exist in the featured_dataset
    cols_to_keep = [c for c in CONTEXT_FEATURES if c in df_lagged.columns]
    cols_to_keep.extend(new_lag_names)
    cols_to_keep.append(TARGET_VARIABLE)
    
    final_df = df_lagged[cols_to_keep].copy()

    # --- 3. Clean Up ---
    final_df.dropna(subset=new_lag_names, inplace=True)
    final_df.fillna(0, inplace=True)
    
    # --- 4. Save ---
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved WR time-series dataset to: {OUTPUT_FILE}")
    print(f"Final shape: {final_df.shape}")

if __name__ == "__main__":
    main()