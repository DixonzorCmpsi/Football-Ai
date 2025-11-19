# dataPrep/create_timeseries_dataset_TE.py
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
INPUT_FILE = Path("featured_dataset.csv")
OUTPUT_FILE = Path("timeseries_training_data_TE.csv")
N_LAGS = 3

# --- 1. PLAYER Stats to Lag ---
PLAYER_STATS_TO_LAG = [
    'offense_snaps', 'offense_pct', 
    'targets', 'receptions', 'receiving_yards', 'yards_after_catch', 'receiving_air_yards', 
    'adot', 'ypr', 'targets_redzone', 'receptions_redzone', 'receiving_touchdown',
    'team_targets_share', 'team_receptions_share',
    'y_fantasy_points_ppr' # Target
]

# --- 2. CONTEXT Features (Current Week) ---
CONTEXT_FEATURES = [
    'season', 'week', 'player_id', 'player_name', 'position', 'team', 'opponent',
    'age', 'years_exp', 'draft_ovr', 'shotgun', 'no_huddle',
    'rolling_avg_points_allowed_4_weeks',
    'rolling_avg_passing_yards_allowed_4_weeks',
    'rolling_avg_sack_4_weeks',
    'rolling_avg_points_allowed_to_TE',
    'rolling_avg_points_allowed_to_QB'
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
    print(f"--- Creating TE Time-Series Dataset ---")
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except Exception as e: print(f"Error: {e}"); return
        
    print(f"Loaded {len(df)} rows.")
    actual_lag_cols = [c for c in PLAYER_STATS_TO_LAG if c in df.columns]
    df_lagged, new_lag_names = create_lagged_features(df, actual_lag_cols, n_lags=N_LAGS)

    cols_to_keep = [c for c in CONTEXT_FEATURES if c in df_lagged.columns]
    cols_to_keep.extend(new_lag_names)
    cols_to_keep.append(TARGET_VARIABLE)
    
    final_df = df_lagged[cols_to_keep].copy()
    final_df.dropna(subset=new_lag_names, inplace=True)
    final_df.fillna(0, inplace=True)
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved TE time-series dataset to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()