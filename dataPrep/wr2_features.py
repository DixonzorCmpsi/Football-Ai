# dataPrep/prepare_retraining_data_WR_v2.py
import pandas as pd
import numpy as np
from pathlib import Path

# --- Configuration ---
INPUT_FILE = Path("featured_dataset.csv")
OUTPUT_FILE = Path("featured_dataset_WR_clean_v2.csv") # New, truly clean file

# --- Define ALL Columns to DROP for a WR model ---
# This list now includes all current-week production stats and other leaky features
FEATURES_TO_DROP = [
    # --- Leaky Current-Week Stats (The "Answers") ---
    'touches', 'targets', 'receptions', 'rush_attempts', 'receiving_yards', 
    'rushing_yards', 'yards_after_catch', 'receiving_air_yards', 'adot', 
    'ayptarget', 'yptouch', 'ypr', 'targets_redzone', 'receptions_redzone', 
    'receiving_touchdown', 'rush_touchdown', 'total_off_yards',

    # --- Leaky Averages that include current week/career ---
    'career_average_ppr_ppg',
    'season_average_targets',
    'season_average_touches',
    
    # --- Leaky QB/RB Stats ---
    'pass_attempts', 'passing_air_yards', 'passer_rating', 'interception',
    'pass_attempts_redzone', 'qb_dropback', 'qb_scramble', 'pass_pct',
    'team_rush_attempts_share', 'ypc', 'rush_attempts_redzone', 
    
    # --- Leaky Target Average ---
    '3_game_avg_y_fantasy_points_ppr',

    # --- Irrelevant Opponent Stats ---
    'rolling_avg_points_allowed_to_RB'
]

def main():
    print(f"--- Preparing TRUE-CLEAN WR-ONLY training data from: {INPUT_FILE} ---")

    try:
        df_original = pd.read_csv(INPUT_FILE, low_memory=False)
        print(f"Loaded dataset with {df_original.shape[0]} rows.")

        # --- 1. Filter for WRs ---
        df_clean = df_original[df_original['position'] == 'WR'].copy()
        print(f"Filtered down to {df_clean.shape[0]} rows for WRs.")
        if df_clean.empty: 
            print("No WR data found. Exiting.")
            return

        # --- 2. Drop Irrelevant & Leaky Features ---
        existing_cols_to_drop = [col for col in FEATURES_TO_DROP if col in df_clean.columns]
        df_featured = df_clean.drop(columns=existing_cols_to_drop, errors='ignore')
        
        print(f"Dropped {len(existing_cols_to_drop)} irrelevant/leaky columns.")
        print(f"  Dropped: {existing_cols_to_drop}")

        # Fill missing numerical values with 0
        for col in df_featured.columns:
            if df_featured[col].dtype in ['float64', 'int64']:
                df_featured.loc[:, col] = df_featured.loc[:, col].fillna(0)
        
        # --- 3. Save ---
        df_featured.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Successfully created WR-only *clean v2* dataset at '{OUTPUT_FILE}'")
        print(f"Final WR dataset has {df_featured.shape[0]} rows and {df_featured.shape[1]} columns.")
        print(f"Final Columns: {df_featured.columns.tolist()}")

    except FileNotFoundError:
        print(f"\n!!! ERROR: Input file not found at '{INPUT_FILE}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()