# dataPrep/create_team_ecosystem_dataset.py
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
# Use the full, original dataset as our source
INPUT_FILE = Path("featured_dataset.csv") 
OUTPUT_FILE = Path("team_ecosystem_dataset.csv")

# Define the key stats we want to aggregate for each position
# We'll sum these up for all players of a given position in a game
STATS_TO_AGGREGATE = [
    'pass_attempts', 'passing_yards', 'passing_air_yards', 'passer_rating', 'interception',
    'pass_attempts_redzone', 'pass_pct',
    'touches', 'rush_attempts', 'rushing_yards', 'ypc', 'rush_attempts_redzone', 'rush_touchdown',
    'targets', 'receptions', 'receiving_yards', 'yards_after_catch', 'receiving_air_yards',
    'adot', 'ayptarget', 'yptouch', 'ypr', 'targets_redzone', 'receptions_redzone',
    'receiving_touchdown',
    'y_fantasy_points_ppr' # The total fantasy points for the position
]

def main():
    print(f"--- Creating Team Ecosystem Dataset from: {INPUT_FILE} ---")

    try:
        df_original = pd.read_csv(INPUT_FILE, low_memory=False)
        print(f"Loaded dataset with {df_original.shape[0]} rows.")

        # --- 1. Filter for main fantasy positions ---
        fantasy_positions = ['QB', 'RB', 'WR', 'TE']
        df = df_original[df_original['position'].isin(fantasy_positions)].copy()
        print(f"Filtered down to {df.shape[0]} rows for QB, RB, WR, TE.")
        
        # --- 2. Select only necessary columns ---
        # Find which of our stats to aggregate actually exist
        existing_stats = [col for col in STATS_TO_AGGREGATE if col in df.columns]
        
        # Define the "group keys"
        group_keys = ['season', 'week', 'team', 'opponent', 'position']
        
        # Keep only the keys and the stats we want to sum
        df_clean = df[group_keys + existing_stats]

        # --- 3. Group and Aggregate ---
        print("Aggregating player stats up to the team-position level...")
        # Group by game and position, then sum all stats
        df_agg = df_clean.groupby(group_keys).sum(numeric_only=True).reset_index()
        
        # --- 4. Pivot the Data ---
        print("Pivoting data to create team-centric rows...")
        # This is the key step:
        # Index = one game (season, week, team, opponent)
        # Columns = the position
        # Values = the aggregated stats
        df_pivot = df_agg.pivot(
            index=['season', 'week', 'team', 'opponent'],
            columns='position',
            values=existing_stats
        )
        
        # --- 5. Flatten Column Headers ---
        # The pivot creates multi-level columns (e.g., ('targets', 'WR')).
        # We'll flatten them to 'targets_WR'
        df_pivot.columns = [f'{stat}_{pos}' for stat, pos in df_pivot.columns]
        df_pivot.reset_index(inplace=True)
        
        # Fill all NaNs (e.g., a week where no TE got a target) with 0
        df_pivot.fillna(0, inplace=True)

        print(f"Created new dataset with {df_pivot.shape[0]} team-game rows.")
        
        # --- 6. Save ---
        df_pivot.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Successfully created team ecosystem dataset at '{OUTPUT_FILE}'")
        print(f"Final dataset has {df_pivot.shape[0]} rows and {df_pivot.shape[1]} columns.")
        print("\nExample Columns:")
        print(df_pivot.columns.tolist()[:10]) # Show first 10 columns

    except FileNotFoundError:
        print(f"\n!!! ERROR: Input file not found at '{INPUT_FILE}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()