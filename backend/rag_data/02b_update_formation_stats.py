import nflreadpy as nfl
import pandas as pd
from pathlib import Path
import sys

# --- Configuration ---
SEASON = 2025
RAG_DATA_DIR = Path("rag_data") if Path("rag_data").exists() else Path(".")
PLAYER_STATS_FILE = RAG_DATA_DIR / f"weekly_player_stats_{SEASON}.csv"

def main():
    print(f"--- Updating Formation Stats (Shotgun/No Huddle) for {SEASON} ---")
    
    # 1. Load Play-by-Play Data
    print(f"Loading PBP data for {SEASON}...")
    try:
        pbp = nfl.load_pbp(SEASON)
        
        # --- FIX: Convert Polars to Pandas if needed ---
        if not isinstance(pbp, pd.DataFrame):
            if hasattr(pbp, 'to_pandas'):
                print("Converting Polars DataFrame to Pandas...")
                pbp = pbp.to_pandas()
            else:
                print("Warning: Unknown DataFrame type. Script might fail.")
                
    except Exception as e:
        print(f"Error loading PBP data: {e}")
        return

    if pbp.empty:
        print("No PBP data found.")
        return

    # 2. Filter for Real Offensive Plays (Pass/Run)
    print(f"Filtering {len(pbp)} plays...")
    mask = pbp['play_type'].isin(['pass', 'run'])
    plays = pbp[mask].copy()

    # 3. Calculate Team-Level Percentages
    # Group by 'posteam' (Offense Team) and 'week'
    print("Calculating formation percentages...")
    # Ensure columns are numeric for calculation
    plays['shotgun'] = pd.to_numeric(plays['shotgun'], errors='coerce').fillna(0)
    plays['no_huddle'] = pd.to_numeric(plays['no_huddle'], errors='coerce').fillna(0)
    
    formations = plays.groupby(['posteam', 'week'])[['shotgun', 'no_huddle']].mean().reset_index()
    
    # Rename columns to match what our models expect
    formations.rename(columns={'posteam': 'team'}, inplace=True)
    
    # 4. Load Existing Player Stats
    if not PLAYER_STATS_FILE.exists():
        print(f"CRITICAL ERROR: {PLAYER_STATS_FILE} not found. Run 02_update_weekly_stats.py first.")
        return

    df_players = pd.read_csv(PLAYER_STATS_FILE)
    print(f"Loaded {len(df_players)} player rows.")

    # 5. Merge Formations
    # Remove old columns if they exist (to prevent duplicates/collisions)
    if 'shotgun' in df_players.columns:
        print("Dropping existing 'shotgun' column to update...")
        df_players.drop(columns=['shotgun'], inplace=True)
    if 'no_huddle' in df_players.columns:
        print("Dropping existing 'no_huddle' column to update...")
        df_players.drop(columns=['no_huddle'], inplace=True)

    # Merge: Player's Team + Week -> Team's Formation Stats
    df_merged = pd.merge(df_players, formations, on=['team', 'week'], how='left')

    # Fill Missing (e.g., if a team had 0 offensive plays recorded or bye week issues)
    df_merged['shotgun'] = df_merged['shotgun'].fillna(0)
    df_merged['no_huddle'] = df_merged['no_huddle'].fillna(0)

    # 6. Save
    df_merged.to_csv(PLAYER_STATS_FILE, index=False)
    print(f"✅ Successfully updated {PLAYER_STATS_FILE}")
    print(f"   Added columns: shotgun, no_huddle")
    print(f"   Sample Shotgun (Mean): {df_merged['shotgun'].mean():.4f}")

if __name__ == "__main__":
    main()