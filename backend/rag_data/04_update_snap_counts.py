import nflreadpy as nfl
import polars as pl
from pathlib import Path
import sys

# --- Configuration ---
SEASON = 2025
OUTPUT_FILE = Path("weekly_snap_counts_2025.csv")

# --- Define offensive positions ---
OFFENSIVE_POSITIONS = ['QB', 'RB', 'WR', 'TE', 'FB']

def update_snap_counts(season, output_file):
    """
    Loads all season-to-date snap count data from nflreadpy,
    filters for OFFENSIVE players only,
    joins with the player master list to get 'gsis_id' (player_id) and 'player_name',
    and saves the result to a CSV file.
    """
    print(f"Loading all {season} weekly snap counts from nflreadpy...")
    try:
        # 1. Load snap counts
        snaps = nfl.load_snap_counts(seasons=[season])
        if snaps.is_empty():
            print(f"No snap counts found for {season}.")
            return
            
        # 2. Load Player Master List (the "Rosetta Stone")
        print("Loading player master list for ID and name matching...")
        players_map = nfl.load_players()
        
        # --- FIX: Change 'full_name' to 'display_name' ---
        cols_to_map = ['pfr_id', 'gsis_id', 'display_name']
        
        # Check if new/old nflreadpy version column names are used
        if 'pfr_player_id' in snaps.columns:
            join_key_snaps = 'pfr_player_id'
        elif 'pfr_id' in snaps.columns:
            join_key_snaps = 'pfr_id'
        else:
            print("Error: Snap count data has no 'pfr_player_id' or 'pfr_id'. Cannot join.")
            return

        if 'pfr_id' not in players_map.columns:
            print("Error: Player map has no 'pfr_id'. Cannot join.")
            return
            
        # --- FIX: Rename 'display_name' to 'player_name' ---
        players_map = players_map.select(cols_to_map).rename({
            'gsis_id': 'player_id',
            'display_name': 'player_name', # <-- Corrected column
            'pfr_id': join_key_snaps 
        })
        
        # --- 3. Filter for offensive positions ---
        snaps_offensive = snaps.filter(
            pl.col('position').is_in(OFFENSIVE_POSITIONS)
        )
        print(f"Filtered snap counts down to {len(snaps_offensive)} rows for offensive players.")
        
        if snaps_offensive.is_empty():
            print("No snap counts found for specified offensive positions.")
            return

        # --- 4. Join Snaps with Player Map ---
        print(f"Joining snap data with player map on '{join_key_snaps}'...")
        snaps_with_ids = snaps_offensive.join(
            players_map,
            on=join_key_snaps,
            how='left'
        )
        
        # --- 5. Filter for REG season only ---
        if 'game_type' in snaps_with_ids.columns:
            snaps_clean = snaps_with_ids.filter(pl.col('game_type') == 'REG')
        else:
            print("Warning: 'game_type' column not found, could not filter for regular season.")
            snaps_clean = snaps_with_ids
        
        # --- 6. Select final columns to save ---
        final_cols_to_save = [
            'player_id',     # <-- gsis_id
            'player_name',   # <-- display_name
            'season', 
            'week', 
            'offense_snaps', 
            'offense_pct'
        ]
        
        available_cols = [col for col in final_cols_to_save if col in snaps_clean.columns]
        if 'player_id' not in available_cols or 'player_name' not in available_cols:
             print("Error: Join failed to add 'player_id' or 'player_name'.")
             print(f"Columns available after join: {snaps_clean.columns}")
             return
             
        snaps_final = snaps_clean.select(available_cols)
        
        snaps_final = snaps_final.with_columns(
            pl.col("week").cast(pl.Int64, strict=False)
        )
        
        # Drop rows where gsis_id (player_id) is null
        snaps_final = snaps_final.drop_nulls(subset=['player_id'])
        
        snaps_final.write_csv(output_file)
        
        print(f"✅ Successfully updated {output_file} with {len(snaps_final)} rows.")
        print(f"Final Columns: {snaps_final.columns}")

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Failed to update snap counts.")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    update_snap_counts(SEASON, OUTPUT_FILE)