import nflreadpy as nfl
import polars as pl
import sys

# Define the current season
SEASON = 2025
PLAYER_STATS_FILE = 'weekly_player_stats_2025.csv'

# Define relevant fantasy positions
FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']

def update_weekly_stats(season):
    """
    Downloads weekly stats, filters for fantasy positions,
    adds columns based on EDA, calculates derived stats, and saves.
    Uses Polars syntax.
    """
    print(f"Loading all {season} weekly player stats from nflreadpy...")
    print("This may take a moment...")

    try:
        weekly_stats_raw = nfl.load_player_stats(seasons=season, summary_level='week')

        if weekly_stats_raw.is_empty():
            print(f"No weekly player stats found for {season}.")
            return

        # --- DEBUG ---
        print("\n[DEBUG Player Stats] All available raw columns:")
        print(weekly_stats_raw.columns)
        print("---------------------------------")
        # --- END DEBUG ---

        # === Filter for Fantasy Relevant Positions ===
        print(f"Filtering for positions: {FANTASY_POSITIONS}...")
        player_stats_filtered = weekly_stats_raw.filter(pl.col('position').is_in(FANTASY_POSITIONS))
        print(f"Filtered down to {len(player_stats_filtered)} rows.")

        if player_stats_filtered.is_empty():
             print("No rows found for specified fantasy positions.")
             return

        # === Process Offensive Player Stats (using filtered data) ===
        player_stats = player_stats_filtered # Use the filtered data from now on

        # Define columns to keep directly (using nflreadpy names)
        direct_columns_source = [
            'player_id', 'week', 'opponent_team', 'position', # Basic IDs
            'completions', 'attempts', 'passing_yards', 'passing_tds', 'passing_interceptions',
            'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles_lost',
            'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles_lost',
            'fantasy_points_ppr', # Target variable
            'offense_snaps', 'offense_pct', # Snap counts (If available)
            'target_share', # Share stats (If available)
            'receiving_yards_after_catch', # Advanced Receiving (If available)
            'passing_air_yards', 'receiving_air_yards', # Air Yards (If available)
            'avg_depth_of_target', # ADOT (If available)
            'passer_rating', # QB specific (If available)
            'rz_carry', 'rz_target', 'rz_attempt', # Redzone attempts (If available)
            'shotgun', 'no_huddle', # Play type flags (If available)
            'special_teams_tds'
        ]

        # Select only the available columns
        available_direct_columns = [col for col in direct_columns_source if col in player_stats.columns]
        missing_direct_cols = set(direct_columns_source) - set(available_direct_columns)
        if missing_direct_cols:
             # This is expected based on previous output, less of a warning now
             print(f"[INFO Player Stats] Columns not found in source and skipped: {missing_direct_cols}")

        player_stats_selected = player_stats.select(available_direct_columns)

        # --- Calculate Derived Columns ---
        print("[DEBUG Player Stats] Calculating derived columns...")
        # (Calculation logic remains the same)
        player_stats_calculated = player_stats_selected.with_columns([
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
            (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
            (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None) * 100).alias('pass_pct'),
            (pl.col('passing_yards').fill_null(0) + pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0)).alias('total_off_yards')
        ])
        player_stats_calculated = player_stats_calculated.with_columns(
            (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch')
        )


        # --- Final Renaming to Match EDA Output ---
        print("[DEBUG Player Stats] Renaming columns...")
        # (Rename logic remains the same)
        rename_map_final = {
            'carries': 'rush_attempts', 'attempts': 'pass_attempts', 'target_share': 'team_targets_share',
            'receiving_yards_after_catch': 'yards_after_catch', 'avg_depth_of_target': 'adot',
            'passing_interceptions': 'interception', 'rz_carry': 'rush_attempts_redzone',
            'rz_target': 'targets_redzone', 'rz_attempt': 'pass_attempts_redzone',
            'receiving_tds': 'receiving_touchdown', 'rushing_tds': 'rush_touchdown',
            'fantasy_points_ppr': 'y_fantasy_points_ppr'
        }
        current_cols = player_stats_calculated.columns
        final_rename = {k: v for k, v in rename_map_final.items() if k in current_cols}
        player_stats_final = player_stats_calculated.rename(final_rename)


        # --- Save the final player stats file ---
        player_stats_final.write_csv(PLAYER_STATS_FILE)
        print(f"\nSuccessfully updated {PLAYER_STATS_FILE} with {len(player_stats_final)} rows (filtered).")
        print(f"Final columns: {player_stats_final.columns}")

    except Exception as e:
        print(f"\nAn error occurred: {e}", file=sys.stderr)
        print("Failed to update weekly player stats.")


if __name__ == "__main__":
    update_weekly_stats(SEASON)
    print("\nWeekly player stats file is now up-to-date.")