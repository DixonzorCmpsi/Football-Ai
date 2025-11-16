import nflreadpy as nfl
import polars as pl
import sys
from pathlib import Path # Import Path for checking file

SEASON = 2025
DEFENSE_FILE = 'weekly_defense_stats_2025.csv'
OFFENSE_FILE = 'weekly_offense_stats_2025.csv'
SCHEDULE_FILE = 'schedule_2025.csv'

def create_team_stats_files(season):
    """
    Loads weekly TEAM stats and the schedule.
    Creates TWO files:
    1. weekly_defense_stats_2025.csv: Defensive actions and allowed stats.
    2. weekly_offense_stats_2025.csv: Offensive production stats.
    *** UPDATED to include def_qb_hits, attempts, receptions, and carries ***
    """
    print(f"Loading all {season} weekly TEAM stats from nflreadpy...")
    try:
        # 1. Load the main team stats file
        team_stats = nfl.load_team_stats(seasons=season, summary_level='week')
        if team_stats.is_empty():
            print(f"No weekly team stats found for {season}.")
            return
            
        print("\n[DEBUG] Columns found in nfl.load_team_stats():")
        print(team_stats.columns)

        # 2. Load the schedule file
        print(f"\nLoading {SCHEDULE_FILE} for points scored/allowed data...")
        try:
            schedule = pl.read_csv(SCHEDULE_FILE)
            schedule = schedule.with_columns(pl.col("week").cast(pl.Int64, strict=False))
            team_stats = team_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))
            if not team_stats.is_empty(): # Check again after potential cast
                schedule = schedule.filter(pl.col('week') <= team_stats['week'].max())
            else:
                print("Warning: team_stats became empty after type casting, cannot filter schedule.")
                return # Cannot proceed without team stats
        except Exception as e:
            print(f"Error: Could not read or process {SCHEDULE_FILE}. {e}", file=sys.stderr)
            return

        # 3. Prepare Points Scored/Allowed Table
        home_scores = schedule.select(
            pl.col('home_team').alias('team'), 'week',
            pl.col('home_score').alias('points_for'),
            pl.col('away_score').alias('points_allowed')
        )
        away_scores = schedule.select(
            pl.col('away_team').alias('team'), 'week',
            pl.col('away_score').alias('points_for'),
            pl.col('home_score').alias('points_allowed')
        )
        points_table = pl.concat([home_scores, away_scores])
        points_table = points_table.with_columns(pl.col("week").cast(pl.Int64, strict=False))


        # --- 4. Create DEFENSE File ---
        print("\n--- Processing Defense File ---")
        offense_stats_for_join = team_stats.select(
            'team', 'week',
            pl.col('passing_yards').alias('passing_yards_allowed'),
            pl.col('rushing_yards').alias('rushing_yards_allowed')
        )
        offense_stats_for_join = offense_stats_for_join.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        # --- MODIFIED: Added 'def_qb_hits' ---
        core_defense_stats = team_stats.select(
            'team', 'week', 'opponent_team',
            'def_sacks', 'def_interceptions', 'def_fumbles_forced',
            'def_qb_hits' # ADDED
        )
        core_defense_stats = core_defense_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        # Join core defense with opponent's offense stats
        defense_df = core_defense_stats.join(
            offense_stats_for_join,
            left_on=['opponent_team', 'week'],
            right_on=['team', 'week'],
        ) # Removed .drop()

        # Join with points allowed data
        defense_df = defense_df.join(
            points_table.select(['team', 'week', 'points_allowed']),
            on=['team', 'week'],
            how='left'
        )

        # --- MODIFIED: Added 'def_qb_hits' ---
        final_defense_cols_expr = [
            pl.col('team').alias('team_abbr'), # Use the alias directly
            pl.col('week'),
            pl.col('opponent_team'),
            pl.col('points_allowed'),
            pl.col('passing_yards_allowed'),
            pl.col('rushing_yards_allowed'),
            pl.col('def_sacks'),
            pl.col('def_interceptions'),
            pl.col('def_fumbles_forced'),
            pl.col('def_qb_hits') # ADDED
        ]
        
        # Get the source column names needed for these expressions
        source_cols_needed = set()
        for expr in final_defense_cols_expr:
             # Handle both string columns and expressions
             if isinstance(expr, str):
                 source_cols_needed.add(expr)
             else:
                 source_cols_needed.update(expr.meta.root_names())

        # Filter the DataFrame to only contain necessary source columns before applying final select/alias
        existing_source_cols = [col for col in source_cols_needed if col in defense_df.columns]
        
        # Build the final list of expressions that can actually be run
        final_defense_cols_to_run = []
        for expr in final_defense_cols_expr:
            if isinstance(expr, str):
                if expr in existing_source_cols:
                    final_defense_cols_to_run.append(expr)
            else:
                if set(expr.meta.root_names()).issubset(existing_source_cols):
                    final_defense_cols_to_run.append(expr)

        if len(final_defense_cols_to_run) < len(final_defense_cols_expr):
            print(f"[DEBUG Defense Select] Warning: Some columns were missing and dropped.")

        if not final_defense_cols_to_run:
            print("Error: No valid columns left to select for defense file.", file=sys.stderr)
            return

        defense_final_df = defense_df.select(final_defense_cols_to_run).sort('team_abbr', 'week')
        defense_final_df.write_csv(DEFENSE_FILE)
        print(f"Successfully created/updated {DEFENSE_FILE} with {len(defense_final_df)} rows.")
        print(f"Defense File Columns: {defense_final_df.columns}") # Debug

        # --- 5. Create OFFENSE File ---
        print("\n--- Processing Offense File ---")
        # --- MODIFIED: Added 'attempts', 'receptions', 'carries' ---
        core_offense_stats = team_stats.select(
            'team', 'week', 'opponent_team',
            'passing_yards', 'rushing_yards',
            'passing_tds', 'rushing_tds',
            'passing_interceptions', 'rushing_fumbles_lost',
            'passing_first_downs', 'rushing_first_downs',
            'attempts',    # ADDED (for team pass attempts -> target share)
            'receptions',  # ADDED (for team reception share)
            'carries'      # ADDED (for team rush attempt share)
        )
        core_offense_stats = core_offense_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        # Join with points scored data
        offense_df = core_offense_stats.join(
            points_table.select(['team', 'week', 'points_for']),
            on=['team', 'week'],
            how='left'
        )

        # --- MODIFIED: Added new columns to final list ---
        final_offense_cols_expr = [
            pl.col('team').alias('team_abbr'), # Use the alias directly
            pl.col('week'),
            pl.col('opponent_team'),
            pl.col('points_for'),
            pl.col('passing_yards'),
            pl.col('rushing_yards'),
            pl.col('passing_tds'),
            pl.col('rushing_tds'),
            pl.col('passing_interceptions'),
            pl.col('rushing_fumbles_lost'),
            pl.col('passing_first_downs'),
            pl.col('rushing_first_downs'),
            pl.col('attempts'),   # ADDED
            pl.col('receptions'), # ADDED
            pl.col('carries')     # ADDED
        ]
        
        # Get the source column names needed
        source_cols_needed_off = set()
        for expr in final_offense_cols_expr:
            if isinstance(expr, str):
                source_cols_needed_off.add(expr)
            else:
                source_cols_needed_off.update(expr.meta.root_names())

        # Filter the DataFrame
        existing_source_cols_off = [col for col in source_cols_needed_off if col in offense_df.columns]
        
        final_offense_cols_to_run = []
        for expr in final_offense_cols_expr:
            if isinstance(expr, str):
                if expr in existing_source_cols_off:
                    final_offense_cols_to_run.append(expr)
            else:
                if set(expr.meta.root_names()).issubset(existing_source_cols_off):
                    final_offense_cols_to_run.append(expr)

        if len(final_offense_cols_to_run) < len(final_offense_cols_expr):
            print(f"[DEBUG Offense Select] Warning: Some columns were missing and dropped.")

        if not final_offense_cols_to_run:
            print("Error: No valid columns left to select for offense file.", file=sys.stderr)
            return

        offense_final_df = offense_df.select(final_offense_cols_to_run).sort('team_abbr', 'week')
        offense_final_df.write_csv(OFFENSE_FILE)
        print(f"Successfully created/updated {OFFENSE_FILE} with {len(offense_final_df)} rows.")
        print(f"Offense File Columns: {offense_final_df.columns}") # Debug

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        print("Failed to create team stats files.")

if __name__ == "__main__":
    # Check if schedule file exists first
    if not Path(SCHEDULE_FILE).exists():
        print(f"Error: {SCHEDULE_FILE} not found. Please run '01_create_static_files.py' first.")
    else:
        create_team_stats_files(SEASON)
    print("\nTeam offense and defense files script finished.")