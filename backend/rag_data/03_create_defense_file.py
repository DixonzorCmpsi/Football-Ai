import nflreadpy as nfl
import polars as pl
import sys
from pathlib import Path 
import os
from datetime import datetime
import traceback
from dotenv import load_dotenv # Import dotenv
load_dotenv()

# --- Configuration & Path Setup ---
# Find project root (assuming this script is in backend/rag_data/)
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
load_dotenv(project_root / 'applications' / '.env')
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not DB_CONNECTION_STRING:
    sys.exit("Error: DB_CONNECTION_STRING not found in .env")

# --- Dynamic Season Logic ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()
DEFENSE_FILE = f'weekly_defense_stats_{SEASON}.csv'
OFFENSE_FILE = f'weekly_offense_stats_{SEASON}.csv'
SCHEDULE_FILE = f'schedule_{SEASON}.csv' 

# --- NEW/RESTORED: Function to ensure schedule file contains spread/total ---
def refresh_schedule_with_spread(season):
    """
    Loads schedule data including spread/total from nflreadpy 
    and saves it to the main SCHEDULE_FILE.
    """
    print(f"\n--- Refreshing {SCHEDULE_FILE} to include betting lines ---")
    try:
        schedules = nfl.load_schedules(seasons=[season])
        
        spread_cols = [
            pl.col('game_id'), pl.col('week'), pl.col('season'), 
            pl.col('home_team'), pl.col('away_team'), 
            pl.col('home_score'), pl.col('away_score'), 
            pl.col('spread_line').alias('spread'),
            pl.col('total_line').alias('over_under') 
        ]
        
        clean_schedule = schedules.select(spread_cols)
        clean_schedule.write_csv(SCHEDULE_FILE)
        
        # --- UPLOAD SCHEDULE DATA TO DB ---
        print(f"📤 Uploading {SCHEDULE_FILE} to 'schedule' table...")
        clean_schedule.write_database(
            table_name="schedule",
            connection=DB_CONNECTION_STRING,
            if_table_exists="replace",
            engine="sqlalchemy"
        )
        print("✅ Schedule table updated successfully.")
        
    except Exception as e:
        print(f"❌ Error refreshing and uploading schedule data: {e}", file=sys.stderr)
        return False
    return True


def create_team_stats_files(season):
    """
    Loads weekly TEAM stats, calculates derived features, saves CSVs, and uploads to DB.
    """
    
    # *** 1. ENSURE SCHEDULE IS REFRESHED AND UPLOADED ***
    if not refresh_schedule_with_spread(season):
        print("Aborting team stat processing.")
        return

    # --- Start main stat processing ---
    print(f"\nLoading all {season} weekly TEAM stats from nflreadpy...")
    try:
        team_stats = nfl.load_team_stats(seasons=season, summary_level='week')
        if team_stats.is_empty():
            print(f"No weekly team stats found for {season}.")
            return
            
        print("\n[DEBUG] Columns found in nfl.load_team_stats():")
        print(team_stats.columns)

        # 2. Load the refreshed schedule file (NOW GUARANTEED TO HAVE over_under)
        print(f"\nLoading {SCHEDULE_FILE} for points scored/allowed data...")
        try:
            # We load the CSV we just wrote to ensure consistency
            schedule = pl.read_csv(SCHEDULE_FILE)
            schedule = schedule.with_columns(pl.col("week").cast(pl.Int64, strict=False))
            team_stats = team_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))
            
            if not team_stats.is_empty():
                schedule = schedule.filter(pl.col('week') <= team_stats['week'].max())
            else:
                print("Warning: team_stats became empty after type casting, cannot filter schedule.")
                return 
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


        # --- 4. Create and Upload DEFENSE File ---
        print("\n--- Processing Defense File ---")
        offense_stats_for_join = team_stats.select(
            'team', 'week',
            pl.col('passing_yards').alias('passing_yards_allowed'),
            pl.col('rushing_yards').alias('rushing_yards_allowed')
        )
        offense_stats_for_join = offense_stats_for_join.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        core_defense_stats = team_stats.select(
            'team', 'week', 'opponent_team',
            'def_sacks', 'def_interceptions', 'def_fumbles_forced',
            'def_qb_hits'
        )
        core_defense_stats = core_defense_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        defense_df = core_defense_stats.join(
            offense_stats_for_join,
            left_on=['opponent_team', 'week'],
            right_on=['team', 'week'],
        )

        defense_df = defense_df.join(
            points_table.select(['team', 'week', 'points_allowed']),
            on=['team', 'week'],
            how='left'
        )

        # Final select logic (simplified)
        defense_final_df = defense_df.select([
            pl.col('team').alias('team_abbr'),
            pl.col('week'),
            pl.col('opponent_team'),
            pl.col('points_allowed'),
            pl.col('passing_yards_allowed'),
            pl.col('rushing_yards_allowed'),
            pl.col('def_sacks'),
            pl.col('def_interceptions'),
            pl.col('def_fumbles_forced'),
            pl.col('def_qb_hits')
        ]).drop_nulls(subset=['team_abbr', 'week']).sort('team_abbr', 'week')

        defense_final_df.write_csv(DEFENSE_FILE)
        
        # --- UPLOAD DEFENSE DATA TO DB ---
        print(f"📤 Uploading {DEFENSE_FILE} to 'weekly_defense_stats'...")
        defense_final_df.write_database(
            table_name="weekly_defense_stats",
            connection=DB_CONNECTION_STRING,
            if_table_exists="replace",
            engine="sqlalchemy"
        )
        print(f"✅ Successfully updated 'weekly_defense_stats' with {len(defense_final_df)} rows.")

        # --- 5. Create and Upload OFFENSE File ---
        print("\n--- Processing Offense File ---")
        core_offense_stats = team_stats.select(
            'team', 'week', 'opponent_team',
            'passing_yards', 'rushing_yards',
            'passing_tds', 'rushing_tds',
            'passing_interceptions', 'rushing_fumbles_lost',
            'passing_first_downs', 'rushing_first_downs',
            'attempts',
            'receptions',
            'carries'
        )
        core_offense_stats = core_offense_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        offense_df = core_offense_stats.join(
            points_table.select(['team', 'week', 'points_for']),
            on=['team', 'week'],
            how='left'
        )

        # Final select logic (simplified)
        offense_final_df = offense_df.select([
            pl.col('team').alias('team_abbr'),
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
            pl.col('attempts'),
            pl.col('receptions'),
            pl.col('carries')
        ]).drop_nulls(subset=['team_abbr', 'week']).sort('team_abbr', 'week')

        offense_final_df.write_csv(OFFENSE_FILE)
        
        # --- UPLOAD OFFENSE DATA TO DB ---
        print(f"📤 Uploading {OFFENSE_FILE} to 'weekly_offense_stats'...")
        offense_final_df.write_database(
            table_name="weekly_offense_stats",
            connection=DB_CONNECTION_STRING,
            if_table_exists="replace",
            engine="sqlalchemy"
        )
        print(f"✅ Successfully updated 'weekly_offense_stats' with {len(offense_final_df)} rows.")
        
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        traceback.print_exc()
        print("Failed to create and upload team stats files.")

if __name__ == "__main__":
    create_team_stats_files(SEASON)
    print("\nTeam offense and defense files script finished.")