# rag_data/05_etl_to_postgres.py
import nflreadpy as nfl
import polars as pl
from sqlalchemy import create_engine
import sys
import os
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from datetime import datetime # --- NEW IMPORT ---
from pathlib import Path
from sqlalchemy import text
import subprocess


# --- Configuration ---
load_dotenv()

# --- NEW: Dynamic Season Logic ---
def get_current_season():
    """
    Determines the current NFL season based on the date.
    The NFL 'League Year' typically starts in March.
    - If it's Jan/Feb (Playoffs), we are still in the *previous* calendar year's season.
    - If it's March-Dec, we are in the *current* calendar year's season.
    """
    now = datetime.now()
    if now.month >= 3: # March or later
        return now.year
    else: # Jan or Feb
        return now.year - 1

SEASON = get_current_season()
print(f"Dynamic Season Detected: {SEASON}")
# ---------------------------------


DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    print("Error: DB_CONNECTION_STRING not found in environment variables.")
    sys.exit(1)

# Constants
FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']
OFFENSIVE_POSITIONS = ['QB', 'RB', 'WR', 'TE', 'FB']

def get_db_engine():
    return create_engine(DB_CONNECTION_STRING)

def ensure_polars(df):
    if isinstance(df, pl.DataFrame):
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        return pl.from_pandas(pd.DataFrame(df))

def push_to_postgres(df: pl.DataFrame, table_name: str, engine):
    print(f"Pushing {len(df)} rows to table '{table_name}'...")
    try:
        df.to_pandas().to_sql(table_name, engine, if_exists='replace', index=False)
        print(f" Table '{table_name}' updated.")
    except Exception as e:
        print(f"Error writing to {table_name}: {e}")
        import traceback; traceback.print_exc()

# --- 1. Player Profiles ---
def etl_profiles(engine):
    print("\n--- Processing Player Profiles ---")
    try:
        df_players = nfl.load_players()
        
        cols = [
            'gsis_id', 'display_name', 'position', 'latest_team', 
            'headshot', 'draft_year', 'draft_pick', 'status', 
            'height', 'weight', 'birth_date', 'pfr_id',
            'injury_status', 'years_of_experience'
        ]
        
        existing_cols = [c for c in cols if c in df_players.columns]
        df_prof = ensure_polars(df_players).select(existing_cols)

        df_prof = df_prof.rename({
            'gsis_id': 'player_id',
            'display_name': 'player_name',
            'latest_team': 'team_abbr',   
            'draft_pick': 'draft_number',  
            'years_of_experience': 'years_exp'
        })
        
        df_prof = df_prof.filter(pl.col('position').is_in(OFFENSIVE_POSITIONS))
        
        if "birth_date" in df_prof.columns:
            df_prof = df_prof.with_columns(
                ((pl.lit(SEASON) - pl.col("birth_date").str.slice(0, 4).cast(pl.Int32, strict=False)).alias("age"))
            )
            
        push_to_postgres(df_prof, 'player_profiles', engine)
        return df_prof 
    except Exception as e:
        print(f"Error in profiles ETL: {e}")
        return None

# --- 2. Schedule ---
def etl_schedule(engine):
    print("\n--- Processing Schedule ---")
    try:
        # Load schedule data
        df = ensure_polars(nfl.load_schedules(seasons=[SEASON]))
        
        # --- FIX: Keep betting lines (spread_line, total_line) ---
        # We explicitly select these to ensure they aren't dropped
        cols_to_keep = [
            'game_id', 'week', 'home_team', 'away_team', 
            'home_score', 'away_score', 'spread_line', 'total_line'
        ]
        # Filter for columns that actually exist in the dataframe
        valid_cols = [c for c in cols_to_keep if c in df.columns]
        df = df.select(valid_cols)

        df = df.with_columns([
            pl.col("week").cast(pl.Int64, strict=False),
            pl.col("home_score").cast(pl.Float64, strict=False),
            pl.col("away_score").cast(pl.Float64, strict=False)
        ])
        
        # Handle betting lines if they exist
        if "spread_line" in df.columns:
             df = df.with_columns(pl.col("spread_line").cast(pl.Float64, strict=False))
        if "total_line" in df.columns:
             df = df.with_columns(pl.col("total_line").cast(pl.Float64, strict=False))

        push_to_postgres(df, 'schedule', engine)
        return df
    except Exception as e:
        print(f"Error in schedule ETL: {e}")
        return None

# --- 3. Team Stats (Offense & Defense) ---
def etl_team_stats(engine, df_schedule):
    print("\n--- Processing Team Stats (Offense & Defense) ---")
    try:
        team_stats = ensure_polars(nfl.load_team_stats(seasons=SEASON, summary_level='week')).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        schedule = df_schedule.with_columns(pl.col("week").cast(pl.Int64))
        home = schedule.select(pl.col('home_team').alias('team'), 'week', pl.col('home_score').alias('points_for'), pl.col('away_score').alias('points_allowed'))
        away = schedule.select(pl.col('away_team').alias('team'), 'week', pl.col('away_score').alias('points_for'), pl.col('home_score').alias('points_allowed'))
        points_table = pl.concat([home, away])

        # --- Defense ---
        offense_join = team_stats.select('team', 'week', pl.col('passing_yards').alias('passing_yards_allowed'), pl.col('rushing_yards').alias('rushing_yards_allowed'))
        core_cols = ['team', 'week', 'opponent_team', 'def_sacks', 'def_interceptions', 'def_fumbles_forced', 'def_qb_hits']
        avail_core = [c for c in core_cols if c in team_stats.columns]
        core_defense = team_stats.select(avail_core)
        
        defense_df = core_defense.join(offense_join, left_on=['opponent_team', 'week'], right_on=['team', 'week'])
        if 'team_right' in defense_df.columns: defense_df = defense_df.drop('team_right')
        
        defense_df = defense_df.join(points_table.select(['team', 'week', 'points_allowed']), on=['team', 'week'], how='left')
        
        defense_final = defense_df.rename({'team': 'team_abbr'})
        push_to_postgres(defense_final, 'weekly_defense_stats', engine)

        # --- Offense ---
        off_cols = ['team', 'week', 'opponent_team', 'passing_yards', 'rushing_yards', 'passing_tds', 'rushing_tds', 'passing_interceptions', 'rushing_fumbles_lost', 'passing_first_downs', 'rushing_first_downs', 'attempts', 'receptions', 'carries']
        avail_off = [c for c in off_cols if c in team_stats.columns]
        core_offense = team_stats.select(avail_off)
        
        offense_df = core_offense.join(points_table.select(['team', 'week', 'points_for']), on=['team', 'week'], how='left')
        
        offense_final = offense_df.rename({'team': 'team_abbr'})
        push_to_postgres(offense_final, 'weekly_offense_stats', engine)
        
        return offense_final

    except Exception as e:
        print(f"Error in Team Stats ETL: {e}")
        import traceback; traceback.print_exc()
        return None

# --- 4. Player Stats (with Team Shares) ---
def etl_player_stats(engine, df_offense, df_profiles):
    print("\n--- Processing Weekly Player Stats ---")
    try:
        stats = ensure_polars(nfl.load_player_stats(seasons=SEASON, summary_level='week')).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        if df_offense is not None:
            team_shares = df_offense.select(
                'team_abbr', 'week', 
                pl.col('attempts').alias('team_pass_att'), 
                pl.col('receptions').alias('team_rec'), 
                pl.col('carries').alias('team_rush_att')
            )
        else:
            team_shares = None
        
        if df_profiles is not None:
            profiles_map = df_profiles.select(['player_id', 'team_abbr']).unique(subset=['player_id'])
        else:
            profiles_map = None

        stats = stats.filter(pl.col('position').is_in(FANTASY_POSITIONS))
        
        if 'team' not in stats.columns and profiles_map is not None:
             stats = stats.join(profiles_map.rename({'team_abbr': 'team'}), on='player_id', how='left')

        stats = stats.with_columns([
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0) + pl.col('passing_yards').fill_null(0)).alias('total_off_yards')
        ]).with_columns([
             (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch'),
             (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
             (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
             (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None)).alias('pass_pct')
        ])
        
        if team_shares is not None and 'team' in stats.columns:
            stats = stats.join(team_shares, left_on=['team', 'week'], right_on=['team_abbr', 'week'], how='left')
            
            stats = stats.with_columns([
                (pl.col('targets') / pl.col('team_pass_att')).fill_nan(0.0).alias('team_targets_share'),
                (pl.col('receptions') / pl.col('team_rec')).fill_nan(0.0).alias('team_receptions_share'),
                (pl.col('carries') / pl.col('team_rush_att')).fill_nan(0.0).alias('team_rush_attempts_share')
            ])
        
        stats = stats.rename({
            'fantasy_points_ppr': 'y_fantasy_points_ppr',
            'carries': 'rush_attempts',
            'passing_interceptions': 'interception'
        })
        
        push_to_postgres(stats, 'weekly_player_stats', engine)

    except Exception as e:
        print(f"Error in Player Stats ETL: {e}")
        import traceback; traceback.print_exc()

# --- 5. Snap Counts ---
def etl_snap_counts(engine, df_profiles):
    print("\n--- Processing Snap Counts ---")
    try:
        snaps = ensure_polars(nfl.load_snap_counts(seasons=[SEASON]))
        snaps = snaps.filter(pl.col('position').is_in(OFFENSIVE_POSITIONS))
        
        if df_profiles is not None:
            profiles_map = df_profiles.select(['player_id', 'pfr_id', 'player_name']).drop_nulls(subset=['pfr_id'])
            
            join_key = 'pfr_player_id' if 'pfr_player_id' in snaps.columns else 'pfr_id'
            snaps = snaps.join(profiles_map, left_on=join_key, right_on='pfr_id', how='inner')
        
        if 'game_type' in snaps.columns:
             snaps = snaps.filter(pl.col('game_type') == 'REG')
        
        final_snaps = snaps.select(['player_id', 'player_name', 'season', 'week', 'offense_snaps', 'offense_pct'])
        final_snaps = final_snaps.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        push_to_postgres(final_snaps, 'weekly_snap_counts', engine)
        
    except Exception as e:
        print(f"Error in Snap Counts ETL: {e}")


# --- 6. Historical Features (NEW) ---
def etl_historical_features(engine):
    print("\n--- Processing Historical Features Dataset ---")
    # This assumes your script is running from rag_data/ and the file is in dataPrep/
    input_path = Path("../dataPrep/featured_dataset.csv")
    
    if not input_path.exists():
        print(f"Warning: {input_path} not found. Skipping.")
        return

    try:
        # Read the CSV (can be large, so we stick to Pandas for robust CSV reading or Polars)
        # Polars is faster
        df = pl.read_csv(input_path, ignore_errors=True)
        print(f"Loaded {len(df)} rows from featured_dataset.csv")
        
        push_to_postgres(df, 'featured_dataset', engine)
    except Exception as e:
        print(f"Error uploading historical features: {e}")

# --- 7. Weekly Rankings (NEW) ---
def etl_weekly_rankings(engine):
    print("\n--- Processing Weekly Rankings (Local CSV) ---")
    # Look for the CSV generated by 06_generate_rankings.py
    input_path = Path("weekly_rankings.csv")
    
    if not input_path.exists():
        print(f"Warning: {input_path} not found. Run 06_generate_rankings.py first. Skipping.")
        return

    try:
        df = pl.read_csv(input_path)
        print(f"Loaded {len(df)} rows from weekly_rankings.csv")
        
        # Smart Append Logic (Delete old for these weeks, then Insert)
        weeks = df['week'].unique().to_list()
        if weeks:
            weeks_str = ",".join(map(str, weeks))
            delete_query = text(f"DELETE FROM weekly_rankings WHERE season = {SEASON} AND week IN ({weeks_str})")
            
            with engine.connect() as conn:
                conn.execute(delete_query)
                conn.commit()
                print(f"Cleaned old data for weeks {weeks} from DB.")
        
        # Append to DB
        df.to_pandas().to_sql('weekly_rankings', engine, if_exists='append', index=False)
        print("✅ Weekly Rankings table updated successfully.")
        
    except Exception as e:
        print(f"Error uploading weekly rankings: {e}")


# --- 7. Trigger Rankings Generation (NEW) ---
def trigger_rankings_generation():
    print("\n" + "="*50)
    print("--- Triggering Rankings Generation (Script 06) ---")
    print("="*50)
    try:
        # Determine path to 06_generate_rankings.py
        # Assuming this script is in 'rag_data' and 06 is in the same folder
        current_script_dir = Path(__file__).parent
        rankings_script = current_script_dir / "06_generate_rankings.py"
        
        if not rankings_script.exists():
             print(f"Error: Could not find {rankings_script}")
             return

        # Run the script as a subprocess
        subprocess.run([sys.executable, str(rankings_script)], check=True)
        print("\n✅ Rankings Generation Complete.")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: Rankings script failed with exit code {e.returncode}")
    except Exception as e:
        print(f"\n❌ Error triggering rankings script: {e}")

def main():
    engine = get_db_engine()
    df_profiles = etl_profiles(engine)
    df_schedule = etl_schedule(engine)
    df_offense = None
    if df_schedule is not None: df_offense = etl_team_stats(engine, df_schedule)
    if df_offense is not None and df_profiles is not None: etl_player_stats(engine, df_offense, df_profiles)
    if df_profiles is not None: etl_snap_counts(engine, df_profiles)
    etl_historical_features(engine)
    
    print("\n--- ETL Data Load Complete. Starting Rankings... ---")
    
    # --- Run the rankings script automatically ---
    trigger_rankings_generation()

if __name__ == "__main__":
    main()