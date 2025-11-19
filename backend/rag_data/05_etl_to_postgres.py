# rag_data/05_etl_to_postgres.py
import nflreadpy as nfl
import polars as pl
from sqlalchemy import create_engine
import sys
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import pandas as pd

# --- Configuration ---
load_dotenv()
SEASON = 2025
raw_password = os.getenv('POSTGRE_PASSWORD')

if not raw_password:
    print("Error: POSTGRE_PASSWORD not set in environment variables.")
    sys.exit(1)


DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
# Constants
FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']
OFFENSIVE_POSITIONS = ['QB', 'RB', 'WR', 'TE', 'FB']

def get_db_engine():
    return create_engine(DB_CONNECTION_STRING)

def ensure_polars(df):
    """Safely converts input to Polars DataFrame if it isn't one already."""
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
        print(f"✅ Table '{table_name}' updated.")
    except Exception as e:
        print(f"❌ Error writing to {table_name}: {e}")
        import traceback; traceback.print_exc()

# --- 1. Player Profiles ---
def etl_profiles(engine):
    print("\n--- Processing Player Profiles ---")
    try:
        df_players = nfl.load_players()
        
        # --- FIX: ADD 'injury_status' and 'years_of_experience' ---
        cols = [
            'gsis_id', 'display_name', 'position', 'latest_team', 
            'headshot', 'draft_year', 'draft_pick', 'status', 
            'height', 'weight', 'birth_date', 'pfr_id',
            'injury_status', 'years_of_experience' # ADDED
        ]
        
        # Filter columns that exist
        existing_cols = [c for c in cols if c in df_players.columns]
        df_prof = ensure_polars(df_players).select(existing_cols)

        # Rename for consistency
        df_prof = df_prof.rename({
            'gsis_id': 'player_id',
            'display_name': 'player_name',
            'latest_team': 'team_abbr',   
            'draft_pick': 'draft_number',  
            'years_of_experience': 'years_exp' # Match feature generator's variable name
        })
        
        # Filter for relevant positions
        df_prof = df_prof.filter(pl.col('position').is_in(OFFENSIVE_POSITIONS))
        
        # Calculate Age
        if "birth_date" in df_prof.columns:
            df_prof = df_prof.with_columns(
                ((pl.lit(SEASON) - pl.col("birth_date").str.slice(0, 4).cast(pl.Int32, strict=False)).alias("age"))
            )
            
        push_to_postgres(df_prof, 'player_profiles', engine)
        return df_prof 
    except Exception as e:
        print(f"Error in profiles ETL: {e}")
        import traceback; traceback.print_exc()
        return None

# --- 2. Schedule ---
def etl_schedule(engine):
    print("\n--- Processing Schedule ---")
    try:
        df = ensure_polars(nfl.load_schedules(seasons=[SEASON]))
        df = df.with_columns([pl.col("week").cast(pl.Int64, strict=False), pl.col("home_score").cast(pl.Float64, strict=False), pl.col("away_score").cast(pl.Float64, strict=False)])
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
        
        # Prepare Team Shares Source
        team_shares = df_offense.select('team_abbr', 'week', pl.col('attempts').alias('team_pass_att'), pl.col('receptions').alias('team_rec'), pl.col('carries').alias('team_rush_att'))
        
        # Prepare Profiles for Team mapping
        profiles_map = df_profiles.select(['player_id', 'team_abbr']).unique(subset=['player_id'])

        # Filter
        stats = stats.filter(pl.col('position').is_in(FANTASY_POSITIONS))
        
        # Add Team
        if 'team' not in stats.columns:
             stats = stats.join(profiles_map.rename({'team_abbr': 'team'}), on='player_id', how='left')

        # Calculate Derived Stats
        stats = stats.with_columns([
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0) + pl.col('passing_yards').fill_null(0)).alias('total_off_yards')
        ]).with_columns([
             (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch'),
             (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
             (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
             (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None)).alias('pass_pct')
        ])
        
        # Join for Shares
        stats = stats.join(team_shares, left_on=['team', 'week'], right_on=['team_abbr', 'week'], how='left')
        
        # Calculate Shares
        stats = stats.with_columns([
            (pl.col('targets') / pl.col('team_pass_att')).fill_nan(0.0).alias('team_targets_share'),
            (pl.col('receptions') / pl.col('team_rec')).fill_nan(0.0).alias('team_receptions_share'),
            (pl.col('carries') / pl.col('team_rush_att')).fill_nan(0.0).alias('team_rush_attempts_share')
        ])
        
        # Rename for DB
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
        
        players = df_profiles.select(['player_id', 'pfr_id', 'player_name']).drop_nulls(subset=['pfr_id'])
        
        if 'pfr_player_id' in snaps.columns:
            join_key = 'pfr_player_id'
        else:
            join_key = 'pfr_id'

        snaps = snaps.join(players, left_on=join_key, right_on='pfr_id', how='inner')
        
        if 'game_type' in snaps.columns:
             snaps = snaps.filter(pl.col('game_type') == 'REG')
        
        final_snaps = snaps.select(['player_id', 'player_name', 'season', 'week', 'offense_snaps', 'offense_pct'])
        final_snaps = final_snaps.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        push_to_postgres(final_snaps, 'weekly_snap_counts', engine)
        
    except Exception as e:
        print(f"Error in Snap Counts ETL: {e}")

def main():
    engine = get_db_engine()
    
    # Execute in order, passing DataFrames to avoid reloading
    df_profiles = etl_profiles(engine)
    df_schedule = etl_schedule(engine)
    
    df_offense = None
    if df_schedule is not None:
        df_offense = etl_team_stats(engine, df_schedule)
        
    if df_offense is not None and df_profiles is not None:
        etl_player_stats(engine, df_offense, df_profiles)
        
    if df_profiles is not None:
        etl_snap_counts(engine, df_profiles)
            
    print("\n--- ETL Complete ---")

if __name__ == "__main__":
    main()