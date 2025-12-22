import nflreadpy as nfl
import polars as pl
from pathlib import Path
import sys
from datetime import datetime

# --- Configuration ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()

PLAYER_STATS_FILE = Path(f"weekly_player_stats_{SEASON}.csv")
OFFENSE_STATS_FILE = Path(f"weekly_offense_stats_{SEASON}.csv")
PROFILES_FILE = Path(f"player_profiles_{SEASON}.csv")

FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']

STATS_COLUMNS_BASE = [
    'player_id', 'week', 'opponent_team', 'position', 'team',
    'completions', 'attempts', 'passing_yards', 'passing_tds', 
    'passing_interceptions', 'passing_air_yards',
    'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles_lost',
    'receptions', 'targets', 'receiving_yards', 'receiving_tds', 
    'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch',
    'fantasy_points_ppr'
]

def update_weekly_stats(season, player_file, offense_file, profiles_file):
    print(f"Loading all {season} weekly player stats...")
    try:
        player_stats_raw = nfl.load_player_stats(seasons=season, summary_level='week')
        
        player_stats_raw = player_stats_raw.with_columns(
            pl.col("week").cast(pl.Int64, strict=False)
        )
        
        if player_stats_raw.is_empty():
            print(f"No player stats found for {season}.")
            return
            
        # 1. Load Team Offense
        print(f"Loading team offense data from {offense_file}...")
        df_offense = pl.read_csv(offense_file)
        df_team_shares = df_offense.select(
            pl.col('team_abbr'), 
            pl.col('week').cast(pl.Int64, strict=False),
            pl.col('attempts').alias('team_pass_attempts'),
            pl.col('receptions').alias('team_receptions'),
            pl.col('carries').alias('team_rush_attempts')
        )

        # 2. Load Profiles
        print(f"Loading profiles from {profiles_file}...")
        df_profiles = pl.read_csv(profiles_file)
        df_profiles = df_profiles.select(['player_id', 'team_abbr']).unique(subset=['player_id'])

        # 3. Filter Stats
        player_stats = player_stats_raw.filter(pl.col('position').is_in(FANTASY_POSITIONS))
        
        if 'team' not in player_stats.columns:
            player_stats = player_stats.join(df_profiles.rename({'team_abbr':'team'}), on='player_id', how='left')

        available_cols = [col for col in STATS_COLUMNS_BASE if col in player_stats.columns]
        player_stats_filtered = player_stats.select(available_cols)
        
        # 4. Derived Stats
        player_stats_derived = player_stats_filtered.with_columns(
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0) + pl.col('passing_yards').fill_null(0)).alias('total_off_yards'),
            (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
            (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
            (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None)).alias('pass_pct')
        ).with_columns(
             (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch')
        ).fill_nan(None).fill_null(0.0)

        # 5. Join Team Shares
        player_stats_derived = player_stats_derived.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        player_stats_joined = player_stats_derived.join(
            df_team_shares,
            left_on=['team', 'week'],
            right_on=['team_abbr', 'week'],
            how='left'
        )

        player_stats_with_shares = player_stats_joined.with_columns(
            (pl.col('targets') / pl.col('team_pass_attempts')).alias('team_targets_share'),
            (pl.col('receptions') / pl.col('team_receptions')).alias('team_receptions_share'),
            (pl.col('carries') / pl.col('team_rush_attempts')).alias('team_rush_attempts_share') 
        ).fill_nan(0.0).fill_null(0.0)

        final_df = player_stats_with_shares.rename({
            'fantasy_points_ppr': 'y_fantasy_points_ppr',
            'carries': 'rush_attempts',
            'passing_interceptions': 'interception'
        })

        # --- CRITICAL FIX: Add Season Column ---
        final_df = final_df.with_columns(pl.lit(season).alias("season"))

        final_df.write_csv(player_file)
        print(f"\n✅ Successfully updated {player_file} with {len(final_df)} rows.")
        print(f"Final Columns: {final_df.columns}")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not OFFENSE_STATS_FILE.exists():
        print(f"Error: {OFFENSE_STATS_FILE} not found.")
    elif not PROFILES_FILE.exists():
        print(f"Error: {PROFILES_FILE} not found.")
    else:
        update_weekly_stats(SEASON, PLAYER_STATS_FILE, OFFENSE_STATS_FILE, PROFILES_FILE)