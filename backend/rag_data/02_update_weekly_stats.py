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

# File Paths
PLAYER_STATS_FILE = Path(f"weekly_player_stats_{SEASON}.csv")
OFFENSE_STATS_FILE = Path(f"weekly_offense_stats_{SEASON}.csv")
PROFILES_FILE = Path(f"player_profiles_{SEASON}.csv")

FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']

# Columns to keep from raw data
STATS_COLUMNS_BASE = [
    'player_id', 'week', 'opponent_team', 'position', 'team',
    'completions', 'attempts', 'passing_yards', 'passing_tds', 
    'passing_interceptions', 'passing_air_yards', 'sack',
    'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles_lost',
    'receptions', 'targets', 'receiving_yards', 'receiving_tds', 
    'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch',
    'fantasy_points_ppr'
]

def update_weekly_stats(season, player_file, offense_file, profiles_file):
    print(f"--- Loading Raw Player Stats for {season} ---")
    try:
        # 1. Load Raw Stats from NFLReadPy
        # summary_level='week' gives us one row per player per game
        player_stats_raw = nfl.load_player_stats(seasons=season, summary_level='week')
        
        # Ensure week is integer
        player_stats_raw = player_stats_raw.with_columns(
            pl.col("week").cast(pl.Int64, strict=False)
        )
        
        if player_stats_raw.is_empty():
            print(f"No player stats found for {season}.")
            return
            
        # 2. Load Ancillary Data (Offense Shares & Profiles)
        print(f"Loading team offense & profiles...")
        
        if not offense_file.exists():
            print(f"❌ Warning: {offense_file} missing. Team shares will be 0.")
            df_team_shares = None
        else:
            df_offense = pl.read_csv(offense_file)
            df_team_shares = df_offense.select(
                pl.col('team_abbr'), 
                pl.col('week').cast(pl.Int64, strict=False),
                pl.col('attempts').alias('team_pass_attempts'),
                pl.col('receptions').alias('team_receptions'),
                pl.col('carries').alias('team_rush_attempts')
            )

        if not profiles_file.exists():
            print(f"❌ Warning: {profiles_file} missing. Teams might be inaccurate.")
            df_profiles = None
        else:
            df_profiles = pl.read_csv(profiles_file)
            df_profiles = df_profiles.select(['player_id', 'team_abbr']).unique(subset=['player_id'])

        # 3. Filter & Clean
        player_stats = player_stats_raw.filter(pl.col('position').is_in(FANTASY_POSITIONS))
        
        # Merge Team if missing (often raw stats have 'team', but we double check)
        if 'team' not in player_stats.columns and df_profiles is not None:
            player_stats = player_stats.join(df_profiles.rename({'team_abbr':'team'}), on='player_id', how='left')

        # Select only the columns we care about
        available_cols = [col for col in STATS_COLUMNS_BASE if col in player_stats.columns]
        player_stats = player_stats.select(available_cols)
        
        # 4. Calculate Derived Stats (The "Missing" Pieces)
        print("Calculating derived stats (ADOT, Passer Rating, Touches)...")
        
        # A. Basic Efficiency
        player_stats = player_stats.with_columns(
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0) + pl.col('passing_yards').fill_null(0)).alias('total_off_yards'),
            (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
            (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
            (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None)).alias('pass_pct')
        ).with_columns(
             (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch')
        )

        # B. ADOT (Average Depth of Target) - Critical for WR/TE models
        player_stats = player_stats.with_columns(
            (pl.col('receiving_air_yards') / pl.when(pl.col('targets') != 0).then(pl.col('targets')).otherwise(None)).fill_null(0.0).alias('adot')
        )

        # C. Passer Rating - Critical for QB models
        # Standard NFL Formula components
        attempts = pl.col('attempts')
        comp_pct = pl.col('completions') / attempts
        ypa = pl.col('passing_yards') / attempts
        td_pct = pl.col('passing_tds') / attempts
        int_pct = pl.col('passing_interceptions') / attempts

        # Components clipped between 0 and 2.375
        pr_a = ((comp_pct - 0.3) * 5).clip(0, 2.375)
        pr_b = ((ypa - 3) * 0.25).clip(0, 2.375)
        pr_c = (td_pct * 20).clip(0, 2.375)
        pr_d = (2.375 - (int_pct * 25)).clip(0, 2.375)

        player_stats = player_stats.with_columns(
            pl.when(attempts > 0)
            .then(((pr_a + pr_b + pr_c + pr_d) / 6) * 100)
            .otherwise(0.0)
            .alias('passer_rating')
        )

        # Clean NaNs created by division by zero
        player_stats = player_stats.fill_nan(None).fill_null(0.0)

        # 5. Join Team Shares (if available)
        if df_team_shares is not None:
            player_stats = player_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))
            player_stats = player_stats.join(
                df_team_shares,
                left_on=['team', 'week'],
                right_on=['team_abbr', 'week'],
                how='left'
            )
            # Calculate Shares
            player_stats = player_stats.with_columns(
                (pl.col('targets') / pl.col('team_pass_attempts')).alias('team_targets_share'),
                (pl.col('receptions') / pl.col('team_receptions')).alias('team_receptions_share'),
                (pl.col('carries') / pl.col('team_rush_attempts')).alias('team_rush_attempts_share') 
            ).fill_nan(0.0).fill_null(0.0)
        else:
            # Create empty columns if offense file missing
            player_stats = player_stats.with_columns(
                pl.lit(0.0).alias('team_targets_share'),
                pl.lit(0.0).alias('team_receptions_share'),
                pl.lit(0.0).alias('team_rush_attempts_share')
            )

        # 6. Standardize Column Names (CRITICAL FOR MODELS)
        # We rename raw stats to what the Feature Generator (Script 13) expects
        final_df = player_stats.rename({
            'fantasy_points_ppr': 'y_fantasy_points_ppr',
            'carries': 'rush_attempts',
            'passing_interceptions': 'interception',
            'passing_tds': 'passing_touchdown',
            'rushing_tds': 'rush_touchdown',         # <--- Needed for 'rush_touchdown_lag_1'
            'receiving_tds': 'receiving_touchdown',  # <--- Needed for 'receiving_touchdown_lag_1'
            'receiving_yards_after_catch': 'yards_after_catch'
        })

        # 7. Add Placeholders for Missing Data
        # Script 02b will fill shotgun/no_huddle. Redzone is usually missing from public pbp summaries.
        # We init them to 0.0 so models don't crash.
        for col in ['shotgun', 'no_huddle', 'receptions_redzone', 'targets_redzone', 'rush_touchdown_redzone']:
            if col not in final_df.columns:
                final_df = final_df.with_columns(pl.lit(0.0).alias(col))

        # 8. Add Season
        final_df = final_df.with_columns(pl.lit(season).alias("season"))

        # 9. Save
        final_df.write_csv(player_file)
        print(f"\n✅ Successfully updated {player_file} with {len(final_df)} rows.")
        print(f"   Includes: passer_rating, adot, touches, rush_touchdown, team shares.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    update_weekly_stats(SEASON, PLAYER_STATS_FILE, OFFENSE_STATS_FILE, PROFILES_FILE)