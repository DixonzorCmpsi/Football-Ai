import nflreadpy as nfl
import polars as pl
from pathlib import Path
import sys
from datetime import datetime

# --- Configuration ---
# --- Dynamic Season Logic ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()

PLAYER_STATS_FILE = Path(f"weekly_player_stats_{SEASON}.csv")
OFFENSE_STATS_FILE = Path(f"weekly_offense_stats_{SEASON}.csv")
PROFILES_FILE = Path("player_profiles.csv")

# Define fantasy positions
FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']

# Updated list of columns we can *actually* get
STATS_COLUMNS_BASE = [
    # Identifiers
    'player_id', 'week', 'opponent_team', 'position', 'team',
    
    # Passing
    'completions', 'attempts', 'passing_yards', 'passing_tds', 
    'passing_interceptions', 'passing_air_yards',

    # Rushing
    'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles_lost',
    
    # Receiving
    'receptions', 'targets', 'receiving_yards', 'receiving_tds', 
    'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch',

    # Target
    'fantasy_points_ppr'
]

def update_weekly_stats(season, player_file, offense_file, profiles_file):
    """
    Downloads all season-to-date weekly stats, joins with profiles to get team,
    joins with offense stats to calculate team shares.
    """
    print(f"Loading all {season} weekly player stats...")
    try:
        player_stats_raw = nfl.load_player_stats(seasons=season, summary_level='week')
        
        # We still cast 'week' here as a good first step
        player_stats_raw = player_stats_raw.with_columns(
            pl.col("week").cast(pl.Int64, strict=False)
        )
        
        if player_stats_raw.is_empty():
            print(f"No player stats found for {season}.")
            return
            
        # --- 1. Load Team Offense Data ---
        print(f"Loading team offense data from {offense_file} for team shares...")
        df_offense = pl.read_csv(offense_file)
        
        # Cast 'week' on the right side of the join to Int64
        df_team_shares = df_offense.select(
            pl.col('team_abbr'), 
            pl.col('week').cast(pl.Int64, strict=False), # Ensure right side is Int64
            pl.col('attempts').alias('team_pass_attempts'),
            pl.col('receptions').alias('team_receptions'),
            pl.col('carries').alias('team_rush_attempts')
        )

        # --- 2. Load Player Profiles (to get team_abbr) ---
        print(f"Loading player profiles from {profiles_file} to get teams...")
        df_profiles = pl.read_csv(profiles_file)
        df_profiles = df_profiles.select(['player_id', 'team_abbr']).unique(subset=['player_id'])

        # --- 3. Filter Player Stats ---
        print(f"Filtering for positions: {FANTASY_POSITIONS}...")
        player_stats = player_stats_raw.filter(
            pl.col('position').is_in(FANTASY_POSITIONS)
        )
        
        # Check for 'team' column, add if missing
        if 'team' not in player_stats.columns:
            print("Warning: 'team' column not in player_stats, joining with profiles to get it.")
            # Note: This join might fail if player_id types differ. Assume they match for now.
            player_stats = player_stats.join(df_profiles.rename({'team_abbr':'team'}), on='player_id', how='left')

        available_cols = [col for col in STATS_COLUMNS_BASE if col in player_stats.columns]
        missing_cols = set(STATS_COLUMNS_BASE) - set(available_cols)
        if missing_cols:
            print(f"Info: The following columns were not found in nflreadpy: {missing_cols}")
            
        player_stats_filtered = player_stats.select(available_cols)
        
        # --- 4. Calculate Derived Stats (Touches, YPC, etc.) ---
        print("Calculating derived player stats (touches, ypc, etc.)...")
        player_stats_derived = player_stats_filtered.with_columns(
            (pl.col('carries').fill_null(0) + pl.col('receptions').fill_null(0)).alias('touches'),
            (pl.col('rushing_yards').fill_null(0) + pl.col('receiving_yards').fill_null(0) + pl.col('passing_yards').fill_null(0)).alias('total_off_yards'),
            (pl.col('rushing_yards') / pl.when(pl.col('carries') != 0).then(pl.col('carries')).otherwise(None)).alias('ypc'),
            (pl.col('receiving_yards') / pl.when(pl.col('receptions') != 0).then(pl.col('receptions')).otherwise(None)).alias('ypr'),
            (pl.col('completions') / pl.when(pl.col('attempts') != 0).then(pl.col('attempts')).otherwise(None)).alias('pass_pct')
        ).with_columns(
             (pl.col('total_off_yards') / pl.when(pl.col('touches') != 0).then(pl.col('touches')).otherwise(None)).alias('yptouch')
        ).fill_nan(None).fill_null(0.0) # <-- This is the line that can cause the 'week' column to become f64

        # --- 5. Join Team Data to Calculate Shares ---
        print("Joining team data to calculate shares...")

        # ---
        # --- THIS IS THE FIX ---
        #
        # The .fill_null(0.0) above can accidentally convert 'week' to f64.
        # We MUST explicitly re-cast the 'week' column on the left side to Int64
        # *immediately before* the join to match the Int64 on the right side.
        #
        print(f"[DEBUG] Type of 'week' in left table (before cast): {player_stats_derived.schema['week']}")
        player_stats_derived = player_stats_derived.with_columns(
            pl.col("week").cast(pl.Int64, strict=False)
        )
        print(f"[DEBUG] Type of 'week' in left table (after cast): {player_stats_derived.schema['week']}")
        print(f"[DEBUG] Type of 'week' in right table: {df_team_shares.schema['week']}")
        #
        # --- END OF FIX ---

        player_stats_joined = player_stats_derived.join(
            df_team_shares,
            left_on=['team', 'week'],  # 'team' from player_stats is the team_abbr
            right_on=['team_abbr', 'week'], # 'team_abbr' from df_team_shares
            how='left'
        )

        # --- 6. Calculate Share Features ---
        print("Calculating team share features...")
        player_stats_with_shares = player_stats_joined.with_columns(
            (pl.col('targets') / pl.col('team_pass_attempts')).alias('team_targets_share'),
            (pl.col('receptions') / pl.col('team_receptions')).alias('team_receptions_share'),
            (pl.col('carries') / pl.col('team_rush_attempts')).alias('team_rush_attempts_share') 
        ).fill_nan(0.0).fill_null(0.0) # fill_nan(0.0) is safer here

        # --- 7. Save Final File ---
        final_df = player_stats_with_shares.rename({
            'fantasy_points_ppr': 'y_fantasy_points_ppr',
            'carries': 'rush_attempts', # Rename for consistency
            'passing_interceptions': 'interception' # Rename for consistency
        })

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
        print("Please run '03_create_defense_file.py' first to generate it.")
    elif not PROFILES_FILE.exists():
        print(f"Error: {PROFILES_FILE} not found.")
        print("Please run '01_create_static_files.py' first to generate it.")
    else:
        update_weekly_stats(SEASON, PLAYER_STATS_FILE, OFFENSE_STATS_FILE, PROFILES_FILE)