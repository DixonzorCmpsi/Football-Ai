import nflreadpy as nfl
import polars as pl
import sys
from datetime import datetime

# --- Dynamic Season Logic ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()
print(f"Dynamic Season Detected: {SEASON}")

# Flag to print date format debug info only once
printed_date_format_debug = False

def calculate_age(birth_date_str: str | None) -> int | None:
    global printed_date_format_debug
    if birth_date_str is None: return None
    cleaned_str = str(birth_date_str).strip() 
    try:
        birth_date = datetime.strptime(cleaned_str, '%Y-%m-%d')
    except (ValueError, TypeError) as e:
        if not printed_date_format_debug:
             print(f"[DEBUG DateFormat] Failed to parse: '{cleaned_str}'. {e}")
             printed_date_format_debug = True
        return None 
    try:
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    except Exception: return None

def create_player_profiles(season):
    print(f"Loading {season} rosters from nflreadpy...")
    try:
        rosters = nfl.load_rosters(seasons=season)
        if rosters.is_empty(): print("No roster data found."); return

        print(f"\nLoading player master list...")
        players_df = nfl.load_players()
        
        # --- CRITICAL UPDATE: Join pfr_id and height ---
        if not players_df.is_empty():
            # Select relevant columns from master player list
            # Note: nflreadpy usually has 'pfr_id' in load_players() output
            cols_to_join = ['gsis_id']
            
            if 'pfr_id' in players_df.columns:
                cols_to_join.append('pfr_id')
                print("[DEBUG Players] Found pfr_id in master list.")
            
            if 'height' in players_df.columns:
                players_df = players_df.with_columns(pl.col('height').alias('height_players'))
                cols_to_join.append('height_players')

            players_subset = players_df.select(cols_to_join)
            
            print("Joining master data (PFR ID & Height)...")
            rosters = rosters.join(players_subset, on='gsis_id', how='left')
            
            # Coalesce height if needed
            if 'height_players' in rosters.columns:
                rosters = rosters.with_columns(
                    pl.coalesce(pl.col('height'), pl.col('height_players')).alias('height')
                ).drop('height_players')

        profile_columns_source = [
            'gsis_id', 'pfr_id', 'full_name', 'position', 'team', # Added pfr_id here
            'headshot_url', 'entry_year', 'draft_number',
            'status', 'status_description_abbr', 'week',
            'birth_date', 'height', 'weight'
        ]
        available_columns = [col for col in profile_columns_source if col in rosters.columns]
        player_profiles = rosters.select(available_columns)

        if 'birth_date' in player_profiles.columns:
            print("[DEBUG AgeCalc] Calculating age...")
            try:
                player_profiles = player_profiles.with_columns(
                   pl.col("birth_date").map_elements(calculate_age, return_dtype=pl.Int64).alias("age")
                ).drop("birth_date")
                print(f"[DEBUG AgeCalc] Complete. Null ages: {player_profiles['age'].null_count()}")
            except Exception as e: print(f"Age calc error: {e}")

        rename_map = {
            'gsis_id': 'player_id', 
            'full_name': 'player_name', 
            'team': 'team_abbr',
            'headshot_url': 'headshot', 
            'status_description_abbr': 'injury_status',
            'entry_year': 'draft_year'
            # pfr_id usually stays pfr_id, but good to be explicit if needed
        }
        final_rename = {k: v for k, v in rename_map.items() if k in player_profiles.columns}
        player_profiles = player_profiles.rename(final_rename).drop_nulls(subset=['player_id'])

        if 'week' in player_profiles.columns:
            player_profiles = player_profiles.sort('player_id', 'week', descending=True).unique(subset=['player_id'], keep='first', maintain_order=True).drop('week')
        else:
             player_profiles = player_profiles.unique(subset=['player_id'], keep='first', maintain_order=True)

        player_profiles = player_profiles.with_columns(pl.lit(season).alias('season'))

        output_file = f'player_profiles_{season}.csv'
        player_profiles.write_csv(output_file)
        print(f"\nSuccessfully created {output_file} with {len(player_profiles)} unique players.")
        print(f"Final columns: {player_profiles.columns}") 
        
        # Sanity Check for pfr_id
        if "pfr_id" in player_profiles.columns:
            count = player_profiles["pfr_id"].null_count()
            print(f"   [INFO] Players with PFR IDs: {len(player_profiles) - count} / {len(player_profiles)}")

    except Exception as e: print(f"Error loading rosters: {e}", file=sys.stderr)

def create_schedule(season):
    print(f"\nLoading {season} schedule from nflreadpy...")
    try:
        schedule = nfl.load_schedules(seasons=season)
        if schedule.is_empty(): print("No schedule data found."); return
        
        schedule_reg = schedule.filter(pl.col('game_type') == 'REG')
        
        schedule_columns = [
            'game_id', 'week', 'season',
            'home_team', 'away_team', 'home_score', 'away_score', 
            'result', 'gameday'
        ]
        available_columns = [col for col in schedule_columns if col in schedule.columns]
        
        schedule_clean = schedule_reg.select(available_columns)
        
        output_file = f'schedule_{season}.csv'
        schedule_clean.write_csv(output_file)
        print(f"Successfully created {output_file}")
    except Exception as e:
        print(f"Error loading schedule: {e}", file=sys.stderr)

if __name__ == "__main__":
    create_player_profiles(SEASON)
    create_schedule(SEASON)
    print("\nStatic data files script finished.")