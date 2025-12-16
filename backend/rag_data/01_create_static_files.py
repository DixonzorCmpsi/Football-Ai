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
    """
    Helper function to calculate age from birth date STRING.
    Strips whitespace and focuses on '%Y-%m-%d' format.
    """
    global printed_date_format_debug
    if birth_date_str is None:
        return None

    # Clean the input string
    cleaned_str = str(birth_date_str).strip() # Ensure it's a string and strip whitespace

    birth_date = None
    fmt = '%Y-%m-%d'
    try:
        # print(f"Attempting parse: '{cleaned_str}' with format '{fmt}'") # Deeper debug if needed
        birth_date = datetime.strptime(cleaned_str, fmt)
        # print(f"Parse successful: {birth_date}") # Deeper debug if needed
    except (ValueError, TypeError) as e:
        if not printed_date_format_debug:
             print(f"[DEBUG DateFormat] Failed to parse date string: '{cleaned_str}'. Exception: {e}")
             printed_date_format_debug = True
        return None # Return None if format failed

    # If parsing succeeded, calculate age
    try:
        today = datetime.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except Exception as calc_e:
        if not printed_date_format_debug: # Use same flag to avoid flood
             print(f"[DEBUG AgeCalc] Error calculating age after parsing: {calc_e}")
             printed_date_format_debug = True
        return None

# --- create_player_profiles function (rest is the same as previous version) ---
def create_player_profiles(season):
    """
    Loads roster and player data, joins them, calculates age (with flexible parsing),
    and saves a clean CSV with player profiles.
    """
    print(f"Loading {season} rosters from nflreadpy...")
    try:
        # 1. Load Rosters
        rosters = nfl.load_rosters(seasons=season)
        if rosters.is_empty():
            print("No roster data found.")
            return

        # 2. Load Players (Simplified height join)
        print(f"\nLoading player master list...")
        players_df = nfl.load_players()
        players_height = None
        if not players_df.is_empty() and 'height' in players_df.columns and 'gsis_id' in players_df.columns:
             players_height = players_df.select(['gsis_id', pl.col('height').alias('height_players')])
             print("[DEBUG Players] Prepared player height data for join.")
        else:
             print("[DEBUG Players] Could not load player height data or missing columns.")

        # 3. Join Rosters with Player Height
        if players_height is not None:
             print("\nJoining roster data with player master list...")
             rosters = rosters.join(players_height, on='gsis_id', how='left')
             rosters = rosters.with_columns(
                 pl.coalesce(pl.col('height'), pl.col('height_players')).alias('height')
             ).drop('height_players')
             print("Height data supplemented.")

        # 4. Select initial columns
        profile_columns_source = [
            'gsis_id', 'full_name', 'position', 'team',
            'headshot_url', 'entry_year', 'draft_number',
            'status', 'status_description_abbr', 'week',
            'birth_date', 'height', 'weight'
        ]
        available_columns = [col for col in profile_columns_source if col in rosters.columns]
        player_profiles = rosters.select(available_columns)

        # 5. Calculate Age (using focused parsing function)
        if 'birth_date' in player_profiles.columns:
            print("[DEBUG AgeCalc] Attempting to calculate age (focused parsing)...")
            try:
                # Apply the improved Python function directly on the string column
                player_profiles = player_profiles.with_columns(
                   pl.col("birth_date").map_elements(calculate_age, return_dtype=pl.Int64).alias("age")
                )
                player_profiles = player_profiles.drop("birth_date") # Drop original string date

                null_age_count = player_profiles.filter(pl.col("age").is_null()).height
                total_rows = player_profiles.height
                success_count = total_rows - null_age_count
                print(f"[DEBUG AgeCalc] Age calculation attempt complete.")
                print(f"  Successfully calculated age for: {success_count} / {total_rows} players")
                print(f"  Rows with null age: {null_age_count}")
                if 'age' not in player_profiles.columns:
                     print("[DEBUG AgeCalc] 'age' column was not created successfully.")

            except Exception as age_calc_error:
                print(f"[DEBUG AgeCalc] Error during age calculation: {age_calc_error}", file=sys.stderr)
                if "birth_date" in player_profiles.columns:
                    player_profiles = player_profiles.drop("birth_date")
        else:
             print("[DEBUG AgeCalc] 'birth_date' column not found, skipping age calculation.")

        # 6. Rename columns
        rename_map = {
            'gsis_id': 'player_id', 'full_name': 'player_name', 'team': 'team_abbr',
            'headshot_url': 'headshot', 'status_description_abbr': 'injury_status',
            'entry_year': 'draft_year'
        }
        current_cols_before_rename = player_profiles.columns
        final_rename_map = {k: v for k, v in rename_map.items() if k in current_cols_before_rename}
        player_profiles = player_profiles.rename(final_rename_map)

        player_profiles = player_profiles.drop_nulls(subset=['player_id'])

        # 7. Get most recent status entry
        if 'week' in player_profiles.columns:
            # print("[DEBUG Roster] Selecting most recent player status...") # Optional debug
            player_profiles = player_profiles.sort('player_id', 'week', descending=True)
            player_profiles = player_profiles.unique(subset=['player_id'], keep='first', maintain_order=True) # maintain_order=True often better with sort
            player_profiles = player_profiles.drop('week')
        else:
             # print("[DEBUG Roster] 'week' column not found, using first unique entry per player.") # Optional debug
             player_profiles = player_profiles.unique(subset=['player_id'], keep='first', maintain_order=True)

        # 8. Save
        output_file = 'player_profiles.csv'
        player_profiles.write_csv(output_file)
        print(f"\nSuccessfully created {output_file} with {len(player_profiles)} unique players.")
        print(f"Final columns: {player_profiles.columns}") # Debug final columns

    except Exception as e:
        print(f"Error loading rosters/players: {e}", file=sys.stderr)

def create_schedule(season):
    # (No changes needed in create_schedule)
    print(f"\nLoading {season} schedule from nflreadpy...")
    # ... (rest of function is the same)
    try:
        schedule = nfl.load_schedules(seasons=season)
        if schedule.is_empty():
            print("No schedule data found.")
            return
        schedule_reg = schedule.filter(pl.col('game_type') == 'REG')
        schedule_columns = ['game_id', 'week', 'home_team', 'away_team', 'home_score', 'away_score', 'result']
        available_columns = [col for col in schedule_columns if col in schedule.columns]
        schedule_clean = schedule_reg.select(available_columns)
        output_file = 'schedule_2025.csv'
        schedule_clean.write_csv(output_file)
        print(f"Successfully created {output_file}")
    except Exception as e:
        print(f"Error loading schedule: {e}", file=sys.stderr)


if __name__ == "__main__":
    create_player_profiles(SEASON)
    create_schedule(SEASON)
    print("\nStatic data files script finished.")