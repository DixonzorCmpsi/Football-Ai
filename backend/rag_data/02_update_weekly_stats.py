import nflreadpy as nfl
import polars as pl
import requests
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
SCHEDULE_FILE = Path(f"schedule_{SEASON}.csv")

FANTASY_POSITIONS = ['QB', 'RB', 'WR', 'TE']

# ESPN API endpoints (free, no key required)
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
ESPN_BOXSCORE_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"

# Team abbreviation mappings (ESPN -> Standard NFL abbreviations)
ESPN_TEAM_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BUF": "BUF",
    "CAR": "CAR", "CHI": "CHI", "CIN": "CIN", "CLE": "CLE",
    "DAL": "DAL", "DEN": "DEN", "DET": "DET", "GB": "GB",
    "HOU": "HOU", "IND": "IND", "JAX": "JAX", "KC": "KC",
    "LAC": "LAC", "LAR": "LA", "LV": "LV", "MIA": "MIA",
    "MIN": "MIN", "NE": "NE", "NO": "NO", "NYG": "NYG",
    "NYJ": "NYJ", "PHI": "PHI", "PIT": "PIT", "SEA": "SEA",
    "SF": "SF", "TB": "TB", "TEN": "TEN", "WAS": "WAS",
    "LA": "LA", "WSH": "WAS", "JAC": "JAX"
}

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


# --- ESPN Fallback Functions ---
def fetch_espn_completed_weeks() -> list:
    """Get list of weeks that have completed games according to ESPN."""
    completed_weeks = set()
    try:
        # Check regular season (weeks 1-18)
        for week in range(1, 19):
            resp = requests.get(ESPN_SCOREBOARD_URL, params={"seasontype": 2, "week": week}, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                for event in data.get("events", []):
                    status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name")
                    if status == "STATUS_FINAL":
                        completed_weeks.add(week)
                        break
        # Check playoffs (weeks 19+)
        for playoff_week in range(1, 5):  # Wild Card, Divisional, Conference, Super Bowl
            resp = requests.get(ESPN_SCOREBOARD_URL, params={"seasontype": 3, "week": playoff_week}, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                for event in data.get("events", []):
                    status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name")
                    if status == "STATUS_FINAL":
                        completed_weeks.add(18 + playoff_week)
                        break
    except Exception as e:
        print(f"   ⚠️ Error checking ESPN weeks: {e}")
    return sorted(list(completed_weeks))


def fetch_espn_week_stats(week: int, profiles_df: pl.DataFrame) -> pl.DataFrame:
    """
    Fetch player stats for a specific week from ESPN boxscores.
    Maps ESPN player names to our player_ids using the profiles DataFrame.
    """
    all_stats = []
    
    # Determine season type
    params = {"seasontype": 2, "week": week}
    if week >= 19:
        params = {"seasontype": 3, "week": week - 18}
    
    try:
        # 1. Get games for this week
        resp = requests.get(ESPN_SCOREBOARD_URL, params=params, timeout=10)
        if resp.status_code != 200:
            return pl.DataFrame()
        
        games = resp.json().get("events", [])
        completed_games = [g for g in games if g.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name") == "STATUS_FINAL"]
        
        print(f"   📥 ESPN Week {week}: {len(completed_games)} completed games")
        
        # 2. Fetch boxscores for each completed game
        for game in completed_games:
            espn_id = game.get("id")
            if not espn_id:
                continue
            
            try:
                box_resp = requests.get(ESPN_BOXSCORE_URL, params={"event": espn_id}, timeout=15)
                if box_resp.status_code != 200:
                    continue
                boxscore = box_resp.json()
                
                # Get team info
                competitors = game.get("competitions", [{}])[0].get("competitors", [])
                teams_in_game = {}
                for comp in competitors:
                    abbr = ESPN_TEAM_MAP.get(comp.get("team", {}).get("abbreviation", ""), "")
                    is_home = comp.get("homeAway") == "home"
                    teams_in_game[abbr] = {"is_home": is_home}
                    # Find opponent
                    for other_comp in competitors:
                        other_abbr = ESPN_TEAM_MAP.get(other_comp.get("team", {}).get("abbreviation", ""), "")
                        if other_abbr != abbr:
                            teams_in_game[abbr]["opponent"] = other_abbr
                
                # Parse player stats from boxscore
                for team_data in boxscore.get("boxscore", {}).get("players", []):
                    team_abbr = ESPN_TEAM_MAP.get(team_data.get("team", {}).get("abbreviation", ""), "")
                    opponent = teams_in_game.get(team_abbr, {}).get("opponent", "")
                    
                    for stat_category in team_data.get("statistics", []):
                        category_name = stat_category.get("name", "").lower()
                        if category_name not in ["passing", "rushing", "receiving"]:
                            continue
                        
                        keys = stat_category.get("keys", [])
                        
                        for athlete in stat_category.get("athletes", []):
                            player_info = athlete.get("athlete", {})
                            player_name = player_info.get("displayName", "")
                            position = player_info.get("position", {}).get("abbreviation", "")
                            
                            # Skip non-fantasy positions
                            if position not in FANTASY_POSITIONS:
                                continue
                            
                            stats_values = athlete.get("stats", [])
                            stat_dict = {keys[i].lower(): stats_values[i] for i in range(min(len(keys), len(stats_values)))}
                            
                            row = {
                                "espn_player_name": player_name,
                                "position": position,
                                "team": team_abbr,
                                "opponent_team": opponent,
                                "week": week,
                            }
                            
                            if category_name == "passing":
                                c_att = stat_dict.get("c/att", "0/0").split("/")
                                row.update({
                                    "completions": int(c_att[0]) if c_att[0].isdigit() else 0,
                                    "attempts": int(c_att[1]) if len(c_att) > 1 and c_att[1].isdigit() else 0,
                                    "passing_yards": int(stat_dict.get("yds", 0) or 0),
                                    "passing_tds": int(stat_dict.get("td", 0) or 0),
                                    "passing_interceptions": int(stat_dict.get("int", 0) or 0),
                                })
                            elif category_name == "rushing":
                                row.update({
                                    "carries": int(stat_dict.get("car", 0) or 0),
                                    "rushing_yards": int(float(stat_dict.get("yds", 0) or 0)),
                                    "rushing_tds": int(stat_dict.get("td", 0) or 0),
                                })
                            elif category_name == "receiving":
                                row.update({
                                    "receptions": int(stat_dict.get("rec", 0) or 0),
                                    "receiving_yards": int(float(stat_dict.get("yds", 0) or 0)),
                                    "receiving_tds": int(stat_dict.get("td", 0) or 0),
                                    "targets": int(stat_dict.get("tar", 0) or 0) if "tar" in stat_dict else 0,
                                })
                            
                            all_stats.append(row)
                            
            except Exception as e:
                print(f"      ⚠️ Error fetching boxscore {espn_id}: {e}")
                continue
        
        if not all_stats:
            return pl.DataFrame()
        
        # 3. Convert to DataFrame and aggregate by player/week
        espn_df = pl.DataFrame(all_stats)
        
        # Aggregate stats by player (ESPN sends separate rows for passing/rushing/receiving)
        agg_cols = []
        for col in ["completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions",
                    "carries", "rushing_yards", "rushing_tds", "receptions", "receiving_yards", "receiving_tds", "targets"]:
            if col in espn_df.columns:
                agg_cols.append(pl.col(col).sum().alias(col))
        
        espn_df = espn_df.group_by(["espn_player_name", "position", "team", "opponent_team", "week"]).agg(agg_cols)
        
        # 4. Map ESPN player names to our player_ids using fuzzy matching
        # First, try exact name match
        name_to_id = dict(zip(profiles_df["player_name"].to_list(), profiles_df["player_id"].to_list()))
        
        def match_player_id(name):
            # Exact match
            if name in name_to_id:
                return name_to_id[name]
            # Try removing suffix (Jr., III, etc.)
            clean_name = name.replace(" Jr.", "").replace(" III", "").replace(" II", "").strip()
            if clean_name in name_to_id:
                return name_to_id[clean_name]
            return None
        
        espn_df = espn_df.with_columns(
            pl.col("espn_player_name").map_elements(match_player_id, return_dtype=pl.Utf8).alias("player_id")
        )
        
        # Filter out players we couldn't match
        matched = espn_df.filter(pl.col("player_id").is_not_null())
        unmatched = espn_df.filter(pl.col("player_id").is_null())
        
        if len(unmatched) > 0:
            print(f"      ⚠️ Could not match {len(unmatched)} ESPN players to our roster")
        
        print(f"      ✅ Matched {len(matched)} players from ESPN")
        
        return matched.drop("espn_player_name")
        
    except Exception as e:
        print(f"   ❌ Error fetching ESPN week {week}: {e}")
        return pl.DataFrame()


def fill_missing_weeks_from_espn(nflreadpy_df: pl.DataFrame, profiles_df: pl.DataFrame) -> pl.DataFrame:
    """
    Check for weeks with completed games but missing nflreadpy data,
    and fill them from ESPN as a fallback.
    """
    print("\n--- Checking for Missing Weeks (ESPN Fallback) ---")
    
    # Get weeks we have in nflreadpy data
    nflreadpy_weeks = set(nflreadpy_df["week"].unique().to_list())
    
    # Get weeks that should have data (completed games)
    espn_completed = set(fetch_espn_completed_weeks())
    
    # Find missing weeks
    missing_weeks = espn_completed - nflreadpy_weeks
    
    if not missing_weeks:
        print("   ✅ No missing weeks detected")
        return nflreadpy_df
    
    print(f"   📍 Missing weeks in nflreadpy: {sorted(missing_weeks)}")
    print("   🔄 Fetching from ESPN as fallback...")
    
    espn_dfs = []
    for week in sorted(missing_weeks):
        espn_data = fetch_espn_week_stats(week, profiles_df)
        if not espn_data.is_empty():
            espn_dfs.append(espn_data)
    
    if not espn_dfs:
        print("   ⚠️ Could not fetch any ESPN data for missing weeks")
        return nflreadpy_df
    
    # Combine ESPN data
    espn_combined = pl.concat(espn_dfs, how="diagonal")
    
    # Add missing columns that nflreadpy has
    for col in nflreadpy_df.columns:
        if col not in espn_combined.columns:
            espn_combined = espn_combined.with_columns(pl.lit(None).alias(col))
    
    # Calculate fantasy points for ESPN data
    espn_combined = espn_combined.with_columns([
        (
            pl.col("passing_yards").fill_null(0) * 0.04 +
            pl.col("passing_tds").fill_null(0) * 4 +
            pl.col("passing_interceptions").fill_null(0) * -1 +
            pl.col("rushing_yards").fill_null(0) * 0.1 +
            pl.col("rushing_tds").fill_null(0) * 6 +
            pl.col("receiving_yards").fill_null(0) * 0.1 +
            pl.col("receiving_tds").fill_null(0) * 6 +
            pl.col("receptions").fill_null(0) * 1
        ).alias("fantasy_points_ppr")
    ])
    
    # Select same columns as nflreadpy
    espn_combined = espn_combined.select([c for c in nflreadpy_df.columns if c in espn_combined.columns])
    
    # Combine with nflreadpy data
    combined = pl.concat([nflreadpy_df, espn_combined], how="diagonal")
    
    print(f"   ✅ Added {len(espn_combined)} rows from ESPN fallback")
    
    return combined

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
        
        # 3a. ESPN Fallback - Fill missing weeks from ESPN if nflreadpy is behind
        if df_profiles is not None or profiles_file.exists():
            profiles_for_espn = pl.read_csv(profiles_file) if df_profiles is None else pl.read_csv(profiles_file)
            player_stats = fill_missing_weeks_from_espn(player_stats, profiles_for_espn)
        
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