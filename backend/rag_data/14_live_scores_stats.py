"""
Live Scores & Stats Fetcher
===========================
Fetches real-time game scores and player stats from fast APIs (ESPN, SportsDataIO)
to update the system before nflreadpy data is available (usually 24-48 hours after games).

Usage:
    python3 14_live_scores_stats.py                    # Update current week
    python3 14_live_scores_stats.py --week 19          # Update specific week
    python3 14_live_scores_stats.py --scores-only      # Only update game scores
    python3 14_live_scores_stats.py --stats-only       # Only update player stats

Data Sources:
    - ESPN API (free): Live scores, boxscores, player stats
    - SportsDataIO (requires key): Backup for player stats if ESPN fails
"""

import requests
import polars as pl
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3:
        return now.year
    else:
        return now.year - 1

SEASON = get_current_season()
SPORTSDATAIO_KEY = os.getenv("SPORTSDATAIO_KEY")

# File paths
SCHEDULE_FILE = Path(f"schedule_{SEASON}.csv")
PLAYER_STATS_FILE = Path(f"weekly_player_stats_{SEASON}.csv")

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
    # Handle some ESPN quirks
    "LA": "LA", "WSH": "WAS", "JAC": "JAX"
}


def fetch_espn_scores(week: int = None) -> list:
    """
    Fetch live/final scores from ESPN API.
    Returns list of dicts with: home_team, away_team, home_score, away_score, status
    """
    print(f"\n--- Fetching Live Scores from ESPN ---")
    
    params = {"seasontype": 2}  # Regular season = 2, Postseason = 3
    
    # For playoffs (week 19+), use seasontype=3
    if week and week >= 19:
        params["seasontype"] = 3
        # ESPN uses week numbers 1-4 for playoffs
        params["week"] = week - 18
    elif week:
        params["week"] = week
    
    try:
        resp = requests.get(ESPN_SCOREBOARD_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        games = []
        for event in data.get("events", []):
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            
            if len(competitors) != 2:
                continue
            
            # ESPN: competitors[0] is usually home, [1] is away
            home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
            
            home_abbr = ESPN_TEAM_MAP.get(home["team"]["abbreviation"], home["team"]["abbreviation"])
            away_abbr = ESPN_TEAM_MAP.get(away["team"]["abbreviation"], away["team"]["abbreviation"])
            
            status = competition.get("status", {}).get("type", {}).get("name", "Unknown")
            
            game_info = {
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_score": int(home.get("score", 0)) if home.get("score") else None,
                "away_score": int(away.get("score", 0)) if away.get("score") else None,
                "status": status,  # STATUS_SCHEDULED, STATUS_IN_PROGRESS, STATUS_FINAL
                "espn_game_id": event.get("id"),
                "game_date": event.get("date")
            }
            games.append(game_info)
            
            status_icon = "🏈" if status == "STATUS_IN_PROGRESS" else ("✅" if status == "STATUS_FINAL" else "📅")
            print(f"   {status_icon} {away_abbr} @ {home_abbr}: {game_info['away_score'] or '-'} - {game_info['home_score'] or '-'} ({status})")
        
        return games
        
    except Exception as e:
        print(f"   ❌ ESPN API Error: {e}")
        return []


def fetch_espn_boxscore(espn_game_id: str) -> dict:
    """
    Fetch detailed boxscore/player stats for a specific game from ESPN.
    Returns dict with player stats.
    """
    try:
        resp = requests.get(ESPN_BOXSCORE_URL, params={"event": espn_game_id}, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"   ⚠️ Could not fetch boxscore for game {espn_game_id}: {e}")
        return {}


def parse_espn_player_stats(boxscore: dict, week: int) -> list:
    """
    Parse ESPN boxscore data into player stats rows.
    Returns list of dicts compatible with weekly_player_stats format.
    """
    stats_rows = []
    
    try:
        boxscore_data = boxscore.get("boxscore", {})
        players_data = boxscore_data.get("players", [])
        
        for team_data in players_data:
            team_abbr = ESPN_TEAM_MAP.get(
                team_data.get("team", {}).get("abbreviation", ""),
                team_data.get("team", {}).get("abbreviation", "")
            )
            
            # Get opponent
            home_team = boxscore.get("header", {}).get("competitions", [{}])[0].get("competitors", [{}])
            opponent = None
            for comp in home_team:
                comp_abbr = ESPN_TEAM_MAP.get(comp.get("team", {}).get("abbreviation", ""), "")
                if comp_abbr != team_abbr:
                    opponent = comp_abbr
                    break
            
            # Process each stat category
            for stat_category in team_data.get("statistics", []):
                category_name = stat_category.get("name", "").lower()
                
                # Map category to stat type
                if category_name not in ["passing", "rushing", "receiving"]:
                    continue
                
                athletes = stat_category.get("athletes", [])
                keys = stat_category.get("keys", [])
                labels = stat_category.get("labels", [])
                
                for athlete in athletes:
                    player_info = athlete.get("athlete", {})
                    player_name = player_info.get("displayName", "Unknown")
                    espn_player_id = player_info.get("id")
                    position = player_info.get("position", {}).get("abbreviation", "")
                    
                    stats_values = athlete.get("stats", [])
                    
                    # Create stats dict from keys/values
                    stat_dict = {}
                    for i, key in enumerate(keys):
                        if i < len(stats_values):
                            stat_dict[key.lower()] = stats_values[i]
                    
                    # Build row based on category
                    row = {
                        "espn_id": espn_player_id,
                        "player_name": player_name,
                        "position": position,
                        "team": team_abbr,
                        "opponent_team": opponent,
                        "week": week,
                    }
                    
                    if category_name == "passing":
                        # Parse C/ATT format (e.g., "25/35")
                        c_att = stat_dict.get("c/att", "0/0").split("/")
                        row.update({
                            "completions": int(c_att[0]) if len(c_att) > 0 else 0,
                            "attempts": int(c_att[1]) if len(c_att) > 1 else 0,
                            "passing_yards": int(stat_dict.get("yds", 0) or 0),
                            "passing_tds": int(stat_dict.get("td", 0) or 0),
                            "passing_interceptions": int(stat_dict.get("int", 0) or 0),
                        })
                    elif category_name == "rushing":
                        row.update({
                            "carries": int(stat_dict.get("car", 0) or 0),
                            "rushing_yards": int(stat_dict.get("yds", 0) or 0),
                            "rushing_tds": int(stat_dict.get("td", 0) or 0),
                        })
                    elif category_name == "receiving":
                        row.update({
                            "receptions": int(stat_dict.get("rec", 0) or 0),
                            "receiving_yards": int(stat_dict.get("yds", 0) or 0),
                            "receiving_tds": int(stat_dict.get("td", 0) or 0),
                            "targets": int(stat_dict.get("tar", 0) or 0) if "tar" in stat_dict else None,
                        })
                    
                    stats_rows.append(row)
        
    except Exception as e:
        print(f"   ⚠️ Error parsing boxscore: {e}")
    
    return stats_rows


def update_schedule_scores(games: list):
    """
    Update schedule CSV with live/final scores.
    """
    if not games:
        print("   No games to update.")
        return
    
    if not SCHEDULE_FILE.exists():
        print(f"   ❌ Schedule file not found: {SCHEDULE_FILE}")
        return
    
    print(f"\n--- Updating Schedule with Scores ---")
    
    try:
        schedule = pl.read_csv(SCHEDULE_FILE)
        updated_count = 0
        
        for game in games:
            # Only update if game is final or in progress
            if game["status"] not in ["STATUS_FINAL", "STATUS_IN_PROGRESS"]:
                continue
            
            if game["home_score"] is None:
                continue
            
            # Find matching game in schedule
            mask = (pl.col("home_team") == game["home_team"]) & (pl.col("away_team") == game["away_team"])
            
            # Update scores
            schedule = schedule.with_columns([
                pl.when(mask).then(pl.lit(game["home_score"])).otherwise(pl.col("home_score")).alias("home_score"),
                pl.when(mask).then(pl.lit(game["away_score"])).otherwise(pl.col("away_score")).alias("away_score"),
            ])
            
            updated_count += 1
        
        # Write back
        schedule.write_csv(SCHEDULE_FILE)
        print(f"   ✅ Updated {updated_count} game scores in {SCHEDULE_FILE}")
        
    except Exception as e:
        print(f"   ❌ Error updating schedule: {e}")


def fetch_sportsdata_player_stats(week: int) -> list:
    """
    Fetch player stats from SportsDataIO as backup.
    Requires SPORTSDATAIO_KEY environment variable.
    """
    if not SPORTSDATAIO_KEY:
        print("   ⚠️ No SPORTSDATAIO_KEY set. Skipping SportsDataIO.")
        return []
    
    print(f"\n--- Fetching Player Stats from SportsDataIO ---")
    
    url = f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/{SEASON}/{week}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_KEY}
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        print(f"   📊 Received {len(data)} player stat records")
        return data
        
    except Exception as e:
        print(f"   ❌ SportsDataIO Error: {e}")
        return []


def merge_live_stats_to_csv(live_stats: list, week: int):
    """
    Merge live stats into the weekly_player_stats CSV.
    Uses ESPN player names to match (fallback: position + team).
    Only adds/updates stats for the specified week.
    """
    if not live_stats:
        print("   No stats to merge.")
        return
    
    print(f"\n--- Merging Live Stats into {PLAYER_STATS_FILE} ---")
    
    # Convert to DataFrame
    live_df = pl.DataFrame(live_stats)
    
    # Aggregate stats by player (since ESPN sends separate rows for passing/rushing/receiving)
    if "espn_id" in live_df.columns:
        # Group by ESPN ID and aggregate
        agg_cols = []
        for col in ["completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions",
                    "carries", "rushing_yards", "rushing_tds",
                    "receptions", "receiving_yards", "receiving_tds", "targets"]:
            if col in live_df.columns:
                agg_cols.append(pl.col(col).sum().alias(col))
        
        live_df = live_df.group_by(["espn_id", "player_name", "position", "team", "opponent_team", "week"]).agg(agg_cols)
    
    # Calculate fantasy points
    live_df = live_df.with_columns([
        (
            pl.col("passing_yards").fill_null(0) * 0.04 +
            pl.col("passing_tds").fill_null(0) * 4 +
            pl.col("passing_interceptions").fill_null(0) * -1 +
            pl.col("rushing_yards").fill_null(0) * 0.1 +
            pl.col("rushing_tds").fill_null(0) * 6 +
            pl.col("receiving_yards").fill_null(0) * 0.1 +
            pl.col("receiving_tds").fill_null(0) * 6 +
            pl.col("receptions").fill_null(0) * 1  # PPR
        ).alias("fantasy_points_ppr")
    ])
    
    print(f"   📊 Processed {len(live_df)} player records")
    
    # If existing file exists, merge; otherwise create new
    if PLAYER_STATS_FILE.exists():
        existing = pl.read_csv(PLAYER_STATS_FILE)
        
        # Remove existing data for this week (we're replacing it)
        existing = existing.filter(pl.col("week") != week)
        
        # Ensure column compatibility
        for col in existing.columns:
            if col not in live_df.columns:
                live_df = live_df.with_columns(pl.lit(None).alias(col))
        
        for col in live_df.columns:
            if col not in existing.columns:
                existing = existing.with_columns(pl.lit(None).alias(col))
        
        # Reorder columns to match
        live_df = live_df.select(existing.columns)
        
        # Combine
        combined = pl.concat([existing, live_df])
        combined.write_csv(PLAYER_STATS_FILE)
        print(f"   ✅ Merged {len(live_df)} records into {PLAYER_STATS_FILE}")
    else:
        live_df.write_csv(PLAYER_STATS_FILE)
        print(f"   ✅ Created {PLAYER_STATS_FILE} with {len(live_df)} records")


def get_current_nfl_week() -> int:
    """Determine current NFL week based on schedule."""
    try:
        if SCHEDULE_FILE.exists():
            sched = pl.read_csv(SCHEDULE_FILE)
            # Find first week with unplayed games
            unplayed = sched.filter(pl.col("home_score").is_null())
            if not unplayed.is_empty():
                return int(unplayed["week"].min())
        # Default to week 19 for playoffs
        return 19
    except:
        return 19


def main():
    parser = argparse.ArgumentParser(description="Fetch live scores and stats")
    parser.add_argument("--week", type=int, help="NFL week number (default: current)")
    parser.add_argument("--scores-only", action="store_true", help="Only update game scores")
    parser.add_argument("--stats-only", action="store_true", help="Only update player stats")
    args = parser.parse_args()
    
    week = args.week or get_current_nfl_week()
    print(f"=== Live Scores & Stats Updater (Week {week}) ===")
    
    # 1. Fetch live scores from ESPN
    if not args.stats_only:
        games = fetch_espn_scores(week)
        update_schedule_scores(games)
        
        # 2. Fetch player stats from ESPN boxscores for completed games
        if not args.scores_only:
            completed_games = [g for g in games if g["status"] == "STATUS_FINAL"]
            
            if completed_games:
                print(f"\n--- Fetching Boxscores for {len(completed_games)} Completed Games ---")
                all_stats = []
                
                for game in completed_games:
                    espn_id = game.get("espn_game_id")
                    if espn_id:
                        print(f"   📥 {game['away_team']} @ {game['home_team']}...")
                        boxscore = fetch_espn_boxscore(espn_id)
                        stats = parse_espn_player_stats(boxscore, week)
                        all_stats.extend(stats)
                
                if all_stats:
                    merge_live_stats_to_csv(all_stats, week)
            else:
                print("\n   No completed games yet. Stats will be available after games finish.")
    
    # 3. If requested stats-only or as backup, try SportsDataIO
    if args.stats_only and SPORTSDATAIO_KEY:
        sportsdata_stats = fetch_sportsdata_player_stats(week)
        if sportsdata_stats:
            # Convert SportsDataIO format to our format (would need mapping)
            print("   ⚠️ SportsDataIO stats integration pending - use ESPN for now")
    
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
