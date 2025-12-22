import nflreadpy as nfl
import polars as pl
import sys
import os
import requests
from pathlib import Path 
from datetime import datetime
from dotenv import load_dotenv
import traceback

# --- Configuration ---
load_dotenv()
SPORTSDATAIO_KEY = os.getenv("SPORTSDATAIO_KEY")

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
print(f"Dynamic Season Detected: {SEASON}")

DEFENSE_FILE = f'weekly_defense_stats_{SEASON}.csv'
OFFENSE_FILE = f'weekly_offense_stats_{SEASON}.csv'
SCHEDULE_FILE = f'schedule_{SEASON}.csv' 

def get_sportsdataio_odds(season, week):
    """Fetches Game Odds (Spread/Total/Moneyline) from SportsDataIO"""
    if not SPORTSDATAIO_KEY:
        print("⚠️ No SPORTSDATAIO_KEY found. Skipping API fetch.")
        return None
        
    try:
        url = f"https://api.sportsdata.io/v3/nfl/odds/json/GameOddsByWeek/{season}/{week}"
        headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_KEY}
        
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            return res.json()
        else:
            print(f"⚠️ SportsDataIO Error ({res.status_code}): {res.text}")
            return None
    except Exception as e:
        print(f"⚠️ API Request Failed: {e}")
        return None

def refresh_schedule_with_spread(season):
    print(f"\n--- Refreshing {SCHEDULE_FILE} using SportsDataIO ---")
    try:
        schedules = nfl.load_schedules(seasons=[season])
        
        # Determine current week
        unfinished = schedules.filter(pl.col("result").is_null())
        if not unfinished.is_empty():
            current_week = unfinished["week"].min()
        else:
            current_week = 18 
        
        # Fetch Live Odds
        print(f"   Fetching live odds for Week {current_week}...")
        odds_data = get_sportsdataio_odds(season, current_week)
        
        # Map Odds to Schedule
        odds_map = {}
        if odds_data:
            for game in odds_data:
                home_team = game.get("HomeTeamName")
                if home_team:
                    odds_map[home_team] = {
                        "spread": game.get("PointSpread"), 
                        "total": game.get("OverUnder"),
                        "home_ml": game.get("HomeMoneyLine"),
                        "away_ml": game.get("AwayMoneyLine")
                    }

        # Helper functions for mapping
        def get_val(team, key):
            return odds_map.get(team, {}).get(key, None)

        # Apply Columns
        final_sched = schedules.with_columns([
            pl.col("home_team").map_elements(lambda x: get_val(x, "spread"), return_dtype=pl.Float64).alias("spread_line"),
            pl.col("home_team").map_elements(lambda x: get_val(x, "total"), return_dtype=pl.Float64).alias("total_line"),
            pl.col("home_team").map_elements(lambda x: get_val(x, "home_ml"), return_dtype=pl.Int64).alias("moneyline_home"),
            pl.col("home_team").map_elements(lambda x: get_val(x, "away_ml"), return_dtype=pl.Int64).alias("moneyline_away")
        ])

        # Safeguard: Ensure columns exist
        for col in ["spread_line", "total_line"]:
            if col not in final_sched.columns:
                final_sched = final_sched.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
        for col in ["moneyline_home", "moneyline_away"]:
            if col not in final_sched.columns:
                final_sched = final_sched.with_columns(pl.lit(None).cast(pl.Int64).alias(col))

        # Select Final Columns
        output_cols = [
            pl.col('game_id'), pl.col('week'), pl.col('season'), 
            pl.col('home_team'), pl.col('away_team'), 
            pl.col('home_score'), pl.col('away_score'), 
            pl.col('spread_line').alias('spread'),
            pl.col('total_line').alias('over_under'),
            pl.col('moneyline_home'),
            pl.col('moneyline_away'),
            pl.col('gameday') 
        ]
        
        # Filter and Save
        available = [c for c in output_cols if c.meta.output_name() in final_sched.columns]
        final_sched = final_sched.select(available)
        
        final_sched.write_csv(SCHEDULE_FILE)
        print(f"✅ Schedule refreshed with Moneyline/Spread: {SCHEDULE_FILE}")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        return False

def create_team_stats_files(season):
    if not refresh_schedule_with_spread(season): 
        return

    print(f"\nLoading all {season} weekly TEAM stats from nflreadpy...")
    try:
        team_stats = nfl.load_team_stats(seasons=season, summary_level='week')
        if team_stats.is_empty(): return

        schedule = pl.read_csv(SCHEDULE_FILE)
        
        schedule = schedule.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        team_stats = team_stats.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        if not team_stats.is_empty():
            max_week = team_stats['week'].max()
            schedule = schedule.filter(pl.col('week') <= max_week)

        home_scores = schedule.select(pl.col('home_team').alias('team'), 'week', pl.col('home_score').alias('points_for'), pl.col('away_score').alias('points_allowed'))
        away_scores = schedule.select(pl.col('away_team').alias('team'), 'week', pl.col('away_score').alias('points_for'), pl.col('home_score').alias('points_allowed'))
        points_table = pl.concat([home_scores, away_scores])
        points_table = points_table.with_columns(pl.col("week").cast(pl.Int64, strict=False))

        # --- DEFENSE ---
        offense_join = team_stats.select('team', 'week', pl.col('passing_yards').alias('passing_yards_allowed'), pl.col('rushing_yards').alias('rushing_yards_allowed')).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        core_def = team_stats.select('team', 'week', 'opponent_team', 'def_sacks', 'def_interceptions', 'def_fumbles_forced', 'def_qb_hits').with_columns(pl.col("week").cast(pl.Int64, strict=False))
        
        def_df = core_def.join(offense_join, left_on=['opponent_team', 'week'], right_on=['team', 'week'], how='left')
        def_df = def_df.join(points_table.select(['team', 'week', 'points_allowed']), on=['team', 'week'], how='left')
        
        def_final = def_df.select([
            pl.col('team').alias('team_abbr'), pl.col('week'), pl.col('opponent_team'), pl.col('points_allowed'),
            pl.col('passing_yards_allowed'), pl.col('rushing_yards_allowed'), pl.col('def_sacks'), pl.col('def_interceptions'),
            pl.col('def_fumbles_forced'), pl.col('def_qb_hits')
        ]).drop_nulls(subset=['team_abbr', 'week']).with_columns(pl.lit(season).alias("season"))
        
        def_final.write_csv(DEFENSE_FILE)
        print(f"✅ Generated {DEFENSE_FILE}")

        # --- OFFENSE ---
        core_off = team_stats.select('team', 'week', 'opponent_team', 'passing_yards', 'rushing_yards', 'passing_tds', 'rushing_tds', 'passing_interceptions', 'rushing_fumbles_lost', 'passing_first_downs', 'rushing_first_downs', 'attempts', 'receptions', 'carries').with_columns(pl.col("week").cast(pl.Int64, strict=False))
        off_df = core_off.join(points_table.select(['team', 'week', 'points_for']), on=['team', 'week'], how='left')
        
        off_final = off_df.select([
            pl.col('team').alias('team_abbr'), pl.col('week'), pl.col('opponent_team'), pl.col('points_for').alias('points_scored'),
            pl.col('passing_yards'), pl.col('rushing_yards'), pl.col('passing_tds'), pl.col('rushing_tds'), pl.col('passing_interceptions'),
            pl.col('rushing_fumbles_lost'), pl.col('passing_first_downs'), pl.col('rushing_first_downs'), pl.col('attempts'), pl.col('receptions'), pl.col('carries')
        ]).drop_nulls(subset=['team_abbr', 'week']).with_columns(pl.lit(season).alias("season"))

        off_final = off_final.with_columns((pl.col("passing_yards") + pl.col("rushing_yards")).alias("total_yards"))

        off_final.write_csv(OFFENSE_FILE)
        print(f"✅ Generated {OFFENSE_FILE}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    create_team_stats_files(SEASON)