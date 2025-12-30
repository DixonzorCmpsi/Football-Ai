import nflreadpy as nfl
import polars as pl
import requests
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
SPORTSDATAIO_KEY = os.getenv("SPORTSDATAIO_KEY")

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = f"weekly_player_odds_{SEASON}.csv"

def get_player_props(season, week):
    if not SPORTSDATAIO_KEY:
        print("❌ No SPORTSDATAIO_KEY found.")
        return []

    url = f"https://api.sportsdata.io/v3/nfl/odds/json/PlayerPropsByWeek/{season}/{week}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_KEY}
    
    try:
        print(f"   📡 Hitting SportsDataIO API for Week {week}...")
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            return res.json()
        else:
            print(f"   ⚠️ API Error ({res.status_code}): {res.text}")
            return []
    except Exception as e:
        print(f"   ❌ Network Error: {e}")
        return []

def calculate_implied_prob(odds):
    """
    Converts Moneyline Odds to Implied Probability Percentage.
    Formula:
      Negative (-):  |Odds| / (|Odds| + 100) * 100
      Positive (+):  100 / (Odds + 100) * 100
    """
    if odds is None: return None
    try:
        odds = float(odds)
        if odds < 0:
            return round((abs(odds) / (abs(odds) + 100)) * 100, 2)
        else:
            return round((100 / (odds + 100)) * 100, 2)
    except:
        return None

def main():
    print(f"--- 🎰 Updating Player Odds ({SEASON}) via SportsDataIO ---")
    
    # 1. Determine Current Week
    try:
        sched = nfl.load_schedules(seasons=[SEASON])
        unfinished = sched.filter(pl.col("result").is_null())
        if not unfinished.is_empty():
            current_week = unfinished["week"].min()
        else:
            current_week = 18
        print(f"   Targeting Week: {current_week}")
    except:
        current_week = 1
        print("   ⚠️ Could not detect week, defaulting to Week 1.")

    # 2. Fetch Raw Odds Data
    raw_props = get_player_props(SEASON, current_week)
    
    # Define Schema with both Value and Probability columns
    schema_cols = {
        "player_id": pl.Utf8, "player_name": pl.Utf8, "team": pl.Utf8, "week": pl.Int64, "season": pl.Int64,
        # Values (The Line)
        "prop_passing_yards": pl.Float64, "prop_passing_tds": pl.Float64, "prop_pass_attempts": pl.Float64,
        "prop_rushing_yards": pl.Float64, "prop_rush_attempts": pl.Float64,
        "prop_receiving_yards": pl.Float64, "prop_receptions": pl.Float64, "prop_anytime_td": pl.Float64,
        "prop_total_yards": pl.Float64,
        # Probabilities (%)
        "prop_passing_yards_prob": pl.Float64, "prop_passing_tds_prob": pl.Float64, "prop_pass_attempts_prob": pl.Float64,
        "prop_rushing_yards_prob": pl.Float64, "prop_rush_attempts_prob": pl.Float64,
        "prop_receiving_yards_prob": pl.Float64, "prop_receptions_prob": pl.Float64, "prop_anytime_td_prob": pl.Float64,
        "prop_total_yards_prob": pl.Float64
    }

    if not raw_props:
        print("   ⚠️ No odds data found.")
        if not os.path.exists(OUTPUT_FILE):
             pl.DataFrame(schema=schema_cols).write_csv(OUTPUT_FILE)
        return

    # 3. Load ID Map
    print("   📥 Loading ID Map (SportsDataIO -> GSIS)...")
    try:
        id_map = nfl.load_ff_playerids()
        valid_map = id_map.drop_nulls(subset=['fantasy_data_id', 'gsis_id'])
        
        # Robust Mapping
        sd_keys = valid_map['fantasy_data_id'].cast(pl.Int64).cast(pl.Utf8).to_list()
        gsis_vals = valid_map['gsis_id'].to_list()
        sd_to_gsis = dict(zip(sd_keys, gsis_vals))
        
        print(f"   ✅ Map Ready ({len(sd_to_gsis)} players).")
    except Exception as e:
        print(f"   ❌ Error loading ID Map: {e}")
        return

    # 4. Process Props (Pivot Logic)
    player_data_cache = {}

    for prop in raw_props:
        sd_id = str(prop.get("PlayerID"))
        gsis_id = sd_to_gsis.get(sd_id)
        
        if not gsis_id: continue 

        # Initialize player entry
        if gsis_id not in player_data_cache:
            player_data_cache[gsis_id] = {
                "player_id": gsis_id,
                "player_name": prop.get("Name"),
                "team": prop.get("Team"),
                "week": current_week,
                "season": SEASON,
                # Initialize all prop keys to None
                **{k: None for k in schema_cols.keys() if k not in ["player_id", "player_name", "team", "week", "season"]}
            }

        # Extract Line (Value) and Odds (Payout)
        desc = prop.get("Description", "")
        line_value = prop.get("OverUnder")   # The actual yards/count (e.g., 255.5)
        payout = prop.get("OverPayout")      # The odds (e.g., -110)
        
        if line_value is None and desc != "Anytime Touchdown": continue

        # Helper to set both Line and Prob
        def set_prop(key_base):
            player_data_cache[gsis_id][key_base] = float(line_value)
            player_data_cache[gsis_id][f"{key_base}_prob"] = calculate_implied_prob(payout)

        # Map Descriptions to Columns
        if desc == "Passing Yards": set_prop("prop_passing_yards")
        elif desc == "Passing Touchdowns": set_prop("prop_passing_tds")
        elif desc == "Passing Attempts": set_prop("prop_pass_attempts")
        elif desc == "Rushing Yards": set_prop("prop_rushing_yards")
        elif desc == "Rushing Attempts": set_prop("prop_rush_attempts")
        elif desc == "Receiving Yards": set_prop("prop_receiving_yards")
        elif desc == "Receptions": set_prop("prop_receptions")
        elif desc == "Anytime Touchdown": 
            # ATD usually doesn't have a line (it's binary), so we assume 0.5 or just use prob
            player_data_cache[gsis_id]["prop_anytime_td"] = 1.0 
            player_data_cache[gsis_id]["prop_anytime_td_prob"] = calculate_implied_prob(payout)
        elif desc == "Touchdowns": # Sometimes labeled differently
            player_data_cache[gsis_id]["prop_anytime_td"] = 1.0
            player_data_cache[gsis_id]["prop_anytime_td_prob"] = calculate_implied_prob(payout)

    # 5. Calculate Total Yards & Finalize List
    final_rows = []
    for p_data in player_data_cache.values():
        # Calc Total Yards (Rush + Rec)
        rush = p_data["prop_rushing_yards"] or 0.0
        rec = p_data["prop_receiving_yards"] or 0.0
        
        # Only set if meaningful
        if rush > 0 or rec > 0:
            p_data["prop_total_yards"] = rush + rec
            # Estimate probability (average of the two available probs as a rough heuristic)
            p_rush = p_data["prop_rushing_yards_prob"]
            p_rec = p_data["prop_receiving_yards_prob"]
            
            if p_rush and p_rec: p_data["prop_total_yards_prob"] = (p_rush + p_rec) / 2
            elif p_rush: p_data["prop_total_yards_prob"] = p_rush
            elif p_rec: p_data["prop_total_yards_prob"] = p_rec

        final_rows.append(p_data)

    # 6. Save
    if final_rows:
        df = pl.DataFrame(final_rows)
        # Ensure correct column types/order based on schema
        df = df.select([pl.col(c).cast(t) for c, t in schema_cols.items() if c in df.columns])
        
        df.write_csv(OUTPUT_FILE)
        print(f"✅ Saved {len(df)} records with PROBS and ACTUALS to {OUTPUT_FILE}")
        
        # Sample Debug
        sample = final_rows[0]
        print(f"   🔎 Sample ({sample['player_name']}):")
        print(f"      Pass Yds: {sample['prop_passing_yards']} (Prob: {sample['prop_passing_yards_prob']}%)")
        print(f"      Rush Yds: {sample['prop_rushing_yards']} (Prob: {sample['prop_rushing_yards_prob']}%)")
    else:
        print("   ⚠️ No matching props found.")

if __name__ == "__main__":
    main()