import polars as pl
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import nflreadpy as nfl

# --- Configuration ---
load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))

def get_current_season():
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = os.path.join(current_dir, f"weekly_injuries_{SEASON}.csv")
PROFILES_FILE = os.path.join(current_dir, f"player_profiles_{SEASON}.csv")

DEBUG_PLAYERS = ["judkins", "jeanty", "burden", "mccaffrey", "nacua"] 

def normalize(name):
    return str(name).lower().replace(".", "").replace("'", "").strip()

def fetch_sleeper_map(name_to_id_map):
    print("   ☁️  Fetching Sleeper API (Real-time Injuries)...")
    try:
        resp = requests.get("https://api.sleeper.app/v1/players/nfl")
        if resp.status_code != 200: return None
        data = resp.json()
        records = []
        
        print(f"      - Scanned {len(data)} players from Sleeper.")
        
        for sleeper_id, info in data.items():
            full_name = info.get('full_name', 'Unknown')
            gsis_id = info.get('gsis_id')
            
            # Fallback ID Match
            if not gsis_id:
                clean_name = normalize(full_name)
                if clean_name in name_to_id_map:
                    gsis_id = name_to_id_map[clean_name]

            if not gsis_id: continue

            # Status Logic
            status = info.get('status')
            inj_status = info.get('injury_status')
            final_status = "Active"
            
            if status == "Injured Reserve" or status == "PUP": final_status = "IR"
            elif inj_status: final_status = inj_status
            elif status == "Inactive": final_status = "Inactive"
            elif status == "Suspended": final_status = "Suspended"

            if any(d in normalize(full_name) for d in DEBUG_PLAYERS):
                print(f"      🔎 FOUND {full_name}: {final_status}")

            records.append({"player_id": str(gsis_id), "sleeper_status": final_status})
        
        if not records: return None
        return pl.DataFrame(records).unique(subset=["player_id"])
        
    except Exception as e:
        print(f"      ❌ Sleeper Fetch Error: {e}")
        return None

def main():
    print(f"--- 🏥 Updating Injury History ({SEASON}) ---")
    
    # 1. Load Profiles
    if not os.path.exists(PROFILES_FILE):
        print("❌ Profiles file missing.")
        return
    df_profiles = pl.read_csv(PROFILES_FILE).select(["player_id", "player_name"]).unique(subset=["player_id"])
    name_to_id = {normalize(row['player_name']): row['player_id'] for row in df_profiles.iter_rows(named=True)}

    # 2. Fetch Current Data
    df_sleeper = fetch_sleeper_map(name_to_id)
    
    if df_sleeper is not None:
        # Join & Fill
        current_data = df_profiles.join(df_sleeper, on="player_id", how="left")
        current_data = current_data.with_columns(pl.col("sleeper_status").fill_null("Active").alias("injury_status"))
        
        # 3. TAG WITH CURRENT WEEK
        try:
            target_week = nfl.get_current_week()
            if target_week == 0: target_week = 18 # Offseason fallback
        except: target_week = 18
        
        print(f"   📅 Tagging snapshot as: Week {target_week}")
        current_data = current_data.with_columns(pl.lit(target_week).alias("week"))
        current_data = current_data.select(["player_id", "player_name", "injury_status", "week"])

        # 4. APPEND TO HISTORY (The Time Machine Logic)
        final_df = current_data
        
        if os.path.exists(OUTPUT_FILE):
            print("   🔄 Reading existing history...")
            try:
                history_df = pl.read_csv(OUTPUT_FILE)
                
                # Check if we already have data for this week
                # If so, DELETE IT (replace it with the fresh fetch)
                # This allows you to re-run the script 5 times on Tuesday without creating 5 duplicates
                history_df = history_df.filter(pl.col("week") != target_week)
                
                # Stack History + New Data
                final_df = pl.concat([history_df, current_data])
                print(f"   📚 Merged with history. Total Records: {len(final_df)}")
                
            except Exception as e:
                print(f"   ⚠️ Could not read history ({e}). Overwriting.")

        # 5. Save
        final_df.write_csv(OUTPUT_FILE)
        print(f"   💾 Saved to {OUTPUT_FILE}")
        
    else:
        print("   ❌ Failed to fetch data.")

if __name__ == "__main__":
    main()