import polars as pl
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))

def get_current_season():
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = os.path.join(current_dir, f"weekly_injuries_{SEASON}.csv")
PROFILES_FILE = os.path.join(current_dir, f"player_profiles_{SEASON}.csv")

# --- FETCH SLEEPER DATA ---
def fetch_sleeper_map():
    print("   ☁️  Fetching Sleeper API (Real-time Injuries)...")
    try:
        resp = requests.get("https://api.sleeper.app/v1/players/nfl")
        if resp.status_code != 200: return None
        
        data = resp.json()
        records = []
        
        for pid, info in data.items():
            # We only care about players with a GSIS ID (to link to our data)
            gsis_id = info.get('gsis_id')
            if gsis_id:
                # Logic to determine status
                status = info.get('status')          # e.g. "Active", "Injured Reserve"
                inj_status = info.get('injury_status') # e.g. "Questionable", "Out"
                
                final_status = "Active"
                
                # Priority 1: IR/PUP (Fixes Mahomes)
                if status == "Injured Reserve" or status == "PUP":
                    final_status = "IR"
                # Priority 2: Game Designation (Fixes Kittle if he has Q/D/O tag)
                elif inj_status:
                    final_status = inj_status
                # Priority 3: Inactive/Suspended
                elif status == "Inactive":
                    final_status = "Inactive"
                elif status == "Suspended":
                    final_status = "Suspended"

                records.append({
                    "player_id": str(gsis_id), # Keep "00-" format
                    "sleeper_status": final_status
                })
        
        if not records: return None
        
        # Return as DataFrame for joining
        return pl.DataFrame(records).unique(subset=["player_id"])
        
    except Exception as e:
        print(f"      ❌ Sleeper Fetch Error: {e}")
        return None

def main():
    print(f"--- 🏥 Updating Injury Data ({SEASON}) ---")
    
    # 1. Load Master List (Player Profiles)
    if not os.path.exists(PROFILES_FILE):
        print(f"❌ Critical: Profiles file not found at {PROFILES_FILE}")
        return

    # Load only IDs and Names to be lightweight
    try:
        df_profiles = pl.read_csv(PROFILES_FILE).select(["player_id", "player_name"]).unique(subset=["player_id"])
        print(f"   📋 Loaded {len(df_profiles)} players from Master Profile List.")
    except Exception as e:
        print(f"❌ Error reading profiles: {e}")
        return

    # 2. Fetch Sleeper Injury Data
    df_sleeper = fetch_sleeper_map()
    
    if df_sleeper is not None:
        # 3. PERFORM LEFT JOIN
        # This guarantees EVERY ID in profiles is present in the final file
        final_df = df_profiles.join(df_sleeper, on="player_id", how="left")
        
        # 4. Fill Missing Statuses with "Active"
        # If Sleeper didn't have data for a profile ID, we assume they are Active
        final_df = final_df.with_columns(
            pl.col("sleeper_status").fill_null("Active").alias("injury_status")
        )
        
        # 5. Save Clean File
        # Format: player_id, player_name, injury_status
        final_df = final_df.select(["player_id", "player_name", "injury_status"])
        
        final_df.write_csv(OUTPUT_FILE)
        print(f"   💾 Saved {len(final_df)} injury records to {OUTPUT_FILE}")
        
        # Verification
        print("\n   👀 Verification Check:")
        # Check specific stars
        stars = ["00-0033873", "00-0033906"] # Mahomes, Kittle
        check = final_df.filter(pl.col("player_id").is_in(stars))
        for row in check.iter_rows(named=True):
            print(f"      - {row['player_name']} ({row['player_id']}): {row['injury_status']}")
            
    else:
        print("   ❌ Failed to fetch Sleeper data. Skipping update.")

if __name__ == "__main__":
    main()