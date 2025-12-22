import polars as pl
import nflreadpy as nfl
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# --- Configuration ---
load_dotenv()

# Suppress nflreadpy warnings about 404s (Known issue for 2025 injury report)
logging.getLogger("nflreadpy").setLevel(logging.CRITICAL)

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
OUTPUT_FILE = f"weekly_injuries_{SEASON}.csv"

def to_polars_safe(data):
    """Robustly converts data to Polars DataFrame."""
    if data is None: return pl.DataFrame()
    if isinstance(data, pl.DataFrame): return data
    if isinstance(data, pd.DataFrame): return pl.from_pandas(data)
    if hasattr(data, "to_polars"): return data.to_polars()
    try: return pl.from_pandas(pd.DataFrame(data))
    except: return pl.DataFrame(data)

def main():
    print(f"--- 🏥 Updating Injury Data ({SEASON}) ---")
    final_df = pl.DataFrame()
    
    # 1. Try Official Injury Report
    print(f"1. Checking official injury report source...")
    try:
        data = nfl.load_injuries(seasons=[SEASON])
        df = to_polars_safe(data)

        if not df.is_empty():
            max_week = df['week'].max()
            print(f"   ✅ Success! Found official data for Week {max_week}")
            
            final_df = df.filter(pl.col('week') == max_week).select([
                pl.col("gsis_id").alias("player_id"),
                pl.col("week"),
                pl.col("report_status").alias("injury_status")
            ]).with_columns(pl.lit(SEASON).alias("season"))
            
    except Exception:
        # We expect this for 2025, so we just print a clean message
        print(f"   ℹ️  Official report not published yet (Standard for {SEASON}).")

    # 2. Fallback to Roster Status
    if final_df.is_empty():
        print(f"2. Switching to Roster Data (Fallback)...")
        try:
            rosters = nfl.load_rosters_weekly(seasons=[SEASON])
            r_df = to_polars_safe(rosters)
            
            if not r_df.is_empty():
                max_wk = r_df['week'].max()
                print(f"   ✅ Success! Using Roster Data from Week {max_wk}")
                
                # Logic: Map raw roster status to cleaner injury terms
                final_df = r_df.filter(pl.col('week') == max_wk).select([
                    pl.col("gsis_id").alias("player_id"),
                    pl.col("week"),
                    pl.col("status").alias("raw_status")
                ]).with_columns([
                    pl.lit(SEASON).alias("season"),
                    pl.when(pl.col("raw_status") == "RES").then(pl.lit("IR"))
                      .when(pl.col("raw_status") == "NON").then(pl.lit("Out"))
                      .when(pl.col("raw_status") == "SUS").then(pl.lit("Suspended"))
                      .when(pl.col("raw_status") == "ACT").then(pl.lit("Healthy"))
                      .otherwise(pl.lit("Unknown")).alias("injury_status")
                ]).select(["player_id", "season", "week", "injury_status"])
                
        except Exception as e:
            print(f"   ❌ Roster fallback failed: {e}")

    # 3. Save
    if not final_df.is_empty():
        final_df = final_df.unique(subset=["player_id"])
        final_df.write_csv(OUTPUT_FILE)
        print(f"💾 Saved {len(final_df)} rows to {OUTPUT_FILE}")
        print("✅ Injury update complete.")
    else:
        print("❌ Critical: No injury data found from any source.")

if __name__ == "__main__":
    main()