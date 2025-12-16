import polars as pl
import nflreadpy as nfl
import os
import sys
from dotenv import load_dotenv
load_dotenv()

# Setup paths
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
rag_dir = os.path.join(project_root, 'rag_data')
os.makedirs(rag_dir, exist_ok=True)

# Load Env
load_dotenv(os.path.join(project_root, 'applications', '.env'))
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not DB_CONNECTION_STRING:
    sys.exit("Error: DB_CONNECTION_STRING not found in .env")

CURRENT_SEASON = 2025

def main():
    print(f"--- 🏥 Updating Injury Data ({CURRENT_SEASON}) ---")
    
    final_df = pl.DataFrame()
    
    # --- STRATEGY 1: Official Injury Report ---
    print(f"1. Attempting to load official injury report...")
    try:
        data = nfl.load_injuries(seasons=[CURRENT_SEASON])
        
        # Handle different return types (Pandas vs Polars)
        if hasattr(data, "to_polars"): df = data.to_polars()
        elif isinstance(data, pl.DataFrame): df = data
        else: df = pl.from_pandas(data)

        if not df.is_empty():
            max_week = df['week'].max()
            print(f"   ✅ Success! Found data for Week {max_week}")
            final_df = df.filter(pl.col('week') == max_week).select([
                pl.col("gsis_id"),
                pl.col("report_status"), # 'Questionable', 'Out', etc.
            ])
    except Exception as e:
        print(f"   ⚠️ Official report unavailable: {e}")

    # --- STRATEGY 2: Fallback to Roster Status (If Strategy 1 Failed) ---
    if final_df.is_empty():
        print(f"2. Falling back to Weekly Roster status...")
        try:
            # Load rosters for the current season
            rosters = nfl.load_rosters_weekly(seasons=[CURRENT_SEASON])
            
            if hasattr(rosters, "to_polars"): r_df = rosters.to_polars()
            elif isinstance(rosters, pl.DataFrame): r_df = rosters
            else: r_df = pl.from_pandas(rosters)
            
            # Get latest week available in rosters
            max_wk = r_df['week'].max()
            print(f"   ✅ Using Roster Data from Week {max_wk}")
            
            latest_rosters = r_df.filter(pl.col('week') == max_wk)
            
            # Map roster 'status' to an injury-like label
            # ACT = Active, RES = IR/Injured, NON = NFI, SUS = Suspended
            # We create a new column 'report_status' based on 'status'
            final_df = latest_rosters.select([
                pl.col("gsis_id"),
                pl.col("status").alias("raw_status")
            ]).with_columns(
                pl.when(pl.col("raw_status") == "RES").then(pl.lit("IR"))
                  .when(pl.col("raw_status") == "NON").then(pl.lit("Out"))
                  .when(pl.col("raw_status") == "SUS").then(pl.lit("Suspended"))
                  .when(pl.col("raw_status") == "ACT").then(pl.lit("Active"))
                  .otherwise(pl.lit("Unknown"))
                  .alias("report_status")
            ).select(["gsis_id", "report_status"])
            
        except Exception as e:
            print(f"   ❌ Roster fallback failed: {e}")

    # --- SAVE & UPLOAD ---
    if not final_df.is_empty():
        # Drop duplicates (rosters might have dupes)
        final_df = final_df.unique(subset=["gsis_id"])
        
        # Save CSV
        csv_path = os.path.join(rag_dir, "weekly_injuries_{season}".format(season=CURRENT_SEASON) + ".csv")
        final_df.write_csv(csv_path)
        print(f"💾 Saved backup to: {csv_path}")

        # Upload
        print("📤 Uploading to database...")
        final_df.write_database(
            table_name="weekly_injuries_{season}".format(season=CURRENT_SEASON),
            connection=DB_CONNECTION_STRING,
            if_table_exists="replace",
            engine="sqlalchemy"
        )
        print(f"✅ Success! Updated 'weekly_injuries' with {len(final_df)} records.")
    else:
        print("❌ Critical: No player status data could be found from any source.")

if __name__ == "__main__":
    main()