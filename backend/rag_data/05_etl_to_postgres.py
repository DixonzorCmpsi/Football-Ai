# rag_data/05_etl_to_postgres.py
import sys
import os
import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import subprocess
from datetime import datetime

# --- Configuration ---
load_dotenv()

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    print("Error: DB_CONNECTION_STRING not found in environment variables.")
    sys.exit(1)

# --- Dynamic Season Logic ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: 
        return now.year
    else: 
        return now.year - 1

SEASON = get_current_season()
print(f"Dynamic Season Detected: {SEASON}")

# --- Pipeline Configuration ---
PIPELINE_STEPS = [
    {
        "script": "01_create_static_files.py",
        "uploads": [
            ("player_profiles.csv", "player_profiles", "replace"),
            (f"schedule_{SEASON}.csv", "schedule", "smart_append")
        ]
    },
    {
        "script": "03_create_defense_file.py",
        "uploads": [
             (f"weekly_defense_stats_{SEASON}.csv", "weekly_defense_stats", "smart_append")
        ]
    },
    {
        "script": "02_update_weekly_stats.py",
        "uploads": [
             (f"weekly_player_stats_{SEASON}.csv", "weekly_player_stats", "smart_append"),
             (f"weekly_offense_stats_{SEASON}.csv", "weekly_offense_stats", "smart_append")
        ]
    },
    {
        "script": "04_update_snap_counts.py",
        "uploads": [
             (f"weekly_snap_counts_{SEASON}.csv", "weekly_snap_counts", "smart_append")
        ]
    },
    {
        "script": "injuries_stats.py",
        "uploads": [ 
                (f"weekly_injury_stats_{SEASON}.csv", "weekly_injury_stats", "smart_append")

            ]
    },
    # --- NEW: Refresh Models Step ---
    {
        "script": "refresh_models.py", 
        "uploads": [] # Maintenance script, no DB upload
    },
    # --- weekly odds update ---
    {
        "script": "07_update_odds.py",
        "uploads": [
             (f"weekly_player_odds_{SEASON}.csv", "weekly_player_odds", "smart_append")
        ]   
    }
    # # --- Rankings Generation ---
    # {
    #     "script": "06_generate_rankings.py",
    #     "uploads": [
    #          ("weekly_rankings.csv", "weekly_rankings", "smart_append")
    #     ]
    # }
]

# --- Helpers ---
def get_db_engine():
    return create_engine(DB_CONNECTION_STRING)

def run_external_script(script_name):
    """Runs a Python script located in the same directory."""
    print(f"\n" + "="*50)
    print(f">> EXECUTING: {script_name}")
    print("="*50)
    
    current_dir = Path(__file__).parent
    script_path = current_dir / script_name
    
    if not script_path.exists():
        print(f"[ERROR] Script {script_name} not found at {script_path}")
        return False

    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
        print(f"[OK] FINISHED: {script_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[FAILED] {script_name} exited with code {e.returncode}")
        return False
    except Exception as e:
        print(f"[EXCEPTION] {e}")
        return False

def push_to_postgres(file_name, table_name, mode, engine):
    """Reads a CSV and pushes it to Postgres."""
    file_path = Path(__file__).parent / file_name
    
    if not file_path.exists():
        print(f"Warning: Output file {file_name} not found. Skipping upload.")
        return

    print(f"   -> Uploading {file_name} to table '{table_name}' ({mode})...")
    
    try:
        df = pl.read_csv(file_path, ignore_errors=True)
        
        if mode == 'replace':
            df.to_pandas().to_sql(table_name, engine, if_exists='replace', index=False)
            
        elif mode == 'smart_append':
            if 'week' in df.columns and 'season' in df.columns:
                weeks = df['week'].unique().to_list()
                if weeks:
                    weeks_str = ",".join(map(str, weeks))
                    delete_query = text(f"DELETE FROM {table_name} WHERE season = {SEASON} AND week IN ({weeks_str})")
                    with engine.connect() as conn:
                        conn.execute(delete_query)
                        conn.commit()
                    print(f"      - Cleared old data for weeks {weeks}")
            
            df.to_pandas().to_sql(table_name, engine, if_exists='append', index=False)

        print(f"      - Success! {len(df)} rows uploaded.")

    except Exception as e:
        print(f"[ERROR] uploading {table_name}: {e}")

# --- Main Execution Loop ---
def main():
    print("Starting Master ETL Orchestrator...")
    engine = get_db_engine()
    
    for step in PIPELINE_STEPS:
        script = step["script"]
        uploads = step["uploads"]
        
        success = run_external_script(script)
        
        if success:
            for csv_file, table, mode in uploads:
                push_to_postgres(csv_file, table, mode, engine)
        else:
            print(f"Skipping uploads for {script} due to failure.")

    feature_path = Path(__file__).parent.parent / "dataPrep" / "featured_dataset.csv"
    if feature_path.exists():
        print("\n--- Uploading Historical Features ---")
        try:
            df_feat = pl.read_csv(feature_path, ignore_errors=True)
            df_feat.to_pandas().to_sql('featured_dataset', engine, if_exists='replace', index=False)
            print("[OK] featured_dataset uploaded.")
        except Exception as e:
            print(f"[ERROR] uploading features: {e}")

    print("\n" + "="*50)
    print("ETL PIPELINE COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()