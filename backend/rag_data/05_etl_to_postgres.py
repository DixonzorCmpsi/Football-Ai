import sys
import os
import polars as pl
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from pathlib import Path
import subprocess
from datetime import datetime

# --- Configuration ---
load_dotenv()

# --- WINDOWS CONSOLE FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    print("Error: DB_CONNECTION_STRING not found in environment variables.")
    sys.exit(1)

def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1

SEASON = get_current_season()
print(f"Dynamic Season Detected: {SEASON}")

# --- Helper: Check if Schema Matches ---
def check_schema_match(file_path_str, table_name, engine):
    """
    Returns True if:
    1. Local file exists.
    2. DB Table exists.
    3. DB Table columns match CSV columns (Schema Check).
    """
    current_dir = Path(__file__).parent
    file_path = (current_dir / file_path_str).resolve() if file_path_str.startswith("../") else current_dir / file_path_str

    if not file_path.exists():
        return False

    try:
        insp = inspect(engine)
        if not insp.has_table(table_name):
            return False 

        # Read header only
        df_head = pl.read_csv(file_path, n_rows=0)
        csv_cols = set(df_head.columns)
        db_cols = set([col['name'] for col in insp.get_columns(table_name)])
        
        # If CSV cols are inside DB cols, we are good.
        if csv_cols.issubset(db_cols):
            return True 
        else:
            return False

    except Exception as e:
        return False

# --- Pipeline Configuration ---
PIPELINE_STEPS = [
    # 1. FOUNDATION (Static Files)
    {
        "script": "01_create_static_files.py",
        "uploads": [
            (f"player_profiles_{SEASON}.csv", "player_profiles", "replace"), # Profiles schema might update
            (f"schedule_{SEASON}.csv", "schedule", "replace") 
        ]
    },
    # 2. HISTORICAL DATA
    {
        "script": "09_upload_training_data.py", 
        "uploads": [
            ("../dataPrep/data/yearly_player_stats_offense.csv", "training_player_yearly", "if_missing"),
            ("../dataPrep/data/weekly_player_stats_offense.csv", "training_player_weekly", "if_missing"),
            ("../dataPrep/data/weekly_team_stats_offense.csv", "training_team_offense", "if_missing"),
            ("../dataPrep/data/weekly_team_stats_defense.csv", "training_team_defense", "if_missing")
        ]
    },
    # 3. CURRENT SEASON CONTEXT
    {
        "script": "03_create_defense_file.py",
        "uploads": [
             (f"weekly_defense_stats_{SEASON}.csv", f"weekly_defense_stats_{SEASON}", "smart_append"),
             (f"weekly_offense_stats_{SEASON}.csv", f"weekly_offense_stats_{SEASON}", "smart_append"),
             (f"schedule_{SEASON}.csv", "schedule", "replace") 
        ]
    },
    # 4. CURRENT SEASON PLAYER STATS
    {
        "script": "02_update_weekly_stats.py",
        "uploads": [
             (f"weekly_player_stats_{SEASON}.csv", f"weekly_player_stats_{SEASON}", "smart_append")
        ]
    },
    # 5. ENRICHMENT
    {
        "script": "04_update_snap_counts.py",
        "uploads": [
             (f"weekly_snap_counts_{SEASON}.csv", f"weekly_snap_counts_{SEASON}", "smart_append")
        ]
    },
    {
        "script": "08_update_injuries.py",
        "uploads": [
             (f"weekly_injuries_{SEASON}.csv", f"weekly_injuries_{SEASON}", "smart_append")
        ]
    },
    # 6. FEATURE ENGINEERING (DB CENTRIC)
    # New Architecture: Script 13 pulls from DB -> Computes -> Writes to DB
    # We include the upload step here just as a backup for the local CSV artifact
    {
        "script": "13_generate_production_features.py", 
        "uploads": [
            (f"weekly_feature_set_{SEASON}.csv", f"weekly_feature_set_{SEASON}", "replace") 
        ]
    },
    # 7. MODELING DATASET (Training Prep)
    {
        "script": "../dataPrep/build_modeling_dataset.py", 
        "smart_check_file": "../dataPrep/weekly_modeling_dataset.csv",
        "smart_check_table": "modeling_dataset",
        "uploads": [
            ("../dataPrep/weekly_modeling_dataset.csv", "modeling_dataset", "replace") 
        ]
    },
    # 8. FEATURE ENGINEERING (Training Prep)
    {
        "script": "../dataPrep/feature_engineering.py", 
        "smart_check_file": "../dataPrep/featured_dataset.csv",
        "smart_check_table": "featured_dataset",
        "uploads": [
            ("../dataPrep/featured_dataset.csv", "featured_dataset", "replace") 
        ]
    },
   # 10. BOVADA CRAWLER (Get Game URLs)
    {
        "script": "10_bovada_crawler.py",
        "uploads": [] 
    },

    # 11. BOVADA SCRAPER (Visit URLs -> Menu.json)
    {
        "script": "11_bovada_scraper.py",
        "uploads": []
    },
    {
        "script": "12_process_bovada.py",
        "uploads": [
             (f"weekly_bovada_game_lines_{SEASON}.csv", "bovada_game_lines", "replace"),
             (f"weekly_bovada_player_props_{SEASON}.csv", "bovada_player_props", "replace")
        ]
    },
    # 10. FINAL RANKINGS
    {
        "script": "06_generate_rankings.py",
        "uploads": [
             ("weekly_rankings.csv", "weekly_rankings", "smart_append")
        ]
    }
]

def get_db_engine(): return create_engine(DB_CONNECTION_STRING)

def run_external_script(step, engine):
    script_name = step["script"]
    
    # --- SMART SKIP LOGIC ---
    if "smart_check_file" in step and "smart_check_table" in step:
        check_file = step["smart_check_file"]
        check_table = step["smart_check_table"]
        print(f"\n🔍 Checking if we can skip {script_name}...")
        if check_schema_match(check_file, check_table, engine):
            print(f"   ✅ Schema Match & File Exists ({check_file}). SKIPPING execution.")
            return False 
        else:
            print(f"   ⚠️ Schema mismatch or file missing. PROCEEDING with execution.")

    print(f"\n" + "="*50 + f"\n🚀 EXECUTING: {script_name}\n" + "="*50)
    current_dir = Path(__file__).parent
    script_path = (current_dir / script_name).resolve() if script_name.startswith("../") else current_dir / script_name
    
    if not script_path.exists():
        print(f"❌ Error: Script {script_name} not found.")
        return False

    try:
        subprocess.run([sys.executable, str(script_path)], check=True, cwd=script_path.parent)
        print(f"✅ FINISHED: {script_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ FAILED: {script_name} code {e.returncode}")
        return False

def reset_db_table(table_name, engine):
    """
    NUCLEAR OPTION: Drops and recreates a table if the schema is wrong.
    """
    print(f"      ☢️  RESETTING table '{table_name}' (Dropping & Recreating)...")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.commit()
    except Exception as e:
        print(f"      ❌ Error dropping table: {e}")

def push_to_postgres(file_path_str, table_name, mode, engine, skipped=False):
    current_dir = Path(__file__).parent
    file_path = (current_dir / file_path_str).resolve() if file_path_str.startswith("../") else current_dir / file_path_str
    
    if skipped:
        print(f"   -> Skipping upload for {table_name} (Smart Check Passed).")
        return

    if not file_path.exists():
        print(f"⚠️ Warning: Output file {file_path_str} not found. Skipping upload.")
        return

    print(f"   -> Uploading {file_path.name} to '{table_name}' ({mode})...")
    
    try:
        df = pl.read_csv(file_path, ignore_errors=True)
        
        # --- SELF-HEALING: CHECK FOR SCHEMA MISMATCH ---
        try:
            insp = inspect(engine)
            if insp.has_table(table_name):
                db_cols = set([col['name'] for col in insp.get_columns(table_name)])
                csv_cols = set(df.columns)
                
                if not csv_cols.issubset(db_cols):
                    print(f"      ⚠️ Schema Mismatch! CSV has new columns. Resetting '{table_name}'...")
                    reset_db_table(table_name, engine)
                    mode = 'replace' 
        except Exception: 
            pass 

        # --- MODE: IF MISSING ---
        if mode == 'if_missing':
            with engine.connect() as conn:
                exists = conn.execute(text(f"SELECT to_regclass('public.{table_name}')")).scalar()
                if exists and conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).fetchone():
                    print(f"      - Table exists & populated. SKIPPING.")
                    return
            df.to_pandas().to_sql(table_name, engine, if_exists='append', index=False)
            print(f"      - Success! Initial load complete.")
            return

        # --- MODE: REPLACE ---
        if mode == 'replace':
            df.to_pandas().to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"      - Success! Replaced table.")
            
        # --- MODE: SMART APPEND ---
        elif mode == 'smart_append':
            try:
                with engine.connect() as conn:
                    if 'week' in df.columns and 'season' in df.columns:
                        weeks = ",".join(map(str, df['week'].unique().to_list()))
                        conn.execute(text(f"DELETE FROM {table_name} WHERE season = {SEASON} AND week IN ({weeks})"))
                        conn.commit()
                    elif 'game_date' in df.columns:
                        dates = ",".join([f"'{d}'" for d in df['game_date'].unique().to_list()])
                        conn.execute(text(f"DELETE FROM {table_name} WHERE game_date IN ({dates})"))
                        conn.commit()
                    elif 'season' in df.columns:
                        conn.execute(text(f"DELETE FROM {table_name} WHERE season = {SEASON}"))
                        conn.commit()
                
                df.to_pandas().to_sql(table_name, engine, if_exists='append', index=False)
                print(f"      - Success! Appended {len(df)} rows.")

            except Exception as e:
                # FAIL-SAFE: If Append crashes due to schema (UndefinedColumn), Reset and Replace
                error_str = str(e).lower()
                if "column" in error_str and "does not exist" in error_str:
                    print(f"      ⚠️ Append Failed (Schema Error). Resetting '{table_name}'...")
                    reset_db_table(table_name, engine)
                    df.to_pandas().to_sql(table_name, engine, if_exists='replace', index=False)
                    print(f"      - Success! Replaced table (Self-Healed).")
                elif "relation" in error_str and "does not exist" in error_str:
                     print("      - Table likely missing (creating new)...")
                     df.to_pandas().to_sql(table_name, engine, if_exists='append', index=False)
                     print(f"      - Success! Created table.")
                else:
                    print(f"      ❌ Append failed: {e}")

    except Exception as e:
        print(f"❌ ERROR: {e}")

def main():
    print("Starting Master ETL Orchestrator...")
    engine = get_db_engine()
    
    for step in PIPELINE_STEPS:
        ran_script = run_external_script(step, engine)
        was_skipped = not ran_script
        
        if "uploads" in step:
            for csv, table, mode in step["uploads"]:
                push_to_postgres(csv, table, mode, engine, skipped=was_skipped)

    print("\n" + "="*50)
    print("🎉 ETL PIPELINE COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()