import sys
import os
import shutil
import polars as pl
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from pathlib import Path
import subprocess
from datetime import datetime
import logging

# --- Configuration ---
load_dotenv()
logger = logging.getLogger(__name__)

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
logger.info(f"Dynamic Season Detected: {SEASON}")

# --- Per-file schema overrides (Polars) ---
SCHEMA_OVERRIDES_BY_FILE = {
    # defense CSV sometimes contains fractional values for two-point attempt averages
    "weekly_team_stats_defense.csv": {"average_defensive_two_point_attempt": pl.Float64}
}

# --- Helper: Check if Schema Matches --

def check_schema_match(file_path_str, table_name, engine):
    """
    Returns True if:
    1. Local file exists.
    2. DB Table exists.
    3. DB Table columns match CSV columns.
    4. NEW: DB Table is NOT empty.
    """
    current_dir = Path(__file__).parent
    file_path = (current_dir / file_path_str).resolve() if file_path_str.startswith("../") else current_dir / file_path_str

    if not file_path.exists():
        return False

    try:
        insp = inspect(engine)
        if not insp.has_table(table_name):
            return False 

        # --- Check if table is empty ---
        with engine.connect() as conn:
            # Efficient check: If at least one row exists, result will not be None
            # Using SELECT 1 ... LIMIT 1 is faster than COUNT(*) on large tables
            result = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).fetchone()
            if result is None:
                logger.warning(f"Table '{table_name}' exists but is EMPTY.")
                return False

        # Read header only for schema check
        df_head = pl.read_csv(file_path, n_rows=0)
        csv_cols = set(df_head.columns)
        db_cols = set([col['name'] for col in insp.get_columns(table_name)])
        
        return csv_cols.issubset(db_cols)

    except Exception as e:
        logger.exception(f"Error checking schema/emptiness for {table_name}: {e}")
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
    # 2. HISTORICAL DATA (SKIPPED - Local Only)
    # {
    #     "script": "09_upload_training_data.py", 
    #     "uploads": [
    #         ("../dataPrep/data/yearly_player_stats_offense.csv", "training_player_yearly", "if_missing"),
    #         ("../dataPrep/data/weekly_player_stats_offense.csv", "training_player_weekly", "if_missing"),
    #         ("../dataPrep/data/weekly_team_stats_offense.csv", "training_team_offense", "if_missing"),
    #         ("../dataPrep/data/weekly_team_stats_defense.csv", "training_team_defense", "if_missing")
    #     ]
    # },
    
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
    # 7. MODELING DATASET (Training Prep) (SKIPPED - Local Only)
    # {
    #     "script": "../dataPrep/build_modeling_dataset_avg.py", 
    #     "smart_check_file": "../dataPrep/weekly_modeling_dataset_avg.csv",
    #     "smart_check_table": "modeling_dataset",
    #     "uploads": [
    #         ("../dataPrep/weekly_modeling_dataset_avg.csv", "modeling_dataset", "replace") 
    #     ]
    # },
    # 8. FEATURE ENGINEERING (Training Prep) (SKIPPED - Local Only)
    # {
    #     "script": "../dataPrep/feature_engineering_avg.py", 
    #     "smart_check_file": "../dataPrep/featured_dataset_avg.csv",
    #     "smart_check_table": "featured_dataset",
    #     "uploads": [
    #         ("../dataPrep/featured_dataset_avg.csv", "featured_dataset", "replace") 
    #     ]
    # },
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
        logger.info(f"Skipping upload for {table_name} (Smart Check Passed).")
        return

    if not file_path.exists():
        logger.warning(f"Output file {file_path_str} not found at {file_path}. Skipping upload.")
        return

    # Log file size for quick diagnostics
    try:
        size = file_path.stat().st_size
        print(f"      [DEBUG] Found {file_path.name} (size: {size} bytes)")
    except Exception as e:
        print(f"      [DEBUG] Error getting size for {file_path}: {e}")
        size = 0

    logger.info(f"Uploading {file_path.name} to '{table_name}' ({mode})...")

    # Helper: streaming COPY upload (low-memory)
    overrides = SCHEMA_OVERRIDES_BY_FILE.get(file_path.name, None)
    logger.debug(f"resolved overrides for {file_path.name}: {overrides}")
    if overrides:
        logger.info(f"Applying schema overrides for {file_path.name}: {list(overrides.keys())}")
    else:
        logger.info("No schema overrides configured for this file.")

    def fast_csv_upload(path, tbl, mode):
        insp = inspect(engine)
        table_exists = insp.has_table(tbl)

        # 1) If replacing OR table missing, create the table schema from a small sample
        if mode == 'replace' or not table_exists:
            logger.info(f"Creating/Resetting table '{tbl}' via small sample for COPY...")
            if overrides:
                sample = pl.read_csv(path, n_rows=1000, schema_overrides=overrides).to_pandas()
            else:
                sample = pl.read_csv(path, n_rows=1000).to_pandas()
            reset_db_table(tbl, engine)
            sample.to_sql(tbl, engine, if_exists='replace', index=False)
            # Truncate to avoid duplicates from sample when we COPY full file
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {tbl}"))
                conn.commit()

        # 2) Smart Append Logic (Delete overlaps)
        elif mode == 'smart_append' and table_exists:
            try:
                # Peek at columns first
                # We use read_csv with n_rows=0 to get schema quickly
                schema = pl.read_csv(path, n_rows=0).columns
                if 'season' in schema and 'week' in schema:
                    df_keys = pl.read_csv(path, columns=['season', 'week']).unique()
                    if not df_keys.is_empty():
                        season = df_keys['season'][0]
                        weeks = df_keys['week'].to_list()
                        if weeks:
                            with engine.connect() as conn:
                                weeks_str = ",".join(map(str, weeks))
                                conn.execute(text(f"DELETE FROM {tbl} WHERE season = {season} AND week IN ({weeks_str})"))
                                conn.commit()
                                logger.info(f"Smart Append: Deleted existing rows for season={season}, weeks={weeks}")
            except Exception as e:
                logger.warning(f"Smart Append pre-delete failed (continuing): {e}")

        # 3) Use COPY FROM STDIN to stream the CSV directly into Postgres
        logger.info('Streaming CSV into Postgres via COPY (low-memory)')
        conn = engine.raw_connection()
        try:
            cur = conn.cursor()
            with open(path, 'r', encoding='utf-8') as f:
                cur.copy_expert(f"COPY {tbl} FROM STDIN WITH CSV HEADER", f)
            conn.commit()
            logger.info(f"Success: Streamed file to {tbl}.")
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    try:
        # For very large files, avoid reading into memory and use streaming COPY
        # Lower threshold to 1MB to be safe
        if size > 1_000_000:
            print(f"      [DEBUG] Taking fast streaming path (size > 1MB)")
            try:
                import subprocess
                header = subprocess.check_output(['head', '-n', '1', str(file_path)], text=True).strip()
                cols = len(header.split(','))
                linecount = int(subprocess.check_output(['wc', '-l', str(file_path)], text=True).split()[0])
                rows = max(linecount - 1, 0)
            except Exception:
                rows = None
                cols = None
            print(f"      - Detected large file (>{1_000_000} bytes). Rows ~ {rows}, Cols ~ {cols}")

            fast_csv_upload(file_path, table_name, mode)
            print(f"      - Completed streaming upload for {rows} rows.")
            return

        # 1) Read CSV without silently dropping rows for small files
        try:
            if overrides:
                print(f"      - Reading CSV with schema_overrides: {list(overrides.keys())}")
                df = pl.read_csv(file_path, infer_schema_length=10000, schema_overrides=overrides)
            else:
                df = pl.read_csv(file_path, infer_schema_length=10000)
        except Exception as e:
            msg = str(e)
            print(f"      ⚠️ Polars read_csv failed: {msg}")
            # If Polars complains about parsing into i64, try re-reading with a Float override
            if 'could not parse' in msg and 'as dtype `i64`' in msg:
                import re
                m = re.search(r"at column '([^']+)'", msg)
                if m:
                    bad_col = m.group(1)
                    print(f"      ↪️ Detected problematic column '{bad_col}'. Retrying with Float override.")
                    local_overrides = dict(overrides or {})
                    local_overrides[bad_col] = pl.Float64
                    df = pl.read_csv(file_path, infer_schema_length=10000, schema_overrides=local_overrides)
                else:
                    print("      ↪️ Could not determine offending column, retrying with ignore_errors=True")
                    df = pl.read_csv(file_path, infer_schema_length=10000, ignore_errors=True)
            else:
                # Re-raise for other unexpected errors
                raise

        rows = df.height
        cols = df.width
        print(f"      - Parsed CSV: {rows} rows, {cols} cols")

        # 1a) Abort if empty
        if rows == 0:
            print(f"      ⚠️ DataFrame is empty. Aborting upload to '{table_name}' to avoid emptying the table.")
            return

        # 2) Hardened schema check
        try:
            insp = inspect(engine)
            if insp.has_table(table_name):
                db_cols = set([col['name'].lower() for col in insp.get_columns(table_name)])
                csv_cols = set([c.lower() for c in df.columns])
                if not csv_cols.issubset(db_cols):
                    new_cols = csv_cols - db_cols
                    print(f"      ⚠️ Schema Evolution Detected! New columns: {new_cols}")
                    print(f"      ☢️  Switching mode to 'replace' to update table schema.")
                    mode = 'replace'
        except Exception as e:
            print(f"      ⚠️ Schema check warning: {e}")

        # Convert once for pandas/to_sql (fallback)
        pdf = df.to_pandas()

        # MODE: IF MISSING
        if mode == 'if_missing':
            with engine.connect() as conn:
                exists = conn.execute(text(f"SELECT to_regclass('public.{table_name}')")).scalar()
                populated = False
                if exists:
                    populated = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).fetchone() is not None
                if exists and populated:
                    print(f"      - Table exists & populated. SKIPPING.")
                    return
            pdf.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)
            print(f"      - Success! Initial load complete ({rows} rows).")
            return

        # MODE: REPLACE
        if mode == 'replace':
            reset_db_table(table_name, engine)
            pdf.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=10000)
            print(f"      - Success! Replaced table with new schema ({rows} rows).")
            return

        # MODE: SMART APPEND
        elif mode == 'smart_append':
            try:
                with engine.connect() as conn:
                    # Delete only when we have keys and df is non-empty
                    if 'week' in df.columns and 'season' in df.columns:
                        weeks = df['week'].unique().to_list()
                        if len(weeks) > 0:
                            weeks_str = ",".join(map(str, weeks))
                            conn.execute(text(f"DELETE FROM {table_name} WHERE season = {SEASON} AND week IN ({weeks_str})"))
                            conn.commit()
                            print(f"      - Deleted existing rows for season={SEASON}, weeks={weeks}")
                    elif 'game_date' in df.columns:
                        dates = df['game_date'].unique().to_list()
                        if len(dates) > 0:
                            dates_str = ",".join([f"'{d}'" for d in dates])
                            conn.execute(text(f"DELETE FROM {table_name} WHERE game_date IN ({dates_str})"))
                            conn.commit()
                            print(f"      - Deleted existing rows for game_date in {dates}")
                    elif 'season' in df.columns:
                        conn.execute(text(f"DELETE FROM {table_name} WHERE season = {SEASON}"))
                        conn.commit()
                        print(f"      - Deleted existing rows for season={SEASON}")

                pdf.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)
                print(f"      - Success! Appended {rows} rows.")

            except Exception as e:
                print(f"      ❌ Smart Append Failed: {e}")
                print(f"      ☢️  Falling back to REPLACE strategy...")
                reset_db_table(table_name, engine)
                pdf.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"      - Success! Replaced table (Fail-safe) with {rows} rows.")

    except Exception as e:
        print(f"   ❌ Upload failed for {table_name}: {e}")
        return


def check_system_memory_and_swap():
    # Print quick summary of system memory and swap; recommend enabling swap if missing
    try:
        meminfo = {}
        with open('/proc/meminfo') as f:
            for line in f:
                k, v = line.split(':', 1)
                meminfo[k.strip()] = v.strip()
        mem_total_kb = int(meminfo.get('MemTotal', '0 kB').split()[0])
        swap_total_kb = int(meminfo.get('SwapTotal', '0 kB').split()[0])
        print(f"System Memory: {mem_total_kb//1024} MB; Swap: {swap_total_kb//1024} MB")
        if swap_total_kb == 0:
            print("⚠️ No swap detected. On low-memory VMs this can cause OOM kills during ETL. Consider adding swap (e.g. sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile)")
    except Exception as e:
        print(f"Could not read /proc/meminfo: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run ETL pipeline or a subset of steps")
    parser.add_argument("--only-scripts", nargs="+", help="Only run these script basenames (e.g. 09_upload_training_data.py or 09_upload_training_data)")
    parser.add_argument("--skip-scripts", nargs="+", help="Skip these script basenames entirely")
    parser.add_argument("--import-data", type=str, help="Path to a folder containing CSVs to import (skips generation if found)")
    args = parser.parse_args()

    only = set()
    skip = set()
    if args.only_scripts:
        for s in args.only_scripts:
            only.add(s if s.endswith('.py') else f"{s}.py")
    if args.skip_scripts:
        for s in args.skip_scripts:
            skip.add(s if s.endswith('.py') else f"{s}.py")
            
    import_dir = Path(args.import_data).resolve() if args.import_data else None
    if import_dir and not import_dir.exists():
        print(f"❌ Error: Import directory not found: {import_dir}")
        sys.exit(1)

    print("Starting Master ETL Orchestrator...")
    check_system_memory_and_swap()
    engine = get_db_engine()
    
    # If user passed --only-scripts, filter pipeline steps accordingly
    steps_to_run = PIPELINE_STEPS
    if only:
        print(f"⚙️ Running only these scripts: {sorted(list(only))}")
        steps_to_run = [s for s in PIPELINE_STEPS if s["script"] in only]

    if skip:
        print(f"⚙️ Skipping these scripts: {sorted(list(skip))}")
        steps_to_run = [s for s in steps_to_run if s["script"] not in skip]

    current_dir = Path(__file__).parent

    for step in steps_to_run:
        script_needed = True
        was_skipped = False
        
        # --- IMPORT LOGIC ---
        if import_dir and "uploads" in step:
            # Check if all artifacts for this step exist in the import folder
            all_artifacts_found = True
            for csv_name, _, _ in step["uploads"]:
                src = import_dir / csv_name
                if not src.exists():
                    all_artifacts_found = False
                    break
            
            if all_artifacts_found:
                print(f"\n📦 Found artifacts for {step['script']} in import folder. Importing...")
                for csv_name, _, _ in step["uploads"]:
                    src = import_dir / csv_name
                    dst = current_dir / csv_name
                    try:
                        shutil.copy2(src, dst)
                        print(f"   - Copied {csv_name} to local workspace")
                    except Exception as e:
                        print(f"   ❌ Failed to copy {csv_name}: {e}")
                
                script_needed = False # Skip generation since we have the data
                was_skipped = False   # Force upload (don't treat as skipped)
            else:
                print(f"   ⚠️ Import requested but artifacts missing for {step['script']}. Falling back to generation.")

        if script_needed:
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