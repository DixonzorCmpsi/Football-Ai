import sys
import os
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

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    logger.error("DB_CONNECTION_STRING not found.")
    sys.exit(1)

def get_current_season():
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

SEASON = get_current_season()
ENGINE = create_engine(DB_CONNECTION_STRING)

# --- The Step Registry ---
# Map script names to their expected CSV outputs and upload modes
STEP_MAP = {
    "01_create_static_files.py": [(f"player_profiles_{SEASON}.csv", "player_profiles", "replace")],
    "02_update_weekly_stats.py": [(f"weekly_player_stats_{SEASON}.csv", f"weekly_player_stats_{SEASON}", "smart_append")],
    "03_create_defense_file.py": [(f"weekly_defense_stats_{SEASON}.csv", f"weekly_defense_stats_{SEASON}", "smart_append"), 
                                   (f"weekly_offense_stats_{SEASON}.csv", f"weekly_offense_stats_{SEASON}", "smart_append")],
    "04_update_snap_counts.py": [(f"weekly_snap_counts_{SEASON}.csv", f"weekly_snap_counts_{SEASON}", "smart_append")],
    "06_generate_rankings.py": [("weekly_rankings.csv", "weekly_rankings", "smart_append")],
    "08_update_injuries.py": [(f"weekly_injuries_{SEASON}.csv", f"weekly_injuries_{SEASON}", "smart_append")],
    "13_generate_production_features.py": [(f"weekly_feature_set_{SEASON}.csv", f"weekly_feature_set_{SEASON}", "replace")],
    "../dataPrep/build_modeling_dataset_avg.py": [("../dataPrep/weekly_modeling_dataset_avg.csv", "modeling_dataset", "replace")],
    "../dataPrep/feature_engineering_avg.py": [("../dataPrep/featured_dataset_avg.csv", "featured_dataset", "replace")],
    "12_process_bovada.py": [(f"weekly_bovada_game_lines_{SEASON}.csv", "bovada_game_lines", "replace"),
                             (f"weekly_bovada_player_props_{SEASON}.csv", "bovada_player_props", "replace")]
}

def push_to_postgres(file_path_str, table_name, mode):
    current_dir = Path(__file__).parent
    file_path = (current_dir / file_path_str).resolve()
    
    if not file_path.exists():
        logger.warning(f"Output file {file_path.name} not found.")
        return

    logger.info(f"Uploading {file_path.name} to '{table_name}'...")
    try:
        df = pl.read_csv(file_path, ignore_errors=True)
        # Drop and Recreate if 'replace'
        if mode == 'replace':
            with ENGINE.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()
            df.to_pandas().to_sql(table_name, ENGINE, if_exists='replace', index=False)
        else:
            df.to_pandas().to_sql(table_name, ENGINE, if_exists='append', index=False)
        logger.info(f"Success: {table_name} updated.")
    except Exception as e:
        logger.exception(f"Upload failed for {table_name}: {e}")

def main():
    if len(sys.argv) < 2:
        logger.info("Usage: python run_step.py <script_name>")
        logger.info("Example: python run_step.py 13_generate_production_features.py")
        return

    script_to_run = sys.argv[1]
    current_dir = Path(__file__).parent
    script_path = (current_dir / script_to_run).resolve()

    if not script_path.exists():
        print(f"❌ Script {script_to_run} not found.")
        return

    logger.info(f"RUNNING: {script_to_run}")
    try:
        subprocess.run([sys.executable, str(script_path)], check=True, cwd=script_path.parent)
        
        # Check if we have defined uploads for this script
        if script_to_run in STEP_MAP:
            for csv, table, mode in STEP_MAP[script_to_run]:
                push_to_postgres(csv, table, mode)
        else:
            logger.info("Script finished, but no DB upload mapped for this file.")
            
    except subprocess.CalledProcessError as e:
        logger.exception(f"Execution of {script_to_run} failed: {e}")

if __name__ == "__main__":
    main()