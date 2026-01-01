
#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from typing import Dict, Tuple, List

import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR = SCRIPT_DIR.parent / "dataPrep" / "data"  # /app/dataPrep/data inside the container

TARGETS: Dict[str, Tuple[Path, str, str]] = {
    "training_player_yearly": (DATA_DIR / "yearly_player_stats_offense.csv", "training_player_yearly", "if_missing"),
    "training_player_weekly": (DATA_DIR / "weekly_player_stats_offense.csv", "training_player_weekly", "if_missing"),
    "training_team_offense":  (DATA_DIR / "weekly_team_stats_offense.csv",   "training_team_offense",  "if_missing"),
    "training_team_defense":  (DATA_DIR / "weekly_team_stats_defense.csv",   "training_team_defense",  "if_missing"),
}

SCHEMA_OVERRIDES_DEFENSE = {
    "average_defensive_two_point_attempt": pl.Float64,
}

DB_SCHEMA = "public"  # [NEW] force schema

def load_engine():
    load_dotenv()
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        print("❌ Error: DB_CONNECTION_STRING not found in environment variables (.env).", file=sys.stderr)
        sys.exit(1)
    engine = create_engine(conn_str)
    print(f"🔗 Using DB: {engine.url}")  # redacts pwd
    # [NEW] Log search_path to detect surprises
    try:
        with engine.connect() as conn:
            sp = conn.execute(text("SHOW search_path")).scalar()
            print(f"🧭 search_path: {sp}")
    except Exception as e:
        print(f"⚠️ Could not read search_path: {e}")
    return engine

def log_csv_stats(path: Path) -> None:
    try:
        size = path.stat().st_size
        print(f"     • File: {path} (size: {size} bytes)")
    except Exception:
        print(f"     • File: {path}")

def read_csv_robust(path: Path, key: str):
    try:
        overrides = SCHEMA_OVERRIDES_DEFENSE if key == "training_team_defense" else None
        df_pl = pl.read_csv(path, infer_schema_length=10000, schema_overrides=overrides)
        rows = df_pl.height
        cols = df_pl.width
        pdf = df_pl.to_pandas()
        print(f"      - Parsed CSV via Polars: {rows} rows, {cols} cols")
        return pdf, rows, cols
    except Exception as e:
        print(f"      ⚠️ Polars parse failed: {e}")
        print(f"      ↪️ Falling back to pandas.read_csv(low_memory=False)")
        pdf = pd.read_csv(path, low_memory=False)
        rows, cols = pdf.shape
        print(f"      - Parsed CSV via Pandas: {rows} rows, {cols} cols")
        return pdf, rows, cols

def table_row_count(engine, table_name: str) -> int:
    """[NEW] Count rows explicitly from public.<table>."""
    try:
        with engine.connect() as conn:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{table_name}")).scalar()
            return int(cnt or 0)
    except Exception as e:
        print(f"      ⚠️ Could not count rows in {DB_SCHEMA}.{table_name}: {e}")
        return -1  # signal unknown

def table_exists(engine, table_name: str) -> bool:
    """[NEW] Check table existence explicitly in public schema."""
    try:
        with engine.connect() as conn:
            exists = conn.execute(text("SELECT to_regclass(:regcls)"),
                                  {"regcls": f"{DB_SCHEMA}.{table_name}"}).scalar()
            return exists is not None
    except Exception as e:
        print(f"      ⚠️ to_regclass check failed for {DB_SCHEMA}.{table_name}: {e}")
        return False

def push_to_postgres(file_path: Path, table_name: str, mode: str, engine, key: str) -> None:
    if not file_path.exists():
        print(f"⚠️  Warning: File not found: {file_path}. Skipping '{table_name}'.")
        return

    print(f"   → Uploading {file_path.name} -> '{table_name}' (mode={mode}, schema={DB_SCHEMA})")
    log_csv_stats(file_path)

    try:
        pdf, rows, cols = read_csv_robust(file_path, key)

        if rows == 0:
            print(f"      ⚠️  DataFrame is empty. Aborting upload to '{table_name}'.")
            return

        exists = table_exists(engine, table_name)
        current_rows = table_row_count(engine, table_name) if exists else 0
        print(f"      - Pre-check: exists={exists}, row_count={current_rows}")

        if mode == "if_missing":
            if exists and current_rows > 0:
                print(f"      - Table exists & has {current_rows} rows. SKIPPING.")
                return
            # Create or append initial data to public schema
            pdf.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='append',
                       index=False, method='multi', chunksize=10000)
            print(f"      ✅ Initial load complete ({rows} rows).")
            return

        if mode == "replace":
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE"))
                conn.commit()
            pdf.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='replace',
                       index=False, method='multi', chunksize=10000)
            print(f"      ✅ Replaced table with {rows} rows.")
            return

        print(f"      ❓ Unknown mode '{mode}'. Skipping.")
    except Exception as e:
        print(f"      ❌ ERROR in push_to_postgres: {e}")

def parse_args() -> dict:
    import argparse
    parser = argparse.ArgumentParser(description="Upload historical training CSVs to Postgres safely.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", help="List available keys and resolved paths.")
    group.add_argument("--only", nargs="+", help="Upload only these keys (see --list).")
    parser.add_argument("--force-replace", nargs="*", default=[], help="Keys to force replace instead of if_missing.")  # [NEW]
    args = parser.parse_args()

    if args.list:
        print("--- Available keys ---")
        for k in sorted(TARGETS.keys()):
            path, table, mode = TARGETS[k]
            print(f"  - {k}: path={path}, table={table}, mode={mode}")
        sys.exit(0)

    selected = sorted(TARGETS.keys()) if not args.only else args.only
    invalid = [k for k in selected if k not in TARGETS]
    if invalid:
        print(f"❌ Invalid keys: {invalid}")
        print("Use --list to see available keys.")
        sys.exit(2)

    if not DATA_DIR.exists():
        print(f"❌ Error: Data directory not found at {DATA_DIR}")
        sys.exit(1)

    return {"selected": selected, "force_replace": set(args.force_replace)}

def main():
    print("--- 🚀 Uploading Historical Training Data ---")
    args = parse_args()
    selected_keys = args["selected"]
    force_replace = args["force_replace"]
    engine = load_engine()

    for key in selected_keys:
        path, table, mode = TARGETS[key]
        if key in force_replace:
            print(f"   ⚙️ Forcing mode=replace for key '{key}'")
            mode = "replace"
        push_to_postgres(path, table, mode, engine, key)

    print("\n✅ All requested uploads complete.")

if __name__ == "__main__":
    main()