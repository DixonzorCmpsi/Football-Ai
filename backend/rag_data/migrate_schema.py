"""Idempotent schema migration for the production Postgres.

What this fixes:

1. `player_profiles.draft_number` — historically the column didn't exist in some
   environments (nflreadpy's roster source omits it for the current season
   pre-draft). The application now treats `draft_number` as canonical; this
   migration adds the column on existing DBs and backfills veteran picks from
   `nfl.load_draft_picks()`.

2. `weekly_snap_counts_{season}.pfr_id` — historical snap data keys on
   `pfr_player_id`. Persisting `pfr_id` on current-season snap rows too lets
   `/player/history` join on one column for all seasons. This migration adds
   the column to the current-season table and backfills it from nflreadpy.

Safe to run repeatedly: uses `ADD COLUMN IF NOT EXISTS` and only overwrites
NULL values during backfill.

Usage:
    cd backend && python rag_data/migrate_schema.py
"""

import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")


def get_current_season() -> int:
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        text("SELECT to_regclass(:t) IS NOT NULL AS exists"),
        {"t": table},
    ).fetchone()
    return bool(row and row[0])


def _bulk_update_via_temp_table(
    engine, target_table: str, target_col: str, key_col: str, rows: list[dict],
    value_key: str, key_key: str,
) -> int:
    """Backfill `target_table.target_col` from `rows` via a TEMP TABLE join.

    One statement for the bulk insert + one UPDATE…FROM — orders of magnitude
    faster than looping per-row, which matters when this runs on every container
    boot via the lifespan hook.
    """
    if not rows:
        return 0
    tmp = f"_mig_{target_table}_{target_col}"
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp}"'))
        conn.execute(text(f'CREATE TEMP TABLE "{tmp}" ({key_col} TEXT, val TEXT)'))
        conn.execute(
            text(f'INSERT INTO "{tmp}" ({key_col}, val) VALUES (:k, :v)'),
            [{"k": str(r[key_key]), "v": str(r[value_key])} for r in rows],
        )
        res = conn.execute(
            text(
                f'UPDATE "{target_table}" AS t '
                f'SET {target_col} = src.val::INTEGER '
                f'FROM "{tmp}" src '
                f"WHERE t.{key_col} = src.{key_col} AND t.{target_col} IS NULL"
            )
        )
        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp}"'))
        return res.rowcount or 0


def migrate_player_profiles(engine, *, log=print) -> None:
    log("=== player_profiles.draft_number ===")
    with engine.begin() as conn:
        if not _table_exists(conn, "player_profiles"):
            log("  player_profiles table not found — skip (will be created by next ETL).")
            return

        conn.execute(
            text("ALTER TABLE player_profiles ADD COLUMN IF NOT EXISTS draft_number INTEGER")
        )

        null_count = conn.execute(
            text("SELECT COUNT(*) FROM player_profiles WHERE draft_number IS NULL")
        ).scalar() or 0

    if not null_count:
        log("  ✓ draft_number column present; nothing to backfill.")
        return

    log(f"  draft_number column present; {null_count} rows need backfill.")

    try:
        import nflreadpy as nfl
    except ImportError:
        log("  nflreadpy not installed — column added but backfill skipped.")
        return

    try:
        picks = nfl.load_draft_picks()
    except Exception as e:
        log(f"  ⚠ Draft picks fetch failed: {e}")
        return

    if picks is None or picks.is_empty() or "gsis_id" not in picks.columns or "pick" not in picks.columns:
        log("  No usable draft pick data — skipping backfill.")
        return

    rows = (
        picks.select(["gsis_id", "pick"])
        .drop_nulls(subset=["gsis_id", "pick"])
        .unique(subset=["gsis_id"], keep="last")
        .to_dicts()
    )
    updated = _bulk_update_via_temp_table(
        engine, "player_profiles", "draft_number", "player_id",
        rows, value_key="pick", key_key="gsis_id",
    )
    log(f"  ✓ Backfilled {updated} draft picks.")


def migrate_snap_counts(engine, season: int, *, log=print) -> None:
    table = f"weekly_snap_counts_{season}"
    log(f"=== {table}.pfr_id ===")
    with engine.begin() as conn:
        if not _table_exists(conn, table):
            log(f"  {table} table not found — skip (will be created by next ETL).")
            return

        conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS pfr_id TEXT'))

        null_count = conn.execute(
            text(f'SELECT COUNT(*) FROM "{table}" WHERE pfr_id IS NULL')
        ).scalar() or 0

    if not null_count:
        log("  ✓ pfr_id column present; nothing to backfill.")
        return

    log(f"  pfr_id column present; {null_count} rows need backfill.")

    try:
        import nflreadpy as nfl
    except ImportError:
        log("  nflreadpy not installed — column added but backfill skipped.")
        return

    try:
        raw = nfl.load_snap_counts(seasons=[season])
    except Exception as e:
        log(f"  ⚠ Snap counts fetch failed: {e}")
        return

    if raw is None or raw.is_empty() or "pfr_player_id" not in raw.columns:
        log("  No usable snap data — skipping backfill.")
        return

    # Reverse-map: nflreadpy snaps have pfr_player_id; our table only knows
    # player_name+week. Build (name, week) -> pfr key via a temp table join.
    name_col = "player" if "player" in raw.columns else "player_name"
    rows = (
        raw.select([name_col, "week", "pfr_player_id"])
        .drop_nulls(subset=["pfr_player_id", name_col, "week"])
        .unique(subset=[name_col, "week"], keep="last")
        .to_dicts()
    )

    tmp = f"_mig_{table}_pfr"
    updated = 0
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp}"'))
        conn.execute(text(f'CREATE TEMP TABLE "{tmp}" (player_name TEXT, week INT, pfr TEXT)'))
        conn.execute(
            text(f'INSERT INTO "{tmp}" (player_name, week, pfr) VALUES (:n, :w, :p)'),
            [{"n": r[name_col], "w": int(r["week"]), "p": r["pfr_player_id"]} for r in rows],
        )
        res = conn.execute(
            text(
                f'UPDATE "{table}" AS t SET pfr_id = src.pfr '
                f'FROM "{tmp}" src '
                f"WHERE t.player_name = src.player_name AND t.week = src.week AND t.pfr_id IS NULL"
            )
        )
        updated = res.rowcount or 0
        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp}"'))
    log(f"  ✓ Backfilled {updated} snap rows.")


def run_migrations(engine, season: int, *, log=print) -> None:
    """Single entrypoint other modules can call (e.g. the FastAPI lifespan)."""
    migrate_player_profiles(engine, log=log)
    migrate_snap_counts(engine, season, log=log)


def main() -> int:
    if not DB_CONNECTION_STRING:
        print("DB_CONNECTION_STRING not set in environment. Set it and retry.", file=sys.stderr)
        return 1

    season = int(os.getenv("SEASON", get_current_season()))
    print(f"Migrating schema for season {season}...")
    print(f"Target: {DB_CONNECTION_STRING.split('@')[-1] if '@' in DB_CONNECTION_STRING else 'local'}")

    engine = create_engine(DB_CONNECTION_STRING)
    try:
        run_migrations(engine, season)
    except Exception as e:
        print(f"\n❌ Migration failed: {e}", file=sys.stderr)
        return 2

    print("\n✅ Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
