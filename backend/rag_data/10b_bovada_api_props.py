#!/usr/bin/env python3
"""Pull Bovada game lines and every player prop from their JSON board.

Replaces the Chrome/Selenium text-scrape path (10_bovada_crawler + 11_bovada_scraper
+ 12_process_bovada) for both. Those steps only ever saw the default market tab,
hung for 10+ minutes in the daily ETL, and broke silently when Bovada changed its
page layout: no totals, no spreads, and spread prices stored as moneylines.

Bovada only lists games that haven't finished, so both CSVs are MERGED: rows for
games on today's board are replaced, and earlier games keep the lines (and props,
which 14_update_bovada_results grades after kickoff) they had.

Usage:
    python 10b_bovada_api_props.py [--season 2026] [--dry-run] [--limit N]
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bovada_api_client import (  # noqa: E402
    GAME_LINE_COLUMNS,
    event_teams,
    fetch_event_full,
    fetch_nfl_events,
    parse_event_game_lines,
    parse_event_player_props,
)

RAG_DIR = os.path.dirname(os.path.abspath(__file__))
SEASON = int(os.getenv("CURRENT_SEASON", "2026"))
PROPS_CSV = os.path.join(RAG_DIR, "weekly_bovada_player_props_{season}.csv")
LINES_CSV = os.path.join(RAG_DIR, "weekly_bovada_game_lines_{season}.csv")
SCHEDULE_CSV = os.path.join(RAG_DIR, "schedule_{season}.csv")
PROFILES_CSV = os.path.join(RAG_DIR, "player_profiles_{season}.csv")

OUTPUT_COLUMNS = [
    "player_name", "position", "prop_type", "line", "odds", "side",
    "implied_prob", "week", "game_id", "season", "scraped_at",
    "actual_result", "processed_at",
]


def merge_into_csv(path: str, fresh: pl.DataFrame, columns: list[str], drop_stale=None) -> pl.DataFrame:
    """Existing rows for games NOT in `fresh`, plus `fresh`, in the table's column order.

    Everything is read and written as text: the two sources disagree on dtypes
    (a line is "+1.5" in one and 1.5 in the other) and COPY parses it anyway.
    `drop_stale(df)` removes kept rows known to be bad.
    """
    fresh = fresh.select([pl.col(c).cast(pl.Utf8) if c in fresh.columns else pl.lit(None, dtype=pl.Utf8).alias(c) for c in columns])
    if not os.path.exists(path):
        return fresh
    try:
        old = pl.read_csv(path, infer_schema_length=0)
    except Exception as exc:
        print(f"  could not read existing {os.path.basename(path)} ({exc}); writing fresh rows only")
        return fresh
    old = old.select([pl.col(c) if c in old.columns else pl.lit(None, dtype=pl.Utf8).alias(c) for c in columns])
    refreshed = [g for g in fresh["game_id"].unique().to_list() if g]
    kept = old.filter(~pl.col("game_id").is_in(refreshed) & pl.col("game_id").is_not_null())
    if drop_stale is not None:
        before = kept.height
        kept = drop_stale(kept)
        if kept.height != before:
            print(f"  dropped {before - kept.height} stale row(s) from {os.path.basename(path)}")
    return pl.concat([kept, fresh], how="vertical")


def _text_scrape_damage(df: pl.DataFrame) -> pl.DataFrame:
    """Rows the broken text parser wrote: a 'moneyline' with no total and no spread.

    Those moneylines were really spread prices. Dropping them lets the schedule's
    own lines stand in rather than showing a wrong favourite.
    """
    return df.filter(~(pl.col("total_over").is_null() & pl.col("home_spread").is_null()))


def load_schedule(season: int) -> pl.DataFrame:
    path = SCHEDULE_CSV.format(season=season)
    if not os.path.exists(path):
        return pl.DataFrame()
    return pl.read_csv(path, ignore_errors=True)


def week_and_game_id(schedule: pl.DataFrame, home: str, away: str, season: int):
    """Resolve (week, game_id) from the schedule, matching either orientation."""
    if schedule.is_empty() or not home or not away:
        return None, None
    cols = set(schedule.columns)
    hc = "home_team" if "home_team" in cols else None
    ac = "away_team" if "away_team" in cols else None
    if not hc or not ac or "week" not in cols:
        return None, None
    hit = schedule.filter((pl.col(hc) == home) & (pl.col(ac) == away))
    if hit.is_empty():
        hit = schedule.filter((pl.col(hc) == away) & (pl.col(ac) == home))
    if hit.is_empty():
        return None, None
    row = hit.row(0, named=True)
    week = int(row["week"])
    return week, f"{season}_{week:02d}_{row[ac]}_{row[hc]}"


def build_position_lookup(season: int) -> dict:
    path = PROFILES_CSV.format(season=season)
    if not os.path.exists(path):
        return {}
    df = pl.read_csv(path, ignore_errors=True)
    if "player_name" not in df.columns or "position" not in df.columns:
        return {}
    lookup = {}
    for row in df.iter_rows(named=True):
        name = (row.get("player_name") or "").strip()
        if not name:
            continue
        key = name.replace(".", "").lower()
        lookup.setdefault(key, row.get("position"))
    return lookup


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--limit", type=int, default=0, help="only process N events")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    season = args.season
    print(f"Bovada API props | season {season}")

    events = fetch_nfl_events()
    print(f"  events on NFL board: {len(events)}")
    if not events:
        print("  nothing to do")
        return 1

    schedule = load_schedule(season)
    positions = build_position_lookup(season)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    all_rows = []
    line_rows = []
    unmatched_games = []
    if args.limit:
        events = events[: args.limit]

    for i, stub in enumerate(events, 1):
        link = stub.get("link")
        if not link:
            continue
        try:
            event = fetch_event_full(link)
        except Exception as exc:
            print(f"  [{i}/{len(events)}] {link} -> FAILED: {exc}")
            continue
        if not event:
            continue
        home, away = event_teams(event)
        week, game_id = week_and_game_id(schedule, home, away, season)
        if game_id is None:
            unmatched_games.append(f"{away}@{home}")
        rows = parse_event_player_props(event, season, week, game_id, scraped_at)
        all_rows.extend(rows)
        lines = parse_event_game_lines(event, season, week, game_id, scraped_at) if game_id else None
        if lines:
            line_rows.append(lines)
        total = lines.get("total_over") if lines else None
        print(f"  [{i}/{len(events)}] {away}@{home} wk={week} -> {len(rows)} prop rows, total {total}")
        time.sleep(0.35)  # be a considerate client

    lines_out = LINES_CSV.format(season=season)
    if line_rows:
        merged_lines = merge_into_csv(lines_out, pl.DataFrame(line_rows, infer_schema_length=None), GAME_LINE_COLUMNS, _text_scrape_damage)
        print(f"  game lines        : {len(line_rows)} fresh, {merged_lines.height} total")
        if not args.dry_run:
            merged_lines.write_csv(lines_out)
            print(f"  wrote {lines_out}")
    else:
        print("  no game lines parsed; leaving existing lines CSV untouched")

    if not all_rows:
        print("  no player props parsed; leaving existing props CSV untouched")
        return 0 if line_rows else 1

    df = pl.DataFrame(all_rows)

    # Attach positions by name; unknown players keep a null rather than a guess.
    df = df.with_columns(
        pl.col("player_name")
        .map_elements(
            lambda n: positions.get((n or "").replace(".", "").lower()),
            return_dtype=pl.Utf8,
        )
        .alias("position")
    )
    df = df.with_columns([
        pl.lit(None, dtype=pl.Utf8).alias("actual_result"),
        pl.lit(datetime.now(timezone.utc).isoformat(timespec="seconds")).alias("processed_at"),
        pl.col("odds").cast(pl.Utf8),
    ])

    # One row per player/market/side/line.
    df = df.unique(subset=["player_name", "prop_type", "side", "line", "game_id"], keep="last")
    df = df.select([c for c in OUTPUT_COLUMNS if c in df.columns])

    print()
    print(f"  total rows        : {len(df)}")
    print(f"  distinct players  : {df['player_name'].n_unique()}")
    print(f"  distinct markets  : {df['prop_type'].n_unique()}")
    print(f"  sides             : {df['side'].unique().to_list()}")
    if unmatched_games:
        print(f"  WARNING unmatched against schedule: {unmatched_games}")

    if args.dry_run:
        print("  --dry-run: not writing")
        return 0

    out = PROPS_CSV.format(season=season)
    df = merge_into_csv(out, df, OUTPUT_COLUMNS)
    df.write_csv(out)
    print(f"  wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
