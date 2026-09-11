#!/usr/bin/env python3
"""Pull every Bovada player prop via their JSON board and write the props CSV.

Replaces the Chrome/Selenium text-scrape path (10_bovada_crawler + 11_bovada_scraper
+ 12_process_bovada) for PLAYER PROPS. Those steps only ever saw the default
market tab, which is why the app showed lines for a handful of popular players
and nothing for everyone else.

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
    event_teams,
    fetch_event_full,
    fetch_nfl_events,
    parse_event_player_props,
)

RAG_DIR = os.path.dirname(os.path.abspath(__file__))
SEASON = int(os.getenv("CURRENT_SEASON", "2026"))
PROPS_CSV = os.path.join(RAG_DIR, "weekly_bovada_player_props_{season}.csv")
SCHEDULE_CSV = os.path.join(RAG_DIR, "schedule_{season}.csv")
PROFILES_CSV = os.path.join(RAG_DIR, "player_profiles_{season}.csv")

OUTPUT_COLUMNS = [
    "player_name", "position", "prop_type", "line", "odds", "side",
    "implied_prob", "week", "game_id", "season", "scraped_at",
    "actual_result", "processed_at",
]


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
        print(f"  [{i}/{len(events)}] {away}@{home} wk={week} -> {len(rows)} prop rows")
        time.sleep(0.35)  # be a considerate client

    if not all_rows:
        print("  no player props parsed; leaving existing CSV untouched")
        return 1

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
    df.write_csv(out)
    print(f"  wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
