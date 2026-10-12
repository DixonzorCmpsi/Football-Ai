#!/usr/bin/env python3
"""Is the data the app serves complete and current? One PASS/WARN/FAIL line per check.

    DB_CONNECTION_STRING=postgresql://... python rag_data/audit_data.py [--json]

Exit code 0 when nothing FAILs (WARNs are worth reading but do not fail), 1 otherwise, so it can
gate a deploy or run from cron. It measures every table against the real state of the season,
the last week whose games are all final, instead of trusting that a pipeline "ran":

* the ETL can report success while a step wrote nothing (a month of stale weekly stats went
  unnoticed that way), so each table is compared with that week, not with a run marker;
* duplicates are counted, because a re-run that appends twice quietly doubles a game;
* history (previous seasons) and the trained models are checked the same way.

Pure reads; it changes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
from sqlalchemy import create_engine, inspect, text

HERE = Path(__file__).resolve().parent
BACKEND = HERE.parent


def current_season() -> int:
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1


SEASON = int(os.getenv("AUDIT_SEASON", current_season()))
HISTORY_SEASONS = range(SEASON - 5, SEASON)

results: list[dict] = []


def record(level: str, name: str, detail: str) -> None:
    results.append({"level": level, "check": name, "detail": detail})


def ok(name, detail): record("PASS", name, detail)
def warn(name, detail): record("WARN", name, detail)
def fail(name, detail): record("FAIL", name, detail)


def scalar(conn, sql, **params):
    return conn.execute(text(sql), params).scalar()


def has(insp, table) -> bool:
    return insp.has_table(table)


def check_weekly(conn, insp, table: str, label: str, last_final: int, min_per_week: int, keys: list[str]):
    """A per-week table: every week 1..last_final present, enough rows each, no repeated keys."""
    if not has(insp, table):
        fail(label, f"table {table} does not exist")
        return
    rows = conn.execute(text(f"SELECT week, count(*) FROM {table} GROUP BY week ORDER BY week")).fetchall()
    by_week = {int(w): int(n) for w, n in rows if w is not None}
    if not by_week:
        fail(label, f"{table} is empty")
        return
    newest = max(by_week)
    missing = [w for w in range(1, last_final + 1) if w not in by_week]
    thin = [w for w in range(1, last_final + 1) if 0 < by_week.get(w, 0) < min_per_week]
    if newest < last_final or missing:
        fail(label, f"newest week {newest}, season is final through week {last_final}; missing weeks {missing or 'none'}")
    elif thin:
        warn(label, f"weeks {thin} have fewer than {min_per_week} rows ({[by_week[w] for w in thin]})")
    else:
        ok(label, f"weeks 1-{newest} present, {sum(by_week.values())} rows")
    cols = ", ".join(keys)
    dupes = scalar(conn, f"SELECT count(*) FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1) t")
    if dupes:
        fail(f"{label}: duplicates", f"{dupes} ({cols}) combinations appear more than once")
    else:
        ok(f"{label}: duplicates", "none")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="print machine-readable results")
    args = ap.parse_args()

    url = os.getenv("DB_CONNECTION_STRING")
    if not url:
        print("DB_CONNECTION_STRING is not set", file=sys.stderr)
        return 2
    engine = create_engine(url)
    insp = inspect(engine)

    with engine.connect() as conn:
        # --- the schedule decides what "current" means
        if not has(insp, "schedule"):
            fail("schedule", "table does not exist")
            return finish(args)
        sched = pl.read_database(
            text(f"SELECT week, game_type, home_score, away_score FROM schedule WHERE season = {SEASON} ORDER BY week"),
            conn,
        )
        reg = sched.filter(pl.col("game_type") == "REG")
        if reg.height < 250:
            fail("schedule", f"{reg.height} regular-season games for {SEASON}, expected about 272")
        else:
            ok("schedule", f"{reg.height} regular-season games")
        final = reg.filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
        weeks_final = (
            reg.group_by("week").agg(
                pl.len().alias("games"),
                (pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()).sum().alias("done"),
            ).sort("week")
        )
        done_weeks = [int(r["week"]) for r in weeks_final.iter_rows(named=True) if r["done"] == r["games"] and r["games"] > 0]
        last_final = 0
        for w in sorted(done_weeks):
            if w == last_final + 1:
                last_final = w
        upcoming = last_final + 1
        ok("season position", f"weeks 1-{last_final} are final; week {upcoming} is next ({final.height} games scored)")

        # --- current-season tables
        check_weekly(conn, insp, f"weekly_player_stats_{SEASON}", "player stats", last_final, 150, ["player_id", "week"])
        check_weekly(conn, insp, f"weekly_snap_counts_{SEASON}", "snap counts", last_final, 400, ["player_id", "week"])
        check_weekly(conn, insp, f"weekly_offense_stats_{SEASON}", "team offense", last_final, 28, ["team_abbr", "week"])
        check_weekly(conn, insp, f"weekly_defense_stats_{SEASON}", "team defense", last_final, 28, ["team_abbr", "week"])

        # --- injuries: the report is for the week being played, so the upcoming week must be there
        inj = f"weekly_injuries_{SEASON}"
        if not has(insp, inj):
            fail("injuries", f"table {inj} does not exist")
        else:
            weeks = {int(w): int(n) for w, n in conn.execute(text(f"SELECT week, count(*) FROM {inj} GROUP BY week")).fetchall() if w is not None}
            newest = max(weeks) if weeks else 0
            if newest < min(upcoming, 18):
                fail("injuries", f"newest report is week {newest}; week {upcoming} is next")
            else:
                ok("injuries", f"report through week {newest} ({weeks.get(newest, 0)} players)")
            dupes = scalar(conn, f"SELECT count(*) FROM (SELECT player_id, week FROM {inj} GROUP BY 1, 2 HAVING count(*) > 1) t")
            if dupes > 0:
                warn("injuries: duplicates", f"{dupes} player-weeks repeat; the app keeps the most serious status per player, but the table should be one row each")
            else:
                ok("injuries: duplicates", "none")

        # --- model inputs
        feat = f"weekly_feature_set_{SEASON}"
        if not has(insp, feat):
            fail("model features", f"table {feat} does not exist")
        else:
            fw = {int(w): int(n) for w, n in conn.execute(text(f"SELECT week, count(*) FROM {feat} GROUP BY week")).fetchall() if w is not None}
            fnew = max(fw) if fw else 0
            if fnew < last_final:
                fail("model features", f"newest week {fnew}; the model needs through week {last_final} to project week {upcoming}")
            else:
                ok("model features", f"weeks through {fnew}, {sum(fw.values())} rows")

        # --- people
        if has(insp, "player_profiles"):
            n = scalar(conn, "SELECT count(*) FROM player_profiles")
            active = scalar(conn, "SELECT count(*) FROM player_profiles WHERE status = 'ACT'")
            (fail if n < 2500 else ok)("player profiles", f"{n} players, {active} active")
            dup = scalar(conn, "SELECT count(*) FROM (SELECT player_id FROM player_profiles GROUP BY 1 HAVING count(*) > 1) t")
            (fail if dup else ok)("player profiles: duplicates", f"{dup} repeated ids" if dup else "none")
        else:
            fail("player profiles", "table does not exist")

        # --- odds: informational, game lines come from the schedule feed
        if not has(insp, "bovada_player_props"):
            warn("bovada props", "no props table yet: Bovada's board was empty on the last run, so there are no prop prices")
        if has(insp, "bovada_player_props"):
            wmax = scalar(conn, "SELECT max(week) FROM bovada_player_props")
            if wmax is None or wmax < upcoming:
                warn("bovada props", f"newest week {wmax}; week {upcoming} is next (prop prices are missing until the scrape is fixed)")
            else:
                ok("bovada props", f"through week {wmax}")

    # --- previous seasons (CSV caches the app loads) and models
    for y in HISTORY_SEASONS:
        p = HERE / f"weekly_player_stats_{y}.csv"
        if not p.exists():
            warn(f"history {y}", "no cached stats file; the app fetches it on demand from nflreadpy")
            continue
        df = pl.read_csv(p, columns=["player_id", "week"], ignore_errors=True)
        weeks = set(df["week"].drop_nulls().cast(pl.Int64).to_list())
        gaps = [w for w in range(1, 18) if w not in weeks]
        dups = df.height - df.unique().height
        if df.height < 4000 or gaps:
            fail(f"history {y}", f"{df.height} rows, weeks missing {gaps}")
        elif dups:
            fail(f"history {y}", f"{dups} duplicated player-weeks")
        else:
            ok(f"history {y}", f"{df.height} rows, weeks 1-{max(weeks)}")

    models = sorted((BACKEND / "model_training" / "models").glob("xgboost_*_deviation_v1.joblib")) + \
        sorted((BACKEND / "model_training" / "models").glob("xgboost_META_model_v1.joblib"))
    if len(models) < 5:
        fail("models", f"found {len(models)} of 5 trained models (QB, RB, WR, TE, meta)")
    else:
        try:
            import joblib

            for m in models:
                joblib.load(m)
            ok("models", f"{len(models)} trained models load")
        except Exception as exc:  # a model that doesn't load is a broken projection, not a warning
            fail("models", f"a trained model failed to load: {type(exc).__name__}: {exc}")

    return finish(args)


def finish(args) -> int:
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        width = max(len(r["check"]) for r in results) if results else 10
        for r in results:
            print(f"{r['level']:4s}  {r['check']:<{width}}  {r['detail']}")
        fails = sum(r["level"] == "FAIL" for r in results)
        warns = sum(r["level"] == "WARN" for r in results)
        print(f"\n{len(results)} checks: {len(results) - fails - warns} pass, {warns} warn, {fails} fail")
    return 1 if any(r["level"] == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
