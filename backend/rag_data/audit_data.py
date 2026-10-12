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

With --api it also asks the RUNNING app what it is serving (loaded data age, injury refresh,
game lines on the schedule), because a backend can answer /health while holding old data:

    python rag_data/audit_data.py --api https://host/api --auth admin:password --insecure

Pure reads; it changes nothing.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
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


def _get_json(url: str, auth: str | None, insecure: bool):
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    ctx = ssl._create_unverified_context() if insecure else None
    with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _age_minutes(stamp: str | None):
    if not stamp:
        return None
    try:
        when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - when).total_seconds() / 60


def check_running_app(base: str, auth: str | None, insecure: bool) -> None:
    """What the live API is serving right now, not what the database holds."""
    base = base.rstrip("/")
    try:
        health = _get_json(f"{base}/health", auth, insecure)
    except Exception as exc:
        fail("app: reachable", f"{base}/health failed: {type(exc).__name__}: {exc}")
        return
    ok("app: reachable", f"{base}")
    (ok if health.get("ready") and health.get("db_responding") else fail)(
        "app: ready", f"ready={health.get('ready')} db_responding={health.get('db_responding')}")
    (ok if health.get("models_loaded", 0) >= 4 and health.get("meta_loaded") else fail)(
        "app: models", f"{health.get('models_loaded')} position models, meta={health.get('meta_loaded')}")
    if health.get("db_read_path") not in ("arrow", None):
        warn("app: database reads", f"using the slow path ({health.get('db_read_path')}): {health.get('arrow_unavailable_reason')}")

    loaded = _age_minutes(health.get("data_loaded_at"))
    if loaded is None:
        fail("app: data loaded", "no load timestamp: the app has not loaded data")
    elif loaded > 180:
        fail("app: data loaded", f"the app's in-memory data is {loaded/60:.1f} h old (it reloads hourly)")
    else:
        ok("app: data loaded", f"{loaded:.0f} min ago")

    inj = _age_minutes(health.get("injuries_updated_at"))
    if inj is None:
        warn("app: injury refresh", "no injury refresh yet since the app started (the first runs within 15 minutes of boot)")
    elif inj > 45:
        fail("app: injury refresh", f"last refreshed {inj:.0f} min ago; it should run every 15")
    else:
        ok("app: injury refresh", f"{inj:.0f} min ago")

    story = _age_minutes(health.get("storylines_updated_at"))
    (ok if story is not None and story < 360 else warn)(
        "app: storylines", f"{story:.0f} min ago" if story is not None else "never refreshed")

    week = health.get("current_week")
    try:
        games = _get_json(f"{base}/schedule/{week}", auth, insecure)
        with_total = sum(1 for g in games if g.get("game_total"))
        (ok if games and with_total == len(games) else fail)(
            "app: game lines", f"week {week}: {with_total} of {len(games)} games have an over/under")
    except Exception as exc:
        fail("app: game lines", f"/schedule/{week} failed: {type(exc).__name__}")


def _american_prob(odds) -> float | None:
    """American odds -> implied probability in percent (the same arithmetic the scraper uses)."""
    text_ = str(odds or "").strip().upper()
    if text_ in ("EVEN", "EV"):
        return 50.0
    try:
        v = int(text_.replace("+", ""))
    except ValueError:
        return None
    if v == 0:
        return None
    return 100.0 * (100.0 / (v + 100.0) if v > 0 else abs(v) / (abs(v) + 100.0))


def _norm_name(name: str) -> str:
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def check_odds(conn, insp, upcoming: int) -> None:
    """Is the betting data users see real? Fresh, complete for the next games, and in
    agreement with an independent source (the NFL schedule feed's own lines)."""
    from datetime import datetime, timezone

    # ---- game lines
    if not has(insp, "bovada_game_lines"):
        warn("odds: game lines", "no bovada_game_lines table; the app falls back to the schedule feed's lines")
    else:
        sched = conn.execute(text(
            "SELECT game_id, home_team, away_team, spread_line, total_line FROM schedule WHERE week = :w"
        ), {"w": upcoming}).mappings().all()
        lines = {
            (r["home_team"], r["away_team"]): r for r in conn.execute(text(
                "SELECT home_team, away_team, home_spread, total_over, processed_at FROM bovada_game_lines WHERE week = :w"
            ), {"w": upcoming}).mappings().all()
        }
        have = [g for g in sched if (g["home_team"], g["away_team"]) in lines]
        stamps = [str(r["processed_at"]) for r in lines.values() if r["processed_at"]]
        age_h = None
        if stamps:
            try:
                newest = max(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in stamps)
                if newest.tzinfo is None:
                    newest = newest.replace(tzinfo=timezone.utc)
                age_h = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
            except ValueError:
                pass
        if not sched:
            warn("odds: game lines", f"no schedule rows for week {upcoming} to compare with")
        elif not have:
            warn("odds: game lines", f"none of week {upcoming}'s {len(sched)} games has a Bovada line yet; the schedule feed's lines are shown instead")
        else:
            detail = f"{len(have)} of {len(sched)} week-{upcoming} games have a Bovada line"
            if age_h is not None:
                detail += f"; newest pull {age_h:.1f}h ago"
            (warn if (age_h is not None and age_h > 36) else ok)("odds: game lines", detail + (" (stale: the odds refresh is not running)" if age_h and age_h > 36 else ""))

            # Independent check: the schedule feed publishes its own spread and total.
            flipped, off, checked = [], [], 0
            for g in have:
                b = lines[(g["home_team"], g["away_team"])]
                try:
                    bov_fav = -float(b["home_spread"])  # positive = home favoured, like the schedule's spread_line
                    bov_total = float(b["total_over"])
                    s_fav, s_total = float(g["spread_line"]), float(g["total_line"])
                except (TypeError, ValueError):
                    continue
                checked += 1
                tag = f"{g['away_team']}@{g['home_team']} (Bovada home {bov_fav:+.1f}/{bov_total:g}, schedule {s_fav:+.1f}/{s_total:g})"
                if bov_fav * s_fav < 0 and abs(bov_fav - s_fav) >= 3:
                    flipped.append(tag)
                elif abs(bov_fav - s_fav) > 3 or abs(bov_total - s_total) > 3:
                    off.append(tag)
            if checked == 0:
                warn("odds: lines vs schedule feed", "no game had comparable numbers")
            elif flipped:
                fail("odds: lines vs schedule feed", f"{len(flipped)} game(s) have the OPPOSITE favourite: " + "; ".join(flipped[:3]))
            elif off:
                warn("odds: lines vs schedule feed", f"{len(off)} of {checked} differ by more than 3 points: " + "; ".join(off[:3]))
            else:
                ok("odds: lines vs schedule feed", f"{checked} games agree on the favourite and are within 3 points of the schedule feed")

    # ---- player props
    if not has(insp, "bovada_player_props"):
        warn("odds: player props", "no bovada_player_props table; run the Bovada step (10b)")
        return
    rows = conn.execute(text(
        "SELECT player_name, position, prop_type, line, odds, side, implied_prob, game_id, scraped_at "
        "FROM bovada_player_props WHERE week = :w"
    ), {"w": upcoming}).mappings().all()
    if not rows:
        warn("odds: player props", f"none posted for week {upcoming} yet (Bovada usually lists them from Tuesday or Wednesday)")
        return
    games = {r["game_id"] for r in rows if r["game_id"]}
    ok("odds: player props", f"{len(rows)} rows, {len({r['player_name'] for r in rows})} players, {len(games)} game(s) with props")

    # Every row must be a real price: the implied probability must follow from the odds.
    bad_prob = [r for r in rows if _american_prob(r["odds"]) is not None and r["implied_prob"] is not None
                and abs(_american_prob(r["odds"]) - float(r["implied_prob"])) > 0.1]
    (fail if bad_prob else ok)("odds: probabilities", f"{len(bad_prob)} rows whose implied probability does not follow from the odds" if bad_prob else "implied probability follows from the odds on every row")

    dup = len(rows) - len({(r["player_name"], r["prop_type"], r["side"], r["line"], r["game_id"]) for r in rows})
    (fail if dup else ok)("odds: duplicates", f"{dup} repeated rows" if dup else "none")

    sides = {}
    for r in rows:
        if str(r["side"]).lower() in ("over", "under"):
            sides.setdefault((r["player_name"], r["prop_type"], r["line"], r["game_id"]), set()).add(str(r["side"]).lower())
    if sides:
        paired = sum(1 for v in sides.values() if len(v) == 2)
        (ok if paired / len(sides) >= 0.8 else warn)("odds: over/under pairs", f"{paired} of {len(sides)} lines have both an over and an under price")

    # A prop only helps if it lands on the right player: names are matched against the roster.
    prof = {_norm_name(r[0]) for r in conn.execute(text("SELECT player_name FROM player_profiles")).fetchall()}
    names = {r["player_name"] for r in rows}
    unmatched = sorted(n for n in names if _norm_name(n) not in prof)
    share = 1 - len(unmatched) / len(names)
    (fail if share < 0.7 else warn if unmatched else ok)(
        "odds: players matched",
        f"{len(names) - len(unmatched)} of {len(names)} names match a roster player" + (f"; unmatched: {', '.join(unmatched[:5])}" if unmatched else ""),
    )

    stamps = [str(r["scraped_at"]) for r in rows if r["scraped_at"]]
    if stamps:
        try:
            newest = max(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in stamps)
            if newest.tzinfo is None:
                newest = newest.replace(tzinfo=timezone.utc)
            hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
            (warn if hours > 36 else ok)("odds: props freshness", f"newest pull {hours:.1f}h ago" + (" (stale: the odds refresh is not running)" if hours > 36 else ""))
        except ValueError:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="print machine-readable results")
    ap.add_argument("--api", help="base URL of the running app's API (e.g. https://host/api) to check as well")
    ap.add_argument("--auth", help="user:password for the API's login")
    ap.add_argument("--insecure", action="store_true", help="accept a self-signed certificate")
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

        check_odds(conn, insp, upcoming)

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

    if args.api:
        check_running_app(args.api, args.auth, args.insecure)

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
