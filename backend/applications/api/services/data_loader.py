import polars as pl
import os
import time
import nflreadpy as nfl
from datetime import datetime
from ..config import logger, DB_CONNECTION_STRING, RAG_DIR, CURRENT_SEASON
from ..state import model_data
from .utils import enforce_types
from ..db import read_db

DERIVED_CACHE_KEYS = (
    "team_rankings_cache",
    "player_position_rankings_cache",
    "team_sack_context_cache",
    "season_team_sack_context_cache",
)

# Short-lived current-roster corrections for moves that upstream feeds can lag on.
#
# These are a stopgap, never a source of truth. Each entry carries the date the
# move was confirmed, and an override only wins if it is NEWER than the feed
# observation it contradicts (see `_active_overrides`). That rule is what keeps
# this table from rotting:
#
#   * Kenneth Walker III sat on SEA in-app for months after being traded to KC,
#     because a hardcoded correction outlived the truth it encoded.
#   * DJ Moore was pinned to CHI by an entry asserting the depth-chart feed was
#     wrong about BUF. The feed was right. A stale human call silently beat fresh
#     upstream data.
#
# Both failures share one cause: an undated, unconditional override outranking a
# feed that had already caught up. Comparing timestamps removes the whole class.
# Empty on purpose. Verified 2026-09-11 against a depth-chart feed pulled the same
# day: LAC (Njoku) and KC (Walker) were already correct upstream, and CHI (Moore)
# was simply wrong -- the feed had him on BUF all along. Add an entry only for a
# move the feed has not caught up to yet, always with the date it was confirmed,
# and delete it once `_active_overrides` starts logging that it is redundant.
CURRENT_TEAM_OVERRIDES_DATED: dict = {}
CURRENT_TEAM_OVERRIDES = {pid: team for pid, (team, _) in CURRENT_TEAM_OVERRIDES_DATED.items()}

# Overrides older than this are past their useful life: either the source feed
# has long since caught up, or (as with Kenneth Walker III) the player has moved
# again and the override is now actively wrong. Surface them loudly instead of
# trusting hardcoded data forever.
_OVERRIDE_STALE_AFTER_DAYS = 60


def _parse_date(value):
    """Date part of an ISO timestamp or plain date string, or None.

    Only the calendar date matters here, so take the leading YYYY-MM-DD and
    ignore any time/zone suffix. (Slicing to 19 chars and then matching a format
    ending in "Z" silently fails on every real feed timestamp.)
    """
    if value is None:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _latest_depth_rows(depth: pl.DataFrame) -> pl.DataFrame:
    """One row per player: the most recent depth-chart observation."""
    if depth.is_empty() or "gsis_id" not in depth.columns:
        return depth
    if "dt" in depth.columns:
        return depth.sort("dt").unique(subset=["gsis_id"], keep="last")
    return depth.unique(subset=["gsis_id"], keep="last")


def _depth_chart_observed_on(depth: pl.DataFrame):
    """Date the depth-chart feed was last observed, or None if unknown."""
    if depth.is_empty() or "dt" not in depth.columns:
        return None
    try:
        return _parse_date(depth["dt"].max())
    except Exception:
        return None


def _warn_stale_overrides() -> None:
    today = datetime.now().date()
    for player_id, (team, confirmed) in CURRENT_TEAM_OVERRIDES_DATED.items():
        confirmed_on = _parse_date(confirmed)
        if confirmed_on is None:
            continue
        age_days = (today - confirmed_on).days
        if age_days > _OVERRIDE_STALE_AFTER_DAYS:
            logger.warning(
                "CURRENT_TEAM_OVERRIDES entry for %s -> %s is %d days old (confirmed %s) - "
                "re-verify the player's actual current team; it may have changed again.",
                player_id, team, age_days, confirmed,
            )


def _active_overrides(depth: pl.DataFrame) -> dict:
    """Decide which hardcoded overrides still outrank the live feed.

    An override is honoured only when it is strictly newer than the depth-chart
    observation it contradicts, or when the feed has no opinion on the player at
    all. Anything else means upstream has caught up (or was never wrong), so the
    feed wins and the override retires itself with a loud log line.
    """
    # `refresh_db_data` applies overrides once before the depth charts are loaded
    # and again afterwards. Without a feed there is nothing to adjudicate against,
    # and applying an override here would bake a stale team into df_profile that
    # the later, better-informed pass can no longer undo -- which is exactly how
    # DJ Moore kept showing as CHI even after his override had been retired.
    if depth.is_empty() or "gsis_id" not in depth.columns or "team" not in depth.columns:
        logger.info(
            "Depth-chart feed not loaded yet; deferring roster overrides to the "
            "post-load pass rather than guessing."
        )
        return {}

    observed_on = _depth_chart_observed_on(depth)
    frame = _latest_depth_rows(depth)
    feed_team = dict(zip(frame["gsis_id"].to_list(), frame["team"].to_list()))

    active = {}
    for player_id, (team, confirmed) in CURRENT_TEAM_OVERRIDES_DATED.items():
        current = feed_team.get(player_id)
        if current is None:
            # Feed has no row for this player, so the override is the only signal.
            active[player_id] = team
            continue
        if current == team:
            logger.info(
                "Roster override for %s -> %s is redundant; the depth-chart feed already "
                "agrees. Safe to delete the entry.",
                player_id, team,
            )
            continue
        confirmed_on = _parse_date(confirmed)
        if observed_on is not None and confirmed_on is not None and confirmed_on <= observed_on:
            logger.warning(
                "RETIRING roster override %s -> %s (confirmed %s): the depth-chart feed "
                "observed %s says %s and is newer, so the feed wins. Delete this entry.",
                player_id, team, confirmed, observed_on.isoformat(), current,
            )
            continue
        active[player_id] = team
    return active


def audit_roster_consistency() -> dict:
    """Log active-roster disagreements between the profile and depth-chart feeds.

    Practice-squad and cut players churn between feeds constantly and are noise;
    a disagreement on an ACTIVE player means a real move we are getting wrong,
    which is the signal actually worth surfacing.
    """
    profile = model_data.get("df_profile", pl.DataFrame())
    depth = model_data.get("df_depth_charts", pl.DataFrame())
    result = {"checked": 0, "active_mismatches": 0, "players": []}
    if profile.is_empty() or depth.is_empty():
        return result
    if "player_id" not in profile.columns or "team_abbr" not in profile.columns:
        return result
    if "gsis_id" not in depth.columns or "team" not in depth.columns:
        return result

    frame = _latest_depth_rows(depth)
    cols = [c for c in ("player_id", "player_name", "team_abbr", "status") if c in profile.columns]
    joined = profile.select(cols).join(
        frame.select(["gsis_id", "team"]).rename({"gsis_id": "player_id"}),
        on="player_id", how="inner",
    )
    result["checked"] = len(joined)
    mismatched = joined.filter(pl.col("team_abbr") != pl.col("team"))
    if "status" in joined.columns:
        mismatched = mismatched.filter(pl.col("status") == "ACT")
    result["active_mismatches"] = len(mismatched)
    if len(mismatched):
        result["players"] = mismatched.head(25).to_dicts()
        logger.warning(
            "Roster audit: %d ACTIVE players disagree between profile and depth-chart feeds "
            "(of %d compared). Sample: %s",
            len(mismatched), len(joined),
            ", ".join(
                "%s profile=%s depth=%s" % (r.get("player_name"), r.get("team_abbr"), r.get("team"))
                for r in result["players"][:10]
            ),
        )
    else:
        logger.info(
            "Roster audit: no ACTIVE-player team disagreements across %d players.", len(joined)
        )
    return result


def invalidate_derived_caches() -> None:
    """Clear request-time derived data after ETL/data reloads.

    Team rankings and player position finishes are computed from roster,
    historical player stats, and team stat tables. They must not survive a daily
    ETL refresh when the container is configured to reload in-process instead of
    restarting.
    """
    for key in DERIVED_CACHE_KEYS:
        model_data.pop(key, None)


def load_data_source(query: str, csv_filename: str, retries: int = 3, retry_delay: float = 1.0):
    """Try DB first with retries. By default the server runs in DB-only mode (no CSV fallback) unless ALLOW_CSV_FALLBACK is set to 'true'."""
    ALLOW_CSV_FALLBACK = os.getenv("ALLOW_CSV_FALLBACK", "false").lower() == "true"

    # Try DB with retries
    if DB_CONNECTION_STRING:
        attempt = 0
        while attempt < retries:
            try:
                df = read_db(query)
                logger.info(f"DB Load successful: {csv_filename} (attempt {attempt+1})")
                return enforce_types(df)
            except Exception as e:
                attempt += 1
                if attempt >= retries:
                    # If table missing show a specific hint
                    if "relation" in str(e).lower():
                        logger.warning(f"DB relation/table missing for {csv_filename}: {e}")
                    else:
                        logger.error(f"DB Load failed for {csv_filename} after {attempt} attempts: {e}")
                else:
                    time.sleep(retry_delay)

    # If CSV fallback is explicitly allowed, try it (development only)
    if ALLOW_CSV_FALLBACK:
        csv_path = os.path.join(RAG_DIR, csv_filename)
        if os.path.exists(csv_path):
            try:
                df = pl.read_csv(csv_path, ignore_errors=True)
                logger.info(f"CSV load successful (fallback): {csv_filename}")
                return enforce_types(df)
            except Exception as e:
                logger.error(f"CSV load failed: {csv_filename} - {e}")
        else:
            logger.warning(f"CSV fallback requested but file not found: {csv_filename}")
    logger.warning(f"Returning empty DataFrame for {csv_filename} (DB-only mode)")
    return pl.DataFrame()


def apply_current_roster_overrides() -> None:
    """Patch roster-feed lag, but only where an override still beats the live feed."""
    if not CURRENT_TEAM_OVERRIDES_DATED:
        audit_roster_consistency()
        return

    _warn_stale_overrides()

    overrides = _active_overrides(model_data.get("df_depth_charts", pl.DataFrame()))
    if not overrides:
        logger.info(
            "No roster overrides are newer than the feed; using upstream roster data as-is."
        )
        audit_roster_consistency()
        return

    profile = model_data.get("df_profile", pl.DataFrame())
    if not profile.is_empty() and "player_id" in profile.columns:
        exprs = []
        for col in ("team", "team_abbr", "recent_team"):
            if col not in profile.columns:
                continue
            expr = pl.col(col)
            for player_id, team in overrides.items():
                expr = pl.when(pl.col("player_id") == player_id).then(pl.lit(team)).otherwise(expr)
            exprs.append(expr.alias(col))
        if exprs:
            model_data["df_profile"] = profile.with_columns(exprs)

    depth = model_data.get("df_depth_charts", pl.DataFrame())
    if not depth.is_empty() and "gsis_id" in depth.columns and "team" in depth.columns:
        expr = pl.col("team")
        for player_id, team in overrides.items():
            expr = pl.when(pl.col("gsis_id") == player_id).then(pl.lit(team)).otherwise(expr)
        model_data["df_depth_charts"] = depth.with_columns(expr.alias("team"))

    audit_roster_consistency()

def _ensure_rookies_merged() -> None:
    """Guarantee `df_profile` contains the current-season rookies.

    Called after every `refresh_db_data` (which overwrites df_profile from DB and
    therefore drops rookies merged in earlier). Prefers the cached CSV; if it's
    missing, fetches from ESPN inline so a fresh install never starts
    rookie-less.
    """
    profile = model_data.get("df_profile", pl.DataFrame())
    if profile.is_empty():
        return

    try:
        from ..routes.tier_list import (
            _rookies_csv_path,
            load_persisted_rookies_into_profile,
            run_rookie_refresh,
        )
    except Exception as e:
        logger.warning(f"Rookie merge skipped (import failed): {e}")
        return

    try:
        csv_path = _rookies_csv_path(int(CURRENT_SEASON))
        if os.path.exists(csv_path):
            merged = load_persisted_rookies_into_profile()
            if merged:
                logger.info(f"Merged {merged} persisted rookies into df_profile")
            return

        result = run_rookie_refresh(persist=True)
        if result.get("ok"):
            logger.info(
                f"Fetched {result.get('found', 0)} rookies from ESPN "
                f"(added {result.get('added', 0)}); persisted to {result.get('persisted')}"
            )
        else:
            logger.warning("ESPN rookie fetch returned no rookies on data reload")
    except Exception as e:
        logger.warning(f"Rookie merge after data reload failed: {e}")


def refresh_db_data():
    logger.info("Loading dataframes from DB/CVS sources...")
    invalidate_derived_caches()
    sources = {
        "df_profile": ("SELECT * FROM player_profiles", f"player_profiles_{CURRENT_SEASON}.csv"),
        "df_schedule": ("SELECT * FROM schedule", f"schedule_{CURRENT_SEASON}.csv"),
        "df_player_stats": (f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv"),
        "df_snap_counts": (f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv"),
        "df_lines": ("SELECT * FROM bovada_game_lines", f"weekly_bovada_game_lines_{CURRENT_SEASON}.csv"),
        "df_props": ("SELECT * FROM bovada_player_props", f"weekly_bovada_player_props_{CURRENT_SEASON}.csv"),
        "df_injuries": (f"SELECT * FROM weekly_injuries_{CURRENT_SEASON}", f"weekly_injuries_{CURRENT_SEASON}.csv"),
        "df_features": (f"SELECT * FROM weekly_feature_set_{CURRENT_SEASON}", f"weekly_feature_set_{CURRENT_SEASON}.csv"),
    }
    
    for key, (query, csv) in sources.items():
        model_data[key] = load_data_source(query, csv)

    # Keep only the active season. A schedule table can retain prior seasons,
    # which must not drive the current-week calculation or game board.
    schedule = model_data["df_schedule"]
    if not schedule.is_empty() and "season" in schedule.columns:
        model_data["df_schedule"] = schedule.filter(
            pl.col("season").cast(pl.Int64, strict=False) == int(CURRENT_SEASON)
        )

    # A new season can have a published schedule before the production database
    # is populated. In that case use the checked-in current-season fixture.
    if model_data["df_schedule"].is_empty():
        schedule_path = os.path.join(RAG_DIR, f"schedule_{CURRENT_SEASON}.csv")
        if os.path.exists(schedule_path):
            try:
                model_data["df_schedule"] = enforce_types(pl.read_csv(schedule_path, ignore_errors=True))
                logger.info("Loaded %s schedule rows from the local season fixture", model_data["df_schedule"].height)
            except Exception as e:
                logger.warning("Unable to load local season schedule %s: %s", schedule_path, e)

    apply_current_roster_overrides()
    _ensure_rookies_merged()

    # If critical tables are empty, attempt an aggressive retry for player stats and snaps
    if ("df_player_stats" in model_data and model_data["df_player_stats"].is_empty()) or ("df_snap_counts" in model_data and model_data["df_snap_counts"].is_empty()):
        logger.warning("Critical tables empty after initial load — retrying DB loads for essential tables...")
        try:
            model_data["df_player_stats"] = load_data_source(f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for player_stats: {e}")
        try:
            model_data["df_snap_counts"] = load_data_source(f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for snap_counts: {e}")

    # --- BUILD INJURY MAP (ROBUST FIX) ---
    model_data["injury_map"] = {}
    model_data["gsis_to_sleeper"] = {}
    
    if "df_injuries" in model_data and not model_data["df_injuries"].is_empty():
        try:
            df = model_data["df_injuries"]
            
            # 1. Check if 'week' column exists (New Format)
            if "week" in df.columns:
                # Pick the week the app is actually showing, NOT max(week).
                # This table accumulates across ETL runs and has held stray rows
                # from other weeks/seasons; max(week) once resolved to a week-22
                # block that masked every genuine current-week injury (a player
                # ruled out the night before still rendered as "Active").
                available = set(
                    df.select(pl.col("week")).drop_nulls().unique().to_series().to_list()
                )
                target_wk = model_data.get("current_nfl_week")
                if target_wk is None or target_wk not in available:
                    fallback = df.select(pl.col("week").max()).item()
                    logger.warning(
                        "Injury map: current week %s unavailable (have %s); falling back to %s",
                        model_data.get("current_nfl_week"), sorted(available), fallback,
                    )
                    target_wk = fallback
                logger.info(f"Filtering injury map to week: {target_wk}")
                latest_report = df.filter(pl.col("week") == target_wk)
                
                # One player can still have several rows for the same week if an
                # older duplicate-laden table has not been rebuilt yet. Collapse to
                # one row per player, preferring the most serious status so a stale
                # "Active" duplicate can never hide a real "Out".
                _RANK = {
                    "ir": 6, "out": 5, "inactive": 5, "pup": 5, "sus": 5,
                    "doubtful": 4, "questionable": 3, "na": 1, "active": 0,
                }
                rows = latest_report.select(["player_id", "injury_status"]).to_dicts()
                best: dict = {}
                for r in rows:
                    pid, st = r["player_id"], r["injury_status"]
                    cur = best.get(pid)
                    if cur is None or _RANK.get(str(st).lower(), 2) > _RANK.get(str(cur).lower(), 2):
                        best[pid] = st
                model_data["injury_map"] = best
            else:
                # Fallback for old CSVs without week column
                logger.warning("Injury CSV lacks 'week' column. Loading all rows (last write wins).")
                rows = df.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
                
        except Exception as e: 
            logger.exception(f"Injury map build error: {e}")

    try:
        players_df = nfl.load_ff_playerids()
        if "sleeper_id" in players_df.columns:
            map_df = players_df.drop_nulls(subset=['sleeper_id', 'gsis_id'])
            model_data["gsis_to_sleeper"] = dict(zip(map_df['gsis_id'].to_list(), map_df['sleeper_id'].cast(pl.Utf8).to_list()))
            model_data["sleeper_map"] = dict(zip(map_df['sleeper_id'].cast(pl.Utf8).to_list(), map_df['gsis_id'].to_list()))
    except Exception: pass

    model_data["data_loaded_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    logger.info("Data loaded into memory.")

def refresh_app_state():
    logger.info("Refreshing app state (scheduler) ...")
    try:
        sched = model_data.get("df_schedule", pl.DataFrame())
        if not sched.is_empty() and "gameday" in sched.columns and "week" in sched.columns:
            if "season" in sched.columns:
                sched = sched.filter(pl.col("season").cast(pl.Int64, strict=False) == int(CURRENT_SEASON))

            # The current-season schedule, not the provider's cached week, is
            # authoritative during the preseason and on opening week.
            if not sched.is_empty():
                dated = sched.with_columns(
                    pl.col("gameday").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("_game_date")
                )
                upcoming = dated.filter(pl.col("_game_date") >= datetime.now().date())
                source = upcoming if not upcoming.is_empty() else dated
                active_week = source.sort("_game_date").select(pl.col("week").first()).item()
                if active_week is not None:
                    model_data["current_nfl_week"] = int(active_week)
                    logger.info("Active NFL Week from %s schedule: %s", CURRENT_SEASON, active_week)
                    return

        base_week = nfl.get_current_week()
        
        # Smart week detection: Only advance the week if ALL games in base_week have been played
        # (i.e., have scores). This prevents prematurely jumping to next week during bye weeks
        # or if games haven't started yet (e.g., playoffs on Saturday).
        should_advance = False
        
        if "df_schedule" in model_data and not model_data["df_schedule"].is_empty():
            sched = model_data["df_schedule"]
            week_games = sched.filter(pl.col("week") == base_week)
            
            if not week_games.is_empty():
                # Check if all games have scores (home_score is not null)
                if "home_score" in week_games.columns:
                    games_with_scores = week_games.filter(pl.col("home_score").is_not_null())
                    all_played = len(games_with_scores) == len(week_games)
                    
                    if all_played:
                        should_advance = True
                        logger.info(f"All {len(week_games)} games in Week {base_week} have been played. Advancing to next week.")
                    else:
                        logger.info(f"Week {base_week}: {len(games_with_scores)}/{len(week_games)} games played. Staying on Week {base_week}.")
                else:
                    # If no home_score column, fall back to old Tuesday logic
                    if datetime.now().weekday() == 1:
                        should_advance = True
            else:
                # No games found for this week (e.g., bye week), use base_week as-is
                logger.info(f"No games found for Week {base_week}. Staying on Week {base_week}.")
        else:
            # No schedule data loaded yet, fall back to old Tuesday logic
            if datetime.now().weekday() == 1:
                should_advance = True
        
        if should_advance:
            model_data["current_nfl_week"] = base_week + 1
        else:
            model_data["current_nfl_week"] = base_week
            
        logger.info(f"Active NFL Week: {model_data['current_nfl_week']}")
    except Exception as e:
        logger.exception(f"Error determining current week: {e}")
        model_data["current_nfl_week"] = 1

def load_player_history_from_db(player_id: str, week: int, limit: int = 12):
    """Load a player's recent history directly from DB (limited rows)."""
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT {int(limit)}"
        df = read_db(q)
        return enforce_types(df)
    except Exception as e:
        logger.warning(f"load_player_history_from_db error: {e}")
        return pl.DataFrame()


# ---- Historical (prior-season) weekly stats via nflreadpy --------------------
# nflreadpy uses slightly different column names than our schema, so we map them.
_NFL_TO_OUR_COLS = {
    "passing_tds": "passing_touchdown",
    "rushing_tds": "rush_touchdown",
    "receiving_tds": "receiving_touchdown",
    "carries": "rush_attempts",
    "passing_interceptions": "interceptions",
    "fantasy_points_ppr": "y_fantasy_points_ppr",
}


def _historical_stats_csv_path(season: int) -> str:
    import os

    return os.path.join(RAG_DIR, f"weekly_player_stats_{season}.csv")


def _fetch_and_cache_historical_stats(season: int) -> pl.DataFrame:
    """Pull a season of weekly stats from nflreadpy and cache as CSV."""
    import os

    cache = _historical_stats_csv_path(season)
    if os.path.exists(cache):
        try:
            df = pl.read_csv(cache, ignore_errors=True)
            if not df.is_empty():
                return enforce_types(df)
        except Exception as e:
            logger.warning(f"Reading cached {cache} failed: {e}")

    try:
        import nflreadpy as nfl

        raw = nfl.load_player_stats(seasons=int(season))
        if raw is None or raw.is_empty():
            logger.info(f"nflreadpy returned no stats for {season}")
            return pl.DataFrame()
        # Rename source columns to match our schema; missing columns are ignored.
        rename_map = {src: dst for src, dst in _NFL_TO_OUR_COLS.items() if src in raw.columns}
        if rename_map:
            raw = raw.rename(rename_map)
        if "season" not in raw.columns:
            raw = raw.with_columns(pl.lit(int(season)).alias("season"))
        try:
            raw.write_csv(cache)
            logger.info(f"Cached {len(raw)} weekly stat rows for {season} at {cache}")
        except Exception as e:
            logger.warning(f"Failed to write cache {cache}: {e}")
        return enforce_types(raw)
    except Exception as e:
        logger.warning(f"nflreadpy historical fetch failed for {season}: {e}")
        return pl.DataFrame()


def _depth_chart_csv_path(season: int) -> str:
    import os

    return os.path.join(RAG_DIR, f"depth_charts_{season}.csv")


_DEPTH_CHART_POS_ABBS = (
    "QB", "RB", "FB", "WR", "TE",
    "LT", "LG", "C", "RG", "RT",
    "LDE", "RDE", "LDT", "RDT", "NT",
    "SLB", "WLB", "MLB", "LILB", "RILB",
    "LCB", "RCB", "NB", "SS", "FS",
)
_DEFENSE_POS_ABBS = {"LDE", "RDE", "LDT", "RDT", "NT", "SLB", "WLB", "MLB", "LILB", "RILB", "LCB", "RCB", "NB", "SS", "FS"}
_OL_POS_ABBS = {"LT", "LG", "C", "RG", "RT"}


def _cache_is_fresh(path: str, max_age_hours: float) -> bool:
    try:
        return (time.time() - os.path.getmtime(path)) <= max_age_hours * 3600
    except OSError:
        return False


def _depth_cache_has_full_lineup(df: pl.DataFrame) -> bool:
    if df.is_empty() or "pos_abb" not in df.columns:
        return False
    positions = set(str(p) for p in df["pos_abb"].drop_nulls().unique().to_list())
    return bool(positions & _DEFENSE_POS_ABBS) and bool(positions & _OL_POS_ABBS)


def _fetch_and_cache_depth_charts(season: int, force: bool = False) -> pl.DataFrame:
    """Pull current-season offensive and defensive depth charts from nflreadpy.

    nflreadpy emits one row per (player, slot, snapshot date). We dedupe to the
    latest entry per (gsis_id, pos_id, pos_slot) so the file represents the
    current depth chart, not its history. Filtered to fantasy-relevant lineup
    positions: offense, OL, and defensive starters.
    """
    import os

    cache = _depth_chart_csv_path(season)
    max_age_hours = float(os.getenv("DEPTH_CHART_CACHE_TTL_HOURS", "6"))

    def _read_cached_depth_chart() -> pl.DataFrame:
        if not os.path.exists(cache):
            return pl.DataFrame()
        try:
            df = pl.read_csv(cache, ignore_errors=True)
            if not df.is_empty() and _depth_cache_has_full_lineup(df):
                return df
        except Exception as e:
            logger.warning(f"Reading cached {cache} failed: {e}")
        return pl.DataFrame()

    cached = _read_cached_depth_chart()
    if not force and not cached.is_empty() and _cache_is_fresh(cache, max_age_hours):
        return cached

    try:
        import nflreadpy as nfl

        raw = nfl.load_depth_charts(seasons=int(season))
        if raw is None or raw.is_empty():
            logger.info(f"nflreadpy returned no depth charts for {season}")
            if not cached.is_empty():
                logger.warning(f"Using stale cached depth charts for {season} after empty upstream response")
                return cached
            return pl.DataFrame()
        lineup = raw.filter(pl.col("pos_abb").is_in(list(_DEPTH_CHART_POS_ABBS)))
        lineup = lineup.sort("dt", descending=True).unique(
            subset=["gsis_id", "pos_id", "pos_slot"], keep="first"
        )
        try:
            lineup.write_csv(cache)
            logger.info(f"Cached {len(lineup)} depth chart rows for {season} at {cache}")
        except Exception as e:
            logger.warning(f"Failed to write cache {cache}: {e}")
        return lineup
    except Exception as e:
        logger.warning(f"Depth charts fetch failed for {season}: {e}")
        if not cached.is_empty():
            logger.warning(f"Using stale cached depth charts for {season} after fetch failure")
            return cached
        return pl.DataFrame()


# Profile roster statuses that CLAIM a player has left the team.
#
# On their own these are not trustworthy. Aaron Donald carries status EXE while
# the depth chart -- pulled the same morning -- lists him at pos_rank 1 for LA
# twice (NT on 09-01, RDE on 09-11). He is playing; the status field is what is
# stale. Same shape as the roster-override bug: an undated field outranking a
# dated feed.
#
# So a player counts as gone only when the status says so AND the current depth
# chart does not list him for that team. Of 443 injury rows whose status claims
# off-roster, 243 are still on a depth chart (stale status, keep) and 200 are
# not (genuinely departed).
#
# RES (reserve/IR) and INA (inactive) are deliberately absent: those players are
# on the roster and hurt, which is what an injury report is for.
CLAIMED_OFF_ROSTER_STATUSES = {"RET", "CUT", "EXE"}


def departed_player_ids() -> set:
    """Players the profile calls gone AND the live depth chart does not list."""
    profile = model_data.get("df_profile", pl.DataFrame())
    depth = model_data.get("df_depth_charts", pl.DataFrame())
    if profile.is_empty() or "status" not in profile.columns or "player_id" not in profile.columns:
        return set()

    claimed = profile.filter(pl.col("status").is_in(list(CLAIMED_OFF_ROSTER_STATUSES)))
    if claimed.is_empty():
        return set()
    if depth.is_empty() or "gsis_id" not in depth.columns:
        # With no feed to corroborate, keep everyone rather than hiding players
        # on the strength of a field already shown to lag.
        return set()

    listed = set(depth.drop_nulls(subset=["gsis_id"])["gsis_id"].to_list())
    return {p for p in claimed["player_id"].to_list() if p not in listed}


def load_depth_charts(season: int = CURRENT_SEASON, force: bool = False) -> None:
    """Populate `model_data['df_depth_charts']` + precomputed `starter_gsis_ids`.

    `is_starter` on each player flows from nflreadpy's depth chart (pos_rank == 1
    at any lineup slot), which is an explicit authoritative source — not a
    fallback or inference from snap counts.
    """
    df = _fetch_and_cache_depth_charts(int(season), force=force)
    if df.is_empty() or "gsis_id" not in df.columns or "pos_rank" not in df.columns:
        model_data["df_depth_charts"] = pl.DataFrame()
        model_data["starter_gsis_ids"] = set()
        model_data["depth_charts_loaded_at"] = None
        return
    model_data["df_depth_charts"] = df
    apply_current_roster_overrides()
    starters = (
        model_data["df_depth_charts"].filter(pl.col("pos_rank") == 1)
        .drop_nulls(subset=["gsis_id"])
        .select("gsis_id")
        .unique()
        ["gsis_id"]
        .to_list()
    )
    model_data["starter_gsis_ids"] = set(s for s in starters if s)
    model_data["depth_charts_loaded_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    logger.info(
        f"Depth charts: {len(model_data['starter_gsis_ids'])} starters loaded for {season}"
    )


def load_historical_stats(seasons: tuple[int, ...] = tuple(CURRENT_SEASON - n for n in range(5, 0, -1))) -> None:
    """Populate `model_data['df_player_stats_history']` with prior-season weekly stats.

    Combined dataframe across the requested seasons; safe to call repeatedly (uses
    CSV cache). Also loads prior-season snap counts into `df_snap_counts_history`
    so the player history endpoint can show real snap pct for past seasons.
    """
    frames = []
    for s in seasons:
        if s == CURRENT_SEASON:
            continue
        df = _fetch_and_cache_historical_stats(int(s))
        if not df.is_empty():
            frames.append(df)
    if frames:
        model_data["df_player_stats_history"] = pl.concat(frames, how="diagonal_relaxed")
        logger.info(f"Loaded {sum(len(f) for f in frames)} historical stat rows into df_player_stats_history")
    else:
        model_data["df_player_stats_history"] = pl.DataFrame()

    snap_frames = []
    for s in seasons:
        if s == CURRENT_SEASON:
            continue
        df = _fetch_and_cache_historical_snaps(int(s))
        if not df.is_empty():
            snap_frames.append(df)
    if snap_frames:
        model_data["df_snap_counts_history"] = pl.concat(snap_frames, how="diagonal_relaxed")
        logger.info(
            f"Loaded {sum(len(f) for f in snap_frames)} historical snap rows into df_snap_counts_history"
        )
    else:
        model_data["df_snap_counts_history"] = pl.DataFrame()


def _historical_snaps_csv_path(season: int) -> str:
    import os

    return os.path.join(RAG_DIR, f"weekly_snap_counts_{season}.csv")


def _normalize_snap_schema(df: pl.DataFrame, season: int) -> pl.DataFrame:
    """Bring a snap-count frame into the canonical schema used everywhere.

    nflreadpy keys snaps on `pfr_player_id`; the current-season ETL renames it
    to `pfr_id` so both sources can be joined on a single column. Apply the same
    rename to historical frames (fresh fetch *and* cached CSVs) so callers never
    have to branch.
    """
    if df.is_empty():
        return df
    if "season" not in df.columns:
        df = df.with_columns(pl.lit(int(season)).alias("season"))
    if "pfr_player_id" in df.columns and "pfr_id" not in df.columns:
        df = df.rename({"pfr_player_id": "pfr_id"})
    return df


def _fetch_and_cache_historical_snaps(season: int) -> pl.DataFrame:
    """Pull a season of snap counts from nflreadpy and cache as CSV.

    Output always has `pfr_id` (renamed from nflreadpy's `pfr_player_id`) so
    every consumer can use one join key regardless of season.
    """
    import os

    cache = _historical_snaps_csv_path(season)
    if os.path.exists(cache):
        try:
            df = pl.read_csv(cache, ignore_errors=True)
            if not df.is_empty():
                return _normalize_snap_schema(df, season)
        except Exception as e:
            logger.warning(f"Reading cached {cache} failed: {e}")

    try:
        import nflreadpy as nfl

        raw = nfl.load_snap_counts(seasons=int(season))
        if raw is None or raw.is_empty():
            logger.info(f"nflreadpy returned no snap counts for {season}")
            return pl.DataFrame()
        raw = _normalize_snap_schema(raw, season)
        try:
            raw.write_csv(cache)
            logger.info(f"Cached {len(raw)} snap rows for {season} at {cache}")
        except Exception as e:
            logger.warning(f"Failed to write cache {cache}: {e}")
        return raw
    except Exception as e:
        logger.warning(f"nflreadpy historical snaps fetch failed for {season}: {e}")
        return pl.DataFrame()
