"""Tier list endpoints: position pool (with rookie flag), persistence, and refresh.

Pool returns every active player at a given position with season-level summary stats —
enough to power tier sorting and visualizations without paying the per-player prediction
cost (callers can fetch full player cards on demand for the ones they care about).
"""
import json
import os
import subprocess
from typing import List, Optional

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import CURRENT_SEASON, DB_CONNECTION_STRING, RAG_DIR, logger
from ..services.utils import calculate_fantasy_points, get_headshot_url, normalize_name
from ..state import model_data

# Cache the encoder + candidate embeddings across requests; they're expensive to build.
_ROOKIE_MATCHER_CACHE: dict = {}

router = APIRouter()

TIER_LIST_FILE = os.path.join(RAG_DIR, "tier_lists.json")
VALID_TIERS = ("UNRANKED", "S", "A", "B", "C", "D", "F")
VALID_POSITIONS = ("QB", "RB", "WR", "TE")


def _rookies_csv_path(season: int) -> str:
    return os.path.join(RAG_DIR, f"espn_rookies_{season}.csv")


# ---------------------------------------------------------------- pool endpoint


def _aggregate_player_stats(stats_df: pl.DataFrame, snaps_df: pl.DataFrame) -> dict:
    """Per-player season aggregates keyed by player_id."""
    out: dict = {}
    if stats_df is None or stats_df.is_empty():
        return out

    # Group stats by player_id; tolerate missing columns.
    cols_needed = [
        c
        for c in (
            "player_id",
            "passing_yards",
            "passing_touchdown",
            "rushing_yards",
            "rush_touchdown",
            "receiving_yards",
            "receiving_touchdown",
            "receptions",
            "targets",
            "rush_attempts",
            "y_fantasy_points_ppr",
            "week",
        )
        if c in stats_df.columns
    ]
    if "player_id" not in cols_needed:
        return out

    df = stats_df.select(cols_needed)
    for pid, group in df.group_by("player_id"):
        # group_by on polars yields tuple of grouping keys
        if isinstance(pid, tuple):
            pid = pid[0]
        if pid is None:
            continue
        rows = group.to_dicts()
        games_played = len(rows)
        if games_played == 0:
            continue

        ppr_pts = [calculate_fantasy_points(r) for r in rows]
        ppr_pts = [p for p in ppr_pts if p is not None]
        season_total = sum(ppr_pts)
        season_avg = season_total / games_played if games_played else 0.0

        # Recent form: average of last 4 non-zero outings.
        sorted_rows = sorted(rows, key=lambda r: r.get("week") or 0, reverse=True)
        recent_pts: List[float] = []
        for r in sorted_rows:
            p = calculate_fantasy_points(r)
            if p > 0:
                recent_pts.append(p)
            if len(recent_pts) >= 4:
                break
        recent_avg = sum(recent_pts) / len(recent_pts) if recent_pts else 0.0

        boom = sum(1 for p in ppr_pts if p >= 20)
        bust = sum(1 for p in ppr_pts if 0 < p < 5)

        out[str(pid)] = {
            "games_played": games_played,
            "season_total_pts": round(season_total, 1),
            "season_avg_pts": round(season_avg, 2),
            "recent_avg_pts": round(recent_avg, 2),
            "boom_games": boom,
            "bust_games": bust,
            "total_yds": int(
                sum(
                    (r.get("passing_yards") or 0)
                    + (r.get("rushing_yards") or 0)
                    + (r.get("receiving_yards") or 0)
                    for r in rows
                )
            ),
            "total_tds": int(
                sum(
                    (r.get("passing_touchdown") or 0)
                    + (r.get("rush_touchdown") or 0)
                    + (r.get("receiving_touchdown") or 0)
                    for r in rows
                )
            ),
            "total_receptions": int(sum((r.get("receptions") or 0) for r in rows)),
            "total_targets": int(sum((r.get("targets") or 0) for r in rows)),
            "total_carries": int(sum((r.get("rush_attempts") or 0) for r in rows)),
        }

    if snaps_df is not None and not snaps_df.is_empty() and "player_id" in snaps_df.columns:
        snap_cols = [c for c in ("player_id", "offense_snaps", "offense_pct") if c in snaps_df.columns]
        if "offense_snaps" in snap_cols and "offense_pct" in snap_cols:
            agg = snaps_df.select(snap_cols).group_by("player_id").agg(
                pl.col("offense_snaps").sum().alias("snaps_total"),
                pl.col("offense_pct").mean().alias("snap_pct_avg"),
            )
            for row in agg.to_dicts():
                pid = str(row.get("player_id"))
                if pid in out:
                    pct = row.get("snap_pct_avg") or 0.0
                    if pct and pct <= 1.0:
                        pct *= 100
                    out[pid]["snaps_total"] = int(row.get("snaps_total") or 0)
                    out[pid]["snap_pct_avg"] = round(float(pct), 1)
    return out


def _load_prior_season_stats(season: int) -> pl.DataFrame:
    """Last-resort fallback: query a prior season's stats table directly from DB."""
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        return pl.read_database_uri(
            f"SELECT * FROM weekly_player_stats_{season}", DB_CONNECTION_STRING
        )
    except Exception as e:
        logger.debug(f"Prior season stats load failed for {season}: {e}")
        return pl.DataFrame()


def _load_prior_season_snaps(season: int) -> pl.DataFrame:
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        return pl.read_database_uri(
            f"SELECT * FROM weekly_snap_counts_{season}", DB_CONNECTION_STRING
        )
    except Exception as e:
        logger.debug(f"Prior season snaps load failed for {season}: {e}")
        return pl.DataFrame()


def get_display_stats_with_fallback() -> tuple[pl.DataFrame, pl.DataFrame, int]:
    """Resolve a (stats_df, snaps_df, season_used) pair for *display* purposes.

    Convention: any endpoint that returns a stat number a user will see should
    use this helper. Model feature inputs (run_base_prediction, parlay
    recommender, etc.) must stay current-season-only — backfilling them with
    prior-season data would feed noise to the predictor.

    Fallback order:
      1. Current-season in-memory (`df_player_stats` / `df_snap_counts`).
      2. Most recent in-memory historical season cached at startup.
      3. DB tables for prior seasons (up to 3 back) if ETL populated them.
    """
    stats_df = model_data.get("df_player_stats", pl.DataFrame())
    snaps_df = model_data.get("df_snap_counts", pl.DataFrame())
    stats_season = int(CURRENT_SEASON)

    if not stats_df.is_empty():
        return stats_df, snaps_df, stats_season

    # Tier 2: in-memory historical (loaded by load_historical_stats at startup).
    hist = model_data.get("df_player_stats_history", pl.DataFrame())
    if not hist.is_empty() and "season" in hist.columns:
        try:
            latest = int(hist.select(pl.col("season").max()).item())
            stats_df = hist.filter(pl.col("season") == latest)
            stats_season = latest

            snaps_hist = model_data.get("df_snap_counts_history", pl.DataFrame())
            if not snaps_hist.is_empty() and "season" in snaps_hist.columns:
                snaps_df = snaps_hist.filter(pl.col("season") == latest)
                # Historical snaps key on pfr_id (nflreadpy's native key); downstream
                # aggregation joins on player_id (gsis). Bridge the two via df_profile
                # so callers don't have to know about the dual-key history.
                profile = model_data.get("df_profile", pl.DataFrame())
                if (
                    not snaps_df.is_empty()
                    and "pfr_id" in snaps_df.columns
                    and "player_id" not in snaps_df.columns
                    and not profile.is_empty()
                    and {"player_id", "pfr_id"}.issubset(profile.columns)
                ):
                    bridge = (
                        profile.select(["player_id", "pfr_id"])
                        .drop_nulls(subset=["player_id", "pfr_id"])
                        .unique(subset=["pfr_id"], keep="last")
                    )
                    snaps_df = snaps_df.join(bridge, on="pfr_id", how="left")
            logger.info(f"Display stats using in-memory historical season {latest}")
            return stats_df, snaps_df, stats_season
        except Exception as e:
            logger.debug(f"Historical display slice failed: {e}")

    # Tier 3: DB prior-season tables (handles cases where in-memory cache failed).
    for back in range(1, 4):
        candidate = int(CURRENT_SEASON) - back
        candidate_stats = _load_prior_season_stats(candidate)
        if not candidate_stats.is_empty():
            stats_df = candidate_stats
            snaps_df = _load_prior_season_snaps(candidate)
            stats_season = candidate
            logger.info(f"Display stats falling back to DB season {candidate}")
            break

    return stats_df, snaps_df, stats_season


@router.get("/team/{team_abbr}/offense")
async def get_team_offense(team_abbr: str):
    """Return the offensive setup for a team — QBs, RBs, WRs, TEs, OLine.

    Used by the player-card popup so you can see who else is on the roster around
    the player you're tiering.
    """
    team = team_abbr.upper().strip()
    profile_df = model_data.get("df_profile", pl.DataFrame())
    if profile_df.is_empty():
        return {"team": team, "qb": [], "rb": [], "wr": [], "te": [], "ol": []}

    team_col = "team_abbr" if "team_abbr" in profile_df.columns else "team"
    roster = profile_df.filter(pl.col(team_col) == team)
    if roster.is_empty():
        return {"team": team, "qb": [], "rb": [], "wr": [], "te": [], "ol": []}

    # Display path: fall back to most recent historical season when the current
    # season has no games yet (preseason / week 1). Keeps the TeamOffenseModal
    # useful in the offseason — veterans show real numbers instead of zeros.
    stats_df, snaps_df, _stats_season = get_display_stats_with_fallback()
    stats_summary = _aggregate_player_stats(stats_df, snaps_df)

    def _row_to_dict(row, pos_group):
        pid = str(row.get("player_id"))
        draft_year = row.get("draft_year")
        try:
            draft_year_int = int(draft_year) if draft_year is not None else None
        except (TypeError, ValueError):
            draft_year_int = None
        agg = stats_summary.get(pid, {})
        return {
            "player_id": pid,
            "player_name": row.get("player_name"),
            "position": row.get("position"),
            "position_group": pos_group,
            "team": team,
            "image": row.get("headshot") or get_headshot_url(pid),
            "injury_status": model_data.get("injury_map", {}).get(pid, row.get("injury_status") or "Active"),
            "is_rookie": draft_year_int == int(CURRENT_SEASON),
            "draft_year": draft_year_int,
            "draft_number": row.get("draft_number"),
            "age": row.get("age"),
            "height": row.get("height"),
            "weight": row.get("weight"),
            "college": row.get("college"),
            # latest-season aggregates (empty for rookies until games are played)
            "season_avg_pts": round(float(agg.get("season_avg_pts", 0.0)), 2),
            "season_total_pts": round(float(agg.get("season_total_pts", 0.0)), 1),
            "recent_avg_pts": round(float(agg.get("recent_avg_pts", 0.0)), 2),
            "games_played": int(agg.get("games_played", 0)),
            "boom_games": int(agg.get("boom_games", 0)),
            "bust_games": int(agg.get("bust_games", 0)),
            "total_yds": int(agg.get("total_yds", 0)),
            "total_tds": int(agg.get("total_tds", 0)),
            "total_receptions": int(agg.get("total_receptions", 0)),
            "total_targets": int(agg.get("total_targets", 0)),
            "total_carries": int(agg.get("total_carries", 0)),
            "snaps_total": int(agg.get("snaps_total", 0)),
            "snap_pct_avg": round(float(agg.get("snap_pct_avg", 0.0)), 1),
        }

    groups: dict[str, list] = {"qb": [], "rb": [], "wr": [], "te": [], "ol": []}
    POSITION_TO_GROUP = {"QB": "qb", "RB": "rb", "FB": "rb", "WR": "wr", "TE": "te", "OL": "ol"}
    for r in roster.iter_rows(named=True):
        grp = POSITION_TO_GROUP.get(str(r.get("position") or "").upper())
        if grp:
            groups[grp].append(_row_to_dict(r, grp))

    # Sort each group: starters first (by snap %), then by season avg pts.
    for k in groups:
        groups[k].sort(
            key=lambda p: (p.get("snap_pct_avg") or 0, p.get("season_avg_pts") or 0),
            reverse=True,
        )

    return {"team": team, **groups}


@router.get("/tier_list/pool/{position}")
async def get_position_pool(position: str, include_rookies_only: bool = False):
    """Return the full pool of active players at a position with season aggregates.

    `position` accepts QB/RB/WR/TE or `ALL` for the combined skill-position pool.
    `is_rookie` is True when the player's `draft_year` matches the current season.
    """
    pos = position.upper().strip()
    if pos != "ALL" and pos not in VALID_POSITIONS:
        raise HTTPException(400, f"Invalid position. Must be one of {VALID_POSITIONS} or ALL")

    profile_df = model_data.get("df_profile", pl.DataFrame())
    if profile_df.is_empty():
        return []

    if pos == "ALL":
        pool = profile_df.filter(pl.col("position").is_in(list(VALID_POSITIONS)))
    else:
        pool = profile_df.filter(pl.col("position") == pos)
    if pool.is_empty():
        return []

    stats_df, snaps_df, stats_season = get_display_stats_with_fallback()
    stats_summary = _aggregate_player_stats(stats_df, snaps_df)

    results = []
    for row in pool.iter_rows(named=True):
        pid = str(row.get("player_id"))
        draft_year = row.get("draft_year")
        try:
            draft_year_int = int(draft_year) if draft_year is not None else None
        except (TypeError, ValueError):
            draft_year_int = None

        is_rookie = draft_year_int == int(CURRENT_SEASON)
        if include_rookies_only and not is_rookie:
            continue

        agg = stats_summary.get(pid, {})
        # When pool=ALL, use the player's own position; otherwise echo the queried pos.
        player_pos = str(row.get("position") or pos).upper() if pos == "ALL" else pos
        results.append(
            {
                "player_id": pid,
                "player_name": row.get("player_name"),
                "position": player_pos,
                "team": row.get("team_abbr") or row.get("team") or "FA",
                "image": row.get("headshot") or get_headshot_url(pid),
                "injury_status": model_data.get("injury_map", {}).get(pid, row.get("injury_status") or "Active"),
                "is_rookie": is_rookie,
                "draft_year": draft_year_int,
                "draft_number": row.get("draft_number"),
                "age": row.get("age"),
                "height": row.get("height"),
                "weight": row.get("weight"),
                "season": int(CURRENT_SEASON),
                "stats_season": stats_season,
                "stats": {
                    "games_played": agg.get("games_played", 0),
                    "season_total_pts": agg.get("season_total_pts", 0.0),
                    "season_avg_pts": agg.get("season_avg_pts", 0.0),
                    "recent_avg_pts": agg.get("recent_avg_pts", 0.0),
                    "boom_games": agg.get("boom_games", 0),
                    "bust_games": agg.get("bust_games", 0),
                    "total_yds": agg.get("total_yds", 0),
                    "total_tds": agg.get("total_tds", 0),
                    "total_receptions": agg.get("total_receptions", 0),
                    "total_targets": agg.get("total_targets", 0),
                    "total_carries": agg.get("total_carries", 0),
                    "snaps_total": agg.get("snaps_total", 0),
                    "snap_pct_avg": agg.get("snap_pct_avg", 0.0),
                },
            }
        )

    # Default sort: highest projected PPG first. (Frontend re-sorts by draft pick
    # when the user has the "Rookies only" filter on.)
    results.sort(key=lambda r: (r["stats"]["season_avg_pts"] or 0, r["is_rookie"]), reverse=True)
    return results


# -------------------------------------------------------- tier-list persistence


class TierAssignment(BaseModel):
    player_id: str
    tier: str  # one of VALID_TIERS


class TierListPayload(BaseModel):
    name: str
    position: str
    assignments: List[TierAssignment]
    season: Optional[int] = None


def _read_lists() -> dict:
    if not os.path.exists(TIER_LIST_FILE):
        return {}
    try:
        with open(TIER_LIST_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read tier lists: {e}")
        return {}


def _write_lists(data: dict):
    os.makedirs(os.path.dirname(TIER_LIST_FILE), exist_ok=True)
    with open(TIER_LIST_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _list_key(name: str, position: str) -> str:
    return f"{position.upper()}::{name.strip()}"


@router.get("/tier_list")
async def list_tier_lists():
    """List all saved tier lists (lightweight summary)."""
    data = _read_lists()
    return [
        {
            "name": v.get("name"),
            "position": v.get("position"),
            "season": v.get("season"),
            "updated_at": v.get("updated_at"),
            "count": len(v.get("assignments", [])),
        }
        for v in data.values()
    ]


@router.get("/tier_list/{position}/{name}")
async def get_tier_list(position: str, name: str):
    pos = position.upper()
    if pos not in VALID_POSITIONS:
        raise HTTPException(400, "Invalid position")
    data = _read_lists()
    entry = data.get(_list_key(name, pos))
    if not entry:
        raise HTTPException(404, "Tier list not found")
    return entry


@router.post("/tier_list")
async def upsert_tier_list(payload: TierListPayload):
    pos = payload.position.upper()
    if pos not in VALID_POSITIONS:
        raise HTTPException(400, "Invalid position")
    for a in payload.assignments:
        if a.tier.upper() not in VALID_TIERS:
            raise HTTPException(400, f"Invalid tier: {a.tier}")

    data = _read_lists()
    from datetime import datetime

    entry = {
        "name": payload.name.strip(),
        "position": pos,
        "season": payload.season or int(CURRENT_SEASON),
        "assignments": [{"player_id": a.player_id, "tier": a.tier.upper()} for a in payload.assignments],
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    data[_list_key(payload.name, pos)] = entry
    _write_lists(data)
    return entry


@router.delete("/tier_list/{position}/{name}")
async def delete_tier_list(position: str, name: str):
    pos = position.upper()
    data = _read_lists()
    key = _list_key(name, pos)
    if key not in data:
        raise HTTPException(404, "Tier list not found")
    del data[key]
    _write_lists(data)
    return {"status": "deleted"}


# --------------------------------------------------------- rookie refresh


def _draft_pick_lookup(season: int) -> dict[str, dict[str, int]]:
    """Build {position: {raw_lower_name: pick_number}} for the given draft year via nflreadpy.

    Scoping by position (like 04_update_snap_counts.py scopes by team) prevents
    cross-position false matches in the fuzzy/encoder fallback tiers.
    """
    try:
        import nflreadpy as nfl

        df = nfl.load_draft_picks(seasons=season)
        if df is None or df.is_empty():
            return {}
        cols = df.columns
        name_col = next(
            (c for c in ("pfr_player_name", "player_name", "full_name") if c in cols), None
        )
        pick_col = next((c for c in ("pick", "overall", "pick_overall") if c in cols), None)
        pos_col = next((c for c in ("position", "pos") if c in cols), None)
        if not name_col or not pick_col:
            return {}
        out: dict[str, dict[str, int]] = {}
        for row in df.iter_rows(named=True):
            name = str(row.get(name_col) or "").strip().lower()
            pick = row.get(pick_col)
            pos = str(row.get(pos_col) or "").upper().strip() if pos_col else "*"
            if not name or pick is None:
                continue
            try:
                out.setdefault(pos, {})[name] = int(pick)
            except (TypeError, ValueError):
                pass
        return out
    except Exception as e:
        logger.debug(f"Draft picks lookup failed: {e}")
        return {}


def _build_rookie_matcher(season: int):
    """Tiered, position-scoped name matcher for rookie → draft pick.

    Follows the same pattern as `rag_data/04_update_snap_counts.py` (scope candidates
    by team, fall back to AI semantic match at threshold 0.80). Here we scope by
    **position** instead of team — a rookie QB only matches QB draft picks.

    Tiers per position:
      1. Exact lower-cased match
      2. `normalize_name` (handles "C.J. Stroud" vs "CJ Stroud")
      3. rapidfuzz token_sort_ratio ≥ 85 (handles "Cam" vs "Cameron")
      4. SentenceTransformer (all-MiniLM-L6-v2) cosine ≥ 0.80 (nicknames / order)

    Embeddings are computed once per position and cached for the process lifetime.
    """
    cache_key = f"matcher_{season}"
    if cache_key in _ROOKIE_MATCHER_CACHE:
        return _ROOKIE_MATCHER_CACHE[cache_key]

    pos_pick_lookup = _draft_pick_lookup(season)
    if not pos_pick_lookup:
        matcher = lambda name, position=None: None  # noqa: E731
        _ROOKIE_MATCHER_CACHE[cache_key] = matcher
        return matcher

    # Per-position normalized lookups + sorted candidate list (for embedding/fuzz).
    norm_lookups: dict[str, dict[str, int]] = {}
    candidate_lists: dict[str, list[str]] = {}
    for pos, names in pos_pick_lookup.items():
        norm_lookups[pos] = {}
        for n, p in names.items():
            nn = normalize_name(n)
            if nn:
                norm_lookups[pos].setdefault(nn, p)
        candidate_lists[pos] = list(names.keys())

    encoder = None
    pos_embeddings: dict[str, "any"] = {}
    try:
        from sentence_transformers import SentenceTransformer, util  # noqa: F401

        encoder = SentenceTransformer("all-MiniLM-L6-v2")
        total = sum(len(v) for v in candidate_lists.values())
        for pos, names in candidate_lists.items():
            if names:
                pos_embeddings[pos] = encoder.encode(
                    names, normalize_embeddings=True, show_progress_bar=False
                )
        logger.info(f"Rookie matcher: encoder ready, {total} candidates across {len(candidate_lists)} positions")
    except Exception as e:
        logger.info(f"Rookie matcher: encoder unavailable, fuzz only ({e})")

    try:
        from rapidfuzz import fuzz, process
    except Exception:
        process = None
        fuzz = None

    AI_THRESHOLD = 0.80  # mirrors AI_MATCH_THRESHOLD in 04_update_snap_counts.py

    def _search(pos: str, raw: str, normalized: str, name: str):
        picks = pos_pick_lookup.get(pos, {})
        if not picks:
            return None
        # Tier 1: exact
        if raw in picks:
            return picks[raw]
        # Tier 2: normalized
        nn = normalized
        if nn and nn in norm_lookups.get(pos, {}):
            return norm_lookups[pos][nn]
        # Tier 3: rapidfuzz
        names = candidate_lists.get(pos, [])
        if process is not None and fuzz is not None and names:
            try:
                best = process.extractOne(raw, names, scorer=fuzz.token_sort_ratio)
                if best and best[1] >= 85:
                    return picks[best[0]]
            except Exception:
                pass
        # Tier 4: semantic encoder
        if encoder is not None and pos in pos_embeddings:
            try:
                import numpy as np

                q = encoder.encode([raw], normalize_embeddings=True, show_progress_bar=False)
                sims = (pos_embeddings[pos] @ q.T).flatten()
                idx = int(np.argmax(sims))
                score = float(sims[idx])
                if score >= AI_THRESHOLD:
                    matched = names[idx]
                    logger.debug(f"AI match: {name!r} ≈ {matched!r} ({score:.2f}, {pos})")
                    return picks[matched]
            except Exception as e:
                logger.debug(f"Encoder match failed for {name!r}: {e}")
        return None

    def match(name: str, position: str | None = None):
        if not name:
            return None
        raw = name.strip().lower()
        nn = normalize_name(name)
        # If position is known, search only that position (precise, fewer collisions).
        if position:
            pos = position.upper().strip()
            res = _search(pos, raw, nn, name)
            if res is not None:
                return res
            # Fall back to other positions for cases like ESPN's "ATH" mislabels.
        for pos in pos_pick_lookup.keys():
            if position and pos == position.upper():
                continue  # already tried
            res = _search(pos, raw, nn, name)
            if res is not None:
                return res
        return None

    _ROOKIE_MATCHER_CACHE[cache_key] = match
    return match


def _fetch_espn_rookies() -> list[dict]:
    """Pull current-season rookies from ESPN's public roster API (free, no key).

    ESPN tags first-year players with `experience.years == 0`. We hit each of the
    32 team rosters and collect those.
    """
    import requests

    teams_url = (
        "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams?limit=40"
    )
    rookies: list[dict] = []
    try:
        r = requests.get(teams_url, timeout=10)
        r.raise_for_status()
        sports = r.json().get("sports", [])
        leagues = sports[0].get("leagues", []) if sports else []
        teams = leagues[0].get("teams", []) if leagues else []
    except Exception as e:
        logger.warning(f"ESPN teams fetch failed: {e}")
        return rookies

    for team_wrapper in teams:
        team = team_wrapper.get("team", {})
        team_abbr = team.get("abbreviation")
        team_id = team.get("id")
        if not team_id:
            continue
        try:
            roster_url = (
                f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster"
            )
            rr = requests.get(roster_url, timeout=10)
            rr.raise_for_status()
            data = rr.json()
        except Exception as e:
            logger.debug(f"ESPN roster fetch failed for team {team_abbr}: {e}")
            continue

        for group in data.get("athletes", []):
            for ath in group.get("items", []):
                exp = ath.get("experience") or {}
                years = exp.get("years")
                pos = (ath.get("position") or {}).get("abbreviation")
                if years == 0 and pos in ("QB", "RB", "WR", "TE"):
                    rookies.append(
                        {
                            "espn_id": str(ath.get("id")),
                            "player_name": ath.get("displayName") or ath.get("fullName"),
                            "first_name": ath.get("firstName"),
                            "last_name": ath.get("lastName"),
                            "position": pos,
                            "team_abbr": team_abbr,
                            "headshot": (ath.get("headshot") or {}).get("href"),
                            "height": ath.get("height"),
                            "weight": ath.get("weight"),
                            "age": ath.get("age"),
                            "draft_year": int(CURRENT_SEASON),
                        }
                    )
    return rookies


def _build_rookie_rows(rookies: list[dict], picks_attached_counter: list[int] | None = None) -> list[dict]:
    """Convert ESPN payload into df_profile-compatible row dicts (with picks resolved)."""
    matcher = _build_rookie_matcher(int(CURRENT_SEASON))
    rows = []
    counter = picks_attached_counter if picks_attached_counter is not None else [0]
    for r in rookies:
        pick = matcher(r["player_name"], r.get("position"))
        if pick is not None:
            counter[0] += 1
        rows.append(
            {
                "player_id": f"espn_{r['espn_id']}",
                "player_name": r["player_name"],
                "position": r["position"],
                "team_abbr": r["team_abbr"],
                "headshot": r["headshot"],
                "height": r.get("height"),
                "weight": r.get("weight"),
                "age": r.get("age"),
                "draft_year": r["draft_year"],
                "draft_number": pick,
                "season": int(CURRENT_SEASON),
                "injury_status": "Active",
                "pfr_id": None,
            }
        )
    return rows


def _merge_rows_into_profile(rows: list[dict]) -> int:
    """Add rookie rows into `df_profile`, deduping by player_name. Returns added count."""
    if not rows:
        return 0
    df = model_data.get("df_profile", pl.DataFrame())
    if df.is_empty():
        return 0

    existing_names = set(df["player_name"].to_list()) if "player_name" in df.columns else set()
    new_rows = [r for r in rows if r["player_name"] not in existing_names]
    if not new_rows:
        return 0

    add_df = pl.DataFrame(new_rows)
    model_data["df_profile"] = pl.concat([df, add_df], how="diagonal_relaxed")
    return len(new_rows)


def _persist_rookies_csv(rows: list[dict]) -> str | None:
    """Write rookie rows to `espn_rookies_{season}.csv` for offline-tolerant restarts."""
    if not rows:
        return None
    try:
        path = _rookies_csv_path(int(CURRENT_SEASON))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pl.DataFrame(rows).write_csv(path)
        return path
    except Exception as e:
        logger.warning(f"Failed to persist rookies CSV: {e}")
        return None


def load_persisted_rookies_into_profile() -> int:
    """Called at startup: merge any previously-saved rookie CSV into df_profile.

    Public (no leading underscore) so it can be imported by main.py / scheduler.
    """
    path = _rookies_csv_path(int(CURRENT_SEASON))
    if not os.path.exists(path):
        return 0
    try:
        df = pl.read_csv(path, ignore_errors=True)
        if df.is_empty():
            return 0
        return _merge_rows_into_profile(df.to_dicts())
    except Exception as e:
        logger.warning(f"Failed to load persisted rookies CSV: {e}")
        return 0


def run_rookie_refresh(persist: bool = True) -> dict:
    """The actual refresh routine — callable from endpoints, startup, and the scheduler."""
    rookies = _fetch_espn_rookies()
    if not rookies:
        return {"ok": False, "found": 0, "added": 0, "persisted": None}
    picks = [0]
    rows = _build_rookie_rows(rookies, picks_attached_counter=picks)
    added = _merge_rows_into_profile(rows)
    backfilled = _backfill_draft_numbers_for_existing_rookies()
    persisted = _persist_rookies_csv(rows) if persist else None
    return {
        "ok": True,
        "found": len(rookies),
        "added": added,
        "draft_picks_attached_on_add": picks[0],
        "draft_picks_backfilled_existing": backfilled,
        "persisted": persisted,
    }


def _merge_espn_rookies_into_profile(rookies: list[dict]) -> tuple[int, int]:
    """Back-compat shim retained for the existing /refresh/rookies endpoint."""
    if not rookies:
        return 0, 0
    picks = [0]
    rows = _build_rookie_rows(rookies, picks_attached_counter=picks)
    added = _merge_rows_into_profile(rows)
    _persist_rookies_csv(rows)
    return added, picks[0]


def _backfill_draft_numbers_for_existing_rookies():
    """Fill draft_number for rookies already in df_profile (e.g., loaded from CSV)
    when the column is null. Uses nflreadpy.load_draft_picks for current season."""
    df = model_data.get("df_profile", pl.DataFrame())
    if df.is_empty() or "draft_year" not in df.columns:
        return 0
    matcher = _build_rookie_matcher(int(CURRENT_SEASON))

    # Find rookies (current-season draft_year) without a draft_number.
    season = int(CURRENT_SEASON)
    needs = df.filter(
        (pl.col("draft_year").cast(pl.Int64, strict=False) == season)
        & (pl.col("draft_number").is_null() if "draft_number" in df.columns else pl.lit(True))
    )
    if needs.is_empty():
        return 0

    # Build mapping {player_id: pick} via the tiered, position-scoped matcher.
    fill: dict[str, int] = {}
    for row in needs.iter_rows(named=True):
        pick = matcher(row.get("player_name"), row.get("position"))
        if pick is not None:
            fill[row["player_id"]] = pick
    if not fill:
        return 0

    # Apply with a CASE-style replace via map.
    fill_df = pl.DataFrame(
        {"player_id": list(fill.keys()), "_pick": list(fill.values())}
    )
    merged = df.join(fill_df, on="player_id", how="left")
    if "draft_number" not in merged.columns:
        merged = merged.with_columns(pl.col("_pick").alias("draft_number"))
    else:
        merged = merged.with_columns(
            pl.coalesce(pl.col("draft_number"), pl.col("_pick")).alias("draft_number")
        )
    model_data["df_profile"] = merged.drop("_pick")
    return len(fill)


@router.post("/refresh/rookies")
async def refresh_rookies(source: str = "all"):
    """Refresh roster data to pick up the latest draft class.

    `source` query param:
      - `nflreadpy` — only re-run the static-files ETL (uses nflreadpy.load_rosters)
      - `espn`      — only fetch from ESPN's public roster API
      - `all`       — try nflreadpy first, then merge ESPN as a fallback (default)
    """
    summary: dict = {"status": "success", "nflreadpy": None, "espn": None}

    if source in ("nflreadpy", "all"):
        script = os.path.join(RAG_DIR, "01_create_static_files.py")
        if os.path.exists(script):
            try:
                result = subprocess.run(
                    ["python3", script],
                    cwd=RAG_DIR,
                    capture_output=True,
                    text=True,
                    timeout=180,
                )
                summary["nflreadpy"] = {
                    "ok": result.returncode == 0,
                    "stderr": (result.stderr or "")[-400:],
                }
                if result.returncode == 0:
                    from ..services.data_loader import refresh_db_data

                    refresh_db_data()
            except subprocess.TimeoutExpired:
                summary["nflreadpy"] = {"ok": False, "stderr": "timeout"}
            except Exception as e:
                summary["nflreadpy"] = {"ok": False, "stderr": str(e)[:400]}
        else:
            summary["nflreadpy"] = {"ok": False, "stderr": "script not found"}

    if source in ("espn", "all"):
        try:
            rookies = _fetch_espn_rookies()
            added, picks_attached = _merge_espn_rookies_into_profile(rookies)
            backfilled = _backfill_draft_numbers_for_existing_rookies()
            summary["espn"] = {
                "ok": True,
                "found": len(rookies),
                "added": added,
                "draft_picks_attached_on_add": picks_attached,
                "draft_picks_backfilled_existing": backfilled,
            }
        except Exception as e:
            logger.exception("ESPN rookie fetch failed")
            summary["espn"] = {"ok": False, "stderr": str(e)[:400]}

    # Compose a friendly message.
    parts = []
    if summary.get("nflreadpy") is not None:
        if summary["nflreadpy"]["ok"]:
            parts.append("nflreadpy refreshed")
        else:
            parts.append(f"nflreadpy failed ({summary['nflreadpy']['stderr'][:80]})")
    if summary.get("espn") is not None:
        if summary["espn"]["ok"]:
            parts.append(
                f"ESPN found {summary['espn']['found']} rookies, added {summary['espn']['added']}"
            )
        else:
            parts.append(f"ESPN failed ({summary['espn']['stderr'][:80]})")
    summary["message"] = "; ".join(parts) or "no work performed"
    return summary
