"""Tier list endpoints: position pool (with rookie flag), persistence, and refresh.

Pool returns every active player at a given position with season-level summary stats —
enough to power tier sorting and visualizations without paying the per-player prediction
cost (callers can fetch full player cards on demand for the ones they care about).
"""
import json
import os
import subprocess
from datetime import datetime
from typing import List, Optional

import polars as pl
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..config import CURRENT_SEASON, DB_CONNECTION_STRING, RAG_DIR, logger
from ..rate_limit import limiter
from ..services.utils import calculate_fantasy_points, get_headshot_url, get_team_abbr, normalize_name
from ..services.adp import get_adp_map, lookup_adp
from ..state import model_data
from ..db import read_db

# Cache the encoder + candidate embeddings across requests; they're expensive to build.
_ROOKIE_MATCHER_CACHE: dict = {}

router = APIRouter()

TIER_LIST_FILE = os.path.join(RAG_DIR, "tier_lists.json")
VALID_TIERS = ("UNRANKED", "S", "A", "B", "C", "D", "F")
VALID_POSITIONS = ("QB", "RB", "WR", "TE")


def _rookies_csv_path(season: int) -> str:
    return os.path.join(RAG_DIR, f"espn_rookies_{season}.csv")


# ---------------------------------------------------------------- pool endpoint


def _number(row: dict, *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _calculate_defensive_points(row: dict) -> float:
    solo = _number(row, "def_tackles_solo")
    assisted = _number(row, "def_tackle_assists")
    tfl = _number(row, "def_tackles_for_loss")
    sacks = _number(row, "def_sacks")
    qb_hits = _number(row, "def_qb_hits")
    interceptions = _number(row, "def_interceptions")
    passes_defended = _number(row, "def_pass_defended", "def_passes_defended", "pass_defended")
    forced_fumbles = _number(row, "def_fumbles_forced")
    recoveries = _number(row, "fumble_recovery_opp", "def_fumble_recoveries")
    touchdowns = _number(row, "def_tds") + _number(row, "fumble_recovery_tds")
    safeties = _number(row, "def_safeties")
    return float(
        solo * 1.5
        + assisted * 0.75
        + tfl * 2.0
        + sacks * 4.0
        + qb_hits * 1.0
        + interceptions * 6.0
        + passes_defended * 1.5
        + forced_fumbles * 4.0
        + recoveries * 4.0
        + touchdowns * 6.0
        + safeties * 2.0
    )


def _has_defensive_stats(row: dict) -> bool:
    return any(
        _number(row, name) > 0
        for name in (
            "def_tackles_solo",
            "def_tackle_assists",
            "def_tackles_for_loss",
            "def_sacks",
            "def_qb_hits",
            "def_interceptions",
            "def_pass_defended",
            "def_passes_defended",
            "pass_defended",
            "def_fumbles_forced",
            "fumble_recovery_opp",
            "def_fumble_recoveries",
            "def_tds",
            "fumble_recovery_tds",
            "def_safeties",
        )
    )


def _empty_player_agg() -> dict:
    return {
        "games_played": 0,
        "season_total_pts": 0.0,
        "season_avg_pts": 0.0,
        "recent_avg_pts": 0.0,
        "boom_games": 0,
        "bust_games": 0,
        "total_yds": 0,
        "total_tds": 0,
        "total_receptions": 0,
        "total_targets": 0,
        "total_carries": 0,
        "def_tackles_total": 0,
        "def_tackles_solo": 0,
        "def_tackle_assists": 0,
        "def_tackles_for_loss": 0,
        "def_sacks": 0.0,
        "def_qb_hits": 0,
        "def_interceptions": 0,
        "def_pass_defended": 0,
        "def_fumbles_forced": 0,
        "def_fumble_recoveries": 0,
        "snaps_total": 0,
        "snap_pct_avg": 0.0,
    }


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
            "def_tackles_solo",
            "def_tackle_assists",
            "def_tackles_for_loss",
            "def_sacks",
            "def_qb_hits",
            "def_interceptions",
            "def_pass_defended",
            "def_passes_defended",
            "pass_defended",
            "def_fumbles_forced",
            "fumble_recovery_opp",
            "def_fumble_recoveries",
            "def_tds",
            "fumble_recovery_tds",
            "def_safeties",
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

        def_rows = [r for r in rows if _has_defensive_stats(r)]
        ppr_pts = [_calculate_defensive_points(r) for r in rows] if def_rows else [calculate_fantasy_points(r) for r in rows]
        ppr_pts = [p for p in ppr_pts if p is not None]
        season_total = sum(ppr_pts)
        season_avg = season_total / games_played if games_played else 0.0

        # Recent form: average of last 4 non-zero outings.
        sorted_rows = sorted(rows, key=lambda r: r.get("week") or 0, reverse=True)
        recent_pts: List[float] = []
        for r in sorted_rows:
            p = _calculate_defensive_points(r) if def_rows else calculate_fantasy_points(r)
            if p > 0:
                recent_pts.append(p)
            if len(recent_pts) >= 4:
                break
        recent_avg = sum(recent_pts) / len(recent_pts) if recent_pts else 0.0

        boom = sum(1 for p in ppr_pts if p >= 20)
        bust = sum(1 for p in ppr_pts if 0 < p < 5)

        def_solo = sum(_number(r, "def_tackles_solo") for r in rows)
        def_assists = sum(_number(r, "def_tackle_assists") for r in rows)
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
            "def_tackles_total": int(round(def_solo + def_assists)),
            "def_tackles_solo": int(round(def_solo)),
            "def_tackle_assists": int(round(def_assists)),
            "def_tackles_for_loss": int(round(sum(_number(r, "def_tackles_for_loss") for r in rows))),
            "def_sacks": round(sum(_number(r, "def_sacks") for r in rows), 1),
            "def_qb_hits": int(round(sum(_number(r, "def_qb_hits") for r in rows))),
            "def_interceptions": int(round(sum(_number(r, "def_interceptions") for r in rows))),
            "def_pass_defended": int(round(sum(_number(r, "def_pass_defended", "def_passes_defended", "pass_defended") for r in rows))),
            "def_fumbles_forced": int(round(sum(_number(r, "def_fumbles_forced") for r in rows))),
            "def_fumble_recoveries": int(round(sum(_number(r, "fumble_recovery_opp", "def_fumble_recoveries") for r in rows))),
        }

    if snaps_df is not None and not snaps_df.is_empty() and "player_id" in snaps_df.columns:
        snap_cols = [c for c in ("player_id", "week", "offense_snaps", "offense_pct", "defense_snaps", "defense_pct") if c in snaps_df.columns]
        if "offense_snaps" in snap_cols or "defense_snaps" in snap_cols:
            agg_exprs = []
            if "offense_snaps" in snap_cols:
                agg_exprs.append(pl.col("offense_snaps").sum().alias("offense_snaps_total"))
            else:
                agg_exprs.append(pl.lit(0).alias("offense_snaps_total"))
            if "offense_pct" in snap_cols:
                agg_exprs.append(pl.col("offense_pct").mean().alias("offense_pct_avg"))
            else:
                agg_exprs.append(pl.lit(0.0).alias("offense_pct_avg"))
            if "defense_snaps" in snap_cols:
                agg_exprs.append(pl.col("defense_snaps").sum().alias("defense_snaps_total"))
            else:
                agg_exprs.append(pl.lit(0).alias("defense_snaps_total"))
            if "defense_pct" in snap_cols:
                agg_exprs.append(pl.col("defense_pct").mean().alias("defense_pct_avg"))
            else:
                agg_exprs.append(pl.lit(0.0).alias("defense_pct_avg"))
            if "week" in snap_cols:
                agg_exprs.append(pl.col("week").n_unique().alias("snap_games"))
            agg = snaps_df.select(snap_cols).group_by("player_id").agg(agg_exprs)
            for row in agg.to_dicts():
                pid = str(row.get("player_id"))
                if pid not in out:
                    out[pid] = _empty_player_agg()
                    out[pid]["games_played"] = int(row.get("snap_games") or 0)
                offense_snaps = float(row.get("offense_snaps_total") or 0.0)
                defense_snaps = float(row.get("defense_snaps_total") or 0.0)
                use_defense = defense_snaps > offense_snaps
                snaps_total = defense_snaps if use_defense else offense_snaps
                pct = row.get("defense_pct_avg" if use_defense else "offense_pct_avg") or 0.0
                if pct and pct <= 1.0:
                    pct *= 100
                out[pid]["snaps_total"] = int(snaps_total or 0)
                out[pid]["snap_pct_avg"] = round(float(pct), 1)
    return out


def _load_prior_season_stats(season: int) -> pl.DataFrame:
    """Last-resort fallback: query a prior season's stats table directly from DB."""
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        return read_db(f"SELECT * FROM weekly_player_stats_{season}")
    except Exception as e:
        logger.debug(f"Prior season stats load failed for {season}: {e}")
        return pl.DataFrame()


def _load_prior_season_snaps(season: int) -> pl.DataFrame:
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        return read_db(f"SELECT * FROM weekly_snap_counts_{season}")
    except Exception as e:
        logger.debug(f"Prior season snaps load failed for {season}: {e}")
        return pl.DataFrame()


def _load_player_stats_for_season(season: int) -> pl.DataFrame:
    """Load player weekly stats for a completed season from memory, DB, or CSV."""
    hist = model_data.get("df_player_stats_history", pl.DataFrame())
    if not hist.is_empty() and "season" in hist.columns:
        try:
            df = hist.filter(pl.col("season").cast(pl.Int64, strict=False) == int(season))
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"In-memory historical player stats slice failed for {season}: {e}")

    df = _load_prior_season_stats(int(season))
    if not df.is_empty():
        return df

    csv_path = os.path.join(RAG_DIR, f"weekly_player_stats_{season}.csv")
    if os.path.exists(csv_path):
        try:
            df = pl.read_csv(csv_path, ignore_errors=True)
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"Player stats CSV load failed for {season}: {e}")

    try:
        import nflreadpy as nfl

        df = nfl.load_player_stats(seasons=int(season))
        if df is None or df.is_empty():
            return pl.DataFrame()
        rename_map = {
            "passing_tds": "passing_touchdown",
            "rushing_tds": "rush_touchdown",
            "receiving_tds": "receiving_touchdown",
            "carries": "rush_attempts",
            "passing_interceptions": "interceptions",
            "fantasy_points_ppr": "y_fantasy_points_ppr",
        }
        rename_map = {src: dst for src, dst in rename_map.items() if src in df.columns}
        if rename_map:
            df = df.rename(rename_map)
        if "season" not in df.columns:
            df = df.with_columns(pl.lit(int(season)).alias("season"))
        return df
    except Exception as e:
        logger.debug(f"nflreadpy player stats load failed for {season}: {e}")
        return pl.DataFrame()


def _load_snap_counts_for_season(season: int) -> pl.DataFrame:
    """Load weekly snap counts for a completed season from memory, DB, CSV, or nflreadpy."""
    hist = model_data.get("df_snap_counts_history", pl.DataFrame())
    if not hist.is_empty() and "season" in hist.columns:
        try:
            df = hist.filter(pl.col("season").cast(pl.Int64, strict=False) == int(season))
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"In-memory historical snap-count slice failed for {season}: {e}")

    df = _load_prior_season_snaps(int(season))
    if not df.is_empty():
        return df

    csv_path = os.path.join(RAG_DIR, f"weekly_snap_counts_{season}.csv")
    if os.path.exists(csv_path):
        try:
            df = pl.read_csv(csv_path, ignore_errors=True)
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"Snap-count CSV load failed for {season}: {e}")

    try:
        import nflreadpy as nfl

        df = nfl.load_snap_counts(seasons=int(season))
        if df is None or df.is_empty():
            return pl.DataFrame()
        if "season" not in df.columns:
            df = df.with_columns(pl.lit(int(season)).alias("season"))
        return df
    except Exception as e:
        logger.debug(f"nflreadpy snap-count load failed for {season}: {e}")
        return pl.DataFrame()


def _fantasy_position_group(row: dict) -> str | None:
    pos = str(row.get("position_group") or row.get("position") or "").upper()
    if pos in {"QB", "RB", "WR", "TE", "DL", "LB", "DB"}:
        return pos
    pos = str(row.get("position") or "").upper()
    if pos == "FB":
        return "RB"
    if pos in {"QB", "RB", "WR", "TE", "DL", "LB", "DB"}:
        return pos
    return None


def _calculate_last_season_position_points(row: dict, position: str) -> float:
    if position in {"QB", "RB", "WR", "TE"}:
        return float(calculate_fantasy_points(row) or 0.0)

    # IDP-style scoring for defensive position finishes. This gives fantasy
    # players a useful within-position production signal even though the app's
    # main projection models are offensive-player focused.
    solo = row.get("def_tackles_solo") or 0
    assisted = row.get("def_tackle_assists") or 0
    tfl = row.get("def_tackles_for_loss") or 0
    sacks = row.get("def_sacks") or 0
    qb_hits = row.get("def_qb_hits") or 0
    interceptions = row.get("def_interceptions") or 0
    passes_defended = row.get("def_pass_defended") or 0
    forced_fumbles = row.get("def_fumbles_forced") or 0
    recoveries = row.get("fumble_recovery_opp") or 0
    touchdowns = (row.get("def_tds") or 0) + (row.get("fumble_recovery_tds") or 0)
    safeties = row.get("def_safeties") or 0
    return float(
        solo * 1.5
        + assisted * 0.75
        + tfl * 2.0
        + sacks * 4.0
        + qb_hits * 1.0
        + interceptions * 6.0
        + passes_defended * 1.5
        + forced_fumbles * 4.0
        + recoveries * 4.0
        + touchdowns * 6.0
        + safeties * 2.0
    )


def _rank_desc(values: list[tuple[str, float]]) -> dict[str, int]:
    values = sorted(values, key=lambda item: item[1], reverse=True)
    ranks: dict[str, int] = {}
    previous_value = None
    previous_rank = 0
    for idx, (player_id, value) in enumerate(values, start=1):
        rank = previous_rank if previous_value is not None and value == previous_value else idx
        ranks[player_id] = rank
        previous_value = value
        previous_rank = rank
    return ranks


def _rank_asc(values: list[tuple[str, float]]) -> dict[str, int]:
    values = sorted(values, key=lambda item: item[1])
    ranks: dict[str, int] = {}
    previous_value = None
    previous_rank = 0
    for idx, (key, value) in enumerate(values, start=1):
        rank = previous_rank if previous_value is not None and value == previous_value else idx
        ranks[key] = rank
        previous_value = value
        previous_rank = rank
    return ranks


_OL_SNAP_POSITIONS = {"OL", "T", "G", "C", "OT", "OG", "LT", "LG", "RT", "RG"}


def _get_last_season_team_sack_context(last_season: int) -> dict[str, dict]:
    """Return team-level sacks-taken context for last-season OL rows."""
    cache = model_data.get("team_sack_context_cache")
    if cache and cache.get("season") == last_season:
        return cache.get("context", {})

    offense_df = _load_team_weekly_table(last_season, "offense")
    if offense_df.is_empty() or "sacks_suffered" not in offense_df.columns:
        fallback_offense, _fallback_defense = _fetch_team_weekly_from_nflreadpy(last_season)
        if not fallback_offense.is_empty():
            offense_df = fallback_offense

    if offense_df.is_empty() or "sacks_suffered" not in offense_df.columns:
        return {}

    aggregates: dict[str, dict] = {}
    for row in offense_df.to_dicts():
        if row.get("season") is not None:
            try:
                if int(row.get("season")) != int(last_season):
                    continue
            except (TypeError, ValueError):
                pass
        if row.get("season_type") and str(row.get("season_type")).upper() != "REG":
            continue
        if row.get("game_type") and str(row.get("game_type")).upper() != "REG":
            continue

        team = get_team_abbr(row.get("team_abbr") or row.get("team"))
        if not team or team == "None":
            continue
        bucket = aggregates.setdefault(team, {"sacks": 0.0, "rush_tds": 0.0, "games": 0})
        bucket["sacks"] += _number(row, "sacks_suffered")
        bucket["rush_tds"] += _number(row, "rushing_tds", "rush_touchdown", "rushing_touchdowns")
        bucket["games"] += 1

    values = [
        (team, data["sacks"] / data["games"])
        for team, data in aggregates.items()
        if data["games"] > 0
    ]
    ranks = _rank_asc(values)
    rush_values = [
        (team, data["rush_tds"] / data["games"])
        for team, data in aggregates.items()
        if data["games"] > 0
    ]
    rush_ranks = _rank_desc(rush_values)
    out_of = len(values)
    context = {
        team: {
            "team": team,
            "sacks_taken": round(float(data["sacks"]), 1),
            "sacks_taken_per_game": round(float(data["sacks"]) / data["games"], 2) if data["games"] else 0.0,
            "sacks_taken_rank": ranks.get(team),
            "sacks_taken_rank_out_of": out_of,
            "rush_tds": round(float(data["rush_tds"]), 1),
            "rush_tds_per_game": round(float(data["rush_tds"]) / data["games"], 2) if data["games"] else 0.0,
            "rush_tds_rank": rush_ranks.get(team),
            "rush_tds_rank_out_of": len(rush_values),
        }
        for team, data in aggregates.items()
        if data["games"] > 0
    }
    model_data["team_sack_context_cache"] = {
        "season": last_season,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "context": context,
    }
    return context


def _add_last_season_ol_rankings(rankings: dict, last_season: int) -> None:
    """Add OL last-season finishes using offensive snap volume.

    Linemen do not produce meaningful fantasy-point totals. For team-overview
    context, rank OL by total offensive snaps, with average offensive snap rate
    as the companion rate rank.
    """
    snap_df = _load_snap_counts_for_season(last_season)
    if snap_df.is_empty():
        return

    sack_context = _get_last_season_team_sack_context(last_season)
    aggregates: dict[str, dict] = {}
    for row in snap_df.to_dicts():
        if row.get("season") is not None:
            try:
                if int(row.get("season")) != int(last_season):
                    continue
            except (TypeError, ValueError):
                pass
        if row.get("game_type") and str(row.get("game_type")).upper() != "REG":
            continue

        position = str(row.get("position") or "").upper()
        if position not in _OL_SNAP_POSITIONS:
            continue

        try:
            offense_snaps = float(row.get("offense_snaps") or 0.0)
        except (TypeError, ValueError):
            offense_snaps = 0.0
        if offense_snaps <= 0:
            continue

        raw_name = row.get("player") or row.get("player_name") or row.get("player_display_name")
        normalized = normalize_name(raw_name)
        pfr_id = str(row.get("pfr_id") or row.get("pfr_player_id") or "").strip()
        key = pfr_id or f"name:{normalized}"
        if not key or key == "name:":
            continue

        try:
            offense_pct = float(row.get("offense_pct") or 0.0)
        except (TypeError, ValueError):
            offense_pct = 0.0
        if offense_pct <= 1.5:
            offense_pct *= 100.0

        bucket = aggregates.setdefault(
            key,
            {
                "normalized": normalized,
                "total_snaps": 0.0,
                "snap_pct_values": [],
                "games": 0,
                "team_snaps": {},
            },
        )
        bucket["total_snaps"] += offense_snaps
        bucket["snap_pct_values"].append(offense_pct)
        bucket["games"] += 1
        team = get_team_abbr(row.get("team"))
        if team and team != "None":
            bucket["team_snaps"][team] = bucket["team_snaps"].get(team, 0.0) + offense_snaps

    values = [(key, float(data["total_snaps"])) for key, data in aggregates.items() if data["total_snaps"] > 0]
    if not values:
        return

    total_ranks = _rank_desc(values)
    rate_values = [
        (key, sum(data["snap_pct_values"]) / len(data["snap_pct_values"]))
        for key, data in aggregates.items()
        if data["snap_pct_values"]
    ]
    rate_ranks = _rank_desc(rate_values)
    out_of = len(values)

    by_key: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for key, data in aggregates.items():
        if key not in total_ranks:
            continue
        avg_snap_pct = (
            sum(data["snap_pct_values"]) / len(data["snap_pct_values"])
            if data["snap_pct_values"]
            else 0.0
        )
        last_team = None
        if data.get("team_snaps"):
            last_team = max(data["team_snaps"].items(), key=lambda item: item[1])[0]
        last_team_sacks = sack_context.get(last_team or "", {})
        info = {
            "last_season": last_season,
            "last_season_rank_position": "OL",
            "last_season_games_played": int(data["games"]),
            "last_season_total_pts": round(float(data["total_snaps"]), 1),
            "last_season_avg_pts": round(float(avg_snap_pct), 2),
            "last_season_position_rank": total_ranks.get(key),
            "last_season_position_ppg_rank": rate_ranks.get(key),
            "last_season_position_rank_out_of": out_of,
            "last_season_team": last_team,
            "last_season_team_sacks_taken": last_team_sacks.get("sacks_taken"),
            "last_season_team_sacks_per_game": last_team_sacks.get("sacks_taken_per_game"),
            "last_season_team_sacks_rank": last_team_sacks.get("sacks_taken_rank"),
            "last_season_team_sacks_rank_out_of": last_team_sacks.get("sacks_taken_rank_out_of"),
            "last_season_team_rush_tds": last_team_sacks.get("rush_tds"),
            "last_season_team_rush_tds_per_game": last_team_sacks.get("rush_tds_per_game"),
            "last_season_team_rush_tds_rank": last_team_sacks.get("rush_tds_rank"),
            "last_season_team_rush_tds_rank_out_of": last_team_sacks.get("rush_tds_rank_out_of"),
        }
        by_key[key] = info
        normalized = data.get("normalized")
        if normalized and (
            normalized not in by_name
            or info["last_season_total_pts"] > by_name[normalized]["last_season_total_pts"]
        ):
            by_name[normalized] = info

    profile_df = model_data.get("df_profile", pl.DataFrame())
    if profile_df.is_empty() or "player_id" not in profile_df.columns:
        return

    for row in profile_df.to_dicts():
        position = str(row.get("position") or "").upper()
        if position not in {"OL", "T", "G", "C", "OT", "OG"}:
            continue
        pid = row.get("player_id")
        if not pid:
            continue
        pfr_id = str(row.get("pfr_id") or "").strip()
        normalized = normalize_name(row.get("player_name"))
        info = by_key.get(pfr_id) if pfr_id else None
        if not info:
            info = by_name.get(normalized)
        if info:
            rankings[str(pid)] = dict(info)


def _get_last_season_player_rankings() -> dict:
    """Return per-player 2025-style positional fantasy finishes.

    Offensive players use PPR total within QB/RB/WR/TE. Defensive players use an
    IDP-style production score within DL/LB/DB. OL uses offensive snap volume
    because linemen do not have meaningful fantasy points. `ppg_rank` is also
    returned for context, but the visible rank should match common
    season-finish language.
    """
    last_season = int(CURRENT_SEASON) - 1
    cache = model_data.get("player_position_rankings_cache")
    if cache and cache.get("season") == last_season:
        return cache.get("rankings", {})

    rankings: dict[str, dict] = {}
    stats_df = _load_player_stats_for_season(last_season)
    if stats_df.is_empty() or "player_id" not in stats_df.columns:
        _add_last_season_ol_rankings(rankings, last_season)
        model_data["player_position_rankings_cache"] = {
            "season": last_season,
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "rankings": rankings,
        }
        return rankings

    if "season" in stats_df.columns:
        try:
            stats_df = stats_df.filter(pl.col("season").cast(pl.Int64, strict=False) == last_season)
        except Exception:
            pass
    if "season_type" in stats_df.columns:
        try:
            stats_df = stats_df.filter(pl.col("season_type") == "REG")
        except Exception:
            pass
    if stats_df.is_empty():
        _add_last_season_ol_rankings(rankings, last_season)
        model_data["player_position_rankings_cache"] = {
            "season": last_season,
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "rankings": rankings,
        }
        return rankings

    grouped_by_position: dict[str, list[tuple[str, float, float]]] = {
        "QB": [],
        "RB": [],
        "WR": [],
        "TE": [],
        "DL": [],
        "LB": [],
        "DB": [],
    }

    for player_id, group in stats_df.group_by("player_id"):
        if isinstance(player_id, tuple):
            player_id = player_id[0]
        if player_id is None:
            continue

        rows = group.to_dicts()
        position = None
        for row in rows:
            position = _fantasy_position_group(row)
            if position:
                break
        if not position:
            continue

        points = [_calculate_last_season_position_points(row, position) for row in rows]
        games = len(points)
        if games == 0:
            continue
        total = sum(points)
        avg = total / games if games else 0.0
        pid = str(player_id)
        rankings[pid] = {
            "last_season": last_season,
            "last_season_rank_position": position,
            "last_season_games_played": games,
            "last_season_total_pts": round(total, 1),
            "last_season_avg_pts": round(avg, 2),
        }
        grouped_by_position[position].append((pid, total, avg))

    for position, values in grouped_by_position.items():
        total_ranks = _rank_desc([(pid, total) for pid, total, _avg in values])
        ppg_ranks = _rank_desc([(pid, avg) for pid, _total, avg in values])
        out_of = len(values)
        for pid, _total, _avg in values:
            if pid in rankings:
                rankings[pid]["last_season_position_rank"] = total_ranks.get(pid)
                rankings[pid]["last_season_position_ppg_rank"] = ppg_ranks.get(pid)
                rankings[pid]["last_season_position_rank_out_of"] = out_of

    _add_last_season_ol_rankings(rankings, last_season)

    model_data["player_position_rankings_cache"] = {
        "season": last_season,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rankings": rankings,
    }
    return rankings


def _empty_team_payload(team: str) -> dict:
    last_season = int(CURRENT_SEASON) - 1
    return {
        "team": team,
        "last_season": last_season,
        "rankings": None,
        "freshness": _team_payload_freshness(),
        "qb": [],
        "rb": [],
        "wr": [],
        "te": [],
        "ol": [],
        "defense": {"dl": [], "lb": [], "db": []},
    }


def _team_payload_freshness() -> dict:
    return {
        "roster_loaded_at": model_data.get("data_loaded_at"),
        "depth_charts_loaded_at": model_data.get("depth_charts_loaded_at"),
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def _load_team_weekly_table(season: int, side: str) -> pl.DataFrame:
    """Load a prior-season team stats table from DB, then CSV cache."""
    table = f"weekly_{side}_stats_{season}"
    if DB_CONNECTION_STRING:
        try:
            df = read_db(f"SELECT * FROM {table}")
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"{table} DB load failed: {e}")

    csv_path = os.path.join(RAG_DIR, f"{table}.csv")
    if os.path.exists(csv_path):
        try:
            df = pl.read_csv(csv_path, ignore_errors=True)
            if not df.is_empty():
                return df
        except Exception as e:
            logger.debug(f"{table} CSV load failed: {e}")
    return pl.DataFrame()


def _col_or_zero(df: pl.DataFrame, name: str, alias: str | None = None):
    expr = pl.col(name) if name in df.columns else pl.lit(0)
    return expr.alias(alias or name)


def _fetch_team_weekly_from_nflreadpy(season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build last-season offense/defense frames directly from nflreadpy.

    This is the fallback when historical team stat tables are not in Postgres.
    It uses regular-season weekly team stats plus schedule scores.
    """
    try:
        import nflreadpy as nfl

        team_stats = nfl.load_team_stats(seasons=int(season), summary_level="week")
        if team_stats is None or team_stats.is_empty():
            return pl.DataFrame(), pl.DataFrame()
        if "season_type" in team_stats.columns:
            team_stats = team_stats.filter(pl.col("season_type") == "REG")

        schedule = nfl.load_schedules(seasons=[int(season)])
        if schedule is not None and not schedule.is_empty() and "game_type" in schedule.columns:
            schedule = schedule.filter(pl.col("game_type") == "REG")

        points_table = pl.DataFrame()
        if schedule is not None and not schedule.is_empty():
            needed = {"home_team", "away_team", "home_score", "away_score", "week"}
            if needed.issubset(set(schedule.columns)):
                home_scores = schedule.select(
                    pl.col("home_team").alias("team"),
                    "week",
                    pl.col("home_score").alias("points_for"),
                    pl.col("away_score").alias("points_allowed"),
                )
                away_scores = schedule.select(
                    pl.col("away_team").alias("team"),
                    "week",
                    pl.col("away_score").alias("points_for"),
                    pl.col("home_score").alias("points_allowed"),
                )
                points_table = pl.concat([home_scores, away_scores], how="diagonal_relaxed")

        core_off = team_stats.select(
            pl.col("team").alias("team_abbr"),
            "week",
            _col_or_zero(team_stats, "passing_yards"),
            _col_or_zero(team_stats, "rushing_yards"),
            _col_or_zero(team_stats, "rushing_tds"),
            _col_or_zero(team_stats, "passing_interceptions"),
            _col_or_zero(team_stats, "sacks_suffered"),
            _col_or_zero(team_stats, "sack_fumbles_lost"),
            _col_or_zero(team_stats, "rushing_fumbles_lost"),
            _col_or_zero(team_stats, "receiving_fumbles_lost"),
        )
        if not points_table.is_empty():
            core_off = core_off.join(
                points_table.select(pl.col("team").alias("team_abbr"), "week", pl.col("points_for").alias("points_scored")),
                on=["team_abbr", "week"],
                how="left",
            )
        else:
            core_off = core_off.with_columns(pl.lit(None).cast(pl.Float64).alias("points_scored"))
        core_off = core_off.with_columns(
            (pl.col("passing_yards") + pl.col("rushing_yards")).alias("total_yards"),
            (
                pl.col("passing_interceptions")
                + pl.col("sack_fumbles_lost")
                + pl.col("rushing_fumbles_lost")
                + pl.col("receiving_fumbles_lost")
            ).alias("turnovers"),
            pl.lit(int(season)).alias("season"),
        )

        opponent_offense = team_stats.select(
            pl.col("team").alias("opponent_team"),
            "week",
            pl.col("passing_yards").alias("passing_yards_allowed"),
            pl.col("rushing_yards").alias("rushing_yards_allowed"),
        )
        core_def = team_stats.select(
            pl.col("team").alias("team_abbr"),
            "week",
            "opponent_team",
            _col_or_zero(team_stats, "def_sacks"),
            _col_or_zero(team_stats, "def_interceptions"),
            _col_or_zero(team_stats, "def_qb_hits"),
            _col_or_zero(team_stats, "def_fumbles_forced"),
            _col_or_zero(team_stats, "fumble_recovery_opp"),
        ).join(opponent_offense, on=["opponent_team", "week"], how="left")
        if not points_table.is_empty():
            core_def = core_def.join(
                points_table.select(pl.col("team").alias("team_abbr"), "week", "points_allowed"),
                on=["team_abbr", "week"],
                how="left",
            )
        else:
            core_def = core_def.with_columns(pl.lit(None).cast(pl.Float64).alias("points_allowed"))
        core_def = core_def.with_columns(
            (pl.col("passing_yards_allowed") + pl.col("rushing_yards_allowed")).alias("yards_allowed"),
            (pl.col("def_interceptions") + pl.col("fumble_recovery_opp")).alias("takeaways"),
            pl.lit(int(season)).alias("season"),
        )
        return core_off, core_def
    except Exception as e:
        logger.warning(f"nflreadpy team ranking fallback failed for {season}: {e}")
        return pl.DataFrame(), pl.DataFrame()


def _normalize_team_column(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    if "team_abbr" not in df.columns and "team" in df.columns:
        df = df.rename({"team": "team_abbr"})
    if "team_abbr" in df.columns:
        df = df.with_columns(pl.col("team_abbr").map_elements(get_team_abbr, return_dtype=pl.Utf8))
    return df


def _ensure_rank_columns(offense_df: pl.DataFrame, defense_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    if not offense_df.is_empty():
        offense_df = _normalize_team_column(offense_df)
        exprs = []
        if "total_yards" not in offense_df.columns and {"passing_yards", "rushing_yards"}.issubset(set(offense_df.columns)):
            exprs.append((pl.col("passing_yards") + pl.col("rushing_yards")).alias("total_yards"))
        if "turnovers" not in offense_df.columns:
            turnover_cols = [
                c
                for c in ("passing_interceptions", "sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost")
                if c in offense_df.columns
            ]
            if turnover_cols:
                expr = sum((pl.col(c).fill_null(0) for c in turnover_cols), pl.lit(0))
                exprs.append(expr.alias("turnovers"))
        if exprs:
            offense_df = offense_df.with_columns(exprs)

    if not defense_df.is_empty():
        defense_df = _normalize_team_column(defense_df)
        exprs = []
        if "yards_allowed" not in defense_df.columns and {"passing_yards_allowed", "rushing_yards_allowed"}.issubset(set(defense_df.columns)):
            exprs.append((pl.col("passing_yards_allowed") + pl.col("rushing_yards_allowed")).alias("yards_allowed"))
        if "takeaways" not in defense_df.columns:
            if "fumble_recovery_opp" in defense_df.columns:
                exprs.append((pl.col("def_interceptions").fill_null(0) + pl.col("fumble_recovery_opp").fill_null(0)).alias("takeaways"))
            elif "def_fumbles_forced" in defense_df.columns:
                exprs.append((pl.col("def_interceptions").fill_null(0) + pl.col("def_fumbles_forced").fill_null(0)).alias("takeaways"))
        if exprs:
            defense_df = defense_df.with_columns(exprs)
    return offense_df, defense_df


def _build_side_rankings(df: pl.DataFrame, metric_defs: list[dict]) -> dict:
    if df.is_empty() or "team_abbr" not in df.columns:
        return {}
    exprs = [pl.len().alias("games")]
    for metric in metric_defs:
        source = metric["source"]
        if source in df.columns:
            exprs.append(pl.col(source).cast(pl.Float64, strict=False).mean().alias(metric["key"]))
    grouped = df.group_by("team_abbr").agg(exprs).to_dicts()
    if not grouped:
        return {}

    rank_lookup: dict[tuple[str, str], int] = {}
    rank_out_of: dict[str, int] = {}
    for metric in metric_defs:
        key = metric["key"]
        values = [
            (str(row["team_abbr"]), float(row[key]))
            for row in grouped
            if row.get(key) is not None
        ]
        values.sort(key=lambda item: item[1], reverse=not metric.get("lower_is_better", False))
        rank_out_of[key] = len(values)
        prev_val = None
        prev_rank = 0
        for idx, (team, value) in enumerate(values, start=1):
            rank = prev_rank if prev_val is not None and value == prev_val else idx
            rank_lookup[(team, key)] = rank
            prev_val = value
            prev_rank = rank

    out: dict[str, dict] = {}
    overall_scores = []
    for row in grouped:
        team = str(row["team_abbr"])
        metrics = {}
        ranks = []
        for metric in metric_defs:
            key = metric["key"]
            value = row.get(key)
            rank = rank_lookup.get((team, key))
            if value is None or rank is None:
                continue
            ranks.append(rank)
            metrics[key] = {
                "label": metric["label"],
                "value": round(float(value), metric.get("decimals", 1)),
                "rank": int(rank),
                "rank_out_of": int(rank_out_of.get(key, 0)),
                "lower_is_better": bool(metric.get("lower_is_better", False)),
            }
        if metrics:
            avg_rank = sum(ranks) / len(ranks)
            overall_scores.append((team, avg_rank))
            out[team] = {"metrics": metrics, "average_rank": avg_rank}

    overall_scores.sort(key=lambda item: item[1])
    previous_score = None
    previous_rank = 0
    for idx, (team, score) in enumerate(overall_scores, start=1):
        rank = previous_rank if previous_score is not None and score == previous_score else idx
        if team in out:
            out[team]["overall"] = {
                "label": "Overall",
                "value": round(score, 1),
                "rank": int(rank),
                "rank_out_of": len(overall_scores),
                "lower_is_better": True,
            }
        previous_score = score
        previous_rank = rank
    return out


def _get_last_season_team_rankings(team: str) -> dict | None:
    last_season = int(CURRENT_SEASON) - 1
    cache = model_data.get("team_rankings_cache")
    if cache and cache.get("season") == last_season:
        rankings = cache.get("rankings", {})
        return rankings.get(team)

    offense_df = _load_team_weekly_table(last_season, "offense")
    defense_df = _load_team_weekly_table(last_season, "defense")
    source = "postgres_or_csv"
    if offense_df.is_empty() or defense_df.is_empty():
        offense_df, defense_df = _fetch_team_weekly_from_nflreadpy(last_season)
        source = "nflreadpy"

    offense_df, defense_df = _ensure_rank_columns(offense_df, defense_df)
    if offense_df.is_empty() and defense_df.is_empty():
        return None

    offense_metrics = [
        {"key": "points_per_game", "source": "points_scored", "label": "Points/G", "decimals": 1},
        {"key": "yards_per_game", "source": "total_yards", "label": "Yards/G", "decimals": 1},
        {"key": "pass_yards_per_game", "source": "passing_yards", "label": "Pass Yds/G", "decimals": 1},
        {"key": "rush_yards_per_game", "source": "rushing_yards", "label": "Rush Yds/G", "decimals": 1},
        {"key": "sacks_taken_per_game", "source": "sacks_suffered", "label": "Sacks Taken/G", "decimals": 2, "lower_is_better": True},
        {"key": "turnovers_per_game", "source": "turnovers", "label": "Turnovers/G", "decimals": 2, "lower_is_better": True},
    ]
    defense_metrics = [
        {"key": "points_allowed_per_game", "source": "points_allowed", "label": "Pts Allowed/G", "decimals": 1, "lower_is_better": True},
        {"key": "yards_allowed_per_game", "source": "yards_allowed", "label": "Yds Allowed/G", "decimals": 1, "lower_is_better": True},
        {"key": "pass_yards_allowed_per_game", "source": "passing_yards_allowed", "label": "Pass Yds Allowed/G", "decimals": 1, "lower_is_better": True},
        {"key": "rush_yards_allowed_per_game", "source": "rushing_yards_allowed", "label": "Rush Yds Allowed/G", "decimals": 1, "lower_is_better": True},
        {"key": "sacks_per_game", "source": "def_sacks", "label": "Sacks/G", "decimals": 2},
        {"key": "takeaways_per_game", "source": "takeaways", "label": "Takeaways/G", "decimals": 2},
    ]
    offense_rankings = _build_side_rankings(offense_df, offense_metrics)
    defense_rankings = _build_side_rankings(defense_df, defense_metrics)

    teams = set(offense_rankings.keys()) | set(defense_rankings.keys())
    rankings = {
        t: {
            "season": last_season,
            "source": source,
            "offense": offense_rankings.get(t),
            "defense": defense_rankings.get(t),
        }
        for t in teams
    }
    model_data["team_rankings_cache"] = {
        "season": last_season,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rankings": rankings,
    }
    return rankings.get(team)


@router.get("/team/{team_abbr}/offense")
async def get_team_offense(team_abbr: str):
    """Return the offensive setup for a team — QBs, RBs, WRs, TEs, OLine.

    Used by the player-card popup so you can see who else is on the roster around
    the player you're tiering.
    """
    team = team_abbr.upper().strip()
    profile_df = model_data.get("df_profile", pl.DataFrame())
    if profile_df.is_empty():
        payload = _empty_team_payload(team)
        payload["rankings"] = _get_last_season_team_rankings(team)
        return payload

    team_col = "team_abbr" if "team_abbr" in profile_df.columns else "team"
    roster = profile_df.filter(pl.col(team_col) == team)
    if roster.is_empty():
        payload = _empty_team_payload(team)
        payload["rankings"] = _get_last_season_team_rankings(team)
        return payload

    stats_summary = _aggregate_player_stats(
        model_data.get("df_player_stats", pl.DataFrame()),
        model_data.get("df_snap_counts", pl.DataFrame()),
    )

    starter_ids: set = model_data.get("starter_gsis_ids", set())
    depth_df = model_data.get("df_depth_charts", pl.DataFrame())

    def _primary_depth_starter_id(pos_abb: str) -> str | None:
        if depth_df.is_empty():
            return None
        candidates = []
        for row in depth_df.to_dicts():
            if get_team_abbr(row.get("team")) != team:
                continue
            if str(row.get("pos_abb") or "").upper() != pos_abb:
                continue
            player_id = row.get("gsis_id")
            if not player_id:
                continue
            try:
                rank = int(row.get("pos_rank") or 999)
            except (TypeError, ValueError):
                rank = 999
            try:
                slot = int(row.get("pos_slot") or 999)
            except (TypeError, ValueError):
                slot = 999
            candidates.append((rank, slot, str(player_id)))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][2]

    primary_rb_starter_id = _primary_depth_starter_id("RB")
    last_season_player_rankings = _get_last_season_player_rankings()

    def _row_to_dict(row, pos_group):
        pid = str(row.get("player_id"))
        draft_year = row.get("draft_year")
        try:
            draft_year_int = int(draft_year) if draft_year is not None else None
        except (TypeError, ValueError):
            draft_year_int = None
        agg = stats_summary.get(pid, {})
        rank_info = last_season_player_rankings.get(pid, {})
        is_starter = pid in starter_ids
        if pos_group == "rb" and primary_rb_starter_id:
            # The UI groups FB under RB for roster context, but fantasy users
            # expect one RB starter. Use the actual RB1 depth-chart slot only.
            is_starter = pid == primary_rb_starter_id

        return {
            "player_id": pid,
            "player_name": row.get("player_name"),
            "position": row.get("position"),
            "position_group": pos_group,
            "team": team,
            "image": row.get("headshot") or get_headshot_url(pid),
            "injury_status": model_data.get("injury_map", {}).get(pid, row.get("injury_status") or "Active"),
            "is_rookie": draft_year_int == int(CURRENT_SEASON),
            # Source: nflreadpy depth chart (pos_rank == 1). When depth chart
            # data isn't loaded, this falls to False rather than guessing.
            "is_starter": is_starter,
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
            "def_tackles_total": int(agg.get("def_tackles_total", 0)),
            "def_tackles_solo": int(agg.get("def_tackles_solo", 0)),
            "def_tackle_assists": int(agg.get("def_tackle_assists", 0)),
            "def_tackles_for_loss": int(agg.get("def_tackles_for_loss", 0)),
            "def_sacks": round(float(agg.get("def_sacks", 0.0)), 1),
            "def_qb_hits": int(agg.get("def_qb_hits", 0)),
            "def_interceptions": int(agg.get("def_interceptions", 0)),
            "def_pass_defended": int(agg.get("def_pass_defended", 0)),
            "def_fumbles_forced": int(agg.get("def_fumbles_forced", 0)),
            "def_fumble_recoveries": int(agg.get("def_fumble_recoveries", 0)),
            "snaps_total": int(agg.get("snaps_total", 0)),
            "snap_pct_avg": round(float(agg.get("snap_pct_avg", 0.0)), 1),
            "last_season": rank_info.get("last_season"),
            "last_season_rank_position": rank_info.get("last_season_rank_position"),
            "last_season_position_rank": rank_info.get("last_season_position_rank"),
            "last_season_position_ppg_rank": rank_info.get("last_season_position_ppg_rank"),
            "last_season_position_rank_out_of": rank_info.get("last_season_position_rank_out_of"),
            "last_season_games_played": int(rank_info.get("last_season_games_played", 0) or 0),
            "last_season_total_pts": round(float(rank_info.get("last_season_total_pts", 0.0) or 0.0), 1),
            "last_season_avg_pts": round(float(rank_info.get("last_season_avg_pts", 0.0) or 0.0), 2),
            "last_season_team": rank_info.get("last_season_team"),
            "last_season_team_sacks_taken": rank_info.get("last_season_team_sacks_taken"),
            "last_season_team_sacks_per_game": rank_info.get("last_season_team_sacks_per_game"),
            "last_season_team_sacks_rank": rank_info.get("last_season_team_sacks_rank"),
            "last_season_team_sacks_rank_out_of": rank_info.get("last_season_team_sacks_rank_out_of"),
            "last_season_team_rush_tds": rank_info.get("last_season_team_rush_tds"),
            "last_season_team_rush_tds_per_game": rank_info.get("last_season_team_rush_tds_per_game"),
            "last_season_team_rush_tds_rank": rank_info.get("last_season_team_rush_tds_rank"),
            "last_season_team_rush_tds_rank_out_of": rank_info.get("last_season_team_rush_tds_rank_out_of"),
        }

    groups: dict[str, list] = {"qb": [], "rb": [], "wr": [], "te": [], "ol": []}
    defense_groups: dict[str, list] = {"dl": [], "lb": [], "db": []}
    POSITION_TO_GROUP = {
        "QB": "qb",
        "RB": "rb",
        "FB": "rb",
        "WR": "wr",
        "TE": "te",
        "OL": "ol",
        "DL": "dl",
        "LB": "lb",
        "DB": "db",
    }
    for r in roster.iter_rows(named=True):
        grp = POSITION_TO_GROUP.get(str(r.get("position") or "").upper())
        if grp in groups:
            groups[grp].append(_row_to_dict(r, grp))
        elif grp in defense_groups:
            defense_groups[grp].append(_row_to_dict(r, grp))

    if groups["rb"]:
        rb_starter_seen = False
        for player in groups["rb"]:
            if player.get("player_id") == primary_rb_starter_id:
                player["is_starter"] = True
                rb_starter_seen = True
            else:
                player["is_starter"] = False
        if not rb_starter_seen:
            rb_candidates = [p for p in groups["rb"] if str(p.get("position") or "").upper() == "RB"] or groups["rb"]
            rb_candidates.sort(
                key=lambda p: (
                    -(p.get("snap_pct_avg") or 0),
                    -(p.get("season_avg_pts") or 0),
                    str(p.get("player_name") or ""),
                )
            )
            rb_candidates[0]["is_starter"] = True

    # Sort each group: starters first (by snap %), then by season avg pts.
    def _sort_lineup(players: list) -> None:
        players.sort(
            key=lambda p: (
                0 if p.get("is_starter") else 1,
                -(p.get("snap_pct_avg") or 0),
                -(p.get("season_avg_pts") or 0),
                str(p.get("player_name") or ""),
            )
        )

    for k in groups:
        _sort_lineup(groups[k])
    for k in defense_groups:
        _sort_lineup(defense_groups[k])

    return {
        "team": team,
        "last_season": int(CURRENT_SEASON) - 1,
        "rankings": _get_last_season_team_rankings(team),
        "freshness": _team_payload_freshness(),
        **groups,
        "defense": defense_groups,
    }


@router.get("/team-builder/players")
async def get_team_builder_players():
    """Return offensive players for the custom team-builder pool."""
    profile_df = model_data.get("df_profile", pl.DataFrame())
    if profile_df.is_empty():
        return []

    stats_summary = _aggregate_player_stats(
        model_data.get("df_player_stats", pl.DataFrame()),
        model_data.get("df_snap_counts", pl.DataFrame()),
    )
    starter_ids: set = model_data.get("starter_gsis_ids", set())
    depth_df = model_data.get("df_depth_charts", pl.DataFrame())
    last_season_player_rankings = _get_last_season_player_rankings()

    def _rb_starters_by_team() -> dict[str, str]:
        if depth_df.is_empty():
            return {}
        candidates: dict[str, list[tuple[int, int, str]]] = {}
        for row in depth_df.to_dicts():
            if str(row.get("pos_abb") or "").upper() != "RB":
                continue
            team = get_team_abbr(row.get("team"))
            player_id = row.get("gsis_id")
            if not team or team == "None" or not player_id:
                continue
            try:
                rank = int(row.get("pos_rank") or 999)
            except (TypeError, ValueError):
                rank = 999
            try:
                slot = int(row.get("pos_slot") or 999)
            except (TypeError, ValueError):
                slot = 999
            candidates.setdefault(team, []).append((rank, slot, str(player_id)))
        out = {}
        for team, values in candidates.items():
            values.sort()
            out[team] = values[0][2]
        return out

    rb_starters = _rb_starters_by_team()
    team_col = "team_abbr" if "team_abbr" in profile_df.columns else "team"
    position_to_group = {
        "QB": "qb",
        "RB": "rb",
        "FB": "rb",
        "WR": "wr",
        "TE": "te",
        "OL": "ol",
    }

    out = []
    for row in profile_df.iter_rows(named=True):
        position = str(row.get("position") or "").upper()
        position_group = position_to_group.get(position)
        if not position_group:
            continue

        pid = str(row.get("player_id") or "")
        if not pid:
            continue
        team = get_team_abbr(row.get(team_col))
        draft_year = row.get("draft_year")
        try:
            draft_year_int = int(draft_year) if draft_year is not None else None
        except (TypeError, ValueError):
            draft_year_int = None

        agg = stats_summary.get(pid, {})
        rank_info = last_season_player_rankings.get(pid, {})
        is_starter = pid in starter_ids
        if position_group == "rb" and rb_starters.get(team):
            is_starter = pid == rb_starters[team]

        out.append(
            {
                "player_id": pid,
                "player_name": row.get("player_name"),
                "position": row.get("position"),
                "position_group": position_group,
                "team": team,
                "image": row.get("headshot") or get_headshot_url(pid),
                "injury_status": model_data.get("injury_map", {}).get(pid, row.get("injury_status") or "Active"),
                "is_rookie": draft_year_int == int(CURRENT_SEASON),
                "is_starter": is_starter,
                "draft_year": draft_year_int,
                "draft_number": row.get("draft_number"),
                "age": row.get("age"),
                "height": row.get("height"),
                "weight": row.get("weight"),
                "college": row.get("college"),
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
                "def_tackles_total": int(agg.get("def_tackles_total", 0)),
                "def_tackles_solo": int(agg.get("def_tackles_solo", 0)),
                "def_tackle_assists": int(agg.get("def_tackle_assists", 0)),
                "def_tackles_for_loss": int(agg.get("def_tackles_for_loss", 0)),
                "def_sacks": round(float(agg.get("def_sacks", 0.0)), 1),
                "def_qb_hits": int(agg.get("def_qb_hits", 0)),
                "def_interceptions": int(agg.get("def_interceptions", 0)),
                "def_pass_defended": int(agg.get("def_pass_defended", 0)),
                "def_fumbles_forced": int(agg.get("def_fumbles_forced", 0)),
                "def_fumble_recoveries": int(agg.get("def_fumble_recoveries", 0)),
                "snaps_total": int(agg.get("snaps_total", 0)),
                "snap_pct_avg": round(float(agg.get("snap_pct_avg", 0.0)), 1),
                "last_season": rank_info.get("last_season"),
                "last_season_rank_position": rank_info.get("last_season_rank_position"),
                "last_season_position_rank": rank_info.get("last_season_position_rank"),
                "last_season_position_ppg_rank": rank_info.get("last_season_position_ppg_rank"),
                "last_season_position_rank_out_of": rank_info.get("last_season_position_rank_out_of"),
                "last_season_games_played": int(rank_info.get("last_season_games_played", 0) or 0),
                "last_season_total_pts": round(float(rank_info.get("last_season_total_pts", 0.0) or 0.0), 1),
                "last_season_avg_pts": round(float(rank_info.get("last_season_avg_pts", 0.0) or 0.0), 2),
                "last_season_team": rank_info.get("last_season_team"),
                "last_season_team_sacks_taken": rank_info.get("last_season_team_sacks_taken"),
                "last_season_team_sacks_per_game": rank_info.get("last_season_team_sacks_per_game"),
                "last_season_team_sacks_rank": rank_info.get("last_season_team_sacks_rank"),
                "last_season_team_sacks_rank_out_of": rank_info.get("last_season_team_sacks_rank_out_of"),
                "last_season_team_rush_tds": rank_info.get("last_season_team_rush_tds"),
                "last_season_team_rush_tds_per_game": rank_info.get("last_season_team_rush_tds_per_game"),
                "last_season_team_rush_tds_rank": rank_info.get("last_season_team_rush_tds_rank"),
                "last_season_team_rush_tds_rank_out_of": rank_info.get("last_season_team_rush_tds_rank_out_of"),
            }
        )

    out.sort(
        key=lambda p: (
            str(p.get("team") or ""),
            str(p.get("position_group") or ""),
            0 if p.get("is_starter") else 1,
            p.get("last_season_position_rank") or 9999,
            str(p.get("player_name") or ""),
        )
    )
    return out


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

    stats_df = model_data.get("df_player_stats", pl.DataFrame())
    snaps_df = model_data.get("df_snap_counts", pl.DataFrame())
    stats_season = int(CURRENT_SEASON)

    # Offseason fallback chain:
    #   1. In-memory historical stats (nflreadpy cache) — typically last 1-2 seasons
    #   2. DB tables for prior seasons (if ETL has populated them)
    if stats_df.is_empty():
        hist = model_data.get("df_player_stats_history", pl.DataFrame())
        if not hist.is_empty() and "season" in hist.columns:
            try:
                latest = int(hist.select(pl.col("season").max()).item())
                stats_df = hist.filter(pl.col("season") == latest)
                stats_season = latest
                logger.info(f"Tier list pool using in-memory historical stats for {latest}")
            except Exception as e:
                logger.debug(f"Historical pool slice failed: {e}")

        if stats_df.is_empty():
            for back in range(1, 4):
                candidate = int(CURRENT_SEASON) - back
                stats_df = _load_prior_season_stats(candidate)
                if not stats_df.is_empty():
                    snaps_df = _load_prior_season_snaps(candidate)
                    stats_season = candidate
                    logger.info(f"Tier list pool falling back to DB {candidate} stats")
                    break

    stats_summary = _aggregate_player_stats(stats_df, snaps_df)

    adp_map = get_adp_map(int(CURRENT_SEASON))
    adp_names = list(adp_map.keys())

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
                "adp": lookup_adp(row.get("player_name") or "", adp_map, adp_names),
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

    # Default sort: real ADP (Sleeper/ESPN-style consensus draft board) first —
    # this is the order a user actually drafts in, which is what they're tiering
    # against. Players with no ADP (deep bench/practice squad, no real draft
    # signal) sort after every ADP-ranked player, by projected PPG as a fallback.
    # (Frontend re-sorts by draft pick when "Rookies only" is on.)
    NO_ADP = float("inf")
    results.sort(
        key=lambda r: (r["adp"] if r["adp"] is not None else NO_ADP, -(r["stats"]["season_avg_pts"] or 0)),
    )
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
@limiter.limit("3/minute")
async def refresh_rookies(request: Request, source: str = "all"):
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
