from fastapi import APIRouter, HTTPException
from typing import Optional
import polars as pl
from ..state import model_data
from ..models import PlayerRequest, CompareRequest
from ..services.prediction import get_player_card
from ..services.utils import calculate_fantasy_points, get_team_abbr
from ..config import logger, DB_CONNECTION_STRING, CURRENT_SEASON
from .tier_list import _fetch_team_weekly_from_nflreadpy, _load_team_weekly_table, _rank_asc, _rank_desc

router = APIRouter()


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
    return float(
        _number(row, "def_tackles_solo") * 1.5
        + _number(row, "def_tackle_assists") * 0.75
        + _number(row, "def_tackles_for_loss") * 2.0
        + _number(row, "def_sacks") * 4.0
        + _number(row, "def_qb_hits") * 1.0
        + _number(row, "def_interceptions") * 6.0
        + _number(row, "def_pass_defended", "def_passes_defended", "pass_defended") * 1.5
        + _number(row, "def_fumbles_forced") * 4.0
        + _number(row, "fumble_recovery_opp", "def_fumble_recoveries") * 4.0
        + (_number(row, "def_tds") + _number(row, "fumble_recovery_tds")) * 6.0
        + _number(row, "def_safeties") * 2.0
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


def _position_group(position: str | None) -> str:
    pos = str(position or "").upper()
    if pos in {"QB", "RB", "FB", "WR", "TE"}:
        return "rb" if pos == "FB" else pos.lower()
    if pos in {"OL", "T", "G", "C", "OT", "OG", "LT", "LG", "RT", "RG"}:
        return "ol"
    if pos in {"DL", "DE", "DT", "EDGE", "NT"}:
        return "dl"
    if pos in {"LB", "ILB", "OLB", "MLB"}:
        return "lb"
    if pos in {"DB", "CB", "S", "FS", "SS"}:
        return "db"
    return "wr"


def _team_sack_context(season: int) -> dict[str, dict]:
    cache = model_data.setdefault("season_team_sack_context_cache", {})
    key = str(int(season))
    if key in cache:
        return cache[key]

    offense_df = _load_team_weekly_table(int(season), "offense")
    if offense_df.is_empty() or "sacks_suffered" not in offense_df.columns:
        fallback_offense, _fallback_defense = _fetch_team_weekly_from_nflreadpy(int(season))
        if not fallback_offense.is_empty():
            offense_df = fallback_offense

    if offense_df.is_empty() or "sacks_suffered" not in offense_df.columns:
        cache[key] = {}
        return {}

    aggregates: dict[str, dict] = {}
    for row in offense_df.to_dicts():
        if row.get("season") is not None:
            try:
                if int(row.get("season")) != int(season):
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
    out = {
        team: {
            "team": team,
            "sacks_taken": round(float(data["sacks"]), 1),
            "sacks_taken_per_game": round(float(data["sacks"]) / data["games"], 2) if data["games"] else 0.0,
            "sacks_taken_rank": ranks.get(team),
            "sacks_taken_rank_out_of": len(values),
            "rush_tds": round(float(data["rush_tds"]), 1),
            "rush_tds_per_game": round(float(data["rush_tds"]) / data["games"], 2) if data["games"] else 0.0,
            "rush_tds_rank": rush_ranks.get(team),
            "rush_tds_rank_out_of": len(rush_values),
        }
        for team, data in aggregates.items()
        if data["games"] > 0
    }
    cache[key] = out
    return out


def _dominant_team(rows: list[dict], snap_rows: list[dict]) -> str | None:
    counts: dict[str, float] = {}
    for row in rows:
        team = get_team_abbr(row.get("recent_team") or row.get("team") or row.get("posteam"))
        if team and team != "None":
            counts[team] = counts.get(team, 0.0) + 1.0
    for row in snap_rows:
        team = get_team_abbr(row.get("team") or row.get("team_abbr"))
        if team and team != "None":
            counts[team] = counts.get(team, 0.0) + max(_number(row, "offense_snaps"), _number(row, "defense_snaps"), 1.0)
    if not counts:
        return None
    return max(counts.items(), key=lambda item: item[1])[0]

@router.get("/player/{player_id}")
async def get_player_by_id(player_id: str, week: Optional[int] = None):
    try:
        wk = week if week else model_data["current_nfl_week"]
        card = await get_player_card(player_id, wk)
        if not card: raise HTTPException(404, "Player not found")
        return card
    except Exception as e: raise HTTPException(500, str(e))

@router.post("/predict")
async def predict(req: PlayerRequest):
    try:
        match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == req.player_name.lower())
        if match.is_empty(): raise HTTPException(404, "Player not found")
        pid = match.row(0, named=True)['player_id']
        wk = req.week if req.week else model_data["current_nfl_week"]
        return await get_player_card(pid, wk)
    except Exception as e: raise HTTPException(500, str(e))

@router.post("/compare")
async def compare(req: CompareRequest):
    wk = req.week if req.week else model_data["current_nfl_week"]
    res = []
    for name in [req.player1_name, req.player2_name]:
        try:
            match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == name.lower())
            if not match.is_empty():
                pid = match.row(0, named=True)['player_id']
                res.append(await get_player_card(pid, wk))
            else:
                res.append({"error": f"Player {name} not found"})
        except:
            res.append({"error": "Lookup failed"})
    return {"week": wk, "comparison": res}

@router.get("/player/{player_id}/season_stats")
async def get_player_season_stats(player_id: str, seasons: int = 5):
    """Per-season aggregates for the most recent `seasons` years (default 5).

    Powers the PlayerDetailModal season selector — same shape the modal already
    renders for "latest season", just keyed by season so the UI can cycle.
    """
    try:
        position = None
        pfr_id = None
        profile_team = None
        profile = model_data.get('df_profile', pl.DataFrame())
        if not profile.is_empty() and 'player_id' in profile.columns:
            row = profile.filter(pl.col('player_id') == player_id)
            if not row.is_empty():
                prow = row.row(0, named=True)
                position = prow.get('position')
                pfr_id = prow.get('pfr_id')
                profile_team = get_team_abbr(prow.get('team_abbr') or prow.get('team'))
        pos_group = _position_group(position)

        frames = []
        cur = model_data.get('df_player_stats', pl.DataFrame())
        if not cur.is_empty():
            f = cur.filter(pl.col('player_id') == player_id)
            if not f.is_empty():
                if 'season' not in f.columns:
                    f = f.with_columns(pl.lit(int(CURRENT_SEASON)).alias('season'))
                frames.append(f)
        hist = model_data.get('df_player_stats_history', pl.DataFrame())
        if not hist.is_empty():
            f = hist.filter(pl.col('player_id') == player_id)
            if not f.is_empty():
                frames.append(f)

        snap_frames = []
        cur_snaps = model_data.get('df_snap_counts', pl.DataFrame())
        if not cur_snaps.is_empty():
            if 'season' not in cur_snaps.columns:
                cur_snaps = cur_snaps.with_columns(pl.lit(int(CURRENT_SEASON)).alias('season'))
            snap_frames.append(cur_snaps)
        hist_snaps = model_data.get('df_snap_counts_history', pl.DataFrame())
        if not hist_snaps.is_empty():
            if 'season' not in hist_snaps.columns:
                hist_snaps = hist_snaps.with_columns(pl.lit(int(CURRENT_SEASON) - 1).alias('season'))
            snap_frames.append(hist_snaps)
        combined_snaps = pl.concat(snap_frames, how='diagonal_relaxed') if snap_frames else pl.DataFrame()

        player_snap_rows = pl.DataFrame()
        if not combined_snaps.is_empty():
            if 'player_id' in combined_snaps.columns:
                player_snap_rows = combined_snaps.filter(pl.col('player_id') == player_id)
            if player_snap_rows.is_empty() and pfr_id and 'pfr_id' in combined_snaps.columns:
                player_snap_rows = combined_snaps.filter(pl.col('pfr_id') == pfr_id)

        out = []
        grouped_stats = []
        if frames:
            df = pl.concat(frames, how='diagonal_relaxed')
            if 'season' not in df.columns:
                df = df.with_columns(pl.lit(int(CURRENT_SEASON)).alias('season'))
            grouped_stats = list(df.group_by('season'))

        seen_seasons = set()

        def snap_rows_for_season(season: int) -> list[dict]:
            if player_snap_rows.is_empty() or 'season' not in player_snap_rows.columns:
                return []
            try:
                snap_season = player_snap_rows.filter(pl.col('season').cast(pl.Int64, strict=False) == int(season))
            except Exception:
                snap_season = player_snap_rows.filter(pl.col('season') == int(season))
            return snap_season.to_dicts() if not snap_season.is_empty() else []

        def snap_summary(snap_rows: list[dict]) -> tuple[int, float]:
            if not snap_rows:
                return 0, 0.0
            offense_snaps = sum(_number(r, 'offense_snaps') for r in snap_rows)
            defense_snaps = sum(_number(r, 'defense_snaps') for r in snap_rows)
            if defense_snaps > offense_snaps:
                pct_values = [_number(r, 'defense_pct') for r in snap_rows if _number(r, 'defense_pct') > 0]
                total = defense_snaps
            else:
                pct_values = [_number(r, 'offense_pct') for r in snap_rows if _number(r, 'offense_pct') > 0]
                total = offense_snaps
            avg_pct = sum(pct_values) / len(pct_values) if pct_values else 0.0
            if avg_pct and avg_pct <= 1.0:
                avg_pct *= 100
            return int(total or 0), round(float(avg_pct), 1)

        def build_row(season: int, rows: list[dict], snap_rows: list[dict]) -> dict:
            games = len(rows)
            use_defense = pos_group in {'dl', 'lb', 'db'} or any(_has_defensive_stats(r) for r in rows)
            pts = [_calculate_defensive_points(r) for r in rows] if use_defense else [calculate_fantasy_points(r) for r in rows]
            pts = [p for p in pts if p is not None]
            total = sum(pts)
            avg = total / games if games else 0.0
            sorted_rows = sorted(rows, key=lambda r: r.get('week') or 0, reverse=True)
            recent = []
            for r in sorted_rows:
                p = _calculate_defensive_points(r) if use_defense else calculate_fantasy_points(r)
                if p > 0:
                    recent.append(p)
                if len(recent) >= 4:
                    break
            boom = sum(1 for p in pts if p >= 20)
            bust = sum(1 for p in pts if 0 < p < 5)

            if not rows and snap_rows:
                games = len({r.get('week') for r in snap_rows if r.get('week') is not None}) or len(snap_rows)

            snaps_total, snap_pct_avg = snap_summary(snap_rows)
            def_solo = sum(_number(r, 'def_tackles_solo') for r in rows)
            def_assists = sum(_number(r, 'def_tackle_assists') for r in rows)
            team = _dominant_team(rows, snap_rows) or profile_team
            sack_info = _team_sack_context(season).get(team or "", {}) if pos_group == 'ol' else {}

            return {
                "season": season,
                "games_played": games,
                "season_total_pts": round(total, 1),
                "season_avg_pts": round(avg, 2),
                "recent_avg_pts": round(sum(recent) / len(recent) if recent else 0.0, 2),
                "boom_games": boom,
                "bust_games": bust,
                "total_yds": int(sum(
                    (r.get('passing_yards') or 0)
                    + (r.get('rushing_yards') or 0)
                    + (r.get('receiving_yards') or 0)
                    for r in rows
                )),
                "total_tds": int(sum(
                    (r.get('passing_touchdown') or 0)
                    + (r.get('rush_touchdown') or 0)
                    + (r.get('receiving_touchdown') or 0)
                    for r in rows
                )),
                "total_receptions": int(sum((r.get('receptions') or 0) for r in rows)),
                "total_targets": int(sum((r.get('targets') or 0) for r in rows)),
                "total_carries": int(sum((r.get('rush_attempts') or 0) for r in rows)),
                "def_tackles_total": int(round(def_solo + def_assists)),
                "def_tackles_solo": int(round(def_solo)),
                "def_tackle_assists": int(round(def_assists)),
                "def_tackles_for_loss": int(round(sum(_number(r, 'def_tackles_for_loss') for r in rows))),
                "def_sacks": round(sum(_number(r, 'def_sacks') for r in rows), 1),
                "def_qb_hits": int(round(sum(_number(r, 'def_qb_hits') for r in rows))),
                "def_interceptions": int(round(sum(_number(r, 'def_interceptions') for r in rows))),
                "def_pass_defended": int(round(sum(_number(r, 'def_pass_defended', 'def_passes_defended', 'pass_defended') for r in rows))),
                "def_fumbles_forced": int(round(sum(_number(r, 'def_fumbles_forced') for r in rows))),
                "def_fumble_recoveries": int(round(sum(_number(r, 'fumble_recovery_opp', 'def_fumble_recoveries') for r in rows))),
                "snaps_total": snaps_total,
                "snap_pct_avg": snap_pct_avg,
                "team_sacks_taken": sack_info.get("sacks_taken"),
                "team_sacks_taken_per_game": sack_info.get("sacks_taken_per_game"),
                "team_sacks_taken_rank": sack_info.get("sacks_taken_rank"),
                "team_sacks_taken_rank_out_of": sack_info.get("sacks_taken_rank_out_of"),
                "team_rush_tds": sack_info.get("rush_tds"),
                "team_rush_tds_per_game": sack_info.get("rush_tds_per_game"),
                "team_rush_tds_rank": sack_info.get("rush_tds_rank"),
                "team_rush_tds_rank_out_of": sack_info.get("rush_tds_rank_out_of"),
                "team_sacks_taken_team": team,
            }

        for season_val, group in grouped_stats:
            season = int(season_val[0] if isinstance(season_val, tuple) else season_val)
            seen_seasons.add(season)
            out.append(build_row(season, group.to_dicts(), snap_rows_for_season(season)))

        if pos_group == 'ol' and not player_snap_rows.is_empty() and 'season' in player_snap_rows.columns:
            try:
                snap_seasons = sorted(
                    {
                        int(s)
                        for s in player_snap_rows.select(pl.col('season').cast(pl.Int64, strict=False)).drop_nulls().to_series().to_list()
                    },
                    reverse=True,
                )
            except Exception:
                snap_seasons = []
            for season in snap_seasons:
                if season in seen_seasons:
                    continue
                out.append(build_row(season, [], snap_rows_for_season(season)))

        out.sort(key=lambda s: s['season'], reverse=True)
        return {"player_id": player_id, "position_group": pos_group, "seasons": out[:max(1, int(seasons))]}
    except Exception as e:
        logger.exception(f"Season stats endpoint failed for {player_id}: {e}")
        return {"player_id": player_id, "seasons": []}


@router.get("/player/history/{player_id}")
async def get_player_history(player_id: str):
    """Return weekly history across the current season plus any cached prior seasons.

    The frontend `PlayerHistory` view rolls each row up by week — but with multi-season
    data we also emit a `season` field per row so the UI can group / chart if it wants.
    """
    try:
        frames = []

        # Current-season in-memory (or DB fallback)
        cur = model_data.get('df_player_stats', pl.DataFrame())
        if cur.is_empty() and DB_CONNECTION_STRING:
            try:
                q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}'"
                cur = pl.read_database_uri(q, DB_CONNECTION_STRING)
            except Exception:
                cur = pl.DataFrame()
        if not cur.is_empty():
            f = cur.filter(pl.col('player_id') == player_id)
            if not f.is_empty():
                if 'season' not in f.columns:
                    f = f.with_columns(pl.lit(int(CURRENT_SEASON)).alias('season'))
                frames.append(f)

        # Historical (prior seasons) from cached nflreadpy snapshot
        hist = model_data.get('df_player_stats_history', pl.DataFrame())
        if not hist.is_empty():
            f = hist.filter(pl.col('player_id') == player_id)
            if not f.is_empty():
                frames.append(f)

        if not frames:
            return []
        df = pl.concat(frames, how='diagonal_relaxed').sort(['season', 'week'], descending=[True, True])

        history = []

        # Unified snap lookup: both current and historical frames now expose `pfr_id`,
        # so the same join column works regardless of season. Build a single combined
        # frame once instead of branching per-row.
        snap_frames = []
        cur_snaps = model_data.get('df_snap_counts', pl.DataFrame())
        if not cur_snaps.is_empty():
            snap_frames.append(cur_snaps)
        hist_snaps = model_data.get('df_snap_counts_history', pl.DataFrame())
        if not hist_snaps.is_empty():
            snap_frames.append(hist_snaps)
        combined_snaps = (
            pl.concat(snap_frames, how='diagonal_relaxed') if snap_frames else pl.DataFrame()
        )

        # Resolve pfr_id from the profile so we can match snap rows that don't have gsis.
        pfr_id = None
        profile = model_data.get('df_profile', pl.DataFrame())
        if not profile.is_empty() and 'player_id' in profile.columns and 'pfr_id' in profile.columns:
            prow = profile.filter(pl.col('player_id') == player_id)
            if not prow.is_empty():
                pfr_id = prow.row(0, named=True).get('pfr_id')

        # Filter to just this player's snap rows once — try gsis first, fall back to pfr.
        player_snap_rows = pl.DataFrame()
        if not combined_snaps.is_empty():
            if 'player_id' in combined_snaps.columns:
                player_snap_rows = combined_snaps.filter(pl.col('player_id') == player_id)
            if player_snap_rows.is_empty() and pfr_id and 'pfr_id' in combined_snaps.columns:
                player_snap_rows = combined_snaps.filter(pl.col('pfr_id') == pfr_id)

        for row in df.iter_rows(named=True):
            wk = row.get('week')
            season = row.get('season') or CURRENT_SEASON

            snap_count, snap_pct, team_snaps = 0, 0.0, 0
            if not player_snap_rows.is_empty():
                s_row = player_snap_rows.filter(
                    (pl.col('week') == wk) & (pl.col('season') == int(season))
                )
                if not s_row.is_empty():
                    s0 = s_row.row(0, named=True)
                    snap_count = int(s0.get('offense_snaps') or 0)
                    snap_pct = float(s0.get('offense_pct') or 0.0)
                    if snap_pct > 0:
                        team_snaps = int(snap_count / snap_pct)

            history.append({
                "season": int(season),
                "week": wk,
                "opponent": row.get('opponent_team') or "N/A",
                "points": round(float(calculate_fantasy_points(row)), 2),
                "passing_yds": int(row.get('passing_yards') or 0),
                "rushing_yds": int(row.get('rushing_yards') or 0),
                "receiving_yds": int(row.get('receiving_yards') or 0),
                "touchdowns": int((row.get('passing_touchdown') or 0) + (row.get('rush_touchdown') or 0) + (row.get('receiving_touchdown') or 0)),
                "passing_tds": int(row.get('passing_touchdown') or 0),
                "snap_count": snap_count,
                "snap_percentage": snap_pct,
                "team_total_snaps": team_snaps,
                "receptions": int(row.get('receptions') or 0),
                "targets": int(row.get('targets') or 0),
                "carries": int(row.get('rush_attempts') or 0),
                "pass_attempts": int(row.get('attempts') or row.get('pass_attempts') or 0),
                # Negative-scoring plays. calculate_fantasy_points() already docks
                # 2 points apiece, but without these the client can't reconcile a
                # points breakdown against the total it's given.
                "interceptions": int(row.get('interceptions') or 0),
                "fumbles_lost": int(row.get('fumbles_lost') or 0)
            })
        return history
    except Exception as e:
        logger.exception(f"History endpoint failed for {player_id}: {e}")
        return []
