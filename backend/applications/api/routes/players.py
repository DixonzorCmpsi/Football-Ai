from fastapi import APIRouter, HTTPException
from typing import Optional
import polars as pl
from ..state import model_data
from ..models import PlayerRequest, CompareRequest
from ..services.prediction import get_player_card
from ..services.utils import calculate_fantasy_points
from ..config import logger, DB_CONNECTION_STRING, CURRENT_SEASON

router = APIRouter()

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
        if not frames:
            return {"player_id": player_id, "seasons": []}

        df = pl.concat(frames, how='diagonal_relaxed')

        # Position lookup for the UI (so it can show position-specific stat tiles).
        position = None
        profile = model_data.get('df_profile', pl.DataFrame())
        if not profile.is_empty() and 'player_id' in profile.columns:
            row = profile.filter(pl.col('player_id') == player_id)
            if not row.is_empty():
                position = row.row(0, named=True).get('position')
        pos_group = (str(position or '').upper())
        pos_group = {'QB': 'qb', 'RB': 'rb', 'FB': 'rb', 'WR': 'wr', 'TE': 'te'}.get(pos_group, 'wr')

        # Snap counts only exist for the current season (df_snap_counts is current-season).
        snaps_df = model_data.get('df_snap_counts', pl.DataFrame())
        cur_snaps = (
            snaps_df.filter(pl.col('player_id') == player_id)
            if not snaps_df.is_empty() and 'player_id' in snaps_df.columns
            else pl.DataFrame()
        )

        out = []
        for season_val, group in df.group_by('season'):
            season = int(season_val[0] if isinstance(season_val, tuple) else season_val)
            rows = group.to_dicts()
            games = len(rows)
            pts = [calculate_fantasy_points(r) for r in rows]
            pts = [p for p in pts if p is not None]
            total = sum(pts)
            avg = total / games if games else 0.0
            sorted_rows = sorted(rows, key=lambda r: r.get('week') or 0, reverse=True)
            recent = []
            for r in sorted_rows:
                p = calculate_fantasy_points(r)
                if p > 0:
                    recent.append(p)
                if len(recent) >= 4:
                    break
            boom = sum(1 for p in pts if p >= 20)
            bust = sum(1 for p in pts if 0 < p < 5)

            snaps_total = 0
            snap_pct_avg = 0.0
            if season == int(CURRENT_SEASON) and not cur_snaps.is_empty():
                try:
                    s_total = int(cur_snaps.select(pl.col('offense_snaps').sum()).item() or 0)
                    s_pct = float(cur_snaps.select(pl.col('offense_pct').mean()).item() or 0.0)
                    if s_pct and s_pct <= 1.0:
                        s_pct *= 100
                    snaps_total = s_total
                    snap_pct_avg = round(s_pct, 1)
                except Exception:
                    pass

            out.append({
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
                "snaps_total": snaps_total,
                "snap_pct_avg": snap_pct_avg,
            })

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
                "pass_attempts": int(row.get('attempts') or row.get('pass_attempts') or 0)
            })
        return history
    except Exception as e:
        logger.exception(f"History endpoint failed for {player_id}: {e}")
        return []
