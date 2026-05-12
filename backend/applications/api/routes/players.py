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
        player_snaps = model_data.get('df_snap_counts', pl.DataFrame())

        for row in df.iter_rows(named=True):
            wk = row.get('week')
            season = row.get('season') or CURRENT_SEASON

            snap_count, snap_pct, team_snaps = 0, 0.0, 0
            # Snap counts are current-season only — only fill when row is for current season.
            if int(season) == int(CURRENT_SEASON) and not player_snaps.is_empty():
                s_row = player_snaps.filter((pl.col('week') == wk) & (pl.col('player_id') == player_id))
                if not s_row.is_empty():
                    s0 = s_row.row(0, named=True)
                    snap_count = int(s0.get('offense_snaps', 0))
                    snap_pct = float(s0.get('offense_pct', 0.0))
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
