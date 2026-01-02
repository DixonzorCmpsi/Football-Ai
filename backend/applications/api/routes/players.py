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
    try:
        # Prefer in-memory data (loaded from CSV) for consistency; fallback to DB if empty
        df = model_data.get('df_player_stats', pl.DataFrame())
        if df.is_empty() and DB_CONNECTION_STRING:
            # Only try DB if in-memory is empty
            q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' ORDER BY week DESC"
            df = pl.read_database_uri(q, DB_CONNECTION_STRING)
        else:
            # Filter in-memory data
            df = df.filter(pl.col('player_id') == player_id).sort('week', descending=True)

        if df.is_empty():
            return []

        history = []
        player_snaps = model_data.get('df_snap_counts', pl.DataFrame())
        seen_weeks = set()

        for row in df.iter_rows(named=True):
            wk = row.get('week')
            if wk in seen_weeks:
                continue
            seen_weeks.add(wk)

            snap_count, snap_pct, team_snaps = 0, 0.0, 0
            if not player_snaps.is_empty():
                # Filter by week AND player_id
                s_row = player_snaps.filter((pl.col('week') == wk) & (pl.col('player_id') == player_id))
                if not s_row.is_empty():
                    s0 = s_row.row(0, named=True)
                    snap_count = int(s0.get('offense_snaps', 0))
                    snap_pct = float(s0.get('offense_pct', 0.0))
                    # Calculate team snaps if possible
                    if snap_pct > 0:
                        team_snaps = int(snap_count / snap_pct)
                    else:
                        # Fallback: try to find max snaps for this team/week if we had team info
                        # For now, just leave as 0 if we can't derive it
                        team_snaps = 0

            history.append({
                "week": wk,
                "opponent": row.get('opponent_team') or "N/A",
                "points": round(float(calculate_fantasy_points(row)), 2),
                "passing_yds": int(row.get('passing_yards') or 0),
                "rushing_yds": int(row.get('rushing_yards') or 0),
                "receiving_yds": int(row.get('receiving_yards') or 0),
                "touchdowns": int((row.get('passing_touchdown') or 0) + (row.get('rush_touchdown') or 0) + (row.get('receiving_touchdown') or 0)),
                "snap_count": snap_count,
                "snap_percentage": snap_pct,
                "team_total_snaps": team_snaps,
                "receptions": int(row.get('receptions') or 0),
                "targets": int(row.get('targets') or 0),
                "carries": int(row.get('rush_attempts') or 0)
            })
        return history
    except Exception as e:
        logger.exception(f"History endpoint failed for {player_id}: {e}")
        return []
