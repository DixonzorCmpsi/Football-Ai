"""Compare players by id, with a verdict. See services/player_compare.py."""

from fastapi import APIRouter, HTTPException, Query

from ..services import player_compare
from ..state import model_data

router = APIRouter(tags=["compare"])


@router.get("/compare/players")
async def compare_players(ids: str = Query(..., description="Comma-separated player ids, up to 4"),
                          week: int | None = Query(default=None, ge=1, le=22)):
    player_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if len(player_ids) < 2:
        raise HTTPException(status_code=400, detail="Compare needs at least two players.")
    wk = week or model_data.get("current_nfl_week") or 1
    return await player_compare.compare(player_ids, wk)
