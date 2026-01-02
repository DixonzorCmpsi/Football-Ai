from fastapi import APIRouter, HTTPException
from ..services.prediction import find_usage_boost_reason
from ..config import logger

router = APIRouter()

@router.get('/debug/usage-boost/{player_id}/{week}')
async def debug_usage_boost(player_id: str, week: int):
    try:
        res = find_usage_boost_reason(player_id, week)
        return res
    except Exception as e:
        logger.exception(f"Debug usage-boost failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
