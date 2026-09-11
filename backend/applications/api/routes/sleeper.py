"""Sleeper league endpoints: import a team, analyze it, scan the waiver wire."""

from fastapi import APIRouter, HTTPException, Query

from ..config import CURRENT_SEASON, logger
from ..services import sleeper_league as sl

router = APIRouter(prefix="/sleeper", tags=["sleeper"])


def _guard(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except sl.SleeperError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/user/{username}")
async def sleeper_user(username: str, season: int = Query(default=CURRENT_SEASON)):
    """Resolve a Sleeper username and list that user's NFL leagues for a season."""
    user = _guard(sl.resolve_user, username)
    if not user or not user.get("user_id"):
        raise HTTPException(status_code=404, detail=f"No Sleeper user named '{username}'")
    leagues = _guard(sl.get_user_leagues, user["user_id"], season)
    return {"user": user, "season": season, "leagues": leagues}


@router.get("/league/{league_id}")
async def sleeper_league_detail(league_id: str):
    league = _guard(sl.get_league, league_id)
    if not league:
        raise HTTPException(status_code=404, detail=f"League {league_id} not found")
    return {"league": league, "teams": _guard(sl.roster_owners, league_id)}


@router.get("/league/{league_id}/roster/{roster_id}/analysis")
async def sleeper_roster_analysis(league_id: str, roster_id: int,
                                  week: int = Query(default=1, ge=1, le=22)):
    """Project a roster and compare the model's lineup to the saved one."""
    try:
        return await sl.analyze_roster(league_id, roster_id, week)
    except sl.SleeperError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Sleeper roster analysis failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/league/{league_id}/waivers")
async def sleeper_waivers(league_id: str,
                          week: int = Query(default=1, ge=1, le=22),
                          limit: int = Query(default=25, ge=1, le=100)):
    """Free agents in the league, ranked by this app's projection."""
    try:
        return await sl.waiver_targets(league_id, week, limit)
    except sl.SleeperError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Sleeper waiver scan failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
