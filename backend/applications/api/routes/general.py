from fastapi import APIRouter, Request
import os
import json
import requests
import polars as pl
import subprocess
from ..state import model_data
from ..config import DB_CONNECTION_STRING, ETL_SCRIPT_PATH, WATCHLIST_FILE, RAG_DIR, logger
from ..rate_limit import limiter
from ..services.data_loader import refresh_app_state, refresh_db_data
from ..services.prediction import get_player_card

router = APIRouter()

@router.get("/current_week")
async def get_current_week(): return {"week": model_data.get("current_nfl_week", 1)}

@router.get("/health")
async def health_check():
    """Lightweight health check for orchestrators and load balancers.
    Returns: status, DB availability, models loaded, ETL script existence and current week."""
    status = {
        "status": "ok",
        "db_connection_string_set": bool(DB_CONNECTION_STRING),
        "models_loaded": len(model_data.get("models", {})),
        "meta_loaded": "meta_models" in model_data,
        "etl_script_exists": os.path.exists(ETL_SCRIPT_PATH),
        "current_week": model_data.get("current_nfl_week", None),
        # Freshness of the two fastest-moving feeds, so the UI can show how
        # stale the injury picture is rather than implying it is live.
        "injuries_updated_at": model_data.get("injuries_updated_at"),
        "storylines_updated_at": model_data.get("storylines_updated_at"),
        "data_loaded_at": model_data.get("data_loaded_at"),
    }

    counts = {}
    for key in (
        "df_profile",
        "df_schedule",
        "df_player_stats",
        "df_snap_counts",
        "df_features",
        "df_lines",
        "df_props",
    ):
        df = model_data.get(key)
        counts[key] = int(df.height) if hasattr(df, "height") else 0
    status["data_counts"] = counts
    status["ready"] = counts["df_profile"] > 0

    # Quick DB probe if connection string is configured
    if DB_CONNECTION_STRING:
        try:
            # Run a minimal probe query; some DB drivers may require a small table
            _ = pl.read_database_uri("SELECT 1", DB_CONNECTION_STRING)
            status["db_responding"] = True
        except Exception as e:
            status["db_responding"] = False
            status["db_error"] = str(e)
    else:
        status["db_responding"] = False

    if not status["ready"]:
        status["status"] = "starting"

    return status

SKILL_POSITIONS = ['QB', 'RB', 'WR', 'TE']


@router.get('/players/search')
async def search_players(q: str, scope: str = "skill", limit: int = 20):
    """Players whose name contains `q`, best matches first.

    `scope=skill` (the default) is for places that need a projection, such as
    Compare. `scope=all` also finds linemen, defenders and specialists, whose
    profiles and stats exist even though the model does not project them; without
    it the lookup page could not reach them at all.
    """
    if not q: return []
    try:
        needle = q.strip().lower()
        df = model_data["df_profile"]
        expr = pl.col('player_name').str.to_lowercase().str.contains(needle, literal=True)
        if scope != "all":
            expr = expr & pl.col('position').is_in(SKILL_POSITIONS)
        name = pl.col('player_name').str.to_lowercase()
        ranked = (
            df.filter(expr)
            .with_columns(
                # exact name, then a name or surname starting with the query, then anywhere
                pl.when(name == needle).then(0)
                .when(name.str.starts_with(needle) | name.str.contains(f" {needle}", literal=True)).then(1)
                .otherwise(2).alias("_match"),
                pl.when(pl.col('status') == 'ACT').then(0).otherwise(1).alias("_inactive"),
            )
            .sort(["_match", "_inactive", "player_name"])
        )
        cols = [c for c in ['player_id', 'player_name', 'position', 'team_abbr', 'headshot', 'status'] if c in df.columns]
        return ranked.select(cols).head(max(1, min(int(limit), 100))).to_dicts()
    except Exception as exc:
        logger.warning(f"player search failed for {q!r}: {exc}")
        return []

async def fetch_sleeper_trends(trend_type: str, limit: int = 10, week: int = 1):
    if not model_data.get("sleeper_map"): refresh_app_state() 
    try:
        url = f"https://api.sleeper.app/v1/players/nfl/trending/{trend_type}?lookback_hours=24&limit={limit+10}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=3)
        if response.status_code != 200: return []
        data = response.json()
        
        # Get the max count for percentage calculation
        max_count = max((item.get("count", 0) for item in data), default=1) or 1
        
        cards = []
        for item in data:
            sleeper_id = str(item.get("player_id"))
            count = item.get("count", 0)
            our_id = model_data["sleeper_map"].get(sleeper_id)
            if our_id:
                card = await get_player_card(our_id, week)
                if card:
                    card["trending_count"] = count
                    # Calculate percentage relative to top trending player (0-100)
                    card["trending_pct"] = round((count / max_count) * 100)
                    cards.append(card)
            if len(cards) >= limit: break
        return cards
    except: return []

@router.get("/rankings/past/{week}")
async def get_trending_down(week: int):
    return await fetch_sleeper_trends("drop", limit=30, week=week)

@router.get("/rankings/future/{week}")
async def get_trending_up(week: int):
    return await fetch_sleeper_trends("add", limit=30, week=week)

# --- WATCHLIST ---
def load_wl(): return json.load(open(WATCHLIST_FILE)) if os.path.exists(WATCHLIST_FILE) else []
@router.get('/watchlist')
async def get_watchlist():
    ids = load_wl()
    if not ids: return []
    return model_data["df_profile"].filter(pl.col("player_id").is_in(ids)).select(['player_id', 'player_name', 'team_abbr', 'position']).to_dicts()
@router.post('/watchlist')
async def add_watchlist(item: dict):
    ids = load_wl()
    if item['player_id'] not in ids:
        ids.append(item['player_id'])
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids
@router.delete('/watchlist/{player_id}')
async def remove_watchlist(player_id: str):
    ids = load_wl()
    if player_id in ids:
        ids.remove(player_id)
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids


# --- STORYLINES ---
@router.post("/refresh/storylines")
@limiter.limit("4/minute")
async def refresh_storylines_now(request: Request):
    """Pull the news feed immediately instead of waiting for the next tick."""
    from ..services.storylines import refresh_storylines

    return refresh_storylines()


# --- INJURIES ---
@router.post("/refresh/injuries")
@limiter.limit("4/minute")
async def refresh_injuries(request: Request):
    """Pull the latest injury statuses from Sleeper on demand.

    The scheduler already does this every INJURY_REFRESH_MINUTES, but news
    breaks on its own schedule - this lets the app force a pull right before
    lineups lock instead of waiting for the next tick.
    """
    from ..services.etl import run_injury_refresh_async
    ok = await run_injury_refresh_async()
    return {
        "status": "ok" if ok else "failed",
        "injuries_loaded": len(model_data.get("injury_map", {})),
        "week": model_data.get("current_nfl_week"),
    }


# --- LIVE SCORES & STATS ---
@router.post("/refresh/live-scores")
@limiter.limit("6/minute")
async def refresh_live_scores(request: Request, week: int = None):
    """
    Trigger a live scores/stats update from ESPN API.
    This is faster than nflreadpy and can be run during/after games.
    
    Args:
        week: Optional week number (defaults to current week)
    
    Returns:
        Status of the update operation
    """
    try:
        target_week = week or model_data.get("current_nfl_week", 19)
        live_script = os.path.join(RAG_DIR, "14_live_scores_stats.py")
        
        if not os.path.exists(live_script):
            return {"status": "error", "message": "Live scores script not found"}
        
        logger.info(f"Triggering live scores update for Week {target_week}")
        
        # Run the script
        result = subprocess.run(
            ["python3", live_script, "--week", str(target_week)],
            cwd=RAG_DIR,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            # Reload data after update
            refresh_db_data()
            refresh_app_state()
            
            return {
                "status": "success",
                "week": target_week,
                "message": "Live scores updated",
                "current_week": model_data.get("current_nfl_week")
            }
        else:
            return {
                "status": "error",
                "message": result.stderr or "Script failed",
                "stdout": result.stdout
            }
            
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Update timed out"}
    except Exception as e:
        logger.exception(f"Live scores update error: {e}")
        return {"status": "error", "message": str(e)}


@router.get("/live-scores/{week}")
async def get_live_scores(week: int):
    """
    Fetch live scores directly from ESPN without updating files.
    Good for real-time score checking during games.
    """
    # ESPN to internal team abbreviation mapping
    ESPN_TEAM_MAP = {
        "LAR": "LA",   # LA Rams
        "WSH": "WAS",  # Washington
        "JAC": "JAX",  # Jacksonville
    }
    
    def normalize_team(abbr: str) -> str:
        return ESPN_TEAM_MAP.get(abbr, abbr)
    
    try:
        ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        
        params = {"seasontype": 3 if week >= 19 else 2}
        if week >= 19:
            params["week"] = week - 18
        else:
            params["week"] = week
        
        resp = requests.get(ESPN_SCOREBOARD_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        games = []
        for event in data.get("events", []):
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            
            if len(competitors) != 2:
                continue
            
            home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
            
            status_info = competition.get("status", {}).get("type", {})
            
            # Normalize team abbreviations to match our database
            home_abbr = normalize_team(home["team"]["abbreviation"])
            away_abbr = normalize_team(away["team"]["abbreviation"])
            
            games.append({
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_score": int(home.get("score", 0)) if home.get("score") else None,
                "away_score": int(away.get("score", 0)) if away.get("score") else None,
                "status": status_info.get("name"),
                "status_detail": status_info.get("description"),
                "game_date": event.get("date"),
                "venue": competition.get("venue", {}).get("fullName"),
            })
        
        return {"week": week, "games": games}
        
    except Exception as e:
        logger.exception(f"Live scores fetch error: {e}")
        return {"week": week, "games": [], "error": str(e)}
