from fastapi import APIRouter
import os
import json
import requests
import polars as pl
from ..state import model_data
from ..config import DB_CONNECTION_STRING, ETL_SCRIPT_PATH, WATCHLIST_FILE
from ..services.data_loader import refresh_app_state
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
        "current_week": model_data.get("current_nfl_week", None)
    }

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

    return status

@router.get('/players/search')
async def search_players(q: str):
    if not q: return []
    try:
        expr = (pl.col('player_name').str.to_lowercase().str.contains(q.lower()) & (pl.col('position').is_in(['QB', 'RB', 'WR', 'TE'])))
        return model_data["df_profile"].filter(expr).select(['player_id', 'player_name', 'position', 'team_abbr', 'headshot', 'status']).head(20).to_dicts()
    except: return []

async def fetch_sleeper_trends(trend_type: str, limit: int = 10, week: int = 1):
    if not model_data.get("sleeper_map"): refresh_app_state() 
    try:
        url = f"https://api.sleeper.app/v1/players/nfl/trending/{trend_type}?lookback_hours=24&limit={limit+10}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=3)
        if response.status_code != 200: return []
        data = response.json()
        cards = []
        for item in data:
            sleeper_id = str(item.get("player_id"))
            count = item.get("count", 0)
            our_id = model_data["sleeper_map"].get(sleeper_id)
            if our_id:
                card = await get_player_card(our_id, week)
                if card:
                    card["trending_count"] = count 
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
