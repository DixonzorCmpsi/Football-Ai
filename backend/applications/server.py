from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import polars as pl
import joblib
import json
import sys
import os
import asyncio
import requests
import numpy as np
from contextlib import asynccontextmanager
import nflreadpy as nfl
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:    
    sys.exit("Error: DB_CONNECTION_STRING not found.")

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
dataPrep_dir = os.path.abspath(os.path.join(current_dir, '..', 'dataPrep'))
RAG_DIR = os.path.join(PROJECT_ROOT, 'rag_data')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'model_training', 'models')

if dataPrep_dir not in sys.path: sys.path.insert(0, dataPrep_dir)

try:
    from feature_generator_timeseries import generate_features_all
except ImportError:
    print("Warning: feature_generator_timeseries not found.")

# --- 2. CONSTANTS ---
CURRENT_SEASON = 2025
MAE_VALUES = {'QB': 4.30, 'RB': 5.19, 'WR': 4.33, 'TE': 4.34}
META_MAE_VALUES = {'QB': 4.79, 'RB': 3.66, 'WR': 2.88, 'TE': 2.41}
WATCHLIST_FILE = os.path.join(RAG_DIR, 'watchlist.json')

MODELS_CONFIG = {
    'QB': {'model': os.path.join(MODEL_DIR, 'xgboost_QB_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_QB_sliding_window_v1(TimeSeries).json')},
    'RB': {'model': os.path.join(MODEL_DIR, 'xgboost_RB_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_RB_sliding_window_v1(TimeSeries).json')},
    'WR': {'model': os.path.join(MODEL_DIR, 'xgboost_WR_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_WR_sliding_window_v1(TimeSeries).json')},
    'TE': {'model': os.path.join(MODEL_DIR, 'xgboost_TE_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_TE_sliding_window_v1(TimeSeries).json')}
}
META_MODEL_PATH = os.path.join(MODEL_DIR, 'xgboost_META_model_v1.joblib')
META_FEATURES_PATH = os.path.join(MODEL_DIR, 'feature_names_META_model_v1.json')

model_data = {}

# --- HELPER: Load ID Map Only (Injuries now in DB) ---
def refresh_id_map():
    print("📥 Downloading/Refreshing Player ID Map...")
    try:
        players_df = nfl.load_ff_playerids()
        if "sleeper_id" not in players_df.columns or "gsis_id" not in players_df.columns:
            return False

        map_df = players_df.drop_nulls(subset=['sleeper_id', 'gsis_id'])
        sleeper_ids = map_df['sleeper_id'].cast(pl.Utf8).to_list()
        gsis_ids = map_df['gsis_id'].to_list()
        
        model_data["sleeper_map"] = dict(zip(sleeper_ids, gsis_ids))
        print(f"✅ ID Map Ready ({len(model_data['sleeper_map'])} players mapped)")
        return True
    except Exception as e:
        print(f"⚠️ ID Map Download Failed: {e}")
        model_data["sleeper_map"] = {}
        return False

# --- 3. LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- 🚀 Server Startup ---")
    try:
        # Load Dataframes from DB
        # NOTE: df_injuries now targets the specific season table created by your generator
        queries = {
            "df_profile": "SELECT * FROM player_profiles",
            "df_schedule": "SELECT * FROM schedule",
            "df_player_stats": "SELECT * FROM weekly_player_stats",
            "df_snap_counts": "SELECT * FROM weekly_snap_counts",
            "df_odds": "SELECT * FROM weekly_player_odds",
            "df_defense": "SELECT * FROM weekly_defense_stats",
            "df_offense": "SELECT * FROM weekly_offense_stats",
            "df_overunder": "SELECT * FROM schedule",
            "df_injuries": f"SELECT * FROM weekly_injuries_{CURRENT_SEASON}", # <--- FIXED TABLE NAME
            "df_game_spreads": "SELECT week, home_team, away_team, over_under FROM schedule WHERE over_under IS NOT NULL"
        }
        for key, query in queries.items():
            try:
                model_data[key] = pl.read_database_uri(query, DB_CONNECTION_STRING)
            except Exception as e:
                print(f"⚠️ {key} load failed: {e}")
                model_data[key] = pl.DataFrame()

        # Build Injury Lookup Dict
        try:
            if not model_data["df_injuries"].is_empty():
                print("✅ Injuries Loaded from DB")
                # Map gsis_id -> report_status
                inj = model_data["df_injuries"].select(['gsis_id', 'report_status']).drop_nulls()
                model_data["injury_map"] = dict(zip(inj['gsis_id'], inj['report_status']))
            else:
                model_data["injury_map"] = {}
        except:
            model_data["injury_map"] = {}

        # Load Models
        model_data["models"] = {}
        for pos, paths in MODELS_CONFIG.items():
            if os.path.exists(paths['model']):
                model_data["models"][pos] = {
                    "model": joblib.load(paths['model']),
                    "features": json.load(open(paths['features']))
                }
        
        if os.path.exists(META_MODEL_PATH):
            model_data["meta_models"] = joblib.load(META_MODEL_PATH)
            model_data["meta_features"] = json.load(open(META_FEATURES_PATH))

        refresh_id_map()

        try: model_data["current_nfl_week"] = nfl.get_current_week()
        except: model_data["current_nfl_week"] = 1
        print(f"✅ Ready. Week: {model_data['current_nfl_week']}")

    except Exception as e:
        print(f"❌ Startup Error: {e}")
        sys.exit(1)
    
    yield
    model_data.clear()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 4. CORE LOGIC ---

def get_headshot_url(player_id: str):
    try:
        row = model_data["df_profile"].filter(pl.col("player_id") == player_id)
        if not row.is_empty():
            db_url = row.row(0, named=True).get("headshot")
            if db_url: return db_url
    except: pass
    return f"https://sleepercdn.com/content/nfl/players/{player_id}.jpg"

def format_draft_info(year, number):
    if year and number and not np.isnan(number):
        return f"Pick {int(number)} ({int(year)})"
    return "Undrafted"

def calculate_fantasy_points(row):
    try:
        if row.get('y_fantasy_points_ppr') is not None: return float(row['y_fantasy_points_ppr'])
        if row.get('fantasy_points_ppr') is not None: return float(row['fantasy_points_ppr'])
        if row.get('fantasy_points') is not None: return float(row['fantasy_points'])
        
        p_yds = row.get('passing_yards') or row.get('pass_yds') or 0
        p_tds = row.get('passing_tds') or 0
        r_yds = row.get('rushing_yards') or row.get('rush_yds') or 0
        r_tds = row.get('rushing_tds') or 0
        rec_yds = row.get('receiving_yards') or row.get('rec_yds') or 0
        rec_tds = row.get('receiving_tds') or 0
        receptions = row.get('receptions') or 0
        ints = row.get('interceptions') or 0
        fumbles = row.get('fumbles_lost') or 0
        
        return (
            (p_yds * 0.04) + (p_tds * 4.0) + (r_yds * 0.1) + (r_tds * 6.0) + 
            (rec_yds * 0.1) + (rec_tds * 6.0) + (receptions * 1.0) - 
            (ints * 2.0) - (fumbles * 2.0)
        )
    except: return 0.0

def run_base_prediction(pid, pos, week):
    features, err = generate_features_all(
        pid, week, model_data["df_profile"], model_data["df_schedule"],
        model_data["df_player_stats"], model_data["df_defense"],
        model_data["df_offense"], model_data["df_snap_counts"]
    )
    if not features: return None, err, None
    
    if pos not in model_data["models"]: return None, "No Model", None
    m_info = model_data["models"][pos]
    feats = {k: [features.get(k, 0.0)] for k in m_info["features"]}
    pred = m_info["model"].predict(pl.DataFrame(feats).to_numpy())[0]
    return float(pred), None, features

async def get_player_card(player_id: str, week: int):
    profile = model_data["df_profile"].filter(pl.col('player_id') == player_id)
    if profile.is_empty(): return None
    p_row = profile.row(0, named=True)
    pos, team = p_row['position'], p_row['team_abbr']

    l0_score, err, feats = run_base_prediction(player_id, pos, week)
    if err: l0_score = 0.0

    try:
        teammates = {'QB': 0.0, 'RB': 0.0, 'WR': 0.0, 'TE': 0.0}
        teammates[pos] = l0_score or 0.0
        roster = model_data["df_profile"].filter((pl.col('team_abbr') == team) & (pl.col('status') == 'ACT') & (pl.col('player_id') != player_id))
        for p_pos in ['QB', 'RB', 'WR', 'TE']:
            if p_pos == pos: continue
            mate = roster.filter(pl.col('position') == p_pos).head(1)
            if not mate.is_empty():
                m_id = mate.row(0, named=True)['player_id']
                s, _, _ = run_base_prediction(m_id, p_pos, week)
                if s: teammates[p_pos] = s

        if pos == 'QB': meta_score = l0_score
        elif pos in model_data["meta_models"]:
            meta_in = {f"L0_pred_{k}": [v] for k, v in teammates.items()}
            meta_input = pl.DataFrame(meta_in).select(model_data["meta_features"]).to_numpy()
            meta_score = float(model_data["meta_models"][pos].predict(meta_input)[0])
        else: meta_score = l0_score 
    except: meta_score = l0_score 
    
    mae = META_MAE_VALUES.get(pos, 5.0)
    
    avg_points = 0.0
    try:
        stats_history = model_data['df_player_stats'].filter((pl.col('player_id') == player_id) & (pl.col('week') < week))
        if not stats_history.is_empty():
            total_points, game_count = 0.0, 0
            for row in stats_history.iter_rows(named=True):
                pts = calculate_fantasy_points(row)
                if pts > 0 or row.get('offense_snaps', 0) > 0:
                    total_points += pts
                    game_count += 1
            if game_count > 0: avg_points = total_points / game_count
    except: pass

    snap_count, snap_pct = 0, 0.0
    try:
        row = model_data["df_snap_counts"].filter((pl.col("player_id") == player_id) & (pl.col("week") == week))
        if not row.is_empty(): d = row.row(0, named=True); snap_count, snap_pct = int(d.get("offense_snaps", 0)), float(d.get("offense_pct", 0.0))
    except: pass

    # --- OVER/UNDER (TOTAL LINE) LOGIC ---
    total_line = None
    try:
        spread_df = model_data["df_game_spreads"].filter(pl.col("week") == week)
        
        # Check if the player's team is in the game
        game_row = spread_df.filter((pl.col("home_team") == team) | (pl.col("away_team") == team))
        
        if not game_row.is_empty():
            # The over_under value (total line) is the same regardless of which team the player is on
            total_line = game_row.row(0, named=True).get("over_under")
            if total_line is not None:
                total_line = float(total_line)

    except Exception as e:
        print(f"Over/Under lookup error: {e}")
        total_line = None
    
    # Opponent Lookup
    opponent = "BYE"
    try:
        game = model_data["df_schedule"].filter((pl.col("week") == week) & ((pl.col("home_team") == team) | (pl.col("away_team") == team)))
        if not game.is_empty(): row = game.row(0, named=True); opponent = row['away_team'] if row['home_team'] == team else row['home_team']
    except: pass

    # --- INJURY STATUS (FROM DB) ---
    injury_status = model_data.get("injury_map", {}).get(player_id, "Active")

    return {
        "player_name": p_row['player_name'],
        "player_id": player_id,
        "position": pos,
        "week": week,
        "team": team,
        "opponent": opponent,
        "draft_position": format_draft_info(p_row.get('draft_year'), p_row.get('draft_number')),
        "snap_count": snap_count,
        "snap_percentage": snap_pct,
        "overunder": total_line,
        "spread": total_line, # <--- FIXED: Passing OVER/UNDER value using the 'spread' key for frontend compatibility
        "image": get_headshot_url(player_id),
        "prediction": round(meta_score + mae, 2),  
        "floor_prediction": round(meta_score, 2),
        "average_points": round(avg_points, 1),
        "injury_status": injury_status
    }
async def get_team_roster_cards(team_abbr: str, week: int):
    composition = {"QB": 5, "RB": 5, "WR": 5, "TE": 5}
    roster_result = []
    try:
        q = f"SELECT player_id, position FROM weekly_rankings WHERE week={week} AND team_abbr='{team_abbr}' ORDER BY predicted_points DESC"
        ranked = pl.read_database_uri(q, DB_CONNECTION_STRING)
    except: ranked = pl.DataFrame()
    if ranked.is_empty():
        ranked = model_data["df_profile"].filter((pl.col("team_abbr") == team_abbr) & (pl.col("status") == "ACT")).select(["player_id", "position"])
    for pos, limit in composition.items():
        candidates = ranked.filter(pl.col("position") == pos).head(limit)
        for row in candidates.iter_rows(named=True):
            card = await get_player_card(row['player_id'], week)
            if card: roster_result.append(card)
    order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4}
    roster_result.sort(key=lambda x: order.get(x["position"], 99))
    return roster_result

async def fetch_sleeper_trends(trend_type: str, limit: int = 10, week: int = 1):
    if not model_data.get("sleeper_map"):
        if not refresh_id_map(): return []

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

# --- 5. ENDPOINTS ---
class PlayerRequest(BaseModel):
    player_name: str
    week: Optional[int] = None

class CompareRequest(BaseModel):
    player1_name: str
    player2_name: str
    week: Optional[int] = None

@app.get("/player/{player_id}")
async def get_player_by_id(player_id: str, week: Optional[int] = None):
    try:
        wk = week if week else model_data["current_nfl_week"]
        card = await get_player_card(player_id, wk)
        if not card: raise HTTPException(404, "Player not found")
        return card
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/current_week")
async def get_current_week(): return {"week": model_data.get("current_nfl_week", 1)}

@app.get("/schedule/{week}")
async def get_schedule(week: int):
    try: return model_data["df_schedule"].filter(pl.col("week") == week).to_dicts()
    except: return []

@app.get("/matchup/{week}/{home_team}/{away_team}")
async def get_matchup_rosters(week: int, home_team: str, away_team: str):
    try:
        home_cards = await get_team_roster_cards(home_team, week)
        away_cards = await get_team_roster_cards(away_team, week)
        return {"matchup": f"{away_team} @ {home_team}", "week": week, "home_roster": home_cards, "away_roster": away_cards}
    except Exception as e: raise HTTPException(status_code=500, detail="Failed to load matchup")

@app.post("/predict")
async def predict(req: PlayerRequest):
    try:
        match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == req.player_name.lower())
        if match.is_empty(): raise HTTPException(404, "Player not found")
        pid = match.row(0, named=True)['player_id']
        wk = req.week if req.week else model_data["current_nfl_week"]
        return await get_player_card(pid, wk)
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/compare")
async def compare(req: CompareRequest):
    wk = req.week if req.week else model_data["current_nfl_week"]
    res = []
    for name in [req.player1_name, req.player2_name]:
        try:
            match = model_data["df_profile"].filter(pl.col('player_name').str.to_lowercase() == name.lower())
            if not match.is_empty():
                pid = match.row(0, named=True)['player_id']
                res.append(await get_player_card(pid, wk))
            else: res.append({"error": f"Player {name} not found"})
        except: res.append({"error": "Lookup failed"})
    return {"week": wk, "comparison": res}

@app.get('/players/search')
async def search_players(q: str):
    if not q: return []
    try:
        # REMOVED the 'ACT' status filter so you can find everyone
        expr = (
            pl.col('player_name').str.to_lowercase().str.contains(q.lower()) &
            (pl.col('position').is_in(['QB', 'RB', 'WR', 'TE']))
        )
        return model_data["df_profile"].filter(expr).select([
            'player_id', 'player_name', 'position', 'team_abbr', 'headshot', 'status'
        ]).head(20).to_dicts()
    except: return []
@app.get("/rankings/past/{week}")
async def get_trending_down(week: int):
    return await fetch_sleeper_trends("drop", limit=30, week=week)

@app.get("/rankings/future/{week}")
async def get_trending_up(week: int):
    return await fetch_sleeper_trends("add", limit=30, week=week)

@app.get("/player/history/{player_id}")
async def get_player_history(player_id: str):
    try:
        stats = model_data['df_player_stats'].filter(pl.col('player_id') == player_id).unique(subset=['week']).sort('week')
        player_snaps = model_data['df_snap_counts'].filter(pl.col('player_id') == player_id)
        if stats.is_empty(): return []

        history = []
        for row in stats.iter_rows(named=True):
            wk = row['week']
            snap_count, snap_pct = 0, 0.0
            s_row = player_snaps.filter(pl.col('week') == wk)
            if not s_row.is_empty():
                s_data = s_row.row(0, named=True)
                snap_count, snap_pct = int(s_data.get('offense_snaps', 0)), float(s_data.get('offense_pct', 0.0))

            p_yds = row.get('passing_yards') or row.get('pass_yds') or 0
            r_yds = row.get('rushing_yards') or row.get('rush_yds') or 0
            rec_yds = row.get('receiving_yards') or row.get('rec_yds') or 0
            p_tds = row.get('passing_tds') or 0
            r_tds = row.get('rushing_tds') or 0
            rec_tds = row.get('receiving_tds') or 0
            receptions = int(row.get('receptions') or 0)
            targets = int(row.get('targets') or row.get('tgt') or 0)

            pts = calculate_fantasy_points(row)

            history.append({
                "week": wk,
                "opponent": row.get('opponent_team') or row.get('opponent') or "N/A",
                "points": round(float(pts), 2),
                "passing_yds": int(p_yds),
                "rushing_yds": int(r_yds),
                "receiving_yds": int(rec_yds),
                "touchdowns": int(p_tds + r_tds + rec_tds),
                "snap_count": snap_count,
                "snap_percentage": snap_pct,
                "receptions": receptions,
                "targets": targets
            })
        return history
    except: return []

# --- WATCHLIST ---
def load_wl(): return json.load(open(WATCHLIST_FILE)) if os.path.exists(WATCHLIST_FILE) else []
@app.get('/watchlist')
async def get_watchlist():
    ids = load_wl()
    if not ids: return []
    return model_data["df_profile"].filter(pl.col("player_id").is_in(ids)).select(['player_id', 'player_name', 'team_abbr', 'position']).to_dicts()
@app.post('/watchlist')
async def add_watchlist(item: dict):
    ids = load_wl()
    if item['player_id'] not in ids:
        ids.append(item['player_id'])
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids
@app.delete('/watchlist/{player_id}')
async def remove_watchlist(player_id: str):
    ids = load_wl()
    if player_id in ids:
        ids.remove(player_id)
        with open(WATCHLIST_FILE, 'w') as f: json.dump(ids, f)
    return ids

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)