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
import subprocess
import traceback
from contextlib import asynccontextmanager
import nflreadpy as nfl
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from difflib import get_close_matches

# --- 1. CONFIGURATION ---
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:    
    print("⚠️ Warning: DB_CONNECTION_STRING not found. Server will rely on local CSVs.")

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
dataPrep_dir = os.path.abspath(os.path.join(current_dir, '..', 'dataPrep'))
RAG_DIR = os.path.join(PROJECT_ROOT, 'rag_data')
MODEL_DIR = os.path.join(PROJECT_ROOT, 'model_training', 'models')

# Correctly locate the ETL script
ETL_SCRIPT_PATH = os.path.abspath(os.path.join(current_dir, '..', 'rag_data', '05_etl_to_postgres.py'))

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

# --- TEAM MAPPING (CRITICAL FIX) ---
# Maps Bovada "Full Names" to Schedule "Abbreviations"
TEAM_ABBR_MAP = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS"
}

MODELS_CONFIG = {
    'QB': {'model': os.path.join(MODEL_DIR, 'xgboost_QB_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_QB_sliding_window_v1(TimeSeries).json')},
    'RB': {'model': os.path.join(MODEL_DIR, 'xgboost_RB_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_RB_sliding_window_v1(TimeSeries).json')},
    'WR': {'model': os.path.join(MODEL_DIR, 'xgboost_WR_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_WR_sliding_window_v1(TimeSeries).json')},
    'TE': {'model': os.path.join(MODEL_DIR, 'xgboost_TE_sliding_window_v1(TimeSeries).joblib'), 'features': os.path.join(MODEL_DIR, 'feature_names_TE_sliding_window_v1(TimeSeries).json')}
}
META_MODEL_PATH = os.path.join(MODEL_DIR, 'xgboost_META_model_v1.joblib')
META_FEATURES_PATH = os.path.join(MODEL_DIR, 'feature_names_META_model_v1.json')

model_data = {}

# --- HELPER: Type Enforcement ---
def enforce_types(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty(): return df
    cols = df.columns
    exprs = []
    if "week" in cols: exprs.append(pl.col("week").fill_null(-1).cast(pl.Int64, strict=False))
    if "season" in cols: exprs.append(pl.col("season").fill_null(-1).cast(pl.Int64, strict=False))
    if exprs:
        try: return df.with_columns(exprs)
        except Exception: return df
    return df

# --- HELPER: Normalization ---
def normalize_name(name):
    if not name: return ""
    return str(name).lower().replace(".", "").replace(" ", "").strip()

def get_team_abbr(raw_team):
    """Converts 'Dallas Cowboys' -> 'DAL' safely."""
    clean = str(raw_team).strip()
    return TEAM_ABBR_MAP.get(clean, clean)

# --- HELPER: Load Data ---
def load_data_source(query: str, csv_filename: str):
    if DB_CONNECTION_STRING:
        try:
            df = pl.read_database_uri(query, DB_CONNECTION_STRING)
            return enforce_types(df)
        except Exception as e:
            if "relation" not in str(e):
                print(f"   ⚠️ DB Load Failed: {e}")
    
    csv_path = os.path.join(RAG_DIR, csv_filename)
    if os.path.exists(csv_path):
        try:
            df = pl.read_csv(csv_path, ignore_errors=True)
            return enforce_types(df)
        except Exception as e:
            print(f"   ❌ CSV Load Failed: {csv_filename} - {e}")
    return pl.DataFrame()

def refresh_db_data():
    print("🔄 [System] Loading Dataframes...")
    sources = {
        "df_profile": ("SELECT * FROM player_profiles", f"player_profiles_{CURRENT_SEASON}.csv"),
        "df_schedule": ("SELECT * FROM schedule", f"schedule_{CURRENT_SEASON}.csv"),
        "df_player_stats": ("SELECT * FROM weekly_player_stats", f"weekly_player_stats_{CURRENT_SEASON}.csv"),
        "df_snap_counts": ("SELECT * FROM weekly_snap_counts", f"weekly_snap_counts_{CURRENT_SEASON}.csv"),
        "df_lines": ("SELECT * FROM bovada_game_lines", f"weekly_bovada_game_lines_{CURRENT_SEASON}.csv"),
        "df_props": ("SELECT * FROM bovada_player_props", f"weekly_bovada_player_props_{CURRENT_SEASON}.csv"),
        "df_defense": ("SELECT * FROM weekly_defense_stats", f"weekly_defense_stats_{CURRENT_SEASON}.csv"),
        "df_offense": ("SELECT * FROM weekly_offense_stats", f"weekly_offense_stats_{CURRENT_SEASON}.csv"),
        "df_formation": ("SELECT * FROM weekly_formation_stats", f"weekly_formation_stats_{CURRENT_SEASON}.csv"),
        "df_injuries": ("SELECT * FROM weekly_injuries", f"weekly_injuries_{CURRENT_SEASON}.csv"),
    }
    
    for key, (query, csv) in sources.items():
        model_data[key] = load_data_source(query, csv)

   # --- BUILD INJURY MAPS (ID & NAME) ---
    model_data["injury_map"] = {}
    model_data["injury_name_map"] = {}
    
    if "df_injuries" in model_data and not model_data["df_injuries"].is_empty():
        try:
            df = model_data["df_injuries"]
            # Clean columns: strip whitespace
            if "player_id" in df.columns:
                df = df.with_columns(pl.col("player_id").cast(pl.Utf8).str.strip_chars())
            
            # Map 1: ID -> Status
            if "player_id" in df.columns and "injury_status" in df.columns:
                rows = df.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
            
            # Map 2: Name -> Status (Fallback)
            if "player_name" in df.columns and "injury_status" in df.columns:
                df = df.with_columns(pl.col("player_name").map_elements(normalize_name, return_dtype=pl.Utf8).alias("norm_name"))
                rows = df.select(["norm_name", "injury_status"]).to_dicts()
                model_data["injury_name_map"] = {r["norm_name"]: r["injury_status"] for r in rows}
                
            print(f"   🚑 Loaded Injuries: {len(model_data['injury_map'])} by ID, {len(model_data['injury_name_map'])} by Name.")
        except Exception as e:
            print(f"   ⚠️ Injury Map Build Error: {e}")

    # Build Game Spreads fallback
    if "df_schedule" in model_data and not model_data["df_schedule"].is_empty():
        try:
            cols = [c for c in ["week", "home_team", "away_team", "over_under"] if c in model_data["df_schedule"].columns]
            model_data["df_game_spreads"] = model_data["df_schedule"].select(cols)
        except:
            model_data["df_game_spreads"] = pl.DataFrame()
    else:
        model_data["df_game_spreads"] = pl.DataFrame()

    print("✅ [System] Data Loaded.")
def refresh_app_state():
    print("🔄 [Scheduler] Refreshing App State...")
    try:
        base_week = nfl.get_current_week()
        if datetime.now().weekday() == 1: 
            model_data["current_nfl_week"] = base_week + 1
        else:
            model_data["current_nfl_week"] = base_week
        print(f"✅ Active NFL Week: {model_data['current_nfl_week']}")
    except Exception:
        model_data["current_nfl_week"] = 1

    try:
        players_df = nfl.load_ff_playerids()
        if "sleeper_id" in players_df.columns:
            map_df = players_df.drop_nulls(subset=['sleeper_id', 'gsis_id'])
            model_data["sleeper_map"] = dict(zip(map_df['sleeper_id'].cast(pl.Utf8).to_list(), map_df['gsis_id'].to_list()))
    except Exception:
        model_data["sleeper_map"] = {}

def run_daily_etl():
    print("\n⏰ [Scheduler] Starting Daily ETL Pipeline...")
    if not os.path.exists(ETL_SCRIPT_PATH): return
    try:
        subprocess.run([sys.executable, ETL_SCRIPT_PATH], capture_output=True, text=True, encoding='utf-8')
        refresh_app_state()
        refresh_db_data()
        print("✅ ETL Complete")
    except Exception as e:
        print(f"❌ ETL Failed: {e}")

# --- 3. LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- 🚀 Server Startup Sequence ---")
    try:
        model_data["models"] = {}
        for pos, paths in MODELS_CONFIG.items():
            if os.path.exists(paths['model']):
                model_data["models"][pos] = {"model": joblib.load(paths['model']), "features": json.load(open(paths['features']))}
        if os.path.exists(META_MODEL_PATH):
            model_data["meta_models"] = joblib.load(META_MODEL_PATH)
            model_data["meta_features"] = json.load(open(META_FEATURES_PATH))
        refresh_app_state()
        refresh_db_data()
    except Exception as e:
        print(f"❌ Startup Error: {e}")
    
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_daily_etl, 'cron', hour=5, minute=0)
    scheduler.add_job(refresh_app_state, 'interval', hours=1)
    scheduler.start()
    yield
    scheduler.shutdown()
    model_data.clear()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- 4. CORE LOGIC ---
def get_headshot_url(player_id: str):
    try:
        row = model_data["df_profile"].filter(pl.col("player_id") == player_id)
        if not row.is_empty(): return row.row(0, named=True).get("headshot") or f"https://sleepercdn.com/content/nfl/players/{player_id}.jpg"
    except: pass
    return f"https://sleepercdn.com/content/nfl/players/{player_id}.jpg"

def format_draft_info(year, number):
    if year and number and not np.isnan(number):
        return f"Pick {int(number)} ({int(year)})"
    return "Undrafted"

def calculate_fantasy_points(row):
    try:
        if row.get('y_fantasy_points_ppr') is not None: return float(row['y_fantasy_points_ppr'])
        p_yds = row.get('passing_yards') or 0
        p_tds = row.get('passing_tds') or 0
        r_yds = row.get('rushing_yards') or 0
        r_tds = row.get('rushing_tds') or 0
        rec_yds = row.get('receiving_yards') or 0
        rec_tds = row.get('receiving_tds') or 0
        receptions = row.get('receptions') or 0
        ints = row.get('interceptions') or 0
        fumbles = row.get('fumbles_lost') or 0
        return ((p_yds * 0.04) + (p_tds * 4.0) + (r_yds * 0.1) + (r_tds * 6.0) + (rec_yds * 0.1) + (rec_tds * 6.0) + (receptions * 1.0) - (ints * 2.0) - (fumbles * 2.0))
    except: return 0.0

def run_base_prediction(pid, pos, week):
    formation_df = model_data.get("df_formation", pl.DataFrame())
    features, err = generate_features_all(
        pid, week, model_data["df_profile"], model_data["df_schedule"],
        model_data["df_player_stats"], model_data["df_defense"],
        model_data["df_offense"], model_data["df_snap_counts"],
        df_formation=formation_df
    )
    if not features: return None, err, None
    if pos not in model_data["models"]: return None, "No Model", None
    
    m_info = model_data["models"][pos]
    feats = {k: [features.get(k, 0.0)] for k in m_info["features"]}
    try:
        pred = m_info["model"].predict(pl.DataFrame(feats).to_numpy())[0]
        return float(pred), None, features
    except Exception as e:
        return 0.0, str(e), features

async def get_player_card(player_id: str, week: int):
    profile = model_data["df_profile"].filter(pl.col('player_id') == player_id)
    if profile.is_empty(): return None
    p_row = profile.row(0, named=True)
    
    pos = p_row['position']
    p_name = p_row['player_name']
    team = p_row.get('team_abbr') or p_row.get('team') or 'FA'

    l0_score, err, feats = run_base_prediction(player_id, pos, week)
    if not l0_score: l0_score = 0.0
    l0_score = max(0.0, l0_score)

    try:
        teammates = {'QB': 0.0, 'RB': 0.0, 'WR': 0.0, 'TE': 0.0}
        teammates[pos] = l0_score
        
        team_col = 'team_abbr' if 'team_abbr' in model_data["df_profile"].columns else 'team'
        
        roster = model_data["df_profile"].filter(
            (pl.col(team_col) == team) & 
            (pl.col('status') == 'ACT') & 
            (pl.col('player_id') != player_id)
        )
        for p_pos in ['QB', 'RB', 'WR', 'TE']:
            if p_pos == pos: continue
            mate = roster.filter(pl.col('position') == p_pos).head(1)
            if not mate.is_empty():
                m_id = mate.row(0, named=True)['player_id']
                s, _, _ = run_base_prediction(m_id, p_pos, week)
                if s: teammates[p_pos] = max(0.0, float(s))

        if pos == 'QB': meta_score = l0_score
        elif pos in model_data.get("meta_models", {}):
            meta_in = {f"L0_pred_{k}": [v] for k, v in teammates.items()}
            meta_input = pl.DataFrame(meta_in).select(model_data["meta_features"]).to_numpy()
            meta_score = float(model_data["meta_models"][pos].predict(meta_input)[0])
            meta_score = max(0.0, meta_score)
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
        row = model_data["df_snap_counts"].filter(
            (pl.col("player_id") == player_id) & 
            (pl.col("week") == int(week))
        )
        if not row.is_empty(): 
            d = row.row(0, named=True)
            snap_count = int(d.get("offense_snaps", 0) or 0)
            snap_pct = float(d.get("offense_pct", 0.0) or 0.0)
        else:
            hist = model_data["df_snap_counts"].filter(pl.col("player_id") == player_id)
            if not hist.is_empty():
                snap_pct = hist["offense_pct"].mean()
    except Exception: pass

    # --- LINES ---
    total_line = None
    try:
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines = model_data["df_lines"].filter(
                (pl.col("week") == int(week)) & 
                ((pl.col("home_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team) | 
                 (pl.col("away_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team))
            )
            if not lines.is_empty():
                total_line = lines.row(0, named=True).get("total_over")
    except Exception: pass

    # --- PROPS ---
    props_data = []
    prop_line = None
    prop_prob = None
    pass_td_line = None
    pass_td_prob = None
    anytime_td_prob = None

    try:
        if "df_props" in model_data and not model_data["df_props"].is_empty():
            week_props = model_data["df_props"].filter(pl.col("week") == int(week))
            
            p_norm = normalize_name(p_name)
            week_props = week_props.with_columns(
                pl.col("player_name").map_elements(normalize_name, return_dtype=pl.Utf8).alias("norm_name")
            )
            p_props = week_props.filter(pl.col("norm_name") == p_norm)

            if p_props.is_empty():
                all_names = week_props["player_name"].unique().to_list()
                matches = get_close_matches(p_name, all_names, n=1, cutoff=0.6)
                if matches:
                    p_props = week_props.filter(pl.col("player_name") == matches[0])

            if not p_props.is_empty():
                props_data = p_props.select(["prop_type", "line", "odds", "implied_prob"]).to_dicts()
                
                target_prop = None
                if pos == 'QB': target_prop = "Passing Yards"
                elif pos == 'RB': target_prop = "Rushing Yards"
                elif pos in ['WR', 'TE']: target_prop = "Receiving Yards"
                
                if target_prop:
                    main_p = p_props.filter(pl.col("prop_type").str.contains(target_prop))
                    if not main_p.is_empty():
                        row = main_p.row(0, named=True)
                        prop_line = row['line']
                        prop_prob = row['implied_prob'] 
                
                if pos == 'QB':
                    td_pass = p_props.filter(pl.col("prop_type").str.contains("Passing Touchdowns"))
                    if not td_pass.is_empty():
                        row = td_pass.row(0, named=True)
                        pass_td_line = row['line']
                        pass_td_prob = row['implied_prob']

                td_p = p_props.filter(pl.col("prop_type").str.contains("Anytime TD"))
                if not td_p.is_empty():
                    anytime_td_prob = td_p.row(0, named=True)['implied_prob']

    except Exception as e: print(f"Props extraction error: {e}")

    opponent = "BYE"
    try:
        game = model_data["df_schedule"].filter((pl.col("week") == int(week)) & ((pl.col("home_team") == team) | (pl.col("away_team") == team)))
        if not game.is_empty(): 
            r = game.row(0, named=True)
            opponent = r['away_team'] if r['home_team'] == team else r['home_team']
    except: pass

    return {
        "player_name": p_name,
        "player_id": player_id,
        "position": pos,
        "week": week,
        "team": team,
        "opponent": opponent,
        "draft_position": format_draft_info(p_row.get('draft_year'), p_row.get('draft_number')),
        "snap_count": snap_count,
        "snap_percentage": snap_pct,
        "overunder": float(total_line) if total_line else None,
        "spread": float(total_line) if total_line else None,
        "props": props_data,
        "prop_line": prop_line, 
        "prop_prob": prop_prob,
        "pass_td_line": pass_td_line, 
        "pass_td_prob": pass_td_prob, 
        "anytime_td_prob": anytime_td_prob,
        "image": get_headshot_url(player_id),
        "prediction": round(meta_score + mae, 2),  
        "floor_prediction": round(meta_score, 2),
        "average_points": round(avg_points, 1),
        "injury_status": model_data.get("injury_map", {}).get(player_id, "Active")
    }

async def get_team_roster_cards(team_abbr: str, week: int):
    composition = {"QB": 4, "RB": 8, "WR": 8, "TE": 5}
    roster_result = []
    
    ranked = pl.DataFrame()
    try:
        q = f"SELECT player_id, position FROM weekly_rankings WHERE week={week} AND team_abbr='{team_abbr}' ORDER BY predicted_points DESC"
        ranked = pl.read_database_uri(q, DB_CONNECTION_STRING)
    except: pass

    if ranked.is_empty():
        team_col = "team_abbr" if "team_abbr" in model_data["df_profile"].columns else "team"
        candidates = model_data["df_profile"].filter(
            (pl.col(team_col) == team_abbr) & 
            (pl.col("status") == "ACT")
        ).select(["player_id", "position"])
        
        if not model_data['df_player_stats'].is_empty():
            pts_col = "fantasy_points_ppr"
            if pts_col not in model_data['df_player_stats'].columns:
                if "y_fantasy_points_ppr" in model_data['df_player_stats'].columns:
                    pts_col = "y_fantasy_points_ppr"
                else:
                    pts_col = None
            if pts_col:
                try:
                    stats_agg = model_data['df_player_stats'].group_by("player_id").agg(pl.col(pts_col).sum().alias("season_pts"))
                    candidates = candidates.join(stats_agg, on="player_id", how="left").fill_null(0).sort("season_pts", descending=True)
                except: pass
        ranked = candidates

    for pos, limit in composition.items():
        pos_candidates = ranked.filter(pl.col("position") == pos).head(limit)
        for row in pos_candidates.iter_rows(named=True):
            card = await get_player_card(row['player_id'], week)
            if card: roster_result.append(card)
            
    order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4}
    roster_result.sort(key=lambda x: order.get(x["position"], 99))
    return roster_result

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
    try:
        if model_data["df_schedule"].is_empty(): return []
        
        max_week = model_data["df_schedule"]["week"].max()
        target_week = week if week <= max_week else max_week
        
        sched_df = model_data["df_schedule"].filter(pl.col("week") == int(target_week))
        games = sched_df.to_dicts()
        
        matched_count = 0
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines_df = model_data["df_lines"].filter(pl.col("week") == int(target_week))
            
            # Create a robust lookup map
            odds_map = {}
            for row in lines_df.iter_rows(named=True):
                # Normalizes "Dallas Cowboys" -> "DAL"
                h_abbr = get_team_abbr(row['home_team'])
                a_abbr = get_team_abbr(row['away_team'])
                
                # Store under both permutations to be safe
                odds_map[(h_abbr, a_abbr)] = row
                odds_map[(a_abbr, h_abbr)] = row
            
            for game in games:
                key = (game['home_team'], game['away_team'])
                match = odds_map.get(key)
                
                if match:
                    game['moneyline_home'] = match.get('home_ml')
                    game['moneyline_away'] = match.get('away_ml')
                    game['game_total'] = match.get('total_over')
                    matched_count += 1
                else:
                    game['moneyline_home'] = None
                    game['moneyline_away'] = None
                    game['game_total'] = None
        
        print(f"✅ Schedule (Wk {week}): Odds attached for {matched_count}/{len(games)} games.")
        return games

    except Exception as e:
        print(f"Schedule Error: {e}")
        return []

@app.get("/matchup/{week}/{home_team}/{away_team}")
async def get_matchup_rosters(week: int, home_team: str, away_team: str):
    try:
        home_cards = await get_team_roster_cards(home_team, week)
        away_cards = await get_team_roster_cards(away_team, week)
        
        over_under, home_win, away_win = None, None, None
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines = model_data["df_lines"].filter(pl.col("week") == int(week))
            for row in lines.iter_rows(named=True):
                h = get_team_abbr(row['home_team'])
                a = get_team_abbr(row['away_team'])
                
                if (h == home_team and a == away_team) or (h == away_team and a == home_team):
                    over_under = row.get('total_over')
                    home_win = row.get('home_ml_prob') if h == home_team else row.get('away_ml_prob')
                    away_win = row.get('away_ml_prob') if h == home_team else row.get('home_ml_prob')
                    break

        return {
            "matchup": f"{away_team} @ {home_team}",
            "week": week,
            "over_under": over_under,
            "home_win_prob": home_win,
            "away_win_prob": away_win,
            "home_roster": home_cards,
            "away_roster": away_cards
        }
    except Exception as e: 
        print(f"Matchup Error Details: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to load matchup: {str(e)}")

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
        expr = (pl.col('player_name').str.to_lowercase().str.contains(q.lower()) & (pl.col('position').is_in(['QB', 'RB', 'WR', 'TE'])))
        return model_data["df_profile"].filter(expr).select(['player_id', 'player_name', 'position', 'team_abbr', 'headshot', 'status']).head(20).to_dicts()
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
            p_yds = row.get('passing_yards') or 0
            r_yds = row.get('rushing_yards') or 0
            rec_yds = row.get('receiving_yards') or 0
            p_tds = row.get('passing_tds') or 0
            r_tds = row.get('rushing_tds') or 0
            rec_tds = row.get('receiving_tds') or 0
            receptions = int(row.get('receptions') or 0)
            targets = int(row.get('targets') or 0)
            carries = int(row.get('rush_attempts') or 0)
            pts = calculate_fantasy_points(row)
            history.append({
                "week": wk,
                "opponent": row.get('opponent_team') or "N/A",
                "points": round(float(pts), 2),
                "passing_yds": int(p_yds),
                "rushing_yds": int(r_yds),
                "receiving_yds": int(rec_yds),
                "touchdowns": int(p_tds + r_tds + rec_tds),
                "snap_count": snap_count,
                "snap_percentage": snap_pct,
                "receptions": receptions,
                "targets": targets,
                "carries": carries 
            })
        return history
    except Exception as e:
        print(f"History Error: {e}")
        return []

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)