import polars as pl
import os
import time
import nflreadpy as nfl
from datetime import datetime
from ..config import logger, DB_CONNECTION_STRING, RAG_DIR, CURRENT_SEASON
from ..state import model_data
from .utils import enforce_types

def load_data_source(query: str, csv_filename: str, retries: int = 3, retry_delay: float = 1.0):
    """Try DB first with retries. By default the server runs in DB-only mode (no CSV fallback) unless ALLOW_CSV_FALLBACK is set to 'true'."""
    ALLOW_CSV_FALLBACK = os.getenv("ALLOW_CSV_FALLBACK", "false").lower() == "true"

    # Try DB with retries
    if DB_CONNECTION_STRING:
        attempt = 0
        while attempt < retries:
            try:
                df = pl.read_database_uri(query, DB_CONNECTION_STRING)
                logger.info(f"DB Load successful: {csv_filename} (attempt {attempt+1})")
                return enforce_types(df)
            except Exception as e:
                attempt += 1
                if attempt >= retries:
                    # If table missing show a specific hint
                    if "relation" in str(e).lower():
                        logger.warning(f"DB relation/table missing for {csv_filename}: {e}")
                    else:
                        logger.error(f"DB Load failed for {csv_filename} after {attempt} attempts: {e}")
                else:
                    time.sleep(retry_delay)

    # If CSV fallback is explicitly allowed, try it (development only)
    if ALLOW_CSV_FALLBACK:
        csv_path = os.path.join(RAG_DIR, csv_filename)
        if os.path.exists(csv_path):
            try:
                df = pl.read_csv(csv_path, ignore_errors=True)
                logger.info(f"CSV load successful (fallback): {csv_filename}")
                return enforce_types(df)
            except Exception as e:
                logger.error(f"CSV load failed: {csv_filename} - {e}")
        else:
            logger.warning(f"CSV fallback requested but file not found: {csv_filename}")
    logger.warning(f"Returning empty DataFrame for {csv_filename} (DB-only mode)")
    return pl.DataFrame()

def refresh_db_data():
    logger.info("Loading dataframes from DB/CVS sources...")
    sources = {
        "df_profile": ("SELECT * FROM player_profiles", f"player_profiles_{CURRENT_SEASON}.csv"),
        "df_schedule": ("SELECT * FROM schedule", f"schedule_{CURRENT_SEASON}.csv"),
        "df_player_stats": (f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv"),
        "df_snap_counts": (f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv"),
        "df_lines": ("SELECT * FROM bovada_game_lines", f"weekly_bovada_game_lines_{CURRENT_SEASON}.csv"),
        "df_props": ("SELECT * FROM bovada_player_props", f"weekly_bovada_player_props_{CURRENT_SEASON}.csv"),
        "df_injuries": (f"SELECT * FROM weekly_injuries_{CURRENT_SEASON}", f"weekly_injuries_{CURRENT_SEASON}.csv"),
        "df_features": (f"SELECT * FROM weekly_feature_set_{CURRENT_SEASON}", f"weekly_feature_set_{CURRENT_SEASON}.csv"),
    }
    
    for key, (query, csv) in sources.items():
        model_data[key] = load_data_source(query, csv)

    # If critical tables are empty, attempt an aggressive retry for player stats and snaps
    if ("df_player_stats" in model_data and model_data["df_player_stats"].is_empty()) or ("df_snap_counts" in model_data and model_data["df_snap_counts"].is_empty()):
        logger.warning("Critical tables empty after initial load — retrying DB loads for essential tables...")
        try:
            model_data["df_player_stats"] = load_data_source(f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON}", f"weekly_player_stats_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for player_stats: {e}")
        try:
            model_data["df_snap_counts"] = load_data_source(f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON}", f"weekly_snap_counts_{CURRENT_SEASON}.csv", retries=5, retry_delay=2.0)
        except Exception as e:
            logger.error(f"Retry failed for snap_counts: {e}")

    # --- BUILD INJURY MAP (ROBUST FIX) ---
    model_data["injury_map"] = {}
    model_data["gsis_to_sleeper"] = {}
    
    if "df_injuries" in model_data and not model_data["df_injuries"].is_empty():
        try:
            df = model_data["df_injuries"]
            
            # 1. Check if 'week' column exists (New Format)
            if "week" in df.columns:
                # Find the LATEST week available in the file
                max_wk = df.select(pl.col("week").max()).item()
                logger.info(f"Filtering injury map to latest week: {max_wk}")
                latest_report = df.filter(pl.col("week") == max_wk)
                
                rows = latest_report.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
            else:
                # Fallback for old CSVs without week column
                logger.warning("Injury CSV lacks 'week' column. Loading all rows (last write wins).")
                rows = df.select(["player_id", "injury_status"]).to_dicts()
                model_data["injury_map"] = {r["player_id"]: r["injury_status"] for r in rows}
                
        except Exception as e: 
            logger.exception(f"Injury map build error: {e}")

    try:
        players_df = nfl.load_ff_playerids()
        if "sleeper_id" in players_df.columns:
            map_df = players_df.drop_nulls(subset=['sleeper_id', 'gsis_id'])
            model_data["gsis_to_sleeper"] = dict(zip(map_df['gsis_id'].to_list(), map_df['sleeper_id'].cast(pl.Utf8).to_list()))
            model_data["sleeper_map"] = dict(zip(map_df['sleeper_id'].cast(pl.Utf8).to_list(), map_df['gsis_id'].to_list()))
    except Exception: pass

    logger.info("Data loaded into memory.")

def refresh_app_state():
    logger.info("Refreshing app state (scheduler) ...")
    try:
        base_week = nfl.get_current_week()
        
        # Smart week detection: Only advance the week if ALL games in base_week have been played
        # (i.e., have scores). This prevents prematurely jumping to next week during bye weeks
        # or if games haven't started yet (e.g., playoffs on Saturday).
        should_advance = False
        
        if "df_schedule" in model_data and not model_data["df_schedule"].is_empty():
            sched = model_data["df_schedule"]
            week_games = sched.filter(pl.col("week") == base_week)
            
            if not week_games.is_empty():
                # Check if all games have scores (home_score is not null)
                if "home_score" in week_games.columns:
                    games_with_scores = week_games.filter(pl.col("home_score").is_not_null())
                    all_played = len(games_with_scores) == len(week_games)
                    
                    if all_played:
                        should_advance = True
                        logger.info(f"All {len(week_games)} games in Week {base_week} have been played. Advancing to next week.")
                    else:
                        logger.info(f"Week {base_week}: {len(games_with_scores)}/{len(week_games)} games played. Staying on Week {base_week}.")
                else:
                    # If no home_score column, fall back to old Tuesday logic
                    if datetime.now().weekday() == 1:
                        should_advance = True
            else:
                # No games found for this week (e.g., bye week), use base_week as-is
                logger.info(f"No games found for Week {base_week}. Staying on Week {base_week}.")
        else:
            # No schedule data loaded yet, fall back to old Tuesday logic
            if datetime.now().weekday() == 1:
                should_advance = True
        
        if should_advance:
            model_data["current_nfl_week"] = base_week + 1
        else:
            model_data["current_nfl_week"] = base_week
            
        logger.info(f"Active NFL Week: {model_data['current_nfl_week']}")
    except Exception as e:
        logger.exception(f"Error determining current week: {e}")
        model_data["current_nfl_week"] = 1

def load_player_history_from_db(player_id: str, week: int, limit: int = 12):
    """Load a player's recent history directly from DB (limited rows)."""
    if not DB_CONNECTION_STRING:
        return pl.DataFrame()
    try:
        q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT {int(limit)}"
        df = pl.read_database_uri(q, DB_CONNECTION_STRING)
        return enforce_types(df)
    except Exception as e:
        logger.warning(f"load_player_history_from_db error: {e}")
        return pl.DataFrame()
