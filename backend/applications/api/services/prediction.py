import polars as pl
import math
import numpy as np
from difflib import get_close_matches
from ..config import logger, DB_CONNECTION_STRING, CURRENT_SEASON
from ..state import model_data
from .utils import calculate_fantasy_points, get_team_abbr, normalize_name, get_headshot_url, format_draft_info
from .data_loader import load_player_history_from_db

def get_injury_status_for_week(player_id: str, week: int, default="Active"):
    """
    Intelligent Injury Lookup.
    """
    if "df_injuries" not in model_data or model_data["df_injuries"].is_empty():
        logger.debug("Injury dataframe is missing or empty")
        return default

    df = model_data["df_injuries"]

    # Safety Check
    if "week" not in df.columns:
        logger.debug("Injury dataframe missing 'week' column; using fallback logic")
        # Fallback logic
        record = df.filter(pl.col("player_id") == str(player_id))
        if not record.is_empty():
            return record.row(0, named=True).get("injury_status", default)
        return default
    
    # Check Max Week in Memory
    max_wk = df.select(pl.col("week").max()).item()
    target_week = int(week)
    
    # If Future Week, use Max
    week_exists = not df.filter(pl.col("week") == int(week)).is_empty()
    if not week_exists:
        logger.debug(f"Week {week} not in injury data; falling back to latest known week {max_wk}")
        target_week = max_wk
    
    # Filter
    record = df.filter(
        (pl.col("week") == target_week) & 
        (pl.col("player_id") == str(player_id))
    )
    
    if not record.is_empty():
        status = record.row(0, named=True).get("injury_status", default)
        return status
    
    return default

def run_base_prediction(pid, pos, week):
    """
    Looks up features from DB. 
    1. RECENT FORM: Calculates average of the LAST 4 NON-ZERO GAMES.
    2. BASELINE CORRECTION: Uses that 4-game average as the starting point.
    3. LOGARITHMIC BOOST: 5.0 * ln(1 + deviation).
    4. USAGE VACUUM: Triggers on strict "Out/IR/Doubtful" status (Time-Aware).
    """
    # Initialize defaults
    features_dict = {}
    team = None
    # DEBUG: Report available dataframes and their columns (lightweight)
    try:
        logger.debug(f"Has dataframes: df_features={('df_features' in model_data)}, df_player_stats={('df_player_stats' in model_data)}, df_profile={('df_profile' in model_data)}")
        if 'df_features' in model_data:
            logger.debug(f"df_features columns: {model_data['df_features'].columns}")
        if 'df_player_stats' in model_data:
            logger.debug(f"df_player_stats columns: {model_data['df_player_stats'].columns}")
        if 'df_profile' in model_data:
            logger.debug(f"df_profile columns sample: {model_data['df_profile'].columns[:6]}")
    except Exception as e:
        logger.exception(f"Debug run info failure: {e}")
    
    # 1. Try to get features
    if "df_features" in model_data and not model_data["df_features"].is_empty() and 'player_id' in model_data['df_features'].columns and 'week' in model_data['df_features'].columns:
        player_features = model_data["df_features"].filter(
            (pl.col("player_id") == str(pid)) & (pl.col("week") == int(week))
        )
        if not player_features.is_empty():
            features_dict = player_features.row(0, named=True)
            team = features_dict.get('team') or features_dict.get('team_abbr')

    # 2. Fallback: Get Team/Pos from Profile
    if not team:
        prof = model_data["df_profile"].filter(pl.col("player_id") == str(pid))
        if not prof.is_empty():
            p_row = prof.row(0, named=True)
            team = p_row.get('team_abbr') or p_row.get('team')
        else:
            return None, "Player Not Found", None, 0.0

    if pos not in model_data["models"]: return None, "No Model", None, 0.0
    m_info = model_data["models"][pos]
    
    try:
        # --- 1. RECENT FORM (Last 4 Non-Zero Games) ---
        # Try to use in-memory player stats; if missing, fall back to direct DB query
        history_df = pl.DataFrame()
        if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty() and 'player_id' in model_data['df_player_stats'].columns:
            history_df = model_data['df_player_stats'].filter(
                (pl.col('player_id') == str(pid)) & 
                (pl.col('week') < int(week))
            )
        else:
            # Targeted DB lookup (player-level) as a robust fallback
            try:
                history_df = load_player_history_from_db(pid, week)
                if history_df is None: history_df = pl.DataFrame()
            except Exception as e:
                logger.warning(f"DB fallback failed for player history: {e}")
                history_df = pl.DataFrame()
        
        avg_recent_form = 0.0
        # If history_df is empty, as a last resort try a targeted DB load again
        if history_df.is_empty():
            try:
                history_db = load_player_history_from_db(pid, week)
                if history_db is not None and not history_db.is_empty():
                    history_df = history_db
            except Exception:
                pass

        if not history_df.is_empty():
            sorted_history = history_df.sort("week", descending=True)
            valid_pts = []
            for row in sorted_history.iter_rows(named=True):
                pts = calculate_fantasy_points(row)
                if pts > 0.0:
                    valid_pts.append(pts)
                if len(valid_pts) >= 4:
                    break
            
            if len(valid_pts) > 0:
                avg_recent_form = sum(valid_pts) / len(valid_pts)
            else:
                avg_recent_form = float(features_dict.get('player_season_avg_points', 0.0))
        else:
            avg_recent_form = float(features_dict.get('player_season_avg_points', 0.0))

        # --- 2. MODEL PREDICTION ---
        pred_dev = 0.0
        if features_dict:
            feats_input = {}
            for k in m_info["features"]:
                if k == 'player_season_avg_points':
                    feats_input[k] = [float(avg_recent_form)]
                else:
                    feats_input[k] = [float(features_dict.get(k) or 0.0)]
            
            try:
                pred_dev = m_info["model"].predict(pl.DataFrame(feats_input).to_numpy())[0]
            except: pred_dev = 0.0
        
        # --- 3. LOGARITHMIC BOOST (symmetric, sign-preserving) ---
        # Use log1p on absolute deviation to produce sharp increases for
        # small deviations and a tapering curve for large deviations.
        if pred_dev != 0:
            amplified_dev = math.copysign(5.0 * math.log1p(abs(pred_dev)), pred_dev)
        else:
            amplified_dev = 0.0
        
        # --- 4. BASELINE CORRECTION ---
        baseline = avg_recent_form

        # --- 5. USAGE VACUUM LOGIC (Robust Time-Aware) ---
        injury_boost = 0.0
        injury_statuses = ["IR", "Out", "Doubtful", "Inactive", "PUP"] 
        
        teammates = model_data["df_profile"].filter(
            (pl.col("team_abbr") == team) & (pl.col("position") == pos) & (pl.col("player_id") != pid)
        )
        
        for mate in teammates.iter_rows(named=True):
            mate_id = mate['player_id']

            # Check teammate injury status first; only injured teammates trigger usage boost logic (case-insensitive)
            status = str(get_injury_status_for_week(mate_id, week))
            status_norm = status.lower()
            if not any(s.lower() in status_norm for s in injury_statuses):
                continue  # skip teammates who are not injured

            # Fetch teammate historical stats to ensure they were a meaningful contributor
            mate_stats = pl.DataFrame()
            if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty() and 'player_id' in model_data['df_player_stats'].columns and 'week' in model_data['df_player_stats'].columns:
                mate_stats = model_data["df_player_stats"].filter(
                    (pl.col("player_id") == mate_id) & (pl.col("week") < int(week))
                )
            else:
                try:
                    q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{mate_id}' AND week < {int(week)} ORDER BY week DESC"
                    mate_stats = pl.read_database_uri(q, DB_CONNECTION_STRING)
                except Exception as e:
                    mate_stats = pl.DataFrame()

            if not mate_stats.is_empty():
                mate_pts = [calculate_fantasy_points(row) for row in mate_stats.to_dicts()]
                m_avg = sum(mate_pts) / len(mate_pts) if len(mate_pts) > 0 else 0

                m_snaps = 0.0
                if "df_snap_counts" in model_data:
                    mate_snaps_df = model_data["df_snap_counts"].filter(
                        (pl.col("player_id") == mate_id) & (pl.col("week") < int(week))
                    )
                    if not mate_snaps_df.is_empty():
                        try:
                            m_snaps = float(mate_snaps_df.select(pl.col("offense_pct")).mean().item())
                            # Normalize fraction -> percent
                            if m_snaps < 1.0: m_snaps *= 100
                            # Reject obviously invalid values
                            if math.isnan(m_snaps) or m_snaps < 0 or m_snaps > 200:
                                logger.debug(f"Unexpected mate_snaps value for {mate_id}: {m_snaps}; resetting to 0")
                                m_snaps = 0.0
                        except Exception as e:
                            logger.debug(f"Failed to compute mate_snaps for {mate_id}: {e}")
                            m_snaps = 0.0

                # Only apply boost if teammate was a consistent contributor (snaps and average points)
                # Loosen thresholds for RB/QB to catch more realistic vacancy cases (e.g., Kamara OUT should boost Saints RBs)
                if pos in ["RB", "QB"]:
                    if m_snaps >= 20 and m_avg >= 6:
                        injury_boost = 2.5
                        logger.info(f"Usage boost applied to {pid} due to teammate {mate_id} status {status} (m_avg={m_avg:.1f}, m_snaps={m_snaps:.1f})")
                        break
                else:
                    if m_snaps >= 20 and m_avg >= 8:
                        injury_boost = 1.5
                        logger.info(f"Usage boost applied to {pid} due to teammate {mate_id} status {status} (m_avg={m_avg:.1f}, m_snaps={m_snaps:.1f})")
                        break

        # --- 6. FINAL SCORE ---
        final_score = max(0.0, baseline + amplified_dev + injury_boost)
        is_boosted = injury_boost > 0
        
        return round(float(final_score), 2), is_boosted, features_dict, avg_recent_form
        
    except Exception as e:
        logger.exception(f"Prediction error for {pid}: {e}")
        return 0.0, False, features_dict, 0.0

def get_average_points_fallback(player_id, week):
    """Fallback calculation of average points if DB features are missing or malformed."""
    try:
        # Try in-memory first
        if 'df_player_stats' in model_data and not model_data['df_player_stats'].is_empty() and 'player_id' in model_data['df_player_stats'].columns:
            stats_history = model_data['df_player_stats'].filter((pl.col('player_id') == player_id) & (pl.col('week') < week))
        else:
            # Targeted DB fetch
            stats_history = load_player_history_from_db(player_id, week)
            if stats_history is None: return 0.0

        if not stats_history.is_empty():
            total_points, game_count = 0.0, 0
            for row in stats_history.iter_rows(named=True):
                pts = calculate_fantasy_points(row)
                if pts > 0 or row.get('offense_snaps', 0) > 0:
                    total_points += pts
                    game_count += 1
            if game_count > 0: return total_points / game_count
    except Exception as e:
        logger.warning(f"Average points fallback error: {e}")
    return 0.0

async def get_player_card(player_id: str, week: int):
    profile = model_data["df_profile"].filter(pl.col('player_id') == player_id)
    if profile.is_empty(): return None
    p_row = profile.row(0, named=True)
    
    pos = p_row['position']
    p_name = p_row['player_name']
    team = p_row.get('team_abbr') or p_row.get('team') or 'FA'

    # --- RUN PREDICTION ---
    l0_score, is_boosted, feats, rolling_avg_val = run_base_prediction(player_id, pos, week)
    
    # --- GET SEASON AVERAGE ---
    season_avg = 0.0
    if feats and isinstance(feats, dict) and feats.get("player_season_avg_points", 0) > 0:
        season_avg = feats["player_season_avg_points"]
    else:
        season_avg = get_average_points_fallback(player_id, week)

    if l0_score is None or l0_score == 0.0:
        # If prediction failed, try to compute rolling average directly from DB (robust fallback)
        if (not rolling_avg_val or rolling_avg_val == 0) and DB_CONNECTION_STRING:
            try:
                q = f"SELECT y_fantasy_points_ppr, passing_yards, rushing_yards, receiving_yards, receptions, passing_touchdown, rush_touchdown, receiving_touchdown, interceptions, fumbles_lost, week FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT 12"
                hist_df = pl.read_database_uri(q, DB_CONNECTION_STRING)
                if not hist_df.is_empty():
                    pts = []
                    for row in hist_df.iter_rows(named=True):
                        p = calculate_fantasy_points(row)
                        if p > 0: pts.append(p)
                        if len(pts) >= 4: break
                    if len(pts) > 0:
                        rolling_avg_val = sum(pts) / len(pts)
            except Exception as e:
                logger.warning(f"DB rolling average fallback failed for {player_id}: {e}")
        l0_score = season_avg if season_avg > 0 else rolling_avg_val

    meta_score = l0_score 

    # --- INJURY STATUS (Use Same Logic as Prediction) ---
    final_status = get_injury_status_for_week(player_id, week)

    # --- SNAP COUNT FALLBACK ---
    snap_pct, snap_count = 0.0, 0
    if feats and isinstance(feats, dict):
        snap_pct = float(feats.get('offense_pct', 0.0))
        snap_count = int(feats.get('offense_snaps', 0))

    if snap_count == 0:
        # Prefer in-memory snap history, otherwise query DB directly
        try:
            history_snaps = pl.DataFrame()
            if "df_snap_counts" in model_data and not model_data["df_snap_counts"].is_empty() and 'player_id' in model_data['df_snap_counts'].columns:
                history_snaps = model_data["df_snap_counts"].filter(
                    (pl.col("player_id") == player_id) & 
                    (pl.col("week") < int(week))
                ).sort("week", descending=True).head(1)
            else:
                # DB lookup for last snap counts
                q = f"SELECT * FROM weekly_snap_counts_{CURRENT_SEASON} WHERE player_id = '{player_id}' AND week < {int(week)} ORDER BY week DESC LIMIT 1"
                history_snaps = pl.read_database_uri(q, DB_CONNECTION_STRING)

            if not history_snaps.is_empty():
                last_game = history_snaps.row(0, named=True)
                snap_count = int(last_game.get('offense_snaps', 0))
                snap_pct = float(last_game.get('offense_pct', 0.0))
        except Exception as e:
            logger.warning(f"Snap fallback failed for {player_id}: {e}")
            snap_count, snap_pct = snap_count, snap_pct


    if snap_pct < 1.0 and snap_pct > 0: snap_pct *= 100

    total_line = None 
    spread_val = None
    implied_total = None
    props_data = [] 
    prop_line = None
    prop_prob = None
    pass_td_line = None
    pass_td_prob = None
    anytime_td_prob = None
    
    # New Props
    pass_att_line = None
    pass_att_prob = None
    rec_line = None
    rec_prob = None
    rush_att_line = None
    rush_att_prob = None

    try:
        if "df_lines" in model_data and not model_data["df_lines"].is_empty():
            lines = model_data["df_lines"].filter(
                (pl.col("week") == int(week)) & 
                ((pl.col("home_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team) | 
                 (pl.col("away_team").map_elements(get_team_abbr, return_dtype=pl.Utf8) == team))
            )
            if not lines.is_empty():
                row = lines.row(0, named=True)
                total_line = row.get("total_over")
                raw_spread = row.get("home_spread")
                
                # Calculate spread relative to player's team
                h_team = get_team_abbr(row.get("home_team"))
                p_team = get_team_abbr(team) # Ensure player team is also normalized
                
                if raw_spread is not None:
                    try:
                        s = float(raw_spread)
                        spread_val = s if h_team == p_team else -s
                        
                        # Calculate implied team total
                        if total_line:
                            t = float(total_line)
                            implied_total = (t / 2) - (spread_val / 2)
                    except: pass
    except Exception: pass

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
                
                target_props = []
                if pos == 'QB': target_props = ["Passing Yards", "Pass Yards", "Pass Yds"]
                elif pos == 'RB': target_props = ["Rushing Yards", "Rush Yards", "Rush Yds"]
                elif pos in ['WR', 'TE']: target_props = ["Receiving Yards", "Rec Yards", "Rec Yds"]
                
                if target_props:
                    # Try each target prop keyword
                    main_p = pl.DataFrame()
                    for tp in target_props:
                        main_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(tp.lower()))
                        if not main_p.is_empty():
                            break
                    
                    # If RB and Rushing Yards not found, try Rushing & Receiving Yards
                    if main_p.is_empty() and pos == 'RB':
                         main_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("rushing & receiving yards"))

                    if not main_p.is_empty():
                        # Sort to prefer exact match if possible (though contains is broad)
                        # Just take the first one for now
                        row = main_p.row(0, named=True)
                        prop_line = row['line']
                        prop_prob = row['implied_prob'] 
                
                if pos == 'QB':
                    # Try multiple variations for Passing TDs
                    td_pass = pl.DataFrame()
                    for keyword in ["passing touchdowns", "pass tds", "passing tds", "pass touchdowns"]:
                        td_pass = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(keyword))
                        if not td_pass.is_empty(): break
                    
                    if not td_pass.is_empty():
                        row = td_pass.row(0, named=True)
                        pass_td_line = row['line']
                        pass_td_prob = row['implied_prob']

                    # Pass Attempts
                    att_pass = pl.DataFrame()
                    for keyword in ["passing attempts", "pass attempts", "pass att"]:
                        att_pass = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(keyword))
                        if not att_pass.is_empty(): break
                    if not att_pass.is_empty():
                        row = att_pass.row(0, named=True)
                        pass_att_line = row['line']
                        pass_att_prob = row['implied_prob']

                if pos in ['WR', 'TE']:
                    # Receptions
                    rec_p = pl.DataFrame()
                    for keyword in ["receptions", "rec"]:
                        rec_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(keyword))
                        # Avoid "Receiving Yards" matching "Rec"
                        rec_p = rec_p.filter(~pl.col("prop_type").str.to_lowercase().str.contains("yards"))
                        if not rec_p.is_empty(): break
                    if not rec_p.is_empty():
                        row = rec_p.row(0, named=True)
                        rec_line = row['line']
                        rec_prob = row['implied_prob']

                if pos == 'RB':
                    # Rush Attempts
                    rush_att = pl.DataFrame()
                    for keyword in ["rushing attempts", "rush attempts", "rush att"]:
                        rush_att = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains(keyword))
                        if not rush_att.is_empty(): break
                    if not rush_att.is_empty():
                        row = rush_att.row(0, named=True)
                        rush_att_line = row['line']
                        rush_att_prob = row['implied_prob']

                td_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("anytime td"))
                if td_p.is_empty():
                    td_p = p_props.filter(pl.col("prop_type").str.to_lowercase().str.contains("anytime touchdown"))

                if not td_p.is_empty():
                    anytime_td_prob = td_p.row(0, named=True)['implied_prob']

    except Exception as e: logger.exception(f"Props extraction error: {e}")

    opponent = feats.get('opponent_team') if (feats and isinstance(feats, dict)) else None

    # If we still don't have an opponent, try schedule (DB-backed) lookup
    if not opponent or opponent == "BYE":
        try:
            # Look into in-memory schedule first
            sched_df = model_data.get('df_schedule') if 'df_schedule' in model_data else pl.DataFrame()
            if sched_df is None or sched_df.is_empty():
                # fallback to DB schedule read (ensure DB-only behavior)
                try:
                    sched_df = pl.read_database_uri(f"SELECT * FROM schedule", DB_CONNECTION_STRING)
                except Exception:
                    sched_df = pl.DataFrame()

            if not sched_df.is_empty():
                found = False
                for r in sched_df.iter_rows(named=True):
                    try:
                        if int(r.get('week', -1)) != int(week):
                            continue
                    except Exception:
                        continue
                    h_abbr = get_team_abbr(r.get('home_team') or '')
                    a_abbr = get_team_abbr(r.get('away_team') or '')
                    # If schedule stores abbr directly, this still works
                    if h_abbr == team or a_abbr == team:
                        opponent = a_abbr if h_abbr == team else h_abbr
                        found = True
                        break
                if not found:
                    opponent = opponent or "BYE"
        except Exception as e:
            logger.warning(f"Opponent lookup failed: {e}")
            opponent = opponent or "BYE"

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
        "spread": spread_val,
        "implied_total": round(implied_total, 1) if implied_total else None,
        "props": props_data,
        "prop_line": prop_line, 
        "prop_prob": prop_prob,
        "pass_td_line": pass_td_line, 
        "pass_td_prob": pass_td_prob, 
        "anytime_td_prob": anytime_td_prob,
        "pass_att_line": pass_att_line,
        "pass_att_prob": pass_att_prob,
        "rec_line": rec_line,
        "rec_prob": rec_prob,
        "rush_att_line": rush_att_line,
        "rush_att_prob": rush_att_prob,
        "image": get_headshot_url(player_id),
        "prediction": round(meta_score, 2),  
        "floor_prediction": round(meta_score * 0.8, 2),
        "average_points": round(season_avg, 1), 
        "rolling_4wk_avg": round(rolling_avg_val, 1), 
        "is_injury_boosted": is_boosted, 
        "injury_status": final_status, 
        "debug_err": None 
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
        ranked = candidates

    for pos, limit in composition.items():
        pos_candidates = ranked.filter(pl.col("position") == pos).head(limit)
        for row in pos_candidates.iter_rows(named=True):
            card = await get_player_card(row['player_id'], week)
            if card: roster_result.append(card)
            
    order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4}
    roster_result.sort(key=lambda x: order.get(x["position"], 99))
    return roster_result

def find_usage_boost_reason(player_id: str, week: int):
    """Returns details about whether a usage boost would be applied to this player and why.
    This mirrors the usage-boost check in `run_base_prediction` but returns diagnostic info.
    """
    try:
        profile = model_data.get("df_profile", pl.DataFrame()).filter(pl.col('player_id') == player_id)
        if profile.is_empty():
            return {"found": False, "error": "player profile not found"}
        p = profile.row(0, named=True)
        pos = p['position']
        team = p.get('team_abbr') or p.get('team') or 'FA'

        injury_statuses = [s.lower() for s in ["IR", "Out", "Doubtful", "Inactive", "PUP"]]

        teammates = model_data.get("df_profile", pl.DataFrame()).filter(
            (pl.col("team_abbr") == team) & (pl.col("position") == pos) & (pl.col("player_id") != player_id)
        )

        for mate in teammates.iter_rows(named=True):
            mate_id = mate['player_id']
            status = str(get_injury_status_for_week(mate_id, week)).lower()
            if not any(s in status for s in injury_statuses):
                continue

            # Gather mate stats
            try:
                mate_stats = pl.DataFrame()
                if "df_player_stats" in model_data and not model_data["df_player_stats"].is_empty():
                    mate_stats = model_data["df_player_stats"].filter((pl.col('player_id') == mate_id) & (pl.col('week') < int(week)))
                else:
                    q = f"SELECT * FROM weekly_player_stats_{CURRENT_SEASON} WHERE player_id = '{mate_id}' AND week < {int(week)} ORDER BY week DESC"
                    mate_stats = pl.read_database_uri(q, DB_CONNECTION_STRING)

                if mate_stats.is_empty():
                    continue

                mate_pts = [calculate_fantasy_points(r) for r in mate_stats.to_dicts()]
                m_avg = sum(mate_pts) / len(mate_pts) if len(mate_pts) > 0 else 0

                m_snaps = 0.0
                if "df_snap_counts" in model_data and not model_data["df_snap_counts"].is_empty():
                    sdf = model_data["df_snap_counts"].filter((pl.col('player_id') == mate_id) & (pl.col('week') < int(week)))
                    if not sdf.is_empty():
                        try:
                            m_snaps = float(sdf.select(pl.col('offense_pct')).mean().item())
                            if m_snaps < 1.0: m_snaps *= 100
                            if math.isnan(m_snaps) or m_snaps < 0 or m_snaps > 200:
                                m_snaps = 0.0
                        except:
                            m_snaps = 0.0

                # Match thresholds with run_base_prediction
                threshold_snaps = 20
                threshold_pts = 6 if pos in ["RB", "QB"] else 8

                if m_snaps >= threshold_snaps and m_avg >= threshold_pts:
                    return {"found": True, "boosted": True, "mate_id": mate_id, "mate_status": status, "mate_avg_points": m_avg, "mate_avg_snaps": m_snaps}

            except Exception as e:
                continue

        return {"found": True, "boosted": False}

    except Exception as e:
        return {"found": False, "error": str(e)}
