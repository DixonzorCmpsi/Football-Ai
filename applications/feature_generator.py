# dataPrep/feature_generator.py
import polars as pl
from datetime import datetime
import sys
import os
import numpy as np
import nflreadpy as nfl # Needed for snaps/player map if using richer features

# --- Define file paths (Relative to this script in dataPrep) ---
# Adjust these paths if needed based on your final project structure
PROFILE_PATH = '../rag_data/player_profiles.csv'
SCHEDULE_PATH = '../rag_data/schedule_2025.csv'
PLAYER_STATS_PATH = '../rag_data/weekly_player_stats_2025.csv'
DEFENSE_STATS_PATH = '../rag_data/weekly_defense_stats_2025.csv'
OFFENSE_STATS_PATH = '../rag_data/weekly_offense_stats_2025.csv' # Needed if calculating shares

# --- Define constants ---
PLAYER_ROLLING_WINDOW = 3
OPP_ROLLING_WINDOW = 4
CURRENT_SEASON = 2025

# --- Helper Function ---
def calculate_rolling_avg(df: pl.DataFrame, col: str, window: int) -> float | None:
    """Safely calculates rolling average for the last 'window' rows."""
    if df is None or df.is_empty() or col not in df.columns or window <= 0:
        return 0.0
    try:
        numeric_col = pl.col(col).cast(pl.Float64, strict=False)
        avg = df.head(window).select(numeric_col.mean()).item()
        return avg if avg is not None and not np.isnan(avg) else 0.0
    except (pl.ComputeError, TypeError, pl.SchemaError):
        return 0.0
    except Exception:
        return 0.0

# --- Feature Generation Function (Using simpler feature set for now) ---
def generate_features(player_id: str, # gsis_id
                     target_week: int,
                     current_season: int = CURRENT_SEASON,
                     # Pass loaded dataframes for efficiency
                     df_profile: pl.DataFrame | None = None,
                     df_schedule: pl.DataFrame | None = None,
                     df_player_stats: pl.DataFrame | None = None,
                     df_defense: pl.DataFrame | None = None):
    """
    Generates the simpler feature set matching the original model's likely inputs,
    using pre-loaded dataframes.
    """
    # print(f"Generating features for {player_id}, Wk {target_week}") # Debug

    # --- Load Data IF NOT PROVIDED (Less efficient) ---
    try:
        if df_profile is None: df_profile = pl.read_csv(PROFILE_PATH)
        if df_schedule is None: df_schedule = pl.read_csv(SCHEDULE_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        if df_player_stats is None: df_player_stats = pl.read_csv(PLAYER_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
        if df_defense is None: df_defense = pl.read_csv(DEFENSE_STATS_PATH).with_columns(pl.col("week").cast(pl.Int64, strict=False))
    except Exception as e: print(f"Load Error: {e}", file=sys.stderr); return None

    # --- Get Player Info ---
    player_info = df_profile.filter(pl.col('player_id') == player_id)
    if player_info.is_empty(): print(f"Player ID {player_id} not found."); return None
    try:
        player_team = player_info.select('team_abbr').item()
        player_position = player_info.select('position').item()
        player_name = player_info.select('player_name').item()
        player_age = player_info.select(pl.col('age').fill_null(25)).item()
        player_years_exp = player_info.select(pl.col('years_exp').fill_null(0)).item() if 'years_exp' in player_info.columns else 0
        player_draft_ovr = player_info.select(pl.col('draft_number').fill_null(260)).item() if 'draft_number' in player_info.columns else 260
        player_status = player_info.select('injury_status').item()
    except Exception as e: print(f"Player Info Error for {player_id}: {e}", file=sys.stderr); return None

    # --- Get Opponent Info ---
    game_info = df_schedule.filter((pl.col('week') == target_week) & ((pl.col('home_team') == player_team) | (pl.col('away_team') == player_team)))
    if game_info.is_empty(): print(f"No game for {player_team} in wk {target_week}."); return None
    is_home = (game_info.select('home_team').item() == player_team)
    opponent_team = game_info.select('away_team').item() if is_home else game_info.select('home_team').item()

    # --- Get Histories ---
    player_history = df_player_stats.filter((pl.col('player_id') == player_id) & (pl.col('week') < target_week)).sort('week', descending=True)
    opponent_defense_history = df_defense.filter((pl.col('team_abbr') == opponent_team) & (pl.col('week') < target_week)).sort('week', descending=True)

    # --- Calculate Features ---
    features = {}
    # Identifiers (useful for context, removed before prediction)
    features['player_id'], features['player_name'], features['target_week'] = player_id, player_name, target_week
    features['opponent'], features['team'], features['position'] = opponent_team, player_team, player_position

    # Features likely expected by the model (based on the 62 list)
    features['season'] = current_season
    features['week'] = target_week
    features['age'], features['years_exp'], features['draft_ovr'] = player_age, player_years_exp, player_draft_ovr

    # "Last Game Played" Stats
    last_game_played_stats = player_history.head(1)
    last_game_played_dict = last_game_played_stats.row(0, named=True) if not last_game_played_stats.is_empty() else {}
    features['touches'] = last_game_played_dict.get('touches', 0.0)
    features['targets'] = last_game_played_dict.get('targets', 0.0)
    features['receptions'] = last_game_played_dict.get('receptions', 0.0)
    features['rush_attempts'] = last_game_played_dict.get('rush_attempts', 0.0)
    features['pass_attempts'] = last_game_played_dict.get('pass_attempts', 0.0)
    features['receiving_yards'] = last_game_played_dict.get('receiving_yards', 0.0)
    features['rushing_yards'] = last_game_played_dict.get('rushing_yards', 0.0)
    features['yards_after_catch'] = last_game_played_dict.get('yards_after_catch', 0.0)
    features['passing_air_yards'] = last_game_played_dict.get('passing_air_yards', 0.0)
    features['receiving_air_yards'] = last_game_played_dict.get('receiving_air_yards', 0.0)
    features['adot'] = last_game_played_dict.get('adot', 0.0)
    features['yptouch'] = last_game_played_dict.get('yptouch', 0.0)
    features['passer_rating'] = last_game_played_dict.get('passer_rating', 0.0) if player_position == 'QB' else 0.0
    features['ypc'] = last_game_played_dict.get('ypc', 0.0)
    features['ypr'] = last_game_played_dict.get('ypr', 0.0)
    features['interception'] = last_game_played_dict.get('interception', 0.0)
    features['receiving_touchdown'] = last_game_played_dict.get('receiving_touchdown', 0.0)
    features['rush_touchdown'] = last_game_played_dict.get('rush_touchdown', 0.0)
    features['pass_pct'] = last_game_played_dict.get('pass_pct', 0.0) if player_position == 'QB' else 0.0
    features['total_off_yards'] = last_game_played_dict.get('total_off_yards', 0.0)
    # Explicitly set features NOT generated by this script to 0.0 if model expects them
    features['offense_snaps'], features['offense_pct'] = 0.0, 0.0
    features['team_targets_share'], features['team_receptions_share'], features['team_rush_attempts_share'] = 0.0, 0.0, 0.0
    features['season_average_targets'], features['season_average_touches'] = 0.0, 0.0
    features['ayptarget'], features['career_average_ppr_ppg'] = 0.0, 0.0
    features['rush_attempts_redzone'], features['targets_redzone'], features['receptions_redzone'], features['pass_attempts_redzone'] = 0.0, 0.0, 0.0, 0.0
    features['shotgun'], features['no_huddle'], features['qb_dropback'], features['qb_scramble'] = 0.0, 0.0, 0.0, 0.0
    features['rolling_avg_qb_hit_4_weeks'] = 0.0
    features['rolling_avg_points_allowed_to_QB'], features['rolling_avg_points_allowed_to_RB'] = 0.0, 0.0
    features['rolling_avg_points_allowed_to_WR'], features['rolling_avg_points_allowed_to_TE'] = 0.0, 0.0
    features['3_game_avg_offense_pct'] = 0.0

    # Player Rolling Averages
    prw = PLAYER_ROLLING_WINDOW
    features['3_game_avg_targets'] = calculate_rolling_avg(player_history, 'targets', prw)
    features['3_game_avg_receptions'] = calculate_rolling_avg(player_history, 'receptions', prw)
    features['3_game_avg_rush_attempts'] = calculate_rolling_avg(player_history, 'rush_attempts', prw)
    features['3_game_avg_receiving_yards'] = calculate_rolling_avg(player_history, 'receiving_yards', prw)
    features['3_game_avg_rushing_yards'] = calculate_rolling_avg(player_history, 'rushing_yards', prw)
    features['3_game_avg_y_fantasy_points_ppr'] = calculate_rolling_avg(player_history, 'y_fantasy_points_ppr', prw)

    # Opponent Defense Rolling Averages
    orw = OPP_ROLLING_WINDOW
    league_avg_pts_allowed, league_avg_pass_yds_allowed, league_avg_rush_yds_allowed = 22.0, 230.0, 115.0
    league_avg_sacks, league_avg_interceptions = 2.5, 0.8
    features['rolling_avg_points_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'points_allowed', orw) or league_avg_pts_allowed
    features['rolling_avg_passing_yards_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'passing_yards_allowed', orw) or league_avg_pass_yds_allowed
    features['rolling_avg_rushing_yards_allowed_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'rushing_yards_allowed', orw) or league_avg_rush_yds_allowed
    features['rolling_avg_sack_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'def_sacks', orw) or league_avg_sacks
    features['rolling_avg_interception_4_weeks'] = calculate_rolling_avg(opponent_defense_history, 'def_interceptions', orw) or league_avg_interceptions

    # One-Hot Encode Position (Based on 62 features list - missing QB)
    pos = player_position if player_position else 'UNK'
    # features['position_QB'] = 1 if pos == 'QB' else 0 # Excluded
    features['position_RB'] = 1 if pos == 'RB' else 0
    features['position_TE'] = 1 if pos == 'TE' else 0
    features['position_WR'] = 1 if pos == 'WR' else 0

    # Binary Flags (Excluded as they were missing from model's feature list)
    # features['is_home'] = 1 if is_home else 0
    # features['is_active'] = 1 if player_status in ['ACT', 'A01'] else 0

    # Final Imputation (Should mostly handle type consistency)
    final_features = {}
    for k, v in features.items():
        if k in ['player_id', 'player_name', 'position', 'team', 'opponent']: final_features[k] = v if v is not None else ""
        elif v is None or (isinstance(v, float) and np.isnan(v)): final_features[k] = 0.0
        else:
             try: final_features[k] = float(v) if v is not None else 0.0
             except (ValueError, TypeError): final_features[k] = 0.0
    return final_features

# --- NO MAIN EXECUTION BLOCK ---
# This file is now intended to be imported as a module.