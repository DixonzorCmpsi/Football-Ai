# dataPrep/feature_generator_timeseries.py
import polars as pl
import numpy as np
import nflreadpy as nfl
from pathlib import Path

# --- Configuration ---
CURRENT_SEASON = 2025
N_LAGS = 3 # Use 3 lags, just like in your training data
OPP_ROLLING_WINDOW = 4 # Use 4 games for opponent averages

def get_lagged_value(df: pl.DataFrame, col: str, lag: int) -> float:
    """Retrieves the value from 'lag' games ago."""
    if df is None or df.is_empty() or col not in df.columns:
        return 0.0
    row_idx = lag - 1
    if row_idx < df.height:
        val = df[row_idx, col]
        return float(val) if val is not None and not np.isnan(val) else 0.0
    else:
        return 0.0 # Not enough history

def calculate_live_rolling_avg(df_history: pl.DataFrame, col: str, window: int) -> float:
    """
    Calculates the rolling average from the raw historical data.
    """
    if df_history is None or df_history.is_empty() or col not in df_history.columns:
        return 0.0
    
    avg = df_history.head(window).select(
        pl.col(col).fill_null(0.0).cast(pl.Float64).mean()
    ).item()
    
    return avg if avg is not None and not np.isnan(avg) else 0.0


def generate_features_all(
    player_id: str, # gsis_id
    target_week: int,
    # Pass all pre-loaded dataframes from the server
    df_profile: pl.DataFrame,
    df_schedule: pl.DataFrame,
    df_player_stats: pl.DataFrame,
    df_defense: pl.DataFrame,
    df_offense: pl.DataFrame,
    df_snap_counts: pl.DataFrame
    # --- FIX: 'df_players_map' is no longer needed ---
):
    """
    Generates a superset of time-series features for ANY position
    using the live 2025 RAG data.
    """
    
    # --- 1. Get Player Info ---
    player_info = df_profile.filter(pl.col('player_id') == player_id)
    if player_info.is_empty(): return None, "Player not found."
    
    try:
        player_team = player_info['team_abbr'].item()
        player_position = player_info['position'].item()
        player_name = player_info['player_name'].item()
        # Static features
        age = player_info.select(pl.col('age').fill_null(25)).item()
        years_exp = player_info.select(pl.col('years_exp').fill_null(0)).item() if 'years_exp' in player_info.columns else 0
        draft_ovr = player_info.select(pl.col('draft_number').fill_null(260)).item() if 'draft_number' in player_info.columns else 260
        player_status = player_info['injury_status'].item()
    except Exception as e: return None, f"Player Info Error: {e}"

    # --- 2. Get Opponent Info ---
    game_info = df_schedule.filter((pl.col('week') == target_week) & ((pl.col('home_team') == player_team) | (pl.col('away_team') == player_team)))
    if game_info.is_empty(): return None, "Likely Bye Week"
    is_home = (game_info['home_team'].item() == player_team)
    opponent_team = game_info['away_team'].item() if is_home else game_info['home_team'].item()

    # --- 3. Prepare History (Stats + Snaps) ---
    player_history_stats = df_player_stats.filter((pl.col('player_id') == player_id) & (pl.col('week') < target_week)).sort('week', descending=True)
    
    # --- FIX: Remove pfr_id lookup ---
    # pfr_player_id is no longer needed
    
    player_history = player_history_stats # Default
    if df_snap_counts is not None:
        # --- FIX: Join directly on player_id (gsis_id) ---
        snaps = df_snap_counts.filter((pl.col('player_id') == player_id) & (pl.col('season') == CURRENT_SEASON) & (pl.col('week') < target_week))
        if not snaps.is_empty():
            try:
                # Join player stats with their snap counts
                player_history = player_history_stats.join(
                    snaps.select(['week', 'offense_snaps', 'offense_pct']), 
                    on='week', 
                    how='left'
                ).fill_null(0.0) # Fill nulls that result from the join
            except Exception:
                pass # Fallback to stats only

    opponent_defense_history = df_defense.filter((pl.col('team_abbr') == opponent_team) & (pl.col('week') < target_week)).sort('week', descending=True)

    # --- 4. Calculate Features ---
    features = {}
    
    # --- A: Static & Context Features ---
    features['age'] = age
    features['years_exp'] = years_exp
    features['draft_ovr'] = draft_ovr
    features['shotgun'] = 0.0 
    features['no_huddle'] = 0.0
    features['qb_dropback'] = 0.0
    features['qb_scramble'] = 0.0

    # --- B: Player Lagged Features (Lags 1, 2, 3) ---
    player_cols_to_lag = [
        'y_fantasy_points_ppr', 'offense_pct', 'offense_snaps', 'touches', 
        'targets', 'receptions', 'receiving_yards', 'yards_after_catch', 
        'receiving_air_yards', 'receiving_touchdown',
        'rush_attempts', 'rushing_yards', 'rush_touchdown', 'ypc',
        'pass_attempts', 'passing_air_yards', 'passer_rating', 'interception',
        'total_off_yards', 'team_targets_share', 'team_receptions_share', 'team_rush_attempts_share',
        'adot', 'ayptarget', 'ypr', 'targets_redzone', 'receptions_redzone'
    ]
    
    for col in player_cols_to_lag:
        for lag in range(1, N_LAGS + 1):
            features[f'{col}_lag_{lag}'] = get_lagged_value(player_history, col, lag)

    # --- C: Opponent Rolling Averages (FIXED) ---
    features['rolling_avg_points_allowed_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'points_allowed', OPP_ROLLING_WINDOW)
    features['rolling_avg_passing_yards_allowed_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'passing_yards_allowed', OPP_ROLLING_WINDOW)
    features['rolling_avg_rushing_yards_allowed_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'rushing_yards_allowed', OPP_ROLLING_WINDOW)
    features['rolling_avg_sack_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'def_sacks', OPP_ROLLING_WINDOW)
    features['rolling_avg_interception_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'def_interceptions', OPP_ROLLING_WINDOW)
    features['rolling_avg_qb_hit_4_weeks'] = calculate_live_rolling_avg(opponent_defense_history, 'def_qb_hits', OPP_ROLLING_WINDOW)
    
    features['rolling_avg_points_allowed_to_QB'] = 0.0
    features['rolling_avg_points_allowed_to_RB'] = 0.0
    features['rolling_avg_points_allowed_to_WR'] = 0.0
    features['rolling_avg_points_allowed_to_TE'] = 0.0
        
    # --- D: One-Hot Position ---
    features['position_RB'] = 1 if player_position == 'RB' else 0
    features['position_TE'] = 1 if player_position == 'TE' else 0
    features['position_WR'] = 1 if player_position == 'WR' else 0
    
    # --- E: Identifiers (for server use) ---
    features['player_name'] = player_name
    features['position'] = player_position
    features['team'] = player_team
    features['opponent'] = opponent_team
    
    return features, None