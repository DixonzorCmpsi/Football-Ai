import polars as pl
import numpy as np
from datetime import datetime

# --- Configuration ---
def get_current_season():
    now = datetime.now()
    if now.month >= 3: return now.year
    else: return now.year - 1
        
CURRENT_SEASON = get_current_season()
N_LAGS = 3 
OPP_ROLLING_WINDOW = 4 

# --- Helper Functions ---
def calculate_rolling_avg(df: pl.DataFrame, col: str, window: int) -> float:
    if df is None or df.is_empty() or col not in df.columns or window <= 0: return 0.0
    try:
        numeric_col = pl.col(col).cast(pl.Float64, strict=False)
        avg = df.head(window).select(
            numeric_col.fill_null(0.0).cast(pl.Float64).mean()
        ).item()
        return avg if avg is not None and not np.isnan(avg) else 0.0
    except Exception: return 0.0

def get_lagged_value(df: pl.DataFrame, col: str, lag: int) -> float:
    if df is None or df.is_empty() or col not in df.columns: return 0.0
    row_idx = lag - 1
    if row_idx < df.height:
        val = df[row_idx, col]
        return float(val) if val is not None and not np.isnan(val) else 0.0
    else: return 0.0 

def generate_features_all(
    player_id: str, 
    target_week: int,
    df_profile, df_schedule, df_player_stats, df_defense, df_offense, df_snap_counts,
    df_formation: pl.DataFrame = None 
):
    # --- 1. Get Player Info ---
    player_info = df_profile.filter(pl.col('player_id') == player_id)
    if player_info.is_empty(): return None, "Player not found."
    
    try:
        p_row = player_info.row(0, named=True)
        player_team = p_row['team_abbr']
        player_position = p_row['position']
        player_name = p_row['player_name']
        
        draft_year = p_row.get('draft_year')
        if draft_year and not np.isnan(draft_year):
             years_exp = CURRENT_SEASON - int(draft_year)
        else:
             years_exp = p_row.get('years_exp') if p_row.get('years_exp') is not None else 0
             
        player_age = p_row.get('age') if p_row.get('age') is not None else 25
        draft_ovr = p_row.get('draft_number') if p_row.get('draft_number') is not None else 260
        
    except Exception as e: return None, f"Player Info Error: {e}"

    # --- 2. Get Opponent Info ---
    game_info = df_schedule.filter(
        (pl.col('week') == target_week) & 
        ((pl.col('home_team') == player_team) | (pl.col('away_team') == player_team))
    )
    if game_info.is_empty(): return None, "Likely Bye Week"
    
    try:
        game_row = game_info.row(0, named=True)
        is_home = (game_row['home_team'] == player_team)
        opponent_team = game_row['away_team'] if is_home else game_row['home_team']
    except: return None, "Game Info Error"

    # --- 3. Prepare History ---
    stats_filter = (pl.col('player_id') == player_id) & (pl.col('week') < target_week)
    if 'season' in df_player_stats.columns:
        stats_filter &= (pl.col('season') == CURRENT_SEASON)
        
    player_history_stats = df_player_stats.filter(stats_filter).sort('week', descending=True)
    
    if 'receiving_yards_after_catch' not in player_history_stats.columns and 'receiving_yards' in player_history_stats.columns:
        player_history_stats = player_history_stats.with_columns(
            (pl.col("receiving_yards") * 0.3).alias("receiving_yards_after_catch")
        )

    cols_to_scale = ['team_targets_share', 'team_receptions_share', 'team_rush_attempts_share']
    for col in cols_to_scale:
        if col in player_history_stats.columns:
            player_history_stats = player_history_stats.with_columns(
                (pl.col(col) * 100).alias(col)
            )
    
    player_history = player_history_stats

    # Snap Counts
    if 'player_id' in df_snap_counts.columns:
        snap_filter = (pl.col('player_id') == player_id) & (pl.col('week') < target_week)
        if 'season' in df_snap_counts.columns:
            snap_filter &= (pl.col('season') == CURRENT_SEASON)
        snaps = df_snap_counts.filter(snap_filter)

        if not snaps.is_empty():
            try:
                snaps_prep = snaps.select(['week', 'offense_snaps', (pl.col('offense_pct') * 100).alias('offense_pct')])
                player_history = player_history.join(snaps_prep, on='week', how='left').fill_null(0.0)
            except Exception: pass 

    # --- Formation Stats ---
    if df_formation is not None and not df_formation.is_empty():
        try:
            df_formation_clean = df_formation.with_columns([
                pl.col("week").cast(pl.Int64, strict=False),
                pl.col("season").cast(pl.Int64, strict=False)
            ])
            fmt_stats = df_formation_clean.filter(
                (pl.col('player_id') == player_id) & 
                (pl.col('season') == CURRENT_SEASON) & 
                (pl.col('week') < target_week)
            )
            if not fmt_stats.is_empty():
                # Join to player_history
                player_history = player_history.join(
                    fmt_stats.select(['week', 'shotgun_pct', 'no_huddle_pct', 'qb_dropback_pct', 'qb_scramble_pct']),
                    on='week',
                    how='left'
                ).fill_null(0.0)
        except Exception: pass

    # --- Defense ---
    def_filter = (pl.col('team_abbr') == opponent_team) & (pl.col('week') < target_week)
    if 'season' in df_defense.columns:
        def_filter &= (pl.col('season') == CURRENT_SEASON)
    opponent_defense_history = df_defense.filter(def_filter).sort('week', descending=True)

    # --- 4. Features ---
    features = {}
    features['age'] = player_age
    features['years_exp'] = years_exp
    features['draft_ovr'] = draft_ovr
    
    # Formation Averages (Season Level)
    if not player_history.is_empty():
        features['shotgun'] = player_history['shotgun_pct'].mean() if 'shotgun_pct' in player_history.columns else 0.0
        features['no_huddle'] = player_history['no_huddle_pct'].mean() if 'no_huddle_pct' in player_history.columns else 0.0
        features['qb_dropback'] = player_history['qb_dropback_pct'].mean() if 'qb_dropback_pct' in player_history.columns else 0.0
        features['qb_scramble'] = player_history['qb_scramble_pct'].mean() if 'qb_scramble_pct' in player_history.columns else 0.0
    else:
        features['shotgun'] = 0.0
        features['no_huddle'] = 0.0
        features['qb_dropback'] = 0.0
        features['qb_scramble'] = 0.0

    col_mapping = {
        'y_fantasy_points_ppr': 'y_fantasy_points_ppr',
        'offense_pct': 'offense_pct',
        'offense_snaps': 'offense_snaps',
        'touches': 'touches',
        'targets': 'targets',
        'receptions': 'receptions',
        'receiving_yards': 'receiving_yards',
        'yards_after_catch': 'receiving_yards_after_catch', 
        'receiving_air_yards': 'receiving_air_yards',
        'receiving_touchdown': 'receiving_tds', 
        'rush_attempts': 'rush_attempts',
        'rushing_yards': 'rushing_yards',
        'rush_touchdown': 'rushing_tds', 
        'ypc': 'ypc',
        'pass_attempts': 'attempts', 
        'passing_air_yards': 'passing_air_yards',
        'interception': 'interception',
        'total_off_yards': 'total_off_yards',
        'team_targets_share': 'team_targets_share',
        'team_receptions_share': 'team_receptions_share',
        'team_rush_attempts_share': 'team_rush_attempts_share',
        'ypr': 'ypr'
    }
    
    for feat_name, db_col in col_mapping.items():
        for lag in range(1, N_LAGS + 1):
            features[f'{feat_name}_lag_{lag}'] = get_lagged_value(player_history, db_col, lag)

    def_cols = ['points_allowed', 'passing_yards_allowed', 'rushing_yards_allowed', 'def_sacks', 'def_interceptions', 'def_qb_hits']
    for col in def_cols:
        features[f'rolling_avg_{col}_4_weeks'] = calculate_rolling_avg(opponent_defense_history, col, OPP_ROLLING_WINDOW)
    
    for pos_code in ['QB', 'RB', 'WR', 'TE']:
        key = f'rolling_avg_points_allowed_to_{pos_code}'
        features[key] = 0.0 
        try:
            def_vs_pos = df_player_stats.filter(
                (pl.col('opponent_team') == opponent_team) & 
                (pl.col('position') == pos_code) &
                (pl.col('week') < target_week)
            )
            if 'season' in df_player_stats.columns:
                def_vs_pos = def_vs_pos.filter(pl.col('season') == CURRENT_SEASON)
            if not def_vs_pos.is_empty():
                weekly_points = def_vs_pos.group_by('week').agg(
                    pl.col('y_fantasy_points_ppr').sum().alias('points_allowed')
                ).sort('week', descending=True)
                features[key] = calculate_rolling_avg(weekly_points, 'points_allowed', OPP_ROLLING_WINDOW)
        except Exception: pass 
        
    features['position_RB'] = 1 if player_position == 'RB' else 0
    features['position_TE'] = 1 if player_position == 'TE' else 0
    features['position_WR'] = 1 if player_position == 'WR' else 0
    features['player_name'] = player_name
    features['position'] = player_position
    features['team'] = player_team
    features['opponent'] = opponent_team
    
    return features, None