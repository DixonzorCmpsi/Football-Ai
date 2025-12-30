import pandas as pd
import numpy as np
import joblib
import json
from pathlib import Path
from sklearn.metrics import mean_absolute_error, r2_score

# --- Configuration ---
TEST_WEEK = 17 
SEASON = 2025
RAG_DATA_DIR = Path("../rag_data")
MODEL_DIR = Path("./models")

FILES = {
    'player': RAG_DATA_DIR / f"weekly_player_stats_{SEASON}.csv",
    'defense': RAG_DATA_DIR / f"weekly_defense_stats_{SEASON}.csv",
    'offense': RAG_DATA_DIR / f"weekly_offense_stats_{SEASON}.csv",
    'snaps': RAG_DATA_DIR / f"weekly_snap_counts_{SEASON}.csv",
    'profiles': RAG_DATA_DIR / f"player_profiles_{SEASON}.csv"
}
if not FILES['profiles'].exists(): FILES['profiles'] = RAG_DATA_DIR / "player_profiles.csv"

POS_CONFIG = {
    'QB': {'model': "xgboost_QB_sliding_window_deviation_v1.joblib", 'feats': "feature_names_QB_sliding_window_deviation_v1.json"},
    'RB': {'model': "xgboost_RB_sliding_window_deviation_v1.joblib", 'feats': "feature_names_RB_sliding_window_deviation_v1.json"},
    'WR': {'model': "xgboost_WR_sliding_window_deviation_v1.joblib", 'feats': "feature_names_WR_sliding_window_deviation_v1.json"},
    'TE': {'model': "xgboost_TE_sliding_window_deviation_v1.joblib", 'feats': "feature_names_TE_sliding_window_deviation_v1.json"}
}

def load_and_prep_2025_data():
    print("--- Loading Raw 2025 Data from rag_data ---")
    
    # 1. Load Player Stats
    try:
        df_player = pd.read_csv(FILES['player'])
        print(f"Loaded {len(df_player)} player stats.")
    except FileNotFoundError:
        print(f"CRITICAL ERROR: {FILES['player']} not found.")
        return pd.DataFrame()

    # --- NORMALIZE COLUMN NAMES ---
    # The model expects specific names that might differ from raw data
    name_map = {
        'receiving_tds': 'receiving_touchdown',
        'passing_tds': 'passing_touchdown',
        'rushing_tds': 'rush_touchdown',
        'receiving_yards_after_catch': 'yards_after_catch',
        'attempts': 'pass_attempts'  # <--- CRITICAL FIX: QB Model needs this!
    }
    df_player.rename(columns=name_map, inplace=True)
    
    # 2. Snaps
    if FILES['snaps'].exists():
        df_snaps = pd.read_csv(FILES['snaps'])
        df_player = pd.merge(df_player, df_snaps[['player_id', 'week', 'offense_snaps', 'offense_pct']], 
                             on=['player_id', 'week'], how='left')
        df_player[['offense_snaps', 'offense_pct']] = df_player[['offense_snaps', 'offense_pct']].fillna(0)
    else:
        df_player['offense_snaps'] = 0; df_player['offense_pct'] = 0

    # 3. Profiles
    if FILES['profiles'].exists():
        df_prof = pd.read_csv(FILES['profiles'])
        if 'draft_number' in df_prof.columns: df_prof.rename(columns={'draft_number': 'draft_ovr'}, inplace=True)
        if 'display_name' in df_prof.columns and 'player_name' not in df_prof.columns:
             df_prof.rename(columns={'display_name': 'player_name'}, inplace=True)
        
        if 'years_exp' not in df_prof.columns and 'draft_year' in df_prof.columns:
            df_prof['years_exp'] = (SEASON - df_prof['draft_year']).clip(lower=0)

        cols = [c for c in ['player_id', 'player_name', 'age', 'years_exp', 'draft_ovr'] if c in df_prof.columns]
        df_player = pd.merge(df_player, df_prof[cols], on='player_id', how='left')
        for c in ['age', 'years_exp', 'draft_ovr']: 
            if c in df_player.columns: df_player[c] = df_player[c].fillna(0)
    
    if 'player_name' not in df_player.columns: df_player['player_name'] = df_player['player_id']

    # 4. Defense vs. Position
    print("Engineering Defense vs. Position stats...")
    dvp = df_player.groupby(['opponent_team', 'week', 'position'])['y_fantasy_points_ppr'].sum().reset_index()
    dvp.sort_values(['opponent_team', 'position', 'week'], inplace=True)
    dvp['rolling_avg_points_allowed_to_pos'] = dvp.groupby(['opponent_team', 'position'])['y_fantasy_points_ppr']\
        .transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean()).fillna(0)
    
    dvp_wide = dvp.pivot_table(index=['opponent_team', 'week'], columns='position', values='rolling_avg_points_allowed_to_pos').reset_index()
    dvp_wide.columns = [f"rolling_avg_points_allowed_to_{c}" if c in ['QB', 'RB', 'WR', 'TE'] else c for c in dvp_wide.columns]
    
    df_player = pd.merge(df_player, dvp_wide, on=['opponent_team', 'week'], how='left')
    for pos in ['QB', 'RB', 'WR', 'TE']:
        col = f"rolling_avg_points_allowed_to_{pos}"
        if col in df_player.columns: df_player[col] = df_player[col].fillna(0)

    # 5. Defense Features
    print("Engineering Opponent Defense features...")
    df_def = pd.read_csv(FILES['defense'])
    df_def.sort_values(['team_abbr', 'week'], inplace=True)
    if 'opponent_team' in df_def.columns: df_def.drop(columns=['opponent_team'], inplace=True)

    metrics = ['points_allowed', 'passing_yards_allowed', 'rushing_yards_allowed', 'def_sacks', 'def_interceptions', 'def_qb_hits']
    for col in metrics:
        if col in df_def.columns:
            df_def[f'rolling_avg_{col}_4_weeks'] = df_def.groupby('team_abbr')[col].shift(1).rolling(4, min_periods=1).mean()
            for lag in [1, 2, 3]:
                df_def[f'opp_def_{col}_lag_{lag}'] = df_def.groupby('team_abbr')[col].shift(lag)

    df_def_merge = df_def.rename(columns={'team_abbr': 'opponent_team'})
    df_player = pd.merge(df_player, df_def_merge, on=['opponent_team', 'week'], how='left', suffixes=('', '_def'))

    # 6. Offense Features
    print("Engineering Opponent Offense features...")
    df_off = pd.read_csv(FILES['offense'])
    df_off.sort_values(['team_abbr', 'week'], inplace=True)
    if 'points_scored' in df_off.columns: df_off.rename(columns={'points_scored': 'total_off_points'}, inplace=True)
    if 'opponent_team' in df_off.columns: df_off.drop(columns=['opponent_team'], inplace=True)

    for col in ['total_off_points', 'total_yards', 'passing_yards', 'rushing_yards']:
        if col in df_off.columns:
            df_off[f'opp_off_rolling_{col}_4_weeks'] = df_off.groupby('team_abbr')[col].shift(1).rolling(4, min_periods=1).mean()
            for lag in [1, 2, 3]:
                df_off[f'opp_off_{col}_lag_{lag}'] = df_off.groupby('team_abbr')[col].shift(lag)
    
    df_off_merge = df_off.rename(columns={'team_abbr': 'opponent_team'})
    df_player = pd.merge(df_player, df_off_merge, on=['opponent_team', 'week'], how='left', suffixes=('', '_off'))

    # 7. Player Lags & Baseline
    print("Engineering Player Lags & Baseline...")
    df_player.sort_values(['player_id', 'week'], inplace=True)
    
    df_player['player_season_avg_points'] = df_player.groupby('player_id')['y_fantasy_points_ppr']\
        .transform(lambda x: x.expanding().mean().shift(1)).fillna(0)

    # --- DERIVED STATS (Calculate Missing Features) ---
    # 1. Share Stats
    if 'team_receptions_share' not in df_player.columns and 'team_receptions' in df_player.columns:
         df_player['team_receptions_share'] = (df_player['receptions'] / df_player['team_receptions']).fillna(0)
    
    if 'team_targets_share' not in df_player.columns and 'team_pass_attempts' in df_player.columns:
         df_player['team_targets_share'] = (df_player['targets'] / df_player['team_pass_attempts']).fillna(0)

    if 'team_rush_attempts_share' not in df_player.columns and 'team_rush_attempts' in df_player.columns:
         df_player['team_rush_attempts_share'] = (df_player['rush_attempts'] / df_player['team_rush_attempts']).fillna(0)

    # 2. Per Target/Touch Stats
    df_player['ayptarget'] = (df_player['receiving_air_yards'] / df_player['targets']).fillna(0)
    df_player['ypr'] = (df_player['receiving_yards'] / df_player['receptions']).fillna(0)
    df_player['ypc'] = (df_player['rushing_yards'] / df_player['rush_attempts']).fillna(0)
    
    # 3. Touches (Rush + Rec)
    if 'touches' not in df_player.columns:
        df_player['touches'] = (df_player['rush_attempts'] + df_player['receptions']).fillna(0)

    # 4. ADOT (Average Depth of Target)
    # Formula: Air Yards / Targets
    if 'adot' not in df_player.columns:
        df_player['adot'] = (df_player['receiving_air_yards'] / df_player['targets']).fillna(0)

    # 5. Passer Rating (Approximation if missing)
    if 'passer_rating' not in df_player.columns:
        df_player['passer_rating'] = 0.0 # Placeholder if not in raw data

    # 6. Redzone (Proxy if not in data)
    if 'receptions_redzone' not in df_player.columns: df_player['receptions_redzone'] = 0.0
    if 'targets_redzone' not in df_player.columns: df_player['targets_redzone'] = 0.0
    if 'rush_touchdown_redzone' not in df_player.columns: df_player['rush_touchdown_redzone'] = 0.0

    # --- LAG CALCULATION ---
    # We must lag ALL features that might be used
    lags = [
        # Base Stats
        'offense_snaps', 'offense_pct', 'targets', 'receptions', 'receiving_yards', 
        'rushing_yards', 'rush_attempts', 'y_fantasy_points_ppr', 
        'pass_attempts', 'passing_yards', 'passing_touchdown', 'interception', # 'pass_attempts' is now available!
        'receiving_touchdown', 'rush_touchdown',
        
        # Advanced/Derived
        'team_targets_share', 'team_receptions_share', 'team_rush_attempts_share',
        'receiving_air_yards', 'passing_air_yards', 'yards_after_catch', 
        'ypr', 'ayptarget', 'ypc', 'adot', 'touches', 'passer_rating',
        'receptions_redzone', 'targets_redzone'
    ]
    
    for col in lags:
        if col in df_player.columns:
            for lag in [1, 2, 3]:
                df_player[f'{col}_lag_{lag}'] = df_player.groupby('player_id')[col].shift(lag).fillna(0)

    # 8. Rename Columns (Defense-specific)
    rename_map = {
        'rolling_avg_def_sacks_4_weeks': 'rolling_avg_sack_4_weeks',
        'rolling_avg_def_interceptions_4_weeks': 'rolling_avg_interception_4_weeks',
        'rolling_avg_def_qb_hits_4_weeks': 'rolling_avg_qb_hit_4_weeks',
        'opp_def_def_sacks_lag_1': 'opp_def_sack_lag_1',
    }
    df_player.rename(columns=rename_map, inplace=True)
    df_player['opponent'] = df_player['opponent_team']

    # --- PROJECT VOLUME ---
    current_cols = ['offense_snaps', 'offense_pct', 'targets', 'rush_attempts', 'pass_attempts']
    for col in current_cols:
        if col in df_player.columns and f'{col}_lag_1' in df_player.columns:
            mask = (df_player['week'] == TEST_WEEK) & (df_player[col] == 0)
            df_player.loc[mask, col] = df_player.loc[mask, f'{col}_lag_1']

    # Filter for Test Week
    df_test = df_player[df_player['week'] == TEST_WEEK].copy()
    print(f"✅ Prepared {len(df_test)} rows for Week {TEST_WEEK}.")
    return df_test

def evaluate_position(df_all, pos):
    print(f"\n🏈 Evaluating {pos}...")
    config = POS_CONFIG[pos]
    df_pos = df_all[df_all['position'] == pos].copy()
    if df_pos.empty: print(f"No {pos} found."); return

    try:
        model = joblib.load(MODEL_DIR / config['model'])
        with open(MODEL_DIR / config['feats'], 'r') as f:
            feature_names = json.load(f)
    except Exception as e:
        print(f"❌ Model load error: {e}"); return

    # Check for missing
    missing = [f for f in feature_names if f not in df_pos.columns]
    if missing:
        print(f"⚠️ Warning: {len(missing)} features still missing: {missing[:3]}...")
        for f in missing: df_pos[f] = 0.0
    
    X = df_pos[feature_names]
    pred_dev = model.predict(X)
    
    baseline = df_pos['player_season_avg_points'].values
    actual = df_pos['y_fantasy_points_ppr'].values
    final_pred = np.maximum(baseline + pred_dev, 0)
    
    # Metrics
    if actual.sum() > 0:
        mae = mean_absolute_error(actual, final_pred)
        r2 = r2_score(actual, final_pred)
        print(f"📊 Week {TEST_WEEK} Stats:")
        print(f"✅ MAE: {mae:.4f}")
        print(f"✅ R²:  {r2:.4f}")
    
    df_pos['Predicted'] = final_pred
    df_pos['Error'] = (df_pos['Predicted'] - df_pos['y_fantasy_points_ppr']).abs()
    
    cols = ['player_name', 'team', 'opponent', 'y_fantasy_points_ppr', 'Predicted', 'Error']
    valid_cols = [c for c in cols if c in df_pos.columns]
    
    print(f"\nTop 5 Accurate Predictions:")
    print(df_pos[valid_cols].sort_values('Error').head(5).to_string(index=False, float_format="%.1f"))

    print(f"\nTop 5 Misses:")
    print(df_pos[valid_cols].sort_values('Error', ascending=False).head(5).to_string(index=False, float_format="%.1f"))

def main():
    print(f"🚀 RUNNING LIVE INFERENCE FOR WEEK {TEST_WEEK}, {SEASON}")
    df_2025 = load_and_prep_2025_data()
    if not df_2025.empty:
        for pos in ['QB', 'RB', 'WR', 'TE']:
            evaluate_position(df_2025, pos)

if __name__ == "__main__":
    main()