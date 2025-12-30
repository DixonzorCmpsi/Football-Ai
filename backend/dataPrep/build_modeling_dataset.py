import pandas as pd
from pathlib import Path
import logging
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Local Fallback Paths
DATA_FOLDER = Path(__file__).parent / "data"
OUTPUT_CSV_PATH = Path("weekly_modeling_dataset.csv")

# Mapping: Config Key -> (DB Table Name, Local Filename)
DATA_SOURCES = {
    'player_weekly':       ('training_player_weekly', 'weekly_player_stats_offense.csv'),
    'team_offense_weekly': ('training_team_offense', 'weekly_team_stats_offense.csv'),
    'team_defense_weekly': ('training_team_defense', 'weekly_team_stats_defense.csv'),
}

ROLLING_WINDOW_SIZE = 4
PLAYER_ID_COLS = ['player_id', 'season', 'week']
TARGET_COL = 'fantasy_points_ppr'
RENAMED_TARGET_COL = f'y_{TARGET_COL}'
PLAYER_STATS_TO_ROLL = [TARGET_COL, 'targets', 'receptions', 'rushing_yards', 'receiving_yards']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_engine():
    if not DB_CONNECTION_STRING: return None
    return create_engine(DB_CONNECTION_STRING)

def standardize_columns(key, df):
    """
    Ensures columns loaded from DB match the names expected by the logic.
    """
    if key == 'team_defense_weekly':
        # DB uses 'def_sacks', Logic uses 'sack'
        rename_map = {
            'def_sacks': 'sack', 
            'def_interceptions': 'interception', 
            'def_qb_hits': 'qb_hit'
        }
        return df.rename(columns=rename_map)
    
    if key == 'team_offense_weekly':
        # DB uses 'points_scored', Logic uses 'total_off_points'
        rename_map = {'points_scored': 'total_off_points'}
        return df.rename(columns=rename_map)
        
    return df

def load_data_smart() -> dict[str, pd.DataFrame]:
    """Loads data with a 'DB First, Local Fallback' strategy."""
    logging.info("Loading data sources...")
    data = {}
    engine = get_db_engine()
    
    for key, (table_name, filename) in DATA_SOURCES.items():
        loaded = False
        
        # 1. Try Database
        if engine:
            try:
                logging.info(f"   Attempting to load '{key}' from DB table '{table_name}'...")
                # Check if table exists
                with engine.connect() as conn:
                    exists = conn.execute(text(f"SELECT to_regclass('public.{table_name}')")).scalar()
                    
                if exists:
                    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
                    # Standardize column names to match legacy logic
                    data[key] = standardize_columns(key, df)
                    logging.info(f"     ✅ Loaded {len(data[key])} rows from DB.")
                    loaded = True
                else:
                    logging.warning(f"     ⚠️ Table '{table_name}' not found in DB.")
            except Exception as e:
                logging.warning(f"     ⚠️ DB Error for '{key}': {e}")
        
        # 2. Fallback to Local
        if not loaded:
            local_path = DATA_FOLDER / filename
            logging.info(f"   🔄 Falling back to local file: {filename}")
            if local_path.exists():
                try:
                    data[key] = pd.read_csv(local_path, low_memory=False)
                    logging.info(f"     ✅ Loaded {len(data[key])} rows from local CSV.")
                    loaded = True
                except Exception as e:
                    logging.error(f"     ❌ Error reading local file: {e}")
            else:
                logging.error(f"     ❌ File not found locally: {local_path}")
                
        if not loaded:
            raise FileNotFoundError(f"Could not load '{key}' from DB or Local.")

    return data

def get_weekly_opponents(team_offense_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Identifying weekly opponents...")
    # Normalize 'team' vs 'team_abbr'
    if 'team_abbr' in team_offense_df.columns: 
        team_offense_df = team_offense_df.rename(columns={'team_abbr': 'team'})
        
    team_map = team_offense_df[['game_id', 'season', 'week', 'team']].copy()
    opponents_df = pd.merge(team_map, team_map.rename(columns={'team': 'opponent'}), on=['game_id', 'season', 'week'])
    opponents_df = opponents_df[opponents_df['team'] != opponents_df['opponent']].copy()
    return opponents_df

def engineer_rolling_defense_features(team_offense_df, team_defense_df, opponents_df, player_weekly_df):
    logging.info("Engineering rolling features for opponent defenses...")
    
    # Normalization
    if 'team_abbr' in team_offense_df.columns: team_offense_df = team_offense_df.rename(columns={'team_abbr': 'team'})
    if 'team_abbr' in team_defense_df.columns: team_defense_df = team_defense_df.rename(columns={'team_abbr': 'team'})
    
    # Part A: General Offensive Stats Allowed
    # Note: 'total_off_points' comes from our standardize_columns function
    off_stats_for_def = team_offense_df[['game_id', 'team', 'passing_yards', 'rushing_yards', 'total_off_points']].copy()
    def_merged_df = pd.merge(opponents_df, off_stats_for_def.rename(columns={'team': 'opponent'}), on=['game_id', 'opponent'])
    def_merged_df.rename(columns={'passing_yards': 'passing_yards_allowed', 'rushing_yards': 'rushing_yards_allowed', 'total_off_points': 'points_allowed'}, inplace=True)

    # Part B: Direct Defensive Stats
    direct_def_stats = team_defense_df[['season', 'week', 'team', 'sack', 'interception', 'qb_hit']].copy()
    def_merged_df = pd.merge(def_merged_df, direct_def_stats, on=['season', 'week', 'team'], how='left')

    # Part D: Calculate Rolling Averages
    def_merged_df.sort_values(by=['team', 'season', 'week'], inplace=True)
    stats_to_roll = ['passing_yards_allowed', 'rushing_yards_allowed', 'points_allowed', 'sack', 'interception', 'qb_hit']
    for stat in stats_to_roll:
        if stat in def_merged_df.columns:
            def_merged_df[stat] = def_merged_df[stat].fillna(0)
            def_merged_df[f'rolling_avg_{stat}_4_weeks'] = def_merged_df.groupby('team')[stat].shift(1).rolling(4, min_periods=1).mean()

    return def_merged_df

def engineer_rolling_player_features(player_weekly_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Engineering rolling features for players...")
    df = player_weekly_df.copy()
    df.sort_values(by=PLAYER_ID_COLS, inplace=True)
    
    for stat in PLAYER_STATS_TO_ROLL:
        if stat in df.columns:
            col_name = f'rolling_avg_{stat}_{ROLLING_WINDOW_SIZE}_weeks'
            df[col_name] = df.groupby('player_id')[stat].shift(1).rolling(window=ROLLING_WINDOW_SIZE, min_periods=1).mean()
    return df

def combine_datasets(dataframes: dict, opponents_df: pd.DataFrame, defense_features_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Merging master dataset...")
    master_df = dataframes['player_weekly']
    master_df = pd.merge(master_df, opponents_df[['season', 'week', 'team', 'opponent']], on=['season', 'week', 'team'], how='left')
    
    cols_to_merge = [col for col in defense_features_df.columns if 'rolling_avg' in col] + ['season', 'week', 'team']
    def_stats_to_merge = defense_features_df[cols_to_merge].drop_duplicates().copy()
    def_stats_to_merge.rename(columns={'team': 'opponent'}, inplace=True)
    master_df = pd.merge(master_df, def_stats_to_merge, on=['season', 'week', 'opponent'], how='left')
    
    return master_df

def finalize_dataset(master_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Finalizing dataset...")
    if TARGET_COL in master_df.columns:
        master_df.rename(columns={TARGET_COL: RENAMED_TARGET_COL}, inplace=True)
    
    # For training, drop rows without history. For inference, keep them.
    # We will keep them for now and let the model handle NaNs or fill 0.
    return master_df

def main():
    logging.info("--- Starting Master Dataset Construction (Hybrid) ---")
    
    all_data = load_data_smart()
    
    # Execute Logic
    opponents = get_weekly_opponents(all_data['team_offense_weekly'])
    rolling_def_feats = engineer_rolling_defense_features(all_data['team_offense_weekly'], all_data['team_defense_weekly'], opponents, all_data['player_weekly'])
    all_data['player_weekly'] = engineer_rolling_player_features(all_data['player_weekly'])
    
    master_dataset = combine_datasets(all_data, opponents, rolling_def_feats)
    final_dataset = finalize_dataset(master_dataset)

    # Save to Local CSV (ETL will handle upload)
    final_dataset.to_csv(OUTPUT_CSV_PATH, index=False)
    
    logging.info(f"--- Complete ---")
    logging.info(f"Successfully saved '{OUTPUT_CSV_PATH}' with {len(final_dataset)} rows.")

if __name__ == "__main__":
    main()