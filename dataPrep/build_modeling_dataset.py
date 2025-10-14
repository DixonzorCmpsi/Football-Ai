# build_modeling_dataset.py (v2 - Keep All Features)
import pandas as pd
from pathlib import Path
import logging

# --- 1. CONFIGURATION ---
DATA_FOLDER = Path("data")
OUTPUT_PATH = Path("weekly_modeling_dataset.csv")

PATHS = {
    'player_yearly': DATA_FOLDER / "yearly_player_stats_offense.csv",
    'player_weekly': DATA_FOLDER / "weekly_player_stats_offense.csv",
    'team_offense_weekly': DATA_FOLDER / "weekly_team_stats_offense.csv",
    'team_defense_weekly': DATA_FOLDER / "weekly_team_stats_defense.csv",
}

ROLLING_WINDOW_SIZE = 4
PLAYER_ID_COLS = ['player_id', 'season', 'week']
TARGET_COL = 'fantasy_points_ppr'
RENAMED_TARGET_COL = f'y_{TARGET_COL}'
PLAYER_STATS_TO_ROLL = [TARGET_COL, 'targets', 'receptions', 'rushing_yards', 'receiving_yards']
DEF_STATS_TO_ROLL = ['passing_yards_allowed', 'rushing_yards_allowed', 'points_allowed']
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- HELPER FUNCTIONS ---

def load_data(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """Loads all required CSV files into a dictionary of DataFrames."""
    logging.info("Loading all data sources...")
    try:
        dataframes = {name: pd.read_csv(path) for name, path in paths.items()}
        logging.info("All files loaded successfully.")
        return dataframes
    except FileNotFoundError as e:
        logging.error(f"Could not find the file '{e.filename}'. Check paths in CONFIG.")
        raise

def get_weekly_opponents(team_offense_df: pd.DataFrame) -> pd.DataFrame:
    """Identifies the opponent for each team in each game."""
    logging.info("Identifying weekly opponents...")
    team_map = team_offense_df[['game_id', 'season', 'week', 'team']].copy()
    
    opponents_df = pd.merge(
        team_map,
        team_map.rename(columns={'team': 'opponent'}),
        on=['game_id', 'season', 'week']
    )
    opponents_df = opponents_df[opponents_df['team'] != opponents_df['opponent']].copy()
    
    assert 'opponent' in opponents_df.columns, "Failed to create opponent column."
    return opponents_df

def engineer_rolling_defense_features(team_offense_df: pd.DataFrame, opponents_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates rolling average of points and yards allowed by a team's defense."""
    logging.info("Engineering rolling features for opponent defenses...")
    
    off_stats_for_def = team_offense_df[['game_id', 'team', 'passing_yards', 'rushing_yards', 'total_off_points']].copy()
    
    def_merged_df = pd.merge(
        opponents_df,
        off_stats_for_def.rename(columns={
            'team': 'opponent',
            'passing_yards': 'passing_yards_allowed',
            'rushing_yards': 'rushing_yards_allowed',
            'total_off_points': 'points_allowed'
        }),
        on=['game_id', 'opponent']
    )
    
    def_merged_df.sort_values(by=['team', 'season', 'week'], inplace=True)
    
    for stat in DEF_STATS_TO_ROLL:
        col_name = f'rolling_avg_{stat}_{ROLLING_WINDOW_SIZE}_weeks'
        def_merged_df[col_name] = def_merged_df.groupby('team')[stat].shift(1).rolling(
            window=ROLLING_WINDOW_SIZE, min_periods=1
        ).mean()
        
    final_cols = ['season', 'week', 'team'] + [f'rolling_avg_{s}_{ROLLING_WINDOW_SIZE}_weeks' for s in DEF_STATS_TO_ROLL]
    return def_merged_df[final_cols]

def engineer_rolling_player_features(player_weekly_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates rolling average of key offensive stats for each player."""
    logging.info("Engineering rolling features for players...")
    df = player_weekly_df.copy()
    df.sort_values(by=PLAYER_ID_COLS, inplace=True)
    
    for stat in PLAYER_STATS_TO_ROLL:
        col_name = f'rolling_avg_{stat}_{ROLLING_WINDOW_SIZE}_weeks'
        df[col_name] = df.groupby('player_id')[stat].shift(1).rolling(
            window=ROLLING_WINDOW_SIZE, min_periods=1
        ).mean()
        
    return df

def combine_datasets(dataframes: dict, opponents_df: pd.DataFrame, defense_features_df: pd.DataFrame) -> pd.DataFrame:
    """Merges all player, team, and opponent data into a single master DataFrame."""
    logging.info("Merging all data into the master modeling dataset...")
    
    master_df = dataframes['player_weekly']
    
    master_df = pd.merge(master_df, opponents_df, on=['season', 'week', 'team'], how='left')
    
    master_df = pd.merge(
        master_df,
        defense_features_df.rename(columns={'team': 'opponent'}),
        on=['season', 'week', 'opponent'],
        how='left'
    )
    
    return master_df

def finalize_dataset(master_df: pd.DataFrame) -> pd.DataFrame:
    """Renames target, keeps all original features, and drops rows with missing rolling data."""
    logging.info("Finalizing dataset: renaming target and cleaning rows...")
    
    master_df.rename(columns={TARGET_COL: RENAMED_TARGET_COL}, inplace=True)
    
    essential_rolling_features = [
        f'rolling_avg_{s}_{ROLLING_WINDOW_SIZE}_weeks' for s in PLAYER_STATS_TO_ROLL
    ] + [
        f'rolling_avg_{s}_{ROLLING_WINDOW_SIZE}_weeks' for s in DEF_STATS_TO_ROLL
    ]
    
    final_df = master_df.dropna(subset=essential_rolling_features)
    
    return final_df


def main():
    """Main function to execute the data preparation pipeline."""
    logging.info("--- Starting Master Dataset Construction ---")
    
    all_data = load_data(PATHS)
    opponents = get_weekly_opponents(all_data['team_offense_weekly'])
    rolling_def_feats = engineer_rolling_defense_features(all_data['team_offense_weekly'], opponents)
    all_data['player_weekly'] = engineer_rolling_player_features(all_data['player_weekly'])
    master_dataset = combine_datasets(all_data, opponents, rolling_def_feats)
    final_dataset = finalize_dataset(master_dataset)

    final_dataset.to_csv(OUTPUT_PATH, index=False)
    logging.info(f"--- Master Dataset Construction Complete ---")
    logging.info(f"Successfully saved '{OUTPUT_PATH}' with {len(final_dataset)} rows and {len(final_dataset.columns)} columns.")


if __name__ == "__main__":
    main()