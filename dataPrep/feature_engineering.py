# feature_engineering.py (v6 - Definitive Final Feature Set)
import pandas as pd

def engineer_features(input_path, output_path):
    """
    Loads the dataset, cleans it, and engineers new features for modeling using the final, definitive, professionally curated feature set.
    """
    print(f"--- Starting Final Feature Engineering on {input_path} ---")

    # Use low_memory=False to help with mixed data types in large files
    df = pd.read_csv(input_path, low_memory=False)
    print(f"Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns.")

    # --- 1. Data Cleaning: Filter for relevant fantasy positions ---
    fantasy_positions = ['QB', 'RB', 'WR', 'TE']
    df_clean = df[df['position'].isin(fantasy_positions)].copy()
    print(f"Filtered down to {df_clean.shape[0]} rows for relevant fantasy positions.")

    # --- 2. Feature Selection: The Definitive Dossier ---
    TARGET = 'y_fantasy_points_ppr'

    # This is our final, expanded list of the most impactful features.
    features_to_keep = [
        # Identifiers & Context
        'season', 'week', 'player_id', 'player_name', 'position', 'age', 'years_exp', 'team', 'opponent',
        'draft_ovr',

        # --- Pillar 1: Opportunity & Usage ---
        'offense_snaps', 'offense_pct',
        'touches', 'targets', 'receptions', 'rush_attempts', 'pass_attempts',
        'team_targets_share', 'team_receptions_share', 'team_rush_attempts_share',
        'season_average_targets', 'season_average_touches',

        # --- Pillar 2: Efficiency & Talent ---
        'receiving_yards', 'rushing_yards', 'yards_after_catch',
        'passing_air_yards', 'receiving_air_yards', 'adot',
        'ayptarget', # ADDED
        'yptouch',   # ADDED
        'passer_rating', 'ypc', 'ypr', 'interception',
        'career_average_ppr_ppg',

        # --- Pillar 3: High-Value Touches ---
        'rush_attempts_redzone', 'targets_redzone', 'receptions_redzone',
        'pass_attempts_redzone', 'receiving_touchdown', 'rush_touchdown',

        # --- Pillar 4: Offensive Scheme & Potency ---
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', # ADDED
        'pass_pct', 'total_off_yards',

        # Engineered Opponent Matchup Features
        'rolling_avg_points_allowed_4_weeks',
        'rolling_avg_passing_yards_allowed_4_weeks',
        'rolling_avg_rushing_yards_allowed_4_weeks',
        'rolling_avg_sack_4_weeks',
        'rolling_avg_interception_4_weeks',
        'rolling_avg_qb_hit_4_weeks',
        'rolling_avg_points_allowed_to_QB',
        'rolling_avg_points_allowed_to_RB',
        'rolling_avg_points_allowed_to_WR',
        'rolling_avg_points_allowed_to_TE'
    ]

    existing_features = [col for col in features_to_keep if col in df_clean.columns]

    df_featured = df_clean[existing_features + [TARGET]].copy()
    print(f"Selected {len(existing_features)} high-impact features for the model.")

    # Fill missing numerical values with 0.
    for col in df_featured.columns:
        if df_featured[col].dtype in ['float64', 'int64']:
            # Use a more robust assignment method to avoid warnings
            df_featured.loc[:, col] = df_featured.loc[:, col].fillna(0)
    print("Handled missing numerical values by filling with 0.")

    # --- 3. Advanced Feature Engineering: Player Momentum ---
    print("Engineering 3-game rolling averages for key performance stats...")
    df_featured.sort_values(by=['player_id', 'season', 'week'], inplace=True)

    stats_to_roll = [
        'offense_pct', 'targets', 'receptions', 'rush_attempts',
        'receiving_yards', 'rushing_yards', TARGET
    ]

    for stat in stats_to_roll:
        df_featured[f'3_game_avg_{stat}'] = df_featured.groupby('player_id')[stat].shift(1).rolling(window=3, min_periods=1).mean()

    df_featured.dropna(subset=[f'3_game_avg_{TARGET}'], inplace=True)
    print(f"Final dataset has {df_featured.shape[0]} rows after feature engineering.")

    # --- 4. Save the new dataset ---
    df_featured.to_csv(output_path, index=False)
    print(f"\n✅ Successfully created the final feature-engineered dataset at '{output_path}'")

if __name__ == "__main__":
    # We use the assembled dataset as our input
    input_file = 'weekly_modeling_dataset.csv'
    output_file = 'featured_dataset.csv'
    engineer_features(input_file, output_file)