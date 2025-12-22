# dataPrep/feature_engineering.py (v7 - Final + Game Script)
import pandas as pd

def engineer_features(input_path, output_path):
    """
    Loads the dataset, cleans it, and engineers new features for modeling.
    Includes 'Game Script' features (Opponent Offense).
    """
    print(f"--- Starting Final Feature Engineering on {input_path} ---")

    # Use low_memory=False to help with mixed data types in large files
    try:
        df = pd.read_csv(input_path, low_memory=False)
    except FileNotFoundError:
        print(f"❌ Error: Input file '{input_path}' not found.")
        return

    print(f"Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns.")

    # --- 1. Data Cleaning: Filter for relevant fantasy positions ---
    fantasy_positions = ['QB', 'RB', 'WR', 'TE']
    if 'position' in df.columns:
        df_clean = df[df['position'].isin(fantasy_positions)].copy()
        print(f"Filtered down to {df_clean.shape[0]} rows for relevant fantasy positions.")
    else:
        df_clean = df.copy()
        print("Warning: 'position' column missing, skipping position filter.")

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
        'ayptarget', 
        'yptouch',   
        'passer_rating', 'ypc', 'ypr', 'interception',
        'career_average_ppr_ppg',

        # --- Pillar 3: High-Value Touches ---
        'rush_attempts_redzone', 'targets_redzone', 'receptions_redzone',
        'pass_attempts_redzone', 'receiving_touchdown', 'rush_touchdown',

        # --- Pillar 4: Offensive Scheme & Potency ---
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 
        'pass_pct', 'total_off_yards',

        # --- Pillar 5: Matchup (Defense) ---
        'rolling_avg_points_allowed_4_weeks',
        'rolling_avg_passing_yards_allowed_4_weeks',
        'rolling_avg_rushing_yards_allowed_4_weeks',
        'rolling_avg_sack_4_weeks',
        'rolling_avg_interception_4_weeks',
        'rolling_avg_qb_hit_4_weeks',
        'rolling_avg_points_allowed_to_QB',
        'rolling_avg_points_allowed_to_RB',
        'rolling_avg_points_allowed_to_WR',
        'rolling_avg_points_allowed_to_TE',

        # --- Pillar 6: Game Script (Opponent Offense) ---
        # (NEW) These help predict if we will be trailing (passing) or leading (running)
        'opp_offense_avg_points_scored_4_weeks',
        'opp_offense_avg_total_yards_4_weeks',
        'opp_offense_avg_pass_yards_4_weeks',
        'opp_offense_avg_rush_yards_4_weeks'
    ]

    # Only keep features that actually exist in the input (avoids KeyErrors)
    existing_features = [col for col in features_to_keep if col in df_clean.columns]
    
    # Check if we missed any critical ones
    missing = set(features_to_keep) - set(existing_features)
    if missing:
        print(f"⚠️ Warning: {len(missing)} requested features were not found in input (filled with 0 later).")
        # print(f"   Missing: {missing}") 

    # Select columns + Target
    cols_to_use = existing_features
    if TARGET in df_clean.columns:
        cols_to_use = list(set(existing_features + [TARGET]))
    
    df_featured = df_clean[cols_to_use].copy()
    print(f"Selected {len(existing_features)} high-impact features for the model.")

    # Fill missing numerical values with 0
    # Identifying numeric columns
    numeric_cols = df_featured.select_dtypes(include=['float64', 'int64']).columns
    df_featured[numeric_cols] = df_featured[numeric_cols].fillna(0)
    print("Handled missing numerical values by filling with 0.")

    # --- 3. Advanced Feature Engineering: Player Momentum ---
    print("Engineering 3-game rolling averages for key performance stats...")
    df_featured.sort_values(by=['player_id', 'season', 'week'], inplace=True)

    stats_to_roll = [
        'offense_pct', 'targets', 'receptions', 'rush_attempts',
        'receiving_yards', 'rushing_yards'
    ]
    if TARGET in df_featured.columns:
        stats_to_roll.append(TARGET)

    for stat in stats_to_roll:
        if stat in df_featured.columns:
            df_featured[f'3_game_avg_{stat}'] = df_featured.groupby('player_id')[stat].shift(1).rolling(window=3, min_periods=1).mean()

    # Drop rows where target is missing (if training)
    if TARGET in df_featured.columns:
        original_len = len(df_featured)
        df_featured.dropna(subset=[TARGET], inplace=True)
        # Also drop rows where we couldn't calculate a lag (Week 1 of a career) if desired, 
        # or fill them. For now, we keep them but filled with NaN/0.
        # Actually, let's fill the rolling NaNs with 0 to be safe for XGBoost
        avg_cols = [f'3_game_avg_{stat}' for stat in stats_to_roll if f'3_game_avg_{stat}' in df_featured.columns]
        df_featured[avg_cols] = df_featured[avg_cols].fillna(0)
        
    print(f"Final dataset has {df_featured.shape[0]} rows after feature engineering.")

    # --- 4. Save the new dataset ---
    df_featured.to_csv(output_path, index=False)
    print(f"\n✅ Successfully created the final feature-engineered dataset at '{output_path}'")

if __name__ == "__main__":
    # We use the assembled dataset as our input
    input_file = 'weekly_modeling_dataset.csv'
    output_file = 'featured_dataset.csv'
    engineer_features(input_file, output_file)