# dataPrep/feature_engineering.py (v9 - Feature Parity with Live Inference)
import pandas as pd
import numpy as np

def engineer_features(input_path, output_path):
    """
    Loads the dataset, cleans it, and engineers new features for modeling.
    Includes 'Game Script' features (Opponent Offense) and 'Player Baseline' (Deviation).
    """
    print(f"--- Starting Final Feature Engineering on {input_path} ---")

    # Use low_memory=False to help with mixed data types in large files
    try:
        df = pd.read_csv(input_path, low_memory=False)
    except FileNotFoundError:
        print(f"❌ Error: Input file '{input_path}' not found.")
        return

    print(f"Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns.")

    # --- 1. FEATURE PARITY CHECK (Calc Missing derived stats) ---
    # This ensures training data has the same calculated fields as live inference
    print("Ensuring Feature Parity (Calculating ADOT, Rating if missing)...")
    
    # Fill NAs for base columns used in calculation
    base_cols = ['receiving_air_yards', 'targets', 'receptions', 'receiving_yards', 'rushing_yards', 'rush_attempts', 
                 'completions', 'attempts', 'passing_yards', 'passing_tds', 'passing_interceptions']
    for c in base_cols:
        if c in df.columns: df[c] = df[c].fillna(0)

    # ADOT
    if 'adot' not in df.columns and 'receiving_air_yards' in df.columns:
        df['adot'] = (df['receiving_air_yards'] / df['targets'].replace(0, np.nan)).fillna(0)

    # YPR / YPC
    if 'ypr' not in df.columns:
        df['ypr'] = (df['receiving_yards'] / df['receptions'].replace(0, np.nan)).fillna(0)
    if 'ypc' not in df.columns:
        df['ypc'] = (df['rushing_yards'] / df['rush_attempts'].replace(0, np.nan)).fillna(0)

    # Touches
    if 'touches' not in df.columns:
        df['touches'] = df['rush_attempts'] + df['receptions']

    # Passer Rating (Simple)
    if 'passer_rating' not in df.columns and 'attempts' in df.columns:
        # Simplified logic or placeholder (XGBoost learns non-linear relationships anyway)
        # If we really want the formula, we can add it, but raw stats usually suffice for training if consistent
        # Let's add the formula to match 02_update_weekly_stats
        comp_pct = df['completions'] / df['attempts'].replace(0, np.nan)
        ypa = df['passing_yards'] / df['attempts'].replace(0, np.nan)
        td_pct = df['passing_tds'] / df['attempts'].replace(0, np.nan)
        int_pct = df['passing_interceptions'] / df['attempts'].replace(0, np.nan)

        pr_a = ((comp_pct - 0.3) * 5).clip(0, 2.375)
        pr_b = ((ypa - 3) * 0.25).clip(0, 2.375)
        pr_c = (td_pct * 20).clip(0, 2.375)
        pr_d = (2.375 - (int_pct * 25)).clip(0, 2.375)
        
        df['passer_rating'] = ((pr_a + pr_b + pr_c + pr_d) / 6 * 100).fillna(0)

    # Rename columns if needed (e.g. rushing_tds -> rush_touchdown)
    # This aligns historic naming with new naming
    rename_map = {
        'rushing_tds': 'rush_touchdown',
        'receiving_tds': 'receiving_touchdown',
        'passing_tds': 'passing_touchdown'
    }
    df.rename(columns=rename_map, inplace=True)

    # --- 2. Data Cleaning: Filter for relevant fantasy positions ---
    fantasy_positions = ['QB', 'RB', 'WR', 'TE']
    if 'position' in df.columns:
        df_clean = df[df['position'].isin(fantasy_positions)].copy()
        print(f"Filtered down to {df_clean.shape[0]} rows for relevant fantasy positions.")
    else:
        df_clean = df.copy()
        print("Warning: 'position' column missing, skipping position filter.")

    # --- 3. Feature Selection: The Definitive Dossier ---
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
        
        # [NEW] Explicit Opponent Defense Lags
        'opp_def_points_allowed_lag_1', 'opp_def_points_allowed_lag_2', 'opp_def_points_allowed_lag_3',
        'opp_def_rushing_yards_allowed_lag_1', 'opp_def_rushing_yards_allowed_lag_2', 'opp_def_rushing_yards_allowed_lag_3',

        # --- Pillar 6: Game Script (Opponent Offense) ---
        # Updated to match names from build_modeling_dataset.py
        'opp_off_rolling_total_off_points_4_weeks',
        'opp_off_rolling_total_yards_4_weeks',
        'opp_off_rolling_passing_yards_4_weeks',
        'opp_off_rolling_rushing_yards_4_weeks',

        # [NEW] Explicit Opponent Offense Lags
        'opp_off_total_off_points_lag_1', 'opp_off_total_off_points_lag_2', 'opp_off_total_off_points_lag_3',
        'opp_off_total_yards_lag_1', 'opp_off_total_yards_lag_2', 'opp_off_total_yards_lag_3'
    ]

    # Only keep features that actually exist in the input (avoids KeyErrors)
    existing_features = [col for col in features_to_keep if col in df_clean.columns]
    
    # Check if we missed any critical ones
    missing = set(features_to_keep) - set(existing_features)
    if missing:
        # print(f"⚠️ Warning: {len(missing)} requested features were not found in input (filled with 0 later).")
        pass # Suppress noise, usually just lags missing for Week 1

    # Select columns + Target
    cols_to_use = existing_features
    if TARGET in df_clean.columns:
        cols_to_use = list(set(existing_features + [TARGET]))
    
    df_featured = df_clean[cols_to_use].copy()
    print(f"Selected {len(existing_features)} high-impact features for the model.")

    # Fill missing numerical values with 0
    numeric_cols = df_featured.select_dtypes(include=['float64', 'int64']).columns
    df_featured[numeric_cols] = df_featured[numeric_cols].fillna(0)
    print("Handled missing numerical values by filling with 0.")

    # --- 4. [NEW] Calculate Player Season Baseline (Deviation Anchor) ---
    print("Calculating Player Season Baseline (Deviation Anchor)...")
    if TARGET in df_featured.columns:
        # Sort to ensure proper expansion
        df_featured.sort_values(by=['player_id', 'season', 'week'], inplace=True)
        
        # Calculate expanding mean (average of all PRIOR weeks in the current season)
        # shift(1) ensures we don't include the *current* week's points in the average
        df_featured['player_season_avg_points'] = df_featured.groupby(['player_id', 'season'])[TARGET] \
            .transform(lambda x: x.expanding().mean().shift(1))
        
        # Fill Week 1 NaNs with 0 (or you could use career avg if available)
        df_featured['player_season_avg_points'] = df_featured['player_season_avg_points'].fillna(0)
    else:
        print(f"⚠️ Warning: Target {TARGET} not found. Cannot calculate baseline.")
        df_featured['player_season_avg_points'] = 0.0

    # --- 5. Advanced Feature Engineering: Player Momentum ---
    print("Engineering 3-game rolling averages for key performance stats...")
    # Ensure sort again just in case
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
        
        # Fill the rolling NaNs with 0 to be safe for XGBoost
        avg_cols = [f'3_game_avg_{stat}' for stat in stats_to_roll if f'3_game_avg_{stat}' in df_featured.columns]
        df_featured[avg_cols] = df_featured[avg_cols].fillna(0)
        
    print(f"Final dataset has {df_featured.shape[0]} rows after feature engineering.")

    # --- 6. Save the new dataset ---
    df_featured.to_csv(output_path, index=False)
    print(f"\n✅ Successfully created the final feature-engineered dataset at '{output_path}'")

if __name__ == "__main__":
    # We use the assembled dataset as our input
    input_file = 'weekly_modeling_dataset_avg.csv'
    output_file = 'featured_dataset_avg.csv'
    engineer_features(input_file, output_file)