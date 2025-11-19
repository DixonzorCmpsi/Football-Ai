# model_training/generate_meta_dataset.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from pathlib import Path
import numpy as np
import sys
import os

# --- Configuration ---
# This script will be run 4 times, once for each position
# Manually change this value before each run:
#POSITION_TO_RUN = "QB"
#POSITION_TO_RUN = "RB"
#POSITION_TO_RUN = "WR"
POSITION_TO_RUN = "TE" 

# This is the master file all runs will append to
META_DATASET_OUTPUT_FILE = Path("./meta_training_dataset.csv")

# Define the seasons to use for validation
VALIDATION_SEASONS = [2022, 2023, 2024] 

# Define paths for this position
INPUT_FILE = Path(f"../dataPrep/timeseries_training_data_{POSITION_TO_RUN}.csv")
MODEL_PATH_TEMPLATE = Path(f"./models/base_{POSITION_TO_RUN}_model_fold_")
FEATURES_PATH = Path(f"./models/feature_names_{POSITION_TO_RUN}_sliding_window_v1(TimeSeries).json")

# Define columns that are NOT features (identifiers or the target)
COLS_TO_DROP = [
    'season', 'week', 'player_id', 'player_name', 'position',
    'team', 'opponent', 'y_fantasy_points_ppr'
]
TARGET_VARIABLE = 'y_fantasy_points_ppr'


def main(pos, input_file, features_path):
    print("\n" + "="*60)
    print(f"--- STEP 1: GENERATING META-DATASET FOR: {pos} ---")
    print(f"--- Input: {input_file} ---")
    print(f"--- Features: {features_path} ---")
    print("="*60)

    # --- 1. Load Data ---
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows of {pos} data.")
    except Exception as e:
        print(f"Error loading file: {e}"); return

    # --- 2. Load Feature List ---
    try:
        with open(features_path, 'r') as f:
            feature_names = json.load(f)
        print(f"Loaded {len(feature_names)} features from {features_path}.")
    except Exception as e:
        print(f"Error loading feature list: {e}"); return
        
    df.fillna(0, inplace=True)
    
    # --- 3. Sliding Window Prediction Loop ---
    all_oof_predictions = [] # List to store DataFrames
    
    # --- Find Best Hyperparameters (Run Once on First Fold) ---
    print("\nInitiating hyperparameter search (runs only on first fold)...")
    df_train_tune = df[df['season'] < VALIDATION_SEASONS[0]]
    X_train_tune = df_train_tune[feature_names]
    y_train_tune = df_train_tune[TARGET_VARIABLE]
    
    # Simple temporal split for early stopping in RandomizedSearch
    X_train_sub, X_eval_sub, y_train_sub, y_eval_sub = train_test_split(
        X_train_tune, y_train_tune, test_size=0.15, random_state=42, shuffle=False
    )
            
    # Use the best params you found for this position (example for RB)
    # This is faster than re-tuning, but we can re-tune if needed
    best_params = {
        'QB': {'subsample': 0.7, 'n_estimators': 1200, 'max_depth': 4, 'learning_rate': 0.03, 'gamma': 0.1, 'colsample_bytree': 0.8},
        'RB': {'subsample': 0.7, 'n_estimators': 1000, 'max_depth': 3, 'learning_rate': 0.05, 'gamma': 0.1, 'colsample_bytree': 0.9},
        'WR': {'subsample': 0.8, 'n_estimators': 1000, 'max_depth': 4, 'learning_rate': 0.03, 'gamma': 0, 'colsample_bytree': 0.9},
        'TE': {'subsample': 0.8, 'n_estimators': 1000, 'max_depth': 4, 'learning_rate': 0.03, 'gamma': 0, 'colsample_bytree': 0.9}
    }.get(pos, {}) # Get params or empty dict
    
    if not best_params:
        print(f"No hardcoded params for {pos}, running RandomizedSearch...")
        # (RandomizedSearchCV logic would go here if needed)
        print("Using default params for {pos} (Update script with best params)")
        
    print(f"Using Best Params for {pos}: {best_params}")

    for val_season in VALIDATION_SEASONS:
        print("\n" + "="*50)
        print(f"--- FOLD: Training on < {val_season}, Validating on {val_season} ---")
        
        # 3a. Split Data
        df_train = df[df['season'] < val_season]
        df_val = df[df['season'] == val_season]

        if df_val.empty or df_train.empty:
            print(f"Skipping fold for {val_season}.")
            continue

        X_train = df_train[feature_names]
        y_train = df_train[TARGET_VARIABLE]
        X_val = df_val[feature_names]
        y_val = df_val[TARGET_VARIABLE]

        # 3b. Train Model for this Fold
        print(f"Training fold model for {val_season}...")
        model = xgb.XGBRegressor(
            **best_params,
            random_state=42, n_jobs=-1, tree_method='hist', device="cuda",
            early_stopping_rounds=50
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        # 3c. Generate and Store OOF Predictions
        print(f"Generating Out-of-Fold (OOF) predictions for {val_season}...")
        preds = model.predict(X_val)
        
        # Create a df of the results
        df_fold_results = pd.DataFrame({
            'player_id': df_val['player_id'],
            'season': df_val['season'],
            'week': df_val['week'],
            'position': df_val['position'],
            'y_actual_points': df_val[TARGET_VARIABLE],
            'L0_prediction': preds
        })
        
        all_oof_predictions.append(df_fold_results)
        
        mae = mean_absolute_error(y_val, preds)
        print(f"Fold {val_season} MAE: {mae:.4f}")

    # --- 4. Combine and Save All OOF Predictions ---
    if not all_oof_predictions:
        print("No predictions were generated. Exiting.")
        return

    df_meta = pd.concat(all_oof_predictions)
    print(f"\nTotal OOF predictions generated: {len(df_meta)}")
    
    # Save/Append to the master meta file
    if os.path.exists(META_DATASET_OUTPUT_FILE):
        print(f"Appending {pos} predictions to {META_DATASET_OUTPUT_FILE}...")
        df_meta.to_csv(META_DATASET_OUTPUT_FILE, mode='a', header=False, index=False)
    else:
        print(f"Creating new meta-dataset at {META_DATASET_OUTPUT_FILE}...")
        df_meta.to_csv(META_DATASET_OUTPUT_FILE, mode='w', header=True, index=False)
        
    print(f"✅ Successfully saved {pos} predictions.")

if __name__ == "__main__":
    
    pos_config = {
        'QB': {
            'input': Path("../dataPrep/timeseries_training_data_QB.csv"),
            'features': Path("./models/xgboost_QB_sliding_window_v1(TimeSeries).json")
        },
        'RB': {
            'input': Path("../dataPrep/timeseries_training_data_RB.csv"), # Use your clean RB file
            'features': Path("./models/xgboost_RB_sliding_window_v1(TimeSeries).json") # Use your top 30 RB features
        },
        'WR': {
            'input': Path("../dataPrep/timeseries_training_data_WR.csv"), # Use your clean WR file
            'features': Path("./models/xgboost_WR_sliding_window_v1(TimeSeries).json") # Use your top 30 WR features
        },
        'TE': {
            'input': Path("../dataPrep/timeseries_training_data_TE.csv"), # Use your clean TE file
            'features': Path("./models/xgboost_TE_sliding_window_v1(TimeSeries).json") # Use your top 30 TE features
        }
    }
    
    if not INPUT_FILE.exists():
        print(f"Error: Input file {INPUT_FILE} not found.")
        sys.exit()
    if not FEATURES_PATH.exists():
        print(f"Error: Feature file {FEATURES_PATH} not found.")
        sys.exit()

    # --- Run the main function for the specified position ---
    main(POSITION_TO_RUN, INPUT_FILE, FEATURES_PATH)