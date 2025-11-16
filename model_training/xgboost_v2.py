# model_training/train_xgboost_v4_tuned.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import mean_absolute_error
from pathlib import Path
import numpy as np # Import numpy

# --- Configuration ---
INPUT_FILE = Path("../dataPrep/featured_dataset_RB_clean.csv")
MODEL_OUTPUT_PATH = Path("./models/xgboost_RB_tuned_temporal_v1.joblib")
FEATURES_OUTPUT_PATH = Path("./models/feature_names_RB_tuned_temporal_v1.json")

# Define columns that are NOT features (identifiers or the target)
COLS_TO_DROP = [
    'season', 
    'week', 
    'player_id', 
    'player_name', 
    'position', # This is constant ('QB') so it must be dropped
    'team', 
    'opponent',
    'y_fantasy_points_ppr' # This is our target (Y)
]

# Define QB-irrelevant features to drop (since they are 0)
QB_IRRELEVANT_FEATURES = [
    '3_game_avg_targets',
    '3_game_avg_receptions',
    '3_game_avg_receiving_yards'
]

TARGET_VARIABLE = 'y_fantasy_points_ppr'
VALIDATION_SEASON = 2024 # We will test on the 2024 season

def main():
    print(f"--- Training new XGBoost model on {INPUT_FILE} ---")
    print(f"--- Using TEMPORAL SPLIT (Validate on {VALIDATION_SEASON}) ---")
    print(f"--- Using RandomizedSearchCV for Hyperparameter Tuning ---")

    # --- 1. Load Data ---
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} rows of WR data.")
    except FileNotFoundError:
        print(f"Error: File not found at {INPUT_FILE}.")
        return
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # --- 2. Prepare Data for Training ---
    df.dropna(subset=[TARGET_VARIABLE], inplace=True)
    df.fillna(0, inplace=True)

    # Drop WR-irrelevant columns
    cols_to_drop_final = COLS_TO_DROP + QB_IRRELEVANT_FEATURES
    
    # Get the final feature list
    feature_names = [col for col in df.columns if col not in cols_to_drop_final]
    
    # --- 3. Save the exact feature list ---
    print(f"\nTraining model on {len(feature_names)} features.")
    try:
        print(f"Saving feature list to {FEATURES_OUTPUT_PATH}...")
        # Ensure parent directory exists
        FEATURES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(FEATURES_OUTPUT_PATH, 'w') as f:
            json.dump(feature_names, f, indent=2)
        print("Feature list saved.")
    except Exception as e:
        print(f"Error saving feature list: {e}")

    # --- 4. Split Data by Time (No Shuffle!) ---
    df_train = df[df['season'] < VALIDATION_SEASON]
    df_val = df[df['season'] == VALIDATION_SEASON]

    if df_val.empty or df_train.empty:
        print(f"Error: Not enough data for temporal split (Train < {VALIDATION_SEASON}, Val = {VALIDATION_SEASON}).")
        return

    X_train = df_train[feature_names]
    y_train = df_train[TARGET_VARIABLE]
    
    X_val = df_val[feature_names]
    y_val = df_val[TARGET_VARIABLE]

    print(f"Split data: {len(X_train)} train samples (pre-{VALIDATION_SEASON}), {len(X_val)} validation samples ({VALIDATION_SEASON}).")

    # --- 5. Hyperparameter Tuning with Randomized Search ---
    print("\nInitiating hyperparameter search...")
    
    param_grid = {
        'learning_rate': [0.01, 0.02, 0.05, 0.1],
        'max_depth': [3, 4, 5, 6, 8],
        'n_estimators': [500, 1000, 1500],
        'subsample': [0.7, 0.8, 0.9],
        'colsample_bytree': [0.7, 0.8, 0.9],
        'gamma': [0, 0.1, 0.2, 0.3]
    }

    # Initialize the XGBoost Regressor
    xgb_reg = xgb.XGBRegressor(
        random_state=42, 
        n_jobs=-1, 
        tree_method='hist', # Use 'hist' for CPU, or 'gpu_hist' if you have GPU
        device="cpu",      # Use 'cpu' or 'cuda'
        early_stopping_rounds=50
    )

    # Set up Randomized Search to tune on the TRAINING data (pre-2024)
    random_search = RandomizedSearchCV(
        estimator=xgb_reg,
        param_distributions=param_grid,
        n_iter=50, # Try 50 different combinations
        scoring='neg_mean_absolute_error', # We want to minimize MAE
        cv=5, # 5-fold cross-validation *within the pre-2024 data*
        verbose=1,
        random_state=42
    )

    print("Searching for best hyperparameters on pre-2024 data...")
    # Fit the search on the TRAINING data only
    # We need to provide an eval set for early stopping *during* the search
    # A common practice is to split the training set again
    X_train_sub, X_eval_sub, y_train_sub, y_eval_sub = train_test_split(
        X_train, y_train, test_size=0.15, random_state=42, shuffle=False # Temporal split within training
    )
    
    random_search.fit(
        X_train_sub, y_train_sub, 
        eval_set=[(X_eval_sub, y_eval_sub)], 
        verbose=False
    )

    print("\n--- Hyperparameter Search Complete ---")
    print(f"Best Parameters Found: {random_search.best_params_}")
    
    # --- 6. Train and Evaluate the Final, Tuned Model ---
    print("\nTraining the final model with best parameters on ALL pre-2024 data...")
    best_model = random_search.best_estimator_

    # Now, retrain the best model on the *entire* training set (X_train, y_train)
    # using the held-out 2024 data (X_val, y_val) for early stopping
    best_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=100
    )

    print("\nTraining complete.")
    preds = best_model.predict(X_val)
    mae = mean_absolute_error(y_val, preds)
    
    print("\n--- Tuned Model Evaluation ---")
    print(f"✅ Final TRUE Validation MAE (on {VALIDATION_SEASON} data): {mae:.4f}")

    # --- 7. Save the Final, Tuned Model ---
    print(f"\nSaving the final tuned model to '{MODEL_OUTPUT_PATH}'...")
    # Ensure parent directory exists
    MODEL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model, MODEL_OUTPUT_PATH)
    print("✅ Tuned model saved.")

if __name__ == "__main__":
    main()