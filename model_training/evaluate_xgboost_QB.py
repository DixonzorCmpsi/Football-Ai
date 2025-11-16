# model_training/evaluate_xgboost_QB.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.metrics import mean_absolute_error, r2_score
from pathlib import Path
import numpy as np

# --- Configuration ---
# Path to the dataset you trained on
DATA_FILE = Path("../dataPrep/featured_dataset_RB.csv") 

# Path to the model you just saved
MODEL_PATH = Path("./models/xgboost_RB_tuned_temporal_v1.joblib") 

# Path to the feature names file generated during training
FEATURES_PATH = Path("./models/feature_names_RB_tuned_temporal_v1.json") 

TARGET_VARIABLE = 'y_fantasy_points_ppr'
VALIDATION_SEASON = 2024 # The season you held out for validation

def main():
    print(f"--- Evaluating Model ---")
    print(f"Model: {MODEL_PATH}")
    print(f"Data: {DATA_FILE}")
    print(f"Features: {FEATURES_PATH}")
    print(f"Validation Season: {VALIDATION_SEASON}")

    # --- 1. Load Model, Features, and Data ---
    try:
        print("Loading model...")
        model = joblib.load(MODEL_PATH)
        
        print("Loading feature names...")
        with open(FEATURES_PATH, 'r') as f:
            feature_names = json.load(f)
        print(f"Loaded {len(feature_names)} features.")
        
        print("Loading dataset...")
        df = pd.read_csv(DATA_FILE)
        
    except FileNotFoundError as e:
        print(f"\n!!! ERROR: File not found. {e}")
        return
    except Exception as e:
        print(f"\n!!! ERROR: {e}")
        return

    # --- 2. Prepare Validation Set (2024 data) ---
    print(f"Filtering for validation season {VALIDATION_SEASON}...")
    df_val = df[df['season'] == VALIDATION_SEASON].copy()
    
    if df_val.empty:
        print(f"Error: No data found for validation season {VALIDATION_SEASON}.")
        return
        
    # Fill any NaNs just as in training
    df_val.fillna(0, inplace=True)

    # Separate features (X) and target (y)
    # Ensure all expected features are present, fill with 0 if not
    missing_features = set(feature_names) - set(df_val.columns)
    if missing_features:
        print(f"Warning: {len(missing_features)} features missing from validation data. Filling with 0.")
        for col in missing_features:
            df_val[col] = 0.0

    # Ensure correct feature order
    X_val = df_val[feature_names]
    y_val = df_val[TARGET_VARIABLE]

    print(f"Prepared {len(X_val)} validation samples.")

    # --- 3. Run Predictions ---
    print("Running predictions...")
    try:
        predictions = model.predict(X_val)
    except Exception as e:
        print(f"Error during prediction: {e}")
        print("This might be a feature mismatch error. Ensure the .json file is correct.")
        return

    # --- 4. Calculate and Display Metrics ---
    mae = mean_absolute_error(y_val, predictions)
    r2 = r2_score(y_val, predictions)
    
    print("\n--- Model Evaluation Results ---")
    print(f"✅ Mean Absolute Error (MAE): {mae:.4f}")
    print(f"✅ R-squared (R²): {r2:.4f}")

    # --- 5. Show Prediction Examples ---
    print("\n--- Prediction Examples (Actual vs. Predicted) ---")
    df_results = pd.DataFrame({
        'player_name': df_val['player_name'],
        'week': df_val['week'],
        'actual_points': y_val,
        'predicted_points': predictions
    })
    df_results['error'] = (df_results['predicted_points'] - df_results['actual_points']).abs()
    
    # Sort by error to see the worst misses
    df_results_sorted = df_results.sort_values(by='error', ascending=False)
    
    print("\nTop 10 Worst Misses (Highest Error):")
    print(df_results_sorted.head(10).to_string(index=False, float_format="%.2f"))
    
    print("\nTop 10 Best Hits (Lowest Error):")
    print(df_results_sorted.tail(10).to_string(index=False, float_format="%.2f"))

if __name__ == "__main__":
    main()