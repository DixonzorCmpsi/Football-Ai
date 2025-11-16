# model_training/train_xgboost_v5_sliding_window.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

# --- Configuration ---
# --- MODIFICATION: Point to the FULL WR dataset ---
INPUT_FILE = Path("../dataPrep/timeseries_training_data_WR.csv")
MODEL_OUTPUT_PATH = Path("./models/xgboost_WR_sliding_window_v1(TimeSeries).joblib")
FEATURES_OUTPUT_PATH = Path("./models/feature_names_WR_sliding_window_v1(TimeSeries).json")
PLOT_DIR = Path("./models/evaluation_plots_WR(TimeSeries)")
PLOT_DIR.mkdir(parents=True, exist_ok=True) 

# --- MODIFICATION: Define multiple validation seasons for the sliding window ---
VALIDATION_SEASONS = [2022, 2023, 2024] 
N_TOP_FEATURES = 30 # How many top features to select for the final model

# Define columns that are NOT features (identifiers or the target)
COLS_TO_DROP = [
    'season', 'week', 'player_id', 'player_name', 'position',
    'team', 'opponent', 'y_fantasy_points_ppr'
]
TARGET_VARIABLE = 'y_fantasy_points_ppr'

# --- Plotting Functions (No changes) ---
def plot_results(y_true, y_pred, title, plot_path):
    """Generates and saves Predicted vs. Actual and Residual plots."""
    try:
        plt.figure(figsize=(12, 6))
        
        plt.subplot(1, 2, 1)
        plt.scatter(y_true, y_pred, alpha=0.3)
        plt.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], '--', color='red', lw=2, label="Perfect Prediction")
        plt.title(f'Predicted vs. Actual ({title})')
        plt.xlabel('Actual Points')
        plt.ylabel('Predicted Points')
        plt.legend()
        plt.grid(True)

        residuals = y_true - y_pred
        plt.subplot(1, 2, 2)
        plt.scatter(y_pred, residuals, alpha=0.3)
        plt.hlines(0, y_pred.min(), y_pred.max(), linestyle='--', color='red', lw=2)
        plt.title(f'Residuals ({title})')
        plt.xlabel('Predicted Points')
        plt.ylabel('Error (Actual - Predicted)')
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()
        print(f"Saved validation plots to {plot_path}")
    except Exception as e:
        print(f"Warning: Failed to generate plots. {e}")

def get_feature_importance(model, feature_names, output_dir):
    """Gets, prints, and plots feature importances."""
    print("\n" + "="*50)
    print("--- MODEL FEATURE IMPORTANCE ---")
    print("="*50)
    try:
        importances = model.feature_importances_
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importances
        }).sort_values(by='importance', ascending=False)
        
        print("Top 35 Most Important Features:")
        print(importance_df.head(35).to_string(index=False))

        plt.figure(figsize=(10, 14))
        top_features = importance_df.head(N_TOP_FEATURES)
        plt.barh(top_features['feature'], top_features['importance'])
        plt.title(f'Top {N_TOP_FEATURES} Feature Importances')
        plt.xlabel('Importance (Gain)')
        plt.ylabel('Feature')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        
        plot_path = output_dir / "final_model_feature_importance.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"\nSaved feature importance plot to {plot_path}")
        
        return importance_df['feature'].tolist() # Return all features, sorted
        
    except Exception as e:
        print(f"Warning: Could not generate feature importance plot. {e}")
        return feature_names

def main():
    print(f"--- Training Full WR Model on {INPUT_FILE} ---")
    print(f"--- Using SLIDING WINDOW Validation (Seasons: {VALIDATION_SEASONS}) ---")

    # --- 1. Load Data ---
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} rows of FULL WR data.")
    except FileNotFoundError:
        print(f"Error: File not found at {INPUT_FILE}.")
        return
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # --- 2. Prepare Feature List ---
    df.dropna(subset=[TARGET_VARIABLE], inplace=True)
    df.fillna(0, inplace=True)
    feature_names = [col for col in df.columns if col not in COLS_TO_DROP]
    
    # --- 3. Save Feature List (Full Set) ---
    # We save this once, as it's the same for all folds
    full_features_path = FEATURES_OUTPUT_PATH.with_name(f"{FEATURES_OUTPUT_PATH.stem}_FULL.json")
    print(f"\nTraining model on {len(feature_names)} features.")
    try:
        print(f"Saving FULL feature list to {full_features_path}...")
        full_features_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_features_path, 'w') as f:
            json.dump(feature_names, f, indent=2)
        print("Full feature list saved.")
    except Exception as e:
        print(f"Error saving feature list: {e}")

    # --- 4. Sliding Window Cross-Validation ---
    all_fold_metrics = []
    best_params = None
    initial_model = None # To store the first trained model for importance

    for val_season in VALIDATION_SEASONS:
        print("\n" + "="*50)
        print(f"--- FOLD: Training on data < {val_season}, Validating on {val_season} ---")
        print("="*50)

        # 4a. Split Data
        df_train = df[df['season'] < val_season]
        df_val = df[df['season'] == val_season]

        if df_val.empty or df_train.empty:
            print(f"Error: Not enough data for split on season {val_season}. Skipping fold.")
            continue

        X_train = df_train[feature_names]
        y_train = df_train[TARGET_VARIABLE]
        X_val = df_val[feature_names]
        y_val = df_val[TARGET_VARIABLE]

        print(f"Split data: {len(X_train)} train samples, {len(X_val)} validation samples.")

        # --- 4b. Tune Hyperparameters (ONLY ONCE, on the first fold) ---
        if best_params is None:
            print("\nInitiating hyperparameter search (runs only on first fold)...")
            param_grid_focused = {
                'learning_rate': [0.03, 0.05, 0.07], 'max_depth': [3, 4],
                'n_estimators': [1000, 1200], 'subsample': [0.7, 0.8],
                'colsample_bytree': [0.8, 0.9], 'gamma': [0, 0.1]
            }
            param_grid_to_use = param_grid_focused
            print(f"Using focused grid with {np.prod([len(v) for v in param_grid_to_use.values()])} combinations.")
            
            xgb_reg = xgb.XGBRegressor(random_state=42, n_jobs=-1, tree_method='hist', device="cuda", early_stopping_rounds=50)
            
            # Use a temporal sub-split of the *first* training set for tuning
            X_train_sub, X_eval_sub, y_train_sub, y_eval_sub = train_test_split(
                X_train, y_train, test_size=0.15, random_state=42, shuffle=False # Temporal split
            )
            
            random_search = RandomizedSearchCV(
                estimator=xgb_reg, param_distributions=param_grid_to_use, 
                n_iter=50, # Capped at 96
                scoring='neg_mean_absolute_error', cv=5, verbose=1, random_state=42
            )
            
            print(f"Searching for best hyperparameters on {len(X_train_sub)} samples...")
            random_search.fit(X_train_sub, y_train_sub, eval_set=[(X_eval_sub, y_eval_sub)], verbose=False)
            
            best_params = random_search.best_params_
            print(f"\n--- Hyperparameter Search Complete ---")
            print(f"Best Parameters Found: {best_params}")

        # --- 4c. Train Model for this Fold ---
        print(f"\nTraining fold model for {val_season} with best parameters...")
        model = xgb.XGBRegressor(
            **best_params,
            random_state=42, n_jobs=-1, tree_method='hist', device="cuda",
            early_stopping_rounds=50 # Use early stopping
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)], # Validate on the holdout season
            verbose=100
        )
        
        if val_season == VALIDATION_SEASONS[0]:
            initial_model = model # Save the first model for feature importance

        # --- 4d. Evaluate and Plot for this Fold ---
        print("\nFold training complete. Evaluating...")
        preds = model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        r2 = r2_score(y_val, preds)
        
        all_fold_metrics.append({'season': val_season, 'mae': mae, 'r2': r2})
        
        print(f"--- Fold {val_season} Results ---")
        print(f"✅ MAE: {mae:.4f}")
        print(f"✅ R-squared (R²): {r2:.4f}")
        
        plot_results(y_val, preds, f"Full Model (Season {val_season})", PLOT_DIR)

    # --- 5. Report Average Performance ---
    print("\n" + "="*50)
    print("--- SLIDING WINDOW VALIDATION COMPLEWR ---")
    if all_fold_metrics:
        avg_mae = np.mean([m['mae'] for m in all_fold_metrics])
        avg_r2 = np.mean([m['r2'] for m in all_fold_metrics])
        print(f"Final Model Performance (Averaged over {len(VALIDATION_SEASONS)} seasons):")
        print(f"✅ Average MAE: {avg_mae:.4f}")
        print(f"✅ Average R²: {avg_r2:.4f}")
        print("\nIndividual Fold Metrics:")
        print(pd.DataFrame(all_fold_metrics).set_index('season'))
    else:
        print("No validation folds were completed.")
        return

    # --- 6. Get Feature Importance from First Model ---
    if initial_model:
        sorted_features = get_feature_importance(initial_model, feature_names, PLOT_DIR)
        top_features = sorted_features[:N_TOP_FEATURES]
        print(f"\nSelected Top {N_TOP_FEATURES} features for final model: {top_features}")
    else:
        print("No initial model was trained, cannot select top features. Saving full model.")
        top_features = feature_names # Fallback to all features

    # --- 7. Train FINAL Refined Model on ALL Data ---
    print("\n" + "="*50)
    print(f"--- Training FINAL Refined Model (Top {N_TOP_FEATURES} Features) ---")
    print("="*50)
    
    # Use all data up to the *last* validation season
    final_train_season = max(VALIDATION_SEASONS)
    df_final_train = df[df['season'] <= final_train_season]
    
    # Create new training set with only top features
    X_train_top = df_final_train[top_features]
    y_final_train = df_final_train[TARGET_VARIABLE]

    print(f"Final training set size: {len(X_train_top)} rows.")
    
    final_model = xgb.XGBRegressor(
        **best_params, # Use best params found
        random_state=42, 
        n_jobs=-1, 
        tree_method='hist', 
        device="cuda"
        # No early stopping, train on all available data
    )
    
    final_model.fit(X_train_top, y_final_train, verbose=100)
    
    print("\nFinal refined model training complete.")
    print(f"Saving final production model to '{MODEL_OUTPUT_PATH}'...")
    MODEL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, MODEL_OUTPUT_PATH)
    print("✅ Final model saved successfully.")
    
    print(f"Saving final *feature list* to '{FEATURES_OUTPUT_PATH}'...")
    with open(FEATURES_OUTPUT_PATH, 'w') as f:
        json.dump(top_features, f, indent=2)
    print("✅ Feature list saved.")

if __name__ == "__main__":
    main()