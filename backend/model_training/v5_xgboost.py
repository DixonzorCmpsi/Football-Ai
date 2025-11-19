# model_training/train_xgboost_v5_sliding_window.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score # Import R-squared
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt # Import for plotting

# --- Configuration ---
INPUT_FILE = Path("../dataPrep/featured_dataset_WR.csv")
MODEL_OUTPUT_PATH = Path("./models/xgboost_WR_tuned_sliding_v1.joblib")
FEATURES_OUTPUT_PATH = Path("./models/feature_names_WR_tuned_sliding_v1.json")
# --- NEW: Plot output paths ---
PLOT_DIR = Path("./models/evaluation_plots")
PLOT_DIR.mkdir(parents=True, exist_ok=True) # Create the directory

# Define the seasons to use for validation
VALIDATION_SEASONS = [2022, 2023, 2024] 
# (You can expand this to [2020, 2021, 2022, 2023, 2024] for a 5-fold run)

# Define columns that are NOT features (identifiers or the target)
COLS_TO_DROP = [
    'season', 'week', 'player_id', 'player_name', 'position',
    'team', 'opponent', 'y_fantasy_points_ppr'
]
# Define WR-irrelevant features to drop (since they are 0)
WR_IRRELEVANT_FEATURES = [
    '3_game_avg_targets',
    '3_game_avg_receptions',
    '3_game_avg_receiving_yards'
]
TARGET_VARIABLE = 'y_fantasy_points_ppr'

# --- NEW: Function to plot results ---
def plot_results(y_true, y_pred, season, output_dir):
    """Generates and saves Predicted vs. Actual and Residual plots."""
    try:
        plt.figure(figsize=(12, 6))
        
        # 1. Predicted vs. Actual Scatter Plot
        plt.subplot(1, 2, 1)
        # Add a light jitter to see overlapping points
        y_true_jitter = y_true + np.random.randn(len(y_true)) * 0.1
        y_pred_jitter = y_pred + np.random.randn(len(y_pred)) * 0.1
        plt.scatter(y_true_jitter, y_pred_jitter, alpha=0.3)
        plt.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], '--', color='red', lw=2, label="Perfect Prediction")
        plt.title(f'Predicted vs. Actual (Season {season})')
        plt.xlabel('Actual Points')
        plt.ylabel('Predicted Points')
        plt.legend()
        plt.grid(True)

        # 2. Residual Plot
        residuals = y_true - y_pred
        plt.subplot(1, 2, 2)
        plt.scatter(y_pred, residuals, alpha=0.3)
        plt.hlines(0, y_pred.min(), y_pred.max(), linestyle='--', color='red', lw=2)
        plt.title(f'Residuals (Season {season})')
        plt.xlabel('Predicted Points')
        plt.ylabel('Error (Actual - Predicted)')
        plt.grid(True)
        
        plt.tight_layout()
        plot_path = output_dir / f"validation_plots_season_{season}.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"Saved validation plots to {plot_path}")
    except Exception as e:
        print(f"Warning: Failed to generate plots. {e}")

# --- NEW: Function to plot feature importance ---
def plot_and_print_importance(model, feature_names, output_dir):
    """Gets, prints, and plots the top 25 feature importances."""
    print("\n" + "="*50)
    print("--- FINAL MODEL FEATURE IMPORTANCE ---")
    print("="*50)
    try:
        # Create a DataFrame of feature importances
        importances = model.feature_importances_
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importances
        }).sort_values(by='importance', ascending=False)
        
        # Print the top 25
        print("Top 25 Most Important Features:")
        print(importance_df.head(25).to_string(index=False))

        # Plot the top 25
        plt.figure(figsize=(10, 12))
        top_25 = importance_df.head(25)
        plt.barh(top_25['feature'], top_25['importance'])
        plt.title('Top 25 Feature Importances')
        plt.xlabel('Importance (Gain)')
        plt.ylabel('Feature')
        plt.gca().invert_yaxis() # Display most important at the top
        plt.tight_layout()
        
        plot_path = output_dir / "final_model_feature_importance.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"\nSaved feature importance plot to {plot_path}")
        
    except Exception as e:
        print(f"Warning: Could not generate feature importance plot. {e}")


def main():
    print(f"--- Training new XGBoost model on {INPUT_FILE} ---")
    print(f"--- Using SLIDING WINDOW Validation (Seasons: {VALIDATION_SEASONS}) ---")
    print(f"--- Using GridSearchCV for Hyperparameter Tuning ---")

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

    # --- 2. Prepare Feature List ---
    df.dropna(subset=[TARGET_VARIABLE], inplace=True)
    df.fillna(0, inplace=True)
    cols_to_drop_final = COLS_TO_DROP + WR_IRRELEVANT_FEATURES
    feature_names = [col for col in df.columns if col not in cols_to_drop_final]
    
    # --- 3. Save Feature List ---
    print(f"\nTraining model on {len(feature_names)} features.")
    try:
        print(f"Saving feature list to {FEATURES_OUTPUT_PATH}...")
        FEATURES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(FEATURES_OUTPUT_PATH, 'w') as f:
            json.dump(feature_names, f, indent=2)
        print("Feature list saved.")
    except Exception as e:
        print(f"Error saving feature list: {e}")

    # --- 4. Sliding Window Cross-Validation ---
    all_fold_metrics = []
    best_params = None

    for val_season in VALIDATION_SEASONS:
        print("\n" + "="*50)
        print(f"--- FOLD: Training on data < {val_season}, Validating on {val_season} ---")
        print("="*50)

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

        # --- Tune Hyperparameters (ONLY ONCE, on the first fold) ---
        if best_params is None:
            print("\nInitiating hyperparameter search (runs only on first fold)...")
            
            # Using the focused grid
            param_grid_focused = {
                'learning_rate': [0.03, 0.05, 0.07],
                'max_depth': [3, 4],
                'n_estimators': [1000, 1200],
                'subsample': [0.7, 0.8],
                'colsample_bytree': [0.8, 0.9],
                'gamma': [0, 0.1]
            }
            param_grid_to_use = param_grid_focused
            print(f"Using focused grid with {np.prod([len(v) for v in param_grid_to_use.values()])} combinations.")
            
            xgb_reg = xgb.XGBRegressor(random_state=42, n_jobs=-1, tree_method='hist', device="cuda", early_stopping_rounds=50)
            
            grid_search = GridSearchCV(
                estimator=xgb_reg, param_grid=param_grid_to_use,
                scoring='neg_mean_absolute_error', cv=5, verbose=2, n_jobs=-1
            )

            print(f"Searching for best hyperparameters on {len(X_train)} samples...")
            # Use a sub-split of this fold's training data for early stopping in the search
            X_train_sub, X_eval_sub, y_train_sub, y_eval_sub = train_test_split(
                X_train, y_train, test_size=0.15, random_state=42, shuffle=False
            )
            
            grid_search.fit(X_train_sub, y_train_sub, eval_set=[(X_eval_sub, y_eval_sub)], verbose=False)
            
            best_params = grid_search.best_params_
            print(f"\n--- Hyperparameter Search Complete ---")
            print(f"Best Parameters Found: {best_params}")

        # --- Train Model for this Fold ---
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

        # --- Evaluate and Plot for this Fold ---
        print("\nFold training complete. Evaluating...")
        preds = model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        r2 = r2_score(y_val, preds) # <<<--- NEW METRIC
        
        all_fold_metrics.append({'season': val_season, 'mae': mae, 'r2': r2})
        
        print(f"--- Fold {val_season} Results ---")
        print(f"✅ MAE: {mae:.4f}")
        print(f"✅ R-squared (R²): {r2:.4f}")
        
        # --- NEW: Generate plots for this fold ---
        plot_results(y_val, preds, val_season, PLOT_DIR)

    # --- 5. Report Average Performance ---
    print("\n" + "="*50)
    print("--- SLIDING WINDOW VALIDATION COMPLETE ---")
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

    # --- 6. Train and Save Final Model ---
    print("\n" + "="*50)
    print("Training final model on ALL historical data (pre-2025)...")
    
    final_train_season = max(VALIDATION_SEASONS)
    df_final_train = df[df['season'] <= final_train_season]
    X_final_train = df_final_train[feature_names]
    y_final_train = df_final_train[TARGET_VARIABLE]

    print(f"Final training set size: {len(X_final_train)} rows.")
    
    final_model = xgb.XGBRegressor(
        **best_params, # Use best params found
        random_state=42, 
        n_jobs=-1, 
        tree_method='hist', 
        device="cuda"
        # No early stopping, train on all data
    )
    
    final_model.fit(X_final_train, y_final_train, verbose=100)
    
    print("\nFinal model training complete.")
    print(f"Saving final production model to '{MODEL_OUTPUT_PATH}'...")
    MODEL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, MODEL_OUTPUT_PATH)
    print("✅ Final model saved successfully.")

    # --- 7. NEW: Plot and Print Final Model Importance ---
    plot_and_print_importance(final_model, feature_names, PLOT_DIR)


if __name__ == "__main__":
    main()