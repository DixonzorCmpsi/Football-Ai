# model_training/train_meta_model.py
import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from pathlib import Path
import matplotlib.pyplot as plt

# --- Configuration ---
INPUT_FILE = Path("./meta_training_dataset.csv")
PROFILE_FILE = Path("../rag_data/player_profiles.csv")
MODEL_OUTPUT_PATH = Path("./models/xgboost_META_model_v1.joblib")
FEATURES_OUTPUT_PATH = Path("./models/feature_names_META_model_v1.json")
PLOT_DIR = Path("./models/evaluation_plots_META")
PLOT_DIR.mkdir(parents=True, exist_ok=True) 

def plot_meta_results(df_val, pos):
    """Generates plots for the meta model, faceted by position."""
    try:
        df_pos = df_val[df_val['position'] == pos]
        if df_pos.empty: return
        
        y_true = df_pos['y_actual_points']
        y_pred = df_pos['L1_prediction']
        
        plt.figure(figsize=(12, 6))
        
        plt.subplot(1, 2, 1)
        plt.scatter(y_true, y_pred, alpha=0.3)
        plt.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], '--', color='red', lw=2)
        plt.title(f'Meta-Model: {pos} Predicted vs. Actual')
        plt.xlabel('Actual Points')
        plt.ylabel('Meta-Predicted Points')
        plt.grid(True)

        residuals = y_true - y_pred
        plt.subplot(1, 2, 2)
        plt.scatter(y_pred, residuals, alpha=0.3)
        plt.hlines(0, y_pred.min(), y_pred.max(), linestyle='--', color='red', lw=2)
        plt.title(f'Meta-Model: {pos} Residuals')
        plt.xlabel('Meta-Predicted Points')
        plt.ylabel('Error (Actual - Meta)')
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig(PLOT_DIR / f"meta_model_plots_{pos}.png")
        plt.close()
    except Exception as e:
        print(f"Plotting failed for {pos}: {e}")

def main():
    print(f"--- Training Meta-Model on {INPUT_FILE} ---")

    # --- 1. Load Data ---
    try:
        df_meta = pd.read_csv(INPUT_FILE)
        df_profile = pd.read_csv(PROFILE_FILE)
        print(f"Loaded {len(df_meta)} OOF predictions.")
    except Exception as e:
        print(f"Error loading files: {e}"); return

    # --- 2. Engineer Meta-Features ---
    print("Pivoting data to create ecosystem features...")
    
    # Need team for each player
    df_profile_simple = df_profile[['player_id', 'team_abbr']].drop_duplicates(subset=['player_id'])
    df_meta = df_meta.merge(df_profile_simple, on='player_id', how='left')

    # Pivot the L0 predictions
    df_pivot_preds = df_meta.pivot_table(
        index=['season', 'week', 'team_abbr'],
        columns='position',
        values='L0_prediction'
    ).add_prefix('L0_pred_') # -> L0_pred_QB, L0_pred_RB, ...
    
    # Pivot the actuals (for training)
    df_pivot_actuals = df_meta.pivot_table(
        index=['season', 'week', 'team_abbr'],
        columns='position',
        values='y_actual_points'
    ).add_prefix('y_actual_') # -> y_actual_QB, y_actual_RB, ...
    
    # Join predictions and actuals
    df_train = df_pivot_preds.join(df_pivot_actuals).reset_index()
    df_train.fillna(0, inplace=True)
    
    print(f"Created {len(df_train)} team-game rows for meta-training.")

    # --- 3. Train a Separate Meta-Model for each Position ---
    # This is more robust: the QB meta-model will learn to fix QB predictions, etc.
    positions_to_train = ['QB', 'RB', 'WR', 'TE']
    
    # These are the features for our meta-model: the predictions from the base models
    meta_features = ['L0_pred_QB', 'L0_pred_RB', 'L0_pred_WR', 'L0_pred_TE']
    
    # Save the feature list
    FEATURES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FEATURES_OUTPUT_PATH, 'w') as f:
        json.dump(meta_features, f, indent=2)
    print(f"Saved meta-model feature list to {FEATURES_OUTPUT_PATH}")

    final_meta_models = {}

    for pos in positions_to_train:
        print("\n" + "="*50)
        print(f"--- Training META-MODEL for: {pos} ---")
        
        target = f'y_actual_{pos}'
        if target not in df_train.columns:
            print(f"Target {target} not found. Skipping {pos}.")
            continue
            
        X = df_train[meta_features]
        y = df_train[target]
        
        # Use a simple split for the meta-model, as data is already OOF
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)

        # We can use a simpler model here, as the task is easier
        meta_model = xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators=200,
            learning_rate=0.05,
            max_depth=3,
            device="cuda",
            early_stopping_rounds=20
        )
        
        meta_model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        preds = meta_model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        r2 = r2_score(y_val, preds)
        
        print(f"✅ Meta-Model for {pos} MAE: {mae:.4f} (R²: {r2:.4f})")
        final_meta_models[pos] = meta_model

    # --- 4. Save the Final Meta-Model(s) ---
    print(f"\nSaving final meta-model(s) to '{MODEL_OUTPUT_PATH}'...")
    joblib.dump(final_meta_models, MODEL_OUTPUT_PATH)
    print("✅ Meta-model(s) saved successfully.")
    
if __name__ == "__main__":
    main()