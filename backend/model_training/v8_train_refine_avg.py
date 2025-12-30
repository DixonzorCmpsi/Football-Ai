import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, r2_score
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import sys

# --- Configuration ---
DATA_DIR = Path("../dataPrep")
MODEL_DIR = Path("./models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# List of positions to train
POSITIONS = ['QB', 'RB', 'WR', 'TE']

# Feature Count Config
N_TOP_FEATURES = 40 

# --- DEVIATION STRATEGY CONFIG ---
RAW_TARGET = 'y_fantasy_points_ppr'
BASELINE_COL = 'player_season_avg_points' 
NEW_TARGET = 'target_deviation'

# 🚫 KEYWORDS TO DROP (NOISY/SPARSE DATA)
DROP_KEYWORDS = [
    'shotgun', 
    'no_huddle', 
    'redzone',
    'touchdown_redzone',
    'targets_redzone',
    'receptions_redzone' 
]

# Columns to DROP (Identifiers, Raw Target, Baseline)
COLS_TO_DROP = [
    'season', 'week', 'player_id', 'player_name', 'position',
    'team', 'opponent', 
    RAW_TARGET,      
    BASELINE_COL,    
    NEW_TARGET       
]

# 🎯 HYPERPARAMETER GRID
PARAM_GRID = {
    'n_estimators': [300, 500, 700, 1000, 1200],
    'learning_rate': [0.005, 0.01, 0.02, 0.03, 0.05],
    'max_depth': [3, 4, 5, 6, 7],
    'subsample': [0.6, 0.7, 0.8, 0.9],
    'colsample_bytree': [0.6, 0.7, 0.8, 0.9],
    'min_child_weight': [1, 3, 5, 7],
    'gamma': [0, 0.1, 0.2, 0.3]
}

# --- Plotting Functions ---
def plot_results(y_true, y_pred, title, plot_path):
    plt.figure(figsize=(12, 6))
    
    # Plot 1: Predicted vs Actual
    plt.subplot(1, 2, 1)
    plt.scatter(y_true, y_pred, alpha=0.3)
    mn, mx = min(y_true.min(), y_pred.min()), max(y_true.max(), y_pred.max())
    plt.plot([mn, mx], [mn, mx], '--', color='red', lw=2, label="Perfect Prediction")
    plt.title(f'Predicted vs. Actual ({title})')
    plt.xlabel('Actual Points')
    plt.ylabel('Predicted Points')
    plt.legend()
    plt.grid(True)

    # Plot 2: Residuals
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

def get_feature_importance(model, feature_names, output_dir, pos):
    print("\n   📊 Calculating Feature Importance...", flush=True)
    try:
        importances = model.feature_importances_
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importances
        }).sort_values(by='importance', ascending=False)
        
        # Save Plot
        plt.figure(figsize=(10, 14))
        top_features = importance_df.head(N_TOP_FEATURES)
        plt.barh(top_features['feature'], top_features['importance'])
        plt.title(f'Top {N_TOP_FEATURES} Feature Importances ({pos})')
        plt.xlabel('Importance (Gain)')
        plt.ylabel('Feature')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        
        plot_path = output_dir / f"feature_importance_{pos}.png"
        plt.savefig(plot_path)
        plt.close()
        
        return importance_df['feature'].tolist()
        
    except Exception as e:
        print(f"   ⚠️ Warning: Feature importance plot failed. {e}", flush=True)
        return feature_names

def train_position(pos):
    print(f"\n" + "="*60, flush=True)
    print(f"🏈 STARTING TRAINING SEQUENCE: {pos}", flush=True)
    print("="*60, flush=True)

    # 1. Setup Paths
    input_file = DATA_DIR / f"timeseries_training_data_{pos}_avg.csv"
    
    # Check for file existence (handle _avg suffix optionality)
    if not input_file.exists():
        fallback = DATA_DIR / f"timeseries_training_data_{pos}.csv"
        if fallback.exists():
            input_file = fallback
        else:
            print(f"❌ Critical Error: Input file for {pos} not found.", flush=True)
            return

    model_out = MODEL_DIR / f"xgboost_{pos}_sliding_window_deviation_v1.joblib"
    feats_out = MODEL_DIR / f"feature_names_{pos}_sliding_window_deviation_v1.json"
    plot_dir = MODEL_DIR / f"evaluation_plots_{pos}"
    plot_dir.mkdir(parents=True, exist_ok=True)

    # 2. Load Data
    df = pd.read_csv(input_file)
    print(f"   📄 Loaded {len(df)} rows.", flush=True)

    # 3. Create Target (Deviation)
    if BASELINE_COL not in df.columns:
        print("   ⚠️ Calculating Season Average Baseline...", flush=True)
        df[BASELINE_COL] = df.groupby('player_id')[RAW_TARGET] \
            .transform(lambda x: x.expanding().mean().shift(1)).fillna(0)
    
    df[NEW_TARGET] = df[RAW_TARGET] - df[BASELINE_COL]
    df.dropna(subset=[RAW_TARGET, NEW_TARGET], inplace=True)
    
    # 4. Filter Features
    raw_cols = [col for col in df.columns if col not in COLS_TO_DROP]
    feature_names = []
    dropped_log = []

    for col in raw_cols:
        # Drop if matches keyword
        if any(kw in col for kw in DROP_KEYWORDS):
            dropped_log.append(col)
        # Keep if numeric
        elif pd.api.types.is_numeric_dtype(df[col]):
            feature_names.append(col)
            
    print(f"   🚫 Dropped {len(dropped_log)} noisy features (shotgun, redzone, etc.)", flush=True)
    print(f"   ✅ Final Feature Count: {len(feature_names)}", flush=True)

    # 5. Prepare Training Data
    # Sort by time for sliding window
    df.sort_values(['season', 'week'], inplace=True)
    
    X = df[feature_names]
    y = df[NEW_TARGET]

    # 6. Hyperparameter Tuning (RandomizedSearch + TimeSeriesSplit)
    print("   🔍 Running Randomized Hyperparameter Search (50 Iterations)...", flush=True)
    
    xgb_reg = xgb.XGBRegressor(
        objective='reg:squarederror', 
        n_jobs=-1, 
        random_state=42
    )
    
    # 5-Split Sliding Window
    tscv = TimeSeriesSplit(n_splits=5)
    
    search = RandomizedSearchCV(
        estimator=xgb_reg,
        param_distributions=PARAM_GRID,
        n_iter=50, # UPDATED TO 50
        scoring='neg_mean_absolute_error',
        cv=tscv,
        verbose=0, # Keep it clean
        n_jobs=-1,
        random_state=42
    )
    
    search.fit(X, y)
    best_params = search.best_params_
    print(f"   🏆 Best Params: {best_params}", flush=True)
    print(f"   📉 Best CV Score (MAE): {-search.best_score_:.4f}", flush=True)

    # 7. Train Final Model & Analyze Importance
    print(f"   💪 Training Final Production Model...", flush=True)
    final_model = xgb.XGBRegressor(**best_params, random_state=42, n_jobs=-1)
    final_model.fit(X, y)
    
    # Get Feature Importance to filter top N
    sorted_features = get_feature_importance(final_model, feature_names, plot_dir, pos)
    top_features = sorted_features[:N_TOP_FEATURES]
    
    print(f"   ✂️ Selecting Top {N_TOP_FEATURES} features for final save...", flush=True)
    
    # Retrain on JUST the top features for efficiency/noise reduction
    X_final = df[top_features]
    final_model.fit(X_final, y)

    # 8. Save
    joblib.dump(final_model, model_out)
    with open(feats_out, 'w') as f:
        json.dump(top_features, f, indent=2)
        
    print(f"   💾 Saved Model: {model_out.name}", flush=True)
    print(f"   💾 Saved Feature List: {feats_out.name}", flush=True)
    print(f"   ✅ Finished {pos}", flush=True)

def main():
    print("🚀 STARTING MULTI-POSITION TRAINING PIPELINE", flush=True)
    print(f"   Positions: {POSITIONS}", flush=True)
    print(f"   Strategy: Deviation from Baseline (Sliding Window Optimized)", flush=True)
    
    for pos in POSITIONS:
        try:
            train_position(pos)
        except Exception as e:
            print(f"   ❌ CRITICAL FAILURE for {pos}: {e}", flush=True)
            import traceback
            traceback.print_exc()

    print("\n✅ ALL POSITIONS PROCESSED.", flush=True)

if __name__ == "__main__":
    main()