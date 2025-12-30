import pandas as pd
import xgboost as xgb
import joblib
import json
from sklearn.metrics import mean_absolute_error, r2_score
from pathlib import Path
import numpy as np

# --- Configuration ---
# We test on 2024 (Complete Season) to get a firm accuracy number.
# You can change this to 2025 to test the current partial season.
VALIDATION_SEASON = 2025 

DATA_DIR = Path("../dataPrep")
MODEL_DIR = Path("./models")

# Map positions to their specific files
POSITION_CONFIG = {
    'QB': {
        'data': "timeseries_training_data_QB_avg.csv",
        'model': "xgboost_QB_sliding_window_deviation_v1.joblib",
        'feats': "feature_names_QB_sliding_window_deviation_v1.json"
    },
    'RB': {
        'data': "timeseries_training_data_RB_avg.csv", # Or _avg.csv if you used that
        'model': "xgboost_RB_sliding_window_deviation_v1.joblib",
        'feats': "feature_names_RB_sliding_window_deviation_v1.json"
    },
    'WR': {
        'data': "timeseries_training_data_WR_avg.csv",
        'model': "xgboost_WR_sliding_window_deviation_v1.joblib",
        'feats': "feature_names_WR_sliding_window_deviation_v1.json"
    },
    'TE': {
        'data': "timeseries_training_data_TE_avg.csv",
        'model': "xgboost_TE_sliding_window_deviation_v1.joblib",
        'feats': "feature_names_TE_sliding_window_deviation_v1.json"
    }
}

def evaluate_position(pos, config):
    print(f"\n" + "="*60)
    print(f"🏈 EVALUATING POSITION: {pos}")
    print("="*60)

    # 1. Setup Paths
    data_path = DATA_DIR / config['data']
    model_path = MODEL_DIR / config['model']
    feat_path = MODEL_DIR / config['feats']

    # 2. Load Model & Features
    try:
        model = joblib.load(model_path)
        with open(feat_path, 'r') as f:
            feature_names = json.load(f)
        print(f"✅ Model loaded: {model_path.name}")
        print(f"✅ Features loaded: {len(feature_names)} features")
    except Exception as e:
        print(f"❌ Error loading model/features: {e}")
        return

    # 3. Load Data
    try:
        df = pd.read_csv(data_path)
        # Filter for Validation Season
        df_val = df[df['season'] == VALIDATION_SEASON].copy()
        
        if df_val.empty:
            print(f"❌ No data found for Season {VALIDATION_SEASON}"); return
            
        print(f"✅ Data loaded: {len(df_val)} rows for {VALIDATION_SEASON}")
    except Exception as e:
        print(f"❌ Error loading data: {e}"); return

    # 4. Prepare Features
    # Ensure all expected features exist (fill missing with 0)
    for col in feature_names:
        if col not in df_val.columns:
            df_val[col] = 0.0
            
    # Select Features for Model
    X_val = df_val[feature_names]
    
    # Select Baseline & Actuals for Reconstruction
    # (These are NOT passed to the model, but used for math)
    if 'player_season_avg_points' not in df_val.columns:
        print("❌ Critical Error: 'player_season_avg_points' column missing. Cannot reconstruct predictions.")
        return
        
    baseline = df_val['player_season_avg_points'].values
    actuals = df_val['y_fantasy_points_ppr'].values

    # 5. Run Prediction (Deviation)
    # The model predicts: How much better/worse than average will they be?
    pred_deviation = model.predict(X_val)

    # 6. Reconstruct Final Prediction
    # Formula: Final = Average + Deviation
    final_predictions = baseline + pred_deviation
    
    # Optional: Clip predictions so they aren't negative (unless you allow negative points)
    final_predictions = np.maximum(final_predictions, 0)

    # 7. Metrics
    mae = mean_absolute_error(actuals, final_predictions)
    r2 = r2_score(actuals, final_predictions)

    print(f"\n📊 {VALIDATION_SEASON} PERFORMANCE METRICS:")
    print(f"✅ True MAE: {mae:.4f} (Avg Error in Points)")
    print(f"✅ True R²:  {r2:.4f}  (Correlation)")

    # 8. Show Examples
    results = df_val[['player_name', 'week', 'opponent']].copy()
    results['Actual'] = actuals
    results['Avg'] = baseline
    results['Pred_Dev'] = pred_deviation
    results['Final_Pred'] = final_predictions
    results['Error'] = (results['Final_Pred'] - results['Actual']).abs()

    print(f"\n🔍 Top 5 Best Predictions:")
    print(results.sort_values('Error').head(5).to_string(index=False, float_format="%.1f"))

    print(f"\n⚠️ Top 5 Worst Misses:")
    print(results.sort_values('Error', ascending=False).head(5).to_string(index=False, float_format="%.1f"))

def main():
    for pos in ['QB', 'RB', 'WR', 'TE']:
        evaluate_position(pos, POSITION_CONFIG[pos])

if __name__ == "__main__":
    main()