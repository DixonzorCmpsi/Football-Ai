import pandas as pd
import joblib
import json
import numpy as np
from pathlib import Path
import sys

# Import the main loading function and config
# This ensures we rely on the exact same logic as the inference script
from run_live_inference_2025 import load_and_prep_2025_data, POS_CONFIG, MODEL_DIR, TEST_WEEK

def debug_player(player_name):
    print(f"--- DEBUGGING: {player_name} ---")
    print(f"Target Week: {TEST_WEEK}")
    
    # 1. Load Data (This runs the full prep pipeline)
    print("Loading and prepping data...")
    df = load_and_prep_2025_data()
    
    # 2. Find Player
    # Case insensitive search
    player = df[df['player_name'].str.contains(player_name, case=False, na=False)]
    
    if player.empty:
        print(f"❌ Player '{player_name}' not found in Week {TEST_WEEK} data.")
        return

    row = player.iloc[0]
    pos = row['position']
    print(f"Found: {row['player_name']} ({pos}) - Team: {row.get('team', 'N/A')} vs {row.get('opponent', 'N/A')}")
    
    # 3. Load Model Features
    if pos not in POS_CONFIG:
        print(f"❌ Position {pos} not found in config.")
        return
        
    config = POS_CONFIG[pos]
    with open(MODEL_DIR / config['feats'], 'r') as f:
        feature_names = json.load(f)
        
    # 4. Print Key Stats & Baseline
    print(f"\n1. BASELINE & TARGETS:")
    print(f"   Actual Points (Week {TEST_WEEK}): {row.get('y_fantasy_points_ppr', 'N/A')}")
    print(f"   Calculated Season Avg:   {row.get('player_season_avg_points', 'N/A'):.4f}")
    
    # 5. DEEP DIVE: Print EVERY feature used by the model
    print(f"\n2. MODEL INPUTS (Checking {len(feature_names)} features):")
    print(f"{'Feature Name':<50} | {'Value':<15}")
    print("-" * 70)
    
    X_row = pd.DataFrame([row], columns=df.columns)
    
    missing_cols = []
    
    for feature in feature_names:
        if feature in X_row.columns:
            val = X_row.iloc[0][feature]
            # Highlight zeros in RED (visually, by marking with <--- ZERO)
            marker = " <--- ZERO" if (isinstance(val, (int, float)) and val == 0) else ""
            print(f"{feature:<50} | {val:<10.4f}{marker}")
        else:
            print(f"{feature:<50} | {'MISSING!':<10}")
            missing_cols.append(feature)

    if missing_cols:
        print(f"\n❌ CRITICAL: The following features are MISSING from the dataframe:")
        print(missing_cols)
    else:
        print(f"\n✅ All model features are present in the dataframe.")

    # 6. Run Prediction Manually
    print(f"\n3. RUNNING PREDICTION:")
    try:
        model = joblib.load(MODEL_DIR / config['model'])
        
        # Fill missing with 0 for prediction check
        for f in feature_names:
            if f not in X_row.columns: X_row[f] = 0.0
            
        X_in = X_row[feature_names]
        pred_dev = model.predict(X_in)[0]
        baseline = row.get('player_season_avg_points', 0)
        final = baseline + pred_dev
        
        print(f"   Baseline ({baseline:.2f}) + Deviation ({pred_dev:.2f}) = Final Prediction: {final:.2f}")
    except Exception as e:
        print(f"❌ Prediction failed: {e}")

if __name__ == "__main__":
    debug_player("tyler shough")  # Change player name here