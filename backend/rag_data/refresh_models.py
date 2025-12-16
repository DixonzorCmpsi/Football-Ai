# rag_data/refresh_models.py
import joblib
import os
import sys
import xgboost as xgb
from dotenv import load_dotenv

# Load env to ensure paths align (optional depending on setup)
load_dotenv()

# --- FIX: Correct Path Logic ---
# 1. Get the directory of this script (rag_data)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 2. Go up one level to the project root (backend)
project_root = os.path.dirname(current_dir)
# 3. Construct path to model_training/models
MODEL_DIR = os.path.join(project_root, 'model_training', 'models')

# List of your specific model filenames
model_files = [
    'xgboost_QB_sliding_window_v1(TimeSeries).joblib',
    'xgboost_RB_sliding_window_v1(TimeSeries).joblib',
    'xgboost_WR_sliding_window_v1(TimeSeries).joblib',
    'xgboost_TE_sliding_window_v1(TimeSeries).joblib',
    'xgboost_META_model_v1.joblib'
]

def refresh_models():
    print(f"--- Refreshing XGBoost Models ---")
    print(f"Target Directory: {MODEL_DIR}")
    
    if not os.path.exists(MODEL_DIR):
        print(f"❌ Error: Model directory does not exist at {MODEL_DIR}")
        return

    for filename in model_files:
        file_path = os.path.join(MODEL_DIR, filename)
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {filename}")
            continue
            
        try:
            print(f"🔄 Loading {filename}...")
            # This step triggers the warning (loading old format)
            model = joblib.load(file_path)
            
            print(f"💾 Re-saving {filename}...")
            # This step fixes the warning (saving in new format)
            joblib.dump(model, file_path)
            print(f"✅ Successfully updated {filename}")
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

if __name__ == "__main__":
    refresh_models()