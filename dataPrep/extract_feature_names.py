# dataPrep/extract_feature_names.py
import pandas as pd
import json
from pathlib import Path
import sys

# --- Configuration ---
# Point this to the dataset you are using for training
INPUT_FILE = Path("featured_dataset_QB.csv") 

# Define the output path for the JSON file
OUTPUT_JSON_PATH = Path("../model_training/models/feature_names_QB_v1.json")

# --- Feature Selection Logic (Copied from your train_xgboost.py) ---

# Define columns that are NOT features (identifiers or the target)
COLS_TO_DROP = [
    'season', 
    'week', 
    'player_id', 
    'player_name', 
    # 'position', # We will handle this one separately
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

# --- Main Function ---
def extract_and_save_features(input_path, output_path):
    """
    Loads the training data, applies feature selection and encoding,
    and saves the final list of feature names to a JSON file.
    """
    print(f"--- Extracting Feature Names from: {input_path} ---")

    try:
        # Load the dataset (only need a few rows to check columns)
        df = pd.read_csv(input_path, nrows=5)
        print(f"Loaded {len(df.columns)} total columns from source.")
        
        # 1. Identify all features (excluding identifiers and target)
        # Start with all columns, then remove the ones we don't want
        all_columns = df.columns.tolist()
        features_to_process = [
            col for col in all_columns 
            if col not in COLS_TO_DROP 
            and col not in QB_IRRELEVANT_FEATURES
        ]
        
        # 2. Separate categorical features for encoding
        categorical_features = ['position']
        
        # 3. Get the list of numerical features
        numerical_features = [
            col for col in features_to_process 
            if col not in categorical_features
        ]

        print(f"Found {len(numerical_features)} numerical features.")
        
        # 4. Simulate One-Hot Encoding to get the new column names
        # We know 'position' only has 'QB' in this file,
        # but pd.get_dummies(..., drop_first=True) on a single-value
        # column results in ZERO new columns.
        
        # We must get the *potential* categories from the *original* dataset
        # to be safe, but for a QB-only model, we know 'position'
        # is a constant and shouldn't be a feature anyway.
        
        # Your training script does this:
        # X = df[features + ['position']]
        # X = pd.get_dummies(X, columns=['position'], drop_first=True)
        
        # If the input file is *only* QBs, 'position' is a constant.
        # 'pd.get_dummies' with 'drop_first=True' will *drop* it entirely.
        
        # Let's check:
        if 'position' in numerical_features:
             numerical_features.remove('position') # It's not numeric
             
        final_feature_list = numerical_features
        
        print(f"Identified 'position' as the categorical column to be encoded.")
        print(f"After encoding, 'position' will be dropped (as it's constant 'QB').")
        print(f"Total final features for the model: {len(final_feature_list)}")

        # 5. Save the final list to JSON
        print(f"\nSaving final feature list to: {output_path}...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(final_feature_list, f, indent=2)
        
        print("\n--- Success! ---")
        print("Final feature names JSON file created.")
        print(f"Features: {final_feature_list}")

    except FileNotFoundError:
        print(f"\n!!! ERROR: Input file not found at '{input_path}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

# --- Main Execution ---
if __name__ == "__main__":
    extract_and_save_features(INPUT_FILE, OUTPUT_JSON_PATH)