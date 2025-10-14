# list_columns.py
import pandas as pd
from pathlib import Path

# --- Configuration ---
# Make sure this points to the latest dataset you created
INPUT_DATASET_PATH = Path("point_range_labeled_dataset.csv")

# --- Main Execution ---
print(f"--- Reading columns from: {INPUT_DATASET_PATH} ---")

try:
    # We only need to read the first row to get the headers, which is very fast.
    df = pd.read_csv(INPUT_DATASET_PATH, nrows=1)
    
    # Get the list of all column names
    all_columns = list(df.columns)
    
    print(f"Found {len(all_columns)} total columns.")
    print("\n--- Complete Column List ---")
    
    # Print the list for easy copying
    print(all_columns)

except FileNotFoundError:
    print(f"\n!!! ERROR: File not found at '{INPUT_DATASET_PATH}'.")
    print("Please make sure you have successfully run 'create_point_range_labels.py' first.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")