# inspect_assembled_data.py
import pandas as pd
from pathlib import Path

# --- Configuration ---
# The script is in dataPrep, and the file is in dataPrep.
FILE_TO_INSPECT = Path("featured_dataset.csv")

# --- Main Execution ---
print(f"--- Inspecting Columns from: {FILE_TO_INSPECT} ---")

try:
    # We only need to read the first row to get the headers.
    df = pd.read_csv(FILE_TO_INSPECT, nrows=1)
    
    # Get the list of all column names
    all_columns = list(df.columns)
    
    print(f"Found {len(all_columns)} total columns in the assembled file.")
    print("\n--- Complete Column List ---")
    
    # Print the list for easy copying and analysis
    print(all_columns)

except FileNotFoundError:
    print(f"\n!!! ERROR: File not found at '{FILE_TO_INSPECT}'.")
    print("Please make sure you have successfully run 'build_modeling_dataset.py' first.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")