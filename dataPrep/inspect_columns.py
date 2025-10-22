# inspect_columns.py
import pandas as pd
from pathlib import Path
import json # Import json for cleaner output formatting

# --- Configuration ---
# Assuming the script is in dataPrep, and the file is also in dataPrep.
FILE_TO_INSPECT = Path("featured_dataset.csv")

# --- Main Execution ---
print(f"--- Inspecting Columns from: {FILE_TO_INSPECT} ---")

try:
    # Read just enough to get headers reliably
    df = pd.read_csv(FILE_TO_INSPECT, nrows=5) # Read a few rows just in case

    # Get the list of all column names IN ORDER
    all_columns = df.columns.tolist() # Ensure it's a standard Python list

    print(f"\nFound {len(all_columns)} total columns.")
    print("\n--- Column List (JSON Format) ---")

    # Print the list formatted as a JSON array string
    # indent=2 makes it easy to read and copy
    print(json.dumps(all_columns, indent=2))

    print("\n--- End Column List ---")
    print("Copy the list above (including brackets []) and paste it into feature_names.json")


except FileNotFoundError:
    print(f"\n!!! ERROR: File not found at '{FILE_TO_INSPECT}'.")
    print("Please make sure the file path is correct and the file exists.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")