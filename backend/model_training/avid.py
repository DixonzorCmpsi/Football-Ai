import pandas as pd
from pathlib import Path

# Define the directory where your CSVs are
DATA_DIR = Path("../dataPrep")

# List all possible filenames (both _avg and regular versions)
files_to_check = [
    "timeseries_training_data_QB.csv",
    "timeseries_training_data_QB_avg.csv",
    "timeseries_training_data_RB.csv",
    "timeseries_training_data_RB_avg.csv",
    "timeseries_training_data_WR.csv",
    "timeseries_training_data_WR_avg.csv",
    "timeseries_training_data_TE.csv",
    "timeseries_training_data_TE_avg.csv"
]

print(f"--- Checking Data Availability in {DATA_DIR} ---")

for filename in files_to_check:
    file_path = DATA_DIR / filename
    
    if file_path.exists():
        try:
            df = pd.read_csv(file_path, usecols=['season', 'week'])
            latest_season = df['season'].max()
            # Get max week for the latest season
            latest_week = df[df['season'] == latest_season]['week'].max()
            
            print(f"\n✅ FOUND: {filename}")
            print(f"   Latest Data: Season {latest_season}, Week {latest_week}")
            print(f"   Total Rows: {len(df)}")
            
            # Check specifically for 2025 Week 16
            has_target = not df[(df['season'] == 2025) & (df['week'] == 16)].empty
            print(f"   Contains 2025 Week 16? {'YES' if has_target else 'NO'}")
            
        except Exception as e:
            print(f"\n❌ ERROR reading {filename}: {e}")
    else:
        # Don't print "Missing" for every file, just the ones that exist matter
        pass

print("\n------------------------------------------------")