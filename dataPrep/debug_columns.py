# debug_columns.py (updated for enrichment script)
import nflreadpy as nfl
import pandas as pd

# We only need one season to check the columns
SEASONS = [2023]

print("--- Loading SNAP COUNT data to inspect columns ---")
try:
    df_snaps = nfl.load_snap_counts(seasons=SEASONS).to_pandas()
    print("\n>>> Available columns in the SNAP COUNT data:")
    print(list(df_snaps.columns))
except Exception as e:
    print(f"Failed to load snap count data: {e}")


print("\n" + "="*50 + "\n") # Separator


print("--- Loading SCHEDULE data to inspect columns ---")
try:
    df_schedule = nfl.load_schedules(seasons=SEASONS).to_pandas()
    print("\n>>> Available columns in the SCHEDULE data:")
    print(list(df_schedule.columns))
except Exception as e:
    print(f"Failed to load schedule data: {e}")