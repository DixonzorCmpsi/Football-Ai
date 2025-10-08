# FINAL enrich_data.py (v3, Confirmed)
import nflreadpy as nfl
import pandas as pd

SEASONS = range(2015, 2025)
print("--- Starting Data Enrichment with nflreadpy ---")

# --- 1. Load our existing data files ---
try:
    weekly_stats_df = pd.read_csv('nfl_weekly_stats.csv')
    static_df = pd.read_csv('players_static.csv')
    print("Successfully loaded CSV files.")
except FileNotFoundError:
    print("Error: CSV file not found. Please run create_dataset.py first.")
    exit()

# --- 2. Fetch and Prepare Snap Count Data ---
print("Fetching snap count data...")
snap_counts_df = nfl.load_snap_counts(seasons=SEASONS).to_pandas()

snap_counts_df = snap_counts_df.rename(columns={
    'pfr_player_id': 'pfr_id',
    'offense_pct': 'snap_pct',
    'player': 'player_name.1' # Rename to track the duplicate column
})
snap_counts_df = snap_counts_df[['pfr_id', 'season', 'week', 'opponent', 'team', 'player_name.1', 'snap_pct']]

# --- 3. Link and Merge Data ---
id_map = static_df[['player_id', 'pfr_id']].dropna()
weekly_stats_with_pfr_id = pd.merge(weekly_stats_df, id_map, on='player_id', how='left')
enriched_df = pd.merge(weekly_stats_with_pfr_id, snap_counts_df, on=['pfr_id', 'season', 'week'], how='left')
print("Merged snap count and opponent data.")

# --- 4. Finalize and Save ---
# Use modern syntax to fill missing values and avoid warnings
enriched_df['snap_pct'] = enriched_df['snap_pct'].fillna(0)

# Clean up redundant columns created during the merge
enriched_df.drop(columns=['player_name.1', 'pfr_id'], inplace=True, errors='ignore')

print(f"Created enriched stats table with {len(enriched_df)} entries.")
enriched_df.to_csv('nfl_weekly_enriched.csv', index=False)
print("Saved nfl_weekly_enriched.csv")

print("\n--- Data Enrichment Complete ---")
# This final print statement will now work correctly
print(enriched_df[['player_name', 'season', 'week', 'team', 'opponent', 'snap_pct', 'fantasy_points_ppr']].head())