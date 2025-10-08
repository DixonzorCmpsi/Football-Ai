# FINAL create_dataset.py (v7)
import nflreadpy as nfl
import pandas as pd
import numpy as np

SEASONS = range(2015, 2025)
print("--- Starting Data Ingestion with nflreadpy ---")
print("Fetching roster and draft data...")

roster_df = nfl.load_rosters(seasons=SEASONS).to_pandas()
draft_df = nfl.load_draft_picks(seasons=SEASONS).to_pandas()

# Add 'pfr_id' to the list of columns to keep
players_static_df = roster_df[[
    'gsis_id', 'pfr_id', 'full_name', 'position', 'height', 'weight', 'birth_date', 'college'
]].rename(columns={
    'gsis_id': 'player_id',
    'full_name': 'player_name',
    'birth_date': 'birthdate'
}).drop_duplicates(subset=['player_id']).reset_index(drop=True)

draft_info_df = draft_df[[
    'gsis_id', 'season', 'round', 'pick'
]].rename(columns={
    'gsis_id': 'player_id',
    'season': 'draft_year',
    'round': 'draft_round',
    'pick': 'draft_pick'
}).drop_duplicates(subset=['player_id']).reset_index(drop=True)

players_static_df = pd.merge(
    players_static_df,
    draft_info_df,
    on='player_id',
    how='left'
)

players_static_df['draft_round'] = players_static_df['draft_round'].fillna(8)
players_static_df['draft_pick'] = players_static_df['draft_pick'].fillna(260)

print(f"Created static player table with {len(players_static_df)} players.")
# The static file now contains the 'pfr_id' needed for linking
players_static_df.to_csv('players_static.csv', index=False)
print("Saved players_static.csv")


print("\nFetching weekly offensive stats...")
weekly_df = nfl.load_player_stats(seasons=SEASONS).to_pandas()
offensive_positions = ['QB', 'RB', 'WR', 'TE', 'FB']
weekly_offense_df = weekly_df[weekly_df['position'].isin(offensive_positions)].copy()
weekly_offense_df.rename(columns={'player_display_name': 'player_name'}, inplace=True)
final_weekly_cols = [
    'player_id', 'player_name', 'position', 'recent_team', 'season', 'week', 'passing_yards',
    'passing_tds', 'interceptions', 'rushing_yards', 'rushing_tds', 'receptions',
    'receiving_yards', 'receiving_tds', 'fumbles_lost', 'fantasy_points_ppr'
]
final_weekly_cols = [col for col in final_weekly_cols if col in weekly_offense_df.columns]
nfl_weekly_stats_df = weekly_offense_df[final_weekly_cols].rename(columns={'recent_team': 'team'})

print(f"Created weekly stats table with {len(nfl_weekly_stats_df)} player-week entries.")
nfl_weekly_stats_df.to_csv('nfl_weekly_stats.csv', index=False)
print("Saved nfl_weekly_stats.csv")

print("\n--- Data Ingestion Complete ---")