# create_ranked_labels.py
import pandas as pd
from pathlib import Path

# --- Configuration ---
INPUT_DATASET_PATH = Path("weekly_modeling_dataset.csv")
OUTPUT_DATASET_PATH = Path("ranked_modeling_dataset.csv")

def get_rank_label(row):
    """
    Applies a positional rank tier label (e.g., WR1, RB2) based on the player's rank for that week.
    """
    position = row['position']
    rank = row['position_rank']
    
    if position == 'QB':
        if rank <= 5: return 'Top 5 QB'
        if rank <= 12: return 'QB1'
        return 'Bench QB'
    
    elif position in ['RB', 'WR']:
        if rank <= 6: return 'Top 6'
        if rank <= 12: return f'{position}1' # RB1 or WR1
        if rank <= 24: return f'{position}2' # RB2 or WR2
        if rank <= 36: return 'Flex Worthy'
        return f'Bench {position}'
        
    elif position == 'TE':
        if rank <= 3: return 'Top 3 TE'
        if rank <= 6: return 'TE1'
        if rank <= 12: return 'Flex Worthy'
        return 'Bench TE'
    
    else:
        return 'Other'

# --- Main Execution ---
print(f"Step 1: Loading dataset from '{INPUT_DATASET_PATH}'...")
try:
    df = pd.read_csv(INPUT_DATASET_PATH)
except FileNotFoundError:
    print(f"Error: The file '{INPUT_DATASET_PATH}' was not found.")
    print("Please make sure you have successfully run 'build_modeling_dataset.py' first.")
    exit()

# --- Step 2: Calculate Positional Ranks for Each Week ---
print("Step 2: Calculating weekly positional ranks...")
# This is the key step: Group by game, then rank players within their position group
df['position_rank'] = df.groupby(['season', 'week', 'position'])['y_fantasy_points_ppr'] \
                         .rank(method='first', ascending=False)

# --- Step 3: Apply Rank Tier Labels ---
print("Step 3: Applying positional rank tier labels...")
df['rank_label'] = df.apply(get_rank_label, axis=1)

print("\n--- Label Distribution ---")
print(df['rank_label'].value_counts().sort_index())
print("--------------------------\n")

# --- Step 4: Save the Final Ranked and Labeled Dataset ---
df.to_csv(OUTPUT_DATASET_PATH, index=False)
print(f"Successfully created ranked and labeled dataset: '{OUTPUT_DATASET_PATH}' ✅")