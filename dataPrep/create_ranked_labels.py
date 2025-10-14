# create_point_range_labels.py (v2 - Distinct Labels)
import pandas as pd
from pathlib import Path

# --- Configuration ---
INPUT_DATASET_PATH = Path("weekly_modeling_dataset.csv")
OUTPUT_DATASET_PATH = Path("point_range_labeled_dataset.csv")

def get_point_range_label(row):
    """
    Applies a categorical label based on position-specific fantasy point ranges.
    """
    position = row['position']
    score = row['y_fantasy_points_ppr']
    
    if position == 'QB':
        if score >= 25: return 'Elite QB'
        if score >= 18: return 'Good QB'
        if score >= 10: return 'Average QB'
        return 'Bust QB'
            
    # MODIFIED: Added distinct labels for RB and WR
    elif position == 'RB':
        if score >= 20: return 'Elite RB'
        if score >= 15: return 'Good RB'
        if score >= 10: return 'Flex RB'
        if score >= 5: return 'Bench RB'
        return 'Bust RB'

    elif position == 'WR':
        if score >= 20: return 'Elite WR'
        if score >= 15: return 'Good WR'
        if score >= 10: return 'Flex WR'
        if score >= 5: return 'Bench WR'
        return 'Bust WR'

    elif position == 'TE':
        if score >= 15: return 'Elite TE'
        if score >= 10: return 'Good TE'
        if score >= 6: return 'Average TE'
        return 'Bust TE'
            
    else:
        return 'Other'

# --- Main Execution ---
print(f"Loading dataset from '{INPUT_DATASET_PATH}'...")
df = pd.read_csv(INPUT_DATASET_PATH)

print("Applying distinct, position-aware point range labels...")
df['point_range_label'] = df.apply(get_point_range_label, axis=1)

print("\n--- New Label Distribution ---")
print(df['point_range_label'].value_counts().sort_index())
print("--------------------------\n")

df.to_csv(OUTPUT_DATASET_PATH, index=False)
print(f"Successfully created point-range labeled dataset: '{OUTPUT_DATASET_PATH}' ✅")