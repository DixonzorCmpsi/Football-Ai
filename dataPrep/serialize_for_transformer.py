# serialize_for_transformer.py
import pandas as pd
from pathlib import Path

# --- Configuration ---
INPUT_PATH = Path("ranked_modeling_dataset.csv")
OUTPUT_PATH = Path("transformer_training_data.csv")


# --- Main Execution ---
print(f"Step 1: Loading ranked and labeled dataset from '{INPUT_PATH}'...")
try:
    df = pd.read_csv(INPUT_PATH)
except FileNotFoundError:
    print(f"Error: The file '{INPUT_PATH}' was not found.")
    print("Please make sure you have successfully run 'create_ranked_labels.py' first.")
    exit()


# --- Step 2: Convert Rows into Prompt-Completion Pairs ---
print("Step 2: Converting data into prompt-completion format...")
records = []
for index, row in df.iterrows():
    # The "prompt" is the detailed question we ask the model
    prompt = (
        f"Predict the fantasy football performance tier for a player with these stats:\n"
        f"- Position: {row['position']}\n"
        f"- Age: {int(row['age'])}\n"
        f"- Years Experience: {int(row['years_exp'])}\n"
        f"- Opponent: {row['opponent']}\n"
        f"- 4-Week Avg Fantasy Points: {row['rolling_avg_fantasy_points_ppr_4_weeks']:.2f}\n"
        f"- 4-Week Avg Targets: {row['rolling_avg_targets_4_weeks']:.2f}\n"
        f"- Opponent's Avg Passing Yards Allowed: {row['rolling_avg_passing_yards_allowed_4_weeks']:.2f}\n"
        f"What is their positional rank tier?"
    )
    
    # The "completion" is the single, correct answer the model should learn
    completion = row['rank_label']
    
    records.append({'prompt': prompt, 'completion': completion})

# Create the final DataFrame from our list of records
text_df = pd.DataFrame(records)


# --- Step 3: Save the Final Dataset ---
text_df.to_csv(OUTPUT_PATH, index=False)
print(f"\nSuccessfully created transformer-ready dataset: '{OUTPUT_PATH}' ✅")
print(f"This file contains {len(text_df)} prompt-completion pairs ready for fine-tuning.")