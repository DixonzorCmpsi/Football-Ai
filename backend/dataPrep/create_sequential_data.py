# create_sequential_data.py
import pandas as pd
from tqdm import tqdm

def create_sequences(input_path, output_path, sequence_length=5):
    """
    Transforms the tabular dataset into sequences of games for transformer training.
    """
    print(f"--- Phase 4: Creating Storybooks from {input_path} ---")
    
    df = pd.read_csv(input_path)
    print(f"Loaded dataset with {df.shape[0]} rows.")
    
    # Sort data chronologically for each player
    df.sort_values(by=['player_id', 'season', 'week'], inplace=True)
    
    # Define key stats to include in the sequence summary for each game
    # We select core performance and opportunity metrics
    key_stats = [
        'position', 'age', 'offense_pct', 'targets', 'receptions', 
        'rush_attempts', 'receiving_yards', 'rushing_yards', 
        'y_fantasy_points_ppr' # Include the actual score as a key feature
    ]
    
    sequences = []
    # Use tqdm for a progress bar as this can take a moment
    for player_id, player_df in tqdm(df.groupby('player_id'), desc="Processing Players"):
        # Ensure we have enough games for at least one sequence
        if len(player_df) > sequence_length:
            # Iterate through the player's games to create sliding windows
            for i in range(len(player_df) - sequence_length):
                input_sequence_df = player_df.iloc[i : i + sequence_length]
                target_sequence_df = player_df.iloc[i + sequence_length]
                
                # --- Format the Input Text (Games 1-5) ---
                input_texts = []
                for week_idx, week_row in input_sequence_df.iterrows():
                    week_stats = [f"Wk{week_row['week']}:"]
                    for stat in key_stats:
                        week_stats.append(f"{stat}={week_row[stat]:.2f}" if isinstance(week_row[stat], float) else f"{stat}={week_row[stat]}")
                    input_texts.append(" ".join(week_stats))
                input_str = " | ".join(input_texts)
                
                # --- Format the Target Text (Game 6) ---
                target_stats = []
                # Target will predict a subset of key outcome stats
                target_key_stats = ['offense_pct', 'targets', 'receptions', 'rush_attempts', 'receiving_yards', 'rushing_yards', 'y_fantasy_points_ppr']
                for stat in target_key_stats:
                     target_stats.append(f"{stat}={target_sequence_df[stat]:.2f}" if isinstance(target_sequence_df[stat], float) else f"{stat}={target_sequence_df[stat]}")
                target_str = " ".join(target_stats)
                
                sequences.append({'input_text': input_str, 'target_text': target_str})

    # Create the final DataFrame
    sequence_df = pd.DataFrame(sequences)
    
    # --- Save the sequential dataset ---
    sequence_df.to_csv(output_path, index=False)
    print(f"\n✅ Successfully created {len(sequence_df)} storybooks at '{output_path}'")

if __name__ == "__main__":
    # Use the output from feature engineering
    input_file = 'featured_dataset.csv' 
    output_file = 'transformer_sequences.csv'
    create_sequences(input_file, output_file)