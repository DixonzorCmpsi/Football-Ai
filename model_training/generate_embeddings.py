# generate_embeddings.py
import pandas as pd
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration
from peft import PeftModel
from datasets import Dataset, disable_progress_bar
from tqdm import tqdm
import numpy as np

# --- 1. Configuration ---
BASE_MODEL = 't5-base'
# Path to the LoRA adapters trained in Phase 4
PEFT_MODEL_PATH = './t5_base_sequential_model_grad_accum'
# Path to the dataset we want to add embeddings TO
FEATURED_DATA_FILE = '../dataPrep/featured_dataset.csv'
# Path for the FINAL output dataset
OUTPUT_FILE = '../dataPrep/dataset_with_embeddings.csv'
SEQUENCE_LENGTH = 5 # Must match the sequence length used for training T5

# Define key stats used to build the sequences (must match finetune script)
KEY_STATS_FOR_SEQUENCE = [
    'position', 'age', 'offense_pct', 'targets', 'receptions',
    'rush_attempts', 'receiving_yards', 'rushing_yards',
    'y_fantasy_points_ppr'
]
MAX_INPUT_LENGTH = 512 # Must match the tokenizer settings during training

# --- 2. Load the Fine-Tuned Model and Tokenizer ---
print("Loading base model and tokenizer...")
tokenizer = T5Tokenizer.from_pretrained(PEFT_MODEL_PATH, legacy=False)
base_model = T5ForConditionalGeneration.from_pretrained(BASE_MODEL)

print(f"Loading LoRA adapters from '{PEFT_MODEL_PATH}'...")
model = PeftModel.from_pretrained(base_model, PEFT_MODEL_PATH)

# Move model to GPU if available
if torch.cuda.is_available():
    print("GPU found! Moving model to GPU...")
    device = torch.device("cuda")
    model.to(device)
else:
    print("No GPU found. Running on CPU.")
    device = torch.device("cpu")

model.eval() # Set model to evaluation mode (important!)
disable_progress_bar() # Disable internal datasets progress bars for cleaner tqdm output

# --- 3. Load Featured Dataset ---
print(f"Loading featured dataset from '{FEATURED_DATA_FILE}'...")
df = pd.read_csv(FEATURED_DATA_FILE)
df.sort_values(by=['player_id', 'season', 'week'], inplace=True)
print(f"Loaded dataset with {df.shape[0]} rows.")

# --- 4. Function to Generate Embeddings for Sequences ---
@torch.no_grad() # Disable gradient calculations for efficiency
def get_embeddings_batch(sequences_batch):
    """
    Takes a batch of input text sequences and returns their encoder embeddings.
    """
    inputs = ["summarize sequence: " + seq for seq in sequences_batch]
    tokenized_inputs = tokenizer(
        inputs,
        return_tensors="pt",
        max_length=MAX_INPUT_LENGTH,
        truncation=True,
        padding="max_length"
    ).to(device)

    # Get the encoder outputs
    encoder_outputs = model.encoder(
        input_ids=tokenized_inputs.input_ids,
        attention_mask=tokenized_inputs.attention_mask,
        return_dict=True
    )
    
    # Extract the last hidden state (batch_size, sequence_length, hidden_size)
    # We take the mean across the sequence length dim to get a fixed-size embedding
    embeddings = encoder_outputs.last_hidden_state.mean(dim=1) # Shape: (batch_size, hidden_size)
    
    # Move embeddings to CPU and convert to numpy for easier handling with pandas
    return embeddings.cpu().numpy()

# --- 5. Generate Embeddings for the Entire Dataset ---
print(f"Generating embeddings for {len(df)} player-weeks...")

all_embeddings = []
embedding_dim = model.config.d_model # Get the hidden size (e.g., 768 for t5-base)
embedding_batch_size = 32 # Process in batches for memory efficiency

# We need to iterate carefully to build sequences ending *before* the target week
# Store results temporarily to align them correctly later
embedding_map = {} # Key: (player_id, season, week), Value: embedding

# Use tqdm for a progress bar
for player_id, player_df in tqdm(df.groupby('player_id'), desc="Generating Embeddings"):
    if len(player_df) >= SEQUENCE_LENGTH: # Need enough history
        player_sequences = []
        target_indices = [] # Store the index of the row the embedding corresponds to

        for i in range(SEQUENCE_LENGTH -1, len(player_df)):
             # Sequence ends *before* the week we want the embedding FOR
            input_sequence_df = player_df.iloc[i - (SEQUENCE_LENGTH - 1) : i + 1] # Games 1-5 to predict week 6 context
            target_row = player_df.iloc[i] # The embedding describes the context *for* this week
            
            # --- Format the Input Text (Games 1-5) ---
            input_texts = []
            for week_idx, week_row in input_sequence_df.iterrows():
                week_stats = [f"Wk{week_row['week']}:"]
                for stat in KEY_STATS_FOR_SEQUENCE:
                    week_stats.append(f"{week_row[stat]:.2f}" if isinstance(week_row[stat], float) else f"{week_row[stat]}")
                input_texts.append(" ".join(week_stats))
            input_str = " | ".join(input_texts)
            player_sequences.append(input_str)
            target_indices.append((target_row['player_id'], target_row['season'], target_row['week']))

        # Process sequences in batches
        for j in range(0, len(player_sequences), embedding_batch_size):
            batch_sequences = player_sequences[j : j + embedding_batch_size]
            batch_indices = target_indices[j : j + embedding_batch_size]
            
            batch_embeddings = get_embeddings_batch(batch_sequences)
            
            # Store embeddings in the map
            for idx_tuple, embedding in zip(batch_indices, batch_embeddings):
                embedding_map[idx_tuple] = embedding

# --- 6. Add Embeddings to the DataFrame ---
print("\nAdding embeddings to the main DataFrame...")

# Create new embedding columns initialized with NaNs
embedding_cols = [f"embedding_{i}" for i in range(embedding_dim)]
for col in embedding_cols:
    df[col] = np.nan

# Populate the columns using the map
# This is faster than iterating through the DataFrame again
df_index_tuples = list(zip(df['player_id'], df['season'], df['week']))
embedding_array = np.array([embedding_map.get(idx, [np.nan]*embedding_dim) for idx in df_index_tuples])

df[embedding_cols] = embedding_array

# --- 7. Final Check and Save ---
# Drop rows where we couldn't generate an embedding (e.g., first few games of career)
df.dropna(subset=[embedding_cols[0]], inplace=True)
print(f"Final dataset shape after adding embeddings: {df.shape}")

df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Successfully created the final dataset with embeddings at '{OUTPUT_FILE}'")