# finetune_transformer.py
import pandas as pd
from datasets import Dataset
from transformers import T5Tokenizer, T5ForConditionalGeneration, TrainingArguments, Trainer

# --- 1. Configuration ---
MODEL_NAME = 't5-small'
# New path
DATA_FILE = '../dataPrep/transformer_training_data.csv'
OUTPUT_DIR = 'fantasy-football-t5-model'
NUM_EPOCHS = 3 # An epoch is one full pass through the entire training data
BATCH_SIZE = 8 # Process 8 examples at a time

# --- 2. Load and Prepare the Dataset ---
print("Loading and preparing dataset...")
# Load the CSV into a Hugging Face Dataset object
df = pd.read_csv(DATA_FILE)
dataset = Dataset.from_pandas(df)

# Load the tokenizer for the chosen model
tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME)

def tokenize_function(examples):
    """Converts the text prompts and completions into numerical tokens."""
    # The T5 model requires a prefix for text-to-text tasks.
    # We can use a simple one like "predict fantasy football tier: "
    inputs = ["predict fantasy football tier: " + doc for doc in examples['prompt']]
    
    # Tokenize the prompts (inputs)
    model_inputs = tokenizer(inputs, max_length=256, truncation=True, padding='max_length')
    
    # Tokenize the completions (labels)
    labels = tokenizer(text_target=examples["completion"], max_length=32, truncation=True, padding='max_length')
    
    # The 'labels' are the target tokens the model should learn to predict
    model_inputs["labels"] = labels["input_ids"]
    return model_inputs

# Apply the tokenization across the entire dataset
tokenized_dataset = dataset.map(tokenize_function, batched=True)

# --- 3. Load the Pre-trained Model ---
print("Loading pre-trained T5 model...")
# T5 is a conditional generation model, perfect for our prompt->completion task
model = T5ForConditionalGeneration.from_pretrained(MODEL_NAME)

# --- 4. Define Training Arguments ---
print("Configuring training arguments...")
training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    num_train_epochs=NUM_EPOCHS,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE, # Although we don't have a separate eval set here
    warmup_steps=500,
    weight_decay=0.01,
    logging_dir='./logs',
    logging_steps=100,
    report_to="none" # Disables external logging integrations like wandb
)

# --- 5. Create and Run the Trainer ---
print("Initializing Trainer...")
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_dataset,
    # We could add an evaluation_dataset here if we had split our data
)

print("Starting model fine-tuning... This will take a while.")
trainer.train()

# --- 6. Save the Fine-tuned Model ---
print("Training complete. Saving the fine-tuned model.")
trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)

print(f"\nModel successfully fine-tuned and saved to '{OUTPUT_DIR}' ✅")