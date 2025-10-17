# finetune_lora_base.py (v3.2 - Gradient Accumulation)
import transformers
import pandas as pd
from datasets import Dataset
from transformers import T5Tokenizer, T5ForConditionalGeneration, TrainingArguments, Trainer
from peft import get_peft_model, LoraConfig, TaskType

# --- 0. Diagnostics ---
print("--- DIAGNOSTICS ---")
print(f"Using Transformers Version: {transformers.__version__}")
print("---------------------")

# --- 1. Configuration ---
MODEL_NAME = 't5-base'
DATA_FILE = '../dataPrep/transformer_training_data_v2.csv'
# MODIFIED: New output directory for our gradient accumulation experiment
OUTPUT_DIR = './fantasy-football-t5-base-lora-model-v3.2'
LOGGING_DIR = './logs_v3.2'
NUM_EPOCHS = 8
# This is the size of the mini-batch that fits on your GPU
BATCH_SIZE = 8
TEST_SET_SIZE = 0.1

# --- 2. Load and Split the Dataset ---
print(f"Loading data from '{DATA_FILE}'...")
dataset = Dataset.from_csv(DATA_FILE)
split_dataset = dataset.train_test_split(test_size=TEST_SET_SIZE, seed=42)

# --- 3. Tokenize the Datasets ---
print("Loading tokenizer and tokenizing datasets...")
tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME, legacy=False)

def tokenize_function(examples):
    inputs = ["predict fantasy football tier: " + doc for doc in examples['prompt']]
    model_inputs = tokenizer(inputs, max_length=512, truncation=True, padding='max_length')
    labels = tokenizer(text_target=examples["completion"], max_length=32, truncation=True, padding='max_length')
    model_inputs["labels"] = labels["input_ids"]
    return model_inputs

tokenized_datasets = split_dataset.map(tokenize_function, batched=True)

# --- 4. Load Base Model and Apply LoRA ---
print(f"Loading pre-trained {MODEL_NAME} model...")
model = T5ForConditionalGeneration.from_pretrained(MODEL_NAME)

lora_config = LoraConfig(
    r=16,
    lora_alpha=32,
    lora_dropout=0.1,
    bias="none",
    task_type=TaskType.SEQ_2_SEQ_LM
)

model = get_peft_model(model, lora_config)
print("\nModel configured with LoRA adapters:")
model.print_trainable_parameters()

# --- 5. Configure Training Arguments ---
print("\nConfiguring training arguments...")
training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    num_train_epochs=NUM_EPOCHS,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    # MODIFIED: Add gradient accumulation
    gradient_accumulation_steps=2,  # Effective batch size will be 8 * 2 = 16
    do_eval=True,
    evaluation_strategy="steps",
    eval_steps=250,
    save_steps=250,
    warmup_steps=500,
    weight_decay=0.05,
    logging_dir=LOGGING_DIR,
    logging_steps=50,
    load_best_model_at_end=True,
    report_to="none"
)

# --- 6. Create and Run the Trainer ---
print("Initializing Trainer...")
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_datasets['train'],
    eval_dataset=tokenized_datasets['test'],
)

print(f"Starting hyperparameter tuning run (V3.2) on {MODEL_NAME} with effective batch size of 16...")
trainer.train()

# --- 7. Save the Final LoRA Adapters ---
print("Training complete. Saving the best performing model.")
trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)

print(f"\nTuned LoRA model adapters successfully saved to '{OUTPUT_DIR}' ✅")