# finetune_t5_base_for_embeddings.py (v3.2 - Grad Accumulation)
import transformers
import pandas as pd
from datasets import Dataset
from transformers import T5Tokenizer, T5ForConditionalGeneration, TrainingArguments, Trainer
from peft import get_peft_model, LoraConfig, TaskType
import torch

# --- 0. Diagnostics ---
print("--- DIAGNOSTICS ---")
print(f"Using Transformers Version: {transformers.__version__}")
print(f"GPU Available: {torch.cuda.is_available()}")
print("---------------------")

# --- 1. Configuration ---
MODEL_NAME = 't5-base'
DATA_FILE = '../dataPrep/transformer_sequences.csv'
# MODIFIED: New output directory for this experiment
OUTPUT_DIR = './t5_base_sequential_model_grad_accum'
LOGGING_DIR = './logs_t5_base_sequential_grad_accum'
NUM_EPOCHS = 5
# Keep the per-device batch size that fits on your GPU
BATCH_SIZE = 8
# Set the number of steps to accumulate gradients over
GRAD_ACCUM_STEPS = 2
EFFECTIVE_BATCH_SIZE = BATCH_SIZE * GRAD_ACCUM_STEPS # This will be 16
TEST_SET_SIZE = 0.1

# --- 2. Load and Split the Sequential Dataset ---
print(f"Loading sequential data from '{DATA_FILE}'...")
dataset = Dataset.from_csv(DATA_FILE)
print(f"Splitting data into {1-TEST_SET_SIZE:.0%} train and {TEST_SET_SIZE:.0%} test sets...")
split_dataset = dataset.train_test_split(test_size=TEST_SET_SIZE, seed=42)

# --- 3. Tokenize the Datasets ---
print("Loading tokenizer and tokenizing datasets...")
tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME, legacy=False)
MAX_INPUT_LENGTH = 512
MAX_TARGET_LENGTH = 128

def tokenize_function(examples):
    inputs = ["summarize sequence: " + doc for doc in examples['input_text']]
    model_inputs = tokenizer(inputs, max_length=MAX_INPUT_LENGTH, truncation=True, padding='max_length')
    labels = tokenizer(text_target=examples["target_text"], max_length=MAX_TARGET_LENGTH, truncation=True, padding='max_length')
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
    # ADDED: Gradient Accumulation
    gradient_accumulation_steps=GRAD_ACCUM_STEPS,
    # Adjust evaluation/saving steps based on accumulation
    # Total steps per epoch = (num_train_examples / EFFECTIVE_BATCH_SIZE)
    # Let's evaluate roughly twice per epoch as a starting point
    # num_train_examples = len(tokenized_datasets['train'])
    # steps_per_epoch = num_train_examples // EFFECTIVE_BATCH_SIZE
    # eval_save_steps = max(100, steps_per_epoch // 2) # Evaluate at least every 100 steps, or twice per epoch
    eval_strategy="epoch", # Easier to manage evaluation with accumulation
    save_strategy="epoch",
    learning_rate=5e-5,
    warmup_steps=500,
    weight_decay=0.01,
    logging_dir=LOGGING_DIR,
    logging_steps=50, # Log more frequently to see progress within accumulation steps
    load_best_model_at_end=True,
    metric_for_best_model="loss",
    greater_is_better=False,
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

print(f"Starting {MODEL_NAME} fine-tuning (LoRA) for sequence understanding with effective batch size {EFFECTIVE_BATCH_SIZE}...")
trainer.train()

# --- 7. Save the Fine-tuned Model (LoRA Adapters) ---
print("Training complete. Saving the best performing LoRA adapters.")
trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)

print(f"\n{MODEL_NAME} LoRA adapters saved to '{OUTPUT_DIR}' ✅")