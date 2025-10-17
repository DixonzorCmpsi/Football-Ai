# use_model.py (v2 - Final Version)
import pandas as pd
from datasets import Dataset
from transformers import T5Tokenizer, T5ForConditionalGeneration
from peft import PeftModel
import torch # Import torch to check for GPU

# --- 1. Configuration ---
BASE_MODEL = 't5-base'
# MODIFIED: Point to the new V2 model directory
PEFT_MODEL_PATH = './fantasy-football-t5-base-lora-model-v3.1'
# MODIFIED: Point to the new V2 data file to load the correct test set
DATA_FILE = '../dataPrep/transformer_training_data_v2.csv'
TEST_SET_SIZE = 0.1 # Must be the same value used during training

# --- 2. Load the Fine-Tuned V2 Model ---
print("Loading base model and tokenizer...")
tokenizer = T5Tokenizer.from_pretrained(PEFT_MODEL_PATH, legacy=False)
base_model = T5ForConditionalGeneration.from_pretrained(BASE_MODEL)

print(f"Loading LoRA adapters from '{PEFT_MODEL_PATH}'...")
model = PeftModel.from_pretrained(base_model, PEFT_MODEL_PATH)

# NEW: Automatically use GPU if available
if torch.cuda.is_available():
    print("GPU found! Moving model to GPU...")
    device = torch.device("cuda")
    model.to(device)
else:
    print("No GPU found. Running on CPU.")
    device = torch.device("cpu")

model.eval() # Set the model to evaluation mode

# --- 3. Function to Make a Prediction ---
def predict(prompt_text):
    """Generates a prediction for a given text prompt."""
    print("\n--- Generating Prediction ---")
    print(f"Input Prompt:\n{prompt_text}")
    
    input_text = "predict fantasy football tier: " + prompt_text
    
    # MODIFIED: Increased max_length to match training
    inputs = tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True).to(device)

    # Generate the output from the model
    with torch.no_grad(): # Disable gradient calculations for faster inference
        outputs = model.generate(input_ids=inputs["input_ids"], max_new_tokens=10)
    
    prediction = tokenizer.decode(outputs[0], skip_special_tokens=True)
    print(f"\nModel Prediction: {prediction}")
    return prediction

# --- 4. Example Prediction with V2 Prompt ---
# MODIFIED: The sample prompt now uses the detailed, position-specific format
sample_prompt = (
    "Predict the fantasy football performance tier for a player with these stats:\n"
    "## Player & Game Context\n"
    "- Position: WR\n"
    "- Age: 25\n"
    "- Years Experience: 4\n"
    "- Opponent: KC\n"
    "## Opponent Matchup Profile\n"
    "- Avg Fantasy Points Allowed to WRs (4-wk): 28.50\n"
    "- Avg Total Points Allowed (4-wk): 24.00\n"
    "- Avg Sacks per Game (4-wk): 3.50\n"
    "- Avg Interceptions per Game (4-wk): 1.25\n"
    "- Avg Pass Yards Allowed (4-wk): 265.75\n"
    "## WR Performance Indicators\n"
    "- Targets (Last Game): 12\n"
    "- Receptions (Last Game): 8\n"
    "- Receiving Yards (Last Game): 110\n"
    "- Team Target Share (Season): 28.5%\n"
    "- Team Receiving Yards Share (Season): 32.0%\n"
    "- Offense Snap % (Last Game): 95.0%\n"
    "- Average Depth of Target (Last Game): 14.20\n"
    "- Career Avg Receptions per Game: 5.50\n"
    "- 4-Week Avg Fantasy Points: 22.30\n\n"
    "What is their performance tier?"
)
predict(sample_prompt)

# --- 5. Evaluate Accuracy on the V2 Test Set ---
print("\n--- Evaluating V3.1 Model Accuracy on Test Set ---")
dataset = Dataset.from_csv(DATA_FILE)
split_dataset = dataset.train_test_split(test_size=TEST_SET_SIZE, seed=42)
test_set = split_dataset['test']

true_labels = test_set['completion']
prompts = test_set['prompt']

print(f"Making predictions on {len(prompts)} test examples...")
predictions = []
for prompt in prompts:
    input_text = "predict fantasy football tier: " + prompt
    inputs = tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True).to(device)
    with torch.no_grad():
        outputs = model.generate(input_ids=inputs["input_ids"], max_new_tokens=10)
    pred_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    predictions.append(pred_text)

# Calculate accuracy
correct_predictions = sum(1 for true, pred in zip(true_labels, predictions) if true.strip() == pred.strip())
accuracy = (correct_predictions / len(true_labels)) * 100
print(f"\nModel V2 Accuracy on the test set: {accuracy:.2f}%")