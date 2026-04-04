"""
Sample: Training a Prop Prediction Model with Bovada Data
This demonstrates how to use historical Bovada prop data for ML training.
"""

import polars as pl
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if not DB_CONNECTION_STRING:
    raise ValueError("DB_CONNECTION_STRING not set in .env")

ENGINE = create_engine(DB_CONNECTION_STRING)

def get_current_season():
    from datetime import datetime
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

SEASON = get_current_season()

print("="*60)
print("PROP PREDICTION MODEL - TRAINING EXAMPLE")
print("="*60)
print(f"Season: {SEASON}")
print(f"Database: {DB_CONNECTION_STRING.split('@')[1] if '@' in DB_CONNECTION_STRING else 'local'}")
print()

# ============================================================================
# STEP 1: Load Data
# ============================================================================
print("📥 Loading data from database...")

# Load props with results
props_query = """
SELECT 
    player_name,
    position,
    prop_type,
    line,
    odds,
    side,
    implied_prob,
    week,
    game_id,
    actual_result
FROM bovada_player_props
WHERE actual_result IS NOT NULL
    AND prop_type IN ('Passing Yards', 'Rushing Yards', 'Receiving Yards')
"""

df_props = pl.read_database(props_query, ENGINE)
print(f"   ✅ Loaded {len(df_props)} props with results")

if df_props.is_empty():
    print("\n❌ No prop data with results found. Run 14_update_bovada_results.py first.")
    exit(1)

# Load player stats for features
stats_table = f"weekly_player_stats_{SEASON}"
df_stats = pl.read_database(f"SELECT * FROM {stats_table}", ENGINE)
print(f"   ✅ Loaded {len(df_stats)} player stat records")

# Load game lines for context
df_lines = pl.read_database("SELECT * FROM bovada_game_lines", ENGINE)
print(f"   ✅ Loaded {len(df_lines)} game line records")

# ============================================================================
# STEP 2: Feature Engineering
# ============================================================================
print("\n🔧 Engineering features...")

# Join props with stats (for historical context)
df_merged = df_props.join(
    df_stats, 
    on=['player_name', 'week'], 
    how='left'
)

# Join with game lines (for Vegas context)
df_merged = df_merged.join(
    df_lines.select(['game_id', 'total_over', 'home_spread']),
    on='game_id',
    how='left'
)

# Calculate rolling averages (using stats from previous weeks)
print("   📊 Calculating rolling features...")

# Create binary target
df_merged = df_merged.with_columns(
    pl.when(pl.col("actual_result") == "hit")
    .then(1)
    .otherwise(0)
    .alias("target")
)

# Feature: Distance from recent average
df_merged = df_merged.with_columns([
    # For passing props, how far is the line from recent avg?
    pl.when(pl.col("prop_type") == "Passing Yards")
    .then(pl.col("line") - pl.col("passing_yards"))
    .otherwise(None)
    .alias("line_vs_recent_passing"),
    
    # For rushing props
    pl.when(pl.col("prop_type") == "Rushing Yards")
    .then(pl.col("line") - pl.col("rushing_yards"))
    .otherwise(None)
    .alias("line_vs_recent_rushing"),
    
    # For receiving props
    pl.when(pl.col("prop_type") == "Receiving Yards")
    .then(pl.col("line") - pl.col("receiving_yards"))
    .otherwise(None)
    .alias("line_vs_recent_receiving"),
])

# Fill nulls
df_merged = df_merged.fill_null(0)

# ============================================================================
# STEP 3: Prepare Training Data
# ============================================================================
print("\n📋 Preparing training data...")

# Select features
feature_cols = [
    'line',
    'implied_prob',
    'total_over',
    'fantasy_points_ppr',
    'passing_yards',
    'rushing_yards',
    'receiving_yards',
    'line_vs_recent_passing',
    'line_vs_recent_rushing',
    'line_vs_recent_receiving'
]

# Convert to pandas for sklearn
df_pandas = df_merged.select(feature_cols + ['target']).to_pandas()

# Handle any remaining nulls
df_pandas = df_pandas.fillna(0)

X = df_pandas[feature_cols]
y = df_pandas['target']

print(f"   Features: {len(feature_cols)}")
print(f"   Samples: {len(X)}")
print(f"   Hit Rate: {y.mean():.2%}")

# ============================================================================
# STEP 4: Train Model
# ============================================================================
print("\n🤖 Training Random Forest model...")

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train model
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

# ============================================================================
# STEP 5: Evaluate
# ============================================================================
print("\n📊 Model Performance:")
print("-" * 60)

# Predictions
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# Metrics
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

auc = roc_auc_score(y_test, y_pred_proba)
print(f"\nROC AUC Score: {auc:.4f}")

# Feature importance
print("\n📈 Top 10 Most Important Features:")
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

for idx, row in feature_importance.head(10).iterrows():
    print(f"   {row['feature']:<30} {row['importance']:.4f}")

# ============================================================================
# STEP 6: Simulated Betting Performance
# ============================================================================
print("\n💰 Simulated Betting Performance:")
print("-" * 60)

# Calculate ROI assuming we bet on props where model predicts >55% hit prob
threshold = 0.55
confident_bets = y_pred_proba > threshold

if confident_bets.sum() > 0:
    confident_accuracy = y_test[confident_bets].mean()
    total_bets = confident_bets.sum()
    
    print(f"\nBets placed (>55% confidence): {total_bets}")
    print(f"Win rate: {confident_accuracy:.2%}")
    
    # Assuming -110 odds (need to win 52.4% to break even)
    implied_breakeven = 0.524
    
    if confident_accuracy > implied_breakeven:
        print(f"✅ Model beats the vig! (+{(confident_accuracy - implied_breakeven)*100:.1f}% edge)")
    else:
        print(f"❌ Model doesn't beat the vig ({(confident_accuracy - implied_breakeven)*100:.1f}% edge)")
else:
    print("\n⚠️ No bets met confidence threshold")

print("\n" + "="*60)
print("✅ Training complete!")
print("\nNEXT STEPS:")
print("1. Tune hyperparameters with GridSearchCV")
print("2. Add more features (opponent defense, weather, injuries)")
print("3. Train separate models per prop type")
print("4. Implement time-series cross-validation")
print("5. Track performance on live upcoming props")
print("="*60)
