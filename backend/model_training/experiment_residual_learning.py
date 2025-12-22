import polars as pl
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
from pathlib import Path

# --- Config ---
# Features to exclude from "Importance Drop" logic (we need these to exist)
PROTECTED_COLS = ['player_id', 'week', 'season', 'player_name', 'team', 'opponent', 'position']
# Drop features with importance greater than this:
IMPORTANCE_THRESHOLD = 0.10 

def load_and_prep_data():
    print("Loading featured dataset...")
    # Load your featured dataset (ensure this path is correct)
    data_path = Path(__file__).parent.parent / "dataPrep" / "featured_dataset.csv"
    if not data_path.exists():
        raise FileNotFoundError(f"Could not find {data_path}")
    
    df = pl.read_csv(data_path)
    
    # 1. Calculate 'Baseline Average' (Expanding Mean)
    # This represents "What the player usually scores" up to this point
    print("Calculating rolling averages (The Baseline)...")
    df = df.sort(["player_id", "season", "week"])
    
    # We use a shifting window to avoid data leakage (avg of PRIOR games)
    df = df.with_columns(
        pl.col("y_fantasy_points_ppr")
        .shift(1)
        .over("player_id")
        .alias("shifted_points")
    )
    
    # Calculate expanding mean of the *shifted* points
    # If it's Week 1, fill with a position baseline (e.g., 10.0) or 0
    df = df.with_columns(
        pl.col("shifted_points")
        .cumulative_eval(pl.element().mean())
        .over(["player_id", "season"])
        .fill_null(0.0)
        .alias("baseline_avg_points")
    )
    
    # 2. Create the Residual Target
    # Target = Actual - Baseline
    # Example: Actual 25, Avg 15 -> Target +10 (User performed +10 above expectation)
    df = df.with_columns(
        (pl.col("y_fantasy_points_ppr") - pl.col("baseline_avg_points")).alias("y_residual")
    )
    
    return df

def train_position_model(df, position):
    print(f"\n--- Training {position} Model (Residual Strategy) ---")
    
    # Filter Position & valid targets
    pos_df = df.filter(
        (pl.col("position") == position) & 
        (pl.col("y_fantasy_points_ppr").is_not_null())
    )
    
    # Define Features (exclude strings and targets)
    exclude = [
        "y_fantasy_points_ppr", "y_residual", "baseline_avg_points", 
        "shifted_points", "player_id", "player_name", "team", "opponent", 
        "game_id", "season", "week", "position", "injury_status"
    ]
    feature_cols = [c for c in pos_df.columns if c not in exclude and pos_df[c].dtype in [pl.Float64, pl.Int64]]
    
    # --- Time Series Split ---
    # Train on Weeks 1-14, Test on 15+ (or last few weeks)
    max_week = pos_df["week"].max()
    split_week = max_week - 2 # Test on last 2 weeks available
    
    train = pos_df.filter(pl.col("week") <= split_week)
    test = pos_df.filter(pl.col("week") > split_week)
    
    X_train = train.select(feature_cols).to_pandas()
    y_train = train["y_residual"].to_pandas() # Predicting Residual!
    
    X_test = test.select(feature_cols).to_pandas()
    y_test_actual_resid = test["y_residual"].to_pandas()
    y_test_actual_total = test["y_fantasy_points_ppr"].to_pandas()
    baseline_avgs = test["baseline_avg_points"].to_pandas()
    
    # --- PHASE 1: Feature Pruning ---
    # Train a "Probe" model to find dominant features
    print("  > Phase 1: Identifying dominant features (>10% importance)...")
    probe_model = xgb.XGBRegressor(n_estimators=50, objective='reg:squarederror', random_state=42)
    probe_model.fit(X_train, y_train)
    
    importances = probe_model.feature_importances_
    feats_to_drop = []
    
    for name, imp in zip(feature_cols, importances):
        if imp > IMPORTANCE_THRESHOLD:
            print(f"    ! Dropping '{name}' (Importance: {imp:.1%}) - Too dominant.")
            feats_to_drop.append(name)
            
    if not feats_to_drop:
        print("    (No features exceeded the 10% threshold)")
        
    # Remove dropped features
    final_features = [f for f in feature_cols if f not in feats_to_drop]
    X_train_pruned = X_train[final_features]
    X_test_pruned = X_test[final_features]
    
    # --- PHASE 2: Final Training ---
    print(f"  > Phase 2: Training final model on {len(final_features)} features...")
    final_model = xgb.XGBRegressor(
        n_estimators=500, 
        learning_rate=0.05, 
        max_depth=5, 
        objective='reg:squarederror',
        n_jobs=-1,
        random_state=42
    )
    final_model.fit(X_train_pruned, y_train)
    
    # --- Evaluation ---
    # 1. Predict Residual
    pred_residual = final_model.predict(X_test_pruned)
    
    # 2. Reconstruct Total Score
    # Final = (Predicted Residual) + (Player's Baseline Avg)
    pred_total = pred_residual + baseline_avgs
    
    # Metrics
    mae = mean_absolute_error(y_test_actual_total, pred_total)
    rmse = np.sqrt(mean_squared_error(y_test_actual_total, pred_total))
    
    print(f"  > Results for {position}:")
    print(f"    RMSE: {rmse:.4f}")
    print(f"    MAE:  {mae:.4f}")
    
    # Compare to just guessing the average (Baseline)
    baseline_mae = mean_absolute_error(y_test_actual_total, baseline_avgs)
    print(f"    (Baseline MAE if we just guessed their avg: {baseline_mae:.4f})")
    
    if mae < baseline_mae:
        print(f"    ✅ SUCCESS: Model improved on the baseline by {baseline_mae - mae:.2f} points.")
    else:
        print(f"    ❌ FAIL: Model could not beat the simple average.")

    return final_model

def main():
    print("Starting Experimental Run: Residual Learning with Feature Pruning")
    print("="*60)
    
    try:
        df = load_and_prep_data()
        
        # Train for all positions
        for pos in ['QB', 'RB', 'WR', 'TE']:
            train_position_model(df, pos)
            
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()