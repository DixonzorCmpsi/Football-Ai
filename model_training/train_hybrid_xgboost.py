# train_hybrid_xgboost.py (v2 - with Hyperparameter Tuning)
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_absolute_error
import joblib

def tune_and_train_hybrid_model(data_path, model_output_path):
    """
    Performs hyperparameter tuning for the hybrid XGBoost model and trains the final version.
    """
    print(f"--- Phase 5: Tuning and Training Final Hybrid XGBoost Model ---")

    # --- 1. Load the data with embeddings ---
    try:
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        print(f"\n!!! ERROR: File not found at '{data_path}'.")
        print("Please ensure 'generate_embeddings.py' ran successfully.")
        return

    print(f"Loaded dataset with embeddings: {df.shape[0]} rows, {df.shape[1]} columns.")

    # --- 2. Prepare Data for the Final Council ---
    TARGET = 'y_fantasy_points_ppr'
    features_to_exclude = [TARGET, 'player_id', 'player_name', 'team', 'opponent']
    features = [col for col in df.columns if df[col].dtype in ['float64', 'int64'] and col not in features_to_exclude]

    X = df[features + ['position']].copy()
    y = df[TARGET]
    X = pd.get_dummies(X, columns=['position'], drop_first=True)

    print(f"Tuning final model with {len(X.columns)} features (including embeddings).")

    # --- 3. Split Data into Training and Testing Sets ---
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Split data into {len(X_train)} training rows and {len(X_test)} testing rows.")

    # --- 4. Hyperparameter Tuning with Randomized Search ---
    print("\nInitiating hyperparameter search for the Hybrid Council...")

    # Define the same grid of hyperparameters to explore
    param_grid = {
        'learning_rate': [0.01, 0.02, 0.05, 0.1],
        'max_depth': [3, 4, 5, 6, 7, 8],
        'n_estimators': [500, 1000, 1500],
        'subsample': [0.7, 0.8, 0.9, 1.0],
        'colsample_bytree': [0.7, 0.8, 0.9, 1.0],
        'gamma': [0, 0.1, 0.2]
    }

    # Initialize the XGBoost Regressor
    # Using updated syntax for GPU if possible
    xgb_reg = xgb.XGBRegressor(
        random_state=42,
        n_jobs=-1,
        tree_method='hist', # Use hist method
        device='cuda',      # Specify CUDA device
        early_stopping_rounds=50
    )

    random_search = RandomizedSearchCV(
        estimator=xgb_reg,
        param_distributions=param_grid,
        n_iter=50,
        scoring='neg_mean_absolute_error',
        cv=5,
        verbose=1,
        random_state=42
    )

    print("Searching for the best hyperparameters for the hybrid model... This may take a while.")
    # Fit RandomizedSearch - it handles the training and validation internally
    # Note: Early stopping happens within each CV fold fit
    random_search.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)


    print("\n--- Hyperparameter Search Complete ---")
    print(f"Best Parameters Found for Hybrid Model: {random_search.best_params_}")

    # --- 5. Train and Evaluate the Final, Tuned Hybrid Model ---
    print("\nTraining the final hybrid model with the best parameters...")
    best_hybrid_model = random_search.best_estimator_

    # The best model has been trained on the full training data during the search's final refit
    predictions = best_hybrid_model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    print("\n--- Tuned Hybrid Model Evaluation ---")
    print(f"✅ Final Tuned Hybrid Mean Absolute Error (MAE): {mae:.4f}")
    print("Compare this MAE to the tuned baseline MAE (0.5573) to measure the final impact.")

    # --- 6. Save the Final Tuned Oracle ---
    print(f"\nSaving the final tuned hybrid model to '{model_output_path}'...")
    joblib.dump(best_hybrid_model, model_output_path)
    print("✅ Final tuned hybrid model saved.")


if __name__ == "__main__":
    data_with_embeddings_file = '../dataPrep/dataset_with_embeddings.csv'
    # Save the tuned hybrid model to a new file
    final_tuned_model_path = 'tuned_hybrid_xgboost_model.joblib'
    tune_and_train_hybrid_model(data_with_embeddings_file, final_tuned_model_path)