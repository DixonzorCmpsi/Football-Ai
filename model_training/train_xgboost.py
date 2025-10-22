# train_xgboost.py (v2 - with Hyperparameter Tuning)
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_absolute_error
import joblib

def tune_and_train_model(data_path, model_output_path):
    """
    Performs hyperparameter tuning for XGBoost and trains the final, best model.
    """
    print(f"--- Phase 3: Tuning and Training XGBoost Model ---")
    
    # --- 1. Load the final feature-engineered data ---
    try:
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        print(f"\n!!! ERROR: File not found at '{data_path}'.")
        print("Please ensure 'featured_dataset.csv' exists in the 'dataPrep' folder.")
        return
        
    print(f"Loaded featured dataset with {df.shape[0]} rows.")

    # --- 2. Prepare Data for the Council of Experts ---
    TARGET = 'y_fantasy_points_ppr'
    features_to_exclude = [TARGET, 'player_id', 'player_name', 'team', 'opponent']
    features = [col for col in df.columns if df[col].dtype in ['float64', 'int64'] and col not in features_to_exclude]
    
    X = df[features + ['position']]
    y = df[TARGET]
    
    X = pd.get_dummies(X, columns=['position'], drop_first=True)
    
    print(f"Tuning model with {len(X.columns)} features.")

    # --- 3. Split Data into Training and Testing Sets ---
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Split data into {len(X_train)} training rows and {len(X_test)} testing rows.")

    # --- 4. Hyperparameter Tuning with Randomized Search ---
    print("\nInitiating hyperparameter search for the Council of Experts...")
    
    # Define the grid of hyperparameters to search
    param_grid = {
        'learning_rate': [0.01, 0.02, 0.05, 0.1],
        'max_depth': [3, 4, 5, 6, 7, 8],
        'n_estimators': [500, 1000, 1500],
        'subsample': [0.7, 0.8, 0.9, 1.0],
        'colsample_bytree': [0.7, 0.8, 0.9, 1.0],
        'gamma': [0, 0.1, 0.2] # A regularization parameter
    }

    # Initialize the XGBoost Regressor
    xgb_reg = xgb.XGBRegressor(random_state=42, n_jobs=-1, tree_method='gpu_hist', early_stopping_rounds=50)

    # Set up the Randomized Search with Cross-Validation
    # n_iter: number of random combinations to try
    # cv: number of cross-validation folds (e.g., 5 means 80% train, 20% validation for each fold)
    random_search = RandomizedSearchCV(
        estimator=xgb_reg,
        param_distributions=param_grid,
        n_iter=50, # Try 50 different combinations
        scoring='neg_mean_absolute_error', # We want to minimize MAE
        cv=5,
        verbose=1,
        random_state=42
    )

    print("Searching for the best hyperparameters... This may take a while.")
    random_search.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    print("\n--- Hyperparameter Search Complete ---")
    print(f"Best Parameters Found: {random_search.best_params_}")
    
    # --- 5. Train and Evaluate the Final, Tuned Model ---
    print("\nTraining the final model with the best parameters...")
    best_model = random_search.best_estimator_

    # The best model has already been trained on the full training data by RandomizedSearchCV
    predictions = best_model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    
    print("\n--- Tuned Baseline Model Evaluation ---")
    print(f"✅ Final Tuned Mean Absolute Error (MAE): {mae:.4f}")

    # --- 6. Save the Final, Tuned Model ---
    print(f"\nSaving the final tuned model to '{model_output_path}'...")
    joblib.dump(best_model, model_output_path)
    print("✅ Tuned baseline model saved.")

if __name__ == "__main__":
    featured_data_file = '../dataPrep/featured_dataset.csv'
    # The final, tuned model will be saved here
    final_model_path = 'tuned_xgboost_baseline.joblib'
    tune_and_train_model(featured_data_file, final_model_path)