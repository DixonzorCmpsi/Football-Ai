# eda.py
import pandas as pd

def initial_data_inspection(file_path):
    """
    Performs the first-pass exploratory data analysis on the dataset.
    """
    print(f"--- Loading and Inspecting {file_path} ---")
    
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"\n!!! ERROR: File not found at '{file_path}'.")
        print("Please make sure the dataset is in the same folder as this script.")
        return

    # 1. First Look: Print the first 5 rows to see the structure.
    print("\n## 1. Data Head (First 5 Rows):")
    print(df.head())

    # 2. Data Shape: How many rows and columns?
    print(f"\n## 2. Data Shape:")
    print(f"The dataset has {df.shape[0]} rows and {df.shape[1]} columns.")

    # 3. Data Info: Get the data type of each column.
    print("\n## 3. Data Types and Non-Null Counts:")
    df.info()

    # 4. Missing Values: Count how many nulls are in each column.
    print("\n## 4. Missing Value Analysis:")
    missing_values = df.isnull().sum()
    missing_cols = missing_values[missing_values > 0].sort_values(ascending=False)
    if not missing_cols.empty:
        print(f"Found {len(missing_cols)} columns with missing values:")
        print(missing_cols)
    else:
        print("No missing values found in any columns. Excellent!")

    # 5. Statistical Summary: Get the basic stats for numerical columns.
    print("\n## 5. Descriptive Statistics:")
    print(df.describe())

if __name__ == "__main__":
    # IMPORTANT: Change this to the actual name of your uploaded file.
    dataset_filename = 'featured_dataset.csv'
    initial_data_inspection(dataset_filename)