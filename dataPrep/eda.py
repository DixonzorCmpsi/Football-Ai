# eda.py
import pandas as pd
import numpy as np

def enhanced_data_inspection(file_path):
    """
    Performs enhanced exploratory data analysis on the dataset,
    including checks for duplicate columns, unique values, categorical data,
    team columns, columns containing only zeros, and lists all column names.
    """
    print(f"--- Loading and Inspecting {file_path} ---")

    try:
        df = pd.read_csv(file_path, low_memory=False) # low_memory=False can help with mixed types
    except FileNotFoundError:
        print(f"\n!!! ERROR: File not found at '{file_path}'.")
        print("Please ensure the file path is correct relative to this script.")
        return
    except Exception as e:
        print(f"\n!!! ERROR loading CSV: {e}")
        return

    # --- 1. Basic Information ---
    print("\n## 1. Basic Information")
    print("\n### Data Head (First 5 Rows):")
    with pd.option_context('display.max_columns', 20):
        print(df.head())
    print(f"\n### Data Shape:")
    print(f"The dataset has {df.shape[0]} rows and {df.shape[1]} columns.")
    print("\n### Data Types Summary:")
    print(df.dtypes.value_counts())

    # --- 1b. List All Column Names --- # <<<< NEW SECTION ADDED HERE
    print("\n" + "="*40 + "\n") # Separator
    print("## 1b. All Column Names (Features)")
    all_columns = df.columns.tolist()
    print(f"\nTotal number of columns: {len(all_columns)}")
    print("\nFull list of column names:")
    # Print the list - consider printing in multiple columns if very long
    # for i, col in enumerate(all_columns):
    #     print(f"{i+1}. {col}")
    print(all_columns) # Simpler print for direct copy/paste
    # --- END NEW SECTION ---


    # --- 2. Duplicate Column Check ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 2. Duplicate Column Check")
    potential_duplicates_mangled = [col for col in df.columns if '.' in col and col.split('.')[-1].isdigit()]
    duplicated_columns_list = df.columns[df.columns.duplicated()].tolist()

    if duplicated_columns_list:
        print(f"\n🔴 WARNING: Found {len(duplicated_columns_list)} duplicate column name(s) directly:")
        print(duplicated_columns_list)
    elif potential_duplicates_mangled:
         print(f"\n🟡 WARNING: Found column names suggesting potential duplicates handled by read_csv (e.g., '.1'):")
         print(potential_duplicates_mangled)
         print("   This indicates duplicate names in the CSV file itself.")
    else:
        print("\n✅ No duplicate column names found.")


    # --- 3. Missing Value Analysis ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 3. Missing Value Analysis")
    missing_values = df.isnull().sum()
    missing_cols = missing_values[missing_values > 0].sort_values(ascending=False)
    if not missing_cols.empty:
        print(f"\nFound {len(missing_cols)} columns with missing values:")
        missing_percentage = (missing_cols / len(df)) * 100
        missing_df = pd.DataFrame({'Missing Count': missing_cols, 'Missing Percentage': missing_percentage.round(2)})
        print(missing_df)

        totally_empty = missing_df[missing_df['Missing Count'] == len(df)]
        if not totally_empty.empty:
            print("\n🔴 WARNING: The following columns are completely empty (all NaN):")
            print(totally_empty.index.tolist())

    else:
        print("\n✅ No missing values found in any columns.")

    # --- 4. Unique Value Analysis ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 4. Unique Value Analysis (for potential IDs or high-cardinality columns)")
    unique_counts = df.nunique().sort_values(ascending=False)
    print("\n### Top 10 Columns with Most Unique Values:")
    print(unique_counts.head(10))
    print("\n### Columns with Only 1 Unique Value (Constants):")
    constant_cols = unique_counts[unique_counts == 1].index.tolist()
    if constant_cols:
        print(constant_cols)
    else:
        print("None found.")

    # --- 4b. Identify Team Columns ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 4b. Identifying 'Team' Columns")
    team_columns = [col for col in df.columns if 'team' in col.lower()]
    if team_columns:
        print(f"\nFound {len(team_columns)} columns containing 'team' (case-insensitive):")
        print(team_columns)
        print("\nMissing values in identified 'team' columns:")
        print(df[team_columns].isnull().sum())
    else:
        print("\nNo columns containing 'team' found.")

    # --- 4c. Identify Columns with Only Zeros ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 4c. Identifying Columns Containing Only Zeros")
    zero_cols = []
    numeric_cols = df.select_dtypes(include=np.number).columns
    for col in numeric_cols:
        try:
            if (df[col].fillna(0) == 0).all():
                zero_cols.append(col)
        except Exception as e:
            print(f"Could not check column '{col}' for zeros: {e}")

    if zero_cols:
        print(f"\nFound {len(zero_cols)} numeric columns containing only zeros (and potentially NaNs):")
        print(zero_cols)
    else:
        print("\nNo numeric columns containing only zeros found.")


    # --- 5. Categorical Data Snippet ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 5. Categorical Data Snippet")
    object_cols = df.select_dtypes(include='object').columns
    print(f"\nFound {len(object_cols)} object (likely categorical/string) columns.")
    cols_to_show_counts = ['position', 'team', 'pro_team', 'season_type', 'opponent'] # These might need adjustment based on actual column names
    print("\n### Value Counts for Selected Categorical Columns (Top 5):")
    for col in cols_to_show_counts:
        if col in df.columns:
            print(f"\n--- {col} ---")
            try:
                col_data = df[col].astype(str)
                print(col_data.value_counts().head())
                original_nulls = df[col].isnull().sum()
                if original_nulls > 0:
                    print(f"(Original NaN/Null values count: {original_nulls})")
            except TypeError:
                print(f"Could not compute value counts for {col} (likely contains unhashable types).")
            except Exception as e:
                print(f"Error computing value counts for {col}: {e}")
        else:
            print(f"\n--- Column '{col}' not found ---")


    # --- 6. Descriptive Statistics for Numerical Data ---
    print("\n" + "="*40 + "\n") # Separator
    print("## 6. Descriptive Statistics (Numerical Columns)")
    with pd.option_context('display.float_format', '{:.2f}'.format):
        print(df.describe())

    print("\n" + "="*40 + "\n") # Separator
    print("--- EDA Complete ---")


if __name__ == "__main__":
    # Path relative to the script's location (assuming script is in rag_data folder)
    dataset_filename = '../dataPrep/featured_dataset.csv' # Adjusted path if script is IN rag_data
    enhanced_data_inspection(dataset_filename)