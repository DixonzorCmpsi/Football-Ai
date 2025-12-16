# verify_data.py
import pandas as pd

# Load the dataset you created
df = pd.read_csv("weekly_modeling_dataset.csv")

print("--- Data Verification Report ---")
# Calculate the number of missing (null) values for each column
missing_values = df.isnull().sum()

# Print the count of missing values for columns that have them
print("Columns with missing values (and their counts):")
print(missing_values[missing_values > 0].sort_values(ascending=False))

print("\nVerification complete. This shows which columns have blank cells.")