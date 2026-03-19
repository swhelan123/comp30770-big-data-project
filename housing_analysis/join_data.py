import pandas as pd
import os
import glob

# 1. Automatically find the stock CSV inside the 'final_results_index' folder
stock_folder = 'final_results_index'
# This looks for any file starting with 'part-' and ending in '.csv' inside the folder
stock_files = glob.glob(os.path.join(stock_folder, 'part-*.csv'))

if not stock_files:
    print(f"Error: No stock CSV found in {stock_folder}. Make sure the folder exists!")
    exit()

stock_file = stock_files[0]
housing_file = 'CSUSHPISA.csv'

print(f"Reading stock data from: {stock_file}")
print(f"Reading housing data from: {housing_file}")

# 2. Load the files
stock_df = pd.read_csv(stock_file)
housing_df = pd.read_csv(housing_file)

# 3. Match the date format (Housing has YYYY-MM-DD, we need YYYY-MM)
housing_df['month'] = pd.to_datetime(housing_df['observation_date']).dt.strftime('%Y-%m')

# 4. Re-base Housing to Jan 2000 = 100
# Find the value at Jan 2000
try:
    base_val = housing_df[housing_df['month'] == '2000-01']['CSUSHPISA'].values[0]
    housing_df['housing_index'] = (housing_df['CSUSHPISA'] / base_val) * 100
except IndexError:
    print("Error: Could not find '2000-01' in CSUSHPISA.csv")
    exit()

# 5. Join them together
final_df = pd.merge(
    stock_df[['month', 'stock_index']], 
    housing_df[['month', 'housing_index']], 
    on='month', 
    how='inner'
).sort_values('month')

# 6. Save the final CSV
final_df.to_csv('stock_vs_housing_final.csv', index=False)

print("\nSUCCESS! 'stock_vs_housing_final.csv' has been created.")
print(f"Final Count: {len(final_df)} months compared.")
