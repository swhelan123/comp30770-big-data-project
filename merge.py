import pandas as pd
import glob
import os

# Point to your specific folder
folder_path = "./data/1d" 
all_files = glob.glob(os.path.join(folder_path, "*.csv"))

df_list = []

print("Combining files, this might take a minute...")
for file in all_files:
    # Read each CSV
    df = pd.read_csv(file)
    
    # Extract the ticker from the filename (e.g., 'AAPL' from 'AAPL.csv')
    ticker_name = os.path.basename(file).split('.')[0]
    
    # Add the Ticker column to the dataframe
    df['Ticker'] = ticker_name
    
    df_list.append(df)

# Stitch them all together
big_df = pd.concat(df_list, ignore_index=True)

# Rearrange columns so Ticker is first (just for readability)
cols = ['Ticker', 'Date', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
big_df = big_df[cols]

# Save the result as one giant CSV file
big_df.to_csv('combined_stocks.csv', index=False)

print(f"Done! Combined {len(all_files)} files into 'combined_stocks.csv'")
