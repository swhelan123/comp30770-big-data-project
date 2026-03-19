import pandas as pd
import matplotlib.pyplot as plt

# 1. Load the dataset
# Ensure 'stock_vs_housing_final.csv' is in your working directory
df = pd.read_csv('stock_vs_housing_final.csv')

# 2. Convert 'month' column to datetime for proper chronological plotting
df['month'] = pd.to_datetime(df['month'])

# 3. Sort the data by month (just in case it's unsorted)
df = df.sort_values('month')

# 4. Create the plot
plt.figure(figsize=(12, 6))

# Plot both lines
plt.plot(df['month'], df['stock_index'], label='Stock Index', color='#1f77b4', linewidth=1.5)
plt.plot(df['month'], df['housing_index'], label='Housing Index', color='#ff7f0e', linewidth=1.5)

# 5. Add titles, labels, and formatting
plt.title('Comparison: Stock Index vs. Housing Index (1987 - Present)', fontsize=16)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Index Value', fontsize=12)

# Add a legend to identify the lines
plt.legend(loc='upper left')

# Add a grid for easier reading
plt.grid(True, linestyle='--', alpha=0.6)

# 6. Save and show the plot
plt.tight_layout()
plt.savefig('stock_vs_housing_chart.png', dpi=300)
plt.show()

print("Chart successfully generated and saved as 'stock_vs_housing_chart.png'.")
