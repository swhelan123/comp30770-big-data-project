#!/usr/bin/env python3
import math
import sys

# Reducer: Processes data from mapper to compute Pearson correlation
# between monthly stock returns and housing index returns.
# Expects keys to be sorted. Since '__HOUSING__' comes lexicographically
# before most tickers, it will be processed first (assuming a single reducer
# or secondary sorting).

housing_data = {}  # YearMonth -> Housing Index
current_ticker = None
stock_data = {}  # YearMonth -> List of Close Prices


def process_ticker(ticker, stock_data, housing_data):
    if not stock_data:
        return

    # 1. Temporal Aggregation: Monthly Averages
    monthly_avg = {}
    for ym, prices in stock_data.items():
        monthly_avg[ym] = sum(prices) / len(prices)

    # 2. Sort months chronologically to compute LAG
    sorted_months = sorted(monthly_avg.keys())

    stock_pct_changes = []
    housing_pct_changes = []

    # 3. Feature Engineering: Compute Month-over-Month Percentage Change
    for i in range(1, len(sorted_months)):
        prev_month = sorted_months[i - 1]
        curr_month = sorted_months[i]

        # Only process if both months have housing data
        if prev_month not in housing_data or curr_month not in housing_data:
            continue

        prev_stock = monthly_avg[prev_month]
        curr_stock = monthly_avg[curr_month]
        prev_housing = housing_data[prev_month]
        curr_housing = housing_data[curr_month]

        # Prevent division by zero
        if prev_stock == 0 or prev_housing == 0:
            continue

        stock_pct = (curr_stock - prev_stock) / prev_stock
        housing_pct = (curr_housing - prev_housing) / prev_housing

        stock_pct_changes.append(stock_pct)
        housing_pct_changes.append(housing_pct)

    # 4. Statistical Analysis: Pearson Correlation
    n = len(stock_pct_changes)
    if n >= 12:
        sum_x = sum(stock_pct_changes)
        sum_y = sum(housing_pct_changes)
        sum_xy = sum(x * y for x, y in zip(stock_pct_changes, housing_pct_changes))
        sum_x2 = sum(x * x for x in stock_pct_changes)
        sum_y2 = sum(y * y for y in housing_pct_changes)

        numerator = (n * sum_xy) - (sum_x * sum_y)
        denominator_sq = ((n * sum_x2) - (sum_x**2)) * ((n * sum_y2) - (sum_y**2))

        if denominator_sq > 0:
            correlation = numerator / math.sqrt(denominator_sq)
            # Output: Ticker | Months Correlated | Pearson Correlation
            print(f"{ticker}\t{n}\t{correlation:.6f}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    key, value = parts
    val_parts = value.split(",")

    if key == "__HOUSING__":
        if len(val_parts) >= 3 and val_parts[0] == "HOUSING":
            ym = val_parts[1]
            try:
                index_val = float(val_parts[2])
                housing_data[ym] = index_val
            except ValueError:
                pass
        continue

    # Processing Stock Data
    if len(val_parts) >= 3 and val_parts[0] == "STOCK":
        ym = val_parts[1]
        try:
            close_price = float(val_parts[2])
        except ValueError:
            continue

        if current_ticker == key:
            if ym not in stock_data:
                stock_data[ym] = []
            stock_data[ym].append(close_price)
        else:
            if current_ticker is not None:
                process_ticker(current_ticker, stock_data, housing_data)

            current_ticker = key
            stock_data = {ym: [close_price]}

# Process the last ticker
if current_ticker is not None:
    process_ticker(current_ticker, stock_data, housing_data)
