#!/usr/bin/env python3
import os
import sys

# Mapper: Reads both Stock and Housing data from stdin.
# Determines the source based on the number of columns or filename (if available in env).
# Emits:
# - For Stock: Key = Ticker, Value = STOCK,YearMonth,Close
# - For Housing: Key = __HOUSING__, Value = HOUSING,YearMonth,Index

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split(",")

    # Simple heuristic to distinguish stock data from housing data:
    # Stock data typically has 7-8 columns (Ticker, Date, Open, High, Low, Close, Adj Close, Volume)
    # Housing data typically has 2 columns (DATE, CSUSHPINSA)

    if len(parts) >= 7:
        # Stock Data
        if parts[0] == "Ticker" or parts[0] == "Symbol":
            continue  # Skip header

        ticker = parts[0]
        date_str = parts[1]

        try:
            close_price = float(
                parts[5] if "Close" not in parts else parts[3]
            )  # Assuming Close is at index 3 or 5
            # Extract YearMonth (YYYY-MM)
            year_month = date_str[:7]
            print(f"{ticker}\tSTOCK,{year_month},{close_price}")
        except ValueError:
            pass

    elif len(parts) == 2:
        # Housing Data
        if parts[0].upper() == "DATE" or "RECORD_DATE" in parts[0].upper():
            continue  # Skip header

        date_str = parts[0]
        try:
            housing_index = float(parts[1])
            year_month = date_str[:7]
            # Emit with a special key so the reducer can cache it, or emit for all tickers?
            # In Hadoop streaming, we usually handle small datasets by loading them in memory,
            # but to be pure MapReduce, we'll emit a special key and use a secondary sort,
            # or just rely on the reducer to process HOUSING records first if we sort properly.
            print(f"__HOUSING__\tHOUSING,{year_month},{housing_index}")
        except ValueError:
            pass
