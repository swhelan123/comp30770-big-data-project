#!/usr/bin/env python3
import sys

# need to skip the first line since its just column names
header_skipped = False

for line in sys.stdin:
    line = line.strip()
    if not header_skipped:
        if line.startswith("Ticker"):
            header_skipped = True
            continue

    parts = line.split(",")
    # make sure we actually have all 8 columns otherwise its a bad row
    if len(parts) == 8:
        ticker = parts[0]
        date = parts[1]
        try:
            close_price = float(parts[3])
            year = date.split("-")[0]  # just get the year part

            # emit ticker_year and the close price separated by tab
            print(f"{ticker}_{year}\t{close_price}")
        except ValueError:
            pass  # some rows have missing prices so just skip those
