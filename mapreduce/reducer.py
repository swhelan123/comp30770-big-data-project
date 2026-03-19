#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0.0
current_count = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split("\t")

    if len(parts) != 2:
        continue

    key, price_str = parts
    try:
        price = float(price_str)
    except ValueError:
        continue

    # same ticker+year combo so just keep a running total
    if current_key == key:
        current_sum += price
        current_count += 1
    else:
        # hit a new key, so print out the avg for the last one
        if current_key:
            avg = current_sum / current_count
            print(f"{current_key}\t{round(avg, 2)}")
        # start fresh for this new key
        current_key = key
        current_sum = price
        current_count = 1

# gotta handle the last key or it wont get printed
if current_key == key:
    avg = current_sum / current_count
    print(f"{current_key}\t{round(avg, 2)}")
