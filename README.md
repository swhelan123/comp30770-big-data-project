# S&P 500 Big Data Analysis (COMP30770)

## Project Overview
This project processes and analyses historical stock price data for S&P 500 companies. It compares traditional relational database performance (MySQL) against distributed processing (MapReduce) using a 2.7 GB dataset.

## Data Source
The raw data consists of 2,000 individual CSV files containing daily stock prices. 
* **Dataset Link:** [S&P 500 Components Stock Price on Kaggle](https://www.kaggle.com/datasets/benjaminpo/sp500-spx-components-stock-price)

## Files in this Repository
* `merge.py`: Python script used to consolidate the 2,000 individual CSV files into a single 2.7 GB dataset, appending the ticker symbol to each row.
* `1_database_setup.sql`: SQL script to generate the database schema and bulk-load the 56.7 million rows via `LOAD DATA LOCAL INFILE`.
