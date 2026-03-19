# Stock & Housing Market Analysis (COMP30770 - Big Data)

## Project Overview

This project processes and analyses historical stock price data alongside the S&P/Case-Shiller U.S. National Home Price Index. The goal is to investigate whether month-over-month changes in stock prices correlate with corresponding changes in housing prices by computing the Pearson Correlation Coefficient for each asset.

The project compares traditional relational database performance (SQL) against distributed processing frameworks (Hadoop MapReduce in both Python and Java) using a large-scale dataset (over 56.8 million rows, ~3.12 GB).

## Data Sources

1. **Stock Data:** [2000+ Assets (Stock & FX) Historical Price](https://www.kaggle.com/) (Kaggle) - Contains daily price data for thousands of assets across separate CSV files.
2. **Housing Data:** [S&P/Case-Shiller U.S. National Home Price Index](https://fred.stlouisfed.org/series/CSUSHPINSA) - A monthly composite index tracking residential property values.

## Repository Structure

The repository has been modularised by language and function:

### 1. `scripts/` (Data Preparation)

- `merge.py`: A Python script used to consolidate the thousands of individual stock CSV files into a single large dataset prior to relational database ingestion.

### 2. `sql/` (Relational Database Pipeline)

- `1_database_setup.sql`: SQL script to generate the database schema and bulk-load the raw data.
- `2_data_processing_pipeline.sql`: A 4-step SQL pipeline that performs:
  1. Temporal Aggregation (Daily to Monthly)
  2. Data Integration (Inner Join on Year-Month)
  3. Feature Engineering (Month-over-Month percentage change via Window Functions)
  4. Statistical Analysis (Pearson Correlation for assets with >= 12 months of data)

### 3. `mapreduce/` (Distributed Processing Pipeline)

Contains equivalent distributed implementations of the SQL pipeline.

- **`python/`**: Hadoop Streaming implementation (`mapper.py` and `reducer.py`).
- **`java/`**: Native Hadoop MapReduce implementation (`StockHousingCorrelation.java`).

Both implementations read the fragmented stock CSVs and the housing CSV simultaneously, joining and correlating them in a single MapReduce pass.

### 4. `report/` (Documentation)

- `report.tex`: LaTeX source code for the project report detailing architecture, bottlenecks, and performance analysis.
- **GitHub Actions:** The repository features an automated workflow (`.github/workflows/compile-latex.yml`) that automatically compiles the LaTeX document into a PDF (`report.pdf`) upon any commit or push, making the latest version available as a downloadable Artifact in the Actions tab.
