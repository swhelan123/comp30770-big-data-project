-- Query 1: temporal aggregation
CREATE TABLE monthly_stocks AS
SELECT Ticker,
       DATE_FORMAT(Date, '%Y-%m') AS YearMonth,
       AVG(Close)                 AS Monthly_Avg_Close
FROM stock_prices
GROUP BY Ticker, DATE_FORMAT(Date, '%Y-%m');

-- Query 2: inner join
CREATE TABLE joined_data AS
SELECT s.Ticker,
       s.YearMonth,
       s.Monthly_Avg_Close,
       h.Housing_Index
FROM monthly_stocks s
         JOIN housing_data h
              ON s.YearMonth = DATE_FORMAT(h.Record_Date, '%Y-%m');

-- Query 3: feature engineering
CREATE TABLE feature_engineered_data AS
SELECT Ticker,
       YearMonth,
       (Monthly_Avg_Close - LAG(Monthly_Avg_Close) OVER (PARTITION BY Ticker ORDER BY YearMonth)) /
       NULLIF(LAG(Monthly_Avg_Close) OVER (PARTITION BY Ticker ORDER BY YearMonth), 0) AS Stock_Pct_Change,

       (Housing_Index - LAG(Housing_Index) OVER (PARTITION BY Ticker ORDER BY YearMonth)) /
       NULLIF(LAG(Housing_Index) OVER (PARTITION BY Ticker ORDER BY YearMonth), 0)     AS Housing_Pct_Change
FROM joined_data;

-- Query 4: pearson correlation
CREATE TABLE final_correlation AS
SELECT Ticker,
       COUNT(*) AS Months_Correlated,
       (COUNT(*) * SUM(Stock_Pct_Change * Housing_Pct_Change) - SUM(Stock_Pct_Change) * SUM(Housing_Pct_Change)) /
       NULLIF(
               SQRT(
                       (COUNT(*) * SUM(Stock_Pct_Change * Stock_Pct_Change) - POW(SUM(Stock_Pct_Change), 2)) *
                       (COUNT(*) * SUM(Housing_Pct_Change * Housing_Pct_Change) - POW(SUM(Housing_Pct_Change), 2))
               ), 0
       )        AS Pearson_Correlation
FROM feature_engineered_data
WHERE Stock_Pct_Change IS NOT NULL
  AND Housing_Pct_Change IS NOT NULL
GROUP BY Ticker
HAVING COUNT(*) >= 12
ORDER BY Pearson_Correlation DESC;
