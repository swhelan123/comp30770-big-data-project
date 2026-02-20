CREATE DATABASE big_d_ass_1;

USE big_d_ass_1;

CREATE TABLE stock_prices (
    Ticker VARCHAR(10),
    Date DATETIME,
    Adj_Close FLOAT,
    Close FLOAT,
    High FLOAT,
    Low FLOAT,
    Open FLOAT,
    Volume BIGINT
);

SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/shanewhelan/Documents/BigData/sp500-spx-components-stock-price/combined_stocks.csv'
INTO TABLE stock_prices
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


