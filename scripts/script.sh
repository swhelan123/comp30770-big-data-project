#! /bin/bash
#
javac -classpath $(hadoop classpath) StockHousingCorrelation.java
jar cf StockHousingCorrelation.jar StockHousingCorrelation*.class
hadoop fs -rm -r /output/correlation

time hadoop jar StockHousingCorrelation.jar StockHousingCorrelation \
  /input/correlation \
  /output/correlation
