from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("SP500_Formatted_Returns").getOrCreate()

# 1. Load Data
stocks = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark/work-dir/data/*.csv")

# 2. Select and Clean
s_df = stocks.select(
    F.input_file_name().alias("stock_id"),
    F.to_date("Date").alias("date"),
    F.col("Adj Close").cast("double").alias("price")
).filter("price > 0 AND date IS NOT NULL")

# 3. Get Last Price of Each Month (Binning logic)
# This identifies the very last trading day available in each month
month_window = Window.partitionBy("stock_id", F.trunc("date", "MM")).orderBy(F.col("date").desc())

monthly_data = s_df.withColumn("rank", F.row_number().over(month_window)) \
    .filter("rank = 1") \
    .select("stock_id", F.trunc("date", "MM").alias("month_date"), "price")

# 4. Calculate Monthly Percentage Return
return_window = Window.partitionBy("stock_id").orderBy("month_date")

returns_df = monthly_data.withColumn("prev_price", F.lag("price").over(return_window)) \
    .withColumn("monthly_return_pct", ((F.col("price") - F.col("prev_price")) / F.col("prev_price")) * 100) \
    .filter("monthly_return_pct IS NOT NULL")

# 5. Statistical Outlier Filtering (IQR Method)
quantiles = returns_df.stat.approxQuantile("monthly_return_pct", [0.25, 0.75], 0.01)
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr

cleaned_returns = returns_df.filter((F.col("monthly_return_pct") >= lower_bound) & 
                                    (F.col("monthly_return_pct") <= upper_bound))

# 6. Final Output: YYYY-MM Labels
# We convert the internal date into your exact requested string format
final_output = cleaned_returns.select(
    F.date_format("month_date", "yyyy-MM").alias("month"), 
    "monthly_return_pct"
).orderBy("month")

# Save as a single CSV
final_output.coalesce(1).write.csv("/opt/spark/work-dir/final_returns_distribution", header=True, mode="overwrite")

spark.stop()
