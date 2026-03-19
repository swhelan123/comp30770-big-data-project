from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

# Initialize Spark
spark = SparkSession.builder.appName("Master_Index_Generator").getOrCreate()

# 1. LOAD: Start from the individual stock returns
input_path = "/opt/spark/work-dir/final_returns_distribution"
try:
    df = spark.read.option("header", "true").csv(input_path)
    df = df.withColumn("monthly_return_pct", F.col("monthly_return_pct").cast("double"))
except Exception as e:
    print(f"ERROR: Could not find input data at {input_path}")
    sys.exit(1)

# 2. AVERAGE: Group by month and find the market average
monthly_avg_df = df.groupBy("month") \
    .agg(F.avg("monthly_return_pct").alias("avg_return")) \
    .orderBy("month")

# 3. INDEX: Calculate Multiplier and Cumulative Growth
# We use math: Product(x) = Exp(Sum(Log(x))) to get cumulative growth
monthly_avg_df = monthly_avg_df.withColumn("multiplier", 1 + (F.col("avg_return") / 100))
window_spec = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
monthly_avg_df = monthly_avg_df.withColumn("cum_growth", F.exp(F.sum(F.log(F.col("multiplier"))).over(window_spec)))

# 4. RE-BASE: Find the value at 2000-01 and set it to 100
base_row = monthly_avg_df.filter(F.col("month") == "2000-01").select("cum_growth").collect()

if not base_row:
    print("ERROR: Could not find January 2000 in your data to use as a base.")
else:
    base_val = base_row[0][0]
    final_index_df = monthly_avg_df.withColumn("stock_index", (F.col("cum_growth") / base_val) * 100)

    # 5. SAVE: Write the final result to a new folder
    output_path = "/opt/spark/work-dir/final_stock_index_2000"
    final_index_df.select("month", "avg_return", "stock_index") \
        .coalesce(1) \
        .write.csv(output_path, header=True, mode="overwrite")
    
    print(f"SUCCESS: Final index created at {output_path}")

spark.stop()
