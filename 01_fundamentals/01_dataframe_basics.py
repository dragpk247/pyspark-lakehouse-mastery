"""
01_fundamentals/01_dataframe_basics.py
--------------------------------------
Demonstrates basic PySpark DataFrame operations:
- Initializing a SparkSession
- Creating DataFrames with explicit schemas
- Transformations (select, filter, withColumn, groupBy)
- Actions (show, count, collect)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, avg, round

def main():
    print("=" * 60)
    print("🚀 Initializing Apache Spark Session...")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("01_DataFrameBasics") \
        .master("local[*]") \
        .getOrCreate()

    # Suppress verbose Spark info logs for cleaner learning output
    spark.sparkContext.setLogLevel("WARN")

    # Define an explicit schema (Best Practice in production)
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("session_duration_sec", IntegerType(), True),
        StructField("purchase_amount", DoubleType(), True)
    ])

    data = [
        ("U101", "mobile", "US", 320, 49.99),
        ("U102", "desktop", "US", 850, 199.50),
        ("U103", "mobile", "CA", 120, 0.00),
        ("U104", "tablet", "UK", 410, 24.99),
        ("U105", "desktop", "CA", 940, 310.00),
        ("U106", "mobile", "UK", 210, 15.50),
        ("U107", "desktop", "US", 620, 0.00),
        ("U108", "mobile", "US", 180, 89.99),
    ]

    df = spark.createDataFrame(data, schema=schema)

    print("\n--- 1. Raw Dataset ---")
    df.show(truncate=False)

    print("\n--- 2. Filter: Paying Customers with Session > 200s ---")
    paying_users = df.filter((col("purchase_amount") > 0) & (col("session_duration_sec") > 200))
    paying_users.show()

    print("\n--- 3. Transformation: Categorizing Customers ---")
    categorized_df = df.withColumn(
        "tier",
        when(col("purchase_amount") >= 100, "VIP")
        .when(col("purchase_amount") > 0, "Standard")
        .otherwise("Free")
    )
    categorized_df.show()

    print("\n--- 4. Aggregation: Metrics by Country ---")
    country_metrics = categorized_df.groupBy("country").agg(
        round(avg("purchase_amount"), 2).alias("avg_spend"),
        round(avg("session_duration_sec"), 1).alias("avg_duration")
    )
    country_metrics.show()

    print("=" * 60)
    print("✅ Module 1 Finished Successfully!")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
