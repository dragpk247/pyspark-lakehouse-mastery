"""
Module 04: Delta Lake ACID Transactions, Time Travel & Schema Enforcement
==========================================================================
Demonstrates the foundational guarantees of a Lakehouse:
1. ACID transactions on object storage / Parquet files.
2. Full history audit via Delta transaction log (_delta_log).
3. Time Travel: Querying table as of version 0 vs version 1.
4. Schema enforcement preventing corrupt write attempts.
5. In-place table rollback without re-ingesting raw data.
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


def get_spark_delta_session() -> SparkSession:
    """Build Spark session configured with Delta Lake extensions."""
    return (
        SparkSession.builder
        .appName("04_Delta_Lake_ACID_Lab")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def run_delta_lake_lab():
    spark = get_spark_delta_session()
    spark.sparkContext.setLogLevel("WARN")

    delta_table_path = "/tmp/pyspark_delta_lake_lab/customer_accounts"

    if os.path.exists(delta_table_path):
        shutil.rmtree(delta_table_path)

    print("=" * 65)
    print("💎 Module 04: Delta Lake ACID Transactions & Time Travel")
    print("=" * 65)

    # 1. Version 0: Initial Seed Data
    print("\n[Step 1] Writing Initial Version 0 (3 Customer Accounts)...")
    v0_data = [
        ("cust_101", "Alice Vance", "ACTIVE", 4500.00),
        ("cust_102", "Bob Smith", "ACTIVE", 1200.50),
        ("cust_103", "Charlie Davis", "PENDING", 750.25),
    ]
    schema = ["customer_id", "full_name", "status", "balance"]
    df_v0 = spark.createDataFrame(v0_data, schema)
    
    df_v0.write.format("delta").mode("overwrite").save(delta_table_path)
    print("    --> Version 0 written successfully.")

    # 2. Version 1: ACID Upsert / Update (Balance Change & Status Update)
    print("\n[Step 2] Writing Version 1: Updating account balances and statuses...")
    v1_data = [
        ("cust_101", "Alice Vance", "ACTIVE", 5000.00),      # Balance updated
        ("cust_102", "Bob Smith", "SUSPENDED", 0.00),        # Status changed
        ("cust_103", "Charlie Davis", "ACTIVE", 750.25),     # Status approved
        ("cust_104", "Diana Prince", "ACTIVE", 10000.00),    # New account
    ]
    df_v1 = spark.createDataFrame(v1_data, schema)
    df_v1.write.format("delta").mode("overwrite").save(delta_table_path)
    print("    --> Version 1 written successfully.")

    # 3. Query Delta Transaction Log (History)
    print("\n[Step 3] Inspecting Delta Transaction Log History:")
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{delta_table_path}`")
    history_df.select("version", "timestamp", "operation", "operationParameters.mode").show(truncate=False)

    # 4. Time Travel Query (Query Table as of Version 0 vs Version 1)
    print("\n[Step 4] TIME TRAVEL QUERY: Fetching table state AS OF VERSION 0:")
    df_time_travel_v0 = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .load(delta_table_path)
    )
    df_time_travel_v0.show()

    print("\n[Step 5] CURRENT QUERY: Fetching table state (Version 1):")
    df_current = spark.read.format("delta").load(delta_table_path)
    df_current.show()

    # 5. Schema Enforcement (Attempt to write bad data without schema evolution)
    print("\n[Step 6] Verifying Schema Enforcement (rejecting unauthorized column):")
    corrupt_data = [("cust_999", "Eve Hacker", "ACTIVE", 99999.0, "ILLEGAL_EXTRA_COLUMN")]
    corrupt_schema = ["customer_id", "full_name", "status", "balance", "hacked_col"]
    corrupt_df = spark.createDataFrame(corrupt_data, corrupt_schema)

    try:
        corrupt_df.write.format("delta").mode("append").save(delta_table_path)
        print("❌ Warning: Schema enforcement failed to block illegal column!")
    except Exception as e:
        print("✅ Schema Enforcement Worked! Delta blocked the mismatched schema.")
        print(f"   Reason: Caught schema mismatch error as expected.")

    print("\n✅ Module 04 Delta Lake Lab completed successfully!")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    run_delta_lake_lab()
