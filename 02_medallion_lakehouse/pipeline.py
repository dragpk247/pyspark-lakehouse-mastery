"""
02_medallion_lakehouse/pipeline.py
----------------------------------
Demonstrates a production Medallion Lakehouse Pipeline:
1. Bronze Layer: Ingest raw dirty JSON events
2. Silver Layer: Clean, filter nulls, deduplicate, cast timestamps
3. Gold Layer: Aggregate business KPIs ready for executive dashboards
"""

import os
import shutil
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sum as _sum, count

DATA_DIR = "/tmp/lakehouse_demo"

def setup_mock_raw_data():
    """Generates mock JSON event logs simulating real streaming/ingest sources."""
    bronze_source = os.path.join(DATA_DIR, "raw_landing")
    os.makedirs(bronze_source, exist_ok=True)
    
    events = [
        {"event_id": "e1", "user_id": "u1", "action": "checkout", "amount": 120.50, "ts": "2026-10-07 08:12:00"},
        {"event_id": "e2", "user_id": "u2", "action": "click", "amount": None, "ts": "2026-10-07 08:12:05"},
        {"event_id": "e3", "user_id": None, "action": "bot_scan", "amount": 0.0, "ts": "2026-10-07 08:12:10"}, # Null user (dirty)
        {"event_id": "e1", "user_id": "u1", "action": "checkout", "amount": 120.50, "ts": "2026-10-07 08:12:00"}, # Duplicate event!
        {"event_id": "e4", "user_id": "u3", "action": "checkout", "amount": 89.00, "ts": "2026-10-07 08:13:00"},
        {"event_id": "e5", "user_id": "u1", "action": "checkout", "amount": 45.00, "ts": "2026-10-07 08:14:00"},
    ]
    
    with open(os.path.join(bronze_source, "events_batch_1.json"), "w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    return bronze_source

def main():
    print("=" * 65)
    print("🏛️ Starting Medallion Lakehouse Pipeline (Bronze ➔ Silver ➔ Gold)")
    print("=" * 65)

    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    raw_landing_path = setup_mock_raw_data()

    spark = SparkSession.builder \
        .appName("MedallionPipelineDemo") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------------
    # 🥉 1. BRONZE LAYER: Raw Append-Only Ingest
    # -------------------------------------------------------------
    print("\n[🥉 1. Bronze Stage] Ingesting raw landing files as-is...")
    bronze_df = spark.read.json(raw_landing_path)
    bronze_path = os.path.join(DATA_DIR, "bronze_events")
    bronze_df.write.mode("overwrite").parquet(bronze_path)
    
    print(f"Bronze Count (includes duplicates & bad rows): {bronze_df.count()}")
    bronze_df.show(truncate=False)

    # -------------------------------------------------------------
    # 🥈 2. SILVER LAYER: Cleaning, Deduplication, Schema Enforcement
    # -------------------------------------------------------------
    print("\n[🥈 2. Silver Stage] Filtering corrupt records & deduplicating...")
    raw_bronze = spark.read.parquet(bronze_path)
    
    silver_df = raw_bronze \
        .filter(col("user_id").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("event_timestamp", to_timestamp(col("ts"))) \
        .withColumn("ingested_at", current_timestamp()) \
        .drop("ts")

    silver_path = os.path.join(DATA_DIR, "silver_events")
    silver_df.write.mode("overwrite").parquet(silver_path)

    print(f"Silver Count (cleaned & deduped): {silver_df.count()}")
    silver_df.show(truncate=False)

    # -------------------------------------------------------------
    # 🥇 3. GOLD LAYER: Aggregated Business KPIs
    # -------------------------------------------------------------
    print("\n[🥇 3. Gold Stage] Generating executive summary KPIs...")
    clean_silver = spark.read.parquet(silver_path)

    gold_df = clean_silver \
        .filter(col("action") == "checkout") \
        .groupBy("user_id").agg(
            count("event_id").alias("total_purchases"),
            _sum("amount").alias("lifetime_value")
        ).orderBy(col("lifetime_value").desc())

    gold_path = os.path.join(DATA_DIR, "gold_user_kpis")
    gold_df.write.mode("overwrite").parquet(gold_path)

    print("Gold Business KPI Table:")
    gold_df.show(truncate=False)

    print("=" * 65)
    print("✅ Pipeline Complete! Clean data stored in Bronze/Silver/Gold Parquet.")
    print("=" * 65)

    spark.stop()

if __name__ == "__main__":
    main()
