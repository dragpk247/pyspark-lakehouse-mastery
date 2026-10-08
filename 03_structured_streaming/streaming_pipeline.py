"""
Module 03: Structured Streaming in Apache Spark
================================================
Demonstrates production real-time stream processing:
1. Micro-batch ingestion from a directory stream.
2. Stateful tumbling-window event-time aggregation with Watermarking.
3. Writing stream outputs safely with checkpointing and fault tolerance.
"""

import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    current_timestamp,
    expr
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)


def get_spark_session() -> SparkSession:
    """Build local Spark session optimized for structured streaming."""
    return (
        SparkSession.builder
        .appName("03_Structured_Streaming_Lab")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def run_streaming_lab():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    base_dir = "/tmp/pyspark_streaming_lab"
    input_dir = f"{base_dir}/stream_input"
    checkpoint_dir = f"{base_dir}/checkpoints"
    output_dir = f"{base_dir}/stream_output"

    # Clean previous run
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("🚀 Module 03: Structured Streaming with Watermarking & Windowing")
    print("=" * 60)

    # 1. Define schema for incoming JSON events (schema enforcement is mandatory in streaming)
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("action", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("event_time", TimestampType(), False),
    ])

    # 2. Ingest stream from JSON files landing in input directory
    print(f"\n[1] Initializing stream reader monitoring: {input_dir}")
    raw_stream = (
        spark.readStream
        .schema(event_schema)
        .option("maxFilesPerTrigger", 1)  # Simulates continuous micro-batches
        .json(input_dir)
    )

    # 3. Apply Watermarking (handle late data) & 10-minute tumbling event-time windows
    print("[2] Configuring 10-minute event-time tumbling window with 5-minute late watermark...")
    windowed_aggregates = (
        raw_stream
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            window(col("event_time"), "10 minutes"),
            col("action")
        )
        .agg(
            expr("count(1) as total_events"),
            expr("sum(amount) as total_volume")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("action"),
            col("total_events"),
            col("total_volume")
        )
    )

    # 4. Start the streaming query writing to memory/console sink
    print("[3] Starting active stream query (format: console)...")
    query = (
        windowed_aggregates.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    # 5. Simulate live micro-batch data landing in real time
    print("\n[4] Simulating micro-batch file arrivals...")
    batch_files = [
        # Batch 1
        """{"event_id": "e1", "user_id": "u1", "action": "purchase", "amount": 120.50, "event_time": "2026-10-08T12:00:05"}
{"event_id": "e2", "user_id": "u2", "action": "purchase", "amount": 49.99, "event_time": "2026-10-08T12:02:15"}
{"event_id": "e3", "user_id": "u3", "action": "page_view", "amount": 0.0, "event_time": "2026-10-08T12:04:30"}""",
        # Batch 2
        """{"event_id": "e4", "user_id": "u1", "action": "purchase", "amount": 300.00, "event_time": "2026-10-08T12:05:00"}
{"event_id": "e5", "user_id": "u4", "action": "page_view", "amount": 0.0, "event_time": "2026-10-08T12:08:20"}"""
    ]

    for idx, batch_json in enumerate(batch_files, 1):
        target_file = f"{input_dir}/batch_{idx}_{int(time.time())}.json"
        with open(target_file, "w") as f:
            f.write(batch_json)
        print(f"    --> Emitted Micro-Batch {idx} to stream folder")
        time.sleep(3)

    query.processAllAvailable()
    query.stop()

    print("\n✅ Structured Streaming Lab completed successfully!")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    run_streaming_lab()
