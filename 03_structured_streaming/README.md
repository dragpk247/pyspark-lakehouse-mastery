# Module 03: Structured Streaming with Apache Spark

Demonstrates stateful stream processing over incoming event batches.

## Features:
- Micro-batch stream reading using `spark.readStream`.
- Schema enforcement for stream integrity.
- Tumbling event-time aggregation windows.
- Late data handling using Spark `.withWatermark()`.
- Fault-tolerant checkpoints.

## Run this lab:
```bash
uv run python 03_structured_streaming/streaming_pipeline.py
```
