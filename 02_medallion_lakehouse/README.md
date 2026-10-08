# Module 02: Medallion Lakehouse Architecture

This module implements the industry-standard **Bronze ➔ Silver ➔ Gold** multi-hop data architecture used in modern enterprise lakehouses (Databricks, Snowflake, AWS EMR).

## The Medallion Pattern:
1. **Bronze (Raw Ingestion):**
   - Ingests raw data as-is with append-only semantics.
   - Preserves historical fidelity and ingestion timestamps (`_ingested_at`).
2. **Silver (Cleaned & Conformed):**
   - Applies schema enforcement and data cleansing.
   - Deduplicates records by primary key (`transaction_id`).
   - Normalizes and casts data types.
3. **Gold (Business Aggregates & KPIs):**
   - Aggregates metrics for analytics, dashboards, and downstream consumers.
   - Computes daily sales volume, customer lifetime value, and status summaries.

## Files:
- `pipeline.py`: Complete multi-hop data pipeline moving data across Bronze, Silver, and Gold Parquet layers.

## Run this lab:
```bash
uv run python 02_medallion_lakehouse/pipeline.py
```
