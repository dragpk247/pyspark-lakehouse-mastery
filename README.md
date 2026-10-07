# PySpark & Modern Lakehouse Mastery 🚀

A comprehensive, production-grade learning lab for **Apache Spark (PySpark 4.x)**, **Delta Lake**, and the **Modern Decoupled Lakehouse Architecture**.

---

## 🏛️ The Modern Lakehouse Architecture

Enterprise data platforms (Netflix, Uber, Apple, Databricks, Stripe) decoupled storage from compute:

```
 [Raw Event Streams / APIs / Logs]
                 │
                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ Storage Layer: Decoupled Object Storage (AWS S3 / MinIO)     │
 └──────────────────────────────────────────────────────────────┘
                 │
                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ Table Format: Delta Lake / Apache Iceberg (ACID, Parquet)    │
 │   • Bronze (Raw)  ➔  Silver (Cleaned)  ➔  Gold (KPIs)        │
 └──────────────────────────────────────────────────────────────┘
                 │
                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ Compute Engine: Distributed PySpark on Kubernetes            │
 └──────────────────────────────────────────────────────────────┘
                 │
                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ Serving & Consumers: Trino / DuckDB / Streamlit / AI & RAG   │
 └──────────────────────────────────────────────────────────────┘
```

---

## 📚 Learning Tracks in this Repository

| Module | Topic | Description |
| :--- | :--- | :--- |
| **[`01_fundamentals/`](./01_fundamentals/)** | Spark SQL & DataFrames | Distributed data loading, schema inference, filtering, aggregations, and query optimization. |
| **[`02_medallion_lakehouse/`](./02_medallion_lakehouse/)** | Medallion Lakehouse | Complete end-to-end pipeline: **Bronze (Raw)** ➔ **Silver (Cleaned/Deduped)** ➔ **Gold (Aggregated KPIs)**. |
| **[`03_structured_streaming/`](./03_structured_streaming/)** | Structured Streaming | Real-time low-latency stream processing over directory micro-batches. |
| **[`04_delta_lake/`](./04_delta_lake/)** | Delta Lake ACID & Time Travel | ACID transactions, schema enforcement, time-travel history queries, and instant rollback. |
| **[`05_production_patterns/`](./05_production_patterns/)** | Secure Automated Pipelines | Zero-leakage PII hashing, broadcast joins, Adaptive Query Execution (AQE), and idempotent partition overwrites. |

---

## ⚡ Quickstart Setup (Powered by `uv`)

This repository uses [`uv`](https://github.com/astral-sh/uv) for fast package management:

### 1. Run the interactive PySpark shell
```bash
uv run --with pyspark pyspark
```

### 2. Run Module 1 (Fundamentals)
```bash
uv run python 01_fundamentals/01_dataframe_basics.py
```

### 3. Run Module 2 (Medallion Lakehouse Pipeline)
```bash
uv run python 02_medallion_lakehouse/pipeline.py
```

### 4. Run Module 5 (Secure Automated Production Pipeline)
```bash
uv run python 05_production_patterns/secure_automated_pipeline.py --execution-date 2026-10-07
```


---

## 🏢 Enterprise Case Studies Included
* **Netflix:** Why and how they invented Apache Iceberg on AWS S3 to solve Spark cloud consistency.
* **Airbnb:** Why they created Apache Airflow to chain Spark Medallion DAGs.
* **Uber:** Processing millions of GPS pings per second with Kafka + PySpark.
* **Stripe:** Financial transaction reconciliation with zero-loss ACID guarantees.
