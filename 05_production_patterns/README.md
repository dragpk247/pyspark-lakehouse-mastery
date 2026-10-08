# Module 05: Production & Enterprise Lakehouse Patterns

This module covers advanced patterns required for deploying robust, secure, and compliant data pipelines in production.

## What You'll Learn:
- **Zero-Leakage PII Protection:** Cryptographic hashing (SHA-256 with salts) and masking of sensitive user fields (emails, names, IP addresses) before storage.
- **Broadcast Joins (`broadcast(df)`):** Optimizing joins between massive fact tables and small lookup/dimension tables by eliminating expensive network shuffles.
- **Adaptive Query Execution (AQE):** Enabling Spark dynamic partition coalescing and runtime skew join handling.
- **Idempotent Partition Overwrites:** Ensuring re-running pipelines never causes duplicated rows (`replaceWhere` / partition-level overwrites).
- **Data Quality & Schema Contracts:** Validating row counts, non-null guarantees, and distribution checks before publishing to downstream consumers.

## Files:
- `secure_automated_pipeline.py`: Production-grade pipeline implementing salted PII hashing, broadcast joins, and idempotent outputs.

## Run this lab:
```bash
uv run python 05_production_patterns/secure_automated_pipeline.py
```
