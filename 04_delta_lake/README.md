# Module 04: Delta Lake ACID & Time Travel

Demonstrates why modern Lakehouse architectures replace plain Parquet files with Delta Lake.

## Features:
- ACID Transactions on local and cloud storage.
- Transaction history audit log (`DESCRIBE HISTORY`).
- Time-Travel queries using `versionAsOf`.
- Schema enforcement preventing rogue pipelines from poisoning tables.

## Run this lab:
```bash
uv run python 04_delta_lake/delta_acid_timetravel.py
```
