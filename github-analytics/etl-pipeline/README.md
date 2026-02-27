# GitHub Analytics — ETL Pipeline

> **Hypothetical Showcase:** This project demonstrates Prefect-based data ingestion patterns for the GitHub Analytics V2 platform. All org names, tokens, and internal service references are fully anonymised. No proprietary data is included.

## Overview

Prefect flow that ingests GitHub pull request activity (PRs, reviews, commit files) via the GitHub REST API, serialises to Parquet, stages in GCS, and merges into Snowflake RAW for downstream dbt transformation.

## Flow

```
GitHub REST API
    │
    ▼
fetch_pull_requests  (paginated, per org, cursor-based via Link header)
    │
fetch_pr_reviews     (per PR, parallel)
fetch_pr_files       (per PR, parallel)
    │
    ▼
transform_pull_requests  (normalise, derive fields, generate row key)
    │
    ▼
upload_to_gcs         (partitioned: github/{entity}/org={org}/date={date}/)
    │
    ▼
copy_to_snowflake     (COPY INTO stage → MERGE INTO target → TRUNCATE stage)
```

## Key Design Decisions

- **GitHub API pagination**: Uses `Link` header cursor to walk all pages rather than offset-based pagination (which GitHub deprecates on large result sets)
- **PyArrow + Parquet**: Columnar serialisation avoids row-by-row inserts; Parquet is natively supported by Snowflake COPY INTO
- **GCS staging**: Files land in GCS before Snowflake load — enables reprocessing, debugging, and data lineage tracking
- **GCP Secret Manager blocks**: GitHub token stored as a Prefect `GcpSecret` block — never in environment variables or code
- **COPY + MERGE pattern**: Idempotent loads — running twice produces the same result

## Patterns Demonstrated

| Pattern | Implementation |
|---------|----------------|
| API pagination | `Link` header cursor loop in `httpx.Client` context manager |
| PyArrow serialisation | `pa.Table.from_pylist` → `pq.write_table` → `pa.BufferOutputStream` |
| GCS upload | `GcsBucket.write_path()` via Prefect GCP block |
| Snowflake COPY+MERGE | Staging table pattern with explicit TRUNCATE after merge |
| Secret management | `GcpSecret.load().read_secret()` |
| Retries | `@task(retries=3, retry_delay_seconds=60)` on all I/O tasks |
| Typed config | `pydantic_settings.BaseSettings` with `Field(alias=...)` |

## Tech Stack

`Python 3.11` · `Prefect 3.x` · `prefect-gcp` · `httpx` · `PyArrow` · `Snowflake` · `GCS` · `pydantic-settings`
