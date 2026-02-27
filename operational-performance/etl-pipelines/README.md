# Operational Performance — ETL Pipelines

> **Hypothetical Showcase:** This project demonstrates data engineering patterns for operational tech health metrics pipelines. All company names, API endpoints, credentials, and identifiers are fully anonymised or generic. No proprietary data is included.

## Overview

Two orchestration frameworks are demonstrated side-by-side: **Apache Airflow** (complex multi-step batch pipelines) and **Prefect** (lightweight event-driven flows with GCS staging). Both target Snowflake as the data warehouse.

## Airflow DAGs

Located in [`dags/`](./dags)

| DAG | Schedule | Description |
|-----|----------|-------------|
| `ops_incidents_dag` | Hourly | Extract incidents from 3 workspaces → transform (TTD/TTR/TTS) → MERGE `FACT_INCIDENTS` |
| `ai_dx_metrics_dag` | Daily 14:30 UTC | Union 7 AI tool sources → `FACT_AI_TOOL_DAILY_METRICS` → weekly rollup |
| `ecommerce_ingestion_dag` | Hourly | E-commerce orders + items → Snowflake RAW via stage-and-merge |

**Framework components** ([`src/`](./src)):

```
src/
├── extractors/
│   ├── base_extractor.py     # Abstract base: retry, backoff, watermark, batch iteration
│   └── api_extractor.py      # Paginated REST (offset/cursor/page), session-based auth
├── transformers/
│   └── data_transformer.py   # Schema contract enforcement, type coercion, NULL validation
└── loaders/
    └── snowflake_loader.py   # write_pandas → staging table → MERGE → truncate stage
```

## Prefect Flows

Located in [`prefect_flows/`](./prefect_flows)

| Flow | Trigger | Description |
|------|---------|-------------|
| `incident_ingestion_flow` | Scheduled / API trigger | Fetch incidents via REST → transform + Parquet → GCS → Snowflake MERGE |
| `ai_tool_metrics_flow` | Daily schedule | Fetch LLM gateway + IDE metrics → normalise (token estimation) → GCS → Snowflake |

### Prefect Patterns Demonstrated

- **`@flow` + `@task` decomposition** — each discrete I/O operation is a retryable task
- **GCP Secret Manager** via `prefect_gcp` blocks — no credentials in code or environment
- **GCS staging** with `GcsBucket.write_path()` → Snowflake COPY INTO (not row-by-row inserts)
- **PyArrow** for efficient in-memory columnar serialisation to Parquet
- **`pydantic_settings` BaseSettings** for typed, environment-variable-driven configuration
- **Prefect native states** (`Failed`, `Cancelled`) for explicit failure signalling
- Token estimation constants for tools without native token reporting (requests × avg tokens/request)

## Design Principles

- **Idempotent** — every pipeline can be re-run for any time window without duplicates
- **Observable** — structured logging on every task, Snowflake `QUERY_TAG` for all sessions
- **Fail fast** — data quality checks gate downstream tasks; failures alert immediately
- **Credential-safe** — secrets via GCP Secret Manager blocks, never in code or logs
- **Incremental first** — watermark-based incremental with configurable lookback buffer

## Tech Stack

`Python 3.11` · `Apache Airflow 2.9` · `Prefect 3.x` · `Snowflake` · `PyArrow` · `Pandas` · `httpx` · `pydantic-settings` · `prefect-gcp`
