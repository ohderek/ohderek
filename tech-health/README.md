# Tech Health Platform

> **Note:** This repository is a hypothetical showcase project demonstrating real-world data engineering patterns for operational tech health metrics. All company names, domains, and identifiers have been anonymised or replaced with generic placeholders. No proprietary data or credentials are included.

## Overview

End-to-end data platform for engineering health metrics: incident management, AI developer experience (AI DX), and SLO tracking. Built on a modern data stack with Apache Airflow for orchestration and dbt for transformation.

## Architecture

```
Source Systems
  ├── Incident.io (CompanyA / CompanyB / CompanyC workspaces) ── Fivetran ──┐
  ├── AI Tools (G2, Goose, Claude Code, Cursor, Firebender, Amp, ChatGPT)    ├──► Snowflake RAW
  └── SLO Monitoring (OEBOT, Vantage API)                                   ┘
                                     │
                                     ▼
                            Airflow ETL Pipelines
                         (ops_incidents_dag, ai_dx_metrics_dag)
                                     │
                                     ▼
                              Snowflake RAW Layer
                                     │
                                     ▼
                              dbt-tech-health
                     ┌───────────────┼───────────────┐
                 Staging          Intermediate       Marts
                (views)          (views/incr)      (tables)
                     │               │               │
            stg_incident_io    int_ops__            fct_ops__incidents
            stg_ai_telemetry   incidents_unioned    bridge_ops__incident_service
                               int_ai_telemetry__   fct_ai_telemetry__
                               tool_daily_metrics   tool_daily_metrics
                                                    fct_ai_telemetry__
                                                    weekly_eng_ai_tokens
```

## Projects

### [`dbt-tech-health/`](./dbt-tech-health)
dbt project with full staging → intermediate → marts layers.

- **Staging**: One model per source/entity, thin ETL wrappers with type casts
- **Intermediate**: Multi-source unions (Jinja-looped), enrichment, business logic
- **Marts**: Analyst-facing fact tables and dimensions, `persist_docs` enabled
- Custom macros: `generate_schema_name`, `hash_pii`, `normalize_order_status`
- Governance: `dbt_project_evaluator` runs in CI

### [`etl-pipelines/`](./etl-pipelines)
Apache Airflow DAGs with a modular Python ETL framework.

| DAG | Schedule | Description |
|-----|----------|-------------|
| `ops_incidents_dag` | Hourly | Merge incidents from 3 workspaces → `FACT_INCIDENTS` |
| `ai_dx_metrics_dag` | Daily 14:30 UTC | Union 7 AI tools → `FACT_AI_TOOL_DAILY_METRICS`, build weekly rollup |

**Framework components:**
- `BaseExtractor` — retry with exponential backoff, watermark-based incremental
- `APIExtractor` — paginated REST API extraction (cursor/offset/page)
- `DataTransformer` — schema contract, type coercion, NULL validation, schema drift logging
- `SnowflakeLoader` — COPY INTO staging → MERGE target pattern

## Key Design Patterns

| Pattern | Where Used | Why |
|---------|-----------|-----|
| Jinja looping over instances | `int_ops__incidents_unioned` | 3 incident workspaces with identical schema → avoid copy-paste |
| Jinja looping over AI tools | `int_ai_telemetry__tool_daily_metrics` | 7 tools unioned with different token estimation logic |
| Stage-and-merge loader | All Airflow loaders | Idempotent upserts without full-table rewrites |
| Incremental + lookback buffer | All mart models | Catch late-arriving updates without full refresh |
| Dynamic warehouse sizing | Mart configs | Right-size compute: incremental = smaller WH, full refresh = larger |
| PII hashing at staging | `stg_*__customers` | Raw PII never propagates into marts |
| SCD Type 2 snapshot | `snp_ecommerce__products` | Track historical cost prices for accurate margin attribution |

## Tech Stack

`Python 3.11` · `Apache Airflow 2.9` · `dbt Core` · `Snowflake` · `Pandas` · `GitHub Actions`
