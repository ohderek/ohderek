# Operational Performance Platform

> **Hypothetical Showcase:** Demonstrates production data engineering patterns for operational health metrics — incident management, AI developer experience (AI DX), and SLO tracking. Built on Apache Airflow, dbt, and Snowflake. All company names, workspace identifiers, and credentials are fully anonymised. No proprietary data is included.

---

## What it does

An end-to-end data platform that answers questions like:
- How long does it take our teams to detect, respond to, and resolve incidents?
- Which AI coding tools are being adopted, and how much are they being used?
- How is engineering health trending week-over-week across different business units?

Data flows from three Incident.io workspaces and seven AI tool APIs through Airflow ETL pipelines into a Snowflake RAW layer, then transformed by dbt into analyst-facing mart tables.

---

## Architecture

```
Source Systems
  ├── Incident.io (CompanyA workspace)  ─┐
  ├── Incident.io (CompanyB workspace)   ├── Fivetran / REST API ──► Snowflake RAW
  ├── Incident.io (CompanyC workspace)  ─┘
  │
  ├── AI Tool APIs (7 tools: LLM gateway, IDE assistants, chat tools)
  └── SLO Monitoring APIs
             │
             ▼
   Apache Airflow (ETL Pipelines)
   ├── ops_incidents_dag     — hourly, merges 3 workspaces into unified FACT_INCIDENTS
   └── ai_dx_metrics_dag     — daily, unions 7 tools into FACT_AI_TOOL_DAILY_METRICS
             │
             ▼
      Snowflake RAW Layer
             │
             ▼
      dbt-operational-performance
   ┌──────────────────────────────────────────────┐
   │  Staging (views)                             │
   │    stg_incident_io__companya__incidents       │
   │    stg_incident_io__companyb__incidents       │
   │    stg_incident_io__companyc__incidents       │
   │    stg_ai_telemetry__[tool]__daily_metrics    │
   │                    │                         │
   │  Intermediate (views / incremental)          │
   │    int_ops__incidents_unioned                 │  ← Jinja loop unions all workspaces
   │    int_ai_telemetry__tool_daily_metrics       │  ← Jinja loop unions all 7 AI tools
   │                    │                         │
   │  Marts (tables, persist_docs)                │
   │    fct_ops__incidents                         │
   │    bridge_ops__incident_service               │
   │    fct_ai_telemetry__tool_daily_metrics       │
   │    fct_ai_telemetry__weekly_eng_ai_tokens     │
   └──────────────────────────────────────────────┘
             │
             ▼
   BI Dashboards (Engineering Health · AI Adoption · MTTR Trends)
```

---

## Projects

### [`etl-pipelines/`](./etl-pipelines)

Apache Airflow DAGs built on a modular Python ETL framework. Each DAG follows the same extract → transform → load pattern using a shared class hierarchy, making it easy to add new sources.

#### Framework components (`src/`)

| Class | File | What it does |
|-------|------|-------------|
| `BaseExtractor` | `extractors/base_extractor.py` | Abstract base class. Defines the extraction contract: `extract()` with configurable retry/backoff, watermark-based incremental logic, and a batch iterator for large payloads. Subclass this to add any new data source. |
| `APIExtractor` | `extractors/api_extractor.py` | Concrete REST API extractor. Manages a `requests.Session` with a retry adapter, injects auth headers from environment variables, and handles cursor/offset/page-based pagination. Pass `pagination_type="cursor"` or `"offset"` in config. |
| `DataTransformer` | `transformers/data_transformer.py` | Schema contract enforcement. Validates required fields, coerces types (string→datetime, string→float), and logs schema drift when unexpected columns arrive. Raises `SchemaValidationError` for NULL violations on required fields. |
| `SnowflakeLoader` | `loaders/snowflake_loader.py` | Idempotent Snowflake loader. Writes a DataFrame to a staging table using `write_pandas`, then executes a `MERGE INTO` the target table, then truncates the stage. Re-running the same data is safe. |

#### DAGs

**`ops_incidents_dag.py`** — runs hourly

Extracts from all three Incident.io workspaces in parallel using a `@task_group`, transforms each workspace's incidents (computing TTD, TTR, TTS, impact duration), and merges into `FACT_INCIDENTS`.

Key decisions:
- Parallel `@task_group` extraction: workspaces are independent, so they run concurrently to reduce wall-clock time
- Per-workspace watermark: each workspace tracks its own `last_updated_at` so a failure in one doesn't reset the others
- TTD/TTR/TTS computed in Python before load — avoids complex Snowflake SQL for epoch arithmetic

**`ai_dx_metrics_dag.py`** — runs daily at 14:30 UTC

Pulls daily token usage and session counts from 7 AI tool APIs (LLM gateway for Claude/GPT/Gemini, IDE tools like Cursor/Amp, chat tools). Builds both a daily grain fact table and a weekly rollup with per-engineer token estimates.

Key decisions:
- Token estimation constants per tool (e.g. 7,500 tokens/request for Claude Code, 15 tokens/line for IDE completions) — tools don't always report raw token counts
- Weekly rollup built as a second downstream task, not a separate DAG run, to guarantee consistency

---

### [`dbt-operational-performance/`](./dbt-operational-performance)

Full dbt project with staging → intermediate → marts layering, following the conventions in the [dbt best practices guide](https://docs.getdbt.com/best-practices).

#### Staging layer (`models/staging/`)

One model per source/entity. Thin wrappers that cast types, rename columns to snake_case, and apply PII hashing — nothing more. Raw source tables are never referenced outside staging.

**Multi-workspace incident staging** — each Incident.io workspace has slightly different field names, so each gets its own staging model that normalises to a canonical schema:

| Model | Workspace quirk handled |
|-------|------------------------|
| `stg_incident_io__companya__incidents.sql` | Uses `impact_start` field (not `detected_at`) |
| `stg_incident_io__companyb__incidents.sql` | Uses `identified_at` → mapped to `detected_at` |
| `stg_incident_io__companyc__incidents.sql` | No `fixed_at` column — defaults to NULL |

Each model deduplicates using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)` so re-ingesting the same incident is safe.

#### Intermediate layer (`models/intermediate/`)

Business logic lives here — never in staging, never in marts.

**`int_ops__incidents_unioned.sql`** — the key pattern in this project:

```sql
{% set instances = ['companya', 'companyb', 'companyc'] %}

{% for instance in instances %}
    SELECT *, '{{ instance }}' AS workspace
    FROM {{ ref('stg_incident_io__' ~ instance ~ '__incidents') }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
```

This Jinja loop unions all three workspaces into a single table, then computes:
- `time_to_detect_sec` — seconds from incident creation to detection
- `time_to_respond_sec` — seconds from detection to first responder assignment
- `time_to_resolve_sec` — seconds from creation to resolution
- `impact_duration_min` — total customer-facing impact window
- `temperature` — severity classification (P0/P1/P2/P3 → critical/high/medium/low)

**`int_ai_telemetry__tool_daily_metrics.sql`** — same Jinja loop pattern over 7 AI tools, normalising different API response shapes into a unified schema with estimated token counts.

#### Marts layer (`models/marts/`)

Analyst-facing tables. Incremental merge strategy with a 7-day lookback buffer to catch late-arriving updates without full table scans.

- `fct_ops__incidents.sql` — central incident fact, `CLUSTER BY (incident_date)`, dynamic warehouse sizing (`ETL_SMALL` for incremental, `ETL_MEDIUM` for full refresh)
- `bridge_ops__incident_service.sql` — many-to-many incident → affected service mapping with `HASHDIFF` for change detection
- `fct_ai_telemetry__tool_daily_metrics.sql` — daily AI tool usage per engineer
- `fct_ai_telemetry__weekly_eng_ai_tokens.sql` — weekly rollup with P50/P75/P95 token estimates

#### Macros (`macros/`)

| Macro | What it does |
|-------|-------------|
| `generate_schema_name.sql` | Environment-aware schema routing. In `prod`, uses target schema. In `dev`, prefixes with `sandbox_{user}_`. Prevents dev models from overwriting production. |
| `hash_pii` | `SHA2(LOWER(TRIM(col)), 256)` — hashes PII (emails, names) at the staging layer. Hashed values propagate to marts; raw values never leave staging. |
| `safe_divide` | `IFF(denominator = 0, NULL, numerator / denominator)` — avoids division-by-zero in ratio calculations. |
| `date_spine_filter` | Generates a date spine CTE for filling gaps in time-series fact tables. |

---

## How to Implement

### 1. Set up Airflow

```bash
pip install apache-airflow==2.9.0 \
  apache-airflow-providers-snowflake \
  requests pandas

# Place DAG files in your $AIRFLOW_HOME/dags/ directory
cp etl-pipelines/dags/*.py $AIRFLOW_HOME/dags/

# Set Airflow variables for API endpoints and credentials
airflow variables set INCIDENT_IO_API_KEY "your-key"
airflow variables set SNOWFLAKE_CONN_ID "snowflake_default"
```

### 2. Set up dbt

```bash
pip install dbt-snowflake

# Copy profiles.yml and update with your Snowflake credentials
cp dbt-operational-performance/profiles.yml ~/.dbt/profiles.yml

# Install dbt packages (dbt_utils, dbt_project_evaluator)
cd dbt-operational-performance && dbt deps

# Run staging models first, then intermediate, then marts
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run tests
dbt test
```

### 3. Adapt to your own sources

To add a new incident workspace or AI tool:

1. Create a new staging model following the existing naming convention: `stg_<source>__<workspace>__<entity>.sql`
2. Add the new instance name to the Jinja `{% set instances = [...] %}` list in the relevant intermediate model
3. Add corresponding source definition to `_sources_*.yml`
4. Run `dbt run --select +fct_ops__incidents` to recompute downstream models

---

## Key Design Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Jinja loop over instances | `int_ops__incidents_unioned`, `int_ai_telemetry__tool_daily_metrics` | Adding a 4th workspace requires one line change, not a new model. Eliminates N×copy-paste. |
| Stage-and-merge | All Airflow loaders | Idempotent upserts — safe to re-run on failure without duplicates or data loss. |
| 7-day incremental lookback | All mart models | Catches late-arriving updates (incidents can be updated days after resolution) without a full table scan. |
| `generate_schema_name` macro | dbt project | Prevents dev runs from touching production schemas. Each developer works in their own `sandbox_{user}` namespace. |
| PII hashing at staging | `stg_*` models | Raw PII never propagates into marts or BI tools — hashed values are used for joins instead. |
| Dynamic warehouse sizing | Mart configs | `ETL_SMALL` for incremental runs (small result set), `ETL_MEDIUM` for full refresh (large scan) — right-size compute to keep costs proportional to work done. |

---

## Tech Stack

`Python 3.11` · `Apache Airflow 2.9` · `dbt Core` · `Snowflake` · `Pandas` · `GitHub Actions`
