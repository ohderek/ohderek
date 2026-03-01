<div align="center">

<img src="https://capsule-render.vercel.app/api?type=rect&color=0d0d0d&height=180&text=Operational+Performance&fontSize=48&fontColor=ff6b35&fontAlignY=52&animation=fadeIn&desc=Airflow+%C2%B7+dbt+%C2%B7+Snowflake+%C2%B7+Incident+Metrics&descSize=19&descAlignY=75&descColor=c9a84c" />

<br/>

<img src="https://readme-typing-svg.demolab.com?font=IBM+Plex+Mono&weight=600&size=19&duration=3200&pause=900&color=ff6b35&center=true&vCenter=true&width=700&height=45&lines=Incidents.+AI+DX.+SLOs.+One+platform.;3+workspaces.+1+Jinja+loop.+Zero+duplication.;MTTR+trending.+Week-over-week.+Measured.;Stage-and-merge.+Idempotent.+Always." alt="Typing SVG" />

</div>

<br/>

> **Hypothetical Showcase** — operational health metrics for incident management, AI developer experience, and SLO tracking. All company names and credentials are fully anonymised.

---

## ◈ Architecture

```mermaid
flowchart LR
    subgraph SRC["Sources"]
        A["Incident.io\n3 workspaces"]
        B["AI Tool APIs\n7 tools"]
    end

    subgraph AIRFLOW["Apache Airflow"]
        C["ops_incidents_dag\nhourly · parallel task_group"]
        D["ai_dx_metrics_dag\ndaily 14:30 UTC"]
    end

    subgraph RAW["Snowflake RAW"]
        E[("FACT_INCIDENTS\nTTD · TTR · TTS per workspace")]
        F[("FACT_AI_TOOL_DAILY_METRICS\n7 tools unified")]
    end

    subgraph DBT["dbt Core"]
        G["stg_ Staging\ncast · PII hash · dedup"]
        H["int_ Intermediate\nJinja loops · business logic"]
        I["fct_ Marts\nincremental · 7-day lookback"]
        G --> H --> I
    end

    J["BI Dashboards\nEngineering Health · AI Adoption · MTTR"]

    A --> C --> E
    B --> D --> F
    E & F --> G
    I --> J
```

Data flows from three Incident.io workspaces and seven AI tool APIs through Airflow ETL into Snowflake RAW, then transformed by dbt into analyst-facing mart tables. A Jinja loop in the intermediate layer means adding a fourth workspace or eighth tool is a one-line change.

---

## ◈ Quick Start

**Prerequisites:** Python 3.11+ · Apache Airflow 2.9 · dbt-snowflake · Snowflake account

```bash
# Airflow setup
pip install apache-airflow==2.9.0 apache-airflow-providers-snowflake requests pandas
cp etl-pipelines/dags/*.py $AIRFLOW_HOME/dags/
airflow variables set INCIDENT_IO_API_KEY "your-key"

# dbt setup
pip install dbt-snowflake
cp dbt-operational-performance/profiles.yml ~/.dbt/profiles.yml
cd dbt-operational-performance
dbt deps && dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt test
```

---

## ◈ ETL Pipelines — `etl-pipelines/`

### Framework Components (`src/`)

| Class | Purpose |
|---|---|
| `BaseExtractor` | Abstract base — extraction contract with retry/backoff, watermark-based incremental logic, batch iterator. Subclass to add any new source. |
| `APIExtractor` | Concrete REST extractor — `requests.Session` with retry adapter, auth headers from env vars, cursor/offset/page pagination |
| `DataTransformer` | Schema enforcement — validates required fields, coerces types, logs schema drift, raises `SchemaValidationError` on NULL violations |
| `SnowflakeLoader` | Idempotent loader — `write_pandas` to staging, `MERGE INTO` target, truncate stage. Re-running the same data is always safe. |

### `ops_incidents_dag.py` — hourly

Extracts from all three Incident.io workspaces in parallel via `@task_group`. Each workspace has its own watermark tracking `last_updated_at` — a failure in one doesn't reset the others.

```python
# Per-workspace TTD/TTR/TTS computed in Python before load
# Avoids epoch arithmetic in Snowflake SQL
event["ttd_sec"] = (detected_at - created_at).total_seconds()
event["ttr_sec"] = (resolved_at - detected_at).total_seconds()
event["tts_sec"] = (resolved_at - created_at).total_seconds()
```

### `ai_dx_metrics_dag.py` — daily 14:30 UTC

Pulls token usage and session counts from 7 AI tools (LLM gateway for Claude/GPT/Gemini, IDE tools like Cursor/Amp, chat tools). Tools don't always report raw token counts — estimation constants bridge the gap:

| Tool | Estimation method |
|---|---|
| Claude Code | 7,500 tokens/request |
| IDE completions | 15 tokens/line accepted |
| Chat sessions | reported tokens (where available) |

---

## ◈ dbt Project — `dbt-operational-performance/`

### Staging

One model per source/entity. Thin wrappers: cast types, rename to `snake_case`, hash PII, dedup. Each Incident.io workspace has slightly different field names — each gets its own staging model that normalises to a canonical schema.

| Model | Workspace quirk handled |
|---|---|
| `stg_incident_io__companya__incidents` | `impact_start` field (not `detected_at`) |
| `stg_incident_io__companyb__incidents` | `identified_at` → mapped to `detected_at` |
| `stg_incident_io__companyc__incidents` | No `fixed_at` column — defaults to NULL |

### Intermediate — Jinja Loop Pattern

The key reuse pattern in this project. Adding a 4th workspace requires one line:

```sql
-- int_ops__incidents_unioned.sql
{% set instances = ['companya', 'companyb', 'companyc'] %}

{% for instance in instances %}
    SELECT *, '{{ instance }}' AS workspace
    FROM {{ ref('stg_incident_io__' ~ instance ~ '__incidents') }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
```

Same pattern used in `int_ai_telemetry__tool_daily_metrics` over 7 AI tools.

### Marts

| Model | Strategy | Notes |
|---|---|---|
| `fct_ops__incidents` | `incremental · merge` | `CLUSTER BY incident_date` · dynamic warehouse sizing |
| `bridge_ops__incident_service` | `table` | M:M incident → service with `HASHDIFF` change detection |
| `fct_ai_telemetry__tool_daily_metrics` | `incremental` | Daily per-engineer AI tool usage |
| `fct_ai_telemetry__weekly_eng_ai_tokens` | `table` | P50/P75/P95 token estimates per engineer |

**7-day lookback buffer** on all incremental models: incidents are updated days after resolution. Without the buffer, late updates would be missed.

### Macros

| Macro | What it does |
|---|---|
| `generate_schema_name` | In `prod`: target schema. In `dev`: `sandbox_{user}_` prefix. Prevents dev from overwriting production. |
| `hash_pii` | `SHA2(LOWER(TRIM(col)), 256)` at staging — raw PII never propagates to marts |
| `safe_divide` | `IFF(denominator = 0, NULL, numerator / denominator)` — zero-safe ratios throughout |
| `date_spine_filter` | Date spine CTE for gap-filling time-series facts |

---

## ◈ Key Design Patterns

| Pattern | Where | Why |
|---|---|---|
| Jinja loop over instances | `int_ops__incidents_unioned`, `int_ai_telemetry__*` | One-line change to add a workspace or tool |
| Stage-and-merge | All Airflow loaders | Idempotent — safe to re-run on failure without duplicates |
| 7-day incremental lookback | All mart models | Catches late-arriving incident updates without full scans |
| `generate_schema_name` macro | dbt project | Each developer works in isolated `sandbox_{user}` schema |
| PII hashing at staging | `stg_*` models | Raw values never reach marts or BI tools |
| Dynamic warehouse sizing | Mart configs | `ETL_SMALL` for incremental, `ETL_MEDIUM` for full refresh — right-size compute |

---

## ◈ Tech Stack

<div align="center">

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)

</div>

---

<div align="center">

<a href="https://www.linkedin.com/in/derek-o-halloran/">
  <img src="https://img.shields.io/badge/LINKEDIN-0d0d0d?style=for-the-badge&logo=linkedin&logoColor=ff6b35" />
</a>&nbsp;
<a href="https://github.com/ohderek/data-engineering-portfolio">
  <img src="https://img.shields.io/badge/DATA_PORTFOLIO-0d0d0d?style=for-the-badge&logo=github&logoColor=ff6b35" />
</a>

<br/><br/>

<img src="https://capsule-render.vercel.app/api?type=waving&color=0,0d0d0d,100,1a1a2e&height=100&section=footer" />

</div>
