# Operational Performance — dbt

> **Hypothetical Showcase:** This dbt project demonstrates production data modelling patterns for engineering operational health metrics (incidents, SLOs, AI developer experience). All source names, company identifiers, and domains are fully anonymised. No proprietary data is included.

## Architecture

```
Source Systems (Fivetran replicas + internal raw tables)
                        │
            ────────────┼────────────
            │                       │
    Incident.io sources        AI Tool usage
    (CompanyA/B/C workspaces)  (LLM gateway, IDE tools,
                                chat assistants, etc.)
            │                       │
  ──────────────────────────────────────────────
  Staging Layer (views — thin ETL wrappers)
    stg_incident_io__companya__incidents
    stg_incident_io__companyb__incidents
    stg_incident_io__companyc__incidents
    stg_ai_telemetry__*
  ──────────────────────────────────────────────
  Intermediate Layer (views + incremental merges)
    int_ops__incidents_unioned      ← Jinja loop over 3 workspaces
    int_ai_telemetry__tool_daily_metrics ← Jinja loop over 7 tools
  ──────────────────────────────────────────────
  Marts Layer (tables, persist_docs, cluster_by)
    ops/
      fct_ops__incidents
      bridge_ops__incident_service
    ai_telemetry/
      fct_ai_telemetry__tool_daily_metrics
      fct_ai_telemetry__weekly_eng_ai_tokens
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Jinja loops over incident workspaces | 3 workspaces with near-identical schemas — avoid copy-paste, enforce consistency |
| Jinja loops over AI tool sources | 7 tools with different token reporting — unified UNION with per-tool token estimation |
| `incremental_predicates` on large marts | Reduces scan size on Snowflake incremental merges — significant cost saving |
| Dynamic `snowflake_warehouse` config | Smaller WH for incremental runs, larger for full refresh — right-size compute automatically |
| `generate_schema_name` macro | Environment-aware schema routing: dev → personal schema, prod → shared schema |

## Naming Conventions

| Layer | Prefix | Example |
|-------|--------|---------|
| Staging | `stg_<source>__<entity>` | `stg_incident_io__companya__incidents` |
| Intermediate | `int_<domain>__<concept>` | `int_ops__incidents_unioned` |
| Fact | `fct_<domain>__<concept>` | `fct_ops__incidents` |
| Bridge | `bridge_<domain>__<concept>` | `bridge_ops__incident_service` |
| Snapshot | `snp_<source>__<entity>` | `snp_ecommerce__products` |

## SQL Style

- CTE chains: `source` → `renamed` → (functional CTEs) → `final` → `select * from final`
- Column grouping comments: `-- -------- ids`, `-- -------- strings`, `-- -------- timestamps`
- Type casting: explicit `::timestamp_ntz`, `::float`, `::boolean`
- Soft-delete filter: `where not coalesce(_fivetran_deleted, false)` in all staging models
- Snowflake-native functions: `div0()`, `dateadd()`, `date_trunc()`, `lateral flatten()`

## Development

```bash
# Install
pip install dbt-snowflake && dbt deps

# Local run
dbt build --target local --select staging

# Run with governance checks
dbt build --select package:dbt_project_evaluator \
  --vars '{"dbt_project_evaluator_enabled": true}'

# Check freshness
dbt source freshness
```
