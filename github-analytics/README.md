# GitHub Analytics V2

> **Hypothetical Showcase:** This project demonstrates advanced data modelling for an engineering velocity and DORA metrics platform. The data model is based on a real ERD I designed for a GitHub Analytics platform. All company names, internal tool references, and credentials are fully anonymised. No proprietary data is included.

## Data Model (ERD)

The GitHub Analytics V2 schema tracks the full engineering delivery lifecycle:

```
                    DIM_USER ────────────┐
                       │                 │
                       │            FACT_PR_REVIEWS
                       │            FACT_PR_REVIEW_COMMENTS
                       │
FACT_PULL_REQUESTS ────┼──── FACT_PR_TIMES
        │              │
        │         BRIDGE_PR_LABELS ──── DIM_LABELS
        │
FACT_COMMIT_FILES
FACT_REPO_STATS ──── DIM_REPOSITORY
LEAD_TIME_TO_DEPLOY
```

**Key metrics this model powers:**
- **DORA metrics**: Lead Time to Deploy, Deployment Frequency, Change Failure Rate, MTTR
- **PR velocity**: Time to first review, time to approve, time to merge
- **Code quality**: Review coverage, self-review rate, noisy commit detection
- **Team benchmarking**: Rolling P50/P75/P95 metrics by team, org, and function

## Architecture

```
GitHub API (REST + GraphQL)
           │
    Fivetran / custom extractor
           │
     Snowflake RAW Layer
           │
  ─────────────────────────────────────────
  dbt-github-analytics
    Staging    →  Intermediate  →  Marts
    ─────────────────────────────────────────
    stg_github__pull_requests    int_github__pr_lifecycle     fct_github__pull_requests
    stg_github__commits          int_github__lead_time         fct_github__lead_time_to_deploy
    stg_github__reviews                                         fct_github__commit_files
    stg_github__repositories                                    fct_github__pr_times
    stg_identity__user_mapping                                  dim_github__users (SCD2)
                                                                dim_github__repositories
                                                                bridge_github__pr_labels
  ─────────────────────────────────────────
           │
  Prefect: github-pr-ingestion-flow
           │
  BI Dashboards (DORA, Engineering Velocity, Code Quality)
```

## Projects

### [`dbt-github-analytics/`](./dbt-github-analytics)
Full staging → intermediate → marts dbt project.

- **Staging**: Thin wrappers normalising GitHub API/Fivetran raw tables
- **Intermediate**: PR lifecycle timing, lead time matching, bot filtering
- **Marts**:
  - `engineering_velocity/` — PR facts, PR times, lead time to deploy
  - `code_quality/` — commit files, change type classification

### [`etl-pipeline/`](./etl-pipeline)
Prefect flow for GitHub data ingestion with PyArrow + GCS staging.

## Interesting Engineering Challenges

- **GitHub → LDAP identity resolution**: GitHub usernames don't map 1:1 to internal LDAP. We built a multi-source resolution pipeline (Fivetran identity cache + manual mapping tables) with deduplication logic.
- **Lead time matching**: Matching a merged PR's commit SHA to the first production deployment requires a multi-step matching algorithm (exact SHA → staging deploy → prod deploy cascade).
- **Noisy commit detection**: Merge commits, lock file changes, and auto-generated files inflate commit metrics. These are flagged at the `FACT_COMMIT_FILES` level and excluded from velocity calculations.
- **SCD Type 2 user dimension**: Engineers move teams. `DIM_USER` is a Type 2 SCD so historical PR metrics correctly attribute to the team the engineer was on at the time.

## Tech Stack

`Python 3.11` · `Prefect 3.x` · `dbt Core` · `Snowflake` · `PyArrow` · `GitHub REST API` · `Fivetran`
