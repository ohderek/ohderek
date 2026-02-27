# GitHub Insights

> **Hypothetical Showcase:** Demonstrates two approaches to building an engineering velocity and DORA metrics platform on GitHub data — raw Snowflake SQL ETL jobs and a full dbt project. The ERD was designed from scratch and both implementations are production-grade patterns. All company names, org identifiers, and credentials are fully anonymised. No proprietary data is included.

---

## What it does

Ingests GitHub pull request activity (PRs, reviews, commits, file changes) from multiple GitHub organisations via Fivetran and a custom Prefect ingestion flow. Transforms the data into DORA metrics, PR velocity metrics, and code quality signals that engineering leaders can use to benchmark teams and identify bottlenecks.

**Key metrics powered by this platform:**
- **DORA Lead Time to Deploy** — first commit → staging → production (hours)
- **DORA Change Frequency** — deploys per day/week by service
- **PR cycle time** — time to first review, time to approval, time to merge
- **Review coverage** — % of PRs reviewed by someone other than the author
- **Code churn** — file-level change volume by language, team, and service (with noise filtering)
- **Team benchmarking** — P50/P75/P95 for all metrics by team, org, and function

---

## Data Model (ERD)

```
                    DIM_USER (SCD Type 2) ───────────────────────┐
                       │  one row per (engineer, period)         │
                       │  is_current = true for latest snapshot  │
                       │                                    FACT_PR_REVIEWS
                       │                                    FACT_PR_REVIEW_COMMENTS
                       │
FACT_PULL_REQUESTS ────┼──── FACT_PR_TIMES
  one row per PR        │      time_to_first_review
                        │      time_to_approval
                   BRIDGE_PR_LABELS ──── DIM_LABELS
                   many-to-many

FACT_COMMIT_FILES       one row per (commit_sha, filepath)
FACT_REPO_STATS         one row per (repo, date)
LEAD_TIME_TO_DEPLOY     one row per (PR, service, deploy event)

DIM_REPOSITORY          repo metadata + service ownership
```

**Why SCD Type 2 for users?** Engineers move teams. Without it, a PR merged when an engineer was on Team A would be attributed to Team B after they transferred. SCD2 ensures point-in-time team attribution is always correct.

---

## Two Approaches: Raw SQL vs dbt

The same data model is implemented twice — first as standalone Snowflake SQL jobs (V1 pattern), then refactored as a dbt project. This mirrors the real-world evolution of a data platform from an ad-hoc scripting approach to a maintainable, tested, documented codebase.

---

### Approach 1 — Raw SQL ETL Jobs ([`sql-jobs/`](./sql-jobs))

Standalone Snowflake SQL scripts. No framework, no abstraction — pure SQL you can run as Snowflake Tasks, schedule via Airflow `SnowflakeOperator`, or run manually during development.

#### [`raw_data_migration.sql`](./sql-jobs/raw_data_migration.sql)

Consolidates GitHub data from multiple Fivetran connectors (one per GitHub org) into unified RAW layer tables. The key challenge: the same pull request can appear in multiple shard connectors when a large org uses multiple Fivetran connections. This script handles it with `UNION BY NAME` (Snowflake-specific — matches columns by name, not position) followed by `QUALIFY ROW_NUMBER()` deduplication.

What it builds:
- `RAW_TABLES.PULL_REQUEST` — merged from 6 connectors (5 shards for Org A + 1 for Org B)
- `RAW_TABLES.PULL_REQUEST_REVIEW` — same multi-connector dedup pattern
- `RAW_TABLES.COMMIT_FILE` — file-level changes per commit with MD5 surrogate key
- `RAW_TABLES.USER_LOOKUP` — GitHub login → LDAP identity mapping from 3 sources

The `USER_LOOKUP` merge is particularly involved: it unions an identity cache, org email extraction, and a manual mapping table, then uses `QUALIFY ROW_NUMBER()` ordered by active HR system presence to resolve conflicts when one GitHub login maps to multiple LDAPs.

**To run:** Execute against your Snowflake account. Replace `FIVETRAN.GITHUB_ORG_A_*` references with your actual Fivetran schema names. Schedule as a Snowflake Task or Airflow DAG to run after Fivetran syncs complete.

#### [`reporting_data_model.sql`](./sql-jobs/reporting_data_model.sql)

Builds the reporting layer on top of the RAW tables. Three objects:

1. **`BRIDGE_GITHUB_LDAP`** — a deduplicated many-to-many mapping table between GitHub logins and LDAP usernames. Uses `HASHDIFF` (MD5 of the row's business key fields) to detect changes so unchanged rows aren't re-written on every run. Incremental filter: only processes records synced in the last 14 days.

2. **`FACT_PULL_REQUESTS`** — central PR fact table. Joins raw PR data with the repo table (to extract org from `full_name`), the user table (for login → LDAP resolution via the bridge), and applies author type classification (human / bot / service_account based on login patterns). Uses `HASHDIFF` to detect state changes (merged, closed, title updated).

3. **`FACT_PR_REVIEW_SUMMARY`** — aggregates review events per PR, excluding self-reviews (reviewer LDAP = author LDAP). Computes `time_to_first_review_sec`, `time_to_first_approval_sec`, and an `is_self_merged` flag for PRs with no external review.

**To run:** Depends on `raw_data_migration.sql` having populated the RAW tables first. Run daily after the raw migration completes.

#### [`lead_time_to_deploy.sql`](./sql-jobs/lead_time_to_deploy.sql)

The most complex script in the project — a 7-stage pipeline that links merged PRs to their first production deployment and calculates DORA lead time metrics.

```
Stage 1  Service registry → normalised repo paths
         (strips https://, handles monorepo sub-paths, deduplicates on reliability tier)

Stage 2  Commit → file → PR mapping
         (joins commits, files, PRs, repos; filters out merge commits)

Stage 3  File → service assignment (longest-prefix match)
         (STARTSWITH(filepath, repo_path) + ROW_NUMBER on LENGTH(repo_path) DESC)
         This is how monorepos work: github.com/org/java/payments-api beats
         github.com/org/java for files under the payments-api directory.

Stage 4  PR timeline construction
         (aggregates to PR × service grain: first_commit_date, merged_at, merge_commit_sha)

Stage 5  Staging deployment matching
         (5a) SHA match: exact merge_commit_sha = git_commit_sha in deployments
         (5b) Time-based fallback: first staging deployment after merged_at
         (5c) Dedup: SHA match wins; earliest time as tiebreak

Stage 6  Production deployment matching
         (same SHA match → time fallback cascade as Stage 5, but for production)

Stage 7  Final assembly
         Union production matches → dedup → LEFT JOIN staging → MERGE target
         Compute lead_time_hours, merge_to_prod_hours, time_to_staging_hours,
         time_on_staging_hours, and DORA bucket (elite/high/medium/low)
         Filter: WHERE first_prod_deploy_time IS NOT NULL (staging-only records excluded)
```

**Matching strategy detail:** SHA matching is 100% accurate but fails when deployments don't capture commit SHAs, or when squash merges create a new SHA. The time-based fallback takes the first deployment to the service after the PR merge time — this is probabilistic (80–85% accurate for moderate-velocity services) and is flagged with `match_scenario = 'time_after_merge'` so analysts can filter it out when precision matters.

**To run:** Depends on the reporting data model and a `OPS_PERFORMANCE.REPORTING_TABLES.FACT_DEPLOYMENTS` table with deployment events. Run daily. Typically takes 5–15 minutes depending on data volume.

---

### Approach 2 — dbt Project ([`dbt-github-insights/`](./dbt-github-insights))

The same logic refactored as a dbt project. Benefits over raw SQL: Jinja templating removes multi-org duplication, schema tests enforce data contracts, `persist_docs` pushes column descriptions to Snowflake, and `incremental_predicates` prunes micro-partitions on large mart models.

#### Staging layer (`models/staging/github/`)

**`stg_github__pull_requests.sql`** — normalises the raw PR table. Key additions over raw SQL:
- Resolves GitHub login → LDAP via a join to `ref('stg_identity__user_mapping')`
- Classifies `author_type` as `human` / `bot` using login pattern matching
- Adds `is_unresolved_identity` flag for PRs where LDAP resolution failed (useful for monitoring identity pipeline health)
- Casts all timestamps to `TIMESTAMP_NTZ` for consistent timezone handling

**`stg_github__commit_files.sql`** — normalises commit file records and adds noise flags:
- `is_lock_file` — package-lock.json, yarn.lock, Pipfile.lock etc.
- `is_pr_merge_commit` — "Merge pull request #" pattern
- `is_noisy_commit` — union of both above; excluded from velocity calculations

#### Intermediate layer (`models/intermediate/`)

**`int_github__pr_lifecycle.sql`** — aggregates review events onto the PR grain. Computes:
- `first_response_at` — first review or comment (any type)
- `first_reviewed_at` — first non-draft review submission
- `first_approved_at` — first `APPROVED` state review
- `branch_created_at` — `MIN(committer_date)` across all PR commits (proxy for when work started)
- All epoch timings (`time_to_first_review_epoch`, `time_to_merge_epoch`)
- Excludes self-reviews from all timing calculations

**`int_github__lead_time_to_deploy.sql`** — the cascading SHA-match algorithm as a dbt model:
```sql
exact_match AS (
  SELECT pr.*, d.deployment_completed_at, 'exact_sha' AS match_scenario
  FROM stg_github__pull_requests pr
  JOIN stg_deployments__events d ON pr.merge_commit_sha = d.git_commit_sha
),
staging_chain_match AS (
  -- Time-based fallback: first prod deployment after merge, for PRs not in exact_match
  ...
),
best_match AS (
  SELECT * FROM exact_match
  UNION ALL
  SELECT * FROM staging_chain_match WHERE pull_request_id NOT IN (SELECT pull_request_id FROM exact_match)
)
```

#### Marts layer (`models/marts/`)

**`fct_github__pull_requests.sql`** — central PR fact. Incremental merge on `pr_pk` with `incremental_predicates` for micro-partition pruning:
```sql
{{ config(
    incremental_strategy = 'merge',
    unique_key = 'pr_pk',
    cluster_by = ['pr_created_date'],
    incremental_predicates = ["pr_created_date >= dateadd('day', -7, current_date())"]
) }}
```

**`dim_github__users.sql`** — SCD Type 2 wrapper over a dbt snapshot. Maps `dbt_valid_from` → `effective_start_date`, `NVL(dbt_valid_to, '9999-12-31')` → `effective_end_date`. Joins to fct tables using `WHERE is_current = TRUE` for current state, or date-range join for point-in-time attribution.

**`fct_github__lead_time_to_deploy.sql`** — DORA lead time fact with bucket classification:

| Bucket | Lead Time |
|--------|-----------|
| `elite` | < 1 hour |
| `high` | < 1 day |
| `medium` | < 1 week |
| `low` | < 6 months |

---

### Prefect Ingestion Flow ([`etl-pipeline/`](./etl-pipeline))

A Prefect 3.x flow that fetches PR data directly from the GitHub REST API and loads it into Snowflake RAW — used when Fivetran isn't available or for more frequent refreshes.

**[`github_pr_ingestion_flow.py`](./etl-pipeline/prefect_flows/github_pr_ingestion_flow.py)** — what it does:

1. Loads the GitHub API token from GCP Secret Manager via `GcpSecret.load()` — the token never touches environment variables or code
2. For each configured org, calls `GET /orgs/{org}/pulls` with `state=all&sort=updated&direction=desc&since={lookback}`
3. Paginates using the `Link: <url>; rel="next"` response header (cursor-based — GitHub deprecates offset pagination on large orgs)
4. Transforms each PR batch to Parquet using PyArrow and uploads to a partitioned GCS path: `github/pull_request/org={org}/date={date}/`
5. COPYs from GCS into a Snowflake staging table, then MERGEs into the target

**To run:**

```bash
pip install prefect prefect-gcp httpx pyarrow snowflake-connector-python pydantic-settings

# Set up Prefect blocks (run once):
# 1. Create a GcsBucket block named 'gcs-data-bucket'
# 2. Create a GcpSecret block named 'github-api-token' containing your GitHub PAT

# Configure orgs and lookback via environment variables
export GITHUB_ORGS='["my-org-1", "my-org-2"]'
export DEPLOYMENT_ENV="prod"

# Run the flow
python etl-pipeline/prefect_flows/github_pr_ingestion_flow.py

# Or deploy to Prefect Cloud and schedule:
prefect deploy --name github-pr-ingestion --cron "0 */6 * * *"
```

---

## How to Implement

### Option A: Run raw SQL jobs directly

```sql
-- Step 1: Set up schemas and grant permissions
CREATE SCHEMA IF NOT EXISTS GITHUB_PIPELINES.RAW_TABLES;
CREATE SCHEMA IF NOT EXISTS GITHUB_PIPELINES.REPORTING_TABLES;

-- Step 2: Run migration script (populates RAW_TABLES)
-- Edit connector references to match your Fivetran schema names
<run sql-jobs/raw_data_migration.sql>

-- Step 3: Run reporting model (builds FACT and BRIDGE tables)
<run sql-jobs/reporting_data_model.sql>

-- Step 4: Run lead time pipeline (requires deployment events table)
<run sql-jobs/lead_time_to_deploy.sql>

-- Step 5: Schedule as Snowflake Tasks
CREATE TASK run_raw_migration
  WAREHOUSE = ETL_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  AS <raw_data_migration logic>;
```

### Option B: Run the dbt project

```bash
pip install dbt-snowflake

# Configure connection
cp dbt-github-insights/profiles.yml ~/.dbt/profiles.yml
# Edit profiles.yml with your Snowflake account, user, role, warehouse

# Install packages
cd dbt-github-insights && dbt deps

# Seed sources (user identity mapping)
dbt seed

# Run all models in dependency order
dbt run

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

---

## Tech Stack

`Python 3.11` · `Prefect 3.x` · `dbt Core` · `Snowflake` · `PyArrow` · `GitHub REST API` · `Fivetran` · `GCS`
