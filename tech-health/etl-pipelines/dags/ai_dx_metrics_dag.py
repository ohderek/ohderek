"""
DAG: ai_dx_metrics

Builds the AI Developer Experience metrics tables from multiple AI tool sources:
  - G2 (internal LLM gateway) — from int_g2_user_activity_daily
  - Goose          — endpoint usage table
  - ChatGPT        — weekly report distributed across active days
  - Cursor         — daily usage rows
  - Claude Code    — daily usage rows
  - Firebender     — daily usage rows
  - Amp            — daily usage rows

Mirrors the tech-health-metrics ai_dx_tables ETL jobs, converted to
idempotent, observable Airflow tasks.

Schedule: Daily at 16:30 UTC (mirrors meta.yaml: cron 0 14 * * *)

Pipeline:
    extract_all_tools ──► union_and_transform ──► enrich_with_dim_user
                                                        │
                                                        ▼
                                               load_fact_ai_tool_daily_metrics
                                                        │
                                                        ▼
                                          aggregate_weekly_eng_ai_tokens
                                                        │
                                                        ▼
                                                    data_quality_check
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Literal

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

AiTool = Literal["g2", "goose", "chatgpt", "cursor", "claude_code", "firebender", "amp"]

AI_TOOLS: list[AiTool] = ["g2", "goose", "chatgpt", "cursor", "claude_code", "firebender", "amp"]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
}

# Token estimation constants (for tools without native token reporting)
TOKENS_PER_REQUEST = 7_500    # avg tokens per chat/request interaction
TOKENS_PER_LINE    = 15       # avg tokens per line of code changed (Amp)

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ai_dx_metrics",
    description=(
        "Daily build of FACT_AI_TOOL_DAILY_METRICS and FACT_WEEKLY_ENG_AI_TOKENS "
        "from G2, Goose, ChatGPT, Cursor, Claude Code, Firebender, Amp"
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="30 14 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ai_dx", "metrics", "snowflake", "daily"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ------------------------------------------------------------------
    # Extract one task per tool (all run in parallel)
    # ------------------------------------------------------------------

    @task_group(group_id="extract_ai_tools")
    def extract_all_tools():

        @task(task_id="extract_g2")
        def extract_g2(**context) -> list[dict]:
            import os
            import snowflake.connector
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    LOWER(TRIM(user_id))           AS email,
                    activity_date                  AS date,
                    'G2'                           AS tool,
                    is_human_active                AS is_active,
                    COALESCE(total_tokens, 0)
                        * COALESCE(
                            success_count / NULLIF(request_count, 0), 1
                        )                          AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'request_count',      COALESCE(request_count, 0),
                        'success_count',      COALESCE(success_count, 0),
                        'error_count',        COALESCE(error_count, 0),
                        'total_tokens',       COALESCE(total_tokens, 0),
                        'is_human_active',    is_human_active
                    )                              AS metrics
                FROM REPORTING_TABLES.int_g2_user_activity_daily
                WHERE activity_date = '{run_date}'
                  AND user_id LIKE '%@example.com'
                  AND user_id IS NOT NULL
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            log.info("G2: extracted %d rows for %s", len(records), run_date)
            return records

        @task(task_id="extract_goose")
        def extract_goose(**context) -> list[dict]:
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    requester                           AS email,
                    DATE(TO_TIMESTAMP(request_time / 1000000000)) AS date,
                    'Goose'                             AS tool,
                    COUNT(*) > 0                        AS is_active,
                    SUM(input_token_count + output_token_count)
                        * COALESCE(
                            COUNT(CASE WHEN status_code = 200 THEN 1 END)
                            / NULLIF(COUNT(*), 0), 1
                        )                              AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'total_requests',      COUNT(*),
                        'successful_requests', COUNT(CASE WHEN status_code = 200 THEN 1 END),
                        'total_input_tokens',  COALESCE(SUM(input_token_count), 0),
                        'total_output_tokens', COALESCE(SUM(output_token_count), 0)
                    )                                  AS metrics
                FROM RAW_TABLES.goose_endpoint_usage
                WHERE DATE(TO_TIMESTAMP(request_time / 1000000000)) = '{run_date}'
                  AND requester LIKE '%@example.com'
                GROUP BY 1, 2
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        @task(task_id="extract_claude_code")
        def extract_claude_code(**context) -> list[dict]:
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    email,
                    date,
                    'Claude Code'                                       AS tool,
                    (COALESCE(tokens_input, 0) + COALESCE(tokens_output, 0)) > 0
                        OR COALESCE(lines_added, 0) > 0                AS is_active,
                    COALESCE(tokens_input, 0) + COALESCE(tokens_output, 0) AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'num_sessions',              COALESCE(num_sessions, 0),
                        'lines_added',               COALESCE(lines_added, 0),
                        'lines_removed',             COALESCE(lines_removed, 0),
                        'commits_by_claude_code',    COALESCE(commits_by_claude_code, 0),
                        'tokens_input',              COALESCE(tokens_input, 0),
                        'tokens_output',             COALESCE(tokens_output, 0),
                        'cost_usd_cents',            COALESCE(cost_usd_cents, 0)
                    )                                                   AS metrics
                FROM RAW_TABLES.claude_code_usage
                WHERE date = '{run_date}'
                  AND email LIKE '%@example.com'
                  AND actor_type = 'user_actor'
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        @task(task_id="extract_cursor")
        def extract_cursor(**context) -> list[dict]:
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    email,
                    DATE(date)                                          AS date,
                    'Cursor'                                            AS tool,
                    is_active,
                    (COALESCE(agent_requests, 0)
                     + COALESCE(chat_requests, 0)
                     + COALESCE(composer_requests, 0)) * {TOKENS_PER_REQUEST}   AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'agent_requests',    COALESCE(agent_requests, 0),
                        'chat_requests',     COALESCE(chat_requests, 0),
                        'total_accepts',     COALESCE(total_accepts, 0),
                        'total_tabs_accepted', COALESCE(total_tabs_accepted, 0)
                    )                                                   AS metrics
                FROM RAW_TABLES.cursor_usage
                WHERE DATE(date) = '{run_date}'
                  AND email LIKE '%@example.com'
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        @task(task_id="extract_chatgpt")
        def extract_chatgpt(**context) -> list[dict]:
            """
            ChatGPT data arrives as weekly aggregates.
            Distribute each weekly row evenly across active days in the reporting window.
            """
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                WITH base AS (
                    SELECT
                        email,
                        TRY_TO_DATE(first_day_active_in_period) AS first_active,
                        TRY_TO_DATE(last_day_active_in_period)  AS last_active,
                        DATEDIFF('day',
                            TRY_TO_DATE(first_day_active_in_period),
                            TRY_TO_DATE(last_day_active_in_period)) + 1 AS active_days,
                        TRY_CAST(messages AS FLOAT) AS weekly_messages
                    FROM RAW_TABLES.chatgpt_usage
                    WHERE email LIKE '%@example.com'
                      AND TRY_CAST(messages AS FLOAT) > 0
                      AND '{run_date}' BETWEEN
                            TRY_TO_DATE(first_day_active_in_period)
                        AND TRY_TO_DATE(last_day_active_in_period)
                ),
                distributed AS (
                    SELECT
                        email,
                        '{run_date}'::DATE                              AS date,
                        'ChatGPT'                                       AS tool,
                        TRUE                                            AS is_active,
                        (weekly_messages / active_days) * {TOKENS_PER_REQUEST}  AS total_tokens,
                        OBJECT_CONSTRUCT(
                            'daily_messages', weekly_messages / active_days,
                            'weekly_messages', weekly_messages
                        )                                               AS metrics
                    FROM base
                )
                SELECT * FROM distributed
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        @task(task_id="extract_firebender")
        def extract_firebender(**context) -> list[dict]:
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    email,
                    usage_date                                          AS date,
                    'Firebender'                                        AS tool,
                    (COALESCE(ide_opened, FALSE)
                     OR COALESCE(agent_prompts, 0) > 0
                     OR COALESCE(autocomplete_accepts, 0) > 0)         AS is_active,
                    (COALESCE(agent_prompts, 0)
                     + COALESCE(inline_edits, 0)
                     + COALESCE(autocomplete_accepts, 0)) * {TOKENS_PER_REQUEST} AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'agent_prompts',       COALESCE(agent_prompts, 0),
                        'autocomplete_accepts', COALESCE(autocomplete_accepts, 0),
                        'inline_edits',        COALESCE(inline_edits, 0)
                    )                                                   AS metrics
                FROM RAW_TABLES.firebender_usage
                WHERE usage_date = '{run_date}'
                  AND email LIKE '%@example.com'
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        @task(task_id="extract_amp")
        def extract_amp(**context) -> list[dict]:
            logical_date = context["logical_date"]
            run_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

            conn = _get_snowflake_conn()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    CONCAT(SPLIT_PART(user_email, '@', 1), '@example.com') AS email,
                    date,
                    'Amp'                                               AS tool,
                    (COALESCE(lines_added, 0) + COALESCE(lines_deleted, 0)) > 0 AS is_active,
                    (COALESCE(lines_added, 0)
                     + COALESCE(lines_deleted, 0)
                     + COALESCE(lines_modified, 0)) * {TOKENS_PER_LINE} AS total_tokens,
                    OBJECT_CONSTRUCT(
                        'lines_added',    COALESCE(lines_added, 0),
                        'lines_deleted',  COALESCE(lines_deleted, 0),
                        'lines_modified', COALESCE(lines_modified, 0),
                        'usage_usd',      COALESCE(usage_usd, 0)
                    )                                                   AS metrics
                FROM RAW_TABLES.amp_usage
                WHERE date = '{run_date}'
                  AND user_email IS NOT NULL
            """)
            records = _cursor_to_records(cur)
            cur.close(); conn.close()
            return records

        return {
            "g2":          extract_g2(),
            "goose":       extract_goose(),
            "claude_code": extract_claude_code(),
            "cursor":      extract_cursor(),
            "chatgpt":     extract_chatgpt(),
            "firebender":  extract_firebender(),
            "amp":         extract_amp(),
        }

    # ------------------------------------------------------------------
    # Union all tool extracts and load
    # ------------------------------------------------------------------

    @task(task_id="load_fact_ai_tool_daily_metrics")
    def load_fact_ai_tool_daily_metrics(records_by_tool: dict) -> dict:
        import hashlib
        import os
        import pandas as pd
        from src.loaders.snowflake_loader import SnowflakeLoader, SnowflakeLoaderConfig

        all_records: list[dict] = []
        for tool_key, records in records_by_tool.items():
            for r in records:
                pk_raw = f"{r.get('email','').lower()}|{r.get('date')}|{r.get('tool')}"
                r["id"] = hashlib.md5(pk_raw.encode()).hexdigest()
                all_records.append(r)

        log.info("Loading %d total AI metrics rows", len(all_records))

        config = SnowflakeLoaderConfig(
            database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
            schema="REPORTING_TABLES",
            target_table="FACT_AI_TOOL_DAILY_METRICS",
            merge_keys=["id"],
            update_columns=["total_tokens", "is_active", "metrics"],
        )
        with SnowflakeLoader(config) as loader:
            return loader.load(pd.DataFrame(all_records))

    # ------------------------------------------------------------------
    # Weekly aggregate (runs after daily metrics are loaded)
    # ------------------------------------------------------------------

    @task(task_id="build_weekly_eng_ai_tokens")
    def build_weekly_eng_ai_tokens() -> dict:
        """
        Runs the weekly rollup SQL directly in Snowflake after the daily
        table is populated. Uses CREATE OR REPLACE TABLE for simplicity
        (full refresh is cheap on this aggregation).
        """
        import os
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE OR REPLACE TABLE REPORTING_TABLES.FACT_WEEKLY_ENG_AI_TOKENS AS
            WITH weeks AS (
                SELECT DISTINCT DATE_TRUNC('week', date) AS week_start
                FROM REPORTING_TABLES.FACT_AI_TOOL_DAILY_METRICS
                WHERE date >= '2024-01-01'
            ),
            cohort AS (
                SELECT
                    SPLIT_PART(LOWER(email), '@', 1) AS ldap,
                    core_lead, core_plus_1, core_plus_2, core_plus_3,
                    function_l1, function_l2, function_l3,
                    sup_org_name, sup_org_id, sup_org_hierarchy
                FROM REPORTING_TABLES.DIM_USER
                WHERE is_current = TRUE
            ),
            user_weeks AS (
                SELECT w.week_start, c.*
                FROM weeks w CROSS JOIN cohort c
            ),
            daily_usage AS (
                SELECT
                    DATE_TRUNC('week', date) AS week_start,
                    SPLIT_PART(LOWER(email), '@', 1) AS ldap,
                    SUM(total_tokens) AS total_tokens
                FROM REPORTING_TABLES.FACT_AI_TOOL_DAILY_METRICS
                GROUP BY 1, 2
            ),
            user_weekly AS (
                SELECT
                    uw.week_start, uw.core_lead, uw.core_plus_1, uw.core_plus_2,
                    uw.core_plus_3, uw.function_l1, uw.function_l2, uw.function_l3,
                    uw.sup_org_name, uw.sup_org_id, uw.sup_org_hierarchy,
                    COALESCE(du.total_tokens, 0) AS user_weekly_tokens
                FROM user_weeks uw
                LEFT JOIN daily_usage du ON uw.week_start = du.week_start AND uw.ldap = du.ldap
            ),
            p95_caps AS (
                SELECT
                    week_start,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY user_weekly_tokens) AS p95_cap
                FROM user_weekly
                WHERE user_weekly_tokens > 0
                GROUP BY 1
            )
            SELECT
                uw.week_start,
                uw.core_lead, uw.core_plus_1, uw.core_plus_2, uw.core_plus_3,
                uw.function_l1, uw.function_l2, uw.function_l3,
                uw.sup_org_name, uw.sup_org_id, uw.sup_org_hierarchy,
                COUNT(*) AS team_size,
                SUM(LEAST(uw.user_weekly_tokens, COALESCE(p.p95_cap, 0))) AS total_winsorized_tokens,
                SUM(uw.user_weekly_tokens) AS total_raw_tokens
            FROM user_weekly uw
            LEFT JOIN p95_caps p ON uw.week_start = p.week_start
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            ORDER BY 1 DESC
        """)
        cur.close()
        conn.close()
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # Quality check
    # ------------------------------------------------------------------

    @task(task_id="data_quality_check", trigger_rule=TriggerRule.ALL_SUCCESS)
    def data_quality_check(daily_metrics: dict, weekly_status: dict) -> None:
        failures = []
        if daily_metrics.get("rows_inserted", -1) < 0:
            failures.append("FACT_AI_TOOL_DAILY_METRICS: unexpected result")
        if weekly_status.get("status") != "ok":
            failures.append("FACT_WEEKLY_ENG_AI_TOKENS: build failed")
        if failures:
            raise ValueError(f"Quality checks failed: {failures}")
        log.info("AI DX metrics loaded", extra={**daily_metrics})

    # ------------------------------------------------------------------
    # Wire up
    # ------------------------------------------------------------------

    extracted  = extract_all_tools()
    daily_load = load_fact_ai_tool_daily_metrics(extracted)
    weekly     = build_weekly_eng_ai_tokens()
    quality    = data_quality_check(daily_load, weekly)

    start >> extracted
    daily_load >> weekly
    quality >> end


# ---------------------------------------------------------------------------
# Shared helpers (module-level to keep tasks DRY)
# ---------------------------------------------------------------------------

def _get_snowflake_conn():
    import os
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        session_parameters={"QUERY_TAG": "airflow:ai_dx_metrics"},
    )


def _cursor_to_records(cursor) -> list[dict]:
    columns = [d[0].lower() for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
