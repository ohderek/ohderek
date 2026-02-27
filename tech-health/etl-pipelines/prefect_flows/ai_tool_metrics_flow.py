"""
Prefect flow: ai-tool-metrics

Hypothetical showcase flow demonstrating how Prefect would aggregate daily
AI developer tool usage across multiple internal and third-party tools into
a unified analytics table.

This is a sample implementation. All endpoints, credentials, and company
references are fully generic/anonymised.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from typing import Any, Literal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task, get_run_logger
from prefect.states import Failed
from prefect_gcp import GcsBucket
from prefect_gcp.secret_manager import GcpSecret
from pydantic import Field
from pydantic_settings import BaseSettings

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class AiToolFlowSettings(BaseSettings):
    deployment_env: str        = Field(default="stage", alias="DEPLOYMENT_ENV")
    gcs_bucket_block: str      = Field(default="gcs-data-bucket", alias="GCS_BUCKET_BLOCK")
    llm_api_secret_block: str  = Field(default="llm-gateway-key", alias="LLM_API_SECRET_BLOCK")
    ide_api_secret_block: str  = Field(default="ide-tool-key", alias="IDE_API_SECRET_BLOCK")
    ai_gateway_base_url: str   = Field(default="https://api.llm-gateway.internal/v1", alias="LLM_API_BASE_URL")
    ide_tool_base_url: str     = Field(default="https://api.ide-tool.example.com/v1", alias="IDE_API_BASE_URL")
    snowflake_schema: str      = "REPORTING_TABLES"
    tokens_per_request: int    = 7_500     # Estimation constant: avg tokens per chat request
    tokens_per_line: int       = 15        # Estimation constant: tokens per LOC changed

    @property
    def is_prod(self) -> bool:
        return self.deployment_env.lower() == "prod"


settings = AiToolFlowSettings()

AiTool = Literal["llm_gateway", "ide_assistant", "code_completion", "chat_assistant"]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=30)
def fetch_llm_gateway_metrics(
    api_key: str,
    report_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily LLM gateway request metrics aggregated by user.

    Args:
        api_key: Bearer token for the LLM gateway API.
        report_date: The date to pull metrics for.

    Returns:
        List of dicts, one per active user per model.
    """
    logger = get_run_logger()
    logger.info("Fetching LLM gateway metrics for %s", report_date)

    with httpx.Client(timeout=60.0) as client:
        resp = client.get(
            f"{settings.ai_gateway_base_url}/usage/daily",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"date": report_date.isoformat(), "granularity": "user"},
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])

    logger.info("LLM gateway: %d user rows for %s", len(data), report_date)
    return data


@task(retries=3, retry_delay_seconds=30)
def fetch_ide_tool_metrics(
    api_key: str,
    report_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily IDE assistant usage metrics per user.

    Args:
        api_key: Bearer token for the IDE tool API.
        report_date: The date to pull metrics for.

    Returns:
        List of dicts with code-level activity per user.
    """
    logger = get_run_logger()
    logger.info("Fetching IDE tool metrics for %s", report_date)

    with httpx.Client(timeout=60.0) as client:
        resp = client.get(
            f"{settings.ide_tool_base_url}/analytics/daily",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"date": report_date.isoformat()},
        )
        resp.raise_for_status()
        data = resp.json().get("usage", [])

    logger.info("IDE tool: %d user rows for %s", len(data), report_date)
    return data


@task
def normalize_llm_gateway(
    raw: list[dict[str, Any]],
    report_date: date,
) -> list[dict[str, Any]]:
    """
    Normalise LLM gateway records to the canonical ai_tool_metrics schema.
    Uses native token counts (input + output tokens from the API response).
    """
    rows = []
    for r in raw:
        email = (r.get("user_email") or "").lower().strip()
        if not email:
            continue
        input_tokens  = int(r.get("input_tokens", 0))
        output_tokens = int(r.get("output_tokens", 0))
        total_tokens  = input_tokens + output_tokens
        rows.append({
            "email":        email,
            "date":         report_date.isoformat(),
            "tool":         "LLM Gateway",
            "is_active":    total_tokens > 0,
            "total_tokens": total_tokens,
            "metrics": {
                "request_count":   int(r.get("request_count", 0)),
                "success_count":   int(r.get("success_count", 0)),
                "error_count":     int(r.get("error_count", 0)),
                "input_tokens":    input_tokens,
                "output_tokens":   output_tokens,
                "active_duration_minutes": float(r.get("active_duration_minutes", 0)),
            },
        })
    return rows


@task
def normalize_ide_tool(
    raw: list[dict[str, Any]],
    report_date: date,
) -> list[dict[str, Any]]:
    """
    Normalise IDE assistant records. Estimates token count from:
    - Chat/agent requests × TOKENS_PER_REQUEST
    - Lines changed × TOKENS_PER_LINE (for code completions)
    """
    rows = []
    for r in raw:
        email = (r.get("email") or "").lower().strip()
        if not email:
            continue

        agent_requests   = int(r.get("agent_requests", 0))
        chat_requests    = int(r.get("chat_requests", 0))
        lines_added      = int(r.get("autocomplete_lines_added", 0))
        total_tokens     = (
            (agent_requests + chat_requests) * settings.tokens_per_request
            + lines_added * settings.tokens_per_line
        )

        rows.append({
            "email":        email,
            "date":         report_date.isoformat(),
            "tool":         "IDE Assistant",
            "is_active":    agent_requests > 0 or chat_requests > 0 or lines_added > 0,
            "total_tokens": total_tokens,
            "metrics": {
                "agent_requests":          agent_requests,
                "chat_requests":           chat_requests,
                "autocomplete_lines_added": lines_added,
                "total_accepts":           int(r.get("total_accepts", 0)),
                "total_rejects":           int(r.get("total_rejects", 0)),
            },
        })
    return rows


@task
def build_parquet(rows: list[dict[str, Any]]) -> bytes:
    """
    Serialise unified metrics rows to Parquet.
    Adds a row-level id: md5(email|date|tool).
    """
    logger = get_run_logger()
    for r in rows:
        r["id"] = hashlib.md5(
            f"{r['email']}|{r['date']}|{r['tool']}".encode()
        ).hexdigest()

    table = pa.Table.from_pylist(rows)
    buf   = pa.BufferOutputStream()
    pq.write_table(table, buf)
    logger.info("Built Parquet: %d rows", len(rows))
    return buf.getvalue().to_pybytes()


@task
def upload_parquet(parquet_bytes: bytes, report_date: date) -> str:
    """Upload unified Parquet file to GCS staging bucket."""
    logger = get_run_logger()
    gcs  = GcsBucket.load(settings.gcs_bucket_block)
    path = f"ai_tool_metrics/date={report_date.isoformat()}/metrics.parquet"
    gcs.write_path(path=path, content=parquet_bytes)
    uri  = f"gs://{gcs.bucket}/{path}"
    logger.info("Uploaded to %s", uri)
    return uri


@task
def merge_to_snowflake(gcs_uri: str) -> dict[str, int]:
    """COPY Parquet from GCS into Snowflake, then MERGE into target table."""
    import os
    import snowflake.connector

    logger = get_run_logger()
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL__MEDIUM"),
        database="TECH_HEALTH",
        schema=settings.snowflake_schema,
        session_parameters={"QUERY_TAG": "prefect:ai_tool_metrics"},
    )
    cur = conn.cursor()
    cur.execute(f"COPY INTO AI_METRICS_STAGE FROM '{gcs_uri}' FILE_FORMAT=(TYPE='PARQUET')")
    result = cur.execute("""
        MERGE INTO FACT_AI_TOOL_DAILY_METRICS AS tgt
        USING AI_METRICS_STAGE AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            tgt.is_active    = src.is_active,
            tgt.total_tokens = src.total_tokens,
            tgt.metrics      = src.metrics
        WHEN NOT MATCHED THEN INSERT SELECT *
    """).fetchone()
    cur.execute("TRUNCATE TABLE AI_METRICS_STAGE")
    cur.close()
    conn.close()
    metrics = {"rows_inserted": result[0] if result else 0, "rows_updated": result[1] if result else 0}
    logger.info("Merge complete: %s", metrics)
    return metrics


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="ai-tool-metrics")
def ai_tool_metrics_flow(report_date: date | None = None) -> dict[str, Any]:
    """
    Aggregate daily AI tool usage across all configured tools → Snowflake.

    Steps:
    1. Load API secrets from GCP Secret Manager
    2. Fetch usage data from each tool API in parallel
    3. Normalise to canonical schema (including token estimation for tools
       without native token reporting)
    4. Serialise to Parquet and upload to GCS
    5. COPY + MERGE into Snowflake

    Args:
        report_date: Date to build metrics for (defaults to yesterday UTC).

    Returns:
        Summary dict with row counts and run metadata.
    """
    logger = get_run_logger()
    run_date = report_date or (datetime.utcnow() - timedelta(days=1)).date()
    logger.info("Starting ai-tool-metrics for %s (env=%s)", run_date, settings.deployment_env)

    # Load secrets
    try:
        llm_key = GcpSecret.load(settings.llm_api_secret_block).read_secret().decode("utf-8").strip()
        ide_key = GcpSecret.load(settings.ide_api_secret_block).read_secret().decode("utf-8").strip()
    except Exception as exc:
        logger.error("Failed to load secrets: %s", exc)
        return Failed(message=str(exc))

    # Extract (run tasks, Prefect handles concurrency)
    llm_raw  = fetch_llm_gateway_metrics(api_key=llm_key, report_date=run_date)
    ide_raw  = fetch_ide_tool_metrics(api_key=ide_key, report_date=run_date)

    # Normalise
    llm_rows = normalize_llm_gateway(raw=llm_raw, report_date=run_date)
    ide_rows = normalize_ide_tool(raw=ide_raw, report_date=run_date)
    all_rows = llm_rows + ide_rows

    # Load
    parquet   = build_parquet(rows=all_rows)
    gcs_uri   = upload_parquet(parquet_bytes=parquet, report_date=run_date)
    metrics   = merge_to_snowflake(gcs_uri=gcs_uri)

    result = {
        "report_date":    run_date.isoformat(),
        "llm_rows":       len(llm_rows),
        "ide_rows":       len(ide_rows),
        "total_rows":     len(all_rows),
        **metrics,
    }

    logger.info("ai-tool-metrics complete: %s", result)
    return result


if __name__ == "__main__":
    ai_tool_metrics_flow()
