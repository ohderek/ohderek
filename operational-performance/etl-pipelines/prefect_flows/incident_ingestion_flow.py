"""
Prefect flow: incident-ingestion

Hypothetical showcase flow demonstrating how Prefect would be used to
ingest incident management data from multiple workspace sources into
a Snowflake data warehouse.

This is a sample implementation. All API endpoints, credentials,
and company references are fully generic/anonymised.
"""

from __future__ import annotations

import io
from datetime import datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task, get_run_logger
from prefect.states import Cancelled, Failed
from prefect_gcp import GcsBucket
from prefect_gcp.secret_manager import GcpSecret
from pydantic import Field
from pydantic_settings import BaseSettings


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class IncidentFlowSettings(BaseSettings):
    deployment_env: str = Field(default="stage", alias="DEPLOYMENT_ENV")
    gcs_bucket_block: str = Field(default="gcs-data-bucket", alias="GCS_BUCKET_BLOCK")
    incident_api_secret_block: str = Field(
        default="incident-api-key", alias="INCIDENT_API_SECRET_BLOCK"
    )
    incident_api_base_url: str = Field(
        default="https://api.incident.example.com/v2", alias="INCIDENT_API_BASE_URL"
    )
    snowflake_target_table: str = "RAW.INCIDENTS.FACT_INCIDENTS"

    @property
    def is_prod(self) -> bool:
        return self.deployment_env.lower() == "prod"


settings = IncidentFlowSettings()

WORKSPACES = ["workspace_a", "workspace_b", "workspace_c"]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=60)
def fetch_incidents(
    api_key: str,
    workspace: str,
    since: datetime,
    base_url: str = settings.incident_api_base_url,
) -> list[dict[str, Any]]:
    """
    Fetch paginated incidents from the incident management API for a single workspace.

    Args:
        api_key: Bearer token for authentication.
        workspace: Workspace identifier (workspace_a, workspace_b, etc.)
        since: Only fetch incidents updated after this timestamp.
        base_url: API base URL.

    Returns:
        List of raw incident dicts.
    """
    logger = get_run_logger()
    logger.info("Fetching incidents for workspace=%s since=%s", workspace, since.isoformat())

    incidents: list[dict] = []
    url: str | None = f"{base_url}/incidents"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Workspace": workspace,
        "Accept": "application/json",
    }
    params: dict = {
        "updated_after": since.isoformat(),
        "per_page": 250,
    }

    with httpx.Client(timeout=30.0) as client:
        while url:
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            payload = resp.json()
            incidents.extend(payload.get("incidents", []))
            params.pop("page_token", None)
            next_cursor = payload.get("pagination", {}).get("next_cursor")
            if next_cursor:
                params["page_token"] = next_cursor
            else:
                url = None

    logger.info("Fetched %d incidents for workspace=%s", len(incidents), workspace)
    return incidents


@task
def transform_incidents(
    raw: list[dict[str, Any]],
    workspace: str,
) -> bytes:
    """
    Normalise raw incident records and serialise to Parquet.

    Computes TTD, TTR, TTS, impact_duration, temperature classification,
    and attaches the workspace identifier.

    Args:
        raw: Raw incident dicts from the API.
        workspace: Source workspace identifier.

    Returns:
        Parquet-serialised bytes.
    """
    logger = get_run_logger()

    def _diff_minutes(start: str | None, end: str | None) -> int | None:
        if not start or not end:
            return None
        try:
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return max(0, int((e - s).total_seconds() / 60))
        except Exception:
            return None

    rows = []
    for r in raw:
        ts = r.get("timestamps", {})
        impact_start = ts.get("impact_started_at") or ts.get("occurred_at")
        ttr = _diff_minutes(impact_start, ts.get("reported_at"))
        rows.append({
            "unique_ref":        f"{r.get('reference', '')}-{workspace}",
            "incident_id":       r.get("id"),
            "workspace":         workspace,
            "reference":         r.get("reference"),
            "name":              r.get("name"),
            "severity":          (r.get("severity", {}).get("name") or "")[:4].strip(),
            "status":            r.get("status"),
            "is_internal":       bool(r.get("is_internal", False)),
            "postmortem_url":    r.get("postmortem_document_url"),
            "impact_start_at":   impact_start,
            "detected_at":       ts.get("detected_at"),
            "reported_at":       ts.get("reported_at"),
            "stabilized_at":     ts.get("stabilized_at"),
            "resolved_at":       ts.get("resolved_at"),
            "ttd_minutes":       _diff_minutes(impact_start, ts.get("detected_at")),
            "ttr_minutes":       ttr,
            "tts_minutes":       _diff_minutes(ts.get("reported_at"), ts.get("stabilized_at")),
            "impact_duration_minutes": _diff_minutes(impact_start, ts.get("stabilized_at")),
            "temperature":       "Hot" if ttr is not None and 0 <= ttr <= 240 else "Cold",
            "data_synced_at":    datetime.utcnow().isoformat(),
        })

    table = pa.Table.from_pylist(rows)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    logger.info("Transformed %d incidents for workspace=%s", len(rows), workspace)
    return buf.getvalue().to_pybytes()


@task
def upload_to_gcs(
    parquet_bytes: bytes,
    workspace: str,
    run_date: str,
) -> str:
    """
    Upload Parquet file to GCS staging bucket.

    Args:
        parquet_bytes: Serialised Parquet data.
        workspace: Workspace identifier (used in file path).
        run_date: Run date string (YYYY-MM-DD) for partitioning.

    Returns:
        GCS URI of the uploaded file.
    """
    logger = get_run_logger()
    gcs = GcsBucket.load(settings.gcs_bucket_block)
    path = f"incidents/{workspace}/date={run_date}/incidents.parquet"
    gcs.write_path(path=path, content=parquet_bytes)
    gcs_uri = f"gs://{gcs.bucket}/{path}"
    logger.info("Uploaded to %s (%d bytes)", gcs_uri, len(parquet_bytes))
    return gcs_uri


@task
def load_to_snowflake(gcs_uri: str, workspace: str) -> dict[str, int]:
    """
    COPY parquet file from GCS into Snowflake staging table, then MERGE.

    Args:
        gcs_uri: GCS path to the Parquet file.
        workspace: Workspace identifier (used for QUERY_TAG).

    Returns:
        Dict with rows_inserted and rows_updated counts.
    """
    import os
    import snowflake.connector

    logger = get_run_logger()

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL__MEDIUM"),
        database="RAW",
        schema="INCIDENTS",
        session_parameters={"QUERY_TAG": f"prefect:incident_ingestion:{workspace}"},
    )
    cur = conn.cursor()

    # Stage the file
    cur.execute(f"COPY INTO INCIDENTS_STAGE FROM '{gcs_uri}' FILE_FORMAT = (TYPE = 'PARQUET')")
    result = cur.execute("""
        MERGE INTO FACT_INCIDENTS AS tgt
        USING INCIDENTS_STAGE AS src
        ON tgt.unique_ref = src.unique_ref
        WHEN MATCHED THEN UPDATE SET
            tgt.status = src.status,
            tgt.ttr_minutes = src.ttr_minutes,
            tgt.stabilized_at = src.stabilized_at,
            tgt.data_synced_at = src.data_synced_at
        WHEN NOT MATCHED THEN INSERT SELECT *
    """).fetchone()

    cur.execute("TRUNCATE TABLE INCIDENTS_STAGE")
    cur.close()
    conn.close()

    metrics = {"rows_inserted": result[0] if result else 0, "rows_updated": result[1] if result else 0}
    logger.info("Snowflake merge complete: %s", metrics)
    return metrics


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="incident-ingestion")
def incident_ingestion_flow(
    lookback_hours: int = 2,
    workspaces: list[str] | None = None,
) -> dict[str, Any]:
    """
    Ingest incident management data from all workspaces into Snowflake.

    Steps:
    1. Load API credentials from secret manager
    2. For each workspace: fetch → transform → upload to GCS
    3. COPY + MERGE each workspace file into Snowflake

    Args:
        lookback_hours: Fetch incidents updated within this window.
        workspaces: Override workspace list (default: all configured workspaces).

    Returns:
        Summary dict with total records processed per workspace.
    """
    logger = get_run_logger()
    logger.info("Starting incident-ingestion flow (lookback=%dh)", lookback_hours)

    since = datetime.utcnow() - timedelta(hours=lookback_hours)
    run_date = since.strftime("%Y-%m-%d")
    active_workspaces = workspaces or WORKSPACES

    # Load secret once (not per workspace)
    try:
        secret = GcpSecret.load(settings.incident_api_secret_block)
        api_key = secret.read_secret().decode("utf-8").strip()
    except Exception as exc:
        logger.error("Failed to load API secret: %s", exc)
        return Failed(message=f"Secret load failed: {exc}")

    summary: dict[str, Any] = {}

    for workspace in active_workspaces:
        try:
            raw       = fetch_incidents(api_key=api_key, workspace=workspace, since=since)
            parquet   = transform_incidents(raw=raw, workspace=workspace)
            gcs_uri   = upload_to_gcs(parquet_bytes=parquet, workspace=workspace, run_date=run_date)
            metrics   = load_to_snowflake(gcs_uri=gcs_uri, workspace=workspace)
            summary[workspace] = {"records": len(raw), **metrics}
        except Exception as exc:
            logger.error("Workspace %s failed: %s", workspace, exc)
            summary[workspace] = {"error": str(exc)}

    total = sum(v.get("records", 0) for v in summary.values())
    logger.info("Incident ingestion complete. Total records: %d", total)
    return {"workspaces": summary, "total_records": total, "run_date": run_date}


if __name__ == "__main__":
    incident_ingestion_flow()
