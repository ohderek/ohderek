"""
DAG: ops_incidents

Ingests incident data from all three incident.io instances (Cash, Square, Block)
into the Snowflake reporting layer. Runs the same SQL logic as the original
tech-health-metrics ETL job but expressed as idempotent, observable Airflow tasks.

Schedule: Hourly (mirrors meta.yaml: cron 0 * * * *)
Grain: One row per incident reference per instance

Pipeline:
    extract_[cash|sq|block]_incidents ──► transform_incidents ──► merge_fact_incidents
                                                                        │
                                                                        ▼
    extract_orig_service ──► deduplicate ──► merge_bridge_incident_service

    data_quality_check ──► end
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

IncidentInstance = Literal["companya", "companyb", "companyc"]

INSTANCES: list[IncidentInstance] = ["companya", "companyb", "companyc"]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ops_incidents",
    description="Hourly merge of incident.io data (Cash + Square + Block) → Snowflake FACT_INCIDENTS",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ops", "incidents", "snowflake", "hourly"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ------------------------------------------------------------------
    # Extract from each incident.io instance
    # ------------------------------------------------------------------

    @task_group(group_id="extract_incidents")
    def extract_all_instances():

        extracted: dict[str, dict] = {}

        for instance in INSTANCES:

            @task(task_id=f"extract_{instance}_incidents")
            def extract_instance(inst: str = instance) -> dict:
                """
                Extract raw incidents + incident times + roles from Snowflake RAW tables.
                Each instance lives in its own RAW schema (CASH_INCIDENTS, SQ_INCIDENTS, etc.)
                """
                import os
                import snowflake.connector

                conn = snowflake.connector.connect(
                    account=os.environ["SNOWFLAKE_ACCOUNT"],
                    user=os.environ["SNOWFLAKE_USER"],
                    password=os.environ["SNOWFLAKE_PASSWORD"],
                    database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
                    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                    role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
                    session_parameters={"QUERY_TAG": f"airflow:ops_incidents:{inst}"},
                )

                prefix = inst.upper()
                cursor = conn.cursor()

                cursor.execute(f"""
                    SELECT
                        i.id             AS incident_id,
                        i.reference,
                        i.created_at,
                        i.name,
                        i.permalink      AS url,
                        i.summary,
                        i.postmortem_document_url AS postmortem_link,
                        i.severity,
                        i.creator_email,
                        i.status,
                        i.mode,
                        i.internal       AS is_internal_impact,
                        t.regressed_at,
                        t.impact_start,
                        t.canceled_at,
                        t.declined_at,
                        t.merged_at,
                        t.accepted_at,
                        t.stabilized_at,
                        t.detected_at,
                        t.reported_at,
                        t.resolved_at,
                        t.fixed_at,
                        t.closed_at,
                        GREATEST(
                            COALESCE(i._fivetran_synced, '1900-01-01'),
                            COALESCE(t._fivetran_synced, '1900-01-01')
                        ) AS data_synced_at
                    FROM RAW.INCIDENTS_{prefix} i
                    LEFT JOIN RAW.INCIDENT_TIMES_{prefix} t
                        ON i.reference = t.reference
                    WHERE i.created_at >= '2023-01-01'
                """)

                columns = [desc[0].lower() for desc in cursor.description]
                records = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                conn.close()

                log.info("Extracted %d %s incidents", len(records), inst)
                return {"instance": inst, "records": records}

            extracted[instance] = extract_instance()

        return extracted

    # ------------------------------------------------------------------
    # Transform: normalise across instances (currency of field names,
    # compute TTD/TTR/TTS, derive unique_ref and temperature)
    # ------------------------------------------------------------------

    @task(task_id="transform_incidents")
    def transform_incidents(extracted_by_instance: dict) -> list[dict]:
        import re
        from datetime import datetime

        def parse_ts(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            try:
                return datetime.fromisoformat(str(val))
            except Exception:
                return None

        def diff_minutes(start, end):
            s, e = parse_ts(start), parse_ts(end)
            if s is None or e is None:
                return None
            delta = (e - s).total_seconds() / 60
            return max(0, delta)

        all_records: list[dict] = []

        for instance, result in extracted_by_instance.items():
            records = result["records"] if isinstance(result, dict) else []

            for r in records:
                ref_num = re.search(r"[0-9]+$", r.get("reference", "") or "")
                suffix = f"-{instance}"
                unique_ref = f"{ref_num.group()}{suffix}" if ref_num else None

                impact_start = r.get("impact_start") or r.get("regressed_at")
                ttr = diff_minutes(impact_start, r.get("reported_at"))

                all_records.append({
                    "unique_ref":        unique_ref,
                    "instance":          instance,
                    "incident_id":       r.get("incident_id"),
                    "reference":         r.get("reference"),
                    "created_at":        str(r["created_at"])[:10] if r.get("created_at") else None,
                    "name":              r.get("name"),
                    "url":               r.get("url"),
                    "summary":           r.get("summary"),
                    "postmortem_link":   r.get("postmortem_link"),
                    "severity":          (r.get("severity") or "")[:4].strip(),
                    "creator_email":     r.get("creator_email"),
                    "status":            r.get("status"),
                    "mode":              r.get("mode"),
                    "impact_start_at":   r.get("impact_start"),
                    "reported_at":       r.get("reported_at"),
                    "stabilized_at":     r.get("stabilized_at"),
                    "detected_at":       r.get("detected_at"),
                    "resolved_at":       r.get("resolved_at"),
                    "fixed_at":          r.get("fixed_at"),
                    "closed_at":         r.get("closed_at"),
                    "ttd":               diff_minutes(impact_start, r.get("detected_at")),
                    "ttr":               ttr,
                    "tts":               diff_minutes(r.get("reported_at"), r.get("stabilized_at")),
                    "impact_duration":   diff_minutes(impact_start, r.get("stabilized_at")),
                    "temperature":       "Hot" if ttr is not None and 0 <= ttr <= 240 else "Cold",
                    "is_internal_impact": r.get("is_internal_impact"),
                    "data_synced_at":    r.get("data_synced_at"),
                })

        log.info("Transformed %d total incidents across %d instances",
                 len(all_records), len(extracted_by_instance))
        return all_records

    # ------------------------------------------------------------------
    # Load: MERGE into FACT_INCIDENTS
    # ------------------------------------------------------------------

    @task(task_id="load_fact_incidents")
    def load_fact_incidents(records: list[dict]) -> dict:
        import os
        import pandas as pd
        from src.loaders.snowflake_loader import SnowflakeLoader, SnowflakeLoaderConfig

        config = SnowflakeLoaderConfig(
            database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
            schema="REPORTING_TABLES",
            target_table="FACT_INCIDENTS",
            merge_keys=["unique_ref"],
            update_columns=[
                "status", "mode", "severity", "ttr", "tts", "ttd",
                "impact_duration", "temperature", "stabilized_at",
                "resolved_at", "closed_at", "postmortem_link", "data_synced_at",
            ],
        )
        with SnowflakeLoader(config) as loader:
            return loader.load(pd.DataFrame(records))

    # ------------------------------------------------------------------
    # Bridge: incident → service linkage
    # ------------------------------------------------------------------

    @task(task_id="extract_orig_service")
    def extract_orig_service() -> list[dict]:
        import os
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT unique_ref, originating_service, _fivetran_synced AS data_synced_at FROM RAW.ORIG_SERVICE")
        cols = [d[0].lower() for d in cursor.description]
        records = [dict(zip(cols, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return records

    @task(task_id="load_bridge_incident_service")
    def load_bridge_incident_service(orig_records: list[dict]) -> dict:
        import hashlib
        import os
        import pandas as pd
        from src.loaders.snowflake_loader import SnowflakeLoader, SnowflakeLoaderConfig

        enriched = []
        seen: dict[tuple, str] = {}
        for r in orig_records:
            key = (r["unique_ref"], (r.get("originating_service") or "").upper().strip())
            if key in seen:
                continue
            seen[key] = ""
            pk = f"{key[0]}|{key[1]}"
            enriched.append({
                "unique_ref":       key[0],
                "application_name": key[1],
                "data_synced_at":   r.get("data_synced_at"),
                "pk":               pk,
                "hashdiff":         hashlib.md5(pk.encode()).hexdigest(),
            })

        config = SnowflakeLoaderConfig(
            database=os.environ.get("SNOWFLAKE_DATABASE", "TECH_HEALTH"),
            schema="REPORTING_TABLES",
            target_table="BRIDGE_INCIDENT_SERVICE",
            merge_keys=["unique_ref", "application_name"],
            update_columns=["data_synced_at", "pk", "hashdiff"],
        )
        with SnowflakeLoader(config) as loader:
            return loader.load(pd.DataFrame(enriched))

    # ------------------------------------------------------------------
    # Data quality
    # ------------------------------------------------------------------

    @task(task_id="data_quality_check", trigger_rule=TriggerRule.ALL_SUCCESS)
    def data_quality_check(load_metrics: dict, bridge_metrics: dict) -> None:
        failures = []
        if load_metrics.get("rows_inserted", -1) < 0:
            failures.append("FACT_INCIDENTS: unexpected negative insert count")
        if bridge_metrics.get("rows_inserted", -1) < 0:
            failures.append("BRIDGE_INCIDENT_SERVICE: unexpected negative insert count")
        if failures:
            raise ValueError(f"Quality checks failed: {failures}")
        log.info("Data quality passed — incidents loaded", extra={**load_metrics})

    # ------------------------------------------------------------------
    # Wire up
    # ------------------------------------------------------------------

    extracted   = extract_all_instances()
    transformed = transform_incidents(extracted)
    fact_load   = load_fact_incidents(transformed)

    orig_svc    = extract_orig_service()
    bridge_load = load_bridge_incident_service(orig_svc)

    quality = data_quality_check(fact_load, bridge_load)

    start >> [extracted, orig_svc]
    quality >> end
