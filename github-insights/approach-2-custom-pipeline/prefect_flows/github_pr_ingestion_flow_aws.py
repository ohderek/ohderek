"""
Prefect flow: github-pr-ingestion  ·  AWS variant
──────────────────────────────────────────────────────────────────────────────
Functionally identical to github_pr_ingestion_flow.py (GCS variant).
The only differences are the cloud storage and secrets layer:

  GCS variant                         AWS variant (this file)
  ─────────────────────────────────   ─────────────────────────────────────
  prefect_gcp.GcsBucket               prefect_aws.S3Bucket
  prefect_gcp.secret_manager          prefect_aws.secrets_manager
    .GcpSecret                          .AwsSecret
  gcs.write_path(path, content=b)     s3.upload_from_file_object(
                                         BytesIO(b), to_path=path)
  gs://bucket/path                    s3://bucket/path
  GCS external stage in Snowflake     S3 storage integration in Snowflake
  Service account key auth            IAM role trust policy auth (no keys)

Everything else — GitHub API calls, PyArrow transform, Snowflake MERGE —
is identical. See github_pr_ingestion_flow.py for inline comments on
the shared logic.

Hypothetical showcase. All org names, tokens, and account IDs are generic.
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from prefect import flow, task, get_run_logger
from prefect.states import Failed

# ── AWS-specific imports (replaces prefect_gcp) ──────────────────────────────
from prefect_aws import S3Bucket                                   # pip install prefect-aws
from prefect_aws.secrets_manager import AwsSecret                  # same package

from pydantic import Field
from pydantic_settings import BaseSettings


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

class GitHubFlowSettings(BaseSettings):
    deployment_env: str       = Field(default="stage",           alias="DEPLOYMENT_ENV")
    # ── Changed: S3 block name instead of GCS block name ─────────────────────
    s3_bucket_block: str      = Field(default="s3-data-bucket",  alias="S3_BUCKET_BLOCK")
    # ── Changed: AWS Secrets Manager block instead of GCP Secret Manager ─────
    aws_secret_block: str     = Field(default="github-api-token-aws", alias="AWS_SECRET_BLOCK")
    github_api_base_url: str  = "https://api.github.com"
    github_orgs: list[str]    = Field(
        default=["org-a", "org-b"],
        alias="GITHUB_ORGS",
    )
    pr_page_size: int         = 100
    lookback_days: int        = 3

    @property
    def is_prod(self) -> bool:
        return self.deployment_env.lower() == "prod"


settings = GitHubFlowSettings()


# ─────────────────────────────────────────────────────────────────────────────
# Tasks — Extract  (identical to GCS variant)
# ─────────────────────────────────────────────────────────────────────────────

@task(retries=3, retry_delay_seconds=60)
def fetch_pull_requests(
    github_token: str,
    org: str,
    since: datetime,
) -> list[dict[str, Any]]:
    """
    Fetch recently updated PRs for a GitHub org via the REST API.
    Cursor-based pagination via Link header. Identical to GCS variant.
    """
    logger = get_run_logger()
    logger.info("Fetching PRs for org=%s since=%s", org, since.isoformat())

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept":        "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    prs: list[dict] = []
    url: str | None = (
        f"{settings.github_api_base_url}/orgs/{org}/pulls"
        f"?state=all&sort=updated&direction=desc"
        f"&per_page={settings.pr_page_size}"
        f"&since={since.isoformat()}"
    )

    with httpx.Client(timeout=30.0) as client:
        while url:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            page = resp.json()
            prs.extend(page)

            if page and page[-1].get("updated_at", "") < since.isoformat():
                break

            links    = resp.headers.get("Link", "")
            next_url = None
            for part in links.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
            url = next_url

    logger.info("Fetched %d PRs for org=%s", len(prs), org)
    return prs


# ─────────────────────────────────────────────────────────────────────────────
# Tasks — Transform  (identical to GCS variant)
# ─────────────────────────────────────────────────────────────────────────────

@task
def transform_pull_requests(
    raw_prs: list[dict[str, Any]],
    org: str,
) -> bytes:
    """
    Normalise raw PR records to canonical schema and serialise to Parquet.
    PyArrow explicit schema, Snappy compression. Identical to GCS variant.
    """
    logger = get_run_logger()

    rows = []
    for pr in raw_prs:
        repo_name = pr.get("head", {}).get("repo", {}).get("name", "")
        pr_id     = str(pr.get("id", ""))
        rows.append({
            "id":               pr_id,
            "number":           int(pr.get("number", 0)),
            "org":              org,
            "repo_name":        repo_name,
            "repo_id":          str(pr.get("head", {}).get("repo", {}).get("id", "")),
            "title":            pr.get("title", ""),
            "url":              pr.get("html_url", ""),
            "state":            pr.get("state", ""),
            "base_ref":         pr.get("base", {}).get("ref", ""),
            "head_ref":         pr.get("head", {}).get("ref", ""),
            "base_sha":         pr.get("base", {}).get("sha", ""),
            "head_sha":         pr.get("head", {}).get("sha", ""),
            "merge_commit_sha": pr.get("merge_commit_sha", ""),
            "user_login":       pr.get("user", {}).get("login", ""),
            "is_merged":        pr.get("merged", False),
            "is_draft":         pr.get("draft", False),
            "auto_merge":       pr.get("auto_merge") is not None,
            "created_at":       pr.get("created_at", ""),
            "updated_at":       pr.get("updated_at", ""),
            "merged_at":        pr.get("merged_at", ""),
            "closed_at":        pr.get("closed_at", ""),
            "_row_key":         hashlib.md5(f"{org}|{repo_name}|{pr_id}".encode()).hexdigest(),
            "_ingested_at":     datetime.utcnow().isoformat(),
        })

    table = pa.Table.from_pylist(rows)
    buf   = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    logger.info("Serialised %d PR records for org=%s", len(rows), org)
    return buf.getvalue().to_pybytes()


# ─────────────────────────────────────────────────────────────────────────────
# Tasks — Load (AWS-specific: S3 instead of GCS)
# ─────────────────────────────────────────────────────────────────────────────

@task(retries=3, retry_delay_seconds=30)
def upload_to_s3(parquet_bytes: bytes, entity: str, org: str, run_date: date) -> str:
    """
    Upload Parquet bytes to S3 with a Hive-partitioned path.

    Differences from upload_to_gcs():
      - Uses prefect_aws.S3Bucket block instead of prefect_gcp.GcsBucket
      - S3Bucket.upload_from_file_object() takes a BytesIO — wraps bytes first
      - Returns an s3:// URI instead of gs://

    The S3 path structure is identical to the GCS variant so Snowflake COPY
    patterns are consistent regardless of cloud.
    """
    logger = get_run_logger()

    path = f"github/{entity}/org={org}/date={run_date.isoformat()}/{entity}.parquet"

    # ── prefect-aws S3Bucket block ────────────────────────────────────────────
    # Block stores bucket name + optional AWS credentials block reference.
    # Auth resolves in order: block credentials → env vars → instance profile.
    s3 = S3Bucket.load(settings.s3_bucket_block)
    s3.upload_from_file_object(
        from_file_object=BytesIO(parquet_bytes),
        to_path=path,
    )

    uri = f"s3://{s3.bucket_name}/{path}"
    logger.info("Uploaded %s → %s", entity, uri)
    return uri


@task(retries=2, retry_delay_seconds=60)
def copy_to_snowflake_from_s3(s3_uri: str, target_table: str) -> dict[str, int]:
    """
    COPY Parquet from S3 into Snowflake staging, then MERGE into target.

    Differences from GCS variant:
      - COPY statement references an S3 storage integration, not a GCS stage
      - Auth is IAM role trust policy (no access keys ever touch Snowflake config)
      - STORAGE_INTEGRATION object must be created in Snowflake first
        (see sql/snowflake_aws_stage_setup.sql)

    The MERGE logic is identical to the GCS variant.
    """
    import os
    import snowflake.connector

    logger = get_run_logger()

    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        # Key-pair auth — same as GCS variant, Snowflake is cloud-agnostic
        private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role      = os.environ.get("SNOWFLAKE_ROLE",      "TRANSFORMER"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL__MEDIUM"),
        database  = "GITHUB_PIPELINES",
        schema    = "RAW_TABLES",
        session_parameters={"QUERY_TAG": f"prefect:github_pr_ingestion_aws:{target_table}"},
    )
    cur = conn.cursor()
    stage_table = f"{target_table}_STAGE"

    # ── COPY from S3 using named stage with storage integration ───────────────
    # The @github_s3_stage was created with STORAGE_INTEGRATION = s3_snowflake_integration
    # (IAM role) — no access keys embedded in the COPY command.
    # Equivalent GCS COPY: FROM 'gs://...' CREDENTIALS=(...) or via named stage.
    cur.execute(f"""
        COPY INTO {stage_table}
        FROM '{s3_uri}'
        STORAGE_INTEGRATION = s3_snowflake_integration
        FILE_FORMAT = (
            TYPE             = 'PARQUET'
            USE_LOGICAL_TYPE = TRUE
            SNAPPY_COMPRESSION = TRUE
        )
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE    = FALSE
    """)

    # ── MERGE — identical to GCS variant ─────────────────────────────────────
    result = cur.execute(f"""
        MERGE INTO {target_table} AS tgt
        USING {stage_table} AS src
        ON tgt._row_key = src._row_key
        WHEN MATCHED AND tgt._ingested_at < src._ingested_at
            THEN UPDATE SET tgt._ingested_at = src._ingested_at,
                            tgt.state        = src.state,
                            tgt.updated_at   = src.updated_at,
                            tgt.merged_at    = src.merged_at,
                            tgt.closed_at    = src.closed_at
        WHEN NOT MATCHED
            THEN INSERT SELECT *
    """).fetchone()

    cur.execute(f"TRUNCATE TABLE {stage_table}")
    cur.close()
    conn.close()

    metrics = {
        "rows_inserted": result[0] if result else 0,
        "rows_updated":  result[1] if result else 0,
    }
    logger.info("Merged into %s: %s", target_table, metrics)
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Main flow
# ─────────────────────────────────────────────────────────────────────────────

@flow(name="github-pr-ingestion-aws")
def github_pr_ingestion_flow_aws(
    orgs: list[str] | None = None,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    """
    Ingest GitHub PR data into Snowflake RAW — AWS variant.

    Steps:
      1. Load GitHub API token from AWS Secrets Manager via Prefect AwsSecret block
         (GCS variant uses GcpSecret → GCP Secret Manager)
      2. For each org: fetch PRs updated in the lookback window  ← identical
      3. Transform to Parquet with PyArrow                       ← identical
      4. Upload to S3 (prefect-aws S3Bucket block)
         (GCS variant uploads to GCS via GcsBucket block)
      5. COPY from S3 + MERGE into Snowflake
         (GCS variant uses GCS stage; MERGE logic is identical)

    Args:
        orgs:          Override list of GitHub orgs.
        lookback_days: Override lookback window (default: 3 days).

    Returns:
        Summary dict with row counts per org and run metadata.
    """
    logger      = get_run_logger()
    active_orgs = orgs or settings.github_orgs
    days        = lookback_days or settings.lookback_days
    since       = datetime.utcnow() - timedelta(days=days)
    run_date    = since.date()

    logger.info(
        "Starting github-pr-ingestion-aws for orgs=%s since=%s (env=%s)",
        active_orgs, since.isoformat(), settings.deployment_env,
    )

    # ── Step 1: Load GitHub token from AWS Secrets Manager ────────────────────
    # GCS variant: GcpSecret.load(block).read_secret().decode("utf-8").strip()
    # AWS variant: AwsSecret.load(block).read_secret()  ← returns str directly
    try:
        token: str = AwsSecret.load(settings.aws_secret_block).read_secret()
    except Exception as exc:
        logger.error("Failed to load GitHub token from AWS Secrets Manager: %s", exc)
        return Failed(message=str(exc))

    summary: dict[str, Any] = {}

    for org in active_orgs:
        try:
            # Steps 2–3 are identical to the GCS variant
            raw_prs  = fetch_pull_requests(github_token=token, org=org, since=since)
            pr_bytes = transform_pull_requests(raw_prs=raw_prs, org=org)

            # Step 4: upload to S3 (GCS variant calls upload_to_gcs)
            pr_uri   = upload_to_s3(
                parquet_bytes=pr_bytes,
                entity="pull_request",
                org=org,
                run_date=run_date,
            )

            # Step 5: COPY from S3 + MERGE (GCS variant calls copy_to_snowflake)
            metrics  = copy_to_snowflake_from_s3(
                s3_uri=pr_uri,
                target_table="PULL_REQUEST",
            )

            summary[org] = {"prs_fetched": len(raw_prs), **metrics}

        except Exception as exc:
            logger.error("Org %s failed: %s", org, exc)
            summary[org] = {"error": str(exc)}

    total = sum(v.get("prs_fetched", 0) for v in summary.values())
    logger.info("github-pr-ingestion-aws complete. Total PRs: %d", total)
    return {"orgs": summary, "total_prs": total, "run_date": run_date.isoformat()}


if __name__ == "__main__":
    github_pr_ingestion_flow_aws()
