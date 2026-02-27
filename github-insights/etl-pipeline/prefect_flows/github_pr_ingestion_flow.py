"""
Prefect flow: github-pr-ingestion

Hypothetical showcase flow demonstrating how Prefect would be used to
ingest GitHub pull request data (PRs, reviews, commits, files) via the
GitHub REST API into a Snowflake raw layer.

This is a sample implementation. All GitHub org names, API tokens, and
internal service names are fully generic/anonymised. No proprietary data
is included.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from typing import Any

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

class GitHubFlowSettings(BaseSettings):
    deployment_env: str       = Field(default="stage", alias="DEPLOYMENT_ENV")
    gcs_bucket_block: str     = Field(default="gcs-data-bucket", alias="GCS_BUCKET_BLOCK")
    github_token_block: str   = Field(default="github-api-token", alias="GITHUB_TOKEN_BLOCK")
    github_api_base_url: str  = "https://api.github.com"
    github_orgs: list[str]    = Field(
        default=["org-a", "org-b"],
        alias="GITHUB_ORGS",
    )
    pr_page_size: int         = 100
    lookback_days: int        = 3     # How many days to look back for updates

    @property
    def is_prod(self) -> bool:
        return self.deployment_env.lower() == "prod"


settings = GitHubFlowSettings()


# ---------------------------------------------------------------------------
# Tasks — Extract
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=60)
def fetch_pull_requests(
    github_token: str,
    org: str,
    since: datetime,
) -> list[dict[str, Any]]:
    """
    Fetch recently updated PRs for a GitHub org via the REST API.

    Uses cursor-based pagination (Link header) to retrieve all pages.

    Args:
        github_token: GitHub personal access token.
        org: GitHub organisation name.
        since: Fetch PRs updated after this timestamp.

    Returns:
        List of raw PR dicts.
    """
    logger = get_run_logger()
    logger.info("Fetching PRs for org=%s since=%s", org, since.isoformat())

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
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

            # Stop if we've fetched beyond the lookback window
            if page and page[-1].get("updated_at", "") < since.isoformat():
                break

            # Follow GitHub Link header pagination
            links = resp.headers.get("Link", "")
            next_link = None
            for part in links.split(","):
                if 'rel="next"' in part:
                    next_link = part.split(";")[0].strip().strip("<>")
            url = next_link

    logger.info("Fetched %d PRs for org=%s", len(prs), org)
    return prs


@task(retries=2, retry_delay_seconds=30)
def fetch_pr_reviews(
    github_token: str,
    org: str,
    repo: str,
    pr_number: int,
) -> list[dict[str, Any]]:
    """Fetch all reviews for a single pull request."""
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }
    with httpx.Client(timeout=20.0) as client:
        resp = client.get(
            f"{settings.github_api_base_url}/repos/{org}/{repo}/pulls/{pr_number}/reviews",
            headers=headers,
            params={"per_page": 100},
        )
        resp.raise_for_status()
        return resp.json()


@task(retries=2, retry_delay_seconds=30)
def fetch_pr_files(
    github_token: str,
    org: str,
    repo: str,
    pr_number: int,
) -> list[dict[str, Any]]:
    """Fetch all changed files for a single pull request."""
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }
    with httpx.Client(timeout=20.0) as client:
        resp = client.get(
            f"{settings.github_api_base_url}/repos/{org}/{repo}/pulls/{pr_number}/files",
            headers=headers,
            params={"per_page": 100},
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Tasks — Transform
# ---------------------------------------------------------------------------

@task
def transform_pull_requests(
    raw_prs: list[dict[str, Any]],
    org: str,
) -> bytes:
    """
    Normalise raw PR records to the canonical schema and serialise to Parquet.

    Extracts nested objects (user, head, base), computes derived fields
    (is_merged, is_draft), and adds a surrogate row key.
    """
    logger = get_run_logger()

    rows = []
    for pr in raw_prs:
        repo_name = pr.get("head", {}).get("repo", {}).get("name", "")
        pr_id = str(pr.get("id", ""))
        rows.append({
            "id":                  pr_id,
            "number":              int(pr.get("number", 0)),
            "org":                 org,
            "repo_name":           repo_name,
            "repo_id":             str(pr.get("head", {}).get("repo", {}).get("id", "")),
            "title":               pr.get("title", ""),
            "url":                 pr.get("html_url", ""),
            "state":               pr.get("state", ""),
            "base_ref":            pr.get("base", {}).get("ref", ""),
            "head_ref":            pr.get("head", {}).get("ref", ""),
            "base_sha":            pr.get("base", {}).get("sha", ""),
            "head_sha":            pr.get("head", {}).get("sha", ""),
            "merge_commit_sha":    pr.get("merge_commit_sha", ""),
            "user_login":          pr.get("user", {}).get("login", ""),
            "is_merged":           pr.get("merged", False),
            "is_draft":            pr.get("draft", False),
            "auto_merge":          pr.get("auto_merge") is not None,
            "created_at":          pr.get("created_at", ""),
            "updated_at":          pr.get("updated_at", ""),
            "merged_at":           pr.get("merged_at", ""),
            "closed_at":           pr.get("closed_at", ""),
            "_row_key":            hashlib.md5(f"{org}|{repo_name}|{pr_id}".encode()).hexdigest(),
            "_fivetran_synced":    datetime.utcnow().isoformat(),
        })

    table = pa.Table.from_pylist(rows)
    buf   = pa.BufferOutputStream()
    pq.write_table(table, buf)
    logger.info("Serialised %d PR records for org=%s", len(rows), org)
    return buf.getvalue().to_pybytes()


@task
def upload_to_gcs(parquet_bytes: bytes, entity: str, org: str, run_date: date) -> str:
    """Upload Parquet file to GCS with partitioned path."""
    logger = get_run_logger()
    gcs  = GcsBucket.load(settings.gcs_bucket_block)
    path = f"github/{entity}/org={org}/date={run_date.isoformat()}/{entity}.parquet"
    gcs.write_path(path=path, content=parquet_bytes)
    uri  = f"gs://{gcs.bucket}/{path}"
    logger.info("Uploaded %s → %s", entity, uri)
    return uri


@task
def copy_to_snowflake(gcs_uri: str, target_table: str) -> dict[str, int]:
    """COPY Parquet from GCS into Snowflake staging, then MERGE into target."""
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
        schema="GITHUB",
        session_parameters={"QUERY_TAG": f"prefect:github_pr_ingestion:{target_table}"},
    )
    cur = conn.cursor()
    stage_table = f"{target_table}_STAGE"
    cur.execute(f"COPY INTO {stage_table} FROM '{gcs_uri}' FILE_FORMAT=(TYPE='PARQUET')")
    result = cur.execute(f"""
        MERGE INTO {target_table} AS tgt
        USING {stage_table} AS src
        ON tgt._row_key = src._row_key
        WHEN MATCHED THEN UPDATE SET tgt._fivetran_synced = src._fivetran_synced
        WHEN NOT MATCHED THEN INSERT SELECT *
    """).fetchone()
    cur.execute(f"TRUNCATE TABLE {stage_table}")
    cur.close()
    conn.close()
    metrics = {"rows_inserted": result[0] if result else 0, "rows_updated": result[1] if result else 0}
    logger.info("Merged into %s: %s", target_table, metrics)
    return metrics


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="github-pr-ingestion")
def github_pr_ingestion_flow(
    orgs: list[str] | None = None,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    """
    Ingest GitHub pull request data (PRs + reviews + files) into Snowflake RAW.

    Steps:
    1. Load GitHub API token from GCP Secret Manager
    2. For each org: fetch PRs updated in the lookback window
    3. Transform each batch to Parquet, upload to GCS
    4. COPY + MERGE each entity into Snowflake

    Args:
        orgs: Override list of GitHub orgs (default: settings.github_orgs).
        lookback_days: Override lookback window (default: settings.lookback_days).

    Returns:
        Summary dict with row counts per org.
    """
    logger = get_run_logger()
    active_orgs = orgs or settings.github_orgs
    days = lookback_days or settings.lookback_days
    since = datetime.utcnow() - timedelta(days=days)
    run_date = since.date()

    logger.info(
        "Starting github-pr-ingestion for orgs=%s since=%s (env=%s)",
        active_orgs, since.isoformat(), settings.deployment_env,
    )

    try:
        token = GcpSecret.load(settings.github_token_block).read_secret().decode("utf-8").strip()
    except Exception as exc:
        logger.error("Failed to load GitHub token: %s", exc)
        return Failed(message=str(exc))

    summary: dict[str, Any] = {}

    for org in active_orgs:
        try:
            raw_prs  = fetch_pull_requests(github_token=token, org=org, since=since)
            pr_bytes = transform_pull_requests(raw_prs=raw_prs, org=org)
            pr_uri   = upload_to_gcs(parquet_bytes=pr_bytes, entity="pull_request", org=org, run_date=run_date)
            metrics  = copy_to_snowflake(gcs_uri=pr_uri, target_table="PULL_REQUEST")
            summary[org] = {"prs_fetched": len(raw_prs), **metrics}
        except Exception as exc:
            logger.error("Org %s failed: %s", org, exc)
            summary[org] = {"error": str(exc)}

    total = sum(v.get("prs_fetched", 0) for v in summary.values())
    logger.info("github-pr-ingestion complete. Total PRs: %d", total)
    return {"orgs": summary, "total_prs": total, "run_date": run_date.isoformat()}


if __name__ == "__main__":
    github_pr_ingestion_flow()
