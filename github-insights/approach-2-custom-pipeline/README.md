# Approach 2 — Custom Ingestion Pipeline

In this approach, **you own the ingestion code**. A Prefect flow calls the GitHub REST API directly, transforms responses to Parquet with PyArrow, and loads them into Snowflake. No third-party connector required.

Two cloud storage options are provided — **GCS** and **AWS S3**. The GitHub API calls, PyArrow transforms, and Snowflake MERGE are identical in both. Only the storage and secrets layer changes.

---

## Architecture

```
                        GitHub REST API
                              │
                   httpx (async HTTP client)
                   cursor-based pagination (Link header)
                              │
                    PyArrow Parquet transform
                    explicit schema · Snappy compression
                    _row_key = MD5(org|repo|pr_id)
                              │
              ┌───────────────┴────────────────┐
              │                                │
     ── GCS Option ──                 ── AWS Option ──
              │                                │
    prefect_gcp.GcsBucket           prefect_aws.S3Bucket
    GcsBucket.write_path()          S3Bucket.upload_from_file_object()
              │                                │
    gs://bucket/github/             s3://bucket/github/
    pull_request/                   pull_request/
    org={org}/date={date}/          org={org}/date={date}/
              │                                │
    Snowflake GCS Stage             Snowflake S3 Stage
    STORAGE_INTEGRATION             STORAGE_INTEGRATION
    (GCS service account)           (IAM role trust policy)
              │                                │
              └───────────────┬────────────────┘
                              │
                   COPY INTO staging table
                   MERGE INTO target table     ← identical for both
                   TRUNCATE staging
                              │
                   GITHUB_PIPELINES.RAW_TABLES.*
```

---

## What Changes Between GCS and AWS

| | GCS Option | AWS Option |
|---|---|---|
| **Prefect package** | `prefect-gcp` | `prefect-aws` |
| **Secrets block** | `GcpSecret` → GCP Secret Manager | `AwsSecret` → AWS Secrets Manager |
| **Storage block** | `GcsBucket` | `S3Bucket` |
| **Upload method** | `gcs.write_path(path, content=bytes)` | `s3.upload_from_file_object(BytesIO(bytes), to_path=path)` |
| **Storage URI** | `gs://bucket/path` | `s3://bucket/path` |
| **Snowflake auth** | GCS service account email (granted via IAM) | IAM role trust policy (ExternalId condition) |
| **Snowflake integration** | `STORAGE_PROVIDER = 'GCS'` | `STORAGE_PROVIDER = 'S3'` |
| **Stage setup** | Grant service account Storage Object Admin | Create IAM role + trust policy + ExternalId |
| **COPY statement** | `FROM 'gs://...' STORAGE_INTEGRATION = gcs_integration` | `FROM 's3://...' STORAGE_INTEGRATION = s3_integration` |
| **MERGE statement** | identical | identical |

---

## Files

```
approach-2-custom-pipeline/
├── prefect_flows/
│   ├── github_pr_ingestion_flow.py          GCS option (prefect-gcp)
│   └── github_pr_ingestion_flow_aws.py      AWS option (prefect-aws)
└── sql/
    └── snowflake_aws_stage_setup.sql        S3 storage integration setup
                                             (IAM role, trust policy, stage, file format)
```

---

## GCS Option — `github_pr_ingestion_flow.py`

### Full flow walkthrough

```
1. GcpSecret.load("github-api-token")
        │
        │  Loads GitHub PAT from GCP Secret Manager.
        │  The token never touches env vars or config files.
        ▼
2. httpx.Client.get("/orgs/{org}/pulls?state=all&sort=updated&since=...")
        │
        │  Cursor-based pagination via Link: <url>; rel="next" header.
        │  GitHub deprecates offset pagination for large orgs.
        │  Retries: 3 attempts, 60s delay.
        ▼
3. pa.Table.from_pylist(rows) → pq.write_table(table, buf, compression="snappy")
        │
        │  PyArrow serialises to Parquet bytes in memory.
        │  No temp files — bytes are passed directly to the upload task.
        ▼
4. GcsBucket.load("gcs-data-bucket").write_path(path, content=parquet_bytes)
        │
        │  Path: github/pull_request/org={org}/date={date}/pull_request.parquet
        │  Hive-style partitioning enables efficient incremental loads.
        │  Returns: gs://bucket/github/pull_request/org=.../pull_request.parquet
        ▼
5. snowflake.connector.connect(private_key_path=...)
        │
        │  Key-pair auth — no passwords in environment variables.
        ▼
   COPY INTO PULL_REQUEST_STAGE
   FROM 'gs://...'
   STORAGE_INTEGRATION = gcs_snowflake_integration
   FILE_FORMAT = (TYPE='PARQUET' USE_LOGICAL_TYPE=TRUE)
        │
        ▼
   MERGE INTO PULL_REQUEST ON _row_key
   UPDATE: state, updated_at, merged_at, closed_at (if _ingested_at is newer)
   INSERT: new rows
        │
        ▼
   TRUNCATE PULL_REQUEST_STAGE
```

### Setup

```bash
pip install prefect prefect-gcp httpx pyarrow snowflake-connector-python pydantic-settings
```

```python
# Create Prefect blocks once (run interactively)
from prefect_gcp import GcsBucket
from prefect_gcp.secret_manager import GcpSecret

GcsBucket(bucket="your-data-bucket").save("gcs-data-bucket")
# Create the secret in GCP first:
# gcloud secrets create github-api-token --data-file=<(echo -n "ghp_your_token")
GcpSecret(secret_name="github-api-token", project="your-gcp-project").save("github-api-token")
```

```bash
# Snowflake GCS stage setup (run once)
# 1. Create storage integration:
#    CREATE STORAGE INTEGRATION gcs_snowflake_integration
#        TYPE = EXTERNAL_STAGE  STORAGE_PROVIDER = 'GCS'  ENABLED = TRUE
#        STORAGE_ALLOWED_LOCATIONS = ('gcs://your-data-bucket/github/');
# 2. DESC INTEGRATION gcs_snowflake_integration  → get STORAGE_GCS_SERVICE_ACCOUNT
# 3. Grant that email Storage Object Admin on the GCS bucket

export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="svc_prefect"
export SNOWFLAKE_PRIVATE_KEY_PATH="~/.ssh/snowflake_key.p8"
export GITHUB_ORGS='["my-org-1", "my-org-2"]'

python prefect_flows/github_pr_ingestion_flow.py

# Deploy with a schedule
prefect deploy --name github-pr-ingestion-gcs --cron "0 */6 * * *"
```

---

## AWS Option — `github_pr_ingestion_flow_aws.py`

### Full flow walkthrough

```
1. AwsSecret.load("github-api-token-aws").read_secret()
        │
        │  Loads GitHub PAT from AWS Secrets Manager.
        │  AwsSecret.read_secret() returns str directly (no .decode() needed).
        │  Auth resolves: Prefect block credentials → env vars → EC2 instance profile.
        ▼
2. httpx.Client.get("/orgs/{org}/pulls?state=all&sort=updated&since=...")
        │
        │  [identical to GCS option]
        ▼
3. pa.Table.from_pylist(rows) → pq.write_table(table, buf, compression="snappy")
        │
        │  [identical to GCS option]
        ▼
4. S3Bucket.load("s3-data-bucket").upload_from_file_object(BytesIO(parquet_bytes), to_path=path)
        │
        │  BytesIO wrapper required — S3Bucket takes a file-like object, not raw bytes.
        │  Path: github/pull_request/org={org}/date={date}/pull_request.parquet
        │  Returns: s3://bucket/github/pull_request/org=.../pull_request.parquet
        ▼
5. snowflake.connector.connect(private_key_path=...)
        │
        │  [identical to GCS option — Snowflake connector is cloud-agnostic]
        ▼
   COPY INTO PULL_REQUEST_STAGE
   FROM 's3://...'
   STORAGE_INTEGRATION = s3_snowflake_integration   ← IAM role, no access keys
   FILE_FORMAT = (TYPE='PARQUET' USE_LOGICAL_TYPE=TRUE SNAPPY_COMPRESSION=TRUE)
        │
        ▼
   MERGE INTO PULL_REQUEST ON _row_key
   [identical to GCS option]
        │
        ▼
   TRUNCATE PULL_REQUEST_STAGE
```

### Setup

```bash
pip install prefect prefect-aws httpx pyarrow snowflake-connector-python pydantic-settings boto3
```

```python
# Create Prefect blocks once
from prefect_aws import S3Bucket
from prefect_aws.credentials import AwsCredentials
from prefect_aws.secrets_manager import AwsSecret

# Optional: create a credentials block (or rely on env vars / instance profile)
AwsCredentials(
    aws_access_key_id="...",        # or leave empty to use instance profile
    aws_secret_access_key="...",
    region_name="us-east-1",
).save("aws-credentials")

S3Bucket(bucket_name="your-data-bucket", credentials=AwsCredentials.load("aws-credentials")).save("s3-data-bucket")

# Create the secret in AWS Secrets Manager first:
# aws secretsmanager create-secret --name github-api-token --secret-string "ghp_your_token"
AwsSecret(secret_name="github-api-token").save("github-api-token-aws")
```

**S3 storage integration in Snowflake** — see [`sql/snowflake_aws_stage_setup.sql`](./sql/snowflake_aws_stage_setup.sql) for the full step-by-step:

```
Step 1  CREATE STORAGE INTEGRATION s3_snowflake_integration
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-role'
              │
Step 2  DESC INTEGRATION s3_snowflake_integration
        → copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
              │
Step 3  Update IAM role trust policy with those two values
        (ExternalId condition prevents confused-deputy attacks)
              │
Step 4  CREATE STAGE github_s3_stage
        URL = 's3://your-data-bucket/github/'
        STORAGE_INTEGRATION = s3_snowflake_integration
              │
Step 5  LIST @github_s3_stage  ← verify access
```

```bash
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="svc_prefect"
export SNOWFLAKE_PRIVATE_KEY_PATH="~/.ssh/snowflake_key.p8"
export GITHUB_ORGS='["my-org-1", "my-org-2"]'
export S3_BUCKET_BLOCK="s3-data-bucket"
export AWS_SECRET_BLOCK="github-api-token-aws"

python prefect_flows/github_pr_ingestion_flow_aws.py

# Deploy with a schedule
prefect deploy --name github-pr-ingestion-aws --cron "0 */6 * * *"
```

---

## Shared Downstream

Once either flow has populated `GITHUB_PIPELINES.RAW_TABLES.*`, the downstream transformation layers are **identical**:

```
approach-2-custom-pipeline/  ← GCS or AWS, your choice
        ↓
../sql-jobs/reporting_data_model.sql
        ↓
../sql-jobs/lead_time_to_deploy.sql
        ↓
../dbt-github-insights/   (use _sources_custom.yml)
```

---

## Pros and Cons

| | GCS Option | AWS Option |
|---|---|---|
| **Best for** | GCP-native stacks | AWS-native stacks |
| **Secret auth** | GCP Service Account | IAM role trust policy |
| **Snowflake stage auth** | Service account email → GCS IAM | IAM role + ExternalId trust policy |
| **Prefect block** | `prefect-gcp` | `prefect-aws` |
| **Boto3 required** | No | Yes (boto3 / prefect-aws) |
| **Upload method** | `write_path(path, content=bytes)` | `upload_from_file_object(BytesIO, path)` |
| **Long-lived credentials** | No (service account key optional) | No (instance profile / role) |
