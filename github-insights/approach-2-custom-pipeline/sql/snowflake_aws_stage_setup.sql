-- snowflake_aws_stage_setup.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- One-time setup: connect Snowflake to an S3 bucket using an IAM role.
--
-- This is the AWS equivalent of granting a GCS service account access to a
-- bucket. Instead of a service account key, Snowflake assumes an IAM role
-- via a trust policy — no long-lived credentials are stored anywhere.
--
-- Run order:
--   Step 1  Create the storage integration in Snowflake
--   Step 2  Retrieve the Snowflake-managed IAM principal from DESC INTEGRATION
--   Step 3  Add a trust relationship to your IAM role (AWS console / Terraform)
--   Step 4  Create the external stage in Snowflake
--   Step 5  Create the file format
--   Step 6  Verify with a LIST command
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Prerequisites ─────────────────────────────────────────────────────────────
-- In AWS, create an IAM role (e.g. snowflake-s3-role) with this inline policy:
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       {
--         "Effect": "Allow",
--         "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"],
--         "Resource": "arn:aws:s3:::your-data-bucket/github/*"
--       },
--       {
--         "Effect": "Allow",
--         "Action": "s3:ListBucket",
--         "Resource": "arn:aws:s3:::your-data-bucket",
--         "Condition": { "StringLike": { "s3:prefix": ["github/*"] } }
--       }
--     ]
--   }
--
-- The trust policy for this role will be updated in Step 3 after Snowflake
-- gives us the IAM user ARN and external ID.


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1 — Create the storage integration
-- ─────────────────────────────────────────────────────────────────────────────
-- A storage integration is a Snowflake account-level object that holds the
-- IAM role ARN. All stages using this integration inherit the same IAM trust —
-- you don't embed credentials in individual COPY statements.

USE ROLE ACCOUNTADMIN;  -- storage integration creation requires ACCOUNTADMIN

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_snowflake_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::123456789012:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-data-bucket/github/')
    COMMENT                   = 'Snowflake → S3 integration for GitHub Insights ingestion';

-- Grant TRANSFORMER role usage on the integration (used by Prefect flow)
GRANT USAGE ON INTEGRATION s3_snowflake_integration TO ROLE TRANSFORMER;


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2 — Retrieve the Snowflake-managed IAM user ARN and external ID
-- ─────────────────────────────────────────────────────────────────────────────
-- Snowflake creates a dedicated IAM user in its own AWS account.
-- You must add this user + external ID to your role's trust policy.

DESC INTEGRATION s3_snowflake_integration;

-- Output will include two key properties:
--   STORAGE_AWS_IAM_USER_ARN  → copy this value (e.g. arn:aws:iam::111122223333:user/...)
--   STORAGE_AWS_EXTERNAL_ID   → copy this value (e.g. ABC12345_SFCRole=...)


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3 — Update the IAM role trust policy (done in AWS, not Snowflake)
-- ─────────────────────────────────────────────────────────────────────────────
-- In the AWS console (IAM → Roles → snowflake-s3-role → Trust relationships),
-- replace the existing trust policy with:
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       {
--         "Effect": "Allow",
--         "Principal": {
--           "AWS": "<STORAGE_AWS_IAM_USER_ARN from Step 2>"
--         },
--         "Action": "sts:AssumeRole",
--         "Condition": {
--           "StringEquals": {
--             "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Step 2>"
--           }
--         }
--       }
--     ]
--   }
--
-- The ExternalId condition prevents the "confused deputy" attack — ensures only
-- your specific Snowflake account can assume this role, not other Snowflake tenants.
--
-- Terraform equivalent:
--   resource "aws_iam_role" "snowflake_s3" {
--     assume_role_policy = jsonencode({
--       Version = "2012-10-17"
--       Statement = [{
--         Effect    = "Allow"
--         Principal = { AWS = "<STORAGE_AWS_IAM_USER_ARN>" }
--         Action    = "sts:AssumeRole"
--         Condition = {
--           StringEquals = { "sts:ExternalId" = "<STORAGE_AWS_EXTERNAL_ID>" }
--         }
--       }]
--     })
--   }


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4 — Create the external stage
-- ─────────────────────────────────────────────────────────────────────────────
-- A named stage is a reusable reference to the S3 path + integration.
-- COPY INTO statements reference the stage name, not the S3 URI directly.

USE ROLE      TRANSFORMER;
USE DATABASE  GITHUB_PIPELINES;
USE SCHEMA    RAW_TABLES;

CREATE OR REPLACE STAGE github_s3_stage
    URL                  = 's3://your-data-bucket/github/'
    STORAGE_INTEGRATION  = s3_snowflake_integration
    DIRECTORY            = (ENABLE = TRUE)
    COMMENT              = 'S3 landing zone for GitHub PR Parquet files';


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5 — Create the Parquet file format
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FILE FORMAT parquet_snappy
    TYPE                = 'PARQUET'
    USE_LOGICAL_TYPE    = TRUE    -- preserves timestamps, booleans as Parquet types
    SNAPPY_COMPRESSION  = TRUE
    NULL_IF             = ('', 'NULL', 'null');


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 6 — Verify the stage is working
-- ─────────────────────────────────────────────────────────────────────────────

-- List files in the stage (runs a HEAD request against S3)
LIST @github_s3_stage;

-- Query a Parquet file directly without loading (useful for schema inspection)
SELECT $1
FROM @github_s3_stage/pull_request/
(FILE_FORMAT => parquet_snappy)
LIMIT 5;


-- ─────────────────────────────────────────────────────────────────────────────
-- COPY + MERGE pattern (called by the Prefect flow on each run)
-- ─────────────────────────────────────────────────────────────────────────────
-- The Prefect flow calls these dynamically. Shown here for reference.

-- COPY from a specific S3 file (called by copy_to_snowflake_from_s3 task)
COPY INTO GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_STAGE
FROM 's3://your-data-bucket/github/pull_request/org=my-org/date=2024-01-15/pull_request.parquet'
STORAGE_INTEGRATION = s3_snowflake_integration
FILE_FORMAT         = (FORMAT_NAME = parquet_snappy)
ON_ERROR            = 'ABORT_STATEMENT'
PURGE               = FALSE;   -- keep source file; Prefect manages lifecycle

-- MERGE staging → target  (identical to GCS variant — Snowflake is cloud-agnostic)
MERGE INTO GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST AS tgt
USING GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_STAGE AS src
ON tgt._row_key = src._row_key
WHEN MATCHED AND tgt._ingested_at < src._ingested_at THEN UPDATE SET
    tgt._ingested_at = src._ingested_at,
    tgt.state        = src.state,
    tgt.updated_at   = src.updated_at,
    tgt.merged_at    = src.merged_at,
    tgt.closed_at    = src.closed_at
WHEN NOT MATCHED THEN
    INSERT SELECT *;

-- Truncate staging after merge
TRUNCATE TABLE GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_STAGE;


-- ─────────────────────────────────────────────────────────────────────────────
-- GCS equivalent for reference
-- ─────────────────────────────────────────────────────────────────────────────
-- If using GCS instead of S3, the stage setup looks like this:
--
--   CREATE STORAGE INTEGRATION gcs_snowflake_integration
--       TYPE                      = EXTERNAL_STAGE
--       STORAGE_PROVIDER          = 'GCS'
--       ENABLED                   = TRUE
--       STORAGE_ALLOWED_LOCATIONS = ('gcs://your-data-bucket/github/');
--
--   -- DESC INTEGRATION gives you a STORAGE_GCS_SERVICE_ACCOUNT email.
--   -- Grant that service account Storage Object Admin on the GCS bucket.
--
--   CREATE OR REPLACE STAGE github_gcs_stage
--       URL                 = 'gcs://your-data-bucket/github/'
--       STORAGE_INTEGRATION = gcs_snowflake_integration;
--
-- The COPY and MERGE statements are identical — only the stage URL changes.
