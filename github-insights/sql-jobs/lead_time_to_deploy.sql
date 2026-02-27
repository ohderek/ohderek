/*
  lead_time_to_deploy.sql
  ─────────────────────────────────────────────────────────────────────────────
  Hypothetical Showcase: DORA Lead Time to Deploy ETL for the GitHub Insights
  platform.

  Purpose : Calculate the time from first commit → staging → production for
            every merged PR, using a 7-stage cascading matching algorithm.

  Matching strategy:
    1. SHA match (exact commit SHA in deployment record)  — preferred
    2. Time-based fallback (first deployment after merge for the service)

  Grain : pull_request_id × service × merge_commit_sha × deployed_commit_sha
          (one PR touching multiple services → multiple rows; one row per
           unique deployment event matched to the PR)

  All org names, service registry names, and internal tool references
  are generic. No proprietary data is included.
  ─────────────────────────────────────────────────────────────────────────────
*/


-- =============================================================================
-- STAGE 1: Service-to-repository mapping
-- Maps service registry entries → normalized GitHub repo paths.
-- Longest-prefix matching used in Stage 3 handles monorepos correctly.
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE service_by_repopath AS

WITH raw_services AS (
  SELECT
    slug                                            AS service,
    reliability_tier,
    LOWER(REGEXP_REPLACE(git_repo, '^https?://', ''))
                                                    AS repo_base,
    LOWER(TRIM(git_repo_path, '/'))                 AS repo_subpath

  FROM SERVICE_REGISTRY.APPLICATIONS
  WHERE decommissioned_at IS NULL
    AND git_repo IS NOT NULL
),

/*
  Build full repo path. Monorepo services have a git_repo_path sub-directory;
  standard repos have repo_subpath = '' and the path is just the repo base.
*/
with_paths AS (
  SELECT
    service,
    reliability_tier,
    CASE
      WHEN repo_subpath IS NOT NULL AND repo_subpath <> ''
        THEN CONCAT(repo_base, '/', repo_subpath)
      ELSE repo_base
    END AS repo_path
  FROM raw_services
),

/*
  Deduplicate: when multiple services share the same repo path (common in
  large monorepos), keep the highest-tier (lowest tier number) service.
  Exact name match between service slug and final path segment is used as
  a tiebreak to ensure the "primary" service wins.
*/
deduped AS (
  SELECT
    service,
    repo_path,
    reliability_tier
  FROM with_paths
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY repo_path
      ORDER BY
        reliability_tier ASC,
        IFF(SPLIT_PART(repo_path, '/', -1) = service, 0, 1) ASC
    ) = 1
)

SELECT * FROM deduped;


-- =============================================================================
-- STAGE 2: Commit-to-file mapping
-- Joins commits → changed files → PRs → repositories.
-- Filters out PR merge commits (noise).
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE commit_file_mappings AS

SELECT
  pr.id                                                                     AS pull_request_id,
  CONCAT('github.com/', r.full_name, '/pull/', i.number)                   AS url,
  c.sha                                                                     AS commit_sha,
  cf.filename,
  LOWER(CONCAT('github.com/', r.full_name, '/', cf.filename))              AS full_filepath,
  r.full_name,
  SPLIT_PART(r.full_name, '/', 1)                                           AS org

FROM GITHUB_PIPELINES.RAW_TABLES.COMMIT             c
JOIN GITHUB_PIPELINES.RAW_TABLES.COMMIT_PULL_REQUEST cpr ON c.sha          = cpr.commit_sha
JOIN GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST        pr  ON cpr.pull_request_id = pr.id
JOIN GITHUB_PIPELINES.RAW_TABLES.ISSUE               i   ON pr.id          = i.pull_request_id
JOIN GITHUB_PIPELINES.RAW_TABLES.REPOSITORY          r   ON pr.repository_id   = r.id
JOIN GITHUB_PIPELINES.RAW_TABLES.COMMIT_FILE         cf  ON c.sha          = cf.commit_sha

WHERE pr.created_at >= '2024-01-01'
  -- Exclude PR merge commits (message patterns common to GitHub squash/merge)
  AND NOT (
        c.message LIKE 'Merge pull request #%'
     OR c.message LIKE 'Merge branch%'
  );


-- =============================================================================
-- STAGE 3: File-to-service assignment (longest prefix match)
-- For each (commit, file), find the most specific matching service repo path.
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE commit_file_services AS

SELECT
  cfm.*,
  svc.service,
  LENGTH(svc.repo_path) AS match_length

FROM commit_file_mappings cfm
JOIN service_by_repopath  svc
  ON STARTSWITH(cfm.full_filepath, svc.repo_path)

QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY cfm.commit_sha, cfm.filename
    ORDER BY LENGTH(svc.repo_path) DESC  -- longest match = most specific
  ) = 1;


-- =============================================================================
-- STAGE 4: PR timeline construction  (PR × service grain)
-- Aggregates commit dates, identifies default-branch merges.
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_service_timeline AS

SELECT
  cfs.pull_request_id,
  cfs.url,
  cfs.full_name                                                             AS repo_name,
  cfs.service,
  pr.merge_commit_sha,
  MIN(c.committer_date)                                                     AS first_commit_date,
  CAST(pr.merged_at AS TIMESTAMP_NTZ)                                       AS merged_at

FROM commit_file_services cfs
JOIN GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST pr
  ON cfs.pull_request_id = pr.id
JOIN GITHUB_PIPELINES.RAW_TABLES.COMMIT c
  ON cfs.commit_sha = c.sha

WHERE cfs.service IS NOT NULL
  AND pr.merged_at IS NOT NULL
  AND pr.merged_at >= '1970-01-01'
  AND LOWER(pr.base_ref) IN ('main', 'master', 'trunk', 'live')

GROUP BY 1, 2, 3, 4, 5, 7;


-- =============================================================================
-- STAGE 5a: Staging deployments — SHA match (preferred)
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_deployments_sha_staging AS

SELECT
  pst.*,
  d.git_commit_sha                                                          AS staging_deployed_commit_sha,
  d.deployment_completed_at                                                 AS first_staging_deploy_time,
  d.deployment_source                                                       AS staging_deployment_source,
  'sha_match'                                                               AS staging_match_scenario

FROM pr_service_timeline pst
JOIN OPS_PERFORMANCE.REPORTING_TABLES.FACT_DEPLOYMENTS d
  ON  pst.merge_commit_sha = d.git_commit_sha
  AND pst.service          = d.application
WHERE d.environment IN ('staging', 'stage')
  AND d.success = TRUE
  AND d.deployment_completed_at > pst.merged_at;


-- =============================================================================
-- STAGE 5b: Staging deployments — time-based fallback
-- First deployment to staging for the service after PR merge.
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_deployments_time_staging AS

SELECT
  pst.*,
  d.git_commit_sha                                                          AS staging_deployed_commit_sha,
  d.deployment_completed_at                                                 AS first_staging_deploy_time,
  d.deployment_source                                                       AS staging_deployment_source,
  'time_after_merge'                                                        AS staging_match_scenario

FROM pr_service_timeline pst
JOIN OPS_PERFORMANCE.REPORTING_TABLES.FACT_DEPLOYMENTS d
  ON  pst.service = d.application
  AND d.deployment_completed_at > pst.merged_at
WHERE d.environment IN ('staging', 'stage')
  AND d.success = TRUE

QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY pst.pull_request_id, pst.service
    ORDER BY d.deployment_completed_at ASC
  ) = 1;


-- =============================================================================
-- STAGE 5c: Deduplicate staging matches (SHA wins, then earliest time)
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_first_staging AS

SELECT *
FROM (
  SELECT *, 1 AS priority FROM pr_deployments_sha_staging
  UNION ALL
  SELECT *, 2 AS priority FROM pr_deployments_time_staging
)
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY pull_request_id, service
    ORDER BY
      priority                ASC,
      first_staging_deploy_time ASC
  ) = 1;


-- =============================================================================
-- STAGE 6a: Production deployments — SHA match (preferred)
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_deployments_sha AS

SELECT
  pst.*,
  d.git_commit_sha                                                          AS deployed_commit_sha,
  d.deployment_completed_at                                                 AS first_prod_deploy_time,
  d.deployment_source                                                       AS prod_deployment_source,
  'sha_match'                                                               AS match_scenario,
  TIMEDIFF('HOUR',  pst.first_commit_date, d.deployment_completed_at)      AS lead_time_hours,
  TIMEDIFF('DAY',   pst.first_commit_date, d.deployment_completed_at)      AS lead_time_days

FROM pr_service_timeline pst
JOIN OPS_PERFORMANCE.REPORTING_TABLES.FACT_DEPLOYMENTS d
  ON  pst.merge_commit_sha = d.git_commit_sha
  AND pst.service          = d.application
WHERE d.environment = 'production'
  AND d.success     = TRUE;


-- =============================================================================
-- STAGE 6b: Production deployments — time-based fallback
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE pr_deployments_time AS

SELECT
  pst.*,
  d.git_commit_sha                                                          AS deployed_commit_sha,
  d.deployment_completed_at                                                 AS first_prod_deploy_time,
  d.deployment_source                                                       AS prod_deployment_source,
  'time_after_merge'                                                        AS match_scenario,
  TIMEDIFF('HOUR',  pst.first_commit_date, d.deployment_completed_at)      AS lead_time_hours,
  TIMEDIFF('DAY',   pst.first_commit_date, d.deployment_completed_at)      AS lead_time_days

FROM pr_service_timeline pst
JOIN OPS_PERFORMANCE.REPORTING_TABLES.FACT_DEPLOYMENTS d
  ON  pst.service = d.application
  AND d.deployment_completed_at > pst.merged_at
WHERE d.environment = 'production'
  AND d.success     = TRUE

QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY pst.pull_request_id, pst.service
    ORDER BY d.deployment_completed_at ASC
  ) = 1;


-- =============================================================================
-- STAGE 7: Final output assembly
-- Union production matches → dedup (SHA wins) → join staging → MERGE target.
-- Only rows with a production deployment are included.
-- =============================================================================

MERGE INTO GITHUB_PIPELINES.REPORTING_TABLES.LEAD_TIME_TO_DEPLOY AS TARGET
USING (

  WITH

  all_prod AS (
    SELECT *, 1 AS priority FROM pr_deployments_sha
    UNION ALL
    SELECT *, 2 AS priority FROM pr_deployments_time
  ),

  /*
    Deduplicate production matches per (PR, service).
    SHA matches take priority; earliest deployment time as tiebreak.
  */
  prod_dedup AS (
    SELECT *
    FROM all_prod
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY pull_request_id, service
        ORDER BY
          priority                ASC,
          first_prod_deploy_time  ASC
      ) = 1
  ),

  final AS (
    SELECT
      MD5_HEX(
        CONCAT(
          COALESCE(TO_VARCHAR(pd.pull_request_id), ''), '|',
          COALESCE(pd.service,                     ''), '|',
          COALESCE(pd.merge_commit_sha,             ''), '|',
          COALESCE(pd.deployed_commit_sha,          '')
        )
      )                                                               AS pk,

      pd.pull_request_id,
      pd.url,
      pd.service,
      pd.repo_name,
      pd.org,
      pd.merge_commit_sha,
      pd.deployed_commit_sha,
      pd.first_commit_date,
      pd.merged_at,

      -- Staging fields (NULL when service deploys direct-to-production)
      ps.staging_deployed_commit_sha,
      ps.first_staging_deploy_time,
      ps.staging_deployment_source,
      ps.staging_match_scenario,

      -- Production fields
      pd.first_prod_deploy_time,
      pd.prod_deployment_source,
      pd.match_scenario                                               AS prod_match_scenario,

      -- Lead time metrics
      pd.lead_time_hours                                              AS lead_time_to_prod_hours,
      pd.lead_time_days                                               AS lead_time_to_prod_days,

      TIMEDIFF('HOUR', pd.merged_at, pd.first_prod_deploy_time)      AS merge_to_prod_hours,

      CASE
        WHEN ps.first_staging_deploy_time IS NOT NULL
          THEN TIMEDIFF('HOUR', pd.merged_at, ps.first_staging_deploy_time)
      END                                                             AS time_to_staging_hours,

      CASE
        WHEN ps.first_staging_deploy_time IS NOT NULL
         AND pd.first_prod_deploy_time >= ps.first_staging_deploy_time
          THEN TIMEDIFF('HOUR', ps.first_staging_deploy_time, pd.first_prod_deploy_time)
      END                                                             AS time_on_staging_hours,

      -- DORA bucket classification
      CASE
        WHEN pd.lead_time_hours < 1            THEN 'elite'    -- < 1 hour
        WHEN pd.lead_time_hours < 24           THEN 'high'     -- < 1 day
        WHEN pd.lead_time_hours < 168          THEN 'medium'   -- < 1 week
        WHEN pd.lead_time_hours < 4380         THEN 'low'      -- < 6 months
        ELSE 'low'
      END                                                             AS dora_lead_time_bucket,

      CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                              AS data_synced_at

    FROM prod_dedup pd
    LEFT JOIN pr_first_staging ps
      ON pd.pull_request_id = ps.pull_request_id
      AND pd.service        = ps.service

    WHERE pd.first_prod_deploy_time IS NOT NULL  -- Exclude staging-only records
  )

  SELECT * FROM final

) AS SOURCE
ON TARGET.PK = SOURCE.PK

WHEN MATCHED THEN
  UPDATE SET
    DEPLOYED_COMMIT_SHA        = SOURCE.DEPLOYED_COMMIT_SHA,
    FIRST_STAGING_DEPLOY_TIME  = SOURCE.FIRST_STAGING_DEPLOY_TIME,
    FIRST_PROD_DEPLOY_TIME     = SOURCE.FIRST_PROD_DEPLOY_TIME,
    LEAD_TIME_TO_PROD_HOURS    = SOURCE.LEAD_TIME_TO_PROD_HOURS,
    LEAD_TIME_TO_PROD_DAYS     = SOURCE.LEAD_TIME_TO_PROD_DAYS,
    MERGE_TO_PROD_HOURS        = SOURCE.MERGE_TO_PROD_HOURS,
    TIME_TO_STAGING_HOURS      = SOURCE.TIME_TO_STAGING_HOURS,
    TIME_ON_STAGING_HOURS      = SOURCE.TIME_ON_STAGING_HOURS,
    DORA_LEAD_TIME_BUCKET      = SOURCE.DORA_LEAD_TIME_BUCKET,
    PROD_MATCH_SCENARIO        = SOURCE.PROD_MATCH_SCENARIO,
    DATA_SYNCED_AT             = SOURCE.DATA_SYNCED_AT

WHEN NOT MATCHED THEN
  INSERT (
    PK, PULL_REQUEST_ID, URL, SERVICE, REPO_NAME, ORG,
    MERGE_COMMIT_SHA, DEPLOYED_COMMIT_SHA,
    FIRST_COMMIT_DATE, MERGED_AT,
    STAGING_DEPLOYED_COMMIT_SHA, FIRST_STAGING_DEPLOY_TIME,
    STAGING_DEPLOYMENT_SOURCE, STAGING_MATCH_SCENARIO,
    FIRST_PROD_DEPLOY_TIME, PROD_DEPLOYMENT_SOURCE, PROD_MATCH_SCENARIO,
    LEAD_TIME_TO_PROD_HOURS, LEAD_TIME_TO_PROD_DAYS,
    MERGE_TO_PROD_HOURS, TIME_TO_STAGING_HOURS, TIME_ON_STAGING_HOURS,
    DORA_LEAD_TIME_BUCKET, DATA_SYNCED_AT
  )
  VALUES (
    SOURCE.PK, SOURCE.PULL_REQUEST_ID, SOURCE.URL, SOURCE.SERVICE, SOURCE.REPO_NAME, SOURCE.ORG,
    SOURCE.MERGE_COMMIT_SHA, SOURCE.DEPLOYED_COMMIT_SHA,
    SOURCE.FIRST_COMMIT_DATE, SOURCE.MERGED_AT,
    SOURCE.STAGING_DEPLOYED_COMMIT_SHA, SOURCE.FIRST_STAGING_DEPLOY_TIME,
    SOURCE.STAGING_DEPLOYMENT_SOURCE, SOURCE.STAGING_MATCH_SCENARIO,
    SOURCE.FIRST_PROD_DEPLOY_TIME, SOURCE.PROD_DEPLOYMENT_SOURCE, SOURCE.PROD_MATCH_SCENARIO,
    SOURCE.LEAD_TIME_TO_PROD_HOURS, SOURCE.LEAD_TIME_TO_PROD_DAYS,
    SOURCE.MERGE_TO_PROD_HOURS, SOURCE.TIME_TO_STAGING_HOURS, SOURCE.TIME_ON_STAGING_HOURS,
    SOURCE.DORA_LEAD_TIME_BUCKET, SOURCE.DATA_SYNCED_AT
  );
