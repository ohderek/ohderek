/*
  reporting_data_model.sql
  ─────────────────────────────────────────────────────────────────────────────
  Hypothetical Showcase: Reporting layer SQL for the GitHub Insights platform.

  Purpose : Build the FACT and BRIDGE tables that power engineering velocity
            dashboards — PR facts with author identity resolution, a
            many-to-many GitHub↔LDAP bridge, and downstream aggregations.

  Patterns demonstrated:
    - Incremental MERGE with HASHDIFF change detection
    - Multi-source identity resolution (GitHub login → LDAP)
    - Bot / human author classification
    - Surrogate key generation with MD5_HEX

  All org names, schema names, and internal service references are generic.
  No proprietary data is included.
  ─────────────────────────────────────────────────────────────────────────────
*/


-- =============================================================================
-- BRIDGE_GITHUB_LDAP  (many-to-many GitHub login ↔ LDAP identity)
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.REPORTING_TABLES.BRIDGE_GITHUB_LDAP (
  PK             STRING,
  LDAP           STRING,
  GITHUB         STRING,
  DATA_SYNCED_AT TIMESTAMP_NTZ,
  HASHDIFF       STRING
);

MERGE INTO GITHUB_PIPELINES.REPORTING_TABLES.BRIDGE_GITHUB_LDAP AS TARGET
USING (

  WITH PARAMS AS (
    SELECT COALESCE(MAX(DATA_SYNCED_AT), '1970-01-01'::TIMESTAMP_NTZ) AS MAX_DATA_SYNCED_AT
    FROM GITHUB_PIPELINES.REPORTING_TABLES.BRIDGE_GITHUB_LDAP
  ),

  /* Source 1: authoritative identities cache (includes deleted, filter accordingly) */
  IDENTITIES AS (
    SELECT
      SPLIT_PART(LOWER(LDAP),   '@', 1) AS LDAP,
      LOWER(GITHUB)                     AS GITHUB,
      CAST(_FIVETRAN_SYNCED AS TIMESTAMP_NTZ) AS SRC_SYNCED_AT
    FROM FIVETRAN.IDENTITY_SERVICE.IDENTITIES_CACHE
    WHERE COALESCE(_FIVETRAN_DELETED, FALSE) = FALSE
      AND LDAP   IS NOT NULL
      AND GITHUB IS NOT NULL
  ),

  /* Source 2: existing USER_LOOKUP (legacy, lower priority) */
  USER_LOOKUP AS (
    SELECT
      SPLIT_PART(LOWER(LDAP), '@', 1) AS LDAP,
      LOWER(GITHUB)                   AS GITHUB,
      NULL::TIMESTAMP_NTZ             AS SRC_SYNCED_AT  -- treated as older
    FROM GITHUB_PIPELINES.RAW_TABLES.USER_LOOKUP
    WHERE LDAP   IS NOT NULL
      AND GITHUB IS NOT NULL
  ),

  UNIONED AS (
    SELECT * FROM IDENTITIES
    UNION ALL
    SELECT * FROM USER_LOOKUP
  ),

  /* Incremental filter: only rows that could have changed since last run */
  INCREMENTAL AS (
    SELECT u.*
    FROM UNIONED u
    CROSS JOIN PARAMS p
    WHERE COALESCE(u.SRC_SYNCED_AT, p.MAX_DATA_SYNCED_AT)
          >= DATEADD('DAY', -14, p.MAX_DATA_SYNCED_AT)
  ),

  DEDUPED AS (
    SELECT
      LDAP,
      GITHUB,
      MAX(SRC_SYNCED_AT) AS DATA_SYNCED_AT
    FROM INCREMENTAL
    GROUP BY 1, 2
  ),

  FINAL AS (
    SELECT
      MD5_HEX(CONCAT(COALESCE(LDAP, ''), '|', COALESCE(GITHUB, ''))) AS PK,
      LDAP,
      GITHUB,
      COALESCE(DATA_SYNCED_AT, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)   AS DATA_SYNCED_AT,
      MD5_HEX(CONCAT(COALESCE(LDAP, ''), '|', COALESCE(GITHUB, ''))) AS HASHDIFF
    FROM DEDUPED
  )

  SELECT * FROM FINAL

) AS SOURCE
ON TARGET.PK = SOURCE.PK

WHEN MATCHED AND TARGET.HASHDIFF <> SOURCE.HASHDIFF THEN
  UPDATE SET
    LDAP           = SOURCE.LDAP,
    GITHUB         = SOURCE.GITHUB,
    DATA_SYNCED_AT = SOURCE.DATA_SYNCED_AT,
    HASHDIFF       = SOURCE.HASHDIFF

WHEN NOT MATCHED THEN
  INSERT (PK, LDAP, GITHUB, DATA_SYNCED_AT, HASHDIFF)
  VALUES (SOURCE.PK, SOURCE.LDAP, SOURCE.GITHUB, SOURCE.DATA_SYNCED_AT, SOURCE.HASHDIFF);


-- =============================================================================
-- FACT_PULL_REQUESTS
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.REPORTING_TABLES.FACT_PULL_REQUESTS (
  PK               STRING,
  PR_ID            NUMBER,
  ORG              STRING,
  NUMBER           NUMBER,
  TITLE            STRING,
  URL              STRING,
  STATE            STRING,
  BASE_REF         STRING,
  BASE_SHA         STRING,
  HEAD_REF         STRING,
  HEAD_SHA         STRING,
  MERGE_COMMIT_SHA STRING,
  DRAFT            BOOLEAN,
  MERGED           BOOLEAN,
  MERGED_AT        TIMESTAMP_NTZ,
  CREATED_AT       TIMESTAMP_NTZ,
  UPDATED_AT       TIMESTAMP_NTZ,
  CLOSED_AT        TIMESTAMP_NTZ,
  REPO_FULL_NAME   STRING,
  REPO_ID          NUMBER,
  USER_LOGIN       STRING,
  USER_LDAP        STRING,
  AUTHOR_TYPE      STRING,   -- human | bot | service_account
  DATA_SYNCED_AT   TIMESTAMP_NTZ,
  HASHDIFF         STRING
);

MERGE INTO GITHUB_PIPELINES.REPORTING_TABLES.FACT_PULL_REQUESTS AS TARGET
USING (

  WITH

  /*
    Resolve GitHub login → LDAP using the bridge table.
    Prefer most recently synced mapping when multiple LDAP entries exist for
    the same GitHub handle (can happen during employee migrations).
  */
  github_to_ldap AS (
    SELECT
      LOWER(github) AS github,
      LOWER(ldap)   AS ldap
    FROM (
      SELECT
        github,
        ldap,
        data_synced_at,
        ROW_NUMBER() OVER (
          PARTITION BY LOWER(github)
          ORDER BY data_synced_at DESC NULLS LAST, LOWER(ldap) ASC
        ) AS rn
      FROM GITHUB_PIPELINES.REPORTING_TABLES.BRIDGE_GITHUB_LDAP
      WHERE github IS NOT NULL AND ldap IS NOT NULL
    )
    WHERE rn = 1
  ),

  repos AS (
    SELECT id AS repo_id, full_name
    FROM GITHUB_PIPELINES.RAW_TABLES.REPOSITORY
  ),

  users AS (
    SELECT id AS user_id, login
    FROM GITHUB_PIPELINES.RAW_TABLES.USER
  ),

  source_prs AS (
    SELECT
      MD5_HEX(CONCAT(LOWER(r.full_name), '|', TO_VARCHAR(pr.number))) AS pk,

      pr.id                                         AS pr_id,
      SPLIT_PART(r.full_name, '/', 1)               AS org,
      pr.number,
      pr.title,
      CONCAT('github.com/', r.full_name, '/pull/', pr.number) AS url,
      pr.state,
      pr.base_ref,
      pr.base_sha,
      pr.head_ref,
      pr.head_sha,
      pr.merge_commit_sha,
      pr.draft,
      IFF(pr.merged_at IS NOT NULL, TRUE, FALSE)    AS merged,
      CAST(pr.merged_at  AS TIMESTAMP_NTZ)          AS merged_at,
      CAST(pr.created_at AS TIMESTAMP_NTZ)          AS created_at,
      CAST(pr.updated_at AS TIMESTAMP_NTZ)          AS updated_at,
      CAST(pr.closed_at  AS TIMESTAMP_NTZ)          AS closed_at,
      r.full_name                                   AS repo_full_name,
      r.repo_id,
      u.login                                       AS user_login,
      gl.ldap                                       AS user_ldap,

      /*
        Author type classification:
          - '[bot]' suffix  → GitHub App / Action bot
          - 'svc-' prefix   → service account (automated tooling)
          - 'ci-' prefix    → CI system account
          - else            → human engineer
      */
      CASE
        WHEN LOWER(u.login) LIKE '%[bot]%'  THEN 'bot'
        WHEN LOWER(u.login) LIKE 'svc-%'    THEN 'service_account'
        WHEN LOWER(u.login) LIKE 'ci-%'     THEN 'service_account'
        ELSE 'human'
      END                                           AS author_type,

      CURRENT_TIMESTAMP()::TIMESTAMP_NTZ            AS data_synced_at,

      MD5_HEX(CONCAT(
        COALESCE(pr.state,            ''), '|',
        COALESCE(TO_VARCHAR(pr.merged_at), ''), '|',
        COALESCE(TO_VARCHAR(pr.closed_at), ''), '|',
        COALESCE(pr.merge_commit_sha, '')
      ))                                            AS hashdiff

    FROM GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST pr
    JOIN repos r  ON pr.repository_id = r.repo_id
    JOIN users u  ON pr.user_id       = u.user_id
    LEFT JOIN github_to_ldap gl ON LOWER(u.login) = gl.github

    WHERE pr.created_at >= DATEADD('day', -{{ lookback_days | default(7) }}, CURRENT_DATE())
  )

  SELECT * FROM source_prs

) AS SOURCE
ON TARGET.PK = SOURCE.PK

WHEN MATCHED AND TARGET.HASHDIFF <> SOURCE.HASHDIFF THEN
  UPDATE SET
    STATE            = SOURCE.STATE,
    MERGED           = SOURCE.MERGED,
    MERGED_AT        = SOURCE.MERGED_AT,
    CLOSED_AT        = SOURCE.CLOSED_AT,
    UPDATED_AT       = SOURCE.UPDATED_AT,
    MERGE_COMMIT_SHA = SOURCE.MERGE_COMMIT_SHA,
    USER_LDAP        = SOURCE.USER_LDAP,
    DATA_SYNCED_AT   = SOURCE.DATA_SYNCED_AT,
    HASHDIFF         = SOURCE.HASHDIFF

WHEN NOT MATCHED THEN
  INSERT (
    PK, PR_ID, ORG, NUMBER, TITLE, URL, STATE,
    BASE_REF, BASE_SHA, HEAD_REF, HEAD_SHA, MERGE_COMMIT_SHA,
    DRAFT, MERGED, MERGED_AT, CREATED_AT, UPDATED_AT, CLOSED_AT,
    REPO_FULL_NAME, REPO_ID, USER_LOGIN, USER_LDAP, AUTHOR_TYPE,
    DATA_SYNCED_AT, HASHDIFF
  )
  VALUES (
    SOURCE.PK, SOURCE.PR_ID, SOURCE.ORG, SOURCE.NUMBER, SOURCE.TITLE, SOURCE.URL, SOURCE.STATE,
    SOURCE.BASE_REF, SOURCE.BASE_SHA, SOURCE.HEAD_REF, SOURCE.HEAD_SHA, SOURCE.MERGE_COMMIT_SHA,
    SOURCE.DRAFT, SOURCE.MERGED, SOURCE.MERGED_AT, SOURCE.CREATED_AT, SOURCE.UPDATED_AT, SOURCE.CLOSED_AT,
    SOURCE.REPO_FULL_NAME, SOURCE.REPO_ID, SOURCE.USER_LOGIN, SOURCE.USER_LDAP, SOURCE.AUTHOR_TYPE,
    SOURCE.DATA_SYNCED_AT, SOURCE.HASHDIFF
  );


-- =============================================================================
-- FACT_PR_REVIEW_SUMMARY  (aggregated review stats per PR)
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.REPORTING_TABLES.FACT_PR_REVIEW_SUMMARY (
  PR_PK                    STRING,
  PR_ID                    NUMBER,
  REVIEW_COUNT             NUMBER,
  APPROVER_COUNT           NUMBER,
  FIRST_REVIEW_AT          TIMESTAMP_NTZ,
  FIRST_APPROVAL_AT        TIMESTAMP_NTZ,
  IS_SELF_MERGED           BOOLEAN,  -- Author merged without external approval
  TIME_TO_FIRST_REVIEW_SEC NUMBER,
  TIME_TO_FIRST_APPROVAL_SEC NUMBER,
  DATA_SYNCED_AT           TIMESTAMP_NTZ
);

MERGE INTO GITHUB_PIPELINES.REPORTING_TABLES.FACT_PR_REVIEW_SUMMARY AS TARGET
USING (

  WITH

  pr_authors AS (
    SELECT pr_id, user_ldap AS author_ldap, merged_at, created_at
    FROM GITHUB_PIPELINES.REPORTING_TABLES.FACT_PULL_REQUESTS
    WHERE merged_at IS NOT NULL
  ),

  review_users AS (
    SELECT r.id AS review_id, r.pull_request_id, r.state, r.submitted_at,
           gl.ldap AS reviewer_ldap
    FROM GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_REVIEW r
    JOIN GITHUB_PIPELINES.RAW_TABLES.USER u ON r.user_id = u.id
    LEFT JOIN GITHUB_PIPELINES.REPORTING_TABLES.BRIDGE_GITHUB_LDAP gl
      ON LOWER(u.login) = LOWER(gl.github)
  ),

  /*
    Exclude self-reviews: a reviewer whose LDAP matches the PR author LDAP.
    This is required for DORA "peer review coverage" metric.
  */
  external_reviews AS (
    SELECT rv.*
    FROM review_users rv
    JOIN pr_authors pa ON rv.pull_request_id = pa.pr_id
    WHERE COALESCE(rv.reviewer_ldap, 'unknown')
       <> COALESCE(pa.author_ldap,   'unknown')
  ),

  aggregated AS (
    SELECT
      fpr.pk                                           AS pr_pk,
      fpr.pr_id,
      COUNT(er.review_id)                              AS review_count,
      COUNT(DISTINCT CASE WHEN er.state = 'APPROVED' THEN er.reviewer_ldap END)
                                                       AS approver_count,
      MIN(er.submitted_at)                             AS first_review_at,
      MIN(CASE WHEN er.state = 'APPROVED' THEN er.submitted_at END)
                                                       AS first_approval_at,
      IFF(COUNT(er.review_id) = 0, TRUE, FALSE)        AS is_self_merged,
      DATEDIFF('second', fpr.created_at, MIN(er.submitted_at))
                                                       AS time_to_first_review_sec,
      DATEDIFF('second', fpr.created_at,
        MIN(CASE WHEN er.state = 'APPROVED' THEN er.submitted_at END))
                                                       AS time_to_first_approval_sec,
      CURRENT_TIMESTAMP()::TIMESTAMP_NTZ               AS data_synced_at
    FROM GITHUB_PIPELINES.REPORTING_TABLES.FACT_PULL_REQUESTS fpr
    LEFT JOIN external_reviews er ON fpr.pr_id = er.pull_request_id
    WHERE fpr.merged = TRUE
    GROUP BY fpr.pk, fpr.pr_id, fpr.created_at
  )

  SELECT * FROM aggregated

) AS SOURCE
ON TARGET.PR_PK = SOURCE.PR_PK

WHEN MATCHED THEN
  UPDATE SET
    REVIEW_COUNT                = SOURCE.REVIEW_COUNT,
    APPROVER_COUNT              = SOURCE.APPROVER_COUNT,
    FIRST_REVIEW_AT             = SOURCE.FIRST_REVIEW_AT,
    FIRST_APPROVAL_AT           = SOURCE.FIRST_APPROVAL_AT,
    IS_SELF_MERGED              = SOURCE.IS_SELF_MERGED,
    TIME_TO_FIRST_REVIEW_SEC    = SOURCE.TIME_TO_FIRST_REVIEW_SEC,
    TIME_TO_FIRST_APPROVAL_SEC  = SOURCE.TIME_TO_FIRST_APPROVAL_SEC,
    DATA_SYNCED_AT              = SOURCE.DATA_SYNCED_AT

WHEN NOT MATCHED THEN
  INSERT (
    PR_PK, PR_ID,
    REVIEW_COUNT, APPROVER_COUNT,
    FIRST_REVIEW_AT, FIRST_APPROVAL_AT,
    IS_SELF_MERGED,
    TIME_TO_FIRST_REVIEW_SEC, TIME_TO_FIRST_APPROVAL_SEC,
    DATA_SYNCED_AT
  )
  VALUES (
    SOURCE.PR_PK, SOURCE.PR_ID,
    SOURCE.REVIEW_COUNT, SOURCE.APPROVER_COUNT,
    SOURCE.FIRST_REVIEW_AT, SOURCE.FIRST_APPROVAL_AT,
    SOURCE.IS_SELF_MERGED,
    SOURCE.TIME_TO_FIRST_REVIEW_SEC, SOURCE.TIME_TO_FIRST_APPROVAL_SEC,
    SOURCE.DATA_SYNCED_AT
  );
