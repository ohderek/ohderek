/*
  raw_data_migration.sql
  ─────────────────────────────────────────────────────────────────────────────
  Hypothetical Showcase: Raw SQL ETL jobs for the GitHub Insights platform.

  Purpose : Consolidate GitHub pull request, review, commit, and repository
            data from multiple Fivetran connectors (multi-org) into unified
            RAW layer tables in Snowflake.

  Pattern : UNION BY NAME across connectors → QUALIFY ROW_NUMBER() dedup →
            MERGE INTO target (idempotent, re-runnable).

  All org names, connector IDs, and internal schema names are generic.
  No proprietary data is included.
  ─────────────────────────────────────────────────────────────────────────────
*/


-- =============================================================================
-- PULL_REQUESTS  (multi-org consolidation)
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST (
  PK                STRING       NOT NULL,
  ID                NUMBER       NOT NULL,
  NUMBER            NUMBER,
  TITLE             STRING,
  STATE             STRING,
  DRAFT             BOOLEAN,
  CREATED_AT        TIMESTAMP_TZ,
  UPDATED_AT        TIMESTAMP_TZ,
  CLOSED_AT         TIMESTAMP_TZ,
  MERGED_AT         TIMESTAMP_TZ,
  MERGE_COMMIT_SHA  STRING,
  HEAD_REF          STRING,
  HEAD_SHA          STRING,
  HEAD_REPO_ID      NUMBER,
  BASE_REF          STRING,
  BASE_SHA          STRING,
  BASE_REPO_ID      NUMBER,
  USER_ID           NUMBER,
  REPOSITORY_ID     NUMBER,
  _FIVETRAN_SYNCED  TIMESTAMP_TZ,
  CONNECTOR         STRING
);

MERGE INTO GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST AS t
USING (

  WITH combined AS (
    -- Org A sharded connectors (large org, 5 Fivetran connections)
    SELECT pr.*, 'org_a_1' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_1.PULL_REQUEST pr
    UNION BY NAME
    SELECT pr.*, 'org_a_2' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_2.PULL_REQUEST pr
    UNION BY NAME
    SELECT pr.*, 'org_a_3' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_3.PULL_REQUEST pr
    UNION BY NAME
    SELECT pr.*, 'org_a_4' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_4.PULL_REQUEST pr
    UNION BY NAME
    SELECT pr.*, 'org_a_5' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_5.PULL_REQUEST pr

    -- Org B (smaller org, single connector)
    UNION BY NAME
    SELECT pr.*, 'org_b'   AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_B.PULL_REQUEST pr
  ),

  /*
    Dedup: a PR can appear in multiple shard connectors.
    Priority: most recently synced shard wins; connector order as tiebreak.
  */
  deduped AS (
    SELECT
      ID::STRING AS PK,
      ID, NUMBER, TITLE, STATE, DRAFT,
      CREATED_AT, UPDATED_AT, CLOSED_AT, MERGED_AT,
      MERGE_COMMIT_SHA, HEAD_REF, HEAD_SHA, HEAD_REPO_ID,
      BASE_REF, BASE_SHA, BASE_REPO_ID,
      USER_ID, REPOSITORY_ID, _FIVETRAN_SYNCED, CONNECTOR
    FROM combined
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY
          UPDATED_AT        DESC NULLS LAST,
          _FIVETRAN_SYNCED  DESC,
          -- Explicit connector priority (lower = preferred shard)
          DECODE(CONNECTOR,
            'org_a_1', 1, 'org_a_2', 2, 'org_a_3', 3,
            'org_a_4', 4, 'org_a_5', 5, 'org_b',   6, 99)
      ) = 1
  )

  SELECT * FROM deduped

) AS s
ON t.PK = s.PK

WHEN MATCHED
  AND COALESCE(s._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
      > COALESCE(t._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
THEN UPDATE SET
  t.NUMBER           = s.NUMBER,
  t.TITLE            = s.TITLE,
  t.STATE            = s.STATE,
  t.DRAFT            = s.DRAFT,
  t.CREATED_AT       = s.CREATED_AT,
  t.UPDATED_AT       = s.UPDATED_AT,
  t.CLOSED_AT        = s.CLOSED_AT,
  t.MERGED_AT        = s.MERGED_AT,
  t.MERGE_COMMIT_SHA = s.MERGE_COMMIT_SHA,
  t.HEAD_REF         = s.HEAD_REF,
  t.HEAD_SHA         = s.HEAD_SHA,
  t.HEAD_REPO_ID     = s.HEAD_REPO_ID,
  t.BASE_REF         = s.BASE_REF,
  t.BASE_SHA         = s.BASE_SHA,
  t.BASE_REPO_ID     = s.BASE_REPO_ID,
  t.USER_ID          = s.USER_ID,
  t.REPOSITORY_ID    = s.REPOSITORY_ID,
  t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED,
  t.CONNECTOR        = s.CONNECTOR

WHEN NOT MATCHED THEN INSERT (
  PK,
  ID, NUMBER, TITLE, STATE, DRAFT,
  CREATED_AT, UPDATED_AT, CLOSED_AT, MERGED_AT,
  MERGE_COMMIT_SHA, HEAD_REF, HEAD_SHA, HEAD_REPO_ID,
  BASE_REF, BASE_SHA, BASE_REPO_ID,
  USER_ID, REPOSITORY_ID, _FIVETRAN_SYNCED, CONNECTOR
)
VALUES (
  s.PK,
  s.ID, s.NUMBER, s.TITLE, s.STATE, s.DRAFT,
  s.CREATED_AT, s.UPDATED_AT, s.CLOSED_AT, s.MERGED_AT,
  s.MERGE_COMMIT_SHA, s.HEAD_REF, s.HEAD_SHA, s.HEAD_REPO_ID,
  s.BASE_REF, s.BASE_SHA, s.BASE_REPO_ID,
  s.USER_ID, s.REPOSITORY_ID, s._FIVETRAN_SYNCED, s.CONNECTOR
);


-- =============================================================================
-- PR_REVIEW  (multi-org consolidation)
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_REVIEW (
  PK               STRING NOT NULL,
  ID               NUMBER NOT NULL,
  PULL_REQUEST_ID  NUMBER,
  USER_ID          NUMBER,
  STATE            STRING,   -- APPROVED | CHANGES_REQUESTED | COMMENTED
  SUBMITTED_AT     TIMESTAMP_TZ,
  BODY             STRING,
  _FIVETRAN_SYNCED TIMESTAMP_TZ,
  CONNECTOR        STRING
);

MERGE INTO GITHUB_PIPELINES.RAW_TABLES.PULL_REQUEST_REVIEW AS t
USING (

  WITH combined AS (
    SELECT r.*, 'org_a_1' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_1.PULL_REQUEST_REVIEW r
    UNION BY NAME
    SELECT r.*, 'org_a_2' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_2.PULL_REQUEST_REVIEW r
    UNION BY NAME
    SELECT r.*, 'org_a_3' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_3.PULL_REQUEST_REVIEW r
    UNION BY NAME
    SELECT r.*, 'org_a_4' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_4.PULL_REQUEST_REVIEW r
    UNION BY NAME
    SELECT r.*, 'org_a_5' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_5.PULL_REQUEST_REVIEW r
    UNION BY NAME
    SELECT r.*, 'org_b'   AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_B.PULL_REQUEST_REVIEW r
  ),

  deduped AS (
    SELECT
      ID::STRING AS PK,
      ID, PULL_REQUEST_ID, USER_ID, STATE,
      SUBMITTED_AT, BODY, _FIVETRAN_SYNCED, CONNECTOR
    FROM combined
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY _FIVETRAN_SYNCED DESC NULLS LAST
      ) = 1
  )

  SELECT * FROM deduped

) AS s
ON t.PK = s.PK

WHEN MATCHED
  AND COALESCE(s._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
      > COALESCE(t._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
THEN UPDATE SET
  t.STATE            = s.STATE,
  t.SUBMITTED_AT     = s.SUBMITTED_AT,
  t.BODY             = s.BODY,
  t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED,
  t.CONNECTOR        = s.CONNECTOR

WHEN NOT MATCHED THEN INSERT (
  PK,
  ID, PULL_REQUEST_ID, USER_ID, STATE,
  SUBMITTED_AT, BODY, _FIVETRAN_SYNCED, CONNECTOR
)
VALUES (
  s.PK,
  s.ID, s.PULL_REQUEST_ID, s.USER_ID, s.STATE,
  s.SUBMITTED_AT, s.BODY, s._FIVETRAN_SYNCED, s.CONNECTOR
);


-- =============================================================================
-- COMMIT_FILE  (multi-org consolidation)
-- =============================================================================

CREATE TABLE IF NOT EXISTS GITHUB_PIPELINES.RAW_TABLES.COMMIT_FILE (
  PK               STRING NOT NULL,
  COMMIT_SHA       STRING NOT NULL,
  FILENAME         STRING,
  STATUS           STRING,   -- added | modified | removed | renamed
  ADDITIONS        NUMBER,
  DELETIONS        NUMBER,
  CHANGES          NUMBER,
  PATCH            STRING,
  _FIVETRAN_SYNCED TIMESTAMP_TZ,
  CONNECTOR        STRING
);

MERGE INTO GITHUB_PIPELINES.RAW_TABLES.COMMIT_FILE AS t
USING (

  WITH combined AS (
    SELECT cf.*, 'org_a_1' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_1.COMMIT_FILE cf
    UNION BY NAME
    SELECT cf.*, 'org_a_2' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_2.COMMIT_FILE cf
    UNION BY NAME
    SELECT cf.*, 'org_a_3' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_3.COMMIT_FILE cf
    UNION BY NAME
    SELECT cf.*, 'org_a_4' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_4.COMMIT_FILE cf
    UNION BY NAME
    SELECT cf.*, 'org_a_5' AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_A_5.COMMIT_FILE cf
    UNION BY NAME
    SELECT cf.*, 'org_b'   AS CONNECTOR FROM FIVETRAN.GITHUB_ORG_B.COMMIT_FILE cf
  ),

  deduped AS (
    SELECT
      MD5_HEX(CONCAT(COMMIT_SHA, '|', COALESCE(FILENAME, ''))) AS PK,
      COMMIT_SHA, FILENAME, STATUS,
      ADDITIONS, DELETIONS, CHANGES, PATCH,
      _FIVETRAN_SYNCED, CONNECTOR
    FROM combined
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY COMMIT_SHA, FILENAME
        ORDER BY _FIVETRAN_SYNCED DESC NULLS LAST
      ) = 1
  )

  SELECT * FROM deduped

) AS s
ON t.PK = s.PK

WHEN MATCHED
  AND COALESCE(s._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
      > COALESCE(t._FIVETRAN_SYNCED, '1900-01-01'::TIMESTAMP_TZ)
THEN UPDATE SET
  t.STATUS           = s.STATUS,
  t.ADDITIONS        = s.ADDITIONS,
  t.DELETIONS        = s.DELETIONS,
  t.CHANGES          = s.CHANGES,
  t.PATCH            = s.PATCH,
  t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED

WHEN NOT MATCHED THEN INSERT (
  PK,
  COMMIT_SHA, FILENAME, STATUS,
  ADDITIONS, DELETIONS, CHANGES, PATCH,
  _FIVETRAN_SYNCED, CONNECTOR
)
VALUES (
  s.PK,
  s.COMMIT_SHA, s.FILENAME, s.STATUS,
  s.ADDITIONS, s.DELETIONS, s.CHANGES, s.PATCH,
  s._FIVETRAN_SYNCED, s.CONNECTOR
);


-- =============================================================================
-- USER_LOOKUP  (GitHub login → internal LDAP identity mapping)
-- =============================================================================

DELETE FROM GITHUB_PIPELINES.RAW_TABLES.USER_LOOKUP
WHERE LDAP   IS NULL OR TRIM(LDAP)   = ''
   OR GITHUB IS NULL OR TRIM(GITHUB) = '';

MERGE INTO GITHUB_PIPELINES.RAW_TABLES.USER_LOOKUP AS TARGET
USING (

  WITH

  /*
    Source 1: identities cache — authoritative but includes deleted records
    and mixed-format LDAP values (full email vs. username only).
  */
  IDENTITIES AS (
    SELECT
      LOWER(SPLIT_PART(LDAP, '@', 1)) AS LDAP,
      LOWER(TRIM(GITHUB))             AS GITHUB
    FROM FIVETRAN.IDENTITY_SERVICE.IDENTITIES_CACHE
    WHERE COALESCE(_FIVETRAN_DELETED, FALSE) = FALSE
      AND LDAP   IS NOT NULL
      AND GITHUB IS NOT NULL
  ),

  /*
    Source 2: email-based extraction from org user lists.
    Corp email domain used to distinguish internal employees from external.
  */
  ORG_USER_LIST AS (
    SELECT DISTINCT
      LOWER(SPLIT_PART(ue.EMAIL, '@', 1)) AS LDAP,
      LOWER(TRIM(u.LOGIN))                AS GITHUB
    FROM GITHUB_PIPELINES.RAW_TABLES.USER u
    JOIN GITHUB_PIPELINES.RAW_TABLES.USER_EMAIL ue
      ON u.ID = ue.USER_ID
    WHERE SPLIT_PART(ue.EMAIL, '@', 2) IN ('corp.com', 'subsidiary.com')
  ),

  /*
    Source 3: manual mapping table — covers contractors, bots, and legacy accounts
    that aren't resolvable via the identity cache.
  */
  MANUAL_MAPPINGS AS (
    SELECT
      LOWER(SPLIT_PART(LDAP_USERNAME, '@', 1)) AS LDAP,
      LOWER(TRIM(GITHUB_USERNAME))             AS GITHUB
    FROM GITHUB_PIPELINES.RAW_TABLES.GITHUB_LDAP_MANUAL_MAPPINGS
  ),

  UNION_ALL AS (
    SELECT * FROM IDENTITIES
    UNION ALL SELECT * FROM ORG_USER_LIST
    UNION ALL SELECT * FROM MANUAL_MAPPINGS
  ),

  CLEAN AS (
    SELECT
      NULLIF(TRIM(LDAP),   '') AS LDAP,
      NULLIF(TRIM(GITHUB), '') AS GITHUB
    FROM UNION_ALL
  ),

  /*
    Dedup to 1 row per GITHUB login.
    Active employees (present in HR system) take priority over deactivated accounts.
  */
  DEDUPED AS (
    SELECT c.LDAP, c.GITHUB
    FROM CLEAN c
    LEFT JOIN HR_SYSTEM.PEOPLE.WORKERS w
      ON c.LDAP = LOWER(SPLIT_PART(w.EMAIL, '@', 1))
    WHERE c.LDAP IS NOT NULL AND c.GITHUB IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY c.GITHUB
        ORDER BY
          IFF(w.EMAIL IS NOT NULL, 0, 1),  -- Active employee wins
          c.LDAP
      ) = 1
  )

  SELECT LDAP, GITHUB FROM DEDUPED

) AS SOURCE
ON TARGET.GITHUB = SOURCE.GITHUB

WHEN MATCHED AND TARGET.LDAP <> SOURCE.LDAP THEN
  UPDATE SET TARGET.LDAP = SOURCE.LDAP

WHEN NOT MATCHED THEN
  INSERT (LDAP, GITHUB) VALUES (SOURCE.LDAP, SOURCE.GITHUB);
