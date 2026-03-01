/*
  create_tables.sql
  ─────────────────────────────────────────────────────────────────────────────
  DDL for the CoinMarketCap analytics schema in Snowflake.

  Schema:
    CRYPTO.RAW    — raw ingest tables + staging tables + internal stage
    CRYPTO.ANALYTICS — aggregated views for dashboards
  ─────────────────────────────────────────────────────────────────────────────
*/

USE DATABASE CRYPTO;


-- =============================================================================
-- Internal stage (Parquet files land here before COPY INTO)
-- =============================================================================

CREATE STAGE IF NOT EXISTS CRYPTO.RAW.CRYPTO_STAGE
  COMMENT = 'Internal stage for CoinMarketCap Parquet ingestion';


-- =============================================================================
-- CMC_LISTINGS  — one row per (coin, fetch snapshot)
-- =============================================================================

CREATE TABLE IF NOT EXISTS CRYPTO.RAW.CMC_LISTINGS (
  ID                        NUMBER        NOT NULL,
  NAME                      STRING,
  SYMBOL                    STRING,
  SLUG                      STRING,
  CMC_RANK                  NUMBER,
  NUM_MARKET_PAIRS          NUMBER,
  CIRCULATING_SUPPLY        FLOAT,
  TOTAL_SUPPLY              FLOAT,
  MAX_SUPPLY                FLOAT,
  INFINITE_SUPPLY           BOOLEAN,
  PRICE_USD                 FLOAT,
  VOLUME_24H_USD            FLOAT,
  VOLUME_CHANGE_24H_PCT     FLOAT,
  MARKET_CAP_USD            FLOAT,
  MARKET_CAP_DOMINANCE      FLOAT,
  FULLY_DILUTED_MARKET_CAP  FLOAT,
  PCT_CHANGE_1H             FLOAT,
  PCT_CHANGE_24H            FLOAT,
  PCT_CHANGE_7D             FLOAT,
  PCT_CHANGE_30D            FLOAT,
  LAST_UPDATED              TIMESTAMP_NTZ,
  FETCHED_AT                TIMESTAMP_NTZ NOT NULL
)
CLUSTER BY (DATE(FETCHED_AT))
COMMENT = 'CoinMarketCap top-N coin listings snapshot. One row per coin per fetch run.';

-- Staging table (truncated after each merge)
CREATE TABLE IF NOT EXISTS CRYPTO.RAW.CMC_LISTINGS_STAGE
  LIKE CRYPTO.RAW.CMC_LISTINGS;


-- =============================================================================
-- CMC_GLOBAL_METRICS  — one row per fetch snapshot
-- =============================================================================

CREATE TABLE IF NOT EXISTS CRYPTO.RAW.CMC_GLOBAL_METRICS (
  TOTAL_MARKET_CAP_USD        FLOAT,
  TOTAL_VOLUME_24H_USD        FLOAT,
  TOTAL_VOLUME_24H_REPORTED   FLOAT,
  BTC_DOMINANCE               FLOAT,
  ETH_DOMINANCE               FLOAT,
  ACTIVE_CRYPTOCURRENCIES     NUMBER,
  ACTIVE_EXCHANGES            NUMBER,
  ACTIVE_MARKET_PAIRS         NUMBER,
  LAST_UPDATED                TIMESTAMP_NTZ,
  FETCHED_AT                  TIMESTAMP_NTZ NOT NULL
)
CLUSTER BY (DATE(FETCHED_AT))
COMMENT = 'CoinMarketCap global market metrics snapshot. One row per fetch run.';

CREATE TABLE IF NOT EXISTS CRYPTO.RAW.CMC_GLOBAL_METRICS_STAGE
  LIKE CRYPTO.RAW.CMC_GLOBAL_METRICS;


-- =============================================================================
-- ANALYTICS layer — pre-aggregated views for dashboards
-- =============================================================================

-- Daily closing snapshot: latest fetch per coin per calendar day
CREATE OR REPLACE VIEW CRYPTO.ANALYTICS.DAILY_COIN_PRICES AS

SELECT
  DATE(FETCHED_AT)          AS price_date,
  ID                        AS cmc_id,
  NAME,
  SYMBOL,
  CMC_RANK                  AS rank_at_close,
  PRICE_USD,
  VOLUME_24H_USD,
  MARKET_CAP_USD,
  MARKET_CAP_DOMINANCE,
  PCT_CHANGE_24H,
  PCT_CHANGE_7D,
  PCT_CHANGE_30D,
  CIRCULATING_SUPPLY,
  FETCHED_AT

FROM CRYPTO.RAW.CMC_LISTINGS

QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY DATE(FETCHED_AT), ID
    ORDER BY FETCHED_AT DESC
  ) = 1;


-- Top 10 coins by market cap at latest fetch
CREATE OR REPLACE VIEW CRYPTO.ANALYTICS.CURRENT_TOP_10 AS

SELECT
  CMC_RANK        AS rank,
  SYMBOL,
  NAME,
  PRICE_USD,
  MARKET_CAP_USD,
  VOLUME_24H_USD,
  PCT_CHANGE_24H  AS change_24h_pct,
  PCT_CHANGE_7D   AS change_7d_pct,
  FETCHED_AT      AS as_of

FROM CRYPTO.RAW.CMC_LISTINGS

WHERE FETCHED_AT = (SELECT MAX(FETCHED_AT) FROM CRYPTO.RAW.CMC_LISTINGS)
  AND CMC_RANK  <= 10

ORDER BY CMC_RANK;


-- 7-day price history for any coin (filter by symbol in query)
CREATE OR REPLACE VIEW CRYPTO.ANALYTICS.PRICE_HISTORY_7D AS

SELECT
  DATE(FETCHED_AT)  AS price_date,
  SYMBOL,
  NAME,
  AVG(PRICE_USD)    AS avg_price_usd,
  MAX(PRICE_USD)    AS high_price_usd,
  MIN(PRICE_USD)    AS low_price_usd,
  MAX(VOLUME_24H_USD) AS max_volume_usd,
  COUNT(*)          AS snapshot_count

FROM CRYPTO.RAW.CMC_LISTINGS

WHERE FETCHED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())

GROUP BY 1, 2, 3
ORDER BY SYMBOL, price_date;


-- Bitcoin dominance trend (from global metrics)
CREATE OR REPLACE VIEW CRYPTO.ANALYTICS.BTC_DOMINANCE_TREND AS

SELECT
  DATE(FETCHED_AT)        AS metric_date,
  AVG(BTC_DOMINANCE)      AS avg_btc_dominance_pct,
  AVG(ETH_DOMINANCE)      AS avg_eth_dominance_pct,
  AVG(TOTAL_MARKET_CAP_USD) / 1e12 AS avg_total_mcap_trillion_usd,
  AVG(TOTAL_VOLUME_24H_USD)  / 1e9  AS avg_24h_volume_billion_usd,
  MAX(ACTIVE_CRYPTOCURRENCIES) AS active_coins

FROM CRYPTO.RAW.CMC_GLOBAL_METRICS

GROUP BY 1
ORDER BY 1 DESC;
