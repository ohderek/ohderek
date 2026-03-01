"""
snowflake_loader.py
───────────────────────────────────────────────────────────────────────────────
Loads Parquet files from a local buffer into Snowflake using the
stage-and-merge pattern:

  1. PUT Parquet bytes to a named internal stage (@crypto_stage)
  2. COPY INTO a staging table from the stage
  3. MERGE from staging table into target table (idempotent upsert)
  4. TRUNCATE the staging table (keep it clean for next run)

Why stage-and-merge over write_pandas?
  - COPY INTO is the fastest Snowflake ingest path — parallelised server-side
  - MERGE makes the load idempotent: re-running won't create duplicates
  - Staging table acts as a natural checkpoint for debugging load failures
"""

from __future__ import annotations

import io
import logging
import os
import tempfile

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """
    Loads Parquet bytes into a Snowflake table via PUT → COPY INTO → MERGE.

    Connection credentials are read from environment variables:
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
        SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
    """

    def __init__(self) -> None:
        self._conn = snowflake.connector.connect(
            account          = os.environ["SNOWFLAKE_ACCOUNT"],
            user             = os.environ["SNOWFLAKE_USER"],
            private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            role             = os.environ.get("SNOWFLAKE_ROLE",      "TRANSFORMER"),
            warehouse        = os.environ.get("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH"),
            database         = os.environ.get("SNOWFLAKE_DATABASE",  "CRYPTO"),
            schema           = os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
            session_parameters={"QUERY_TAG": "crypto-market-data-etl"},
        )

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "SnowflakeLoader":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load_listings(self, parquet_bytes: bytes, run_date: str) -> dict[str, int]:
        """
        Upsert a batch of coin listing snapshots.

        Merge key: (cmc_id, fetched_at) — allows multiple snapshots per day
        while preventing exact duplicates on re-runs.
        """
        return self._stage_and_merge(
            parquet_bytes = parquet_bytes,
            stage_file    = f"listings_{run_date}.parquet",
            stage_table   = "CMC_LISTINGS_STAGE",
            target_table  = "CMC_LISTINGS",
            merge_keys    = ["id", "fetched_at"],
            update_cols   = [
                "cmc_rank", "price_usd", "volume_24h_usd", "volume_change_24h_pct",
                "market_cap_usd", "market_cap_dominance", "fully_diluted_market_cap",
                "pct_change_1h", "pct_change_24h", "pct_change_7d", "pct_change_30d",
                "circulating_supply", "total_supply",
            ],
        )

    def load_global_metrics(self, parquet_bytes: bytes, run_date: str) -> dict[str, int]:
        """Upsert the global market metrics snapshot."""
        return self._stage_and_merge(
            parquet_bytes = parquet_bytes,
            stage_file    = f"global_metrics_{run_date}.parquet",
            stage_table   = "CMC_GLOBAL_METRICS_STAGE",
            target_table  = "CMC_GLOBAL_METRICS",
            merge_keys    = ["fetched_at"],
            update_cols   = [
                "total_market_cap_usd", "total_volume_24h_usd",
                "total_volume_24h_reported", "btc_dominance", "eth_dominance",
                "active_cryptocurrencies", "active_exchanges", "active_market_pairs",
            ],
        )

    # ------------------------------------------------------------------
    # Core stage-and-merge implementation
    # ------------------------------------------------------------------

    def _stage_and_merge(
        self,
        parquet_bytes: bytes,
        stage_file:    str,
        stage_table:   str,
        target_table:  str,
        merge_keys:    list[str],
        update_cols:   list[str],
    ) -> dict[str, int]:

        cur = self._conn.cursor(DictCursor)

        try:
            # 1. Write Parquet bytes to a temp file and PUT to Snowflake stage
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp.write(parquet_bytes)
                tmp_path = tmp.name

            logger.info("PUT %s → @crypto_stage/%s", tmp_path, stage_file)
            cur.execute(
                f"PUT file://{tmp_path} @crypto_stage/{stage_file} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )

            # 2. COPY into staging table
            logger.info("COPY INTO %s", stage_table)
            cur.execute(f"""
                COPY INTO {stage_table}
                FROM @crypto_stage/{stage_file}
                FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """)

            # 3. MERGE into target
            on_clause      = " AND ".join(f"tgt.{k} = src.{k}" for k in merge_keys)
            update_clause  = ", ".join(f"tgt.{c} = src.{c}" for c in update_cols)

            result = cur.execute(f"""
                MERGE INTO {target_table} AS tgt
                USING {stage_table}       AS src
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT SELECT *
            """).fetchone()

            rows_inserted = result.get("number of rows inserted", 0) if result else 0
            rows_updated  = result.get("number of rows updated",  0) if result else 0

            # 4. Truncate staging table for next run
            cur.execute(f"TRUNCATE TABLE {stage_table}")

            metrics = {"rows_inserted": rows_inserted, "rows_updated": rows_updated}
            logger.info("Loaded %s → %s", target_table, metrics)
            return metrics

        finally:
            cur.close()
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
