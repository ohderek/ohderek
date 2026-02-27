"""
Snowflake loader — bulk COPY INTO staging, then MERGE into target.

Strategy:
  1. Write DataFrame to a local Parquet file (columnar, compressed)
  2. PUT file to Snowflake internal stage
  3. COPY INTO a staging table
  4. MERGE staging → target using configurable merge keys
  5. Truncate staging table

This pattern keeps the target table always queryable and supports
incremental loads without duplicates.
"""

import logging
import os
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeLoaderConfig:
    database: str
    schema: str
    target_table: str
    merge_keys: list[str]                    # Columns that uniquely identify a record
    stage_table_suffix: str = "_stage"
    warehouse: Optional[str] = None
    role: Optional[str] = None
    update_columns: list[str] = field(default_factory=list)  # Columns to update on match
    # Snowflake account / user / password read from env — not stored here


class SnowflakeLoader:
    """
    Loads a DataFrame into Snowflake using a stage-and-merge pattern.

    Credentials are sourced exclusively from environment variables:
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or key pair)
    """

    def __init__(self, config: SnowflakeLoaderConfig):
        self.config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=self.config.database,
            schema=self.config.schema,
            warehouse=self.config.warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
            role=self.config.role or os.environ.get("SNOWFLAKE_ROLE"),
            session_parameters={"QUERY_TAG": f"etl_loader:{self.config.target_table}"},
        )
        logger.info(
            "Snowflake connection opened",
            extra={
                "database": self.config.database,
                "schema": self.config.schema,
                "table": self.config.target_table,
            },
        )

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, df: pd.DataFrame) -> dict:
        """
        Load DataFrame into Snowflake.

        Returns:
            dict with rows_inserted, rows_updated, rows_staged
        """
        if df.empty:
            logger.info("Empty DataFrame — skipping load")
            return {"rows_inserted": 0, "rows_updated": 0, "rows_staged": 0}

        stage_table = f"{self.config.target_table}{self.config.stage_table_suffix}"

        self._write_to_stage(df, stage_table)
        metrics = self._merge_stage_to_target(stage_table)
        self._truncate_stage(stage_table)

        logger.info(
            "Load complete",
            extra={
                "table": self.config.target_table,
                **metrics,
            },
        )
        return metrics

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_to_stage(self, df: pd.DataFrame, stage_table: str) -> None:
        """Write DataFrame to the staging table via write_pandas (Parquet internally)."""
        success, nchunks, nrows, _ = write_pandas(
            conn=self._conn,
            df=df,
            table_name=stage_table.upper(),
            database=self.config.database,
            schema=self.config.schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed for staging table {stage_table}")

        logger.debug(
            "Staged rows",
            extra={"stage_table": stage_table, "rows": nrows, "chunks": nchunks},
        )

    def _merge_stage_to_target(self, stage_table: str) -> dict:
        """Execute MERGE from staging into the target table."""
        target = f"{self.config.database}.{self.config.schema}.{self.config.target_table}"
        stage = f"{self.config.database}.{self.config.schema}.{stage_table}"

        join_clause = " AND ".join(
            f"tgt.{k} = src.{k}" for k in self.config.merge_keys
        )

        update_cols = self.config.update_columns
        update_clause = ", ".join(f"tgt.{c} = src.{c}" for c in update_cols) if update_cols else ""

        merge_sql = f"""
            MERGE INTO {target} AS tgt
            USING {stage} AS src
            ON {join_clause}
            WHEN MATCHED {"AND (" + " OR ".join(f"tgt.{c} <> src.{c}" for c in update_cols) + ")" if update_cols else ""}
                THEN UPDATE SET {update_clause if update_clause else "tgt._loaded_at = src._loaded_at"}
            WHEN NOT MATCHED
                THEN INSERT ({", ".join(f"src.{c}" for c in self._get_stage_columns(stage_table))})
                     VALUES ({", ".join(f"src.{c}" for c in self._get_stage_columns(stage_table))})
        """

        cursor = self._conn.cursor()
        cursor.execute(merge_sql)
        row = cursor.fetchone()
        cursor.close()

        return {
            "rows_inserted": row[0] if row else 0,
            "rows_updated": row[1] if row else 0,
            "rows_staged": 0,
        }

    def _truncate_stage(self, stage_table: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"TRUNCATE TABLE IF EXISTS "
            f"{self.config.database}.{self.config.schema}.{stage_table}"
        )
        cursor.close()

    def _get_stage_columns(self, stage_table: str) -> list[str]:
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{self.config.schema.upper()}' "
            f"AND table_name = '{stage_table.upper()}' "
            f"ORDER BY ordinal_position"
        )
        cols = [row[0].lower() for row in cursor.fetchall()]
        cursor.close()
        return cols
