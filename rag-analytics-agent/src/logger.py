"""
logger.py — Observability for every agent interaction

─────────────────────────────────────────────────────────────────
WHY LOG EVERY QUERY?
─────────────────────────────────────────────────────────────────
Without logging, an AI agent is a black box. You cannot:
  - Debug why a question produced a wrong answer
  - Track which questions fail most often (→ improve the schema metadata)
  - Measure latency and identify slow queries
  - Calculate cost (token usage × price per token)
  - Build a feedback loop to improve the agent over time

─────────────────────────────────────────────────────────────────
WHERE LOGS GO
─────────────────────────────────────────────────────────────────
  1. Console  — printed immediately, visible in the terminal
  2. Database — written to agent_query_log table in DuckDB or Snowflake

Storing logs in the same database as your data means you can query
agent performance with the same SQL tools you use for everything else:

  SELECT
      DATE_TRUNC('day', created_at)  AS day,
      COUNT(*)                        AS total_queries,
      COUNT(*) FILTER (WHERE error IS NOT NULL) AS failed,
      AVG(latency_ms)                 AS avg_latency_ms,
      AVG(row_count)                  AS avg_rows_returned
  FROM agent_query_log
  GROUP BY 1
  ORDER BY 1 DESC;

─────────────────────────────────────────────────────────────────
THE QueryLog DATACLASS
─────────────────────────────────────────────────────────────────
Python dataclasses are like lightweight structs — they define a
typed container without boilerplate. @dataclass auto-generates
__init__, __repr__, and __eq__ based on the field annotations.

The field(default_factory=...) syntax generates a fresh value for
each instance — crucial for mutable defaults like lists and UUIDs.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.config import USE_MOCK_DB

DUCKDB_PATH = "./mock_warehouse.duckdb"

# DDL that creates the log table if it doesn't exist.
# CREATE TABLE IF NOT EXISTS is safe to run repeatedly — idempotent.
_CREATE_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agent_query_log (
    run_id        VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP,
    question      VARCHAR,
    generated_sql VARCHAR,
    answer        VARCHAR,
    row_count     INTEGER,
    latency_ms    DOUBLE,
    tables_used   VARCHAR,   -- JSON array: ["fct_finance__revenue"]
    error         VARCHAR    -- NULL on success
)
"""


@dataclass
class QueryLog:
    """Captures one complete agent interaction from question to answer."""

    question:            str
    generated_sql:       str
    answer:              str
    row_count:           int
    latency_ms:          float
    schema_tables_used:  list[str]      = field(default_factory=list)
    error:               str | None     = None

    # Auto-generated fields — a new UUID and timestamp for each instance
    run_id:     str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def _get_duckdb_conn():
    """Open a read-write DuckDB connection for log writes."""
    import duckdb
    # Note: NOT read_only — we need to write to the log table
    return duckdb.connect(DUCKDB_PATH)


def _get_snowflake_conn():
    """Open a Snowflake connection for log writes."""
    import snowflake.connector
    from src.config import (
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ROLE, SNOWFLAKE_SCHEMA, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE,
    )
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD, database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE,
    )


def _ensure_log_table():
    """Create agent_query_log if it doesn't exist."""
    conn = _get_duckdb_conn() if USE_MOCK_DB else _get_snowflake_conn()
    try:
        conn.execute(_CREATE_LOG_TABLE_SQL)
    finally:
        conn.close()


def write_log(log: QueryLog) -> None:
    """
    Write a completed QueryLog to the database and print a summary.

    Logging is best-effort — if the log write fails (e.g. disk full,
    Snowflake timeout), we print a warning but do NOT raise an exception.
    A logging failure must never crash the main application.

    Args:
        log: A completed QueryLog instance with all fields populated.
    """
    # ── Console output ───────────────────────────────────────────────────────
    status = "ERROR" if log.error else " OK  "
    print(
        f"[agent] {status} | {log.latency_ms:6.0f}ms | "
        f"{log.row_count:4d} rows | "
        f"tables={log.schema_tables_used} | "
        f'q="{log.question[:55]}..."'
    )

    # ── Database write ────────────────────────────────────────────────────────
    try:
        _ensure_log_table()

        insert_sql = """
        INSERT INTO agent_query_log
            (run_id, created_at, question, generated_sql, answer,
             row_count, latency_ms, tables_used, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params: tuple[Any, ...] = (
            log.run_id,
            log.created_at,
            log.question,
            log.generated_sql,
            log.answer,
            log.row_count,
            log.latency_ms,
            json.dumps(log.schema_tables_used),
            log.error,
        )

        conn = _get_duckdb_conn() if USE_MOCK_DB else _get_snowflake_conn()
        try:
            conn.execute(insert_sql, list(params))
        finally:
            conn.close()

    except Exception as exc:
        # Log write failure — warn but do not propagate
        print(f"[agent] WARNING: failed to write log entry: {exc}")
