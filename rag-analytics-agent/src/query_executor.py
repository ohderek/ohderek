"""
query_executor.py — Execute validated SQL against the data warehouse

─────────────────────────────────────────────────────────────────
TWO EXECUTION MODES
─────────────────────────────────────────────────────────────────
This module routes queries to one of two backends:

  MOCK MODE  (USE_MOCK_DB=true, the default)
  ─────────────────────────────────────────
  Uses DuckDB — an in-process analytical database.
  No server, no credentials, no cloud account needed.
  The mock_warehouse.duckdb file is created by seed/mock_data.py
  and contains ~12,500 rows of realistic fake e-commerce + ops data.

  WHY DUCKDB for mock? It's not just a toy:
    - Full SQL support: window functions, CTEs, LATERAL, date arithmetic
    - Columnar storage — same performance characteristics as Snowflake
    - Used in production at many companies (MotherDuck, dbt-duckdb adapter)
    - The SQL generated for DuckDB runs on Snowflake with minor changes

  PRODUCTION MODE  (USE_MOCK_DB=false)
  ─────────────────────────────────────
  Uses Snowflake via snowflake-connector-python.
  Requires credentials in .env (see .env.example).
  For key-pair auth (recommended over password), see CREDENTIALS.md.

─────────────────────────────────────────────────────────────────
INTERFACE CONTRACT
─────────────────────────────────────────────────────────────────
Both modes return List[Dict[str, Any]] — a list of result rows where
each row is a dict mapping column name → value.

This uniform interface means sql_agent.py and api/main.py don't
know or care which backend ran the query.
"""

from typing import Any

from src.config import (
    MAX_ROWS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
    USE_MOCK_DB,
)

# Path to the DuckDB file created by seed/mock_data.py
DUCKDB_PATH = "./mock_warehouse.duckdb"


def _execute_duckdb(sql: str) -> list[dict[str, Any]]:
    """
    Execute SQL against the local DuckDB database.

    Opens a read-only connection so even if validation somehow failed,
    DuckDB will refuse write operations.
    """
    import duckdb

    # read_only=True is a belt-and-suspenders guard — the validator already
    # blocked writes, but this makes it impossible at the driver level too.
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows    = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def _execute_snowflake(sql: str) -> list[dict[str, Any]]:
    """
    Execute SQL against Snowflake.

    DictCursor returns rows as dicts automatically, matching the
    interface of the DuckDB implementation.

    For production: replace password auth with key-pair.
    See CREDENTIALS.md for the private_key_file pattern.
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(sql)
        return cursor.fetchmany(MAX_ROWS)
    finally:
        conn.close()


def execute_query(sql: str) -> list[dict[str, Any]]:
    """
    Execute a validated SQL query and return results as a list of dicts.

    This is the only public function in this module. All internal
    routing between DuckDB and Snowflake is handled here.

    Args:
        sql: A validated, safe SELECT query from sql_validator.py.

    Returns:
        List of result rows. Empty list if the query returns no rows.

    Raises:
        RuntimeError: Wraps any database connection or execution error
                      with a clear message for the API response.
    """
    backend = "DuckDB (mock)" if USE_MOCK_DB else "Snowflake"
    try:
        if USE_MOCK_DB:
            return _execute_duckdb(sql)
        else:
            return _execute_snowflake(sql)
    except Exception as e:
        raise RuntimeError(
            f"Query execution failed on {backend}. "
            f"Check that the database is seeded (make seed) and the SQL is valid. "
            f"Error: {e}"
        ) from e
