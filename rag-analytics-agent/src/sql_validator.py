"""
sql_validator.py — Safety checks before any SQL touches the database

─────────────────────────────────────────────────────────────────
WHY VALIDATE AI-GENERATED SQL?
─────────────────────────────────────────────────────────────────
LLMs are powerful but not infallible. Without validation, an agent
connected to a production database is a liability:

  - A prompt injection attack could ask "drop all tables" disguised
    as a business question
  - The model might generate a query without a LIMIT, scanning
    billions of rows and burning Snowflake credits
  - An ambiguous question might produce a DELETE or TRUNCATE

Validation is a hard guard — not a suggestion. If it fails,
the query never reaches the database.

─────────────────────────────────────────────────────────────────
THREE LAYERS OF PROTECTION
─────────────────────────────────────────────────────────────────
  1. ALLOWLIST (positive check) — query must start with SELECT or WITH
  2. BLOCKLIST (negative check) — reject any dangerous keyword
  3. LIMIT INJECTION          — add LIMIT if the model forgot it

Why both allowlist AND blocklist?
  - Allowlist alone could be bypassed by a cleverly nested subquery
  - Blocklist alone might miss novel attack patterns
  - Together they cover known and unknown attack surfaces
"""

import re


class SQLValidationError(Exception):
    """
    Raised when a generated SQL query fails a safety check.

    This exception is caught in api/main.py and returned as a
    422 Unprocessable Entity response — the database is never touched.
    """
    pass


# Keywords that are never acceptable in an AI-generated query.
# Using a set for O(1) lookup.
BLOCKED_KEYWORDS: set[str] = {
    "DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT",
    "MERGE", "ALTER", "CREATE", "GRANT", "REVOKE",
    "EXECUTE", "EXEC", "CALL",           # procedural execution
    "PUT", "GET", "REMOVE",              # Snowflake file operations
    "COPY INTO",                         # Snowflake bulk load
    "SYSTEM$",                           # Snowflake system functions
}


def validate_sql(sql: str, max_rows: int = 500) -> str:
    """
    Validate and sanitise a generated SQL query.

    This function is the last line of defence before execution.
    It never modifies the intent of the query — only enforces
    READ-ONLY access and adds a LIMIT if one is missing.

    Args:
        sql:      The raw SQL string from the LLM.
        max_rows: Maximum rows to return — injected as LIMIT if absent.

    Returns:
        The (possibly LIMIT-appended) SQL string, safe to execute.

    Raises:
        SQLValidationError: If the query violates any safety check.
    """
    if not sql or not sql.strip():
        raise SQLValidationError("Empty SQL generated — the model returned no output.")

    # The LLM signals "I don't know how to answer this" with this sentinel.
    # Treat it as a user-facing validation failure, not a server error.
    if sql.strip().upper() == "CANNOT_ANSWER":
        raise SQLValidationError(
            "The agent could not generate SQL for this question. "
            "Try rephrasing, or check that the relevant table is in the schema."
        )

    # Normalise whitespace — makes regex matching reliable
    normalised = " ".join(sql.split())

    # ── Check 1: Allowlist — must be a SELECT or WITH (CTE) statement ──────
    # Strip leading whitespace and check the first keyword
    first_keyword = normalised.lstrip().split()[0].upper()
    if first_keyword not in ("SELECT", "WITH"):
        raise SQLValidationError(
            f"Only SELECT queries are permitted. "
            f"The model generated a query starting with: {first_keyword}"
        )

    # ── Check 2: Blocklist — no dangerous keywords anywhere in the query ───
    # Word-boundary regex prevents false positives:
    #   "DELETED_AT" column should not trigger "DELETE"
    #   "CREATE_DATE" column should not trigger "CREATE"
    for keyword in BLOCKED_KEYWORDS:
        # Handle multi-word keywords like "COPY INTO" and "SYSTEM$"
        pattern = rf"\b{re.escape(keyword)}\b"
        if re.search(pattern, normalised.upper()):
            raise SQLValidationError(
                f"Blocked keyword '{keyword}' found in generated SQL. "
                f"Query rejected for safety."
            )

    # ── Check 3: LIMIT injection ────────────────────────────────────────────
    # If the model forgot to add a LIMIT (common for aggregation queries
    # that return few rows), inject one. This caps Snowflake credit spend
    # and prevents accidentally returning multi-million row result sets.
    if "LIMIT" not in normalised.upper():
        normalised = normalised.rstrip(";").rstrip()
        normalised = f"{normalised} LIMIT {max_rows}"

    return normalised
