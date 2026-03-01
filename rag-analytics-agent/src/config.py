"""
config.py — Central configuration loader

All secrets come from environment variables. Never hardcoded.
The dotenv library reads these from a local .env file during development.
In production (GitHub Actions, cloud), they're injected by the platform.

Why environment variables?
  - The .env file is in .gitignore, so secrets never reach version control
  - The same code runs locally and in production — only the env changes
  - If a secret rotates, you change the env var, not the code
"""

import os
from dotenv import load_dotenv

load_dotenv()  # reads .env if present; silently skips if absent

# ---------------------------------------------------------------------------
# LLM (Large Language Model)
# ---------------------------------------------------------------------------
# We use OpenAI for two separate things:
#   1. EMBEDDINGS — converting text (schema descriptions, user questions) into
#      vectors (lists of numbers) so we can compare them mathematically
#   2. CHAT — generating SQL queries and formatting answers in plain English
#
# os.environ["KEY"] (no default) raises an error immediately on startup
# if the key is missing — "fail fast" pattern. Better than a cryptic error
# appearing mid-request.
OPENAI_API_KEY    = os.environ["OPENAI_API_KEY"]
EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")  # cheap, fast, accurate
CHAT_MODEL        = os.getenv("CHAT_MODEL",       "gpt-4o")                  # smart enough for complex SQL

# ---------------------------------------------------------------------------
# Vector Store (ChromaDB)
# ---------------------------------------------------------------------------
# ChromaDB is a database purpose-built for storing and searching embeddings.
# It runs locally on disk — no Docker, no external service, no API key.
# The index is built once (from warehouse_schema.json) and then reused.
CHROMA_PERSIST_DIR  = os.getenv("CHROMA_PERSIST_DIR",  "./chroma_db")
CHROMA_COLLECTION   = os.getenv("CHROMA_COLLECTION",   "warehouse_schema")

# ---------------------------------------------------------------------------
# Data Warehouse
# ---------------------------------------------------------------------------
# USE_MOCK_DB=true  → DuckDB (local in-process database, no credentials needed)
#                     Great for demos, CI, and portfolios
# USE_MOCK_DB=false → Real Snowflake account
#
# Both modes use identical SQL — DuckDB is intentionally SQL-compatible.
USE_MOCK_DB = os.getenv("USE_MOCK_DB", "true").lower() == "true"

# Snowflake credentials — only read if USE_MOCK_DB=false
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",   "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER",      "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD",  "")   # prefer key-pair in production
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "ANALYTICS")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "MARTS")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ADHOC__MEDIUM")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "ANALYST")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# Path to the JSON file that describes your data warehouse tables and columns.
# This is the knowledge base the agent retrieves from.
# Think of it as a lightweight dbt manifest — same concept, simpler format.
SCHEMA_FILE = os.getenv("SCHEMA_FILE", "./schemas/warehouse_schema.json")

# ---------------------------------------------------------------------------
# Safety Limits
# ---------------------------------------------------------------------------
# These caps are applied to every query before execution.
# They exist to prevent runaway costs (Snowflake credits) and token bills.
MAX_ROWS             = int(os.getenv("MAX_ROWS",             "500"))
MAX_TOKENS_RESPONSE  = int(os.getenv("MAX_TOKENS_RESPONSE",  "1024"))
