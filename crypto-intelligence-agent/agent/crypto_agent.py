"""
agent/crypto_agent.py — Natural language querying over CoinMarketCap data

─────────────────────────────────────────────────────────────────
HOW THIS CONNECTS TO THE EXISTING PIPELINE
─────────────────────────────────────────────────────────────────
The main.py pipeline:
  CoinMarketCap API → Parquet → Snowflake stage → MERGE → analytics views

This agent:
  Question → retrieve schema → generate SQL → query analytics views → answer

These two are completely independent. The pipeline writes; the agent reads.
The database is the only thing they share. Nothing in main.py needed to change.

─────────────────────────────────────────────────────────────────
EXECUTION MODES
─────────────────────────────────────────────────────────────────
  --demo      Local DuckDB with seeded mock prices. No credentials needed.
              Run this first to understand how the agent works.

  (default)   Real Snowflake, reading from the analytics views that
              main.py populates. Requires Snowflake credentials in .env.

─────────────────────────────────────────────────────────────────
USAGE
─────────────────────────────────────────────────────────────────
  # Single question
  python agent/crypto_agent.py "What is the price of Bitcoin today?"

  # Interactive mode (keeps asking until you type 'exit')
  python agent/crypto_agent.py --interactive

  # Demo mode — no Snowflake needed
  python agent/crypto_agent.py --demo "What is the price of Bitcoin today?"
  python agent/crypto_agent.py --demo --interactive
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ── Dependency check ──────────────────────────────────────────────────────────
# Give a clear error if packages aren't installed rather than a traceback
_MISSING = []
try:
    import chromadb
    from chromadb.utils import embedding_functions
except ImportError:
    _MISSING.append("chromadb")

try:
    from openai import OpenAI
except ImportError:
    _MISSING.append("openai")

if _MISSING:
    print(f"Missing packages: {', '.join(_MISSING)}")
    print("Run: pip install -r requirements-agent.txt")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

OPENAI_API_KEY  = os.environ["OPENAI_API_KEY"]
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
CHAT_MODEL      = os.getenv("CHAT_MODEL",       "gpt-4o")
MAX_ROWS        = int(os.getenv("MAX_ROWS",  "200"))

SCHEMA_FILE    = Path(__file__).parent / "crypto_schema.json"
CHROMA_DIR     = Path(__file__).parent / ".chroma_cache"
CHROMA_COLL    = "crypto_schema"
DEMO_DB_PATH   = Path(__file__).parent / "demo_data.duckdb"

# Snowflake connection config (only needed if not in demo mode)
SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",   "")
SF_USER      = os.getenv("SNOWFLAKE_USER",       "")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD",   "")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE",       "ANALYST")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE",  "CRYPTO_WH")
SF_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",   "CRYPTO")
SF_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",     "ANALYTICS")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Schema Indexing
#
# We read crypto_schema.json and build a ChromaDB vector index.
# Each table gets embedded as one text chunk (name + description + columns).
# The index is persisted to .chroma_cache/ so we only pay for embedding once.
# ═══════════════════════════════════════════════════════════════════════════════

def _load_schema_docs() -> list[dict]:
    """
    Read crypto_schema.json and convert each table into an indexable text chunk.

    Why one chunk per table (not per column)?
      A question like "what is Bitcoin's price?" needs context about the whole
      CURRENT_TOP_10 table — not just the price_usd column in isolation.
      Keeping all columns together gives the LLM a complete picture.
    """
    with open(SCHEMA_FILE) as f:
        schema = json.load(f)

    docs = []
    for table in schema["tables"]:
        col_lines = "\n".join(
            f"  - {c['name']} ({c['type']}): {c['description']}"
            for c in table["columns"]
        )
        sample_q = "\n".join(f"  - {q}" for q in table.get("sample_questions", []))

        # The text that gets converted to an embedding vector.
        # We use the DuckDB table name as the default; the Snowflake name is
        # included so the model knows both refer to the same table.
        text = (
            f"Table: {table['name']}\n"
            f"Snowflake name: {table['snowflake_name']}\n"
            f"Description: {table['description']}\n"
            f"Columns:\n{col_lines}\n"
            f"Example questions:\n{sample_q}"
        )
        docs.append({
            "id":       table["name"],
            "text":     text,
            "metadata": {
                "table_name":      table["name"],
                "snowflake_name":  table["snowflake_name"],
                "column_names":    ",".join(c["name"] for c in table["columns"]),
            },
        })
    return docs


def build_index(force_rebuild: bool = False) -> chromadb.Collection:
    """
    Build or load the ChromaDB schema index.

    First run: calls OpenAI to embed 4 schema chunks (~$0.0001).
    Subsequent runs: loads from .chroma_cache/ — instant, no API calls.

    Args:
        force_rebuild: Delete and rebuild. Use after editing crypto_schema.json.
    """
    openai_ef = embedding_functions.OpenAIEmbeddingFunction(
        api_key=OPENAI_API_KEY,
        model_name=EMBEDDING_MODEL,
    )
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    existing = [c.name for c in client.list_collections()]

    if CHROMA_COLL in existing and not force_rebuild:
        return client.get_collection(CHROMA_COLL, embedding_function=openai_ef)

    if CHROMA_COLL in existing:
        client.delete_collection(CHROMA_COLL)

    collection = client.create_collection(
        CHROMA_COLL,
        embedding_function=openai_ef,
        metadata={"hnsw:space": "cosine"},
    )
    docs = _load_schema_docs()
    collection.add(
        ids=[d["id"] for d in docs],
        documents=[d["text"] for d in docs],
        metadatas=[d["metadata"] for d in docs],
    )
    print(f"[agent] Schema index built — {len(docs)} tables indexed.")
    return collection


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Schema Retrieval
#
# Embed the user's question and find the most similar schema chunks.
# This is the "R" in RAG — we only send the relevant tables to the LLM,
# not the entire schema.
# ═══════════════════════════════════════════════════════════════════════════════

def retrieve_schema(collection: chromadb.Collection, question: str, n: int = 2) -> tuple[str, list[str]]:
    """
    Find the schema chunks most relevant to the question via cosine similarity.

    Returns:
        (schema_context_string, list_of_table_names_retrieved)

    Why n=2? Most crypto questions involve 1 table. Retrieving 2 gives the LLM
    a fallback if the first match isn't quite right, without bloating the prompt.
    """
    results = collection.query(
        query_texts=[question],
        n_results=min(n, collection.count()),
    )
    docs   = results["documents"][0]
    metas  = results["metadatas"][0]
    tables = [m["table_name"] for m in metas]
    return "\n\n---\n\n".join(docs), tables


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — SQL Generation
#
# Combine the question + retrieved schema into a prompt for GPT-4o.
# Two things make crypto SQL tricky:
#   1. Coin names vary ("Bitcoin" / "BTC" / "bitcoin") — the prompt handles this
#   2. "Today" / "current" maps to MAX(fetched_at), not CURRENT_DATE — mentioned explicitly
# ═══════════════════════════════════════════════════════════════════════════════

def generate_sql(question: str, schema_context: str, demo_mode: bool) -> str:
    """
    Call GPT-4o to generate a SQL query.

    Crypto-specific prompt additions:
      - Symbol matching is case-insensitive (users type 'btc' not 'BTC')
      - "Today"/"current"/"now" means use current_top_10 or MAX(as_of)
      - Always include as_of or fetched_at in SELECT so the answer shows data freshness
      - Data freshness note: the pipeline runs on a schedule, so "today" = last fetch

    Temperature=0: SQL must be deterministic. Same question → same SQL.
    """
    dialect = "DuckDB" if demo_mode else "Snowflake"
    # In demo mode we use simple table names; in Snowflake mode, fully qualified
    table_note = (
        "Use plain table names (e.g. current_top_10, daily_coin_prices)."
        if demo_mode else
        "Use fully qualified Snowflake names as shown in the schema (e.g. CRYPTO.ANALYTICS.CURRENT_TOP_10)."
    )

    prompt = f"""You are an expert {dialect} SQL analyst working with CoinMarketCap market data.
Write a single SELECT query to answer the question below.

IMPORTANT CRYPTO-SPECIFIC RULES:
- Symbol matching must be case-insensitive: use UPPER(symbol) = UPPER('BTC') or ILIKE
- "Today", "current", "now", "latest" → use the current_top_10 table (already filtered to latest)
- For historical questions → use daily_coin_prices or price_history_7d
- Always include the as_of or fetched_at column in SELECT so the user knows how fresh the data is
- {table_note}
- Output ONLY the SQL — no explanation, no markdown, no code fences
- Always include LIMIT {MAX_ROWS}
- If you cannot answer from the available schema, output: CANNOT_ANSWER

SCHEMA CONTEXT:
{schema_context}

QUESTION: {question}

SQL QUERY:"""

    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=512,
    )
    sql = response.choices[0].message.content.strip()

    # Strip markdown code fences if the model added them anyway
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1]).strip()

    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — SQL Validation
#
# Three guards before any SQL touches the database:
#   1. Must be SELECT or WITH (allowlist)
#   2. No destructive keywords (blocklist)
#   3. Must have a LIMIT (inject if missing)
# ═══════════════════════════════════════════════════════════════════════════════

BLOCKED = {"DROP","DELETE","TRUNCATE","UPDATE","INSERT","MERGE","ALTER","CREATE","GRANT","REVOKE","EXECUTE","EXEC","CALL"}

class ValidationError(Exception):
    pass

def validate_sql(sql: str) -> str:
    if not sql or not sql.strip():
        raise ValidationError("Empty SQL returned by the model.")
    if sql.strip().upper() == "CANNOT_ANSWER":
        raise ValidationError(
            "The agent couldn't map this question to the available schema. "
            "Try asking about coin prices, market caps, volume, or BTC dominance."
        )
    normalised = " ".join(sql.split())
    first = normalised.lstrip().split()[0].upper()
    if first not in ("SELECT", "WITH"):
        raise ValidationError(f"Only SELECT queries are allowed. Got: {first}")
    for kw in BLOCKED:
        if re.search(rf"\b{re.escape(kw)}\b", normalised.upper()):
            raise ValidationError(f"Blocked keyword '{kw}' in generated SQL.")
    if "LIMIT" not in normalised.upper():
        normalised = f"{normalised.rstrip(';')} LIMIT {MAX_ROWS}"
    return normalised


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5 — Execution
#
# Routes to DuckDB (demo mode) or Snowflake (production mode).
# Both return List[Dict] — the same interface.
# ═══════════════════════════════════════════════════════════════════════════════

def execute_snowflake(sql: str) -> list[dict]:
    """Execute against real Snowflake analytics views."""
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DATABASE, schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE, role=SF_ROLE,
    )
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql)
        return cur.fetchmany(MAX_ROWS)
    finally:
        conn.close()


def execute_duckdb(sql: str) -> list[dict]:
    """Execute against the local DuckDB demo database."""
    import duckdb
    conn = duckdb.connect(str(DEMO_DB_PATH), read_only=True)
    try:
        cur  = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Answer Formatting
#
# A second LLM call turns result rows into a plain-English answer.
# Crypto-specific: always note the data freshness (as_of timestamp).
# ═══════════════════════════════════════════════════════════════════════════════

def format_answer(question: str, sql: str, results: list[dict]) -> str:
    """
    Call GPT-4o to turn query results into a readable answer.

    Key crypto additions to the prompt:
      - Must mention data freshness ("as of [timestamp]")
      - Format large numbers properly ($95,432.10 not 95432.1)
      - Note if data might be stale (if as_of is old)
    """
    if not results:
        return (
            "No data found. The pipeline may not have run yet, or the coin "
            "you asked about isn't in the top tracked list."
        )

    prompt = f"""You are a crypto market analyst. Answer the question directly and concisely.

RULES:
- Lead with the direct answer (price, value, percentage)
- Format USD amounts with commas and 2 decimal places: $95,432.10
- Format percentages with a + or - sign and 2 decimals: +3.42%
- Always mention the data timestamp (as_of or fetched_at field) so the user knows freshness
- If the timestamp looks old (more than a few hours), note that the pipeline may not have run recently
- Keep it under 120 words
- Do not quote the SQL

QUESTION: {question}
SQL USED: {sql}
RESULTS: {results[:15]}

ANSWER:"""

    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=300,
    )
    return resp.choices[0].message.content.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# DEMO MODE — Local DuckDB with mock crypto prices
#
# Creates demo_data.duckdb with realistic mock prices matching the exact
# column structure of the Snowflake analytics views.
# Run: python agent/crypto_agent.py --seed-demo
# ═══════════════════════════════════════════════════════════════════════════════

DEMO_COINS = [
    # (rank, symbol, name, price_usd, mcap_usd, vol_24h, chg_24h_pct, chg_7d_pct)
    (1,  "BTC",  "Bitcoin",      95_432.10, 1_884_000_000_000,  42_100_000_000,  +2.31,  +8.42),
    (2,  "ETH",  "Ethereum",      3_521.80,   423_000_000_000,  21_800_000_000,  +1.87,  +6.11),
    (3,  "BNB",  "BNB",             621.45,    90_200_000_000,   2_100_000_000,  -0.43,  +3.22),
    (4,  "SOL",  "Solana",          198.72,    93_100_000_000,   6_400_000_000,  +4.52, +14.31),
    (5,  "XRP",  "XRP",               2.18,   124_000_000_000,   8_700_000_000,  +0.91,  +5.67),
    (6,  "USDC", "USD Coin",          1.00,    45_800_000_000,   7_200_000_000,   0.00,   0.01),
    (7,  "ADA",  "Cardano",           0.92,    32_700_000_000,   1_200_000_000,  -1.21,  -2.44),
    (8,  "AVAX", "Avalanche",        41.83,    17_200_000_000,   1_050_000_000,  +3.11,  +9.87),
    (9,  "DOGE", "Dogecoin",          0.38,    55_900_000_000,   4_300_000_000,  +1.44,  +3.01),
    (10, "DOT",  "Polkadot",          9.12,    12_600_000_000,     620_000_000,  -0.87,  +1.23),
]

def seed_demo_db():
    """
    Create demo_data.duckdb with mock data that mirrors the Snowflake view schemas.

    The column names match exactly — SQL generated for demo mode works against
    the real Snowflake views with only the table name prefix changing.
    """
    import duckdb, random
    random.seed(42)

    now     = datetime.now(timezone.utc)
    today   = date.today()

    conn = duckdb.connect(str(DEMO_DB_PATH))

    # ── current_top_10 ─────────────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS current_top_10")
    conn.execute("""
        CREATE TABLE current_top_10 (
            rank           INTEGER,
            symbol         VARCHAR,
            name           VARCHAR,
            price_usd      DOUBLE,
            market_cap_usd DOUBLE,
            volume_24h_usd DOUBLE,
            change_24h_pct DOUBLE,
            change_7d_pct  DOUBLE,
            as_of          TIMESTAMP
        )
    """)
    rows = [(r, s, n, p, mc, v, c24, c7, now) for r, s, n, p, mc, v, c24, c7 in DEMO_COINS]
    conn.executemany("INSERT INTO current_top_10 VALUES (?,?,?,?,?,?,?,?,?)", rows)

    # ── daily_coin_prices — 30 days of history ─────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS daily_coin_prices")
    conn.execute("""
        CREATE TABLE daily_coin_prices (
            price_date          DATE,
            symbol              VARCHAR,
            name                VARCHAR,
            rank_at_close       INTEGER,
            price_usd           DOUBLE,
            volume_24h_usd      DOUBLE,
            market_cap_usd      DOUBLE,
            market_cap_dominance DOUBLE,
            pct_change_24h      DOUBLE,
            pct_change_7d       DOUBLE,
            pct_change_30d      DOUBLE,
            circulating_supply  DOUBLE
        )
    """)
    hist_rows = []
    for days_ago in range(30, 0, -1):
        d = today - timedelta(days=days_ago)
        for rank, sym, name, price, mcap, vol, c24, c7 in DEMO_COINS:
            # Simulate realistic drift — prices walk slightly each day
            factor = 1 + random.uniform(-0.04, 0.04) * (days_ago / 30)
            p = round(price * factor, 2)
            hist_rows.append((
                d, sym, name, rank, p,
                round(vol * random.uniform(0.7, 1.3), 0),
                round(mcap * factor, 0),
                round(random.uniform(1, 55) if sym == "BTC" else random.uniform(0.5, 20), 2),
                round(random.uniform(-5, 5), 2),
                round(random.uniform(-10, 10), 2),
                round(random.uniform(-20, 20), 2),
                round(mcap / p, 0),
            ))
    conn.executemany("INSERT INTO daily_coin_prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", hist_rows)

    # ── price_history_7d ───────────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS price_history_7d")
    conn.execute("""
        CREATE TABLE price_history_7d (
            price_date      DATE,
            symbol          VARCHAR,
            name            VARCHAR,
            avg_price_usd   DOUBLE,
            high_price_usd  DOUBLE,
            low_price_usd   DOUBLE,
            max_volume_usd  DOUBLE,
            snapshot_count  INTEGER
        )
    """)
    week_rows = []
    for days_ago in range(7, 0, -1):
        d = today - timedelta(days=days_ago)
        for rank, sym, name, price, _, vol, c24, c7 in DEMO_COINS:
            avg  = round(price * (1 + random.uniform(-0.02, 0.02)), 2)
            high = round(avg * random.uniform(1.01, 1.05), 2)
            low  = round(avg * random.uniform(0.95, 0.99), 2)
            week_rows.append((d, sym, name, avg, high, low, vol, random.randint(6, 24)))
    conn.executemany("INSERT INTO price_history_7d VALUES (?,?,?,?,?,?,?,?)", week_rows)

    # ── btc_dominance_trend ────────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS btc_dominance_trend")
    conn.execute("""
        CREATE TABLE btc_dominance_trend (
            metric_date                DATE,
            avg_btc_dominance_pct      DOUBLE,
            avg_eth_dominance_pct      DOUBLE,
            avg_total_mcap_trillion_usd DOUBLE,
            avg_24h_volume_billion_usd  DOUBLE,
            active_coins               INTEGER
        )
    """)
    dom_rows = []
    for days_ago in range(30, 0, -1):
        d = today - timedelta(days=days_ago)
        dom_rows.append((
            d,
            round(52.4 + random.uniform(-2, 2), 2),
            round(16.8 + random.uniform(-1, 1), 2),
            round(3.1 + random.uniform(-0.2, 0.2), 3),
            round(180 + random.uniform(-20, 20), 1),
            random.randint(8900, 9100),
        ))
    conn.executemany("INSERT INTO btc_dominance_trend VALUES (?,?,?,?,?,?)", dom_rows)

    conn.close()
    print(f"[demo] Mock database created at {DEMO_DB_PATH}")
    print(f"[demo] Tables: current_top_10, daily_coin_prices, price_history_7d, btc_dominance_trend")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — Pipeline orchestration + CLI
# ═══════════════════════════════════════════════════════════════════════════════

def ask(question: str, collection: chromadb.Collection, demo_mode: bool) -> dict:
    """
    Run one question through the full RAG pipeline.

    Returns a dict with: question, sql, answer, row_count, latency_ms, tables_used
    """
    t0 = time.monotonic()

    # 1. Retrieve relevant schema
    schema_ctx, tables_used = retrieve_schema(collection, question)

    # 2. Generate SQL
    raw_sql = generate_sql(question, schema_ctx, demo_mode)

    # 3. Validate
    safe_sql = validate_sql(raw_sql)

    # 4. Execute
    results = execute_duckdb(safe_sql) if demo_mode else execute_snowflake(safe_sql)

    # 5. Format answer
    answer = format_answer(question, safe_sql, results)

    latency = round((time.monotonic() - t0) * 1000, 1)
    return {
        "question":    question,
        "sql":         safe_sql,
        "answer":      answer,
        "row_count":   len(results),
        "latency_ms":  latency,
        "tables_used": tables_used,
    }


def _print_result(result: dict):
    """Pretty-print one agent result to the terminal."""
    print()
    print("─" * 60)
    print(f"Q: {result['question']}")
    print(f"\nSQL:\n  {result['sql'][:120]}{'...' if len(result['sql']) > 120 else ''}")
    print(f"\nA: {result['answer']}")
    print(f"\n[{result['latency_ms']:.0f}ms · {result['row_count']} rows · tables: {result['tables_used']}]")
    print("─" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Ask natural language questions about CoinMarketCap data."
    )
    parser.add_argument("question", nargs="?", help="Question to ask")
    parser.add_argument("--interactive", action="store_true", help="Interactive Q&A loop")
    parser.add_argument("--demo",        action="store_true", help="Use local DuckDB mock data")
    parser.add_argument("--seed-demo",   action="store_true", help="Create/recreate demo DuckDB database")
    parser.add_argument("--rebuild-index", action="store_true", help="Rebuild ChromaDB schema index")
    args = parser.parse_args()

    # Seed demo database if requested
    if args.seed_demo:
        seed_demo_db()
        if not args.question and not args.interactive:
            return

    # Check demo DB exists if in demo mode
    if args.demo and not DEMO_DB_PATH.exists():
        print("[agent] Demo database not found. Run: python agent/crypto_agent.py --seed-demo")
        sys.exit(1)

    # Build schema index
    collection = build_index(force_rebuild=args.rebuild_index)

    if args.interactive:
        print("\nCrypto Analytics Agent — type 'exit' to quit\n")
        while True:
            q = input("Q: ").strip()
            if q.lower() in ("exit", "quit", "q"):
                break
            if not q:
                continue
            try:
                result = ask(q, collection, demo_mode=args.demo)
                _print_result(result)
            except ValidationError as e:
                print(f"\n[validation error] {e}\n")
            except Exception as e:
                print(f"\n[error] {e}\n")

    elif args.question:
        try:
            result = ask(args.question, collection, demo_mode=args.demo)
            _print_result(result)
        except ValidationError as e:
            print(f"[validation error] {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
