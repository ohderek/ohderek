"""
api/main.py — FastAPI application: the entry point for the RAG Analytics Agent

─────────────────────────────────────────────────────────────────
WHAT IS FASTAPI?
─────────────────────────────────────────────────────────────────
FastAPI is a Python web framework for building APIs. It's the
standard choice for modern Python data/AI services because:

  - Automatic API docs at /docs — interactive in the browser
  - Type hints on request/response models = self-documenting
  - Async-capable — important since LLM calls take 1-5 seconds
  - Pydantic validation — invalid requests are rejected before
    any logic runs

─────────────────────────────────────────────────────────────────
ENDPOINTS
─────────────────────────────────────────────────────────────────
  POST /query   — the main endpoint: question → SQL → answer
  GET  /health  — liveness probe (used by load balancers, k8s)
  GET  /schema  — returns the indexed table names (debug helper)

─────────────────────────────────────────────────────────────────
STARTUP / LIFESPAN
─────────────────────────────────────────────────────────────────
The @asynccontextmanager lifespan function runs code ONCE when
the server starts, before handling any requests. We use it to
build the ChromaDB index — this way every request can share the
same in-memory collection object rather than rebuilding it from
disk on each call.

─────────────────────────────────────────────────────────────────
HOW TO RUN
─────────────────────────────────────────────────────────────────
  uvicorn api.main:app --reload --port 8000

  Then visit: http://localhost:8000/docs
  The /docs page gives you an interactive form to test /query.
"""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.config import USE_MOCK_DB
from src.logger import QueryLog, write_log
from src.query_executor import execute_query
from src.schema_indexer import build_index, retrieve_relevant_schema
from src.sql_agent import format_answer, generate_sql
from src.sql_validator import SQLValidationError, validate_sql

# ── Shared state ──────────────────────────────────────────────────────────────
# The ChromaDB collection is built once at startup and reused on every request.
# Stored as a module-level variable so all requests share the same object.
_schema_collection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan handler.

    Code before 'yield' runs on startup.
    Code after 'yield' runs on shutdown.

    On startup: load (or build) the ChromaDB schema index.
    The first run calls OpenAI and takes ~2 seconds.
    Subsequent runs load from disk in <100ms.
    """
    global _schema_collection
    print("[startup] Initialising schema index...")
    _schema_collection = build_index()
    mode = "DuckDB mock" if USE_MOCK_DB else "Snowflake production"
    print(f"[startup] Ready — data backend: {mode}")
    yield
    print("[shutdown] Agent stopped.")


app = FastAPI(
    title="RAG Analytics Agent",
    description=(
        "Natural language querying for enterprise data warehouses. "
        "Ask a question in plain English — the agent retrieves relevant schema, "
        "generates SQL, executes it, and returns a human-readable answer.\n\n"
        "**Quick test:** POST to /query with `{\"question\": \"What is total revenue by region?\"}`"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request / Response models ─────────────────────────────────────────────────
# Pydantic models define the shape of the JSON payload.
# FastAPI validates incoming requests against these automatically —
# a missing or wrong-type field returns a 422 error before any code runs.

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="A natural language question about the data in the warehouse.",
        examples=["What is total revenue by region last month?"],
    )


class QueryResponse(BaseModel):
    question:            str        = Field(description="The original question as submitted")
    sql:                 str        = Field(description="The SQL query generated and executed")
    answer:              str        = Field(description="Natural language answer to the question")
    row_count:           int        = Field(description="Number of rows the query returned")
    latency_ms:          float      = Field(description="End-to-end latency in milliseconds")
    schema_tables_used:  list[str]  = Field(description="Tables retrieved from the schema index")


class HealthResponse(BaseModel):
    status:         str   = Field(description="'ok' if healthy")
    mock_mode:      bool  = Field(description="True when running against DuckDB mock data")
    schema_indexed: bool  = Field(description="True when the ChromaDB index is loaded and ready")


class SchemaResponse(BaseModel):
    tables:  list[str]  = Field(description="Table names currently indexed")
    backend: str        = Field(description="Data warehouse backend in use")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Operations"])
async def health():
    """
    Liveness probe.

    Returns 200 OK if the service is running and the schema index is loaded.
    Useful for container health checks and load balancer probes.
    """
    return HealthResponse(
        status="ok",
        mock_mode=USE_MOCK_DB,
        schema_indexed=_schema_collection is not None,
    )


@app.get("/schema", response_model=SchemaResponse, tags=["Operations"])
async def schema():
    """
    Return the names of tables currently indexed in ChromaDB.

    Use this to verify the schema loaded correctly, or to debug
    why a question isn't retrieving the expected table.
    """
    if _schema_collection is None:
        raise HTTPException(status_code=503, detail="Schema index not ready.")

    tables = [item["table_name"] for item in _schema_collection.get()["metadatas"]]
    return SchemaResponse(
        tables=tables,
        backend="DuckDB (mock)" if USE_MOCK_DB else "Snowflake",
    )


@app.post("/query", response_model=QueryResponse, tags=["Agent"])
async def query(request: QueryRequest):
    """
    Submit a natural language question to the analytics agent.

    **Pipeline (6 steps):**

    1. **Retrieve** — embed the question, cosine-search ChromaDB,
       return the top 2 most relevant schema chunks
    2. **Generate** — GPT-4o generates SQL using the question + schema context
    3. **Validate** — SELECT-only check, LIMIT injection, blocklist scan
    4. **Execute**  — run against DuckDB (mock) or Snowflake (production)
    5. **Format**   — GPT-4o turns result rows into a plain-English answer
    6. **Log**      — write the full interaction to agent_query_log

    **Example questions to try:**
    - "What is total revenue by region last month?"
    - "Which product category has the highest refund rate?"
    - "How many churned customers do we have by region?"
    - "What is our average MTTR for P1 incidents?"
    - "Which acquisition channel produces the highest lifetime value?"
    """
    global _schema_collection

    if _schema_collection is None:
        raise HTTPException(status_code=503, detail="Schema index not ready. Try again in a moment.")

    t_start = time.monotonic() * 1000
    question = request.question.strip()

    # Pre-populate the log so we always have something to write, even on error
    log = QueryLog(
        question=question,
        generated_sql="",
        answer="",
        row_count=0,
        latency_ms=0.0,
    )

    try:
        # ── Step 1: Schema Retrieval ──────────────────────────────────────────
        schema_context = retrieve_relevant_schema(_schema_collection, question, n_results=2)

        # Extract which table names were retrieved (for response + logging)
        tables_used = [
            line.replace("Table:", "").strip()
            for line in schema_context.splitlines()
            if line.startswith("Table:")
        ]
        log.schema_tables_used = tables_used

        # ── Step 2: SQL Generation ────────────────────────────────────────────
        dialect = "duckdb" if USE_MOCK_DB else "snowflake"
        raw_sql = generate_sql(question, schema_context, dialect=dialect)
        log.generated_sql = raw_sql

        # ── Step 3: Validation ────────────────────────────────────────────────
        safe_sql = validate_sql(raw_sql)

        # ── Step 4: Execution ─────────────────────────────────────────────────
        results = execute_query(safe_sql)
        log.row_count = len(results)

        # ── Step 5: Answer Formatting ─────────────────────────────────────────
        answer = format_answer(question, safe_sql, results)
        log.answer = answer

    except SQLValidationError as exc:
        log.error = str(exc)
        log.latency_ms = time.monotonic() * 1000 - t_start
        write_log(log)
        raise HTTPException(status_code=422, detail=str(exc))

    except RuntimeError as exc:
        log.error = str(exc)
        log.latency_ms = time.monotonic() * 1000 - t_start
        write_log(log)
        raise HTTPException(status_code=500, detail=str(exc))

    log.latency_ms = time.monotonic() * 1000 - t_start
    write_log(log)

    return QueryResponse(
        question=question,
        sql=safe_sql,
        answer=answer,
        row_count=log.row_count,
        latency_ms=round(log.latency_ms, 1),
        schema_tables_used=tables_used,
    )
