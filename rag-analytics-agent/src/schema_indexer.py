"""
schema_indexer.py — Build the retrieval layer (the "R" in RAG)

─────────────────────────────────────────────────────────────────
WHAT IS RAG?
─────────────────────────────────────────────────────────────────
RAG = Retrieval-Augmented Generation.

The problem it solves: a data warehouse can have hundreds of tables
and thousands of columns. You can't paste all of that into an LLM
prompt — there are token limits, and more importantly the LLM gets
confused by irrelevant context.

RAG's solution: pre-index the schema, then at query time retrieve
only the 2-3 tables that are actually relevant to the question.

─────────────────────────────────────────────────────────────────
WHAT IS AN EMBEDDING?
─────────────────────────────────────────────────────────────────
An embedding is a list of numbers (a "vector") that represents the
meaning of a piece of text. Two pieces of text with similar meaning
produce vectors that are close together in space.

Example:
  "total revenue by region"    → [0.21, -0.43, 0.08, ...]   (1536 numbers)
  "sales breakdown per area"   → [0.19, -0.41, 0.09, ...]   (similar!)
  "number of open incidents"   → [-0.31, 0.52, -0.14, ...]  (different)

OpenAI's text-embedding-3-small model produces these vectors.
We embed both the schema descriptions AND the user's question,
then measure how close they are. Close = relevant.

─────────────────────────────────────────────────────────────────
WHAT IS A VECTOR STORE?
─────────────────────────────────────────────────────────────────
ChromaDB is a database that stores embeddings and can search them
efficiently. When you ask "show me revenue by region", ChromaDB
finds the schema chunks whose embeddings are closest to the
question embedding — in milliseconds, even with millions of items.

─────────────────────────────────────────────────────────────────
THE THREE-STEP INDEXING PROCESS (runs once on startup):
─────────────────────────────────────────────────────────────────
  1. READ   — load table/column descriptions from warehouse_schema.json
  2. EMBED  — OpenAI converts each description into a vector
  3. STORE  — ChromaDB saves the vectors to disk (./chroma_db/)

At query time (see api/main.py):
  4. EMBED  — embed the user's question (same model)
  5. SEARCH — ChromaDB finds the closest schema vectors (cosine similarity)
  6. INJECT — those schema chunks go into the LLM's SQL generation prompt
"""

import json
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions

from src.config import (
    CHROMA_COLLECTION,
    CHROMA_PERSIST_DIR,
    EMBEDDING_MODEL,
    OPENAI_API_KEY,
    SCHEMA_FILE,
)


def _load_schema() -> list[dict]:
    """
    Load warehouse_schema.json and convert it into indexable "chunks".

    Chunking strategy: one chunk per TABLE containing all its column
    descriptions concatenated. Why not one chunk per column?

      - Keeping columns together preserves context. A question about
        "revenue by region" needs to know both the net_line_amount
        column AND the region column exist on the same table.
      - Fewer, richer chunks = fewer API calls = lower embedding cost.
      - The retrieved chunk gives the LLM a complete picture of one table.

    Returns:
        A list of dicts, each with:
          - id:       unique string ID for ChromaDB
          - text:     the full text that gets embedded
          - metadata: structured fields stored alongside the vector
    """
    schema_path = Path(SCHEMA_FILE)
    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found at: {SCHEMA_FILE}\n"
            f"Expected path: {schema_path.resolve()}"
        )

    with open(schema_path) as f:
        schema = json.load(f)

    documents = []
    for model in schema["models"]:
        # Build a rich, descriptive text block for this table.
        # More detail = better embedding = more accurate retrieval.
        col_lines = "\n".join(
            f"  - {col['name']} ({col['type']}): {col['description']}"
            for col in model["columns"]
        )
        sample_q_lines = "\n".join(
            f"  - {q}" for q in model.get("sample_questions", [])
        )

        # The text that gets converted to a vector.
        # We include table name, description, every column, and example
        # questions — this maximises the chance of matching any phrasing
        # a user might use.
        text = (
            f"Table: {model['name']}\n"
            f"Description: {model['description']}\n"
            f"Columns:\n{col_lines}\n"
            f"Example questions this table can answer:\n{sample_q_lines}"
        )

        documents.append({
            "id":       model["name"],   # unique ID in ChromaDB (table name)
            "text":     text,            # text that gets embedded
            "metadata": {
                "table_name":   model["name"],
                "column_count": str(len(model["columns"])),
                # Comma-separated column names — useful for debugging
                "column_names": ",".join(c["name"] for c in model["columns"]),
            },
        })

    return documents


def build_index(force_rebuild: bool = False) -> chromadb.Collection:
    """
    Build (or load) the ChromaDB vector index from the schema file.

    This is called once at application startup. On the first run it:
      - Reads warehouse_schema.json
      - Calls OpenAI to embed each schema chunk (~3 API calls, <$0.001)
      - Saves the vectors to ./chroma_db/ on disk

    On subsequent runs (same machine), it loads the existing index from
    disk — no re-embedding, instant startup.

    Args:
        force_rebuild: Delete and recreate the index. Use this after
                       updating warehouse_schema.json with new tables
                       or improved descriptions.

    Returns:
        A ChromaDB Collection — the live vector store, ready to search.
    """
    # PersistentClient stores the index to disk so it survives restarts.
    client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)

    # Tell ChromaDB to use OpenAI for embedding. It calls the API
    # automatically whenever you add documents or run a query.
    openai_ef = embedding_functions.OpenAIEmbeddingFunction(
        api_key=OPENAI_API_KEY,
        model_name=EMBEDDING_MODEL,
    )

    existing_names = [c.name for c in client.list_collections()]

    if CHROMA_COLLECTION in existing_names and not force_rebuild:
        print(f"[indexer] Loading existing index '{CHROMA_COLLECTION}' from {CHROMA_PERSIST_DIR}")
        return client.get_collection(
            name=CHROMA_COLLECTION,
            embedding_function=openai_ef,
        )

    # If rebuilding, delete the old collection first
    if CHROMA_COLLECTION in existing_names:
        client.delete_collection(CHROMA_COLLECTION)
        print(f"[indexer] Deleted old index for rebuild.")

    # Create a new collection.
    # hnsw:space=cosine means similarity is measured by cosine distance,
    # which works better than Euclidean distance for text embeddings.
    collection = client.create_collection(
        name=CHROMA_COLLECTION,
        embedding_function=openai_ef,
        metadata={"hnsw:space": "cosine"},
    )

    documents = _load_schema()
    print(f"[indexer] Embedding {len(documents)} schema chunks via OpenAI...")

    collection.add(
        ids=[d["id"] for d in documents],
        documents=[d["text"] for d in documents],
        metadatas=[d["metadata"] for d in documents],
    )

    print(f"[indexer] Done — {len(documents)} chunks indexed in {CHROMA_PERSIST_DIR}/")
    return collection


def retrieve_relevant_schema(
    collection: chromadb.Collection,
    question: str,
    n_results: int = 2,
) -> str:
    """
    Find the schema chunks most relevant to the user's question.

    This is the "R" (Retrieval) step executed at query time.

    What happens internally:
      1. ChromaDB embeds the question (one OpenAI API call)
      2. It computes cosine similarity between the question vector
         and every stored schema vector
      3. Returns the top n_results matches

    Why n_results=2?
      - Most questions involve 1-2 tables. More chunks = more tokens
        in the SQL generation prompt = higher cost + lower accuracy.
      - For join-heavy questions you might want n_results=3.

    Args:
        collection: The ChromaDB collection from build_index().
        question:   The user's natural language question.
        n_results:  Number of schema chunks to retrieve.

    Returns:
        A single string of concatenated schema context — this is what
        gets pasted into the SQL generation prompt as the "grounding".
    """
    results = collection.query(
        query_texts=[question],
        n_results=min(n_results, collection.count()),  # can't retrieve more than exist
    )

    # results["documents"] is a list of lists — one inner list per query.
    # We always send one query, so we take index [0].
    retrieved_docs: list[str] = results["documents"][0]

    if not retrieved_docs:
        return "No relevant schema found."

    # Separate chunks clearly so the LLM treats them as distinct tables
    return "\n\n---\n\n".join(retrieved_docs)
