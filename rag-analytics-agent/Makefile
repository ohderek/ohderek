.PHONY: install seed index start dev test clean help

# ── Setup ──────────────────────────────────────────────────────────────────────

install:
	pip install -r requirements.txt

# Create the local DuckDB mock database with ~12,500 rows of realistic data.
# Run this once before starting the agent. Safe to re-run — drops and recreates tables.
seed:
	python -m seed.mock_data

# Build (or rebuild) the ChromaDB vector index from warehouse_schema.json.
# Run this after updating the schema file with new tables or descriptions.
# The server does this automatically on startup, but you can force it here.
index:
	python -c "from src.schema_indexer import build_index; build_index(force_rebuild=True)"

# ── Running ────────────────────────────────────────────────────────────────────

# Start the FastAPI server in development mode (auto-reloads on file changes).
# Visit http://localhost:8000/docs for the interactive API explorer.
start:
	uvicorn api.main:app --reload --port 8000

# Full dev setup: seed database + start server.
dev: seed start

# ── Testing ────────────────────────────────────────────────────────────────────

# Send a sample question to the running API and display the formatted response.
# The server must be running (make start) in another terminal first.
test:
	@echo "Sending test query..."
	@curl -s -X POST http://localhost:8000/query \
		-H "Content-Type: application/json" \
		-d '{"question": "What is total revenue by region?"}' \
		| python -m json.tool

test-customer:
	@curl -s -X POST http://localhost:8000/query \
		-H "Content-Type: application/json" \
		-d '{"question": "Which acquisition channel produces the highest lifetime value customers?"}' \
		| python -m json.tool

test-ops:
	@curl -s -X POST http://localhost:8000/query \
		-H "Content-Type: application/json" \
		-d '{"question": "What is our average MTTR for P1 incidents?"}' \
		| python -m json.tool

test-health:
	@curl -s http://localhost:8000/health | python -m json.tool

# ── Cleanup ────────────────────────────────────────────────────────────────────

# Remove local state — useful for a clean reset before a demo.
# Does not touch your .env file or warehouse_schema.json.
clean:
	rm -f mock_warehouse.duckdb
	rm -rf chroma_db/
	@echo "Cleaned. Run 'make dev' to start fresh."

# ── Help ───────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "RAG Analytics Agent — available commands:"
	@echo ""
	@echo "  make install       Install Python dependencies"
	@echo "  make seed          Create mock DuckDB database with sample data"
	@echo "  make start         Start the FastAPI server (port 8000)"
	@echo "  make dev           Seed + start (full setup in one command)"
	@echo "  make test          Send a sample revenue question"
	@echo "  make test-customer Send a sample customer question"
	@echo "  make test-ops      Send a sample ops/incidents question"
	@echo "  make test-health   Check server health endpoint"
	@echo "  make clean         Remove DuckDB file and ChromaDB index"
	@echo ""
	@echo "  After 'make start': visit http://localhost:8000/docs"
	@echo ""
