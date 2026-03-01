"""
agent/streamlit_app.py — Browser-based chat interface for the crypto agent

Streamlit turns a Python script into a web page. No HTML, no CSS, no JavaScript.
It re-runs the script top-to-bottom every time the user interacts with the page.
The session_state dict persists data (like chat history) between re-runs.

Run:
    streamlit run agent/streamlit_app.py

Then open: http://localhost:8501

─────────────────────────────────────────────────────────────────
WHAT THE UI LOOKS LIKE
─────────────────────────────────────────────────────────────────
┌────────────────────────────────────────────────────────────┐
│  🪙 Crypto Analytics Agent                         [demo]  │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  👤 What is the price of Bitcoin today?                   │
│  ─────────────────────────────────────────────────────    │
│  🤖 Bitcoin (BTC) is trading at $95,432.10, up +2.31%    │
│     in the last 24 hours. Data as of 2024-12-14 09:42.   │
│     ▼ Show SQL                                            │
│     SELECT name, symbol, price_usd, change_24h_pct...    │
│                                                            │
│  👤 Which coin is up the most today?                      │
│  ─────────────────────────────────────────────────────    │
│  🤖 Solana (SOL) leads at +4.52%, trading at $198.72...  │
│     ▼ Show SQL                                            │
│                                                            │
│  ────────────────────────────────────────────────────     │
│  [ Ask a question about crypto markets...         ] [→]   │
└────────────────────────────────────────────────────────────┘

─────────────────────────────────────────────────────────────────
HOW SESSION STATE WORKS
─────────────────────────────────────────────────────────────────
Streamlit re-runs the whole script on every user action.
Without session_state, the chat history would be lost each time.

st.session_state["messages"] is a list that persists across re-runs.
Each message is a dict: {"role": "user"|"assistant", "content": str, "sql": str}

The schema index (ChromaDB collection) is also cached in session_state
so it's only built once — not on every message.
"""

import sys
from pathlib import Path

# Allow imports from the parent directory so we can reuse crypto_agent.py
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from agent.crypto_agent import (
    ValidationError,
    ask,
    build_index,
    seed_demo_db,
    DEMO_DB_PATH,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Analytics Agent",
    page_icon="🪙",
    layout="centered",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Settings")

    demo_mode = st.toggle(
        "Demo mode (local data)",
        value=True,
        help=(
            "ON  → reads from local DuckDB mock data. No Snowflake needed.\n"
            "OFF → reads from real Snowflake analytics views (requires credentials in .env)."
        ),
    )

    if demo_mode and not DEMO_DB_PATH.exists():
        st.warning("Demo database not found. Click below to create it.")
        if st.button("Seed demo data"):
            with st.spinner("Creating mock database..."):
                seed_demo_db()
            st.success("Done — mock data ready.")
            st.rerun()

    st.divider()
    st.caption("**Data source**")
    if demo_mode:
        st.info("📦 DuckDB mock data\n\nSimulated prices — not live market data.")
    else:
        st.info("❄️ Snowflake\n\nReads from CRYPTO.ANALYTICS views populated by main.py.")

    st.divider()
    st.caption("**Try these questions**")
    EXAMPLE_QUESTIONS = [
        "What is the price of Bitcoin today?",
        "Which coin is up the most today?",
        "What is Bitcoin dominance this week?",
        "How volatile has Ethereum been this week?",
        "Show me the top 10 coins by market cap",
        "Which acquisition channel produces the highest lifetime value?",
    ]
    for q in EXAMPLE_QUESTIONS:
        if st.button(q, use_container_width=True):
            st.session_state["pending_question"] = q

    if st.button("🗑️ Clear chat", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()

# ── Schema index — built once, cached in session state ────────────────────────
# st.cache_resource caches the return value across all sessions and re-runs.
# Without this, the index would be rebuilt on every message — slow and costly.
@st.cache_resource
def get_schema_index():
    """Build the ChromaDB schema index once and reuse it across all requests."""
    return build_index()

collection = get_schema_index()

# ── Chat history ──────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🪙 Crypto Analytics Agent")
mode_label = "demo mode · local data" if demo_mode else "live · Snowflake"
st.caption(f"Ask anything about crypto market data — powered by GPT-4o · {mode_label}")
st.divider()

# ── Render chat history ───────────────────────────────────────────────────────
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        # Show the generated SQL in a collapsible expander
        if msg.get("sql"):
            with st.expander("Show SQL"):
                st.code(msg["sql"], language="sql")
            if msg.get("row_count") is not None:
                st.caption(
                    f"{msg['row_count']} rows · {msg.get('latency_ms', 0):.0f}ms · "
                    f"tables: {msg.get('tables_used', [])}"
                )

# ── Handle example question clicks from sidebar ───────────────────────────────
# When a sidebar button is clicked, the question is stored in session_state.
# We retrieve and clear it here so it flows into the normal chat handler below.
pending = st.session_state.pop("pending_question", None)

# ── Chat input ────────────────────────────────────────────────────────────────
# st.chat_input renders the text box at the bottom of the page.
# It returns the submitted text (or None if nothing was submitted this run).
user_input = st.chat_input("Ask a question about crypto markets...") or pending

if user_input:
    # Append user message to history and display it
    st.session_state["messages"].append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Run the agent pipeline
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                result = ask(user_input, collection, demo_mode=demo_mode)

                # Display the natural language answer
                st.markdown(result["answer"])

                # Show the SQL in a collapsible expander
                with st.expander("Show SQL"):
                    st.code(result["sql"], language="sql")

                st.caption(
                    f"{result['row_count']} rows · {result['latency_ms']:.0f}ms · "
                    f"tables: {result['schema_tables_used']}"
                )

                # Save to chat history
                st.session_state["messages"].append({
                    "role":        "assistant",
                    "content":     result["answer"],
                    "sql":         result["sql"],
                    "row_count":   result["row_count"],
                    "latency_ms":  result["latency_ms"],
                    "tables_used": result["schema_tables_used"],
                })

            except ValidationError as e:
                msg = f"⚠️ {e}"
                st.warning(msg)
                st.session_state["messages"].append({"role": "assistant", "content": msg})

            except Exception as e:
                msg = f"❌ Error: {e}"
                st.error(msg)
                st.session_state["messages"].append({"role": "assistant", "content": msg})
