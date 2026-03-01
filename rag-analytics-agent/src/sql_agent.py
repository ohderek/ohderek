"""
sql_agent.py — Two LLM calls: question → SQL, results → answer

─────────────────────────────────────────────────────────────────
WHAT IS PROMPT ENGINEERING?
─────────────────────────────────────────────────────────────────
Prompt engineering is writing instructions for an LLM the way you
would write a very precise brief for a contractor. The quality of
the output is almost entirely determined by how well you specify:
  - What role the model should play ("you are an expert SQL analyst")
  - What constraints apply ("output ONLY SQL, no explanation")
  - What context it has (the schema retrieved from ChromaDB)
  - What format you expect ("no markdown, no code fences")

─────────────────────────────────────────────────────────────────
WHY TWO SEPARATE LLM CALLS?
─────────────────────────────────────────────────────────────────
We split generation into two focused calls:

  Call 1 — SQL GENERATION
    Input:  user question + schema context
    Output: a SQL query
    Temperature: 0 (deterministic — you want the same SQL for the
                 same question, not creative variation)

  Call 2 — ANSWER FORMATTING
    Input:  original question + SQL that was run + result rows
    Output: a plain-English answer
    Temperature: 0.3 (slight variation is fine for prose)

Why not one call? Separating them makes debugging much easier.
If the answer is wrong you can inspect the SQL independently of the
prose. It also lets you swap models — a cheaper model can do the
formatting once the hard SQL work is done.

─────────────────────────────────────────────────────────────────
TEMPERATURE EXPLAINED
─────────────────────────────────────────────────────────────────
Temperature controls how "random" the LLM's output is:
  - 0.0 = deterministic, always picks the most likely token
  - 1.0 = highly variable, creative but sometimes incoherent
  - 0.3 = a little natural variation while staying grounded

For SQL: always use 0. Randomness in SQL means broken queries.
For prose: 0.2-0.4 makes answers sound human rather than robotic.
"""

from openai import OpenAI

from src.config import CHAT_MODEL, MAX_ROWS, MAX_TOKENS_RESPONSE, OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)


def _build_sql_prompt(question: str, schema_context: str, dialect: str) -> str:
    """
    Build the system+user prompt for SQL generation.

    The prompt structure:
      - Role definition     ("You are an expert SQL analyst")
      - Hard rules          (SELECT only, LIMIT, exact column names)
      - Schema context      (the retrieved table/column descriptions)
      - The question        (what the user actually asked)
      - Output trigger      ("SQL QUERY:" — nudges the model to start with SQL)

    The CANNOT_ANSWER sentinel is important: without it, the model will
    hallucinate table names it doesn't know. Giving it an explicit
    escape hatch produces cleaner failure messages.
    """
    return f"""You are an expert {dialect.upper()} SQL analyst. Given the following data warehouse \
schema context, write a single SQL SELECT query to answer the user's question.

RULES:
- Output ONLY the SQL query — no explanation, no markdown, no code fences
- Always include LIMIT {MAX_ROWS} unless the question asks for all rows
- Use exact table and column names as shown in the schema — do not invent names
- Use {dialect.upper()}-compatible syntax (e.g. DATE_TRUNC for date truncation)
- If the question cannot be answered from the available schema, output exactly: CANNOT_ANSWER

SCHEMA CONTEXT:
{schema_context}

USER QUESTION:
{question}

SQL QUERY:"""


def _build_format_prompt(question: str, sql: str, results: list[dict]) -> str:
    """
    Build the prompt for turning raw query results into a plain-English answer.

    We include:
      - The original question (so the model knows what was being asked)
      - The SQL that was run (so it can reference column names correctly)
      - The first 20 result rows (capped to avoid token overflow)

    The 150-word limit keeps answers focused. Without it, LLMs tend to
    pad with generic observations. Short, precise answers are more useful
    in a real business context.
    """
    # Cap rows sent to the LLM — full results are returned in the API response,
    # but we only need a representative sample for natural language summarisation
    preview = results[:20]

    return f"""You are a data analyst writing a concise summary for a business stakeholder.
Given the question, the SQL query run, and the results, write a direct answer.

RULES:
- Lead with the direct answer to the question
- Include specific numbers from the results
- Add one analytical observation if the data clearly supports it
- Keep it under 150 words
- Do not repeat the SQL query in your response
- If results are empty, say so clearly and suggest why

QUESTION: {question}

SQL QUERY USED: {sql}

QUERY RESULTS (first 20 rows):
{preview}

ANSWER:"""


def generate_sql(question: str, schema_context: str, dialect: str = "snowflake") -> str:
    """
    Call GPT-4o to generate a SQL query for the question.

    Args:
        question:       The user's natural language question.
        schema_context: Relevant schema chunks from ChromaDB (the "grounding").
        dialect:        "snowflake" for production, "duckdb" for mock mode.
                        Minor syntax differences (e.g. DATEADD vs interval arithmetic).

    Returns:
        A SQL string ready for validation, or "CANNOT_ANSWER" if the model
        cannot map the question to the available schema.
    """
    prompt = _build_sql_prompt(question, schema_context, dialect)

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,    # must be deterministic for SQL
        max_tokens=512,   # SQL queries are short — cap to control cost
    )

    sql = response.choices[0].message.content.strip()

    # Strip markdown code fences if the model added them despite instructions.
    # Models sometimes add ```sql ... ``` regardless of what you tell them.
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1]).strip()

    return sql


def format_answer(question: str, sql: str, results: list[dict]) -> str:
    """
    Call GPT-4o to turn raw query results into a readable answer.

    Args:
        question: The original natural language question.
        sql:      The SQL that was executed.
        results:  Query result rows — each is a dict of column → value.

    Returns:
        A concise natural language answer (under ~150 words).
    """
    if not results:
        return (
            "The query returned no results. "
            "This may mean the data doesn't exist for the specified time range, "
            "or the filter conditions matched nothing in the current dataset."
        )

    prompt = _build_format_prompt(question, sql, results)

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=MAX_TOKENS_RESPONSE,
    )

    return response.choices[0].message.content.strip()
