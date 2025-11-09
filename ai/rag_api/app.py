from __future__ import annotations

import json
import os
import textwrap
from typing import Dict, List, Optional

import duckdb
import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


WAREHOUSE = os.getenv("F1_WAREHOUSE", "/opt/data/warehouse/f1.duckdb")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
AI_MAX_ROWS = int(os.getenv("AI_MAX_ROWS", "200"))

OPENAI_STYLE_SYSTEM_PROMPT = """You are an analytics SQL copilot for a Formula 1 DuckDB warehouse.
You must ONLY produce read-only DuckDB SQL queries against the provided schemas.
Rules:
- Use fully qualified names with the resolved schemas.
- Never mutate data (no INSERT/UPDATE/DELETE/CREATE/ALTER/DROP).
- Limit every result set to at most {max_rows} rows.
- If the user requests unsupported data, explain gracefully.
- Respond strictly in compact JSON: {{"sql": "...", "chart_type": "table|line|bar|scatter", "chart_fields": {{"x": "...", "y": "..."}}, "justification": "..."}}.
""".format(
  max_rows=AI_MAX_ROWS
)


class AskRequest(BaseModel):
  question: str = Field(..., min_length=3, description="Natural language request about the warehouse")
  season: Optional[int] = Field(None, description="Optional season hint")
  session_code: Optional[str] = Field(None, description="Optional session hint such as R, Q, FP1")


class AskResponse(BaseModel):
  sql: str
  rows: List[Dict[str, object]]
  columns: List[str]
  row_count: int
  chart: Dict[str, object]
  message: str
  silver_schema: str
  gold_schema: str


app = FastAPI(title="F1 AI Copilot", version="0.1.0")


@app.get("/healthz")
def healthcheck() -> Dict[str, str]:
  return {"status": "ok", "warehouse": WAREHOUSE}


def _require_warehouse() -> None:
  if not os.path.exists(WAREHOUSE):
    raise HTTPException(status_code=500, detail=f"Warehouse not found at {WAREHOUSE}")


def resolve_schema(con: duckdb.DuckDBPyConnection, base_name: str) -> Optional[str]:
  for schema in (f"main_{base_name}", base_name):
    try:
      exists = con.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = ? LIMIT 1",
        [schema],
      ).fetchone()
      if exists:
        return schema
    except Exception:
      continue
  return None


def schema_overview(con: duckdb.DuckDBPyConnection, schema: str) -> str:
  rows = con.execute(
    """
    SELECT table_name,
           string_agg(column_name || ' ' || data_type, ', ') AS cols
    FROM information_schema.columns
    WHERE table_schema = ?
    GROUP BY 1
    ORDER BY 1
    """,
    [schema],
  ).fetchall()
  parts = [f"{schema}.{table}({cols})" for table, cols in rows]
  return "\n".join(parts)


def build_prompt(question: str, schema_doc: str, request: AskRequest, silver_schema: str, gold_schema: str) -> str:
  filters = []
  if request.season:
    filters.append(f"Season hint: {request.season}")
  if request.session_code:
    filters.append(f"Session hint: {request.session_code}")
  filter_block = ("Hints:\n" + "\n".join(filters)) if filters else "Hints: none provided."
  table_guidance = f"""
Important modeling guidance:
- Use {gold_schema}.driver_session_summary for driver-level metrics (driver, team, best_lap_time, laps_total, personal_best_laps).
- Use {gold_schema}.team_event_summary for team-level metrics (team_laps_on_track, team_pitstops, team_best_lap_time).
- Use {silver_schema}.laps for raw lap telemetry (lap_time, lapnumber, driver, driver_number, team, pit_in_time, pit_out_time, sector1time..3time).
Do NOT reference columns that are not listed in the schema dump below.
Always prefer the gold tables when best_lap_time or aggregated insights are requested.
"""
  prompt = f"""
{OPENAI_STYLE_SYSTEM_PROMPT}

{table_guidance}

Schemas:
{schema_doc}

{filter_block}

Question:
{question}

Respond strictly with JSON (no markdown) containing sql, chart_type, chart_fields, and justification.
"""
  return textwrap.dedent(prompt).strip()


def call_ollama(prompt: str) -> Dict[str, object]:
  url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
  payload = {
    "model": OLLAMA_MODEL,
    "messages": [
      {"role": "system", "content": OPENAI_STYLE_SYSTEM_PROMPT},
      {"role": "user", "content": prompt},
    ],
    "stream": False,
    "options": {"temperature": 0.1},
  }
  try:
    with httpx.Client(timeout=90.0) as client:
      response = client.post(url, json=payload)
      response.raise_for_status()
      data = response.json()
  except httpx.RequestError as exc:
    raise HTTPException(status_code=502, detail=f"Ollama connection failed: {exc}") from exc
  except httpx.HTTPStatusError as exc:
    raise HTTPException(status_code=502, detail=f"Ollama error: {exc.response.text}") from exc

  message = data.get("message", {})
  content = message.get("content") if isinstance(message, dict) else data.get("response")
  if not content:
    raise HTTPException(status_code=500, detail="Ollama returned no content.")
  return parse_ai_response(content)


def parse_ai_response(content: str) -> Dict[str, object]:
  cleaned = content.strip()
  if cleaned.startswith("```"):
    cleaned = cleaned.split("\n", 1)[1]
    if "```" in cleaned:
      cleaned = cleaned.split("```", 1)[0]
  try:
    parsed = json.loads(cleaned)
  except json.JSONDecodeError as exc:
    raise HTTPException(status_code=500, detail=f"AI response was not valid JSON: {exc}") from exc
  return parsed


def ensure_safe_sql(sql: str) -> str:
  if not sql:
    raise HTTPException(status_code=400, detail="AI did not provide SQL.")
  stmt = sql.strip().rstrip(";")
  lower = stmt.lower()
  forbidden = ("insert", "update", "delete", "drop", "alter", "create", "truncate")
  if not lower.startswith("select"):
    raise HTTPException(status_code=400, detail="Only SELECT statements are allowed.")
  if any(keyword in lower for keyword in forbidden):
    raise HTTPException(status_code=400, detail="Statement appears to modify data; rejecting.")
  return f"SELECT * FROM ({stmt}) AS safe_view LIMIT {AI_MAX_ROWS}"


def execute_sql(sql: str) -> pd.DataFrame:
  con = duckdb.connect(WAREHOUSE, read_only=True)
  try:
    return con.execute(sql).df()
  except duckdb.Error as exc:  # DuckDB binder/execution errors
    raise HTTPException(status_code=400, detail=f"DuckDB failed to execute the AI-generated SQL: {exc}") from exc
  finally:
    con.close()


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest) -> AskResponse:
  _require_warehouse()
  with duckdb.connect(WAREHOUSE, read_only=True) as con:
    silver_schema = resolve_schema(con, "silver")
    gold_schema = resolve_schema(con, "gold")
    if not silver_schema or not gold_schema:
      raise HTTPException(status_code=400, detail="Silver/Gold schemas not found. Run dbt build first.")
    schema_doc = "\n".join(
      part for part in [
        schema_overview(con, silver_schema),
        schema_overview(con, gold_schema),
      ]
      if part
    )
  prompt = build_prompt(request.question, schema_doc, request, silver_schema, gold_schema)
  ai_payload = call_ollama(prompt)
  safe_sql = ensure_safe_sql(ai_payload.get("sql", ""))
  df = execute_sql(safe_sql)

  chart = {
    "type": ai_payload.get("chart_type", "table"),
    "fields": ai_payload.get("chart_fields") or {},
  }
  message = ai_payload.get("justification") or "Query executed successfully."

  return AskResponse(
    sql=safe_sql,
    rows=df.to_dict(orient="records"),
    columns=list(df.columns),
    row_count=len(df),
    chart=chart,
    message=message,
    silver_schema=silver_schema,
    gold_schema=gold_schema,
  )
