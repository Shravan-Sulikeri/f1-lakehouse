import os, re
from typing import Optional, Literal
import duckdb, httpx, pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

WAREHOUSE = os.getenv("F1_WAREHOUSE", "/opt/data/warehouse/f1.duckdb")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:3b")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")

app = FastAPI(title="F1 AI Copilot", version="0.1.0")

# ---------- DB helpers ----------
def connect_ro():
    # DuckDB needs write access for temp ops; we still enforce SELECT-only at SQL level
    return duckdb.connect(WAREHOUSE, read_only=False)

def detect_schema_prefix(con: duckdb.DuckDBPyConnection) -> tuple[str,str]:
    for s,g in (("main_silver","main_gold"), ("silver","gold")):
        try:
            con.execute(f"SELECT 1 FROM information_schema.tables WHERE table_schema='{s}' AND table_name='laps' LIMIT 1").fetchone()
            con.execute(f"SELECT 1 FROM information_schema.tables WHERE table_schema='{g}' AND table_name='driver_session_summary' LIMIT 1").fetchone()
            return s,g
        except Exception:
            pass
    return "main_silver","main_gold"

def schema_snapshot(con: duckdb.DuckDBPyConnection) -> str:
    s,g = detect_schema_prefix(con)
    q = f"""
      select table_schema, table_name, column_name, data_type
      from information_schema.columns
      where table_schema in ('{s}','{g}')
      order by table_schema, table_name, ordinal_position
    """
    df = con.sql(q).df()
    lines = []
    for (sch, tbl), grp in df.groupby(["table_schema","table_name"]):
        cols = ", ".join(f"{r['column_name']}:{r['data_type']}" for _,r in grp.iterrows())
        lines.append(f"{sch}.{tbl}({cols})")
    return "\n".join(lines)

# ---------- LLM helpers ----------
def pull_model_if_needed():
    try:
        with httpx.Client(timeout=None) as cli:
            tags = cli.get(f"{OLLAMA_URL}/api/tags").json()
            names = [m.get("name","") for m in tags.get("models",[])]
            if LLM_MODEL not in names:
                cli.post(f"{OLLAMA_URL}/api/pull", json={"name": LLM_MODEL})
    except Exception:
        # best-effort; health will show if it's missing
        pass

def make_system_prompt(schema_text: str, limit_rows: int = 200) -> str:
    return f"""
You are a senior analytics engineer. Convert the user's question into a SINGLE DuckDB SQL SELECT query.

RULES:
- READ-ONLY. Only SELECT (no DDL/DML).
- Use only the schemas shown below. If both main_* and plain exist, prefer the main_* ones.
- Prefer gold.driver_session_summary and gold.team_event_summary when possible.
- Times are stored as nanoseconds (ints). If human-readable display is needed, return raw ns; client will format.
- Always LIMIT results to <= {limit_rows} rows unless the user asks for a single value.
- Return ONLY the SQL between a ```sql fence.

SCHEMA:
{schema_text}
""".strip()

def call_ollama(prompt: str) -> str:
    with httpx.Client(timeout=None) as cli:
        r = cli.post(f"{OLLAMA_URL}/api/generate",
                     json={"model": LLM_MODEL, "prompt": prompt, "stream": False})
        if r.status_code != 200:
            raise HTTPException(500, f"Ollama error: {r.text}")
        return r.json().get("response","")

def extract_sql(text: str) -> str:
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.S|re.I)
    sql = (m.group(1) if m else text).strip().rstrip(";")
    if re.search(r"\b(insert|update|delete|drop|alter|create)\b", sql, flags=re.I):
        raise HTTPException(400, "Only SELECT queries are allowed.")
    if not re.match(r"^\s*select\b", sql, flags=re.I):
        raise HTTPException(400, "Model did not return a SELECT query.")
    if not re.search(r"\blimit\b", sql, flags=re.I):
        sql += " LIMIT 200"
    return sql

def suggest_chart(cols: list[str], df: pd.DataFrame) -> str:
    numeric = df.select_dtypes(include=["number"]).columns.tolist()
    if len(numeric) >= 1 and any(k in [c.lower() for c in cols] for k in ["lapnumber","round","season"]):
        return "line"
    if len(numeric) >= 1 and df.shape[0] <= 25:
        return "bar"
    return "table"

# ---------- API models ----------
class AskIn(BaseModel):
    question: str
    limit: Optional[int] = 200

class AskOut(BaseModel):
    sql: str
    chart: Literal["table","bar","line","auto"] = "auto"
    rows: list[list]
    columns: list[str]

# ---------- Routes ----------
@app.on_event("startup")
def _startup():
    pull_model_if_needed()

@app.get("/")
def root():
    return {"ok": True, "service": "f1-ai", "model": LLM_MODEL, "warehouse": WAREHOUSE}

@app.get("/health")
def health():
    ok_db = os.path.exists(WAREHOUSE)
    try:
        with connect_ro() as con:
            con.sql("select 1").fetchone()
        ok_sql = True
    except Exception:
        ok_sql = False
    ok_ollama = False
    try:
        with httpx.Client(timeout=30) as cli:
            r = cli.get(f"{OLLAMA_URL}/api/tags")
            ok_ollama = r.status_code == 200
    except Exception:
        pass
    return {"warehouse": WAREHOUSE, "db_ok": ok_db and ok_sql, "ollama_ok": ok_ollama, "model": LLM_MODEL}

@app.post("/ask", response_model=AskOut)
def ask(payload: AskIn):
    with connect_ro() as con:
        s,g = detect_schema_prefix(con)
        snapshot = schema_snapshot(con)
        sys = make_system_prompt(snapshot, limit_rows=payload.limit or 200)
        user = f"Question: {payload.question}\nUse schemas actually present: silver={s}, gold={g}."
        raw = call_ollama(sys + "\n\n" + user)
        sql = extract_sql(raw)
        # rewrite schema names if the model returned plain 'silver.' / 'gold.'
        sql = re.sub(r"\bsilver\.", f"{s}.", sql)
        sql = re.sub(r"\bgold\.",   f"{g}.", sql)
        df = con.sql(sql).df()
        return AskOut(sql=sql, chart=suggest_chart(df.columns.tolist(), df),
                      rows=df.values.tolist(), columns=df.columns.tolist())
