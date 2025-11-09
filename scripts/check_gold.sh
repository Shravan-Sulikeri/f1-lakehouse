#!/usr/bin/env bash
set -euo pipefail
echo "== Running dbt build for Silver + Gold =="
docker compose run --rm dbt bash -lc "dbt deps && dbt build"
echo "== Verifying DuckDB tables on external SSD =="
docker compose run --rm dbt bash -lc "python - <<'PY'
import duckdb
con = duckdb.connect('/opt/data/warehouse/f1.duckdb')
def cnt(t):
    try: return con.sql(f'select count(*) as cnt from {t}').df()
    except Exception as e: return f'missing: {e}'
tables = [
  'main_silver.laps','main_silver.weather','main_silver.results',
  'main_gold.driver_session_summary','main_gold.team_event_summary'
]
for t in tables:
    print(t, cnt(t))
print('\\nSample fastest laps:')
print(con.sql(\"\"\"select season, round, grand_prix, session_code, driver, best_lap_time
                from main_gold.driver_session_summary
                order by season desc, round desc, best_lap_time
                limit 5\"\"\").df())
PY"
echo "✅ Gold verification complete"
