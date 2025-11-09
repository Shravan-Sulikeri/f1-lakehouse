# F1 Lakehouse (Fast-F1) – External-SSD Native Architecture

![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Poetry](https://img.shields.io/badge/Poetry-1.8.3-60A5FA?logo=poetry&logoColor=white)
![FastF1](https://img.shields.io/badge/FastF1-Data-orange)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.1.3-FFF000?logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-1.39-FF4B4B?logo=streamlit&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-multi--service-2496ED?logo=docker&logoColor=white)

Fully containerized Formula 1 “bronze → silver → gold” lakehouse built on an external SSD so no race data touches the host disk. Ingestion pulls telemetry with Fast-F1, dbt + DuckDB model the refined layers, and a Streamlit dashboard reads directly from the warehouse.

## Highlights
- **External-first storage**: every service mounts `/Volumes/SAMSUNG/f1-lakehouse-data`, ensuring reproducible host-independent state.
- **Poetry-driven ingestion**: Fast-F1 pulls laps, weather, and results into bronze Parquet partitions by season/round/session.
- **DuckDB + dbt**: hive-partitioned bronze feeds Silver tables and Gold analytics marts (driver and team summaries).
- **Quality gates**: `scripts/check_gold.sh` rebuilds models and inspects row counts/samples directly in DuckDB.
- **Dashboard ready**: Streamlit container reads the warehouse, auto-detects schema prefixes (`silver` vs `main_silver`) and surfaces KPIs plus fastest laps.
- **AI Copilot**: FastAPI service + Ollama (llama3.2:3b) translate natural language into read-only DuckDB SQL, surfaced in Streamlit as an “AI Copilot” panel.

## Repository Layout
```
f1-lakehouse/
├── docker-compose.yml
├── Makefile
├── scripts/
│   ├── init_external.sh         # provisions /Volumes/SAMSUNG/f1-lakehouse-data/*
│   └── check_gold.sh            # dbt build + DuckDB verification
├── ingestion/
│   ├── pyproject.toml           # Poetry metadata + Fast-F1 deps
│   └── src/ingestion/           # utils + fastf1_ingest module
├── transform/
│   ├── dbt_project.yml
│   ├── profiles/                # DuckDB profile -> /opt/data/warehouse/f1.duckdb
│   └── models/
│       ├── silver/              # bronze → silver hive readers
│       └── gold/                # driver_session_summary, team_event_summary
├── ai/
│   └── rag_api/                 # FastAPI + Ollama SQL copilot
├── dashboard/
│   ├── app.py                   # Streamlit explorer
│   └── requirements.txt
└── docker/
    ├── ingestion/               # Poetry-enabled Fast-F1 image
    ├── dbt/                     # dbt-duckdb image
    ├── dashboard/               # Streamlit image
    └── ai/                      # AI copilot image
```

## External SSD Storage
All persistent data lives on `/Volumes/SAMSUNG/f1-lakehouse-data` (mounted into containers as `${EXTERNAL_DATA_ROOT}:/opt/data`). Initialize it once per machine:

```bash
cp .env.example .env          # define EXTERNAL_DATA_ROOT + container paths
bash scripts/init_external.sh # creates bronze/silver/gold/cache/warehouse/ollama
```

The script verifies write access by touching `/opt/data/cache` before continuing and ensures an `ollama/` directory exists for local model weights.

## Working with the Data Lake

### 1. Bronze Ingestion (Fast-F1 → Parquet)
```bash
docker compose build ingestion
docker compose run --rm ingestion
# or use the Makefile helper
make gold      # builds dbt image as well (ingestion runs separately)
```
The ingestion module loads schedules for `F1_SEASONS`, caches Fast-F1 responses in `/opt/data/cache`, and materializes bronze partitions such as `.../bronze/laps/season=2024/round=01/...`.

### 2. Silver & Gold Modeling (dbt + DuckDB)
```bash
docker compose build dbt
docker compose run --rm dbt
# or run both model + quality checks
make verify-gold
```
- **Silver** models read hive-partitioned Parquet directly (`read_parquet('/opt/data/bronze/...', hive_partitioning=1)`).
- **Gold** models produce driver- and team-level aggregates stored in `gold.driver_session_summary` and `gold.team_event_summary`.
- Schemas automatically resolve to `silver`/`gold` or `main_silver`/`main_gold` depending on DuckDB defaults.

### 3. Analytics Dashboard (Streamlit)
```bash
docker compose build dashboard
docker compose up dashboard
```
Visit `http://localhost:8501` to:
- Filter by season/session (sidebar).
- Monitor lap counts, driver counts, and team coverage.
- Inspect top 50 fastest laps with formatted timing strings.
- Review team-level summaries for qualifying/sprint/race sessions.
- Use the **AI Copilot** panel to ask natural-language questions; the backend converts them into safe DuckDB SQL and suggests a chart.

### 4. AI Copilot (Ollama + FastAPI)
```bash
# optionally, pull the model once (inside the ollama container)
docker compose up -d ollama
docker compose exec ollama ollama pull llama3.2:3b

# build + run the copilot API
docker compose build ai
docker compose up ai
```
- The `ollama` service stores its model cache under `${EXTERNAL_DATA_ROOT}/ollama`.
- The `ai` service connects to DuckDB in read-only mode, calls Ollama (`OLLAMA_HOST`, `OLLAMA_MODEL`), validates the generated SQL, enforces LIMITs, and returns rows plus a chart suggestion.
- The Streamlit dashboard calls `AI_SERVICE_URL` to display answers, the generated SQL, and a plotly visualization when applicable.

## Verification & Quality
- `scripts/check_gold.sh` runs `dbt deps && dbt build`, prints row counts for silver/gold tables, and samples the five latest best laps directly from `/opt/data/warehouse/f1.duckdb`.
- CI-friendly make targets:
  - `make gold` – rebuild dbt image and run dbt build.
  - `make verify-gold` – execute the verification script (dbt build + DuckDB checks).

## Service Inventory
| Service      | Image Source            | Responsibilities                                                        |
|--------------|------------------------|-------------------------------------------------------------------------|
| `ingestion`  | `docker/ingestion`     | Fast-F1 → Parquet bronze ingestion (Poetry, Fast-F1 cache on SSD).      |
| `dbt`        | `docker/dbt`           | Transform bronze → silver/gold inside DuckDB.                           |
| `dashboard`  | `docker/dashboard`     | Streamlit UI + AI Copilot front-end.                                   |
| `ai`         | `docker/ai`            | FastAPI service calling Ollama to translate NL → safe DuckDB SQL.      |
| `ollama`     | `ollama/ollama`        | Hosts local models with cache bound to `${EXTERNAL_DATA_ROOT}/ollama`. |

Every service mounts:
```
- ${EXTERNAL_DATA_ROOT}:/opt/data        # shared SSD
- ./<service>:/opt/<service>             # live source for hot reloads
```

## Next Steps
- Automate ingestion/dbt schedules (GitHub Actions runner mounted to the SSD).
- Add more dbt tests/metrics (freshness, exposures, contracts).
- Expand the AI surface area (e.g., incorporate weather joins, telemetry plotting).

---  
Questions or ideas? Open an issue or start a discussion—this repo is ready for collaborative Fast-F1 analytics.  
