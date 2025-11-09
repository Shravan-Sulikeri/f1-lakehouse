FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential tzdata && \
    rm -rf /var/lib/apt/lists/*

# install poetry
RUN pip install --no-cache-dir poetry==1.8.3

WORKDIR /opt/ingestion

# ⬇️ copy ONLY pyproject; do NOT copy poetry.lock
COPY ingestion/pyproject.toml ./

# install deps (Poetry will resolve and create a lock)
RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-root --with dev

# copy source last
COPY ingestion /opt/ingestion
