FROM python:3.11-slim
# core deps + duckdb + numpy/pandas/pyarrow for verification scripts
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 dbt-duckdb==1.8.2 duckdb==1.1.3 \
    numpy pandas pyarrow
WORKDIR /opt/transform
COPY transform /opt/transform
