FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dashboard

COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard /opt/dashboard

EXPOSE 8501

CMD ["bash","-lc","streamlit run app.py --server.address 0.0.0.0 --server.port 8501"]
