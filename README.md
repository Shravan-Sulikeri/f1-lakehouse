# F1 Lakehouse (Fast-F1) – Bronze/Silver/Gold

## External SSD Storage
All lakehouse data lives on `/Volumes/SAMSUNG/f1-lakehouse-data`. Copy the environment template if you do not have a local `.env`, then run the initialization script to provision the directory tree:

```bash
cp .env.example .env
bash scripts/init_external.sh
```

This host path will be mapped into the containers in the next step.

## Bronze ingestion (first run)
Build and execute the ingestion job once the external SSD is initialized:

```bash
docker compose build ingestion
docker compose run --rm ingestion
```
