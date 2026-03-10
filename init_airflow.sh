#!/usr/bin/env bash
set -e

echo "🔧 Running Airflow DB migrations..."
docker-compose run --rm airflow-webserver db migrate

echo "👤 Creating Airflow admin user (if not already exists)..."
docker-compose run --rm airflow-webserver users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

echo "⬆️ Starting Airflow services..."
docker-compose up -d
