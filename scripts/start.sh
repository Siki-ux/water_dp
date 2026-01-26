#!/bin/sh
set -e

echo "Running database migrations..."
python scripts/run_migrations.py upgrade

echo "Running Keycloak setup..."
python scripts/setup_keycloak.py

echo "Starting application..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
