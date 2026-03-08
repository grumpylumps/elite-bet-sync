#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting Docker compose..."
docker-compose up -d

echo "Waiting for Postgres to be ready..."
for i in {1..15}; do
  if docker exec $(docker-compose ps -q db) pg_isready -U postgres -d elite_bet_sync >/dev/null 2>&1; then
    echo "Postgres is ready"
    break
  fi
  sleep 1
done

echo "Running migrations..."
npm run migrate

echo "Running smoke tests..."
npm run test:smoke

echo "Tearing down docker-compose..."
docker-compose down

echo "Done."