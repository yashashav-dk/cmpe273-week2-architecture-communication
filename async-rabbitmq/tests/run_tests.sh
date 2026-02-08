#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "==> Starting services..."
docker compose up -d --build

echo "==> Waiting for services to be healthy..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1 && \
       curl -sf http://localhost:8001/health > /dev/null 2>&1; then
        echo "    Services ready."
        break
    fi
    echo "    Waiting... ($i)"
    sleep 2
done

echo "==> Running tests..."
cd tests
pip install -q -r requirements.txt
pytest test_async.py -v -s

echo "==> Tearing down..."
cd ..
docker compose down
