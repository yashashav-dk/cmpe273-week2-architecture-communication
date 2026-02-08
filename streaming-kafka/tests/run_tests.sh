#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "==> Starting services (Kafka may take a while)..."
docker compose up -d --build

echo "==> Waiting for services to be healthy..."
for i in $(seq 1 90); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1 && \
       curl -sf http://localhost:8001/health > /dev/null 2>&1 && \
       curl -sf http://localhost:8002/health > /dev/null 2>&1; then
        echo "    Services ready."
        break
    fi
    echo "    Waiting... ($i)"
    sleep 3
done

# Extra wait for Kafka consumers to subscribe
sleep 15

echo "==> Running tests..."
cd tests
pip install -q -r requirements.txt
pytest test_streaming.py -v -s

echo "==> Tearing down..."
cd ..
docker compose down
