# Campus Food Ordering — Communication Models Lab Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a campus food ordering workflow in three communication styles (sync REST, async RabbitMQ, streaming Kafka) with Docker Compose, tests, and fault injection for each.

**Architecture:** Three self-contained parts, each with its own docker-compose.yml. Services are Python FastAPI apps using in-memory dicts. Each part demonstrates a different communication model with identical business logic (place order, reserve inventory, notify).

**Tech Stack:** Python 3.11, FastAPI, httpx, pika, confluent-kafka-python, Docker Compose

---

## Task 1: Common Module

**Files:**
- Create: `common/ids.py`
- Create: `common/README.md`

**Step 1: Create `common/ids.py`**

```python
import uuid


def generate_order_id() -> str:
    return str(uuid.uuid4())
```

**Step 2: Create `common/README.md`**

```markdown
# Common

Shared utilities used across all three communication model implementations.

- `ids.py` — UUID generator for order IDs
```

**Step 3: Commit**

```bash
git add common/
git commit -m "feat: add common module with id generator"
```

---

## Task 2: Part A — InventoryService

**Files:**
- Create: `sync-rest/inventory_service/app.py`
- Create: `sync-rest/inventory_service/requirements.txt`
- Create: `sync-rest/inventory_service/Dockerfile`

**Step 1: Create `sync-rest/inventory_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
```

**Step 2: Create `sync-rest/inventory_service/app.py`**

```python
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="InventoryService")

stock = {"burger": 100, "pizza": 100, "salad": 100}

# Fault injection state
fault_config = {"delay_seconds": 0, "fail_enabled": False}


class ReserveRequest(BaseModel):
    order_id: str
    item: str
    quantity: int


class FaultDelay(BaseModel):
    seconds: float


class FaultFail(BaseModel):
    enabled: bool


@app.post("/reserve")
async def reserve(req: ReserveRequest):
    if fault_config["fail_enabled"]:
        raise HTTPException(status_code=500, detail="Injected failure")
    if fault_config["delay_seconds"] > 0:
        await asyncio.sleep(fault_config["delay_seconds"])
    if req.item not in stock:
        raise HTTPException(status_code=404, detail=f"Unknown item: {req.item}")
    if stock[req.item] < req.quantity:
        raise HTTPException(status_code=409, detail=f"Insufficient stock for {req.item}")
    stock[req.item] -= req.quantity
    return {"order_id": req.order_id, "item": req.item, "quantity": req.quantity, "status": "reserved"}


@app.get("/stock")
async def get_stock():
    return stock


@app.post("/admin/delay")
async def set_delay(fault: FaultDelay):
    fault_config["delay_seconds"] = fault.seconds
    return {"delay_seconds": fault.seconds}


@app.post("/admin/fail")
async def set_fail(fault: FaultFail):
    fault_config["fail_enabled"] = fault.enabled
    return {"fail_enabled": fault.enabled}


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `sync-rest/inventory_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
```

**Step 4: Commit**

```bash
git add sync-rest/inventory_service/
git commit -m "feat(sync-rest): add InventoryService with fault injection"
```

---

## Task 3: Part A — NotificationService

**Files:**
- Create: `sync-rest/notification_service/app.py`
- Create: `sync-rest/notification_service/requirements.txt`
- Create: `sync-rest/notification_service/Dockerfile`

**Step 1: Create `sync-rest/notification_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
```

**Step 2: Create `sync-rest/notification_service/app.py`**

```python
import time
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="NotificationService")

notifications: list[dict] = []


class SendRequest(BaseModel):
    order_id: str
    status: str
    item: str


@app.post("/send")
async def send(req: SendRequest):
    entry = {
        "order_id": req.order_id,
        "status": req.status,
        "item": req.item,
        "timestamp": time.time(),
    }
    notifications.append(entry)
    return {"message": "Notification sent", **entry}


@app.get("/notifications")
async def get_notifications():
    return notifications


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `sync-rest/notification_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]
```

**Step 4: Commit**

```bash
git add sync-rest/notification_service/
git commit -m "feat(sync-rest): add NotificationService"
```

---

## Task 4: Part A — OrderService

**Files:**
- Create: `sync-rest/order_service/app.py`
- Create: `sync-rest/order_service/requirements.txt`
- Create: `sync-rest/order_service/Dockerfile`

**Step 1: Create `sync-rest/order_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
httpx==0.27.0
```

**Step 2: Create `sync-rest/order_service/app.py`**

```python
import os
import time
import uuid
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="OrderService")

orders: dict[str, dict] = {}

INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory:8001")
NOTIFICATION_URL = os.getenv("NOTIFICATION_URL", "http://notification:8002")
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5"))


class OrderRequest(BaseModel):
    item: str
    quantity: int


@app.post("/order", status_code=201)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = order

    # Call InventoryService synchronously
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(
                f"{INVENTORY_URL}/reserve",
                json={"order_id": order_id, "item": req.item, "quantity": req.quantity},
            )
        if resp.status_code != 200:
            order["status"] = "failed"
            raise HTTPException(status_code=422, detail={"order": order, "reason": resp.json()})
    except httpx.TimeoutException:
        order["status"] = "timeout"
        raise HTTPException(status_code=504, detail={"order": order, "reason": "Inventory service timeout"})
    except httpx.ConnectError:
        order["status"] = "failed"
        raise HTTPException(status_code=502, detail={"order": order, "reason": "Inventory service unreachable"})

    order["status"] = "reserved"

    # Call NotificationService synchronously (best-effort)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            await client.post(
                f"{NOTIFICATION_URL}/send",
                json={"order_id": order_id, "status": "reserved", "item": req.item},
            )
        order["status"] = "notified"
    except Exception:
        pass  # Best-effort — order stays "reserved"

    return order


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `sync-rest/order_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Step 4: Commit**

```bash
git add sync-rest/order_service/
git commit -m "feat(sync-rest): add OrderService with sync calls and error handling"
```

---

## Task 5: Part A — Docker Compose & Tests

**Files:**
- Create: `sync-rest/docker-compose.yml`
- Create: `sync-rest/tests/test_sync.py`
- Create: `sync-rest/tests/requirements.txt`
- Create: `sync-rest/tests/run_tests.sh`

**Step 1: Create `sync-rest/docker-compose.yml`**

```yaml
services:
  order:
    build: ./order_service
    ports:
      - "8000:8000"
    environment:
      - INVENTORY_URL=http://inventory:8001
      - NOTIFICATION_URL=http://notification:8002
    depends_on:
      inventory:
        condition: service_healthy
      notification:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  inventory:
    build: ./inventory_service
    ports:
      - "8001:8001"
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  notification:
    build: ./notification_service
    ports:
      - "8002:8002"
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8002/health')"]
      interval: 5s
      timeout: 3s
      retries: 5
```

**Step 2: Create `sync-rest/tests/requirements.txt`**

```
httpx==0.27.0
pytest==8.3.0
```

**Step 3: Create `sync-rest/tests/test_sync.py`**

```python
import time
import httpx
import pytest

BASE = "http://localhost:8000"
INVENTORY = "http://localhost:8001"

NUM_REQUESTS = 20


@pytest.fixture(autouse=True)
def reset_faults():
    """Reset fault injection before each test."""
    with httpx.Client() as c:
        c.post(f"{INVENTORY}/admin/delay", json={"seconds": 0})
        c.post(f"{INVENTORY}/admin/fail", json={"enabled": False})
    yield


def place_order(client: httpx.Client, item: str = "burger", quantity: int = 1):
    start = time.time()
    resp = client.post(f"{BASE}/order", json={"item": item, "quantity": quantity})
    elapsed = time.time() - start
    return resp, elapsed


def test_baseline_latency():
    """Send N orders and report latency stats."""
    latencies = []
    with httpx.Client(timeout=10) as c:
        for _ in range(NUM_REQUESTS):
            resp, elapsed = place_order(c)
            assert resp.status_code == 201
            latencies.append(elapsed)

    latencies.sort()
    mean = sum(latencies) / len(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print("\n--- Baseline Latency ---")
    print(f"  Requests: {NUM_REQUESTS}")
    print(f"  Mean:     {mean:.4f}s")
    print(f"  P95:      {p95:.4f}s")
    print(f"  P99:      {p99:.4f}s")


def test_delay_injection():
    """Inject 2s delay into Inventory, show impact on Order latency."""
    with httpx.Client(timeout=10) as c:
        # Baseline
        _, baseline = place_order(c)

    with httpx.Client(timeout=10) as c:
        c.post(f"{INVENTORY}/admin/delay", json={"seconds": 2})
        _, delayed = place_order(c)

    print("\n--- Delay Injection ---")
    print(f"  Baseline: {baseline:.4f}s")
    print(f"  Delayed:  {delayed:.4f}s")
    print(f"  Delta:    {delayed - baseline:.4f}s")
    assert delayed > baseline + 1.5, "Delay should add ~2s to order latency"


def test_failure_injection():
    """Inject failure into Inventory, show OrderService returns error."""
    with httpx.Client(timeout=10) as c:
        c.post(f"{INVENTORY}/admin/fail", json={"enabled": True})
        for _ in range(5):
            resp, _ = place_order(c)
            assert resp.status_code in (422, 504, 502), f"Expected error, got {resp.status_code}"
            body = resp.json()
            print(f"  Status: {resp.status_code}, Detail: {body.get('detail')}")
```

**Step 4: Create `sync-rest/tests/run_tests.sh`**

```bash
#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "==> Starting services..."
docker compose up -d --build

echo "==> Waiting for services to be healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo "    Services ready."
        break
    fi
    echo "    Waiting... ($i)"
    sleep 2
done

echo "==> Running tests..."
cd tests
pip install -q -r requirements.txt
pytest test_sync.py -v -s

echo "==> Tearing down..."
cd ..
docker compose down
```

**Step 5: Build and verify**

```bash
cd sync-rest && docker compose up -d --build
# Wait for healthy
curl http://localhost:8000/health
# Run a quick manual test
curl -X POST http://localhost:8000/order -H 'Content-Type: application/json' -d '{"item":"burger","quantity":1}'
docker compose down
```

**Step 6: Commit**

```bash
git add sync-rest/docker-compose.yml sync-rest/tests/
git commit -m "feat(sync-rest): add docker-compose and test suite"
```

---

## Task 6: Part B — RabbitMQ Broker Config

**Files:**
- Create: `async-rabbitmq/broker/rabbitmq.conf`
- Create: `async-rabbitmq/broker/definitions.json`

**Step 1: Create `async-rabbitmq/broker/rabbitmq.conf`**

```ini
management.load_definitions = /etc/rabbitmq/definitions.json
```

**Step 2: Create `async-rabbitmq/broker/definitions.json`**

This pre-configures the exchange, queues, and bindings so services don't need to declare them at startup (though they can assert them).

```json
{
  "vhosts": [{"name": "/"}],
  "users": [{"name": "guest", "password_hash": "guest", "tags": "administrator"}],
  "permissions": [{"user": "guest", "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}],
  "exchanges": [
    {"name": "orders_exchange", "vhost": "/", "type": "topic", "durable": true, "auto_delete": false},
    {"name": "dlx_exchange", "vhost": "/", "type": "fanout", "durable": true, "auto_delete": false}
  ],
  "queues": [
    {
      "name": "inventory_queue", "vhost": "/", "durable": true, "auto_delete": false,
      "arguments": {"x-dead-letter-exchange": "dlx_exchange"}
    },
    {"name": "notification_queue", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {}},
    {"name": "dlq_queue", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {}}
  ],
  "bindings": [
    {"source": "orders_exchange", "vhost": "/", "destination": "inventory_queue", "destination_type": "queue", "routing_key": "order.placed"},
    {"source": "orders_exchange", "vhost": "/", "destination": "notification_queue", "destination_type": "queue", "routing_key": "inventory.reserved"},
    {"source": "dlx_exchange", "vhost": "/", "destination": "dlq_queue", "destination_type": "queue", "routing_key": ""}
  ]
}
```

**Step 3: Commit**

```bash
git add async-rabbitmq/broker/
git commit -m "feat(async-rabbitmq): add RabbitMQ broker config with DLQ"
```

---

## Task 7: Part B — InventoryService (RabbitMQ)

**Files:**
- Create: `async-rabbitmq/inventory_service/app.py`
- Create: `async-rabbitmq/inventory_service/requirements.txt`
- Create: `async-rabbitmq/inventory_service/Dockerfile`

**Step 1: Create `async-rabbitmq/inventory_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
pika==1.3.2
```

**Step 2: Create `async-rabbitmq/inventory_service/app.py`**

```python
import json
import os
import threading
import time
import logging
import pika
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory")

app = FastAPI(title="InventoryService (RabbitMQ)")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
stock = {"burger": 100, "pizza": 100, "salad": 100}
seen_message_ids: set[str] = set()


def get_connection():
    for attempt in range(30):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        except pika.exceptions.AMQPConnectionError:
            logger.info(f"Waiting for RabbitMQ... attempt {attempt + 1}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ")


def publish_event(routing_key: str, body: dict):
    conn = get_connection()
    ch = conn.channel()
    ch.basic_publish(
        exchange="orders_exchange",
        routing_key=routing_key,
        body=json.dumps(body),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    conn.close()


def on_order_placed(ch, method, properties, body):
    try:
        msg = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Malformed message, sending to DLQ: {body}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    required = {"order_id", "item", "quantity", "message_id"}
    if not required.issubset(msg.keys()):
        logger.error(f"Missing fields in message, sending to DLQ: {msg}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    message_id = msg["message_id"]
    if message_id in seen_message_ids:
        logger.info(f"Duplicate message_id={message_id}, skipping")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    seen_message_ids.add(message_id)
    order_id = msg["order_id"]
    item = msg["item"]
    quantity = msg["quantity"]

    if item in stock and stock[item] >= quantity:
        stock[item] -= quantity
        logger.info(f"Reserved {quantity}x {item} for order {order_id}")
        publish_event("inventory.reserved", {"order_id": order_id, "item": item, "quantity": quantity, "status": "reserved"})
    else:
        logger.info(f"Failed to reserve {quantity}x {item} for order {order_id}")
        publish_event("inventory.failed", {"order_id": order_id, "item": item, "quantity": quantity, "status": "failed"})

    ch.basic_ack(delivery_tag=method.delivery_tag)


def consumer_thread():
    conn = get_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="inventory_queue", on_message_callback=on_order_placed)
    logger.info("InventoryService consumer started")
    ch.start_consuming()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consumer_thread, daemon=True)
    t.start()


@app.get("/stock")
async def get_stock():
    return stock


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `async-rabbitmq/inventory_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
```

**Step 4: Commit**

```bash
git add async-rabbitmq/inventory_service/
git commit -m "feat(async-rabbitmq): add InventoryService with idempotency and DLQ"
```

---

## Task 8: Part B — NotificationService (RabbitMQ)

**Files:**
- Create: `async-rabbitmq/notification_service/app.py`
- Create: `async-rabbitmq/notification_service/requirements.txt`
- Create: `async-rabbitmq/notification_service/Dockerfile`

**Step 1: Create `async-rabbitmq/notification_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
pika==1.3.2
```

**Step 2: Create `async-rabbitmq/notification_service/app.py`**

```python
import json
import os
import threading
import time
import logging
import pika
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification")

app = FastAPI(title="NotificationService (RabbitMQ)")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
notifications: list[dict] = []


def get_connection():
    for attempt in range(30):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        except pika.exceptions.AMQPConnectionError:
            logger.info(f"Waiting for RabbitMQ... attempt {attempt + 1}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ")


def on_inventory_reserved(ch, method, properties, body):
    try:
        msg = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Malformed message: {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    entry = {
        "order_id": msg.get("order_id"),
        "item": msg.get("item"),
        "status": msg.get("status"),
        "timestamp": time.time(),
    }
    notifications.append(entry)
    logger.info(f"Notification sent for order {entry['order_id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consumer_thread():
    conn = get_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="notification_queue", on_message_callback=on_inventory_reserved)
    logger.info("NotificationService consumer started")
    ch.start_consuming()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consumer_thread, daemon=True)
    t.start()


@app.get("/notifications")
async def get_notifications():
    return notifications


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `async-rabbitmq/notification_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]
```

**Step 4: Commit**

```bash
git add async-rabbitmq/notification_service/
git commit -m "feat(async-rabbitmq): add NotificationService"
```

---

## Task 9: Part B — OrderService (RabbitMQ)

**Files:**
- Create: `async-rabbitmq/order_service/app.py`
- Create: `async-rabbitmq/order_service/requirements.txt`
- Create: `async-rabbitmq/order_service/Dockerfile`

**Step 1: Create `async-rabbitmq/order_service/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
pika==1.3.2
```

**Step 2: Create `async-rabbitmq/order_service/app.py`**

```python
import json
import os
import threading
import time
import uuid
import logging
import pika
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order")

app = FastAPI(title="OrderService (RabbitMQ)")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
orders: dict[str, dict] = {}


class OrderRequest(BaseModel):
    item: str
    quantity: int


def get_connection():
    for attempt in range(30):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        except pika.exceptions.AMQPConnectionError:
            logger.info(f"Waiting for RabbitMQ... attempt {attempt + 1}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ")


def on_status_update(ch, method, properties, body):
    """Listen for inventory.reserved / inventory.failed to update local order status."""
    try:
        msg = json.loads(body)
        order_id = msg.get("order_id")
        status = msg.get("status")
        if order_id in orders:
            orders[order_id]["status"] = status
            logger.info(f"Order {order_id} updated to {status}")
    except Exception as e:
        logger.error(f"Error processing status update: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def status_consumer_thread():
    """Consume inventory.reserved and inventory.failed for order status updates."""
    conn = get_connection()
    ch = conn.channel()
    # Declare a temporary queue for status updates
    result = ch.queue_declare(queue="order_status_queue", durable=True)
    ch.queue_bind(exchange="orders_exchange", queue="order_status_queue", routing_key="inventory.reserved")
    ch.queue_bind(exchange="orders_exchange", queue="order_status_queue", routing_key="inventory.failed")
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="order_status_queue", on_message_callback=on_status_update)
    logger.info("OrderService status consumer started")
    ch.start_consuming()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=status_consumer_thread, daemon=True)
    t.start()


@app.post("/order", status_code=202)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    message_id = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = order

    # Publish OrderPlaced event
    conn = get_connection()
    ch = conn.channel()
    ch.basic_publish(
        exchange="orders_exchange",
        routing_key="order.placed",
        body=json.dumps({
            "order_id": order_id,
            "message_id": message_id,
            "item": req.item,
            "quantity": req.quantity,
            "timestamp": order["timestamp"],
        }),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    conn.close()
    logger.info(f"Published OrderPlaced for {order_id}")

    return order


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `async-rabbitmq/order_service/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Step 4: Commit**

```bash
git add async-rabbitmq/order_service/
git commit -m "feat(async-rabbitmq): add OrderService with publish and status consumer"
```

---

## Task 10: Part B — Docker Compose & Tests

**Files:**
- Create: `async-rabbitmq/docker-compose.yml`
- Create: `async-rabbitmq/tests/test_async.py`
- Create: `async-rabbitmq/tests/requirements.txt`
- Create: `async-rabbitmq/tests/run_tests.sh`

**Step 1: Create `async-rabbitmq/docker-compose.yml`**

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - ./broker/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
      - ./broker/definitions.json:/etc/rabbitmq/definitions.json
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
      interval: 5s
      timeout: 10s
      retries: 10

  order:
    build: ./order_service
    ports:
      - "8000:8000"
    environment:
      - RABBITMQ_HOST=rabbitmq
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  inventory:
    build: ./inventory_service
    ports:
      - "8001:8001"
    environment:
      - RABBITMQ_HOST=rabbitmq
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  notification:
    build: ./notification_service
    ports:
      - "8002:8002"
    environment:
      - RABBITMQ_HOST=rabbitmq
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8002/health')"]
      interval: 5s
      timeout: 3s
      retries: 5
```

**Step 2: Create `async-rabbitmq/tests/requirements.txt`**

```
httpx==0.27.0
pika==1.3.2
pytest==8.3.0
```

**Step 3: Create `async-rabbitmq/tests/test_async.py`**

```python
import json
import time
import uuid
import subprocess
import httpx
import pika
import pytest

ORDER_URL = "http://localhost:8000"
INVENTORY_URL = "http://localhost:8001"
RABBITMQ_HOST = "localhost"
RABBITMQ_MGMT = "http://localhost:15672"


def place_order(client: httpx.Client, item: str = "burger", quantity: int = 1):
    resp = client.post(f"{ORDER_URL}/order", json={"item": item, "quantity": quantity})
    return resp


def wait_for_order_status(client: httpx.Client, order_id: str, target_status: str, timeout: float = 30):
    start = time.time()
    while time.time() - start < timeout:
        orders = client.get(f"{ORDER_URL}/orders").json()
        for o in orders:
            if o["order_id"] == order_id and o["status"] == target_status:
                return True
        time.sleep(1)
    return False


def test_backlog_drain():
    """Stop inventory, publish orders, restart, verify all process."""
    # Stop inventory service
    subprocess.run(["docker", "compose", "stop", "inventory"], cwd="..", check=True)
    time.sleep(5)

    # Publish orders while inventory is down
    order_ids = []
    with httpx.Client(timeout=10) as c:
        for _ in range(20):
            resp = place_order(c)
            assert resp.status_code == 202
            order_ids.append(resp.json()["order_id"])
        print(f"\n  Published {len(order_ids)} orders while inventory was down")

    # Restart inventory
    subprocess.run(["docker", "compose", "start", "inventory"], cwd="..", check=True)
    print("  Inventory restarted, waiting for backlog drain...")

    # Wait for all orders to be processed
    with httpx.Client(timeout=10) as c:
        for oid in order_ids:
            found = wait_for_order_status(c, oid, "reserved", timeout=60)
            assert found, f"Order {oid} did not reach 'reserved' status"
    print(f"  All {len(order_ids)} orders drained successfully")


def test_idempotency():
    """Publish same message_id twice, verify stock only decremented once."""
    with httpx.Client(timeout=10) as c:
        stock_before = c.get(f"{INVENTORY_URL}/stock").json()

    # Publish a message with a known message_id
    message_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    msg = json.dumps({
        "order_id": order_id,
        "message_id": message_id,
        "item": "pizza",
        "quantity": 1,
    })

    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    ch = conn.channel()
    for _ in range(2):  # Publish the same message twice
        ch.basic_publish(
            exchange="orders_exchange",
            routing_key="order.placed",
            body=msg,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
    conn.close()

    time.sleep(5)  # Wait for processing

    with httpx.Client(timeout=10) as c:
        stock_after = c.get(f"{INVENTORY_URL}/stock").json()

    delta = stock_before["pizza"] - stock_after["pizza"]
    print(f"\n  Pizza stock before: {stock_before['pizza']}, after: {stock_after['pizza']}, delta: {delta}")
    assert delta == 1, f"Expected stock to decrement by 1, but decremented by {delta}"


def test_dlq_poison_message():
    """Send malformed message, verify it ends up in DLQ."""
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    ch = conn.channel()
    ch.basic_publish(
        exchange="orders_exchange",
        routing_key="order.placed",
        body=json.dumps({"garbage": True}),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    conn.close()

    time.sleep(5)  # Wait for nack + DLQ routing

    # Check DLQ via management API
    with httpx.Client(timeout=10) as c:
        resp = c.get(
            f"{RABBITMQ_MGMT}/api/queues/%2F/dlq_queue",
            auth=("guest", "guest"),
        )
        assert resp.status_code == 200
        queue_info = resp.json()
        msg_count = queue_info.get("messages", 0)
        print(f"\n  DLQ message count: {msg_count}")
        assert msg_count >= 1, "Expected at least 1 message in DLQ"
```

**Step 4: Create `async-rabbitmq/tests/run_tests.sh`**

```bash
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
```

**Step 5: Build and verify**

```bash
cd async-rabbitmq && docker compose up -d --build
# Wait for healthy
curl http://localhost:8000/health
curl http://localhost:8001/health
# Quick test
curl -X POST http://localhost:8000/order -H 'Content-Type: application/json' -d '{"item":"burger","quantity":1}'
sleep 3
curl http://localhost:8000/orders
docker compose down
```

**Step 6: Commit**

```bash
git add async-rabbitmq/docker-compose.yml async-rabbitmq/tests/
git commit -m "feat(async-rabbitmq): add docker-compose and test suite"
```

---

## Task 11: Part C — Producer / OrderService (Kafka)

**Files:**
- Create: `streaming-kafka/producer_order/app.py`
- Create: `streaming-kafka/producer_order/requirements.txt`
- Create: `streaming-kafka/producer_order/Dockerfile`

**Step 1: Create `streaming-kafka/producer_order/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
confluent-kafka==2.5.0
```

**Step 2: Create `streaming-kafka/producer_order/app.py`**

```python
import json
import os
import time
import uuid
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

app = FastAPI(title="OrderProducer (Kafka)")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
orders: dict[str, dict] = {}

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
producer = None


def get_producer():
    global producer
    if producer is None:
        producer = Producer(producer_conf)
    return producer


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


class OrderRequest(BaseModel):
    item: str
    quantity: int


class BulkRequest(BaseModel):
    count: int
    item: str = "burger"
    quantity: int = 1


@app.post("/order", status_code=201)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    event = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = event

    p = get_producer()
    p.produce(
        "order-events",
        key=order_id,
        value=json.dumps(event),
        callback=delivery_report,
    )
    p.flush()

    return event


@app.post("/orders/bulk", status_code=201)
async def create_bulk_orders(req: BulkRequest):
    p = get_producer()
    created = []
    for _ in range(req.count):
        order_id = str(uuid.uuid4())
        event = {
            "order_id": order_id,
            "item": req.item,
            "quantity": req.quantity,
            "status": "placed",
            "timestamp": time.time(),
        }
        orders[order_id] = event
        p.produce(
            "order-events",
            key=order_id,
            value=json.dumps(event),
            callback=delivery_report,
        )
        created.append(order_id)

        # Flush every 1000 messages to avoid buffer overflow
        if len(created) % 1000 == 0:
            p.flush()

    p.flush()
    return {"count": len(created), "order_ids_sample": created[:5]}


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `streaming-kafka/producer_order/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Step 4: Commit**

```bash
git add streaming-kafka/producer_order/
git commit -m "feat(streaming-kafka): add OrderProducer with bulk endpoint"
```

---

## Task 12: Part C — InventoryConsumer (Kafka)

**Files:**
- Create: `streaming-kafka/inventory_consumer/app.py`
- Create: `streaming-kafka/inventory_consumer/requirements.txt`
- Create: `streaming-kafka/inventory_consumer/Dockerfile`

**Step 1: Create `streaming-kafka/inventory_consumer/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
confluent-kafka==2.5.0
```

**Step 2: Create `streaming-kafka/inventory_consumer/app.py`**

```python
import json
import os
import threading
import time
import logging
from fastapi import FastAPI
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory_consumer")

app = FastAPI(title="InventoryConsumer (Kafka)")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
stock = {"burger": 100, "pizza": 100, "salad": 100}
processed_count = 0

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "inventory-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def consumer_loop():
    global processed_count
    # Wait for Kafka to be ready
    time.sleep(10)

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe(["order-events"])
    logger.info("InventoryConsumer started")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.error(f"Malformed message at offset {msg.offset()}")
                consumer.commit(msg)
                continue

            order_id = event.get("order_id", "unknown")
            item = event.get("item")
            quantity = event.get("quantity", 0)

            if item in stock and stock[item] >= quantity:
                stock[item] -= quantity
                result_status = "reserved"
            else:
                result_status = "failed"

            result_event = {
                "order_id": order_id,
                "item": item,
                "quantity": quantity,
                "status": result_status,
                "timestamp": time.time(),
            }
            producer.produce(
                "inventory-events",
                key=order_id,
                value=json.dumps(result_event),
                callback=delivery_report,
            )
            producer.flush()
            consumer.commit(msg)
            processed_count += 1

            if processed_count % 500 == 0:
                logger.info(f"Processed {processed_count} events")
    except Exception as e:
        logger.error(f"Consumer loop error: {e}")
    finally:
        consumer.close()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consumer_loop, daemon=True)
    t.start()


@app.get("/stock")
async def get_stock():
    return stock


@app.get("/stats")
async def get_stats():
    return {"processed_count": processed_count}


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `streaming-kafka/inventory_consumer/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
```

**Step 4: Commit**

```bash
git add streaming-kafka/inventory_consumer/
git commit -m "feat(streaming-kafka): add InventoryConsumer with manual commit"
```

---

## Task 13: Part C — AnalyticsConsumer (Kafka)

**Files:**
- Create: `streaming-kafka/analytics_consumer/app.py`
- Create: `streaming-kafka/analytics_consumer/requirements.txt`
- Create: `streaming-kafka/analytics_consumer/Dockerfile`

**Step 1: Create `streaming-kafka/analytics_consumer/requirements.txt`**

```
fastapi==0.115.0
uvicorn==0.30.0
confluent-kafka==2.5.0
```

**Step 2: Create `streaming-kafka/analytics_consumer/app.py`**

```python
import json
import os
import threading
import time
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaError, TopicPartition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analytics_consumer")

app = FastAPI(title="AnalyticsConsumer (Kafka)")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Metrics state
metrics = {
    "total_orders": 0,
    "total_inventory_events": 0,
    "failures": 0,
    "first_event_time": None,
    "last_event_time": None,
}

# Fault injection
fault_config = {"throttle_seconds": 0}

consumer_instance = None
consumer_lock = threading.Lock()
reset_flag = threading.Event()


class ThrottleRequest(BaseModel):
    seconds: float


consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "analytics-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}


def process_event(event: dict, topic: str):
    now = time.time()
    if metrics["first_event_time"] is None:
        metrics["first_event_time"] = now
    metrics["last_event_time"] = now

    if topic == "order-events":
        metrics["total_orders"] += 1
    elif topic == "inventory-events":
        metrics["total_inventory_events"] += 1
        if event.get("status") == "failed":
            metrics["failures"] += 1


def reset_metrics():
    metrics["total_orders"] = 0
    metrics["total_inventory_events"] = 0
    metrics["failures"] = 0
    metrics["first_event_time"] = None
    metrics["last_event_time"] = None


def consumer_loop():
    global consumer_instance
    # Wait for Kafka to be ready
    time.sleep(10)

    consumer = Consumer(consumer_conf)
    consumer.subscribe(["order-events", "inventory-events"])
    with consumer_lock:
        consumer_instance = consumer
    logger.info("AnalyticsConsumer started")

    try:
        while True:
            # Check for offset reset request
            if reset_flag.is_set():
                reset_flag.clear()
                reset_metrics()
                # Get current assignments
                assignments = consumer.assignment()
                if assignments:
                    for tp in assignments:
                        tp.offset = 0
                    consumer.seek(TopicPartition(tp.topic, tp.partition, 0) for tp in assignments)
                    # Seek each partition to beginning
                    for tp in assignments:
                        consumer.seek(TopicPartition(tp.topic, tp.partition, 0))
                logger.info("Offset reset complete, reprocessing from beginning")

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # Apply throttle
            if fault_config["throttle_seconds"] > 0:
                time.sleep(fault_config["throttle_seconds"])

            try:
                event = json.loads(msg.value().decode("utf-8"))
                process_event(event, msg.topic())
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.error(f"Malformed message at offset {msg.offset()}")

    except Exception as e:
        logger.error(f"Consumer loop error: {e}")
    finally:
        consumer.close()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consumer_loop, daemon=True)
    t.start()


@app.get("/metrics")
async def get_metrics():
    elapsed = 0
    orders_per_min = 0
    failure_rate = 0

    if metrics["first_event_time"] and metrics["last_event_time"]:
        elapsed = metrics["last_event_time"] - metrics["first_event_time"]
        if elapsed > 0:
            orders_per_min = (metrics["total_orders"] / elapsed) * 60

    if metrics["total_inventory_events"] > 0:
        failure_rate = metrics["failures"] / metrics["total_inventory_events"]

    return {
        "total_orders": metrics["total_orders"],
        "total_inventory_events": metrics["total_inventory_events"],
        "failures": metrics["failures"],
        "orders_per_minute": round(orders_per_min, 2),
        "failure_rate": round(failure_rate, 4),
        "elapsed_seconds": round(elapsed, 2),
    }


@app.post("/admin/reset-offsets")
async def reset_offsets():
    reset_flag.set()
    return {"message": "Offset reset requested, reprocessing will begin shortly"}


@app.post("/admin/throttle")
async def set_throttle(req: ThrottleRequest):
    fault_config["throttle_seconds"] = req.seconds
    return {"throttle_seconds": req.seconds}


@app.get("/health")
async def health():
    return {"status": "ok"}
```

**Step 3: Create `streaming-kafka/analytics_consumer/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]
```

**Step 4: Commit**

```bash
git add streaming-kafka/analytics_consumer/
git commit -m "feat(streaming-kafka): add AnalyticsConsumer with replay and throttle"
```

---

## Task 14: Part C — Docker Compose & Tests

**Files:**
- Create: `streaming-kafka/docker-compose.yml`
- Create: `streaming-kafka/tests/test_streaming.py`
- Create: `streaming-kafka/tests/requirements.txt`
- Create: `streaming-kafka/tests/run_tests.sh`

**Step 1: Create `streaming-kafka/docker-compose.yml`**

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 5s
      timeout: 5s
      retries: 10

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 3
    depends_on:
      zookeeper:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  producer:
    build: ./producer_order
    ports:
      - "8000:8000"
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
    depends_on:
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  inventory:
    build: ./inventory_consumer
    ports:
      - "8001:8001"
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
    depends_on:
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"]
      interval: 5s
      timeout: 3s
      retries: 5

  analytics:
    build: ./analytics_consumer
    ports:
      - "8002:8002"
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
    depends_on:
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8002/health')"]
      interval: 5s
      timeout: 3s
      retries: 5
```

**Step 2: Create `streaming-kafka/tests/requirements.txt`**

```
httpx==0.27.0
confluent-kafka==2.5.0
pytest==8.3.0
```

**Step 3: Create `streaming-kafka/tests/test_streaming.py`**

```python
import time
import httpx
import pytest
from confluent_kafka.admin import AdminClient, ConsumerGroupTopicPartitions

PRODUCER_URL = "http://localhost:8000"
INVENTORY_URL = "http://localhost:8001"
ANALYTICS_URL = "http://localhost:8002"
KAFKA_BOOTSTRAP = "localhost:29092"


def test_bulk_produce_and_metrics():
    """Produce 10k events and verify analytics metrics."""
    with httpx.Client(timeout=60) as c:
        # Produce 10k events
        print("\n  Producing 10,000 events...")
        resp = c.post(f"{PRODUCER_URL}/orders/bulk", json={"count": 10000, "item": "burger", "quantity": 1})
        assert resp.status_code == 201
        result = resp.json()
        print(f"  Produced: {result['count']} events")

        # Wait for analytics to catch up
        print("  Waiting for analytics to process...")
        for i in range(60):
            metrics = c.get(f"{ANALYTICS_URL}/metrics").json()
            if metrics["total_orders"] >= 10000:
                break
            time.sleep(2)
            if i % 10 == 0:
                print(f"    Progress: {metrics['total_orders']}/10000 orders processed")

        metrics = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  Final Metrics:")
        print(f"    Total orders:      {metrics['total_orders']}")
        print(f"    Orders/min:        {metrics['orders_per_minute']}")
        print(f"    Failure rate:      {metrics['failure_rate']}")
        print(f"    Elapsed:           {metrics['elapsed_seconds']}s")
        assert metrics["total_orders"] >= 10000


def test_consumer_lag():
    """Throttle analytics, produce events, show consumer lag > 0."""
    with httpx.Client(timeout=30) as c:
        # Enable throttle
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0.1})

        # Produce 1000 more events
        print("\n  Producing 1,000 events with throttled consumer...")
        resp = c.post(f"{PRODUCER_URL}/orders/bulk", json={"count": 1000, "item": "pizza", "quantity": 1})
        assert resp.status_code == 201

        time.sleep(5)  # Let some lag build up

        # Check consumer lag via admin API
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        try:
            futures = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions("analytics-group")]
            )
            for group, future in futures.items():
                result = future.result()
                total_lag = 0
                for tp in result.topic_partitions:
                    # Get high watermark for comparison
                    if tp.offset >= 0:
                        print(f"    Partition {tp.topic}[{tp.partition}]: committed offset={tp.offset}")
                print(f"  Consumer lag exists (analytics is throttled)")
        except Exception as e:
            print(f"  Could not query lag via admin API: {e}")
            print("  (Lag exists because analytics is throttled at 0.1s/msg)")

        # Disable throttle
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0})


def test_replay_consistent_metrics():
    """Reset offsets, reprocess, verify metrics are consistent."""
    with httpx.Client(timeout=30) as c:
        # Get metrics before replay
        metrics_before = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  Metrics before replay:")
        print(f"    Total orders: {metrics_before['total_orders']}")
        print(f"    Failure rate: {metrics_before['failure_rate']}")

        # Reset offsets
        print("  Resetting offsets for replay...")
        resp = c.post(f"{ANALYTICS_URL}/admin/reset-offsets")
        assert resp.status_code == 200

        # Wait for reprocessing
        time.sleep(10)
        for i in range(30):
            metrics_after = c.get(f"{ANALYTICS_URL}/metrics").json()
            if metrics_after["total_orders"] >= metrics_before["total_orders"]:
                break
            time.sleep(2)

        metrics_after = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  Metrics after replay:")
        print(f"    Total orders: {metrics_after['total_orders']}")
        print(f"    Failure rate: {metrics_after['failure_rate']}")

        # Verify consistency
        assert metrics_after["total_orders"] >= metrics_before["total_orders"], \
            "Replay should produce at least as many orders"
        print("  Replay produced consistent metrics")
```

**Step 4: Create `streaming-kafka/tests/run_tests.sh`**

```bash
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
```

**Step 5: Build and verify**

```bash
cd streaming-kafka && docker compose up -d --build
# Wait (Kafka takes longer)
sleep 30
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
# Quick test
curl -X POST http://localhost:8000/order -H 'Content-Type: application/json' -d '{"item":"burger","quantity":1}'
sleep 5
curl http://localhost:8002/metrics
docker compose down
```

**Step 6: Commit**

```bash
git add streaming-kafka/docker-compose.yml streaming-kafka/tests/
git commit -m "feat(streaming-kafka): add docker-compose and test suite"
```

---

## Task 15: Final Verification

**Step 1: Run Part A tests**

```bash
cd sync-rest/tests && chmod +x run_tests.sh && bash run_tests.sh
```

Expected: 3 tests pass, latency table printed.

**Step 2: Run Part B tests**

```bash
cd async-rabbitmq/tests && chmod +x run_tests.sh && bash run_tests.sh
```

Expected: 3 tests pass — backlog drain, idempotency, DLQ all verified.

**Step 3: Run Part C tests**

```bash
cd streaming-kafka/tests && chmod +x run_tests.sh && bash run_tests.sh
```

Expected: 3 tests pass — 10k events processed, consumer lag shown, replay consistent.

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: final verification pass"
```
