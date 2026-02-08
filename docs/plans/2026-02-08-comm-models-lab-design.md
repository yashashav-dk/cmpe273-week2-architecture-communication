# Campus Food Ordering — Communication Models Lab

## Overview

Implement the same "campus food ordering" workflow in three communication styles:

1. **Synchronous REST** — blocking HTTP calls between services
2. **Async Messaging (RabbitMQ)** — event-driven with message queues
3. **Streaming (Kafka)** — event streaming with replay and analytics

Each part is self-contained with its own `docker-compose.yml`, services, and tests.

## Technology Choices

| Concern | Choice |
|---|---|
| Language | Python 3.11 |
| Framework | FastAPI + Uvicorn |
| HTTP client | httpx |
| RabbitMQ client | pika |
| Kafka client | confluent-kafka-python |
| Data store | In-memory dicts (per-service) |
| Containerization | Docker Compose per part |

## Repo Structure

```
cmpe273-comm-models-lab/
  common/
    README.md
    ids.py
  sync-rest/
    docker-compose.yml
    order_service/
    inventory_service/
    notification_service/
    tests/
  async-rabbitmq/
    docker-compose.yml
    order_service/
    inventory_service/
    notification_service/
    broker/
    tests/
  streaming-kafka/
    docker-compose.yml
    producer_order/
    inventory_consumer/
    analytics_consumer/
    tests/
```

## Shared Domain Model

```python
Order:
  order_id: str       # UUID
  item: str           # "burger", "pizza", "salad"
  quantity: int
  status: str         # "placed", "reserved", "failed", "notified"
  timestamp: float
```

Each service maintains its own `dict[str, Order]` keyed by `order_id`. No shared state between services.

### Common Module

`common/ids.py` — UUID generator for consistent order IDs across all three parts.

### Service Port Convention

| Service | Port |
|---|---|
| OrderService | 8000 |
| InventoryService | 8001 |
| NotificationService / Analytics | 8002 |

### Inventory Stock

```python
stock = {"burger": 100, "pizza": 100, "salad": 100}
```

### Fault Injection

Every service exposes admin endpoints (or env vars):

- `POST /admin/delay` `{"seconds": N}` — injects artificial latency
- `POST /admin/fail` `{"enabled": true}` — forces 500 responses

---

## Part A: Synchronous REST

### Flow

```
Client -> POST /order -> OrderService
  -> POST /reserve -> InventoryService (blocks)
  -> POST /send -> NotificationService (blocks)
  <- 201 Created
```

### OrderService (port 8000)

- `POST /order` — accepts `{item, quantity}`, generates `order_id`, calls InventoryService via `httpx` (5s timeout), then calls NotificationService. Returns final order with status.
- `GET /orders` — returns all orders.
- `POST /admin/delay`, `POST /admin/fail` — fault injection.

### InventoryService (port 8001)

- `POST /reserve` — accepts `{order_id, item, quantity}`, decrements stock or returns 409.
- `GET /stock` — returns current stock levels.
- `POST /admin/delay`, `POST /admin/fail` — fault injection.

### NotificationService (port 8002)

- `POST /send` — accepts `{order_id, status, item}`, logs notification, returns 200.
- `GET /notifications` — returns notification log.

### Error Handling

- `httpx.TimeoutException` -> 504 with `"status": "timeout"`
- Non-2xx from Inventory -> 422 with `"status": "failed"`, skip notification
- Non-2xx from Notification -> order still `"reserved"` (best-effort)

### Docker Compose

Three services on a shared network. Each: `python:3.11-slim`, installs `fastapi uvicorn httpx`.

---

## Part B: Async RabbitMQ

### Flow

```
Client -> POST /order -> OrderService
  <- 202 Accepted (immediate)
  -> publishes "OrderPlaced" to exchange

InventoryService consumes "OrderPlaced"
  -> reserves stock
  -> publishes "InventoryReserved" or "InventoryFailed"

NotificationService consumes "InventoryReserved"
  -> logs confirmation
```

### RabbitMQ Topology

- Exchange: `orders_exchange` (topic type)
- Queues:
  - `inventory_queue` bound to `order.placed`
  - `notification_queue` bound to `inventory.reserved`
  - `dlq_queue` — dead-letter queue via `x-dead-letter-exchange`

### OrderService (port 8000)

- `POST /order` — saves order locally as `"placed"`, publishes `OrderPlaced` with `message_id` (UUID), returns 202.
- `GET /orders` — returns all orders with current status.
- Background pika consumer listens for `inventory.reserved` / `inventory.failed` to update local status.

### InventoryService (port 8001)

- Purely event-driven (no business HTTP endpoints).
- Consumes from `inventory_queue`.
- **Idempotency:** maintains `seen_message_ids: set`. Checks `message_id` before processing — skips duplicates.
- Malformed messages -> `basic_nack(requeue=False)` -> routed to DLQ.
- `GET /stock` — inspection only.

### NotificationService (port 8002)

- Consumes from `notification_queue`.
- Logs confirmation. No further publishing.

### Docker Compose

Three services + `rabbitmq:3-management` (management UI on port 15672).

---

## Part C: Streaming Kafka

### Flow

```
Producer -> publishes OrderPlaced to "order-events"

InventoryConsumer (group: "inventory-group")
  -> consumes "order-events"
  -> reserves stock
  -> publishes to "inventory-events"

AnalyticsConsumer (group: "analytics-group")
  -> consumes "order-events" AND "inventory-events"
  -> computes orders/min, failure rate
  -> exposes GET /metrics
```

### Kafka Topics

- `order-events` — 3 partitions, replication factor 1
- `inventory-events` — 3 partitions, replication factor 1

### Producer / OrderService (port 8000)

- `POST /order` — produces `OrderPlaced` to `order-events`, keyed by `order_id`.
- `POST /orders/bulk` — produces N events for the 10k test.
- `GET /orders` — returns produced orders.

### InventoryConsumer (port 8001)

- `confluent_kafka.Consumer`, group `"inventory-group"`.
- Manual commit (`enable.auto.commit=false`).
- Processes events, produces to `inventory-events`.
- `GET /stock` — inspection endpoint.

### AnalyticsConsumer (port 8002)

- Separate group `"analytics-group"` — independent read of both topics.
- Rolling window counters: orders/min, failure count, total.
- `GET /metrics` — current computed metrics.
- `POST /admin/reset-offsets` — seeks to beginning, clears counters, reprocesses (replay demo).
- `POST /admin/throttle` — adds per-message sleep to generate consumer lag.

### Docker Compose

Three services + `confluentinc/cp-kafka` + `confluentinc/cp-zookeeper`. Auto topic creation enabled.

---

## Testing Strategy

### Part A: `sync-rest/tests/test_sync.py`

1. **Baseline latency** — 20 sequential orders, record round-trip times, compute mean/p95/p99. Print table.
2. **Delay injection** — `POST /admin/delay {"seconds": 2}` on InventoryService, repeat 20 orders, show ~2s increase per request.
3. **Failure injection** — `POST /admin/fail` on InventoryService, send 5 orders, assert 504/422 responses.

### Part B: `async-rabbitmq/tests/test_async.py`

1. **Backlog drain** — stop InventoryService, publish 20 orders (all 202), wait 60s, restart, poll `GET /orders` until all `"reserved"`. Log timestamps.
2. **Idempotency** — publish order, manually republish same `message_id` via pika, verify stock decremented once.
3. **DLQ** — publish malformed message to `inventory_queue`, verify arrival in `dlq_queue` via RabbitMQ management API.

### Part C: `streaming-kafka/tests/test_streaming.py`

1. **Bulk produce** — 10k events via `POST /orders/bulk`, poll `GET /metrics` until done, print orders/min and failure rate.
2. **Consumer lag** — throttle AnalyticsConsumer, produce 1k events, query consumer group lag, show lag > 0.
3. **Replay** — `POST /admin/reset-offsets`, wait for reprocessing, compare metrics before/after. Assert consistent totals.

### Test Runner

Each `tests/` folder includes `run_tests.sh`: `docker compose up -d` -> wait for health -> `pytest` -> `docker compose down`.
