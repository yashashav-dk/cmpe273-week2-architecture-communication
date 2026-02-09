# Campus Food Ordering — Communication Models Lab

**CMPE 273 | Week 2 — Architecture and Communication**

The same campus food ordering workflow implemented in three communication styles to demonstrate their tradeoffs:

| Part | Model | Key Insight |
|---|---|---|
| A | Synchronous REST | Blocking calls — simple but fragile under latency/failure |
| B | Async Messaging (RabbitMQ) | Decoupled via queues — resilient but eventually consistent |
| C | Streaming (Kafka) | Persistent log — replayable, scalable, operationally heavier |

## Tech Stack

Python 3.11, FastAPI, httpx, pika, confluent-kafka, Docker Compose

## Quick Start

Each part is self-contained. Pick one and run:

```bash
# Part A
cd sync-rest/tests && bash run_tests.sh

# Part B
cd async-rabbitmq/tests && bash run_tests.sh

# Part C
cd streaming-kafka/tests && bash run_tests.sh
```

## Repo Structure

```
common/                  Shared UUID generator
sync-rest/               Part A — Synchronous REST
async-rabbitmq/          Part B — Async Messaging
streaming-kafka/         Part C — Event Streaming
screenshots/             Test output evidence
```

---

## Part A: Synchronous REST

### Communication Flow

```
Client --> POST /order --> OrderService
             |-> POST /reserve --> InventoryService (blocks)
             |-> POST /send   --> NotificationService (blocks)
             <-- 201 Created
```

Every call blocks. If InventoryService is slow, the entire request is slow. If it's down, the order fails immediately.

### Test Results

Three tests demonstrate synchronous communication tradeoffs:

- **Baseline Latency** — 20 sequential orders, mean/P95/P99 round-trip times
- **Delay Injection** — 2s delay on InventoryService propagates directly to order latency (~10ms → ~2s)
- **Failure Injection** — InventoryService forced 500 → OrderService returns 422 for every request

![Sync REST Tests](screenshots/sync-rest/sync-rest-tests.png)

---

## Part B: Async Messaging (RabbitMQ)

### Communication Flow

```
Client --> POST /order --> OrderService
             <-- 202 Accepted (immediate)
             |-> publishes "OrderPlaced" to exchange

InventoryService consumes "OrderPlaced"
  -> reserves stock
  -> publishes "InventoryReserved" or "InventoryFailed"

NotificationService consumes "InventoryReserved"
  -> logs confirmation
```

The client gets an immediate 202. Processing happens asynchronously through message queues.

### Backlog Drain

InventoryService is stopped, 20 orders are published (all return 202 immediately). The queue absorbs the backlog:

![Backlog — 20 messages queued](screenshots/async-rabbitmq/backlog-queue-full.png)

InventoryService is restarted. All queued messages drain and process:

![Backlog — drained to 0](screenshots/async-rabbitmq/backlog-queue-drained.png)

### Idempotency

The same message (same `message_id`) is published twice. Stock is only decremented once — the `seen_message_ids` set prevents duplicate processing.

![Idempotency](screenshots/async-rabbitmq/idempotency.png)

### Dead Letter Queue

A malformed message is published to the inventory queue. It gets `nack`'d and routed to the DLQ — poison messages don't block the queue.

![DLQ Test](screenshots/async-rabbitmq/dlq.png)

![DLQ in RabbitMQ UI — 1 message in dlq_queue](screenshots/async-rabbitmq/dlq-ui.png)

### RabbitMQ Management UI

The RabbitMQ management dashboard (localhost:15672) shows the exchange topology, queue depths, and message rates.

![RabbitMQ UI](screenshots/async-rabbitmq/rabbitmq-ui.png)

---

## Part C: Streaming (Kafka)

### Communication Flow

```
Producer --> publishes OrderPlaced to "order-events"

InventoryConsumer (group: "inventory-group")
  -> consumes "order-events"
  -> reserves stock
  -> publishes to "inventory-events"

AnalyticsConsumer (group: "analytics-group")
  -> consumes "order-events" AND "inventory-events"
  -> computes orders/min, failure rate
  -> exposes GET /metrics
```

Two independent consumer groups read the same event stream for different purposes. Events are persistent and replayable.

### Bulk Produce — 10,000 Events

10,000 order events produced and processed through both consumers. The metrics report shows throughput and stock depletion.

![Bulk Produce](screenshots/streaming-kafka/bulk-produce.png)

### Consumer Lag

AnalyticsConsumer is throttled (0.1s per message), then 1,000 events are produced. The lag table shows committed offsets falling behind high watermarks — demonstrating that consumers process at their own pace.

![Consumer Lag](screenshots/streaming-kafka/consumer-lag.png)

### Replay Evidence

Offsets are reset to the beginning. Metrics drop to ~0, then climb back as all events are reprocessed from the log. The final totals match the original — proving the event log is a source of truth that can rebuild state.

| Metric | Before | At Reset | After |
|---|---|---|---|
| Total orders | 11,000 | 36 | 11,000 |

![Replay Progress](screenshots/streaming-kafka/replay-evidence.png)

### Metrics Report

A full metrics report is auto-generated at `streaming-kafka/tests/metrics_report.md` after each test run.

---

## Key Tradeoffs

| Dimension | Sync REST | Async RabbitMQ | Streaming Kafka |
|---|---|---|---|
| Latency | Immediate response | Fire-and-forget (202) | Fire-and-forget (201) |
| Coupling | Tight — caller blocks | Loose — queue decouples | Loose — log decouples |
| Failure handling | Cascading failures | Queue absorbs outages | Log persists through failures |
| Data replay | Not possible | Not possible (consumed = gone) | Replay from any offset |
| Duplicate handling | N/A (request-response) | Needs message-ID dedup | Consumer offset management |
| Operational cost | Low | Medium (broker) | High (Zookeeper + broker + partitions) |
| Best for | Simple CRUD, low latency needs | Task queues, background jobs | Event sourcing, analytics, audit logs |

---

## Screenshots Checklist

Use this as a guide for which screenshots to capture:

- [x] `screenshots/sync-rest/sync-rest-tests.png` — All three tests: baseline latency, delay injection, failure injection
- [x] `screenshots/async-rabbitmq/backlog-queue-full.png` — 20 messages queued while inventory down
- [x] `screenshots/async-rabbitmq/backlog-queue-drained.png` — All messages drained after restart
- [x] `screenshots/async-rabbitmq/idempotency.png` — Stock delta = 1 after duplicate publish
- [x] `screenshots/async-rabbitmq/dlq.png` — DLQ message count >= 1
- [x] `screenshots/async-rabbitmq/dlq-ui.png` — DLQ with 1 message in RabbitMQ UI
- [x] `screenshots/async-rabbitmq/rabbitmq-ui.png` — Management dashboard showing queues/exchanges
- [x] `screenshots/streaming-kafka/bulk-produce.png` — 10k events metrics report
- [x] `screenshots/streaming-kafka/consumer-lag.png` — Per-partition lag table
- [x] `screenshots/streaming-kafka/replay-evidence.png` — Before/reset/after comparison + progress
