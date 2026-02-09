# Streaming Kafka — Metrics Report

**Generated:** 2026-02-08 16:08:38

---

## 1. Bulk Produce — 10,000 Events

| Metric | Value |
|---|---|
| Total orders produced | 10000 |
| Total inventory events | 0 |
| Orders/min | 34808464.96 |
| Failures | 0 |
| Failure rate | 0 |
| Elapsed time | 0.02s |
| Inventory processed | 0 |
| Remaining stock (burger) | 99 |
| Remaining stock (pizza) | 100 |
| Remaining stock (salad) | 100 |


## 2. Consumer Lag

Throttle set to 0.1s per message, then 1,000 events produced.

| Topic | Partition | Committed | High Watermark | Lag |
|---|---|---|---|---|
| order-events | 0 | 3312 | 3616 | 304 |
| order-events | 1 | 3408 | 3777 | 369 |
| order-events | 2 | 3289 | 3607 | 318 |
| inventory-events | 0 | 0 | 3616 | 3616 |
| inventory-events | 1 | 0 | 3777 | 3777 |
| inventory-events | 2 | 0 | 3607 | 3607 |

Lag demonstrates that the throttled analytics consumer falls behind the producer.


## 3. Replay Evidence — Before vs After

Offset reset triggers full reprocessing from the beginning of all topics.
Metrics are cleared to 0, then re-accumulated as events are replayed.

| Metric | Before Replay | At Reset | After Replay |
|---|---|---|---|
| Total orders | 11000 | 36 | 11000 |
| Inventory events | 0 | 0 | 0 |
| Failures | 0 | 0 | 0 |
| Failure rate | 0 | 0 | 0 |
| Orders/min | 102797.26 | 9044.4 | 9023.22 |

**Replay progress:** [37, 186, 336, 485, 635, 783, 934, 1082, 1233, 1387, 1542, 1693, 1844, 1996, 2150, 2299, 2450, 2599, 2749, 2900, 3052, 3204, 3356, 3507, 3660, 3812, 3962, 4112, 4262, 4411, 4564, 4715, 4873, 5027, 5180, 5331, 5483, 5631, 5782, 5933, 6083, 6233, 6384, 6538, 6687, 6841, 6993, 7144, 7297, 7443, 7596, 7747, 7897, 8046, 8199, 8351, 8503, 8653, 8807, 8957, 9108, 9259, 9409, 9560, 9712, 9864, 10016, 10169, 10324, 10473, 10626, 10784, 10935, 11000]

**Result:** CONSISTENT — replay reprocessed all events and produced matching totals.

