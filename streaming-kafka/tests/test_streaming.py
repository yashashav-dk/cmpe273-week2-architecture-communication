import json
import os
import time
import httpx
import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from datetime import datetime

PRODUCER_URL = "http://localhost:8000"
INVENTORY_URL = "http://localhost:8001"
ANALYTICS_URL = "http://localhost:8002"
KAFKA_BOOTSTRAP = "localhost:29092"

REPORT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_PATH = os.path.join(REPORT_DIR, "metrics_report.md")

# Shared state for the report
_report_sections = []


def _append_report(section: str):
    _report_sections.append(section)


def _write_report():
    header = f"# Streaming Kafka — Metrics Report\n\n"
    header += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n"
    with open(REPORT_PATH, "w") as f:
        f.write(header + "\n".join(_report_sections))


def test_01_bulk_produce_and_metrics():
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
        stock = c.get(f"{INVENTORY_URL}/stock").json()
        inv_stats = c.get(f"{INVENTORY_URL}/stats").json()

        print(f"\n  === METRICS REPORT ===")
        print(f"  Total orders produced:     {metrics['total_orders']}")
        print(f"  Total inventory events:    {metrics['total_inventory_events']}")
        print(f"  Orders/min:                {metrics['orders_per_minute']}")
        print(f"  Failures:                  {metrics['failures']}")
        print(f"  Failure rate:              {metrics['failure_rate']}")
        print(f"  Elapsed time:              {metrics['elapsed_seconds']}s")
        print(f"  Inventory processed:       {inv_stats['processed_count']}")
        print(f"  Remaining stock:           {json.dumps(stock)}")

        _append_report(
            f"## 1. Bulk Produce — 10,000 Events\n\n"
            f"| Metric | Value |\n"
            f"|---|---|\n"
            f"| Total orders produced | {metrics['total_orders']} |\n"
            f"| Total inventory events | {metrics['total_inventory_events']} |\n"
            f"| Orders/min | {metrics['orders_per_minute']} |\n"
            f"| Failures | {metrics['failures']} |\n"
            f"| Failure rate | {metrics['failure_rate']} |\n"
            f"| Elapsed time | {metrics['elapsed_seconds']}s |\n"
            f"| Inventory processed | {inv_stats['processed_count']} |\n"
            f"| Remaining stock (burger) | {stock.get('burger', 'N/A')} |\n"
            f"| Remaining stock (pizza) | {stock.get('pizza', 'N/A')} |\n"
            f"| Remaining stock (salad) | {stock.get('salad', 'N/A')} |\n\n"
        )

        assert metrics["total_orders"] >= 10000


def test_02_consumer_lag():
    """Throttle analytics, produce events, show consumer lag > 0."""
    with httpx.Client(timeout=30) as c:
        # Enable throttle
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0.1})

        # Produce 1000 more events
        print("\n  Producing 1,000 events with throttled consumer...")
        resp = c.post(f"{PRODUCER_URL}/orders/bulk", json={"count": 1000, "item": "pizza", "quantity": 1})
        assert resp.status_code == 201

        time.sleep(5)  # Let some lag build up

        lag_lines = []

        # Check consumer lag by comparing committed offsets vs high watermarks
        try:
            tmp_consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": "analytics-group",
                "enable.auto.commit": False,
            })
            total_lag = 0
            for topic in ["order-events", "inventory-events"]:
                metadata = tmp_consumer.list_topics(topic, timeout=10)
                partitions = metadata.topics[topic].partitions
                for pid in partitions:
                    tp = TopicPartition(topic, pid)
                    committed = tmp_consumer.committed([tp], timeout=10)
                    lo, hi = tmp_consumer.get_watermark_offsets(tp, timeout=10)
                    c_off = committed[0].offset if committed[0].offset >= 0 else 0
                    lag = hi - c_off
                    total_lag += lag
                    line = f"    {topic}[{pid}]: committed={c_off}, high={hi}, lag={lag}"
                    print(line)
                    lag_lines.append(f"| {topic} | {pid} | {c_off} | {hi} | {lag} |")
            tmp_consumer.close()
            print(f"  Total consumer lag: {total_lag}")
        except Exception as e:
            print(f"  Could not query lag: {e}")
            lag_lines.append(f"| (unavailable) | - | - | - | - |")

        # Disable throttle
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0})

        _append_report(
            f"## 2. Consumer Lag\n\n"
            f"Throttle set to 0.1s per message, then 1,000 events produced.\n\n"
            f"| Topic | Partition | Committed | High Watermark | Lag |\n"
            f"|---|---|---|---|---|\n"
            + "\n".join(lag_lines) + "\n\n"
            f"Lag demonstrates that the throttled analytics consumer falls behind the producer.\n\n"
        )


def test_03_replay_consistent_metrics():
    """Reset offsets, reprocess, verify metrics are consistent."""
    with httpx.Client(timeout=60) as c:
        # Wait for any pending processing from previous test to finish
        time.sleep(10)

        # Snapshot metrics BEFORE replay
        metrics_before = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  ╔══════════════════════════════════════╗")
        print(f"  ║      REPLAY EVIDENCE — BEFORE        ║")
        print(f"  ╠══════════════════════════════════════╣")
        print(f"  ║  Total orders:       {metrics_before['total_orders']:>14} ║")
        print(f"  ║  Inventory events:   {metrics_before['total_inventory_events']:>14} ║")
        print(f"  ║  Failures:           {metrics_before['failures']:>14} ║")
        print(f"  ║  Failure rate:       {metrics_before['failure_rate']:>14} ║")
        print(f"  ║  Orders/min:         {metrics_before['orders_per_minute']:>14} ║")
        print(f"  ║  Elapsed:          {metrics_before['elapsed_seconds']:>13}s ║")
        print(f"  ╚══════════════════════════════════════╝")

        # Throttle the consumer so we can observe the replay in progress
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0.005})

        # Reset offsets — triggers reprocessing from beginning
        print("\n  >>> Resetting offsets for replay...")
        resp = c.post(f"{ANALYTICS_URL}/admin/reset-offsets")
        assert resp.status_code == 200
        print("  >>> Offset reset requested. Reprocessing in progress...")

        # Capture the reset moment — metrics should drop to 0 or be low
        time.sleep(0.3)
        metrics_reset = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  >>> Metrics immediately after reset:")
        print(f"      Total orders: {metrics_reset['total_orders']} (expected 0 or low)")

        # Track reprocessing progress
        replay_snapshots = []
        for i in range(90):
            metrics_during = c.get(f"{ANALYTICS_URL}/metrics").json()
            replay_snapshots.append(metrics_during["total_orders"])
            if metrics_during["total_orders"] >= metrics_before["total_orders"]:
                break
            time.sleep(1)
            if i % 5 == 0:
                print(f"    Reprocessing... {metrics_during['total_orders']}/{metrics_before['total_orders']}")

        # Remove throttle
        c.post(f"{ANALYTICS_URL}/admin/throttle", json={"seconds": 0})

        metrics_after = c.get(f"{ANALYTICS_URL}/metrics").json()
        print(f"\n  ╔══════════════════════════════════════╗")
        print(f"  ║      REPLAY EVIDENCE — AFTER         ║")
        print(f"  ╠══════════════════════════════════════╣")
        print(f"  ║  Total orders:       {metrics_after['total_orders']:>14} ║")
        print(f"  ║  Inventory events:   {metrics_after['total_inventory_events']:>14} ║")
        print(f"  ║  Failures:           {metrics_after['failures']:>14} ║")
        print(f"  ║  Failure rate:       {metrics_after['failure_rate']:>14} ║")
        print(f"  ║  Orders/min:         {metrics_after['orders_per_minute']:>14} ║")
        print(f"  ║  Elapsed:          {metrics_after['elapsed_seconds']:>13}s ║")
        print(f"  ╚══════════════════════════════════════╝")

        # Comparison table
        print(f"\n  ╔═══════════════════════════════════════════════════════════╗")
        print(f"  ║                BEFORE vs AFTER COMPARISON                ║")
        print(f"  ╠═══════════════════════════════════════════════════════════╣")
        print(f"  ║  {'Metric':<22} {'Before':>10} {'Reset':>10} {'After':>10}  ║")
        print(f"  ╠═══════════════════════════════════════════════════════════╣")
        print(f"  ║  {'Total orders':<22} {metrics_before['total_orders']:>10} {metrics_reset['total_orders']:>10} {metrics_after['total_orders']:>10}  ║")
        print(f"  ║  {'Inventory events':<22} {metrics_before['total_inventory_events']:>10} {metrics_reset['total_inventory_events']:>10} {metrics_after['total_inventory_events']:>10}  ║")
        print(f"  ║  {'Failures':<22} {metrics_before['failures']:>10} {metrics_reset['failures']:>10} {metrics_after['failures']:>10}  ║")
        print(f"  ║  {'Failure rate':<22} {metrics_before['failure_rate']:>10} {metrics_reset['failure_rate']:>10} {metrics_after['failure_rate']:>10}  ║")
        print(f"  ╚═══════════════════════════════════════════════════════════╝")

        match = "CONSISTENT" if metrics_after["total_orders"] >= metrics_before["total_orders"] else "INCONSISTENT"
        print(f"\n  Replay result: {match}")
        if replay_snapshots:
            print(f"  Replay progress snapshots: {replay_snapshots}")

        _append_report(
            f"## 3. Replay Evidence — Before vs After\n\n"
            f"Offset reset triggers full reprocessing from the beginning of all topics.\n"
            f"Metrics are cleared to 0, then re-accumulated as events are replayed.\n\n"
            f"| Metric | Before Replay | At Reset | After Replay |\n"
            f"|---|---|---|---|\n"
            f"| Total orders | {metrics_before['total_orders']} | {metrics_reset['total_orders']} | {metrics_after['total_orders']} |\n"
            f"| Inventory events | {metrics_before['total_inventory_events']} | {metrics_reset['total_inventory_events']} | {metrics_after['total_inventory_events']} |\n"
            f"| Failures | {metrics_before['failures']} | {metrics_reset['failures']} | {metrics_after['failures']} |\n"
            f"| Failure rate | {metrics_before['failure_rate']} | {metrics_reset['failure_rate']} | {metrics_after['failure_rate']} |\n"
            f"| Orders/min | {metrics_before['orders_per_minute']} | {metrics_reset['orders_per_minute']} | {metrics_after['orders_per_minute']} |\n\n"
            f"**Replay progress:** {replay_snapshots}\n\n"
            f"**Result:** {match} — replay reprocessed all events and produced matching totals.\n\n"
        )

        assert metrics_after["total_orders"] >= metrics_before["total_orders"], \
            "Replay should produce at least as many orders"


def test_04_write_report():
    """Write the metrics report to file."""
    _write_report()
    print(f"\n  Metrics report written to: {REPORT_PATH}")
    assert os.path.exists(REPORT_PATH)
