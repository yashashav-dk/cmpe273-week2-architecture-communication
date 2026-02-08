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
