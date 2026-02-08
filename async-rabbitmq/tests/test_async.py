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
    subprocess.run(["docker", "compose", "stop", "inventory"], cwd="..", check=True)
    time.sleep(5)

    order_ids = []
    with httpx.Client(timeout=10) as c:
        for _ in range(20):
            resp = place_order(c)
            assert resp.status_code == 202
            order_ids.append(resp.json()["order_id"])
        print(f"\n  Published {len(order_ids)} orders while inventory was down")

    subprocess.run(["docker", "compose", "start", "inventory"], cwd="..", check=True)
    print("  Inventory restarted, waiting for backlog drain...")

    with httpx.Client(timeout=10) as c:
        for oid in order_ids:
            found = wait_for_order_status(c, oid, "reserved", timeout=60)
            assert found, f"Order {oid} did not reach 'reserved' status"
    print(f"  All {len(order_ids)} orders drained successfully")


def test_idempotency():
    """Publish same message_id twice, verify stock only decremented once."""
    with httpx.Client(timeout=10) as c:
        stock_before = c.get(f"{INVENTORY_URL}/stock").json()

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
    for _ in range(2):
        ch.basic_publish(
            exchange="orders_exchange",
            routing_key="order.placed",
            body=msg,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
    conn.close()

    time.sleep(5)

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

    time.sleep(5)

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
