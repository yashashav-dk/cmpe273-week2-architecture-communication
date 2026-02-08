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
