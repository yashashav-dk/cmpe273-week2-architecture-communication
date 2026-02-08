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

metrics = {
    "total_orders": 0,
    "total_inventory_events": 0,
    "failures": 0,
    "first_event_time": None,
    "last_event_time": None,
}

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
    time.sleep(10)

    consumer = Consumer(consumer_conf)
    consumer.subscribe(["order-events", "inventory-events"])
    with consumer_lock:
        consumer_instance = consumer
    logger.info("AnalyticsConsumer started")

    try:
        while True:
            if reset_flag.is_set():
                reset_flag.clear()
                reset_metrics()
                assignments = consumer.assignment()
                if assignments:
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
