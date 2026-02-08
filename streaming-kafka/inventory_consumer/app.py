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
