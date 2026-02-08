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
