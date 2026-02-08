import json
import os
import threading
import time
import logging
import pika
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification")

app = FastAPI(title="NotificationService (RabbitMQ)")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
notifications: list[dict] = []


def get_connection():
    for attempt in range(30):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        except pika.exceptions.AMQPConnectionError:
            logger.info(f"Waiting for RabbitMQ... attempt {attempt + 1}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ")


def on_inventory_reserved(ch, method, properties, body):
    try:
        msg = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Malformed message: {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    entry = {
        "order_id": msg.get("order_id"),
        "item": msg.get("item"),
        "status": msg.get("status"),
        "timestamp": time.time(),
    }
    notifications.append(entry)
    logger.info(f"Notification sent for order {entry['order_id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consumer_thread():
    conn = get_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="notification_queue", on_message_callback=on_inventory_reserved)
    logger.info("NotificationService consumer started")
    ch.start_consuming()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consumer_thread, daemon=True)
    t.start()


@app.get("/notifications")
async def get_notifications():
    return notifications


@app.get("/health")
async def health():
    return {"status": "ok"}
