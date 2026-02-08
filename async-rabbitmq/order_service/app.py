import json
import os
import threading
import time
import uuid
import logging
import pika
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order")

app = FastAPI(title="OrderService (RabbitMQ)")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
orders: dict[str, dict] = {}


class OrderRequest(BaseModel):
    item: str
    quantity: int


def get_connection():
    for attempt in range(30):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        except pika.exceptions.AMQPConnectionError:
            logger.info(f"Waiting for RabbitMQ... attempt {attempt + 1}")
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ")


def on_status_update(ch, method, properties, body):
    """Listen for inventory.reserved / inventory.failed to update local order status."""
    try:
        msg = json.loads(body)
        order_id = msg.get("order_id")
        status = msg.get("status")
        if order_id in orders:
            orders[order_id]["status"] = status
            logger.info(f"Order {order_id} updated to {status}")
    except Exception as e:
        logger.error(f"Error processing status update: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def status_consumer_thread():
    """Consume inventory.reserved and inventory.failed for order status updates."""
    conn = get_connection()
    ch = conn.channel()
    result = ch.queue_declare(queue="order_status_queue", durable=True)
    ch.queue_bind(exchange="orders_exchange", queue="order_status_queue", routing_key="inventory.reserved")
    ch.queue_bind(exchange="orders_exchange", queue="order_status_queue", routing_key="inventory.failed")
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="order_status_queue", on_message_callback=on_status_update)
    logger.info("OrderService status consumer started")
    ch.start_consuming()


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=status_consumer_thread, daemon=True)
    t.start()


@app.post("/order", status_code=202)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    message_id = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = order

    conn = get_connection()
    ch = conn.channel()
    ch.basic_publish(
        exchange="orders_exchange",
        routing_key="order.placed",
        body=json.dumps({
            "order_id": order_id,
            "message_id": message_id,
            "item": req.item,
            "quantity": req.quantity,
            "timestamp": order["timestamp"],
        }),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    conn.close()
    logger.info(f"Published OrderPlaced for {order_id}")

    return order


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
