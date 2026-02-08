import json
import os
import time
import uuid
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

app = FastAPI(title="OrderProducer (Kafka)")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
orders: dict[str, dict] = {}

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
producer = None


def get_producer():
    global producer
    if producer is None:
        producer = Producer(producer_conf)
    return producer


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


class OrderRequest(BaseModel):
    item: str
    quantity: int


class BulkRequest(BaseModel):
    count: int
    item: str = "burger"
    quantity: int = 1


@app.post("/order", status_code=201)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    event = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = event

    p = get_producer()
    p.produce(
        "order-events",
        key=order_id,
        value=json.dumps(event),
        callback=delivery_report,
    )
    p.flush()

    return event


@app.post("/orders/bulk", status_code=201)
async def create_bulk_orders(req: BulkRequest):
    p = get_producer()
    created = []
    for _ in range(req.count):
        order_id = str(uuid.uuid4())
        event = {
            "order_id": order_id,
            "item": req.item,
            "quantity": req.quantity,
            "status": "placed",
            "timestamp": time.time(),
        }
        orders[order_id] = event
        p.produce(
            "order-events",
            key=order_id,
            value=json.dumps(event),
            callback=delivery_report,
        )
        created.append(order_id)

        if len(created) % 1000 == 0:
            p.flush()

    p.flush()
    return {"count": len(created), "order_ids_sample": created[:5]}


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
