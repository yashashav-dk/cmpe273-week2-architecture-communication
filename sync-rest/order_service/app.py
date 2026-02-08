import os
import time
import uuid
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="OrderService")

orders: dict[str, dict] = {}

INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory:8001")
NOTIFICATION_URL = os.getenv("NOTIFICATION_URL", "http://notification:8002")
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5"))


class OrderRequest(BaseModel):
    item: str
    quantity: int


@app.post("/order", status_code=201)
async def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "item": req.item,
        "quantity": req.quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = order

    # Call InventoryService synchronously
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(
                f"{INVENTORY_URL}/reserve",
                json={"order_id": order_id, "item": req.item, "quantity": req.quantity},
            )
        if resp.status_code != 200:
            order["status"] = "failed"
            raise HTTPException(status_code=422, detail={"order": order, "reason": resp.json()})
    except httpx.TimeoutException:
        order["status"] = "timeout"
        raise HTTPException(status_code=504, detail={"order": order, "reason": "Inventory service timeout"})
    except httpx.ConnectError:
        order["status"] = "failed"
        raise HTTPException(status_code=502, detail={"order": order, "reason": "Inventory service unreachable"})

    order["status"] = "reserved"

    # Call NotificationService synchronously (best-effort)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            await client.post(
                f"{NOTIFICATION_URL}/send",
                json={"order_id": order_id, "status": "reserved", "item": req.item},
            )
        order["status"] = "notified"
    except Exception:
        pass  # Best-effort — order stays "reserved"

    return order


@app.get("/orders")
async def get_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"status": "ok"}
