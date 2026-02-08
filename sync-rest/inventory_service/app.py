import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="InventoryService")

stock = {"burger": 100, "pizza": 100, "salad": 100}

# Fault injection state
fault_config = {"delay_seconds": 0, "fail_enabled": False}


class ReserveRequest(BaseModel):
    order_id: str
    item: str
    quantity: int


class FaultDelay(BaseModel):
    seconds: float


class FaultFail(BaseModel):
    enabled: bool


@app.post("/reserve")
async def reserve(req: ReserveRequest):
    if fault_config["fail_enabled"]:
        raise HTTPException(status_code=500, detail="Injected failure")
    if fault_config["delay_seconds"] > 0:
        await asyncio.sleep(fault_config["delay_seconds"])
    if req.item not in stock:
        raise HTTPException(status_code=404, detail=f"Unknown item: {req.item}")
    if stock[req.item] < req.quantity:
        raise HTTPException(status_code=409, detail=f"Insufficient stock for {req.item}")
    stock[req.item] -= req.quantity
    return {"order_id": req.order_id, "item": req.item, "quantity": req.quantity, "status": "reserved"}


@app.get("/stock")
async def get_stock():
    return stock


@app.post("/admin/delay")
async def set_delay(fault: FaultDelay):
    fault_config["delay_seconds"] = fault.seconds
    return {"delay_seconds": fault.seconds}


@app.post("/admin/fail")
async def set_fail(fault: FaultFail):
    fault_config["fail_enabled"] = fault.enabled
    return {"fail_enabled": fault.enabled}


@app.get("/health")
async def health():
    return {"status": "ok"}
