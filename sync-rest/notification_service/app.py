import time
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="NotificationService")

notifications: list[dict] = []


class SendRequest(BaseModel):
    order_id: str
    status: str
    item: str


@app.post("/send")
async def send(req: SendRequest):
    entry = {
        "order_id": req.order_id,
        "status": req.status,
        "item": req.item,
        "timestamp": time.time(),
    }
    notifications.append(entry)
    return {"message": "Notification sent", **entry}


@app.get("/notifications")
async def get_notifications():
    return notifications


@app.get("/health")
async def health():
    return {"status": "ok"}
