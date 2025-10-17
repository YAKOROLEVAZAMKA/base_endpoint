from fastapi import FastAPI, Request
from asyncio import Queue
import asyncio
import pandas as pd
from datetime import datetime
import pytz
import json
from models import AFEvent
from loader import load_to_pg

app = FastAPI(title="Appsflyer Events API", version="1.0.0")

event_buffer = Queue()
background_tasks: set[asyncio.Task] = set()  # контейнер


@app.post("/api/af-event")
async def receive_event(request: Request, event: AFEvent):
    forwarded_for = request.headers.get("X-Forwarded-For")
    ip_address = forwarded_for.split(",")[0].strip() if forwarded_for else request.client.host

    tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(tz)

    event_dict = event.model_dump()
    event_dict["received_ip"] = ip_address
    event_dict["received_dttm"] = now.isoformat()

    if event_dict.get("event_value"):
        event_dict["event_value"] = json.dumps(event_dict["event_value"])

    df = pd.DataFrame([event_dict])
    await event_buffer.put(df)

    return {"message": "Event received", "app_id": event.app_id, "event": event.event_name}


async def process_buffer():
    while True:
        batch = []
        while not event_buffer.empty():
            batch.append(event_buffer.get_nowait())

        if batch:
            df = pd.concat(batch, ignore_index=True)
            await load_to_pg(df)

        await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    task = asyncio.create_task(process_buffer())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)  # удаление после завершения


@app.get("/health")
async def health():
    return {
        "status": "OK",
        "buffer_size": event_buffer.qsize(),
        "background_tasks": len(background_tasks),
    }
