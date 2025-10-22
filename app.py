import asyncio
from fastapi import FastAPI, Request
import pandas as pd
from datetime import datetime
import pytz
import json
from models import AFEvent
from loader import load_to_pg, save_failed_messages

app = FastAPI(title="Appsflyer Events API", version="1.2.1")

event_buffer = asyncio.Queue()
background_tasks: set[asyncio.Task] = set()


@app.post("/api/af-event")
async def receive_event(request: Request, event: AFEvent):
    """Receive and enqueue one Appsflyer event."""

    try:
        raw_body = (await request.body()).decode("utf-8", errors="ignore")
    except Exception:
        raw_body = ""

    # Determine client IP
    forwarded_for = request.headers.get("X-Forwarded-For")
    ip_address = forwarded_for.split(",")[0].strip() if forwarded_for else request.client.host

    tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(tz)

    # Convert Pydantic model to dict
    event_dict = event.model_dump()

    # Normalize event_value (can be dict or JSON string)
    ev = event_dict.get("event_value")
    if isinstance(ev, str):
        try:
            event_dict["event_value"] = json.loads(ev)
        except json.JSONDecodeError:
            err_text = "Invalid JSON in event_value"
            await save_failed_messages_safe(raw_body, err_text)
            return {"status": "failed", "reason": err_text}

    # Add metadata
    event_dict["received_ip"] = ip_address
    event_dict["processed_dttm"] = now.isoformat()

    # Serialize event_value for DB
    if event_dict.get("event_value") is not None:
        try:
            event_dict["event_value"] = json.dumps(event_dict["event_value"], ensure_ascii=False)
        except Exception:
            err_text = "Failed to json.dumps(event_value)"
            await save_failed_messages_safe(raw_body, err_text)
            return {"status": "failed", "reason": err_text}

    # Enqueue to buffer for batch processing
    await event_buffer.put(pd.DataFrame([event_dict]))

    return {"message": "Event received", "app_id": event.app_id, "event": event.event_name}


async def save_failed_messages_safe(msg_text: str, error_text: str):
    """Helper wrapper to safely store failed events."""
    try:
        await save_failed_messages(
            pd.DataFrame([{
                "msg_text": msg_text,
                "error_text": error_text,
                "processed_dttm": datetime.now()
            }]),
            error_text
        )
    except Exception as ex:
        print(f"[{datetime.now()}] Failed to save bad message: {ex}")


async def process_buffer():
    """Background worker to periodically flush buffered events to Postgres."""
    while True:
        batch = []
        while not event_buffer.empty():
            batch.append(event_buffer.get_nowait())

        if batch:
            df = pd.concat(batch, ignore_index=True)
            try:
                await load_to_pg(df)
            except Exception as ex:
                err_text = f"DB insert error: {ex}"
                await save_failed_messages_safe(
                    df.to_json(orient="records", force_ascii=False),
                    err_text
                )

        await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    """Start background loader on app startup."""
    task = asyncio.create_task(process_buffer())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    print("[startup] Background loader started")


@app.get("/health")
async def health():
    """Simple healthcheck endpoint."""
    return {
        "status": "OK",
        "buffer_size": event_buffer.qsize(),
        "background_tasks": len(background_tasks),
    }
