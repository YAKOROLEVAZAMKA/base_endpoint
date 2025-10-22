import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_SCHEMA = os.getenv("PG_SCHEMA", "appsflyer_monitoring")

TARGET_TABLE = f"{PG_SCHEMA}.af_events"
FAILED_TABLE = f"{PG_SCHEMA}.af_failed_messages"


def get_pg_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str, connect_args={"options": f"-c search_path={PG_SCHEMA}"})


def _load_to_pg_sync(df: pd.DataFrame, table_name: str):
    if df.empty:
        return 0

    engine = get_pg_engine()
    with engine.begin() as conn:
        df.to_sql(table_name.split('.')[-1], conn, if_exists="append", index=False)
    engine.dispose()
    return len(df)


async def load_to_pg(df: pd.DataFrame):
    if df.empty:
        return

    loop = asyncio.get_running_loop()
    try:
        count = await loop.run_in_executor(None, _load_to_pg_sync, df, TARGET_TABLE)
        print(f"[{datetime.now()}] Loaded {count} rows to {TARGET_TABLE}")
    except Exception as ex:
        print(f"[{datetime.now()}] PostgreSQL load error: {ex}")
        await save_failed_messages(df, str(ex))


async def save_failed_messages(df: pd.DataFrame, error_text: str):
    try:
        failed_df = pd.DataFrame(
            {
                "msg_text": [df.to_json(orient="records", force_ascii=False)],
                "error_text": [error_text],
                "processed_dttm": [datetime.now()],
            }
        )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _load_to_pg_sync, failed_df, FAILED_TABLE)
        print(f"[{datetime.now()}] Saved failed batch ({len(df)} rows) to {FAILED_TABLE}")
    except Exception as inner_ex:
        print(f"[{datetime.now()}] Failed to log bad message: {inner_ex}")
