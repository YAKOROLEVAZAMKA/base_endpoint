import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_SCHEMA = os.getenv("PG_SCHEMA", "appsflyer_monitoring")

TARGET_TABLE = f"{PG_SCHEMA}.af_events"

def get_pg_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str, connect_args={"options": f"-c search_path={PG_SCHEMA}"})

async def load_to_pg(df: pd.DataFrame):
    if df.empty:
        return

    try:
        engine = get_pg_engine()
        df.to_sql(TARGET_TABLE.split('.')[-1], engine, if_exists="append", index=False)
        print(f"[{datetime.now()}] Loaded {len(df)} rows to {TARGET_TABLE}")
    except Exception as ex:
        print(f"[{datetime.now()}] PostgreSQL load error: {ex}")
        print(df.head().to_string())
