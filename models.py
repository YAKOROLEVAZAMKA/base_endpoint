from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any
import json


class AFEvent(BaseModel):
    app_id: str = Field(..., description="App ID AppsFlyer")
    event_name: str = Field(..., description="")
    event_time: str = Field(..., description="")
    idfa: Optional[str] = None
    advertising_id: Optional[str] = None
    app_version: Optional[str] = None
    sdk_version: Optional[str] = None
    app_name: Optional[str] = None
    bundle_id: Optional[str] = None
    platform: Optional[str] = None
    os_version: Optional[str] = None
    install_time: Optional[str] = None
    appsflyer_id: Optional[str] = None
    customer_user_id: Optional[str] = None
    event_value: Optional[Any] = Field(None, description="JSON (dict or str)")
    ip: Optional[str] = None
    wifi: Optional[bool] = None
    operator: Optional[str] = None
    install_app_store: Optional[str] = None
    region: Optional[str] = None
    country_code: Optional[str] = None
    city: Optional[str] = None

    # ключевая часть — парсим строку если это JSON
    @field_validator("event_value", mode="before")
    @classmethod
    def parse_event_value(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # если строка, но не JSON — оставляем как есть (обработаем в app.py)
                return v
        return v
