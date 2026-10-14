"""Weather proxy: serves cached Open-Meteo data and fails on command.

You cannot make the real API fail on demand, so the pipeline reads weather
from here and the fault injector flips the mode through POST /_control.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

CACHE_DIR = Path(os.environ.get("WEATHER_CACHE_DIR", "data/proxy_cache/weather"))
CONTROL_TOKEN = os.environ.get("WEATHER_PROXY_CONTROL_TOKEN", "")

Mode = Literal["normal", "http_503", "timeout", "drop_hours"]

app = FastAPI(title="Weather proxy")


class ControlRequest(BaseModel):
    mode: Mode
    duration_s: int = Field(default=600, ge=1, le=86_400)
    drop_hours_per_day: int = Field(default=4, ge=1, le=23)


class _State:
    mode: Mode = "normal"
    until: float = 0.0
    drop_hours_per_day: int = 4

    def current(self) -> Mode:
        if self.mode != "normal" and time.time() >= self.until:
            self.mode = "normal"
        return self.mode


state = _State()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "mode": state.current()}


@app.post("/_control")
def control(req: ControlRequest, x_control_token: str = Header(default="")) -> dict:
    if not CONTROL_TOKEN or x_control_token != CONTROL_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    state.mode = req.mode
    state.until = time.time() + req.duration_s
    state.drop_hours_per_day = req.drop_hours_per_day
    return {"mode": state.mode, "until": state.until}


@app.get("/v1/archive")
async def archive(month: str = Query(pattern=r"^\d{4}-\d{2}$")) -> dict:
    mode = state.current()
    if mode == "http_503":
        raise HTTPException(status_code=503, detail="Service Unavailable")
    if mode == "timeout":
        await asyncio.sleep(60)

    path = CACHE_DIR / f"{month}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"no cached weather for {month}")
    payload = json.loads(path.read_text(encoding="utf-8"))

    if mode == "drop_hours":
        hourly = payload["hourly"]
        n = state.drop_hours_per_day
        keep = [i for i in range(len(hourly["time"])) if i % 24 >= n]
        payload["hourly"] = {k: [v[i] for i in keep] for k, v in hourly.items()}
    return payload
