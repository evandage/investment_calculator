from __future__ import annotations

import asyncio
import importlib
import json
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .market_data import fetch_quotes, futu_opend_config, is_futu_opend_available
from .ohlcv import fetch_ohlcv
from .portfolio import build_dashboard, confirm_buys, save_rebalance_budget
from .storage import load_balances, load_holdings, save_balances, save_holdings


class HoldingPayload(BaseModel):
    holdings: dict[str, dict[str, float]]


class BalancesPayload(BaseModel):
    balances: dict[str, float]


class ExecutionItem(BaseModel):
    symbol: str
    amount_usd: float
    shares: float
    intensity: str = "normal"


class ExecutionPayload(BaseModel):
    user_id: str = "evan"
    executions: list[ExecutionItem]


class RebalanceBudgetPayload(BaseModel):
    user_id: str = "evan"
    planned_cash_by_month: dict[str, float]


CHART_LABELS = {
    "VOO": "VOO",
    "QQQ": "QQQ",
    "ISRG": "ISRG",
    "GOOGL": "GOOGL",
    "MSFT": "MSFT",
    "AVGO": "AVGO",
    "NVDA": "NVDA",
    "SGOV": "SGOV",
}


app = FastAPI(title="Investment Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "Investment Dashboard API",
        "frontend": "http://127.0.0.1:5173",
        "health": "/api/health",
        "docs": "/docs",
    }


@app.get("/api/health")
def health() -> dict[str, Any]:
    host, port = futu_opend_config()
    return {
        "ok": True,
        "futu": {"host": host, "port": port, "available": is_futu_opend_available()},
    }


@app.get("/api/quotes")
def quotes() -> dict[str, Any]:
    return fetch_quotes()


@app.get("/api/dashboard")
def dashboard(user_id: str = "evan") -> dict[str, Any]:
    return build_dashboard(user_id)


@app.get("/api/ohlcv")
def ohlcv(symbol: str = "VOO", interval: str = "1d") -> dict[str, Any]:
    return fetch_ohlcv(symbol, interval)


@app.get("/api/chart-board")
def chart_board(
    symbol: str = "VOO",
    interval: str = "1d",
    theme: str = "Trading Dark",
) -> dict[str, Any]:
    sym = str(symbol or "VOO").upper()
    if sym not in CHART_LABELS:
        sym = "VOO"
    chart_api = importlib.import_module("chart_boards")
    chart_api.configure_market_provider("tencent")
    calls = {
        "1d": chart_api.fig_daily,
        "15m": chart_api.fig_15m_vwap_rsi,
        "5m": chart_api.fig_5m_vwap_rsi7,
    }
    key = interval if interval in calls else "1d"
    try:
        fig = calls[key](sym, CHART_LABELS[sym], chart_theme=theme, cache_only=False)
        return {
            "symbol": sym,
            "interval": key,
            "source": "my-template",
            "figure": json.loads(fig.to_json()),
            "error": "",
        }
    except Exception as exc:
        return {"symbol": sym, "interval": key, "source": "my-template", "figure": None, "error": str(exc)}


@app.get("/api/holdings")
def holdings() -> dict[str, Any]:
    return {"holdings": load_holdings(), "balances": load_balances()}


@app.put("/api/holdings")
def update_holdings(payload: HoldingPayload) -> dict[str, Any]:
    save_holdings(payload.holdings)
    return {"saved": True, "holdings": load_holdings()}


@app.put("/api/balances")
def update_balances(payload: BalancesPayload) -> dict[str, Any]:
    save_balances(payload.balances)
    return {"saved": True, "balances": load_balances()}


@app.post("/api/rebalance/confirm")
def confirm_execution(payload: ExecutionPayload) -> dict[str, Any]:
    result = confirm_buys(payload.user_id, [item.model_dump() for item in payload.executions])
    result["dashboard"] = build_dashboard(payload.user_id)
    return result


@app.put("/api/rebalance/budget")
def update_rebalance_budget(payload: RebalanceBudgetPayload) -> dict[str, Any]:
    result = save_rebalance_budget(payload.user_id, payload.planned_cash_by_month)
    result["dashboard"] = build_dashboard(payload.user_id)
    return result


@app.websocket("/ws/quotes")
async def quotes_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(fetch_quotes())
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        return
