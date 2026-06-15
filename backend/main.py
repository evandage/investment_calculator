from __future__ import annotations

import asyncio
import importlib
import json
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .market_data import (
    fetch_quotes,
    futu_opend_config,
    get_futu_kline_revision,
    futu_subscription_status,
    is_futu_opend_available,
    start_futu_quote_subscription,
    stop_futu_quote_subscription,
)
from .ohlcv import fetch_ohlcv
from .portfolio import build_dashboard, confirm_trades, save_rebalance_budget
from .storage import load_balances, load_holdings, save_balances, save_holdings


class HoldingPayload(BaseModel):
    holdings: dict[str, dict[str, float]]


class BalancesPayload(BaseModel):
    balances: dict[str, float]


class ExecutionItem(BaseModel):
    symbol: str
    action: str = "buy"
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


@app.on_event("startup")
def startup() -> None:
    start_futu_quote_subscription()


@app.on_event("shutdown")
def shutdown() -> None:
    stop_futu_quote_subscription()


@app.get("/api/health")
def health() -> dict[str, Any]:
    host, port = futu_opend_config()
    return {
        "ok": True,
        "futu": {"host": host, "port": port, "available": is_futu_opend_available()},
        "futu_subscription": futu_subscription_status(),
    }


@app.get("/api/quotes")
def quotes() -> dict[str, Any]:
    return fetch_quotes()


@app.get("/api/dashboard")
def dashboard(user_id: str = "evan") -> dict[str, Any]:
    return build_dashboard(user_id)


@app.get("/api/ohlcv")
def ohlcv(symbol: str = "VOO", interval: str = "1d", show_extended: bool = True) -> dict[str, Any]:
    return fetch_ohlcv(symbol, interval, show_extended)


def _build_chart_board(
    symbol: str = "VOO",
    interval: str = "1d",
    theme: str = "Trading Dark",
    avwap_mode: str = "earnings",
    show_extended: bool = True,
) -> dict[str, Any]:
    sym = str(symbol or "VOO").upper()
    if sym not in CHART_LABELS:
        sym = "VOO"
    chart_api = importlib.import_module("chart_boards")
    chart_api.configure_market_provider("futu")
    holding = load_holdings().get(sym, {})
    shares = float(holding.get("shares", 0.0) or 0.0)
    avg_cost = float(holding.get("avg_cost", 0.0) or 0.0)
    user_avg_cost = avg_cost if shares > 0 and avg_cost > 0 else None
    calls = {
        "1d": chart_api.fig_daily,
        "15m": chart_api.fig_15m_vwap_rsi,
        "5m": chart_api.fig_5m_vwap_rsi7,
    }
    key = interval if interval in calls else "1d"
    try:
        effective_avwap_mode = avwap_mode
        if sym in {"VOO", "QQQ", "SGOV"} and effective_avwap_mode == "earnings":
            effective_avwap_mode = "high_60d"
        if key == "1d" and effective_avwap_mode == "today_open":
            effective_avwap_mode = "earnings" if sym not in {"VOO", "QQQ", "SGOV"} else "high_60d"
        kwargs = {
            "chart_theme": theme,
            "user_avg_cost": user_avg_cost if key == "1d" else None,
            "cache_only": False,
        }
        kwargs["avwap_mode"] = effective_avwap_mode
        if key != "1d":
            kwargs["show_extended"] = show_extended
        fig = calls[key](sym, CHART_LABELS[sym], **kwargs)
        figure = json.loads(fig.to_json())
        avwap_meta = (figure.get("layout") or {}).get("meta") or {}
        return {
            "symbol": sym,
            "interval": key,
            "source": "my-template",
            "market_provider": chart_api.get_market_provider(),
            "user_avg_cost": user_avg_cost if key == "1d" else None,
            "avwap_mode": avwap_meta.get("avwap_mode"),
            "avwap_label": avwap_meta.get("avwap_label"),
            "avwap_anchor": avwap_meta.get("avwap_anchor"),
            "show_extended": show_extended if key != "1d" else None,
            "figure": figure,
            "error": "",
        }
    except Exception as exc:
        return {"symbol": sym, "interval": key, "source": "my-template", "figure": None, "error": str(exc)}


@app.get("/api/chart-board")
def chart_board(
    symbol: str = "VOO",
    interval: str = "1d",
    theme: str = "Trading Dark",
    avwap_mode: str = "earnings",
    show_extended: bool = True,
) -> dict[str, Any]:
    return _build_chart_board(symbol, interval, theme, avwap_mode, show_extended)


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
    try:
        result = confirm_trades(payload.user_id, [item.model_dump() for item in payload.executions])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return


@app.websocket("/ws/chart-board")
async def chart_board_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    symbol = str(websocket.query_params.get("symbol", "VOO")).upper()
    interval = str(websocket.query_params.get("interval", "1d"))
    theme = str(websocket.query_params.get("theme", "Trading Dark"))
    avwap_mode = str(websocket.query_params.get("avwap_mode", "earnings"))
    show_extended = str(websocket.query_params.get("show_extended", "true")).lower() not in {"0", "false", "no"}
    if symbol not in CHART_LABELS:
        symbol = "VOO"
    if interval not in {"1d", "15m", "5m"}:
        interval = "1d"

    last_revision = -1
    last_sent_at = 0.0
    try:
        while True:
            revision = get_futu_kline_revision(symbol, interval)
            now = asyncio.get_running_loop().time()
            if last_revision < 0 or (revision != last_revision and now - last_sent_at >= 1.5):
                payload = await asyncio.to_thread(
                    _build_chart_board,
                    symbol,
                    interval,
                    theme,
                    avwap_mode,
                    show_extended,
                )
                payload["realtime"] = True
                payload["revision"] = revision
                subscription = futu_subscription_status()
                payload["kline_subscription"] = (
                    not subscription.get("kline_error")
                    and symbol in set(subscription.get("kline_symbols") or [])
                )
                await websocket.send_json(payload)
                last_revision = revision
                last_sent_at = now
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return
