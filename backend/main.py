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
    get_futu_quote_revision,
    get_futu_subscription_quotes,
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
        quote = get_futu_subscription_quotes().get(sym) or {}
        latest_price = float(quote.get("price") or 0.0)
        latest_change_pct = float(quote["change_pct"]) if quote.get("change_pct") is not None else None
        effective_avwap_mode = avwap_mode
        if sym in {"VOO", "QQQ", "SGOV"} and effective_avwap_mode == "earnings":
            effective_avwap_mode = "high_60d"
        if key == "1d" and effective_avwap_mode == "today_open":
            effective_avwap_mode = "earnings" if sym not in {"VOO", "QQQ", "SGOV"} else "high_60d"
        kwargs = {
            "chart_theme": theme,
            "user_avg_cost": user_avg_cost if key == "1d" else None,
            "cache_only": False,
            "latest_price": latest_price if latest_price > 0 else None,
            "latest_change_pct": latest_change_pct,
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
            "latest_price": latest_price if latest_price > 0 else None,
            "latest_change_pct": latest_change_pct,
            "figure": figure,
            "error": "",
        }
    except Exception as exc:
        return {"symbol": sym, "interval": key, "source": "my-template", "figure": None, "error": str(exc)}


def _build_global_chart_board(
    interval: str = "5m",
    theme: str = "Trading Dark",
    show_extended: bool = True,
    columns: int = 1,
) -> dict[str, Any]:
    chart_api = importlib.import_module("chart_boards")
    chart_api.configure_market_provider("futu")
    key = interval if interval in {"1d", "15m", "5m"} else "5m"
    cols = min(3, max(1, int(columns or 1)))
    symbols = list(CHART_LABELS.keys())
    try:
        quotes = get_futu_subscription_quotes()
        fig = chart_api.fig_global_kline_board(
            symbols,
            interval=key,
            chart_theme=theme,
            show_extended=show_extended,
            columns=cols,
            latest_quotes=quotes,
            cache_only=False,
        )
        return {
            "symbol": "GLOBAL",
            "symbols": symbols,
            "interval": key,
            "source": "my-template-global",
            "market_provider": chart_api.get_market_provider(),
            "show_extended": show_extended if key != "1d" else None,
            "columns": cols,
            "figure": json.loads(fig.to_json()),
            "error": "",
        }
    except Exception as exc:
        return {"symbol": "GLOBAL", "symbols": symbols, "interval": key, "source": "my-template-global", "figure": None, "error": str(exc)}


def _patch_latest_candle(payload: dict[str, Any], price: float) -> None:
    figure = payload.get("figure") or {}
    traces = figure.get("data") or []
    target: dict[str, Any] | None = None
    target_x = ""
    for trace in traces:
        if trace.get("type") != "candlestick":
            continue
        xs = trace.get("x") or []
        if not xs:
            continue
        last_x = str(xs[-1])
        if target is None or last_x > target_x:
            target = trace
            target_x = last_x
    if target is None:
        return
    for key in ("close", "high", "low"):
        values = target.get(key)
        if not isinstance(values, list) or not values:
            return
    target["close"][-1] = price
    target["high"][-1] = max(float(target["high"][-1]), price)
    target["low"][-1] = min(float(target["low"][-1]), price)


def _patch_latest_price(payload: dict[str, Any], price: float, change_pct: float | None = None) -> None:
    if price <= 0:
        return
    _patch_latest_candle(payload, price)
    figure = payload.get("figure") or {}
    layout = figure.get("layout") or {}
    for shape in layout.get("shapes") or []:
        if shape.get("name") == "latest_price_line":
            shape["y0"] = price
            shape["y1"] = price
    for annotation in layout.get("annotations") or []:
        if annotation.get("name") == "latest_price_label":
            annotation["y"] = price
            change_text = f"<br>{change_pct:+.2f}%" if change_pct is not None else ""
            annotation["text"] = f"最新 {price:.2f}{change_text}"
    payload["latest_price"] = price
    payload["latest_change_pct"] = change_pct


@app.get("/api/chart-board")
def chart_board(
    symbol: str = "VOO",
    interval: str = "1d",
    theme: str = "Trading Dark",
    avwap_mode: str = "earnings",
    show_extended: bool = True,
) -> dict[str, Any]:
    return _build_chart_board(symbol, interval, theme, avwap_mode, show_extended)


@app.get("/api/chart-board-global")
def chart_board_global(
    interval: str = "5m",
    theme: str = "Trading Dark",
    show_extended: bool = True,
    columns: int = 1,
) -> dict[str, Any]:
    return _build_global_chart_board(interval, theme, show_extended, columns)


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
    last_quote_revision = -1
    last_sent_at = 0.0
    payload: dict[str, Any] | None = None
    try:
        while True:
            revision = get_futu_kline_revision(symbol, interval)
            quote_revision = get_futu_quote_revision(symbol)
            now = asyncio.get_running_loop().time()
            kline_changed = last_revision < 0 or revision != last_revision
            quote_changed = quote_revision != last_quote_revision
            if kline_changed and (last_revision < 0 or now - last_sent_at >= 1.0):
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
                payload["quote_revision"] = quote_revision
                subscription = futu_subscription_status()
                payload["kline_subscription"] = (
                    not subscription.get("kline_error")
                    and symbol in set(subscription.get("kline_symbols") or [])
                )
                await websocket.send_json(payload)
                last_revision = revision
                last_quote_revision = quote_revision
                last_sent_at = now
            elif quote_changed and payload is not None and now - last_sent_at >= 0.75:
                quote = get_futu_subscription_quotes().get(symbol) or {}
                latest_price = float(quote.get("price") or 0.0)
                latest_change_pct = float(quote["change_pct"]) if quote.get("change_pct") is not None else None
                if latest_price > 0:
                    _patch_latest_price(payload, latest_price, latest_change_pct)
                    payload["quote_revision"] = quote_revision
                    await websocket.send_json(payload)
                    last_quote_revision = quote_revision
                    last_sent_at = now
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return

