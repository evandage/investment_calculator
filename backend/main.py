from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import sys
import threading
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from . import config as config_module
from . import market_data as market_data_module
from . import portfolio as portfolio_module
from . import storage as storage_module
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
from .portfolio import (
    add_fx_conversion_record,
    build_dashboard,
    confirm_trades,
    delete_fx_conversion_record,
    delete_trade_record,
    save_rebalance_budget,
    start_performance_history_scheduler,
)
from .storage import (
    load_balances,
    load_closed_satellite_pnl,
    load_drawdown_episode_store,
    load_holdings,
    load_portfolio_adjustments,
    load_portfolio_snapshot_ledger,
    load_satellite_targets,
    load_swing_positions,
    load_trade_records,
    save_balances,
    save_closed_satellite_pnl,
    save_holdings,
    save_satellite_targets,
    save_swing_position,
    record_portfolio_adjustment,
)
from .drawdown_recalculation import start_monthly_drawdown_scheduler

TZ_SHANGHAI = config_module.TZ_SHANGHAI
TZ_NEW_YORK = ZoneInfo("America/New_York")


class HoldingPayload(BaseModel):
    holdings: dict[str, dict[str, float]]


class BalancesPayload(BaseModel):
    balances: dict[str, float]


class SatelliteTargetsPayload(BaseModel):
    targets: dict[str, float]


class SatelliteUniverseItem(BaseModel):
    symbol: str
    label: str | None = None
    target_pct: float = 0.0


class SatelliteUniversePayload(BaseModel):
    items: list[SatelliteUniverseItem]


class ExecutionItem(BaseModel):
    symbol: str
    action: str = "buy"
    trade_date: str | None = None
    amount_usd: float
    shares: float
    intensity: str = "normal"


class ExecutionPayload(BaseModel):
    user_id: str = "evan"
    executions: list[ExecutionItem]


class RebalanceBudgetPayload(BaseModel):
    user_id: str = "evan"
    planned_cash_by_month: dict[str, float]


class SwingPositionPayload(BaseModel):
    shares: float = 0.0
    avg_cost: float = 0.0
    target_pct: float = 0.0
    stop_loss_min_pct: float = 0.0
    stop_loss_max_pct: float = 0.0
    take_profit_min_pct: float = 0.0
    take_profit_max_pct: float = 0.0


class DeleteTradePayload(BaseModel):
    user_id: str = "evan"


class FxConversionPayload(BaseModel):
    user_id: str = "evan"
    converted_date: str | None = None
    cny_amount: float
    usd_amount: float
    note: str = ""


def _chart_symbols() -> set[str]:
    return {"VOO", "QQQ", *config_module.SATELLITE_SYMBOLS, "510330.SS"}


def _chart_labels() -> dict[str, str]:
    symbols = ("VOO", "QQQ", *config_module.SATELLITE_SYMBOLS)
    labels = {
        symbol: config_module.ASSET_META.get(symbol, {}).get("label", symbol)
        for symbol in symbols
        if symbol in config_module.ASSET_META
    }
    labels["510330.SS"] = "沪深300ETF"
    return labels


def _chart_full_labels() -> dict[str, str]:
    return {
        "VOO": "Vanguard S&P 500 ETF",
        "QQQ": "Invesco QQQ Trust",
        "ISRG": "Intuitive Surgical",
        "TEM": "Tempus AI",
        "PLTR": "Palantir Technologies",
        "GOOGL": "Alphabet",
        "MSFT": "Microsoft",
        "AVGO": "Broadcom",
        "NVDA": "NVIDIA",
        "SGOV": "iShares 0-3 Month Treasury Bond ETF",
        "510330.SS": "Huatai-PineBridge CSI 300 ETF",
    }


def _refresh_satellite_runtime_config() -> None:
    for module in (storage_module, market_data_module, portfolio_module):
        if hasattr(module, "ALL_SYMBOLS"):
            module.ALL_SYMBOLS = config_module.ALL_SYMBOLS
        if hasattr(module, "SATELLITE_SYMBOLS"):
            module.SATELLITE_SYMBOLS = config_module.SATELLITE_SYMBOLS
        if hasattr(module, "USD_SYMBOLS"):
            module.USD_SYMBOLS = config_module.USD_SYMBOLS


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
    threading.Thread(target=start_futu_quote_subscription, daemon=True).start()
    threading.Thread(target=start_performance_history_scheduler, daemon=True).start()
    threading.Thread(target=start_monthly_drawdown_scheduler, daemon=True).start()


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
def dashboard(user_id: str = "evan", refresh: bool = False) -> dict[str, Any]:
    return build_dashboard(user_id, force_refresh=refresh)


@app.get("/api/ohlcv")
def ohlcv(
    symbol: str = "VOO",
    interval: str = "1d",
    show_extended: bool = True,
    selected_date: str = "",
) -> dict[str, Any]:
    return fetch_ohlcv(symbol, interval, show_extended, selected_date)


def _timestamp_for_lightweight(value: Any, interval: str = "5m", symbol: str = "") -> int | str:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if interval == "1d":
            return value.date().isoformat()
        if value.tzinfo is None:
            # Chart-board frames use naive timestamps in the local market
            # timezone. Treating a US historical frame as Shanghai time moves
            # it back twelve hours, which breaks selected-date minute charts.
            value = value.replace(tzinfo=TZ_SHANGHAI if str(symbol).endswith((".SS", ".SZ")) else TZ_NEW_YORK)
        return int(value.timestamp())
    text = str(value)
    if interval == "1d" or (" " not in text and "T" not in text):
        return text[:10]
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ_SHANGHAI if str(symbol).endswith((".SS", ".SZ")) else TZ_NEW_YORK)
        return int(parsed.timestamp())
    except Exception:
        return text


def _kline_header_change_pct(
    quote: dict[str, Any],
    price: float,
    interval: str,
    *,
    fallback_open: float | None = None,
) -> float | None:
    """Return the K-line header percentage versus the previous close.

    The header keeps one denominator across premarket, regular, postmarket,
    intraday intervals, and daily bars. This also preserves the original
    behavior for the China ETF benchmark.
    """
    # The header price follows the extended quote whenever one is available,
    # so its percentage must use the matching extended-session return.
    value = quote.get("extended_change_pct")
    if value is None:
        value = quote.get("change_pct")
    if value is not None:
        return float(value)
    try:
        previous_close = float(quote.get("prev_close") or 0.0)
        if price > 0 and previous_close > 0:
            return (price / previous_close - 1.0) * 100.0
    except (TypeError, ValueError):
        pass
    return None


def _default_avwap_mode(interval: str, symbol: str) -> str:
    if interval in {"5m", "15m"}:
        return "today_open"
    return "year_start" if symbol in {"VOO", "QQQ", "SGOV", "510330.SS"} else "earnings"


def _build_global_chart_board_light(
    interval: str = "15m",
    show_extended: bool = True,
    columns: int = 1,
    selected_date: str = "",
) -> dict[str, Any]:
    chart_api = importlib.import_module("chart_boards")
    chart_api.configure_market_provider("futu")
    key = interval if interval in {"1d", "15m", "5m"} else "15m"
    selected_day = selected_date[:10] if key != "1d" else ""
    cols = min(5, max(1, int(columns or 1)))
    labels = _chart_labels()
    full_labels = _chart_full_labels()
    symbols = list(labels.keys())
    quotes = get_futu_subscription_quotes()
    if not futu_subscription_status().get("started"):
        start_futu_quote_subscription(force=True)
        quotes = get_futu_subscription_quotes()
    # A single Futu history request can take several seconds when OpenD is
    # busy. Fetching the board symbols serially made the 9-symbol board exceed
    # the browser's 30s request deadline (9 × 8s history timeout). Run the
    # independent symbol requests concurrently and assemble the result in the
    # stable display order below.
    def load_bars(sym: str) -> list[dict[str, Any]]:
        try:
            return fetch_ohlcv(sym, key, show_extended, selected_day).get("bars") or []
        except Exception:
            return []

    # OpenD history queries are local socket requests and become much slower
    # when too many contexts are opened together. Keep this aligned with the
    # bounded history executor in backend.ohlcv.
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, max(1, len(symbols)))) as executor:
        bars_by_symbol = dict(zip(symbols, executor.map(load_bars, symbols)))

    charts: list[dict[str, Any]] = []
    for sym in symbols:
        bars = bars_by_symbol.get(sym) or []
        quote = quotes.get(sym) or {}
        last_close = float(bars[-1].get("close") or 0.0) if bars else 0.0
        latest_price = last_close if selected_day else float(quote.get("price") or last_close or 0.0)
        fallback_open = float(bars[0].get("open") or 0.0) if bars else None
        latest_change_pct = (
            (last_close / fallback_open - 1.0) * 100.0
            if selected_day and last_close > 0 and fallback_open and fallback_open > 0
            else _kline_header_change_pct(quote, latest_price, key, fallback_open=fallback_open)
        )
        candles = [
            {
                "time": _timestamp_for_lightweight(bar.get("time"), key, sym),
                "open": float(bar.get("open") or 0.0),
                "high": float(bar.get("high") or 0.0),
                "low": float(bar.get("low") or 0.0),
                "close": float(bar.get("close") or 0.0),
            }
            for bar in bars
            if bar.get("time") is not None
        ]
        volumes = [
            {
                "time": item["time"],
                "value": float(bar.get("volume") or 0.0),
                "color": "rgba(34, 197, 94, 0.28)" if item["close"] >= item["open"] else "rgba(239, 68, 68, 0.28)",
            }
            for item, bar in zip(candles, bars)
        ]
        charts.append(
            {
                "symbol": sym,
                "label": labels.get(sym, sym),
                "full_label": full_labels.get(sym, labels.get(sym, sym)),
                "candles": candles,
                "volumes": volumes,
                "latest_price": latest_price,
                "latest_change_pct": latest_change_pct,
                "selected_date": selected_day,
                "source": "lightweight",
            }
        )

    return {
        "symbol": "GLOBAL",
        "symbols": symbols,
        "interval": key,
        "source": "lightweight-global",
        "market_provider": chart_api.get_market_provider(),
        "show_extended": show_extended if key != "1d" else None,
        "selected_date": selected_day,
        "columns": cols,
        "charts": charts,
        "error": "",
    }


def _series_for_lightweight(series: Any, interval: str, symbol: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        clean = series.replace([float("inf"), float("-inf")], float("nan")).dropna()
    except Exception:
        clean = series
    for idx, value in clean.items():
        try:
            numeric = float(value)
        except Exception:
            continue
        if numeric != numeric:
            continue
        out.append({"time": _timestamp_for_lightweight(idx, interval, symbol), "value": numeric})
    return out


def _trade_markers_for_chart(
    symbol: str,
    interval: str,
    frame: Any,
    candles: list[dict[str, Any]],
    records: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate the trade ledger into one buy/sell marker per trading day."""
    if frame is None or getattr(frame, "empty", True) or not candles:
        return []

    candle_time_by_day: dict[str, Any] = {}
    candle_order_by_day: dict[str, int] = {}
    for position, (idx, candle) in enumerate(zip(frame.index, candles)):
        try:
            day = idx.date().isoformat()
        except AttributeError:
            day = str(idx)[:10]
        if day and day not in candle_time_by_day:
            candle_time_by_day[day] = candle.get("time")
            candle_order_by_day[day] = position

    grouped: dict[str, dict[str, Any]] = {}
    for record in records if records is not None else load_trade_records("evan"):
        if str(record.get("symbol") or "").upper() != symbol:
            continue
        trade_date = str(record.get("trade_date") or record.get("date") or "")[:10]
        action = str(record.get("action") or "buy").lower()
        if trade_date not in candle_time_by_day or action not in {"buy", "sell"}:
            continue
        group = grouped.setdefault(
            trade_date,
            {
                "trade_date": trade_date,
                "buy_count": 0,
                "buy_shares": 0.0,
                "buy_amount": 0.0,
                "sell_count": 0,
                "sell_shares": 0.0,
                "sell_amount": 0.0,
            },
        )
        group[f"{action}_count"] += 1
        group[f"{action}_shares"] += float(record.get("shares") or 0.0)
        group[f"{action}_amount"] += float(record.get("amount_usd") or record.get("amount") or 0.0)

    markers: list[dict[str, Any]] = []
    for trade_date, group in grouped.items():
        buy_count = int(group["buy_count"])
        sell_count = int(group["sell_count"])
        action = "trade" if buy_count and sell_count else ("buy" if buy_count else "sell")
        shares = float(group[f"{action}_shares"]) if action != "trade" else float(group["buy_shares"]) + float(group["sell_shares"])
        amount = float(group[f"{action}_amount"]) if action != "trade" else float(group["buy_amount"]) + float(group["sell_amount"])
        markers.append(
            {
                "id": f"trade-{symbol}-{trade_date}-{action}",
                "time": candle_time_by_day[trade_date],
                "trade_date": trade_date,
                "action": action,
                "position": "belowBar" if action == "buy" else "aboveBar",
                "shape": "circle",
                "color": "#22c55e" if action == "buy" else ("#ef4444" if action == "sell" else "#f59e0b"),
                "text": "Ⓑ" if action == "buy" else ("Ⓢ" if action == "sell" else "Ⓣ"),
                "count": buy_count + sell_count,
                "shares": shares,
                "amount": amount,
                "average_price": amount / shares if action != "trade" and shares > 0 else None,
                "buy_count": buy_count,
                "buy_shares": float(group["buy_shares"]),
                "buy_amount": float(group["buy_amount"]),
                "buy_average_price": (
                    float(group["buy_amount"]) / float(group["buy_shares"])
                    if float(group["buy_shares"]) > 0
                    else None
                ),
                "sell_count": sell_count,
                "sell_shares": float(group["sell_shares"]),
                "sell_amount": float(group["sell_amount"]),
                "sell_average_price": (
                    float(group["sell_amount"]) / float(group["sell_shares"])
                    if float(group["sell_shares"]) > 0
                    else None
                ),
                "_order": candle_order_by_day[trade_date],
            }
        )
    markers.sort(key=lambda item: int(item["_order"]))
    for marker in markers:
        marker.pop("_order", None)
    return markers


def _build_chart_board_light(
    symbol: str = "VOO",
    interval: str = "15m",
    avwap_mode: str | None = None,
    show_extended: bool = True,
    custom_anchor_date: str | None = None,
    selected_date: str = "",
) -> dict[str, Any]:
    sym = str(symbol or "VOO").upper()
    if sym not in _chart_symbols():
        sym = "VOO"
    key = interval if interval in {"1d", "15m", "5m"} else "15m"
    selected_day = selected_date[:10] if key != "1d" else ""
    chart_api = importlib.import_module("chart_boards")
    chart_api.configure_market_provider("futu")
    labels = _chart_labels()
    holding = load_holdings().get(sym, {})
    shares = float(holding.get("shares", 0.0) or 0.0)
    avg_cost = float(holding.get("avg_cost", 0.0) or 0.0)
    user_avg_cost = avg_cost if shares > 0 and avg_cost > 0 else None
    quote = get_futu_subscription_quotes().get(sym) or {}
    latest_price = float(quote.get("price") or 0.0)
    latest_change_pct = None
    session_open = float(quote.get("open_price") or 0.0)
    previous_close = float(quote.get("prev_close") or 0.0)
    effective_avwap_mode = avwap_mode or _default_avwap_mode(key, sym)
    if sym in {"VOO", "QQQ", "SGOV", "510330.SS"} and effective_avwap_mode == "earnings":
        effective_avwap_mode = "high_60d"
    cycle_start_date = None
    if effective_avwap_mode in {"cycle_high", "cycle_low"}:
        episode_store = load_drawdown_episode_store("evan")
        episode_state = dict(episode_store.get("episodes") or {}).get(sym) or {}
        cycle_start_date = episode_state.get("anchor_date")

    try:
        period = "5y" if key == "1d" else "2d"
        if selected_day:
            selected_bars = fetch_ohlcv(sym, key, show_extended, selected_day).get("bars") or []
            df = pd.DataFrame(selected_bars).rename(
                columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
            )
            if not df.empty:
                df.index = pd.to_datetime(df.pop("time"), unit="s", utc=True, errors="coerce")
                df = df.loc[~df.index.isna(), ["Open", "High", "Low", "Close", "Volume"]]
                df = chart_api._normalize_plot_time_index(df, sym)
        else:
            df = chart_api.fetch_ohlcv(sym, key, period, cache_only=False)
        full_df = df.copy()
        if key != "1d" and not selected_day:
            if show_extended:
                df, _ = chart_api.slice_intraday_today_or_yesterday(
                    df,
                    sym,
                    min_current_bars=12 if key == "5m" else 4,
                    # Do not add yesterday's bars while today's extended
                    # session has only produced a few candles.
                    include_previous_context=False,
                )
            else:
                df, _ = chart_api.slice_regular_intraday_with_context(
                    df,
                    sym,
                    min_current_bars=12 if key == "5m" else 4,
                    # Intraday boards should show the active trading day only.
                    # Adding yesterday's bars at the open makes it look as if
                    # the board has not refreshed yet.
                    include_previous_context=False,
                )
        if df.empty:
            return {
                "symbol": sym,
                "label": labels.get(sym, sym),
                "interval": key,
                "source": "lightweight-single",
                "market_provider": chart_api.get_market_provider(),
                "show_extended": show_extended,
                "selected_date": selected_day,
                "user_avg_cost": user_avg_cost,
                "latest_price": None if selected_day else (latest_price if latest_price > 0 else None),
                "latest_change_pct": latest_change_pct,
                "session_open": None if selected_day else (session_open if session_open > 0 else None),
                "previous_close": None if selected_day else (previous_close if previous_close > 0 else None),
                "candles": [],
                "volumes": [],
                "overlays": {},
                "indicators": {},
                "error": "",
            }

        fallback_open = float(df.iloc[0]["Open"]) if key != "1d" else None
        latest_change_pct = _kline_header_change_pct(
            quote,
            latest_price,
            key,
            fallback_open=fallback_open,
        )

        earnings_anchor = None
        if key == "1d" and sym not in {"VOO", "QQQ", "SGOV", "510330.SS"}:
            earnings_date = chart_api.latest_earnings_anchor(sym)
            if earnings_date is not None:
                earnings_anchor = earnings_date.strftime("%Y-%m-%d")

        avwap, avwap_upper, avwap_lower, avwap_anchor, avwap_label = chart_api.anchored_vwap_and_bands(
            sym,
            df,
            effective_avwap_mode,
            cache_only=False,
            cycle_start_date=cycle_start_date,
            custom_anchor_date=custom_anchor_date,
        )
        if key != "1d" and effective_avwap_mode == "today_open":
            atr_band = chart_api.atr_series(df, 14).reindex(avwap.index)
            avwap_upper = avwap + atr_band
            avwap_lower = avwap - atr_band
        try:
            avwap_clean = avwap.replace([float("inf"), float("-inf")], float("nan")).dropna()
            avwap_value = float(avwap_clean.iloc[-1]) if not avwap_clean.empty else None
        except Exception:
            avwap_value = None
        # Keep the API profile consistent with the candles returned to the
        # client.  When extended hours are enabled, those bars must also be
        # represented in the profile.
        profile_source = df
        vp_price, vp_vol, vp_low, vp_high, _ = chart_api._volume_profile_by_price(profile_source, bins=24)
        max_profile_volume = float(vp_vol.max()) if not vp_vol.empty else 0.0
        volume_profile = [
            {
                "price": float(price),
                "low": float(low),
                "high": float(high),
                "volume": float(volume),
                "pct": float(volume) / max_profile_volume if max_profile_volume > 0 else 0.0,
            }
            for price, low, high, volume in zip(vp_price.values, vp_low.values, vp_high.values, vp_vol.values)
        ]
        close = df["Close"]
        daily_ema20 = chart_api.ema(full_df["Close"], 20).reindex(df.index) if key == "1d" and not full_df.empty else close.iloc[0:0]
        daily_ma50 = full_df["Close"].rolling(50).mean().reindex(df.index) if key == "1d" and not full_df.empty else close.iloc[0:0]
        daily_ma200 = full_df["Close"].rolling(200).mean().reindex(df.index) if key == "1d" and not full_df.empty else close.iloc[0:0]
        rsi_period = 7 if key == "5m" else 14
        rsi_series = chart_api.rsi(close, rsi_period)
        rsi_ma = chart_api.ema(rsi_series, 9)
        macd_line, macd_signal, macd_hist = chart_api.macd_series(close)
        last_close = float(close.iloc[-1]) if len(close) else 0.0
        if selected_day:
            latest_price = last_close
            previous_close = 0.0
            session_open = float(df.iloc[0]["Open"]) if len(df) else 0.0
            latest_change_pct = (
                (last_close / session_open - 1.0) * 100.0
                if last_close > 0 and session_open > 0
                else None
            )
        elif latest_price <= 0:
            latest_price = last_close
        candles = [
            {
                "time": _timestamp_for_lightweight(idx, key, sym),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            }
            for idx, row in df.iterrows()
        ]
        volumes = [
            {
                "time": item["time"],
                "value": float(row.get("Volume") or 0.0),
                "color": "rgba(34, 197, 94, 0.28)" if item["close"] >= item["open"] else "rgba(239, 68, 68, 0.28)",
            }
            for item, (_, row) in zip(candles, df.iterrows())
        ]
        trade_markers = _trade_markers_for_chart(sym, key, df, candles)
        return {
            "symbol": sym,
            "label": labels.get(sym, sym),
            "interval": key,
            "source": "lightweight-single",
            "market_provider": chart_api.get_market_provider(),
            "show_extended": show_extended if key != "1d" else None,
            "selected_date": selected_day,
            "user_avg_cost": user_avg_cost,
            "avwap_mode": effective_avwap_mode,
            "avwap_label": avwap_label,
            "avwap_anchor": None if effective_avwap_mode == "none" else avwap_anchor.strftime("%Y-%m-%d"),
            "earnings_anchor": earnings_anchor,
            "avwap_value": avwap_value,
            "rsi_period": rsi_period,
            "latest_price": latest_price if latest_price > 0 else None,
            "latest_change_pct": latest_change_pct,
            "session_open": session_open if session_open > 0 else None,
            "previous_close": previous_close if previous_close > 0 else None,
            "candles": candles,
            "volumes": volumes,
            "trade_markers": trade_markers,
            "overlays": {
                "avwap": _series_for_lightweight(avwap, key, sym),
                "avwap_upper": _series_for_lightweight(avwap_upper, key, sym),
                "avwap_lower": _series_for_lightweight(avwap_lower, key, sym),
                "ema20": _series_for_lightweight(daily_ema20, key, sym),
                "ma50": _series_for_lightweight(daily_ma50, key, sym),
                "ma200": _series_for_lightweight(daily_ma200, key, sym),
            },
            "volume_profile": volume_profile,
            "indicators": {
                "rsi": _series_for_lightweight(rsi_series, key, sym),
                "rsi_ma": _series_for_lightweight(rsi_ma, key, sym),
                "macd": _series_for_lightweight(macd_line, key, sym),
                "macd_signal": _series_for_lightweight(macd_signal, key, sym),
                "macd_hist": _series_for_lightweight(macd_hist, key, sym),
            },
            "error": "",
        }
    except Exception as exc:
        return {"symbol": sym, "interval": key, "source": "lightweight-single", "error": str(exc)}

@app.get("/api/chart-board-light")
def chart_board_light(
    symbol: str = "VOO",
    interval: str = "15m",
    avwap_mode: str | None = None,
    show_extended: bool = True,
    custom_anchor_date: str | None = None,
    selected_date: str = "",
) -> dict[str, Any]:
    return _build_chart_board_light(
        symbol,
        interval,
        avwap_mode,
        show_extended,
        custom_anchor_date,
        selected_date,
    )


@app.get("/api/chart-board-global-light")
def chart_board_global_light(
    interval: str = "15m",
    show_extended: bool = True,
    columns: int = 1,
    selected_date: str = "",
) -> dict[str, Any]:
    return _build_global_chart_board_light(interval, show_extended, columns, selected_date)


@app.get("/api/holdings")
def holdings() -> dict[str, Any]:
    return {"holdings": load_holdings(), "balances": load_balances()}


@app.put("/api/holdings")
def update_holdings(payload: HoldingPayload) -> dict[str, Any]:
    before = load_holdings()
    save_holdings(payload.holdings)
    after = load_holdings()
    effective_date = portfolio_module.performance_history_date()
    adjustment = record_portfolio_adjustment(
        "evan",
        "holdings",
        effective_date,
        before,
        after,
        "exact_holdings_anchor_reconciliation",
        allow_noop=True,
    )
    if adjustment:
        portfolio_module.invalidate_performance_history_from("evan", effective_date)
    # Rebuild the live P&L and performance payload before acknowledging the
    # save.  The exact anchor remains authoritative for future EOD snapshots;
    # this call refreshes the current point immediately without finalizing an
    # incomplete trading day.
    refreshed_dashboard = build_dashboard("evan")
    return {
        "saved": True,
        "holdings": after,
        "adjustment": adjustment,
        "recalculated": True,
        "dashboard": refreshed_dashboard,
    }


@app.put("/api/balances")
def update_balances(payload: BalancesPayload) -> dict[str, Any]:
    before = load_balances()
    requested = dict(payload.balances)
    # A manual cash edit is normally an external deposit/withdrawal, while a
    # simultaneous realized-P&L or dividend edit is income already contained
    # in that cash.  Move only the principal portion into the cash basis unless
    # the user explicitly supplies a different basis value.
    basis_rules = {
        "USD": ("cash_usd", "cash_cost_basis_usd", ("realized_usd", "voo_dividend_usd", "sgov_dividend_usd")),
        "CNY": ("cash_cny", "cash_cost_basis_cny", ("realized_cny",)),
    }
    for cash_key, basis_key, income_keys in basis_rules.values():
        old_basis = float(before.get(basis_key, 0.0) or 0.0)
        try:
            requested_basis = float(requested.get(basis_key, old_basis) or 0.0)
        except (TypeError, ValueError):
            requested_basis = old_basis
        explicitly_changed = basis_key in requested and abs(requested_basis - old_basis) > 1e-9
        if explicitly_changed:
            continue
        old_cash = float(before.get(cash_key, 0.0) or 0.0)
        new_cash = float(requested.get(cash_key, old_cash) or 0.0)
        income_delta = sum(
            float(requested.get(key, before.get(key, 0.0)) or 0.0)
            - float(before.get(key, 0.0) or 0.0)
            for key in income_keys
        )
        requested[basis_key] = old_basis + (new_cash - old_cash) - income_delta
    save_balances(requested)
    after = load_balances()
    effective_date = portfolio_module.performance_history_date()
    adjustment = record_portfolio_adjustment(
        "evan",
        "balances",
        effective_date,
        before,
        after,
        "manual_edit",
        {"reconstruct_from_date": effective_date},
    )
    if adjustment:
        portfolio_module.invalidate_performance_history_from("evan", effective_date)
    return {"saved": True, "balances": after, "adjustment": adjustment}


@app.get("/api/portfolio-audit")
def portfolio_audit(user_id: str = "evan") -> dict[str, Any]:
    ledger = load_portfolio_snapshot_ledger(user_id)
    latest_by_date: dict[str, dict[str, Any]] = {}
    for item in ledger:
        day = str(item.get("date") or "")
        if not day:
            continue
        if int(item.get("revision", 0) or 0) >= int((latest_by_date.get(day) or {}).get("revision", 0) or 0):
            latest_by_date[day] = item
    return {
        "adjustments": load_portfolio_adjustments(user_id),
        "snapshot_revisions": ledger,
        "latest_snapshots": [latest_by_date[day] for day in sorted(latest_by_date)],
    }


@app.get("/api/satellite-targets")
def satellite_targets() -> dict[str, Any]:
    return {"targets": load_satellite_targets()}


@app.put("/api/satellite-targets")
def update_satellite_targets(payload: SatelliteTargetsPayload) -> dict[str, Any]:
    save_satellite_targets(payload.targets)
    return {"saved": True, "targets": load_satellite_targets()}


@app.get("/api/swing-positions")
def swing_positions() -> dict[str, Any]:
    return {"positions": load_swing_positions()}


@app.put("/api/swing-positions/{symbol}")
def update_swing_position(symbol: str, payload: SwingPositionPayload) -> dict[str, Any]:
    try:
        positions = save_swing_position(symbol, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"saved": True, "positions": positions}


@app.get("/api/satellite-universe")
def satellite_universe() -> dict[str, Any]:
    return {"items": config_module.load_satellite_universe_config()}


@app.put("/api/satellite-universe")
def update_satellite_universe(payload: SatelliteUniversePayload) -> dict[str, Any]:
    previous_items = config_module.load_satellite_universe_config()
    previous_symbols = {item["symbol"] for item in previous_items}
    requested_items = [item.model_dump() for item in payload.items]
    requested_symbols = {str(item.get("symbol") or "").strip().upper() for item in requested_items}
    removed_symbols = previous_symbols - requested_symbols
    if removed_symbols:
        holdings_before = load_holdings()
        trade_records = load_trade_records()
        archived_rows = load_closed_satellite_pnl()
        needs_quotes = any(float((holdings_before.get(sym) or {}).get("shares", 0.0) or 0.0) > 0 for sym in removed_symbols)
        quotes = (fetch_quotes().get("quotes") or {}) if needs_quotes else {}
        closed_at = datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")
        for sym in removed_symbols:
            realized_pnl = sum(
                float(record.get("realized_pnl", 0.0) or 0.0)
                for record in trade_records
                if record.get("symbol") == sym and record.get("action") == "sell"
            )
            holding = holdings_before.get(sym) or {}
            shares = float(holding.get("shares", 0.0) or 0.0)
            avg_cost = float(holding.get("avg_cost", 0.0) or 0.0)
            price = float((quotes.get(sym) or {}).get("price", 0.0) or 0.0)
            remaining_pnl = shares * (price - avg_cost) if shares > 0 and price > 0 else 0.0
            archived_pnl = realized_pnl + remaining_pnl
            if abs(archived_pnl) > 1e-9 or sym not in archived_rows:
                archived_rows[sym] = {
                    "symbol": sym,
                    "label": next((item.get("label") or sym for item in previous_items if item["symbol"] == sym), sym),
                    "pnl_usd": archived_pnl,
                    "closed_at": closed_at,
                    "included_in_realized": shares <= 1e-9,
                }
        save_closed_satellite_pnl(archived_rows)

    items = config_module.save_satellite_universe_config(requested_items)
    _refresh_satellite_runtime_config()
    save_holdings(load_holdings())
    save_satellite_targets({item["symbol"]: float(item.get("target_pct") or 0.0) for item in items})
    if "chart_boards" in sys.modules:
        importlib.reload(sys.modules["chart_boards"])
    stop_futu_quote_subscription()
    threading.Thread(target=start_futu_quote_subscription, kwargs={"force": True}, daemon=True).start()
    return {
        "saved": True,
        "items": items,
        "holdings": load_holdings(),
        "targets": load_satellite_targets(),
        "archived_pnl": load_closed_satellite_pnl(),
    }


@app.post("/api/rebalance/confirm")
def confirm_execution(payload: ExecutionPayload) -> dict[str, Any]:
    try:
        result = confirm_trades(payload.user_id, [item.model_dump() for item in payload.executions])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result["dashboard"] = build_dashboard(payload.user_id)
    return result


@app.delete("/api/trades/{trade_id}")
def delete_trade(trade_id: str, payload: DeleteTradePayload) -> dict[str, Any]:
    try:
        result = delete_trade_record(payload.user_id, trade_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result["dashboard"] = build_dashboard(payload.user_id)
    return result


@app.post("/api/fx-conversions")
def add_fx_conversion(payload: FxConversionPayload) -> dict[str, Any]:
    try:
        result = add_fx_conversion_record(payload.user_id, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result["dashboard"] = build_dashboard(payload.user_id)
    return result


@app.delete("/api/fx-conversions/{record_id}")
def delete_fx_conversion(record_id: str, payload: DeleteTradePayload) -> dict[str, Any]:
    try:
        result = delete_fx_conversion_record(payload.user_id, record_id)
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



def _light_revision_snapshot(symbols: list[str], interval: str) -> dict[str, tuple[int, int]]:
    return {
        symbol: (get_futu_kline_revision(symbol, interval), get_futu_quote_revision(symbol))
        for symbol in symbols
    }


@app.websocket("/ws/chart-board-light")
async def chart_board_light_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    symbol = str(websocket.query_params.get("symbol", "VOO")).upper()
    interval = str(websocket.query_params.get("interval", "15m"))
    avwap_mode = str(websocket.query_params.get("avwap_mode") or _default_avwap_mode(interval, symbol))
    custom_anchor_date = str(websocket.query_params.get("custom_anchor_date") or "") or None
    selected_date = str(websocket.query_params.get("selected_date") or "")[:10]
    show_extended = str(websocket.query_params.get("show_extended", "true")).lower() not in {"0", "false", "no"}
    if symbol not in _chart_labels():
        symbol = "VOO"
    if interval not in {"15m", "5m"}:
        await websocket.close()
        return

    last_revision: tuple[int, int] | None = None
    last_sent_at = 0.0
    try:
        while True:
            current = (get_futu_kline_revision(symbol, interval), get_futu_quote_revision(symbol))
            now = asyncio.get_running_loop().time()
            if last_revision is None or (current != last_revision and now - last_sent_at >= 0.75):
                payload = await asyncio.to_thread(
                    _build_chart_board_light,
                    symbol,
                    interval,
                    avwap_mode,
                    show_extended,
                    custom_anchor_date,
                    selected_date,
                )
                payload["realtime"] = True
                payload["revision"] = current[0]
                payload["quote_revision"] = current[1]
                await websocket.send_json(payload)
                last_revision = current
                last_sent_at = now
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return


@app.websocket("/ws/chart-board-global-light")
async def chart_board_global_light_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    interval = str(websocket.query_params.get("interval", "15m"))
    show_extended = str(websocket.query_params.get("show_extended", "true")).lower() not in {"0", "false", "no"}
    selected_date = str(websocket.query_params.get("selected_date") or "")[:10]
    try:
        columns = int(websocket.query_params.get("columns", "1"))
    except Exception:
        columns = 1
    if interval not in {"15m", "5m"}:
        await websocket.close()
        return

    symbols = list(_chart_labels().keys())
    last_revision: dict[str, tuple[int, int]] | None = None
    last_sent_at = 0.0
    try:
        while True:
            current = _light_revision_snapshot(symbols, interval)
            now = asyncio.get_running_loop().time()
            if last_revision is None or (current != last_revision and now - last_sent_at >= 0.75):
                payload = await asyncio.to_thread(
                    _build_global_chart_board_light,
                    interval,
                    show_extended,
                    columns,
                    selected_date,
                )
                payload["realtime"] = True
                payload["revisions"] = current
                await websocket.send_json(payload)
                last_revision = current
                last_sent_at = now
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return
