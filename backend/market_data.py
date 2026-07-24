from __future__ import annotations

import json
import os
import re
import socket
import threading
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

from . import config as app_config
from .config import (
    HTTP_TIMEOUT,
    REQUEST_HEADERS,
    ROOT_DIR,
    TZ_SHANGHAI,
)

_QUOTES_CACHE: dict[str, Any] | None = None
_QUOTES_CACHE_AT = 0.0
_QUOTES_CACHE_TTL_SECONDS = 1.0
_FUND_QUOTES_CACHE: dict[str, tuple[dict[str, Any], float]] = {}
_FUND_QUOTES_CACHE_LOADED = False
_FUND_QUOTES_CACHE_FILE = ROOT_DIR / ".fund_quotes_cache.json"
_FUND_QUOTES_TTL_SECONDS = 300
_FX_CACHE: dict[str, Any] | None = None
_FX_CACHE_AT = 0.0
_FX_CACHE_FILE = ROOT_DIR / ".fx_rate_cache.json"
_FX_CACHE_TTL_SECONDS = 60
_FX_HISTORY_CACHE: dict[str, float] = {}
_FX_HISTORY_CACHE_AT = 0.0
_FX_HISTORY_CACHE_TTL_SECONDS = 900
_VALUATION_METRICS_CACHE: dict[str, dict[str, float]] | None = None
_FORWARD_PE_CACHE_AT = 0.0
_FUTU_SUB_LOCK = threading.RLock()
_FUTU_SUB_CTX: Any | None = None
_FUTU_SUB_STARTED = False
_FUTU_SUB_LAST_ERROR = ""
_FUTU_SUB_QUOTES: dict[str, dict[str, Any]] = {}
_FUTU_SUB_UPDATED_AT: dict[str, float] = {}
_FUTU_SUB_QUOTE_REVISIONS: dict[str, int] = {}
_FUTU_SUB_TTL_SECONDS = 300.0
_FUTU_SUB_KLINES: dict[tuple[str, str], dict[str, Any]] = {}
_FUTU_SUB_KLINE_REVISIONS: dict[tuple[str, str], int] = {}
_FUTU_SUB_KLINE_ERROR = ""
_FUTU_SUB_TICKER_ERROR = ""
NY_TZ = ZoneInfo("America/New_York")
_EXTENDED_US_SESSIONS = frozenset({"premarket", "postmarket", "overnight"})


def _usd_symbols() -> tuple[str, ...]:
    return tuple(app_config.USD_SYMBOLS)


def _satellite_symbols() -> tuple[str, ...]:
    return tuple(app_config.SATELLITE_SYMBOLS)


def _futu_subscribe_symbols() -> tuple[str, ...]:
    # 510330.SS is the dashboard's CSI 300 ETF benchmark; include it in the
    # same QUOTE/K-line subscription so the single-symbol chart can be live.
    return tuple(
        sym
        for sym in dict.fromkeys((*_usd_symbols(), "510330.SS"))
        if sym in app_config.FUTU_US
    )


def futu_opend_config() -> tuple[str, int]:
    host = os.getenv("FUTU_OPEND_HOST", "127.0.0.1").strip() or "127.0.0.1"
    try:
        port = int(os.getenv("FUTU_OPEND_PORT", "11111"))
    except (TypeError, ValueError):
        port = 11111
    return host, port


def is_futu_opend_available() -> bool:
    host, port = futu_opend_config()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.6)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _coerce_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        val = row.get(key, default)
    except AttributeError:
        try:
            val = row[key]
        except Exception:
            return default
    try:
        import pandas as pd

        if pd.isna(val):
            return default
    except Exception:
        pass
    return val


def _infer_us_session(state: str = "") -> str:
    now = datetime.now(NY_TZ)
    normalized_state = str(state or "").lower()
    weekday = now.weekday()
    minutes = now.hour * 60 + now.minute

    clock_session = "closed"
    if weekday == 5:
        clock_session = "closed"
    elif weekday == 6 and minutes < 20 * 60:
        clock_session = "closed"
    elif weekday == 4 and minutes >= 20 * 60:
        clock_session = "closed"
    elif 4 * 60 <= minutes < 9 * 60 + 30:
        clock_session = "premarket"
    elif 9 * 60 + 30 <= minutes < 16 * 60:
        clock_session = "regular"
    elif 16 * 60 <= minutes < 20 * 60:
        clock_session = "postmarket"
    else:
        clock_session = "overnight"

    if any(key in normalized_state for key in ("pre_market", "premarket", "pre-market")):
        return "premarket"
    if any(key in normalized_state for key in ("after_hours", "after_hour", "post_market", "postmarket", "after-hours")):
        return "postmarket"
    if "overnight" in normalized_state:
        return "overnight"
    if "open" in normalized_state and not any(key in normalized_state for key in ("pre", "after", "post", "overnight")):
        return "regular"
    if any(key in normalized_state for key in ("closed", "close", "rest")):
        return clock_session if clock_session == "regular" else "closed"
    return clock_session


def _pct_from_base(price: float | None, base: float | None) -> float | None:
    if not price or not base or base <= 0:
        return None
    return (price / base - 1.0) * 100.0


def _price_matches_change_pct(price: float | None, base: float | None, pct: float | None) -> bool:
    if not price or not base or base <= 0 or pct is None:
        return False
    implied = (price / base - 1.0) * 100.0
    return abs(implied - pct) <= max(0.15, abs(pct) * 0.2)


def _is_us_futu_symbol(sym: str) -> bool:
    """Return whether a dashboard symbol maps to Futu's US market."""
    return str(app_config.FUTU_US.get(sym, "")).upper().startswith("US.")


def _build_futu_quote(sym: str, row: Any, state: str = "") -> dict[str, Any] | None:
    last_price = _coerce_float(_row_get(row, "last_price") or _row_get(row, "cur_price") or _row_get(row, "price"))
    prev_close = _coerce_float(_row_get(row, "prev_close_price") or _row_get(row, "prev_close"))
    open_price = _coerce_float(_row_get(row, "open_price") or _row_get(row, "open"))
    pre_price = _coerce_float(
        _row_get(row, "pre_market_price")
        or _row_get(row, "pre_price")
        or _row_get(row, "preMarketPrice")
    )
    pre_change_pct = _coerce_float(_row_get(row, "pre_change_rate"))
    after_price = _coerce_float(
        _row_get(row, "after_market_price")
        or _row_get(row, "after_hours_price")
        or _row_get(row, "after_price")
        or _row_get(row, "postMarketPrice")
    )
    after_change_pct = _coerce_float(_row_get(row, "after_change_rate"))
    overnight_price = _coerce_float(_row_get(row, "overnight_price"))
    overnight_change_pct = _coerce_float(_row_get(row, "overnight_change_rate"))
    # Futu QUOTE pushes for China securities do not carry US extended-hours
    # fields. Running them through the New York session clock mislabeled the
    # CSI 300 ETF as "overnight", after which ticker pushes manufactured a
    # zero extended-session return that hid its real daily change.
    is_us_symbol = _is_us_futu_symbol(sym)
    session = _infer_us_session(state) if is_us_symbol else "regular"
    extended_price: float | None = None
    extended_change_pct: float | None = None
    if session == "premarket":
        extended_price, extended_change_pct = pre_price, pre_change_pct
    elif session == "overnight":
        extended_price, extended_change_pct = overnight_price, overnight_change_pct
        if (
            not _price_matches_change_pct(overnight_price, last_price, overnight_change_pct)
            and _price_matches_change_pct(after_price, last_price, after_change_pct)
        ):
            extended_price, extended_change_pct = after_price, after_change_pct
    elif session == "postmarket":
        extended_price, extended_change_pct = after_price, after_change_pct
    elif session == "closed":
        # Freeze the last post-market print while the market is closed. Futu's
        # overnight fields describe the session before the regular close, so
        # the post-market print is the latest completed extended-hours quote.
        extended_price, extended_change_pct = after_price, after_change_pct
    if not last_price or last_price <= 0:
        return None
    base = prev_close if prev_close and prev_close > 0 else last_price
    price = extended_price if session != "regular" and extended_price and extended_price > 0 else last_price
    if session not in {"regular", "closed"} and extended_change_pct is None:
        extended_change_pct = _pct_from_base(extended_price, last_price)
    if not extended_price or extended_price <= 0 or abs(extended_price - last_price) <= 1e-9:
        extended_price = None
        extended_change_pct = None
    return {
        "symbol": sym,
        "price": price,
        "prev_close": base,
        "open_price": open_price if open_price and open_price > 0 else None,
        "regular_price": last_price,
        "change_pct": (price / base - 1.0) * 100.0 if base > 0 else 0.0,
        "regular_change_pct": (last_price / base - 1.0) * 100.0 if base > 0 else 0.0,
        "extended_price": extended_price,
        "extended_change_pct": extended_change_pct,
        "session": session,
        "source": "Futu OpenD 订阅",
    }


def _merge_futu_subscription_quote(
    previous: dict[str, Any] | None,
    incoming: dict[str, Any],
) -> dict[str, Any]:
    """Keep a valid extended quote across sparse QUOTE pushes in one session."""
    merged = dict(incoming)
    prior = previous or {}
    session = str(merged.get("session") or "").lower()
    prior_session = str(prior.get("session") or "").lower()
    if (
        session in (*_EXTENDED_US_SESSIONS, "closed")
        and (session == prior_session or session == "closed")
        and _coerce_float(merged.get("extended_price")) is None
    ):
        prior_extended_price = _coerce_float(prior.get("extended_price"))
        if prior_extended_price and prior_extended_price > 0:
            merged["extended_price"] = prior_extended_price
            merged["extended_change_pct"] = _coerce_float(prior.get("extended_change_pct"))
            merged["price"] = prior_extended_price
            prev_close = _coerce_float(merged.get("prev_close"))
            if prev_close and prev_close > 0:
                merged["change_pct"] = (prior_extended_price / prev_close - 1.0) * 100.0
    return merged


def _apply_futu_ticker_price(quote: dict[str, Any], sym: str, price: float) -> dict[str, Any]:
    """Apply a trade tick using regular/extended-session field semantics."""
    updated = dict(quote)
    updated["symbol"] = sym
    updated["price"] = price
    is_us_symbol = _is_us_futu_symbol(sym)
    session = str(updated.get("session") or "").lower() if is_us_symbol else "regular"
    updated["session"] = session
    prev_close = _coerce_float(updated.get("prev_close"))
    if prev_close and prev_close > 0:
        updated["change_pct"] = (price / prev_close - 1.0) * 100.0
    if session in (*_EXTENDED_US_SESSIONS, "closed"):
        regular_price = _coerce_float(updated.get("regular_price"))
        updated["extended_price"] = price
        updated["extended_change_pct"] = _pct_from_base(price, regular_price)
    elif session == "regular":
        updated["regular_price"] = price
        updated["regular_change_pct"] = _pct_from_base(price, prev_close) or 0.0
        updated["extended_price"] = None
        updated["extended_change_pct"] = None
    updated["source"] = "Futu OpenD 逐笔订阅"
    return updated


def _update_futu_subscription_quotes(data: Any) -> None:
    code_to_sym = {code: sym for sym, code in app_config.FUTU_US.items()}
    now = time.time()
    next_quotes: dict[str, dict[str, Any]] = {}
    for i in range(len(data)):
        row = data.iloc[i] if hasattr(data, "iloc") else data[i]
        code = str(_row_get(row, "code", ""))
        sym = code_to_sym.get(code)
        if not sym:
            continue
        quote = _build_futu_quote(sym, row)
        if quote:
            next_quotes[sym] = quote
    if not next_quotes:
        return
    with _FUTU_SUB_LOCK:
        for sym, quote in next_quotes.items():
            _FUTU_SUB_QUOTES[sym] = _merge_futu_subscription_quote(_FUTU_SUB_QUOTES.get(sym), quote)
            _FUTU_SUB_UPDATED_AT[sym] = now
            _FUTU_SUB_QUOTE_REVISIONS[sym] = _FUTU_SUB_QUOTE_REVISIONS.get(sym, 0) + 1


def _update_futu_subscription_klines(data: Any) -> None:
    code_to_sym = {code: sym for sym, code in app_config.FUTU_US.items()}
    interval_by_type = {"K_15M": "15m", "K_5M": "5m"}
    with _FUTU_SUB_LOCK:
        for i in range(len(data)):
            row = data.iloc[i] if hasattr(data, "iloc") else data[i]
            sym = code_to_sym.get(str(_row_get(row, "code", "")))
            interval = interval_by_type.get(str(_row_get(row, "k_type", "")))
            if not sym or not interval:
                continue
            key = (sym, interval)
            _FUTU_SUB_KLINES[key] = {
                "time_key": str(_row_get(row, "time_key", "")),
                "open": _coerce_float(_row_get(row, "open")),
                "high": _coerce_float(_row_get(row, "high")),
                "low": _coerce_float(_row_get(row, "low")),
                "close": _coerce_float(_row_get(row, "close")),
                "volume": _coerce_float(_row_get(row, "volume")) or 0.0,
                "updated_at": time.time(),
            }
            _FUTU_SUB_KLINE_REVISIONS[key] = _FUTU_SUB_KLINE_REVISIONS.get(key, 0) + 1


def _update_futu_subscription_tickers(data: Any) -> None:
    code_to_sym = {code: sym for sym, code in app_config.FUTU_US.items()}
    now = time.time()
    with _FUTU_SUB_LOCK:
        for i in range(len(data)):
            row = data.iloc[i] if hasattr(data, "iloc") else data[i]
            sym = code_to_sym.get(str(_row_get(row, "code", "")))
            price = _coerce_float(_row_get(row, "price"))
            if not sym or not price or price <= 0:
                continue
            quote = _apply_futu_ticker_price(_FUTU_SUB_QUOTES.get(sym) or {}, sym, price)
            _FUTU_SUB_QUOTES[sym] = quote
            _FUTU_SUB_UPDATED_AT[sym] = now
            _FUTU_SUB_QUOTE_REVISIONS[sym] = _FUTU_SUB_QUOTE_REVISIONS.get(sym, 0) + 1


def start_futu_quote_subscription(force: bool = False) -> dict[str, Any]:
    global _FUTU_SUB_CTX, _FUTU_SUB_STARTED, _FUTU_SUB_LAST_ERROR, _FUTU_SUB_KLINE_ERROR, _FUTU_SUB_TICKER_ERROR, _QUOTES_CACHE, _QUOTES_CACHE_AT
    with _FUTU_SUB_LOCK:
        if _FUTU_SUB_STARTED and _FUTU_SUB_CTX is not None and not force:
            return futu_subscription_status()
        if _FUTU_SUB_CTX is not None:
            try:
                _FUTU_SUB_CTX.close()
            except Exception:
                pass
        _FUTU_SUB_CTX = None
        _FUTU_SUB_STARTED = False
        _FUTU_SUB_LAST_ERROR = ""
        _FUTU_SUB_KLINE_ERROR = ""
        _FUTU_SUB_TICKER_ERROR = ""
        _FUTU_SUB_QUOTES.clear()
        _FUTU_SUB_UPDATED_AT.clear()
        _FUTU_SUB_KLINES.clear()
        _QUOTES_CACHE = None
        _QUOTES_CACHE_AT = 0.0

    if not is_futu_opend_available():
        with _FUTU_SUB_LOCK:
            _FUTU_SUB_LAST_ERROR = "Futu OpenD unavailable"
        return futu_subscription_status()

    try:
        from futu import (
            AuType,
            CurKlineHandlerBase,
            KLType,
            OpenQuoteContext,
            RET_OK,
            StockQuoteHandlerBase,
            SubType,
            TickerHandlerBase,
        )
    except Exception as exc:
        with _FUTU_SUB_LOCK:
            _FUTU_SUB_LAST_ERROR = f"futu_import_failed: {exc}"
        return futu_subscription_status()

    class QuoteHandler(StockQuoteHandlerBase):
        def on_recv_rsp(self, rsp_pb: Any) -> tuple[int, Any]:
            ret_code, data = super().on_recv_rsp(rsp_pb)
            if ret_code == RET_OK:
                _update_futu_subscription_quotes(data)
            return ret_code, data

    class KlineHandler(CurKlineHandlerBase):
        def on_recv_rsp(self, rsp_pb: Any) -> tuple[int, Any]:
            ret_code, data = super().on_recv_rsp(rsp_pb)
            if ret_code == RET_OK:
                _update_futu_subscription_klines(data)
            return ret_code, data

    class TickerHandler(TickerHandlerBase):
        def on_recv_rsp(self, rsp_pb: Any) -> tuple[int, Any]:
            ret_code, data = super().on_recv_rsp(rsp_pb)
            if ret_code == RET_OK:
                _update_futu_subscription_tickers(data)
            return ret_code, data

    host, port = futu_opend_config()
    ctx = None
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        ctx.set_handler(QuoteHandler())
        ctx.set_handler(KlineHandler())
        ctx.set_handler(TickerHandler())
        subscribe_symbols = _futu_subscribe_symbols()
        ret, msg = ctx.subscribe(
            [app_config.FUTU_US[sym] for sym in subscribe_symbols],
            [SubType.QUOTE],
            is_first_push=True,
            subscribe_push=True,
        )
        if ret != RET_OK:
            try:
                ctx.close()
            except Exception:
                pass
            with _FUTU_SUB_LOCK:
                _FUTU_SUB_LAST_ERROR = str(msg)
            return futu_subscription_status()
        ticker_ret, ticker_msg = ctx.subscribe(
            [app_config.FUTU_US[sym] for sym in subscribe_symbols],
            [SubType.TICKER],
            is_first_push=True,
            subscribe_push=True,
            extended_time=True,
        )
        if ticker_ret != RET_OK:
            _FUTU_SUB_TICKER_ERROR = str(ticker_msg)
        kline_ret, kline_msg = ctx.subscribe(
            [app_config.FUTU_US[sym] for sym in subscribe_symbols],
            [SubType.K_15M, SubType.K_5M],
            is_first_push=True,
            subscribe_push=True,
            extended_time=True,
        )
        if kline_ret != RET_OK:
            _FUTU_SUB_KLINE_ERROR = str(kline_msg)
        else:
            for sym in subscribe_symbols:
                code = app_config.FUTU_US[sym]
                for interval, ktype in (("15m", KLType.K_15M), ("5m", KLType.K_5M)):
                    seed_ret, seed_data = ctx.get_cur_kline(code, 1, ktype=ktype, autype=AuType.QFQ)
                    if seed_ret != RET_OK or seed_data is None or len(seed_data) == 0:
                        continue
                    seed = seed_data.copy()
                    seed["k_type"] = {"15m": "K_15M", "5m": "K_5M"}[interval]
                    _update_futu_subscription_klines(seed)
        with _FUTU_SUB_LOCK:
            _FUTU_SUB_CTX = ctx
            _FUTU_SUB_STARTED = True
            _FUTU_SUB_LAST_ERROR = ""
    except Exception as exc:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
        with _FUTU_SUB_LOCK:
            _FUTU_SUB_LAST_ERROR = str(exc)
    return futu_subscription_status()


def stop_futu_quote_subscription() -> None:
    global _FUTU_SUB_CTX, _FUTU_SUB_STARTED
    with _FUTU_SUB_LOCK:
        ctx = _FUTU_SUB_CTX
        _FUTU_SUB_CTX = None
        _FUTU_SUB_STARTED = False
    try:
        if ctx is not None:
            ctx.close()
    except Exception:
        pass


def get_futu_subscription_quotes(max_age: float = _FUTU_SUB_TTL_SECONDS) -> dict[str, dict[str, Any]]:
    now = time.time()
    with _FUTU_SUB_LOCK:
        return {
            sym: dict(quote)
            for sym, quote in _FUTU_SUB_QUOTES.items()
            if now - _FUTU_SUB_UPDATED_AT.get(sym, 0.0) <= max_age
        }


def get_futu_subscription_kline(symbol: str, interval: str) -> dict[str, Any] | None:
    with _FUTU_SUB_LOCK:
        bar = _FUTU_SUB_KLINES.get((symbol, interval))
        return dict(bar) if bar else None


def get_futu_kline_revision(symbol: str, interval: str) -> int:
    with _FUTU_SUB_LOCK:
        return int(_FUTU_SUB_KLINE_REVISIONS.get((symbol, interval), 0))


def get_futu_quote_revision(symbol: str) -> int:
    with _FUTU_SUB_LOCK:
        return int(_FUTU_SUB_QUOTE_REVISIONS.get(symbol, 0))


def futu_subscription_status() -> dict[str, Any]:
    now = time.time()
    with _FUTU_SUB_LOCK:
        ages = {
            sym: round(now - updated_at, 2)
            for sym, updated_at in _FUTU_SUB_UPDATED_AT.items()
        }
        return {
            "started": _FUTU_SUB_STARTED,
            "symbols": sorted(_FUTU_SUB_QUOTES.keys()),
            "fresh_symbols": sorted(get_futu_subscription_quotes().keys()),
            "ages_seconds": ages,
            "last_error": _FUTU_SUB_LAST_ERROR,
            "kline_symbols": sorted({symbol for symbol, _ in _FUTU_SUB_KLINES}),
            "kline_error": _FUTU_SUB_KLINE_ERROR,
            "ticker_error": _FUTU_SUB_TICKER_ERROR,
        }


def _load_fund_quotes_cache() -> None:
    global _FUND_QUOTES_CACHE_LOADED
    if _FUND_QUOTES_CACHE_LOADED:
        return
    _FUND_QUOTES_CACHE_LOADED = True
    try:
        raw = json.loads(_FUND_QUOTES_CACHE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(raw, dict):
        return
    for code, item in raw.items():
        if not isinstance(item, dict):
            continue
        quote = item.get("quote")
        try:
            fetched_at = float(item.get("fetched_at", 0.0))
        except (TypeError, ValueError):
            continue
        if isinstance(quote, dict) and fetched_at > 0:
            _FUND_QUOTES_CACHE[str(code)] = (dict(quote), fetched_at)


def _save_fund_quotes_cache() -> None:
    payload = {
        code: {"quote": quote, "fetched_at": fetched_at}
        for code, (quote, fetched_at) in _FUND_QUOTES_CACHE.items()
    }
    try:
        _FUND_QUOTES_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass


def cache_fund_quote(code: str, quote: dict[str, Any]) -> dict[str, Any]:
    """Persist a fund quote without allowing an older provider row to replace it."""
    _load_fund_quotes_cache()
    now = time.time()
    candidate = dict(quote)
    cached = _FUND_QUOTES_CACHE.get(code)
    if cached:
        current = dict(cached[0])
        current_day = str(current.get("quote_date") or current.get("quote_time") or "")[:10]
        candidate_day = str(candidate.get("quote_date") or candidate.get("quote_time") or "")[:10]
        current_source = str(current.get("source") or "")
        candidate_source = str(candidate.get("source") or "")
        current_official = "\u51c0\u503c" in current_source and "\u4f30" not in current_source
        candidate_official = "\u51c0\u503c" in candidate_source and "\u4f30" not in candidate_source
        if candidate_day < current_day or (
            candidate_day == current_day and current_official and not candidate_official
        ):
            candidate = current
    _FUND_QUOTES_CACHE[code] = (dict(candidate), now)
    _save_fund_quotes_cache()
    return candidate


def _read_fx_cache() -> dict[str, Any] | None:
    if _FX_CACHE is not None:
        return dict(_FX_CACHE)
    try:
        raw = json.loads(_FX_CACHE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    rate = _coerce_float(raw.get("rate"))
    if rate and 5.0 < rate < 10.0:
        return {"rate": rate, "source": str(raw.get("source") or "缓存汇率")}
    return None


def _write_fx_cache(payload: dict[str, Any]) -> None:
    global _FX_CACHE, _FX_CACHE_AT
    _FX_CACHE = dict(payload)
    _FX_CACHE_AT = time.time()
    try:
        _FX_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass


def _parse_sina_fx(text: str) -> float | None:
    match = re.search(r'="([^"]*)"', text)
    if not match:
        return None
    parts = match.group(1).split(",")
    if len(parts) < 2:
        return None
    rate = _coerce_float(parts[1])
    return rate if rate and 5.0 < rate < 10.0 else None


def fetch_futu_us_quotes() -> dict[str, dict[str, Any]]:
    if not is_futu_opend_available():
        return {}
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception:
        return {}

    host, port = futu_opend_config()
    ctx = None
    out: dict[str, dict[str, Any]] = {}
    usd_symbols = _usd_symbols()
    futu_codes = [app_config.FUTU_US[sym] for sym in usd_symbols if sym in app_config.FUTU_US]
    code_to_sym = {code: sym for sym, code in app_config.FUTU_US.items()}
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        ret, snapshot = ctx.get_market_snapshot(futu_codes)
        if ret != RET_OK or snapshot is None or len(snapshot) == 0:
            return {}

        states: dict[str, str] = {}
        try:
            state_ret, state_data = ctx.get_market_state(futu_codes)
            if state_ret == RET_OK and state_data is not None:
                for i in range(len(state_data)):
                    row = state_data.iloc[i] if hasattr(state_data, "iloc") else state_data[i]
                    states[str(_row_get(row, "code", ""))] = str(_row_get(row, "market_state", "") or "")
        except Exception:
            pass

        for i in range(len(snapshot)):
            row = snapshot.iloc[i] if hasattr(snapshot, "iloc") else snapshot[i]
            code = str(_row_get(row, "code", ""))
            sym = code_to_sym.get(code)
            if not sym:
                continue
            last_price = _coerce_float(_row_get(row, "last_price") or _row_get(row, "cur_price") or _row_get(row, "price"))
            prev_close = _coerce_float(_row_get(row, "prev_close_price") or _row_get(row, "prev_close"))
            pre_price = _coerce_float(
                _row_get(row, "pre_market_price")
                or _row_get(row, "pre_price")
                or _row_get(row, "preMarketPrice")
            )
            pre_change_pct = _coerce_float(_row_get(row, "pre_change_rate"))
            after_price = _coerce_float(
                _row_get(row, "after_market_price")
                or _row_get(row, "after_hours_price")
                or _row_get(row, "after_price")
                or _row_get(row, "postMarketPrice")
            )
            after_change_pct = _coerce_float(_row_get(row, "after_change_rate"))
            overnight_price = _coerce_float(_row_get(row, "overnight_price"))
            overnight_change_pct = _coerce_float(_row_get(row, "overnight_change_rate"))
            state = states.get(code, "")
            session = _infer_us_session(state)
            extended_price: float | None = None
            extended_change_pct: float | None = None
            if session == "premarket":
                extended_price, extended_change_pct = pre_price, pre_change_pct
            elif session == "overnight":
                extended_price, extended_change_pct = overnight_price, overnight_change_pct
                if (
                    not _price_matches_change_pct(overnight_price, last_price, overnight_change_pct)
                    and _price_matches_change_pct(after_price, last_price, after_change_pct)
                ):
                    extended_price, extended_change_pct = after_price, after_change_pct
            elif session == "postmarket":
                extended_price, extended_change_pct = after_price, after_change_pct
            elif session == "closed":
                extended_price, extended_change_pct = after_price, after_change_pct
            if not last_price or last_price <= 0:
                continue
            base = prev_close if prev_close and prev_close > 0 else last_price
            price = extended_price if session != "regular" and extended_price and extended_price > 0 else last_price
            if session not in {"regular", "closed"} and extended_change_pct is None:
                extended_change_pct = _pct_from_base(extended_price, last_price)
            if not extended_price or extended_price <= 0 or abs(extended_price - last_price) <= 1e-9:
                extended_price = None
                extended_change_pct = None
            out[sym] = {
                "symbol": sym,
                "price": price,
                "regular_price": last_price,
                "change_pct": (price / base - 1.0) * 100.0 if base > 0 else 0.0,
                "regular_change_pct": (last_price / base - 1.0) * 100.0 if base > 0 else 0.0,
                "extended_price": extended_price,
                "extended_change_pct": extended_change_pct,
                "session": session,
                "source": "Futu OpenD 快照",
            }
    except Exception:
        return {}
    finally:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
    return out


def fetch_tencent_us_quotes() -> dict[str, dict[str, Any]]:
    qq_us = dict(app_config.QQ_US)
    url = "http://qt.gtimg.cn/q=" + ",".join(qq_us.values())
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT, headers=REQUEST_HEADERS)
        r.encoding = "gbk"
    except requests.RequestException:
        return {}
    raw: dict[str, dict[str, float]] = {}
    for line in r.text.replace("\n", "").split(";"):
        line = line.strip()
        match = re.match(r'v_(us[A-Za-z]+)="([^"]*)"', line)
        if not match:
            continue
        code, body = match.group(1), match.group(2)
        parts = body.split("~")
        if len(parts) <= 32:
            continue
        try:
            price = float(parts[3])
            change_pct = float(parts[32])
        except ValueError:
            continue
        if price > 0:
            raw[code] = {"price": price, "change_pct": change_pct}
    out = {}
    for sym, code in qq_us.items():
        item = raw.get(code)
        if item:
            out[sym] = {
                "symbol": sym,
                "price": item["price"],
                "regular_price": item["price"],
                "change_pct": item["change_pct"],
                "regular_change_pct": item["change_pct"],
                "extended_price": None,
                "extended_change_pct": None,
                "session": "regular",
                "source": "腾讯",
            }
    return out


def fetch_sina_us_quote(symbol: str) -> dict[str, Any] | None:
    code = app_config.SINA_GB.get(symbol)
    if not code:
        return None
    try:
        r = requests.get(
            "https://hq.sinajs.cn/list=" + code,
            timeout=HTTP_TIMEOUT,
            headers={**REQUEST_HEADERS, "Referer": "https://finance.sina.com.cn/"},
        )
        r.encoding = "gbk"
    except requests.RequestException:
        return None
    match = re.search(r'="([^"]*)"', r.text)
    if not match:
        return None
    parts = match.group(1).split(",")
    if len(parts) < 3:
        return None
    try:
        price = float(parts[1])
        delta = float(parts[2])
        prev = price - delta
        change_pct = delta / prev * 100.0 if prev > 0 else 0.0
    except ValueError:
        return None
    if price <= 0:
        return None
    return {
        "symbol": symbol,
        "price": price,
        "regular_price": price,
        "change_pct": change_pct,
        "regular_change_pct": change_pct,
        "extended_price": None,
        "extended_change_pct": None,
        "session": "regular",
        "source": "新浪",
    }


def fetch_fund_quote(code: str) -> dict[str, Any] | None:
    _load_fund_quotes_cache()
    now = time.time()
    cached = _FUND_QUOTES_CACHE.get(code)
    if cached and now - cached[1] < _FUND_QUOTES_TTL_SECONDS:
        return dict(cached[0])

    try:
        r = requests.get(
            f"https://fundgz.1234567.com.cn/js/{code}.js",
            timeout=HTTP_TIMEOUT,
            headers={**REQUEST_HEADERS, "Referer": "https://fund.eastmoney.com/"},
        )
    except requests.RequestException:
        return dict(cached[0]) if cached else None
    match = re.search(r"\((\{.*\})\)", r.text.strip())
    if not match:
        return dict(cached[0]) if cached else None
    try:
        obj = json.loads(match.group(1))
        price = float(obj.get("gsz") or obj.get("dwjz") or 0.0)
        change_pct = float(obj.get("gszzl") or 0.0)
    except (ValueError, json.JSONDecodeError, TypeError):
        return dict(cached[0]) if cached else None
    if price <= 0:
        return dict(cached[0]) if cached else None
    quote = {
        "symbol": code,
        "price": price,
        "regular_price": price,
        "change_pct": change_pct,
        "regular_change_pct": change_pct,
        # Keep the provider timestamp with the quote.  Around the A-share open
        # Eastmoney can still return the previous trading day's last estimate;
        # consumers need this field to avoid treating that stale move as today.
        "quote_date": str(obj.get("gztime") or obj.get("jzrq") or "")[:10],
        "quote_time": str(obj.get("gztime") or ""),
        "extended_price": None,
        "extended_change_pct": None,
        "session": "regular",
        "source": "东方财富基金估算",
    }
    return cache_fund_quote(code, quote)


def _parse_sina_fund_estimate(code: str, text: str) -> dict[str, Any] | None:
    match = re.search(rf'hq_str_fu_{re.escape(code)}="([^"]*)"', text or "")
    if not match:
        return None
    fields = match.group(1).split(",")
    if len(fields) < 8:
        return None
    try:
        price = float(fields[2])
        change_pct = float(fields[6])
    except (TypeError, ValueError):
        return None
    quote_date = str(fields[7]).strip()[:10]
    quote_clock = str(fields[1]).strip()
    if price <= 0 or not re.match(r"^\d{4}-\d{2}-\d{2}$", quote_date):
        return None
    return {
        "symbol": code,
        "price": price,
        "regular_price": price,
        "change_pct": change_pct,
        "regular_change_pct": change_pct,
        "quote_date": quote_date,
        "quote_time": f"{quote_date} {quote_clock}" if quote_clock else quote_date,
        "extended_price": None,
        "extended_change_pct": None,
        "session": "regular",
        "source": "新浪基金估值",
    }


def fetch_sina_fund_estimate(code: str) -> dict[str, Any] | None:
    try:
        response = requests.get(
            f"http://hq.sinajs.cn/list=fu_{code}",
            timeout=HTTP_TIMEOUT,
            headers={**REQUEST_HEADERS, "Referer": "https://finance.sina.com.cn/"},
        )
        response.encoding = "gbk"
    except requests.RequestException:
        return None
    return _parse_sina_fund_estimate(code, response.text)


def fetch_direct_fund_quote(code: str, today: str | None = None) -> dict[str, Any] | None:
    """Use Eastmoney first and Sina only when Eastmoney has no quote for today."""
    expected_day = today or datetime.now(TZ_SHANGHAI).date().isoformat()
    fund = fetch_fund_quote(code)
    fund_day = str((fund or {}).get("quote_date") or "")[:10]
    if fund_day == expected_day:
        return fund
    sina_estimate = fetch_sina_fund_estimate(code)
    if sina_estimate and str(sina_estimate.get("quote_date") or "")[:10] == expected_day:
        return cache_fund_quote(code, sina_estimate)
    return fund


def fetch_fx_usdcny() -> dict[str, Any]:
    global _FX_CACHE_AT
    now = time.time()
    if _FX_CACHE is not None and now - _FX_CACHE_AT < _FX_CACHE_TTL_SECONDS:
        return dict(_FX_CACHE)

    for code, label in (("USDCNY", "Sina USDCNY"), ("fx_susdcny", "Sina fx_susdcny"), ("fx_susdcnh", "Sina fx_susdcnh")):
        try:
            r = requests.get(
                f"https://hq.sinajs.cn/list={code}",
                timeout=HTTP_TIMEOUT,
                headers={**REQUEST_HEADERS, "Referer": "https://finance.sina.com.cn/"},
            )
            r.encoding = "gbk"
            rate = _parse_sina_fx(r.text)
            if rate:
                payload = {"rate": rate, "source": label}
                _write_fx_cache(payload)
                return payload
        except Exception:
            continue

    for secid, label in (("133.USDCNH", "Eastmoney USDCNH"), ("120.USDCNYC", "Eastmoney USDCNYC")):
        try:
            r = requests.get(
                f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43,f57,f58",
                timeout=HTTP_TIMEOUT,
                headers=REQUEST_HEADERS,
            )
            obj = r.json()
            raw = _coerce_float((obj.get("data") or {}).get("f43"))
            if raw:
                rate = raw / 10000.0 if raw > 1000 else raw
                if 5.0 < rate < 10.0:
                    payload = {"rate": rate, "source": label}
                    _write_fx_cache(payload)
                    return payload
        except Exception:
            continue
    cached = _read_fx_cache()
    if cached:
        return cached
    return {"rate": 7.2, "source": "Fallback"}


def _parse_sina_fx_daily_history(text: str) -> dict[str, float]:
    match = re.search(r'=\("(.*)"\);?\s*$', text or "", flags=re.S)
    if not match:
        return {}
    prices: dict[str, float] = {}
    for raw_row in match.group(1).split("|"):
        fields = raw_row.split(",")
        if len(fields) < 5:
            continue
        day = str(fields[0]).strip()[:10]
        try:
            close = float(fields[4])
        except (TypeError, ValueError):
            continue
        if re.match(r"^\d{4}-\d{2}-\d{2}$", day) and 5.0 < close < 10.0:
            prices[day] = close
    return prices


def fetch_fx_usdcny_history() -> dict[str, float]:
    """Return onshore USD/CNY daily closes keyed by trading date."""
    global _FX_HISTORY_CACHE, _FX_HISTORY_CACHE_AT
    now = time.time()
    if _FX_HISTORY_CACHE and now - _FX_HISTORY_CACHE_AT < _FX_HISTORY_CACHE_TTL_SECONDS:
        return dict(_FX_HISTORY_CACHE)
    try:
        response = requests.get(
            "https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/"
            "var%20_fx_susdcny=/NewForexService.getDayKLine?symbol=fx_susdcny",
            timeout=HTTP_TIMEOUT,
            headers={**REQUEST_HEADERS, "Referer": "https://finance.sina.com.cn/"},
        )
        response.encoding = "gbk"
        prices = _parse_sina_fx_daily_history(response.text)
    except requests.RequestException:
        prices = {}
    if prices:
        _FX_HISTORY_CACHE = dict(prices)
        _FX_HISTORY_CACHE_AT = now
    return dict(_FX_HISTORY_CACHE)


def _extract_peg(forward_pe: float | None, data: dict[str, Any]) -> float | None:
    if not forward_pe or forward_pe <= 0:
        return None
    growth = data.get("profit_growth_rate") or data.get("profitGrowthRate") or {}
    multiple = _coerce_float(growth.get("financial_ttm_multiple") or growth.get("financialTtmMultiple"))
    year_count = _coerce_float(growth.get("year_count") or growth.get("yearCount"))
    if (not multiple or not year_count) and isinstance(growth.get("profit_data"), list):
        profit_data = growth.get("profit_data") or []
        if len(profit_data) >= 2:
            first = _coerce_float(profit_data[0].get("finance_data_multiple"))
            last = _coerce_float(profit_data[-1].get("finance_data_multiple"))
            if first and last and first > 0 and last > 0:
                multiple = last / first
                year_count = max(1.0, len(profit_data) / 4.0)
    if not multiple or not year_count or multiple <= 0 or year_count <= 0:
        return None
    annual_growth_pct = ((multiple ** (1.0 / year_count)) - 1.0) * 100.0
    if annual_growth_pct <= 0:
        return None
    return forward_pe / annual_growth_pct


def fetch_valuation_metrics(symbols: tuple[str, ...] | None = None) -> dict[str, dict[str, float]]:
    global _VALUATION_METRICS_CACHE, _FORWARD_PE_CACHE_AT
    now = time.time()
    if _VALUATION_METRICS_CACHE is not None and now - _FORWARD_PE_CACHE_AT < 1800:
        return {sym: dict(metrics) for sym, metrics in _VALUATION_METRICS_CACHE.items()}
    if not is_futu_opend_available():
        return {}
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception:
        return {}
    host, port = futu_opend_config()
    ctx = None
    out: dict[str, dict[str, float]] = {}
    symbols = symbols if symbols is not None else _satellite_symbols()
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        for sym in symbols:
            code = app_config.FUTU_US.get(sym)
            if not code:
                continue
            try:
                ret, data = ctx.get_valuation_detail(code, valuation_type=1, interval_type=8)
            except Exception:
                continue
            if ret != RET_OK or not data:
                continue
            value = _coerce_float((data.get("trend") or {}).get("forward_value"))
            if value and value > 0:
                metrics = {"forward_pe": value}
                peg = _extract_peg(value, data)
                if peg is not None and peg > 0:
                    metrics["peg"] = peg
                out[sym] = metrics
            if sym in app_config.PS_BANDS:
                try:
                    ps_ret, ps_data = ctx.get_valuation_detail(code, valuation_type=3, interval_type=8)
                except Exception:
                    ps_ret, ps_data = None, None
                if ps_ret == RET_OK and ps_data:
                    trend = ps_data.get("trend") or {}
                    forward_ps = _coerce_float(trend.get("forward_value"))
                    current_ps = _coerce_float(trend.get("current_value"))
                    metrics = out.setdefault(sym, {})
                    if forward_ps is not None and forward_ps > 0:
                        metrics["forward_ps"] = forward_ps
                    if current_ps is not None and current_ps > 0:
                        metrics["ps"] = current_ps
    except Exception:
        return {}
    finally:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
    # Do not poison the long-lived valuation cache with a transient empty
    # response from OpenD.  A brief connection/query failure should be retried
    # on the next dashboard refresh instead of showing blank Forward PE for
    # the entire cache window.
    if out:
        _VALUATION_METRICS_CACHE = {sym: dict(metrics) for sym, metrics in out.items()}
        _FORWARD_PE_CACHE_AT = now
    else:
        _VALUATION_METRICS_CACHE = None
        _FORWARD_PE_CACHE_AT = 0.0
    return out


def fetch_forward_pe(symbols: tuple[str, ...] | None = None) -> dict[str, float]:
    return {
        sym: metrics["forward_pe"]
        for sym, metrics in fetch_valuation_metrics(symbols).items()
        if "forward_pe" in metrics
    }


def fetch_quotes() -> dict[str, Any]:
    global _QUOTES_CACHE, _QUOTES_CACHE_AT
    now = time.time()
    if _QUOTES_CACHE is not None and now - _QUOTES_CACHE_AT < _QUOTES_CACHE_TTL_SECONDS:
        return dict(_QUOTES_CACHE)
    futu_available = is_futu_opend_available()
    subscription_quotes = get_futu_subscription_quotes() if futu_available else {}
    if futu_available and not subscription_quotes and not futu_subscription_status().get("started"):
        start_futu_quote_subscription(force=True)
        subscription_quotes = get_futu_subscription_quotes()
    provider = "futu-subscribe" if subscription_quotes else ("futu" if futu_available else "tencent")
    quotes = dict(subscription_quotes)
    usd_symbols = _usd_symbols()
    # Subscription quotes also contain 510330.SS.  A count comparison can
    # therefore look complete while one USD holding (for example ISRG) is
    # actually missing. Check required symbols explicitly before falling back
    # to a Futu snapshot.
    if futu_available and any(sym not in quotes for sym in usd_symbols):
        snapshot_quotes = fetch_futu_us_quotes()
        for sym, quote in snapshot_quotes.items():
            quotes.setdefault(sym, quote)
        if quotes:
            provider = "futu-subscribe+snapshot" if subscription_quotes else "futu-snapshot"
    if not quotes:
        provider = "tencent"
        quotes = fetch_tencent_us_quotes()
    for sym in usd_symbols:
        if sym not in quotes:
            fallback = fetch_sina_us_quote(sym)
            if fallback:
                quotes[sym] = fallback
    for sym, code in app_config.FUND_CODES.items():
        fund = fetch_direct_fund_quote(code)
        if fund:
            fund["symbol"] = sym
            quotes[sym] = fund
    for sym in app_config.ALL_SYMBOLS:
        if sym not in quotes:
            price = app_config.FALLBACK_PRICES[sym]
            quotes[sym] = {
                "symbol": sym,
                "price": price,
                "regular_price": price,
                "change_pct": 0.0,
                "regular_change_pct": 0.0,
                "extended_price": None,
                "extended_change_pct": None,
                "session": "regular",
                "source": "Fallback",
            }
    valuation_metrics = fetch_valuation_metrics() if futu_available else {}
    valuation_metrics = {sym: dict(metrics) for sym, metrics in valuation_metrics.items()}
    forward_pe = {
        sym: metrics["forward_pe"]
        for sym, metrics in valuation_metrics.items()
        if "forward_pe" in metrics
    }
    payload = {
        "provider": provider,
        "futu_available": futu_available,
        "futu_subscription": futu_subscription_status(),
        "fetched_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
        "quotes": quotes,
        "fx": fetch_fx_usdcny(),
        "valuation_metrics": valuation_metrics,
        "forward_pe": forward_pe,
        "pe_bands": app_config.PE_BANDS,
    }
    _QUOTES_CACHE = payload
    _QUOTES_CACHE_AT = now
    return payload
