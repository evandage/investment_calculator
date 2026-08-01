from __future__ import annotations

import concurrent.futures
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from .config import FUTU_US
from .market_data import futu_opend_config, get_futu_subscription_kline, is_futu_opend_available

Interval = Literal["1d", "15m", "5m"]

_OHLCV_CACHE: dict[tuple[str, str, bool, str], tuple[dict[str, Any], float]] = {}
_OHLCV_TTL_SECONDS = {"1d": 300, "15m": 45, "5m": 30}
_TZ_NEW_YORK = ZoneInfo("America/New_York")
_TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
_FUTU_HISTORY_TIMEOUT_SECONDS = float(os.environ.get("FUTU_HISTORY_TIMEOUT_SECONDS", "8"))
_FUTU_HISTORY_MAX_WORKERS = max(1, int(os.environ.get("FUTU_HISTORY_MAX_WORKERS", "3")))
_FUTU_HISTORY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_FUTU_HISTORY_MAX_WORKERS,
    thread_name_prefix="futu-history",
)
_FUTU_HISTORY_LOCK = threading.Lock()
_FUTU_HISTORY_INFLIGHT: dict[
    tuple[str, Interval, str],
    concurrent.futures.Future[tuple[list[dict[str, Any]], str]],
] = {}
_FUTU_HISTORY_RESULTS: dict[
    tuple[str, Interval, str],
    tuple[tuple[list[dict[str, Any]], str], float],
] = {}


def _market_tz(symbol: str) -> ZoneInfo:
    code = FUTU_US.get(symbol, "")
    return _TZ_SHANGHAI if code.startswith(("SH.", "SZ.")) else _TZ_NEW_YORK


def _period_for_interval(interval: Interval) -> str:
    return "5y" if interval == "1d" else "2d"


def _futu_ktype(interval: Interval) -> str:
    try:
        from futu import KLType

        return {
            "1d": KLType.K_DAY,
            "15m": KLType.K_15M,
            "5m": KLType.K_5M,
        }[interval]
    except Exception:
        return {"1d": "K_DAY", "15m": "K_15M", "5m": "K_5M"}[interval]


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row.get(key, default)
    except AttributeError:
        try:
            return row[key]
        except Exception:
            return default


def _ts_to_lightweight(
    value: Any,
    interval: Interval,
    source_tz: ZoneInfo | None = None,
) -> int | str | None:
    try:
        ts = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return None
    if interval == "1d":
        return ts.date().isoformat()
    if ts.tzinfo is None and source_tz is not None:
        ts = ts.replace(tzinfo=source_tz)
    return int(ts.timestamp())


def _futu_ts_to_lightweight(
    value: Any,
    interval: Interval,
    symbol: str,
) -> int | str | None:
    """Convert Futu's end-labelled intraday K-line time to a bar start.

    Futu labels a 5-minute bar covering 09:30-09:35 as 09:35.  Lightweight
    Charts treats the timestamp as the candle position, so passing it through
    unchanged makes the active candle appear to sit in the future.  At the
    China-market open OpenD can also briefly expose an auction placeholder at
    exactly 09:30 (and similarly at the 13:00 reopen); it is superseded by the
    first real interval bar and must not become a second candle.
    """
    try:
        ts = datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return None
    if interval == "1d":
        return ts.date().isoformat()

    code = FUTU_US.get(symbol, "")
    if code.startswith(("SH.", "SZ.")) and (ts.hour, ts.minute) in {(9, 30), (13, 0)}:
        return None

    minutes = 15 if interval == "15m" else 5
    ts -= timedelta(minutes=minutes)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_market_tz(symbol))
    return int(ts.timestamp())


def _clean_bar(row: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return {
            "time": row["time"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume") or 0.0),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _merge_realtime_bar(
    bars: list[dict[str, Any]],
    symbol: str,
    interval: Interval,
) -> list[dict[str, Any]]:
    pushed = get_futu_subscription_kline(symbol, interval)
    if not pushed:
        return bars
    ts = _futu_ts_to_lightweight(pushed.get("time_key"), interval, symbol)
    if ts is None:
        return bars
    realtime = _clean_bar({**pushed, "time": ts})
    if realtime is None:
        return bars
    # A subscription update is only allowed to revise the active/latest bar.
    # Providers can briefly replay an older tick while reconnecting; letting
    # that tick overwrite a closed candle makes historical candles move.
    latest_ts = max((bar.get("time") for bar in bars), default=None)
    if latest_ts is not None and str(ts) < str(latest_ts):
        return bars
    merged = [bar for bar in bars if bar.get("time") != realtime["time"]]
    merged.append(realtime)
    return sorted(merged, key=lambda bar: str(bar.get("time")))


def _freeze_closed_intraday_bars(
    previous: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep previously seen candles immutable, except for the active bar.

    History providers may revise already-closed minute bars on every request.
    We accept new timestamps and refresh only the greatest timestamp (the
    still-forming candle), while retaining the prior value for older bars.
    """
    if not previous:
        return list(incoming)
    if not incoming:
        return list(previous)
    previous_by_time = {bar.get("time"): bar for bar in previous}
    incoming_by_time = {bar.get("time"): bar for bar in incoming}
    previous_latest = max(previous_by_time, default=None)
    incoming_latest = max(incoming_by_time, default=None)
    merged = dict(previous_by_time)
    for timestamp, bar in incoming_by_time.items():
        # Never move a closed candle. If the provider is stale, don't roll
        # the chart backwards either; only a new/latest timestamp can change.
        if timestamp not in merged or timestamp >= (previous_latest if previous_latest is not None else timestamp):
            merged[timestamp] = bar
    if incoming_latest is not None and previous_latest is not None and incoming_latest < previous_latest:
        return sorted(previous_by_time.values(), key=lambda bar: str(bar.get("time")))
    return sorted(merged.values(), key=lambda bar: str(bar.get("time")))


def _trading_day_bars(
    symbol: str,
    bars: list[dict[str, Any]],
    selected_date: str = "",
) -> list[dict[str, Any]]:
    if not bars:
        return bars
    tz = _market_tz(symbol)
    dated: list[tuple[dict[str, Any], object]] = []
    for bar in bars:
        try:
            trading_day = datetime.fromtimestamp(int(bar["time"]), tz).date()
        except (KeyError, TypeError, ValueError, OSError):
            continue
        dated.append((bar, trading_day))
    if not dated:
        return []
    available_days = {day for _, day in dated}
    target_day = None
    if selected_date:
        try:
            target_day = datetime.fromisoformat(selected_date[:10]).date()
        except ValueError:
            target_day = None
    if target_day is None:
        target_day = max(available_days)
    return [bar for bar, day in dated if day == target_day]


def _latest_trading_day_bars(symbol: str, bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _trading_day_bars(symbol, bars)


def _in_regular_session(symbol: str, local_time: datetime) -> bool:
    minutes = local_time.hour * 60 + local_time.minute
    if FUTU_US.get(symbol, "").startswith(("SH.", "SZ.")):
        return (9 * 60 + 30 <= minutes < 11 * 60 + 30) or (13 * 60 <= minutes < 15 * 60)
    return 9 * 60 + 30 <= minutes < 16 * 60


def _latest_regular_session_bars(
    symbol: str,
    bars: list[dict[str, Any]],
    min_current_bars: int = 6,
    include_previous_context: bool = False,
) -> list[dict[str, Any]]:
    tz = _market_tz(symbol)
    regular: list[tuple[dict[str, Any], object]] = []
    for bar in bars:
        try:
            local_time = datetime.fromtimestamp(int(bar["time"]), tz)
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if _in_regular_session(symbol, local_time):
            regular.append((bar, local_time.date()))
    if not regular:
        return []
    dates = sorted({day for _, day in regular})
    latest = dates[-1]
    selected = {latest}
    latest_count = sum(1 for _, day in regular if day == latest)
    if include_previous_context and latest_count < max(1, min_current_bars) and len(dates) > 1:
        selected.add(dates[-2])
    return [bar for bar, day in regular if day in selected]


def _fetch_futu_ohlcv_sync(
    symbol: str,
    interval: Interval,
    selected_date: str = "",
) -> tuple[list[dict[str, Any]], str]:
    if symbol not in FUTU_US or not is_futu_opend_available():
        return [], "futu_unavailable"
    try:
        from futu import AuType, KL_FIELD, OpenQuoteContext, RET_OK
    except Exception as exc:
        return [], f"futu_import_failed: {exc}"

    host, port = futu_opend_config()
    code = FUTU_US[symbol]
    end = datetime.now()
    start = end - (timedelta(days=365 * 3 + 30) if interval == "1d" else timedelta(days=7))
    if interval != "1d" and selected_date:
        try:
            selected = datetime.fromisoformat(selected_date[:10])
            start = selected
            end = selected
        except ValueError:
            pass
    ctx = None
    bars: list[dict[str, Any]] = []
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        page_req_key = None
        while True:
            ret, data, page_req_key = ctx.request_history_kline(
                code,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                ktype=_futu_ktype(interval),
                autype=AuType.QFQ,
                fields=[KL_FIELD.ALL],
                max_count=1000,
                page_req_key=page_req_key,
                extended_time=True,
            )
            if ret != RET_OK or data is None:
                return [], "futu_empty"
            for i in range(len(data)):
                item = data.iloc[i] if hasattr(data, "iloc") else data[i]
                ts = _futu_ts_to_lightweight(_row_value(item, "time_key"), interval, symbol)
                if ts is None:
                    continue
                bar = _clean_bar(
                    {
                        "time": ts,
                        "open": _row_value(item, "open"),
                        "high": _row_value(item, "high"),
                        "low": _row_value(item, "low"),
                        "close": _row_value(item, "close"),
                        "volume": _row_value(item, "volume"),
                    }
                )
                if bar:
                    bars.append(bar)
            if page_req_key is None:
                break
    except Exception as exc:
        return [], f"futu_failed: {exc}"
    finally:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
    return _merge_realtime_bar(bars, symbol, interval), "futu-history"


def _remember_futu_history_result(
    key: tuple[str, Interval, str],
    future: concurrent.futures.Future[tuple[list[dict[str, Any]], str]],
) -> None:
    try:
        result = future.result()
    except Exception as exc:
        result = ([], f"futu_failed: {exc}")
    with _FUTU_HISTORY_LOCK:
        if _FUTU_HISTORY_INFLIGHT.get(key) is future:
            _FUTU_HISTORY_INFLIGHT.pop(key, None)
        _FUTU_HISTORY_RESULTS[key] = (result, time.time())


def _fetch_futu_ohlcv(
    symbol: str,
    interval: Interval,
    selected_date: str = "",
) -> tuple[list[dict[str, Any]], str]:
    selected_day = selected_date[:10] if interval != "1d" else ""
    key = (symbol, interval, selected_day)
    now = time.time()
    created = False

    def _with_realtime(result: tuple[list[dict[str, Any]], str]) -> tuple[list[dict[str, Any]], str]:
        bars, source = result
        copied = [dict(bar) for bar in bars]
        return (_merge_realtime_bar(copied, symbol, interval) if not selected_day else copied), source

    with _FUTU_HISTORY_LOCK:
        cached = _FUTU_HISTORY_RESULTS.get(key)
        if cached and now - cached[1] < _OHLCV_TTL_SECONDS[interval]:
            return _with_realtime(cached[0])
        future = _FUTU_HISTORY_INFLIGHT.get(key)
        if future is None:
            future = _FUTU_HISTORY_EXECUTOR.submit(_fetch_futu_ohlcv_sync, symbol, interval, selected_day)
            _FUTU_HISTORY_INFLIGHT[key] = future
            created = True
    if created:
        future.add_done_callback(lambda done, request_key=key: _remember_futu_history_result(request_key, done))
    try:
        result = future.result(timeout=_FUTU_HISTORY_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        # The SDK call cannot be force-cancelled safely. Keep the shared future
        # alive so refreshes reuse it instead of creating more OpenD sockets.
        return [], "futu_timeout"
    except Exception as exc:
        return [], f"futu_failed: {exc}"
    _remember_futu_history_result(key, future)
    return _with_realtime(result)


def _fetch_tencent_ohlcv(symbol: str, interval: Interval) -> tuple[list[dict[str, Any]], str]:
    try:
        import chart_boards
    except Exception as exc:
        return [], f"tencent_import_failed: {exc}"
    try:
        chart_boards.configure_market_provider("tencent")
        df = chart_boards.fetch_ohlcv(symbol, interval, _period_for_interval(interval), cache_only=False)
    except Exception as exc:
        return [], f"tencent_failed: {exc}"
    if df is None or df.empty:
        return [], "tencent_empty"
    bars: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        ts = _ts_to_lightweight(idx, interval)
        if ts is None:
            continue
        bar = _clean_bar(
            {
                "time": ts,
                "open": row.get("Open"),
                "high": row.get("High"),
                "low": row.get("Low"),
                "close": row.get("Close"),
                "volume": row.get("Volume"),
            }
        )
        if bar:
            bars.append(bar)
    return bars, "tencent"


def fetch_ohlcv(
    symbol: str,
    interval: str,
    show_extended: bool = True,
    selected_date: str = "",
) -> dict[str, Any]:
    sym = str(symbol or "VOO").upper()
    if sym not in FUTU_US:
        sym = "VOO"
    iv = interval if interval in {"1d", "15m", "5m"} else "1d"
    selected_day = selected_date[:10] if iv != "1d" else ""
    key = (sym, iv, show_extended, selected_day)
    now = time.time()
    cached = _OHLCV_CACHE.get(key)
    ttl = _OHLCV_TTL_SECONDS[iv]  # type: ignore[index]
    if cached and now - cached[1] < ttl:
        payload = dict(cached[0])
        merged = list(payload.get("bars") or [])
        if not selected_day:
            merged = _merge_realtime_bar(merged, sym, iv)  # type: ignore[arg-type]
        payload["bars"] = (
            merged
            if iv == "1d"
            else (
                _trading_day_bars(sym, merged, selected_day)
                if show_extended
                else [
                    bar
                    for bar in _trading_day_bars(sym, merged, selected_day)
                    if _in_regular_session(sym, datetime.fromtimestamp(int(bar["time"]), _market_tz(sym)))
                ]
            )
        )
        payload["source"] = "futu-subscribe"
        return payload

    bars, source = _fetch_futu_ohlcv(sym, iv, selected_day)  # type: ignore[arg-type]
    fallback_source = ""
    if not bars:
        fallback_source = source
        bars, source = _fetch_tencent_ohlcv(sym, iv)  # type: ignore[arg-type]

    # Keep closed intraday candles stable across history refreshes. The live
    # subscription/quote path still updates the final forming candle below.
    if iv != "1d":
        previous_payload = _OHLCV_CACHE.get(key)
        previous_bars = list((previous_payload[0].get("bars") or []) if previous_payload else [])
        bars = _freeze_closed_intraday_bars(previous_bars, bars)

    payload = {
        "symbol": sym,
        "interval": iv,
        "source": source,
        "fallback_reason": fallback_source,
        "bars": (
            bars[-1200:]
            if iv == "1d"
            else (
                _trading_day_bars(sym, bars, selected_day)
                if show_extended
                else [
                    bar
                    for bar in _trading_day_bars(sym, bars, selected_day)
                    if _in_regular_session(sym, datetime.fromtimestamp(int(bar["time"]), _market_tz(sym)))
                ]
            )
        ),
        "selected_date": selected_day,
    }
    if bars:
        _OHLCV_CACHE[key] = (dict(payload), now)
    return payload
