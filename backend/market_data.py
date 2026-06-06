from __future__ import annotations

import json
import os
import re
import socket
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .config import (
    ALL_SYMBOLS,
    FALLBACK_PRICES,
    FUND_CODES,
    FUTU_US,
    HTTP_TIMEOUT,
    PE_BANDS,
    QQ_US,
    REQUEST_HEADERS,
    ROOT_DIR,
    SATELLITE_SYMBOLS,
    SINA_GB,
    TZ_SHANGHAI,
    USD_SYMBOLS,
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
_FORWARD_PE_CACHE: dict[str, float] | None = None
_FORWARD_PE_CACHE_AT = 0.0
NY_TZ = ZoneInfo("America/New_York")


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
    minutes = now.hour * 60 + now.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "premarket"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "regular"
    if 16 * 60 <= minutes < 20 * 60:
        return "postmarket"
    return "overnight"


def _price_matches_change_pct(price: float | None, base: float | None, pct: float | None) -> bool:
    if not price or not base or base <= 0 or pct is None:
        return False
    implied = (price / base - 1.0) * 100.0
    return abs(implied - pct) <= max(0.15, abs(pct) * 0.2)


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
    futu_codes = [FUTU_US[sym] for sym in USD_SYMBOLS]
    code_to_sym = {code: sym for sym, code in FUTU_US.items()}
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
            if not last_price or last_price <= 0:
                continue
            base = prev_close if prev_close and prev_close > 0 else last_price
            price = extended_price if session != "regular" and extended_price and extended_price > 0 else last_price
            if (
                session != "regular"
                and extended_change_pct is None
                and extended_price
                and extended_price > 0
                and last_price > 0
            ):
                extended_change_pct = (extended_price / last_price - 1.0) * 100.0
            if not extended_price or extended_price <= 0 or abs(extended_price - last_price) <= 1e-9:
                extended_price = None
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
    url = "http://qt.gtimg.cn/q=" + ",".join(QQ_US.values())
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
    for sym, code in QQ_US.items():
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
    code = SINA_GB.get(symbol)
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
        "extended_price": None,
        "extended_change_pct": None,
        "session": "regular",
        "source": "东方财富基金估算",
    }
    _FUND_QUOTES_CACHE[code] = (dict(quote), now)
    _save_fund_quotes_cache()
    return quote


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


def fetch_forward_pe(symbols: tuple[str, ...] = SATELLITE_SYMBOLS) -> dict[str, float]:
    global _FORWARD_PE_CACHE, _FORWARD_PE_CACHE_AT
    now = time.time()
    if _FORWARD_PE_CACHE is not None and now - _FORWARD_PE_CACHE_AT < 1800:
        return dict(_FORWARD_PE_CACHE)
    if not is_futu_opend_available():
        return {}
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception:
        return {}
    host, port = futu_opend_config()
    ctx = None
    out: dict[str, float] = {}
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        for sym in symbols:
            code = FUTU_US.get(sym)
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
                out[sym] = value
    except Exception:
        return {}
    finally:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass
    _FORWARD_PE_CACHE = dict(out)
    _FORWARD_PE_CACHE_AT = now
    return out


def fetch_quotes() -> dict[str, Any]:
    global _QUOTES_CACHE, _QUOTES_CACHE_AT
    now = time.time()
    if _QUOTES_CACHE is not None and now - _QUOTES_CACHE_AT < _QUOTES_CACHE_TTL_SECONDS:
        return dict(_QUOTES_CACHE)
    provider = "futu" if is_futu_opend_available() else "tencent"
    quotes = fetch_futu_us_quotes() if provider == "futu" else {}
    if not quotes:
        provider = "tencent"
        quotes = fetch_tencent_us_quotes()
    for sym in USD_SYMBOLS:
        if sym not in quotes:
            fallback = fetch_sina_us_quote(sym)
            if fallback:
                quotes[sym] = fallback
    for sym, code in FUND_CODES.items():
        fund = fetch_fund_quote(code)
        if fund:
            fund["symbol"] = sym
            quotes[sym] = fund
    for sym in ALL_SYMBOLS:
        if sym not in quotes:
            price = FALLBACK_PRICES[sym]
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
    payload = {
        "provider": provider,
        "futu_available": is_futu_opend_available(),
        "fetched_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
        "quotes": quotes,
        "fx": fetch_fx_usdcny(),
        "forward_pe": fetch_forward_pe() if provider == "futu" else {},
        "pe_bands": PE_BANDS,
    }
    _QUOTES_CACHE = payload
    _QUOTES_CACHE_AT = now
    return payload
