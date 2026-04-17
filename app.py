import re
import json
import base64
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    page_title="Investment Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

from chart_boards import (
    CHART_THEME_OPTIONS,
    configure_market_storage,
    fig_15m_vwap_rsi,
    fig_5m_vwap_rsi7,
    fig_daily,
    probe_market_inventory,
    probe_symbol_interval_raw_rows,
    probe_market_cache_status,
    probe_recent_market_rows,
)

# 拉取失败时的回退价（与常见区间一致）
_FALLBACK = {
    "VOO": 400.0,
    "QQQ": 500.0,
    "TLT": 90.0,
    "IEI": 115.0,
    "001015": 1.0,
    "007994": 1.0,
}

_TICKERS = {
    "voo": "VOO",
    "qqq": "QQQ",
    "tlt": "TLT",
    "iei": "IEI",
    "hs300": "001015",  # 华夏沪深300指数增强A
    "zz500": "007994",  # 华夏中证500指数增强
}

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# (connect, read) 秒；避免长时间挂死
_HTTP_TIMEOUT = (5, 15)

# 美股：腾讯财经批量接口；失败则用新浪全球行情
_QQ_US = {"VOO": "usVOO", "QQQ": "usQQQ", "TLT": "usTLT", "IEI": "usIEI"}
_SINA_GB = {"VOO": "gb_voo", "QQQ": "gb_qqq", "TLT": "gb_tlt", "IEI": "gb_iei"}
_FUND_CODES = {"001015": "001015", "007994": "007994"}

_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "QQQ": {"label": "QQQ", "currency": "USD"},
    "TLT": {"label": "债券(TLT)", "currency": "USD"},
    "IEI": {"label": "债券(IEI)", "currency": "USD"},
    "001015": {"label": "华夏沪深300指数增强A(001015)", "currency": "CNY"},
    "007994": {"label": "华夏中证500指数增强(007994)", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    # 目标比例：
    # 美元资产: VOO 25%, QQQ 15%, 债券 20%（TLT/IEI各10%）
    # 人民币资产: 沪深300(001015) 20%, 中证500(007994) 20%
    "VOO": 0.25,
    "QQQ": 0.15,
    "TLT": 0.10,
    "IEI": 0.10,
    "001015": 0.20,
    "007994": 0.20,
}

# 美元资产 PE 参考区间（经验口径，仅作辅助，不构成投资建议）
_USD_ASSET_PE_BANDS: dict[str, tuple[float, float]] = {
    "VOO": (18.0, 24.0),
    "QQQ": (22.0, 32.0),
    "TLT": (14.0, 24.0),
    "IEI": (12.0, 22.0),
}


_TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
_UI_THEMES = {
    "主题1：绿跌红涨": {
        "delta_color": "inverse",
        "profit_color": "#dc2626",
        "loss_color": "#16a34a",
        "accent": "#2563eb",
        "card_bg": "rgba(248, 250, 252, 0.85)",
    },
    "主题2：绿涨红跌": {
        "delta_color": "normal",
        "profit_color": "#16a34a",
        "loss_color": "#dc2626",
        "accent": "#7c3aed",
        "card_bg": "rgba(248, 250, 252, 0.85)",
    },
}


def _apply_theme_css(theme: dict[str, str]) -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
        
        .stApp {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: {theme.get("page_bg", "transparent")};
        }}
        
        /* Modern metric cards */
        [data-testid="stMetric"] {{
            background: {theme["card_bg"]};
            border: 1px solid rgba(148, 163, 184, 0.15);
            border-radius: 16px;
            padding: 16px 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        [data-testid="stMetric"]:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.08), 0 4px 6px -2px rgba(0, 0, 0, 0.04);
        }}
        [data-testid="stMetricValue"] {{
            font-size: 1.8rem;
            font-weight: 800;
            color: var(--text-color);
        }}
        [data-testid="stMetricLabel"] {{
            font-weight: 600;
            color: var(--text-color);
            opacity: 0.75;
            font-size: 0.95rem;
        }}
        
        /* Button styling */
        .stButton > button, .stDownloadButton > button {{
            border-radius: 12px;
            border: none;
            background: linear-gradient(135deg, {theme["accent"]} 0%, #3b82f6 100%);
            color: white !important;
            font-weight: 600;
            padding: 0.5rem 1.5rem;
            box-shadow: 0 4px 14px rgba(37, 99, 235, 0.25);
            transition: all 0.2s ease;
        }}
        .stButton > button:hover {{
            transform: scale(1.02);
            box-shadow: 0 6px 20px rgba(37, 99, 235, 0.4);
        }}
        
        /* Expander / container styling */
        [data-testid="stExpander"] {{
            background: {theme["card_bg"]};
            border-radius: 16px;
            border: 1px solid rgba(148, 163, 184, 0.2);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
            overflow: hidden;
            margin-bottom: 1rem;
        }}
        [data-testid="stExpander"] summary {{
            font-weight: 700;
            font-size: 1.1rem;
            color: var(--text-color);
        }}
        
        /* Dataframes */
        [data-testid="stDataFrame"] {{
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(148, 163, 184, 0.2);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.02);
        }}
        
        /* Headers */
        h1, h2, h3, h4 {{
            font-weight: 800 !important;
            letter-spacing: -0.025em !important;
        }}
        
        div[data-testid="stCaptionContainer"] p {{
            color: #64748b;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _fetch_fx_from_erapi() -> float | None:
    url = "https://open.er-api.com/v6/latest/USD"
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    j = r.json()
    rates = j.get("rates", {})
    cny = rates.get("CNY")
    if cny is None:
        return None
    fx = float(cny)
    return fx if fx > 0 else None


def _fetch_fx_from_qq() -> float | None:
    r = requests.get(
        "http://qt.gtimg.cn/q=USDCNY",
        timeout=_HTTP_TIMEOUT,
        headers=_REQUEST_HEADERS,
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m:
        return None
    parts = m.group(1).split("~")
    if len(parts) < 4:
        return None
    fx = float(parts[3])
    return fx if fx > 0 else None


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_usdcny_rate_meta() -> dict[str, str | float]:
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    for source, fn in (
        ("ER-API", _fetch_fx_from_erapi),
        ("腾讯 USDCNY", _fetch_fx_from_qq),
    ):
        try:
            fx = fn()
            if fx is not None:
                return {"fx": fx, "source": source, "fetched_at": fetched_at}
        except Exception:
            pass
    return {"fx": 6.9, "source": "Fallback(6.9)", "fetched_at": fetched_at}


def _fetch_usdcny_rate() -> float:
    return float(_fetch_usdcny_rate_meta()["fx"])


def _parse_qq_us_response(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.replace("\n", "").split(";"):
        line = line.strip()
        if not line.startswith("v_us"):
            continue
        m = re.match(r'v_(us[A-Za-z]+)="([^"]*)"', line)
        if not m:
            continue
        code, body = m.group(1), m.group(2)
        parts = body.split("~")
        if len(parts) > 3:
            try:
                p = float(parts[3])
                if p > 0:
                    out[code] = p
            except ValueError:
                continue
    return out


def _parse_qq_us_response_price_change(text: str) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for line in text.replace("\n", "").split(";"):
        line = line.strip()
        if not line.startswith("v_us"):
            continue
        m = re.match(r'v_(us[A-Za-z]+)="([^"]*)"', line)
        if not m:
            continue
        code, body = m.group(1), m.group(2)
        parts = body.split("~")
        # 腾讯美股字段中：3=最新价，32=涨跌幅(百分比)
        if len(parts) > 32:
            try:
                price = float(parts[3])
                change_pct = float(parts[32])
                if price > 0:
                    out[code] = {"price": price, "change_pct": change_pct}
            except ValueError:
                continue
    return out


def _fetch_qq_us() -> dict[str, float]:
    url = "http://qt.gtimg.cn/q=" + ",".join(_QQ_US.values())
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    r.encoding = "gbk"
    raw = _parse_qq_us_response(r.text)
    return {sym: raw[qc] for sym, qc in _QQ_US.items() if qc in raw}


def _fetch_qq_us_price_change() -> dict[str, dict[str, float]]:
    url = "http://qt.gtimg.cn/q=" + ",".join(_QQ_US.values())
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    r.encoding = "gbk"
    raw = _parse_qq_us_response_price_change(r.text)
    out: dict[str, dict[str, float]] = {}
    for sym, qc in _QQ_US.items():
        if qc not in raw:
            continue
        out[sym] = raw[qc]
    return out


def _fetch_sina_gb(list_code: str) -> float | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 2:
        return None
    try:
        p = float(parts[1])
        return p if p > 0 else None
    except ValueError:
        return None


def _fetch_sina_gb_price_change(list_code: str) -> tuple[float, float] | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 3:
        return None
    try:
        price = float(parts[1])
        chg_points = float(parts[2])
        # sina gb_*: parts[2] 为涨跌额(点)，用它反推昨收来得到涨跌幅%
        prev_close = price - chg_points
        if price <= 0 or prev_close <= 0:
            return None
        change_pct = chg_points / prev_close * 100.0
        return (price, change_pct)
    except ValueError:
        return None


def _fetch_sina_cn(list_code: str) -> float | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 4:
        return None
    try:
        # 你的口径：parts[3]=当前价
        p = float(parts[3])
        return p if p > 0 else None
    except ValueError:
        return None


def _fetch_sina_cn_price_change(list_code: str) -> tuple[float, float] | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 4:
        return None
    try:
        # 你的口径（已验证）：
        # parts[2]=昨收价，parts[3]=当前价
        price = float(parts[3])
        prev_close = float(parts[2])
        if price <= 0 or prev_close <= 0:
            return None
        change_pct = (price - prev_close) / prev_close * 100.0
        return (price, change_pct)
    except ValueError:
        return None


def _fetch_fund_price_change(code: str) -> tuple[float, float] | None:
    """东方财富基金估值：返回(最新估值, 估算涨跌幅%)。"""
    url = f"https://fundgz.1234567.com.cn/js/{code}.js"
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    text = r.text.strip()
    m = re.search(r"\((\{.*\})\)", text)
    if not m:
        return None
    try:
        obj = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
    try:
        price = float(obj.get("gsz") or obj.get("dwjz") or 0.0)
        change_pct = float(obj.get("gszzl") or 0.0)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price, change_pct


@st.cache_data(ttl=60, show_spinner=False)
def _fetch_spot_prices_meta() -> dict[str, object]:
    out: dict[str, float] = {}
    daily_change_pct_by_symbol: dict[str, float] = {}
    source_by_symbol: dict[str, str] = {}
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")

    try:
        qq_raw = _fetch_qq_us_price_change()
        for sym, item in qq_raw.items():
            out[sym] = float(item["price"])
            daily_change_pct_by_symbol[sym] = float(item["change_pct"])
            source_by_symbol[sym] = "腾讯美股"
    except Exception:
        pass

    for sym in ("VOO", "QQQ", "TLT", "IEI"):
        if sym not in out:
            try:
                res = _fetch_sina_gb_price_change(_SINA_GB[sym])
                if res is not None:
                    p, change_pct = res
                    out[sym] = p
                    daily_change_pct_by_symbol[sym] = change_pct
                    source_by_symbol[sym] = "新浪全球"
            except Exception:
                pass

    # 基金：用东方财富基金估值接口
    symbols = list(_TICKERS.values())
    for sym in ("001015", "007994"):
        try:
            res = _fetch_fund_price_change(_FUND_CODES[sym])
            if res is not None:
                p, change_pct = res
                out[sym] = p
                daily_change_pct_by_symbol[sym] = change_pct
                source_by_symbol[sym] = "东财基金估值"
        except Exception:
            pass

    prices = {sym: out.get(sym, _FALLBACK[sym]) for sym in symbols}
    for sym in symbols:
        source_by_symbol.setdefault(sym, "Fallback")
        daily_change_pct_by_symbol.setdefault(sym, 0.0)
    return {
        "prices": prices,
        "daily_change_pct_by_symbol": daily_change_pct_by_symbol,
        "source_by_symbol": source_by_symbol,
        "fetched_at": fetched_at,
    }


def _fetch_spot_prices() -> dict[str, float]:
    return dict(_fetch_spot_prices_meta()["prices"])


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_vix_meta() -> dict[str, float | str]:
    """获取美股 CBOE VIX 当前值与当日涨跌幅。"""
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    try:
        import yfinance as yf

        t = yf.Ticker("^VIX")
        h = t.history(period="5d", interval="1d", auto_adjust=False)
        if h is not None and not h.empty and "Close" in h.columns:
            c = pd.to_numeric(h["Close"], errors="coerce").dropna()
            if len(c) >= 2:
                cur = float(c.iloc[-1])
                prev = float(c.iloc[-2])
                pct = ((cur / prev - 1.0) * 100.0) if prev > 0 else 0.0
                return {"vix": cur, "change_pct": pct, "source": "yfinance CBOE ^VIX", "fetched_at": fetched_at}
            if len(c) == 1:
                return {"vix": float(c.iloc[-1]), "change_pct": 0.0, "source": "yfinance CBOE ^VIX", "fetched_at": fetched_at}
    except Exception:
        pass
    return {"vix": 20.0, "change_pct": 0.0, "source": "Fallback(20.0)", "fetched_at": fetched_at}


def _vix_regime(vix: float) -> tuple[str, str]:
    if vix < 15:
        return ("低波动", "市场情绪偏乐观，风险偏好较高；注意防止过度乐观。")
    if vix < 20:
        return ("中性偏稳", "常态区间，市场波动温和，可按计划分批配置。")
    if vix < 30:
        return ("偏高波动", "不确定性上升，建议控制单次仓位、分批进场。")
    return ("高波动/恐慌", "风险事件阶段，优先仓位管理与现金流，避免一次性重仓。")


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_us_etf_pe_drawdown(symbol: str) -> dict[str, float | None]:
    """返回美股ETF估值与回撤指标：pe(若可得)、回撤%(近60日高点回撤，负值为回撤)。"""
    try:
        import yfinance as yf
    except Exception:
        return {"pe": None, "drawdown_pct": None}

    pe_val: float | None = None
    dd_val: float | None = None
    try:
        t = yf.Ticker(symbol)
        info = {}
        try:
            info = t.get_info() or {}
        except Exception:
            info = {}
        for k in ("trailingPE", "forwardPE"):
            v = info.get(k)
            try:
                fv = float(v)
                if fv > 0:
                    pe_val = fv
                    break
            except (TypeError, ValueError):
                continue

        hist = t.history(period="1y", interval="1d", auto_adjust=True)
        if hist is not None and not hist.empty and "Close" in hist.columns:
            c = pd.to_numeric(hist["Close"], errors="coerce").dropna()
            if len(c) > 20:
                win = c.tail(60) if len(c) >= 60 else c
                peak = float(win.max())
                last = float(win.iloc[-1])
                if peak > 0:
                    dd_val = (last / peak - 1.0) * 100.0
    except Exception:
        return {"pe": pe_val, "drawdown_pct": dd_val}

    return {"pe": pe_val, "drawdown_pct": dd_val}


def _default_holdings() -> dict[str, dict[str, float]]:
    return {
        sym: {"shares": 0.0, "avg_cost": float(_FALLBACK[sym])}
        for sym in _ASSET_META.keys()
    }


def _load_holdings() -> dict[str, dict[str, float]]:
    if not _HOLDINGS_FILE.exists():
        return _default_holdings()
    try:
        data = json.loads(_HOLDINGS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_holdings()

    holdings = _default_holdings()
    for sym in holdings:
        item = data.get(sym, {})
        shares = item.get("shares", 0.0)
        avg_cost = item.get("avg_cost", holdings[sym]["avg_cost"])
        try:
            holdings[sym]["shares"] = max(0.0, float(shares))
            holdings[sym]["avg_cost"] = max(0.0, float(avg_cost))
        except (TypeError, ValueError):
            continue
    return holdings


def _save_holdings(holdings: dict[str, dict[str, float]]) -> None:
    payload = {
        sym: {
            "shares": float(max(0.0, item["shares"])),
            "avg_cost": float(max(0.0, item["avg_cost"])),
        }
        for sym, item in holdings.items()
    }
    _HOLDINGS_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _merge_buy(
    holding: dict[str, float], add_shares: float, add_price: float
) -> dict[str, float]:
    old_shares = float(holding.get("shares", 0.0))
    old_cost = float(holding.get("avg_cost", 0.0))
    add_shares = max(0.0, float(add_shares))
    add_price = max(0.0, float(add_price))
    if add_shares <= 0:
        return {"shares": old_shares, "avg_cost": old_cost}
    new_shares = old_shares + add_shares
    if new_shares <= 0:
        return {"shares": 0.0, "avg_cost": add_price}
    new_avg = (old_shares * old_cost + add_shares * add_price) / new_shares
    return {"shares": new_shares, "avg_cost": new_avg}


def _default_balances() -> dict[str, float]:
    return {"cash_usd": 0.0, "cash_cny": 0.0}


def _load_balances() -> dict[str, float]:
    if not _BALANCE_FILE.exists():
        return _default_balances()
    try:
        data = json.loads(_BALANCE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_balances()
    out = _default_balances()
    try:
        out["cash_usd"] = max(0.0, float(data.get("cash_usd", 0.0)))
    except (TypeError, ValueError):
        out["cash_usd"] = 0.0
    try:
        out["cash_cny"] = max(0.0, float(data.get("cash_cny", 0.0)))
    except (TypeError, ValueError):
        out["cash_cny"] = 0.0
    # 兼容旧版按标的保存的“结转余额”
    try:
        legacy_cny = max(0.0, float(data.get("001015", 0.0))) + max(0.0, float(data.get("007994", 0.0)))
    except (TypeError, ValueError):
        legacy_cny = 0.0
    if out["cash_cny"] <= 0 and legacy_cny > 0:
        out["cash_cny"] = legacy_cny
    return out


def _save_balances(balances: dict[str, float]) -> None:
    payload = {sym: float(max(0.0, v)) for sym, v in _default_balances().items()}
    for sym in payload:
        payload[sym] = float(max(0.0, balances.get(sym, 0.0)))
    _BALANCE_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _db_conf() -> dict[str, str] | None:
    try:
        url = str(st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
        key = str(st.secrets.get("SUPABASE_KEY", "")).strip()
    except Exception:
        return None
    if not url or not key:
        return None
    return {"url": url, "key": key}


def _supabase_key_kind(key: str) -> str:
    k = str(key or "").strip()
    if not k:
        return "none"
    if k.startswith("sb_publishable_"):
        return "publishable"
    parts = k.split(".")
    if len(parts) == 3:
        try:
            payload = parts[1]
            payload += "=" * ((4 - len(payload) % 4) % 4)
            raw = base64.urlsafe_b64decode(payload.encode("ascii"))
            obj = json.loads(raw.decode("utf-8"))
            role = str(obj.get("role", "")).strip()
            if role:
                return f"jwt:{role}"
            return "jwt"
        except Exception:
            return "jwt?"
    return "unknown"


def _normalize_holdings(raw: Any) -> dict[str, dict[str, float]]:
    holdings = _default_holdings()
    if not isinstance(raw, dict):
        return holdings
    for sym in holdings:
        item = raw.get(sym, {})
        if not isinstance(item, dict):
            continue
        try:
            holdings[sym]["shares"] = max(0.0, float(item.get("shares", 0.0)))
            holdings[sym]["avg_cost"] = max(
                0.0, float(item.get("avg_cost", holdings[sym]["avg_cost"]))
            )
        except (TypeError, ValueError):
            continue
    return holdings


def _normalize_balances(raw: Any) -> dict[str, float]:
    balances = _default_balances()
    if not isinstance(raw, dict):
        return balances
    try:
        balances["cash_usd"] = max(0.0, float(raw.get("cash_usd", 0.0)))
    except (TypeError, ValueError):
        balances["cash_usd"] = 0.0
    try:
        balances["cash_cny"] = max(0.0, float(raw.get("cash_cny", 0.0)))
    except (TypeError, ValueError):
        balances["cash_cny"] = 0.0
    # 兼容旧版字段
    try:
        legacy_cny = max(0.0, float(raw.get("001015", 0.0))) + max(0.0, float(raw.get("007994", 0.0)))
    except (TypeError, ValueError):
        legacy_cny = 0.0
    if balances["cash_cny"] <= 0 and legacy_cny > 0:
        balances["cash_cny"] = legacy_cny
    return balances


def _load_from_supabase(user_id: str) -> tuple[dict[str, dict[str, float]], dict[str, float]] | None:
    conf = _db_conf()
    if not conf or not user_id:
        return None
    url = (
        f"{conf['url']}/rest/v1/portfolio_state"
        f"?user_id=eq.{user_id}&select=holdings,balances&limit=1"
    )
    headers = {
        "apikey": conf["key"],
        "Authorization": f"Bearer {conf['key']}",
    }
    r = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
    if r.status_code >= 400:
        return None
    rows = r.json()
    if not rows:
        return None
    row = rows[0]
    return _normalize_holdings(row.get("holdings")), _normalize_balances(row.get("balances"))


def _save_to_supabase(user_id: str, holdings: dict[str, dict[str, float]], balances: dict[str, float]) -> bool:
    conf = _db_conf()
    if not conf or not user_id:
        return False
    url = f"{conf['url']}/rest/v1/portfolio_state?on_conflict=user_id"
    headers = {
        "apikey": conf["key"],
        "Authorization": f"Bearer {conf['key']}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    payload = [
        {
            "user_id": user_id,
            "holdings": holdings,
            "balances": balances,
            "updated_at": datetime.now(_TZ_SHANGHAI).isoformat(),
        }
    ]
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=_HTTP_TIMEOUT)
    return r.status_code < 400


def _session_cloud_enabled() -> bool:
    """配置了 Supabase 即使用云端（持仓 + K 线缓存），与手动登录无关。"""
    return _db_conf() is not None


def _load_user_state(user_id: str) -> tuple[dict[str, dict[str, float]], dict[str, float], str]:
    if user_id and _session_cloud_enabled():
        cloud = _load_from_supabase(user_id)
        if cloud is not None:
            h, b = cloud
            return h, b, "cloud"
    # 未登录、未配置云端或读失败：本地 JSON
    return _load_holdings(), _load_balances(), "local"


def _save_user_state(
    user_id: str,
    holdings: dict[str, dict[str, float]],
    balances: dict[str, float],
) -> str:
    if user_id and _session_cloud_enabled() and _save_to_supabase(user_id, holdings, balances):
        return "cloud"
    _save_holdings(holdings)
    _save_balances(balances)
    return "local"


def _defaults_from_fetch() -> dict[str, float]:
    raw = _fetch_spot_prices()
    return {
        "voo": raw["VOO"],
        "qqq": raw["QQQ"],
        "tlt": raw["TLT"],
        "iei": raw["IEI"],
        "hs300": raw["001015"],
        "zz500": raw["007994"],
    }


def _ensure_price_session_defaults() -> None:
    d = _defaults_from_fetch()
    st.session_state.setdefault("def_voo", d["voo"])
    st.session_state.setdefault("def_qqq", d["qqq"])
    st.session_state.setdefault("def_tlt", d["tlt"])
    st.session_state.setdefault("def_iei", d["iei"])
    st.session_state.setdefault("def_hs300", d["hs300"])
    st.session_state.setdefault("def_zz500", d["zz500"])
    st.session_state.setdefault("_prices_initialized", True)


def _ensure_fx_session_default() -> None:
    if st.session_state.get("_fx_initialized"):
        return
    st.session_state.setdefault("def_fx", _fetch_usdcny_rate())
    st.session_state["_fx_initialized"] = True


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    try:
        if dt.tzinfo is not None:
            dt = dt.astimezone(_TZ_SHANGHAI).replace(tzinfo=None)
    except Exception:
        pass
    return dt.strftime("%Y-%m-%d")


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_rss_items(url: str, limit: int = 20) -> list[dict[str, str]]:
    try:
        r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        out: list[dict[str, str]] = []
        for it in root.findall("./channel/item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            desc = (it.findtext("description") or "").strip()
            out.append({"title": title, "link": link, "pubDate": pub, "description": desc})
            if len(out) >= max(1, int(limit)):
                break
        return out
    except Exception:
        return []


def _parse_rss_dt(s: str) -> datetime | None:
    try:
        return parsedate_to_datetime(s)
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def _latest_fomc_statement() -> dict[str, str]:
    items = _fetch_rss_items("https://www.federalreserve.gov/feeds/press_monetary.xml", limit=40)
    keys = ("fomc", "statement", "federal reserve issues fomc statement")
    for x in items:
        t = x.get("title", "").lower()
        if any(k in t for k in keys):
            d = _fmt_dt(_parse_rss_dt(x.get("pubDate", "")))
            return {"date": d, "title": x.get("title", "-"), "link": x.get("link", "")}
    if items:
        x = items[0]
        return {
            "date": _fmt_dt(_parse_rss_dt(x.get("pubDate", ""))),
            "title": x.get("title", "-"),
            "link": x.get("link", ""),
        }
    return {"date": "-", "title": "暂无可用数据", "link": ""}


@st.cache_data(ttl=3600, show_spinner=False)
def _recent_powell_speeches(limit: int = 3) -> list[dict[str, str]]:
    items = _fetch_rss_items("https://www.federalreserve.gov/feeds/speeches.xml", limit=80)
    out: list[dict[str, str]] = []
    for x in items:
        blob = f"{x.get('title','')} {x.get('description','')}".lower()
        if "powell" not in blob and "jerome h. powell" not in blob:
            continue
        out.append(
            {
                "date": _fmt_dt(_parse_rss_dt(x.get("pubDate", ""))),
                "title": x.get("title", "-"),
                "link": x.get("link", ""),
            }
        )
        if len(out) >= max(1, int(limit)):
            break
    return out


@st.cache_data(ttl=21600, show_spinner=False)
def _fred_inflation_snapshot(series_id: str) -> dict[str, str | float]:
    csv_url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    try:
        r = requests.get(csv_url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))
        if df.empty or "DATE" not in df.columns or series_id not in df.columns:
            raise ValueError("empty")
        s = pd.to_numeric(df[series_id], errors="coerce")
        d = pd.to_datetime(df["DATE"], errors="coerce")
        data = pd.DataFrame({"date": d, "value": s}).dropna().sort_values("date")
        if len(data) < 13:
            raise ValueError("not enough data")
        latest = float(data.iloc[-1]["value"])
        prev_12m = float(data.iloc[-13]["value"])
        yoy = (latest / prev_12m - 1.0) * 100.0 if prev_12m > 0 else 0.0
        latest_date = pd.Timestamp(data.iloc[-1]["date"]).strftime("%Y-%m")
        return {"series": series_id, "date": latest_date, "value": latest, "yoy": yoy}
    except Exception:
        return {"series": series_id, "date": "-", "value": 0.0, "yoy": 0.0}


def _inflation_comment(yoy: float, metric_name: str) -> str:
    if yoy >= 3.0:
        return f"{metric_name}同比仍偏高，通胀黏性较强。"
    if yoy >= 2.2:
        return f"{metric_name}同比回落中，但仍略高于美联储2%目标。"
    return f"{metric_name}同比接近2%目标区间，通胀压力相对温和。"


theme_name = st.sidebar.selectbox("显示主题", options=list(_UI_THEMES.keys()), index=0)
theme = _UI_THEMES[theme_name]
_apply_theme_css(theme)
chart_theme = st.sidebar.selectbox(
    "K线配色主题",
    options=list(CHART_THEME_OPTIONS),
    index=list(CHART_THEME_OPTIONS).index("Trading Dark") if "Trading Dark" in CHART_THEME_OPTIONS else 0,
    key="chart_plot_theme",
    help="Classic Light 浅色机构风；Trading Dark 暗色终端风（绿涨红跌）；CN Quant 红涨绿跌略饱和。",
)
st.sidebar.caption("显示主题影响盈亏颜色；K线主题只影响技术看板配色。")

cloud_user_id = st.sidebar.text_input(
    "用户ID（用于跨设备同步）",
    value="evan",
    key="sidebar_user_id",
    help="配置了 Supabase 时读写云端 portfolio；否则使用本地 JSON。",
).strip()

_db = _db_conf()
configure_market_storage(_db, read_only=bool(_db))

st.title(f"👋 Hello, {cloud_user_id or 'Guest'}")

if _db:
    st.sidebar.caption("存储后端：Supabase")
    try:
        _host = _db["url"].replace("https://", "").replace("http://", "").split("/")[0]
        _ref = _host.split(".")[0]
    except Exception:
        _ref = "?"
    _k_kind = _supabase_key_kind(_db.get("key", ""))
    st.sidebar.caption(f"Supabase ref={_ref} | key={_k_kind}")
else:
    st.sidebar.caption("存储后端：本地文件（未配置 Supabase Secrets）")

holdings, balances_for_view, storage_mode = _load_user_state(cloud_user_id)

# --- 技术看板（K 线）---
_chart_symbol_labels = {
    "VOO": "VOO",
    "QQQ": "QQQ",
    "债券(TLT)": "TLT",
    "债券(IEI)": "IEI",
    "沪深300ETF(510300)": "510300.SS",
    "中证500ETF(510500)": "510500.SS",
}
_chart_label_options = list(_chart_symbol_labels.keys())
_chart_pick_default_label = "沪深300ETF(510300)"
_chart_pick_default_index = (
    _chart_label_options.index(_chart_pick_default_label) if _chart_pick_default_label in _chart_label_options else 0
)
_chart_pick = st.selectbox(
    "看板标的",
    options=_chart_label_options,
    index=_chart_pick_default_index,
    key="chart_board_symbol",
)
_chart_yf = _chart_symbol_labels[_chart_pick]

# 持仓成本线：只要当前持仓里有有效成本，就画线（不再依赖 cloud/local 来源）
_chart_holdings_ok = True
_chart_user_avg_cost: float | None = None
try:
    _chart_hold = holdings.get(_chart_yf, {})  # type: ignore[assignment]
    _sh = float(_chart_hold.get("shares", 0.0))
    _ac = float(_chart_hold.get("avg_cost", 0.0))
    if _chart_holdings_ok and _sh > 0 and _ac > 0:
        _chart_user_avg_cost = _ac
except Exception:
    _chart_user_avg_cost = None


# --- 刷新市价（并可选同步当前K线到云端）---
if st.button(
    "刷新市价",
    help="拉取现价并刷新默认输入；K线同步在后台定时进行。",
):
    _fetch_spot_prices_meta.clear()
    _fetch_usdcny_rate_meta.clear()
    _fetch_vix_meta.clear()
    d = _defaults_from_fetch()
    st.session_state.def_fx = _fetch_usdcny_rate()
    st.session_state.def_voo = d["voo"]
    st.session_state.def_qqq = d["qqq"]
    st.session_state.def_tlt = d["tlt"]
    st.session_state.def_iei = d["iei"]
    st.session_state.def_hs300 = d["hs300"]
    st.session_state.def_zz500 = d["zz500"]

    # 删除输入框缓存值，让下方 number_input 用新的 def_* 作为默认值。
    for k in ("inp_fx", "inp_voo", "inp_qqq", "inp_tlt", "inp_iei", "inp_hs300", "inp_zz500"):
        if k in st.session_state:
            del st.session_state[k]

    st.success("已刷新市价（K线看板仅读 Supabase 缓存）")
_interval_display_map = {
    "日线（1d）": "1d",
    "15分钟（15m）": "15m",
    "5分钟（5m）": "5m",
}
_interval_default = list(_interval_display_map.keys())
_interval_pick = st.sidebar.multiselect(
    "看板加载哪些周期（不选则不拉取数据）",
    options=list(_interval_display_map.keys()),
    default=_interval_default,
    key="chart_intervals_to_load",
    help="关闭某个周期后，该周期对应的图表不会触发 fetch_ohlcv()，通常能显著减少加载时间。",
)
_interval_keys = [_interval_display_map[x] for x in _interval_pick]
if not _interval_keys:
    _interval_keys = ["1d"]

# 前端只读 Supabase 画图：不在页面线程里做同步

if _db_conf():
    _probe = probe_market_cache_status(_chart_yf, _interval_keys)  # type: ignore[arg-type]
    if _probe.get("reachable"):
        _hits = _probe.get("hits", {})
        _rows = _probe.get("rows", {})
        _lts = _probe.get("latest_ts", {})
        _hit_text = " / ".join(
            [f"{k}:{'有缓存' if _hits.get(k, False) else '空'}({int(_rows.get(k, 0))}条)" for k in _interval_keys]
        )
        _raw_rows = probe_symbol_interval_raw_rows(_chart_yf, _interval_keys)  # type: ignore[arg-type]
        _raw_text = " / ".join([f"{k}:{int(_raw_rows.get(k, 0))}条" for k in _interval_keys])
        _ts_text = " / ".join([f"{k}:{_lts.get(k, '-') or '-'}" for k in _interval_keys])
        st.sidebar.caption(f"Supabase 连通正常；{_chart_yf} 有效缓存：{_hit_text}")
        st.sidebar.caption(f"原始REST行数（同symbol/interval）：{_raw_text}")
        st.sidebar.caption(f"各周期最新时间：{_ts_text}")
        if all(int(_rows.get(k, 0)) <= 0 for k in _interval_keys):
            _recent = probe_recent_market_rows(limit=12)
            if _recent:
                _sample = " | ".join(
                    [f"{x.get('symbol','?')}/{x.get('interval','?')}@{x.get('ts','?')}" for x in _recent[:6]]
                )
                st.sidebar.warning(f"当前标的查询为空；库里最近记录样本：{_sample}")
            else:
                st.sidebar.warning("当前标的查询为空；且未能读取到 market_bars 最近记录样本。")
            _inv = probe_market_inventory(limit=1000)
            if _inv:
                _inv_text = " | ".join(
                    [f"{x.get('symbol','?')}/{x.get('interval','?')}:{x.get('rows',0)}条@{x.get('latest_ts','-')}" for x in _inv]
                )
                st.sidebar.caption(f"当前 Key 可读到的库内窗口概览：{_inv_text}")
    else:
        _err = _probe.get("error", "unknown")
        st.sidebar.warning(f"Supabase 连通探测失败：{_err}")

def _chart_load_progress(slot: Any, step: int, total: int, label: str) -> None:
    """页面进度条 + 服务端 print（Streamlit Cloud 日志可见）。"""
    if total <= 0:
        return
    frac = min(1.0, (step + 1) / total)
    msg = f"正在加载看板：{label}（{step + 1}/{total}）"
    print(f"[investment_calculator] {msg}", flush=True)
    try:
        slot.progress(frac, text=msg)
    except TypeError:
        slot.progress(frac)


_prog_slot = st.empty()
_fig_d = _fig_15 = _fig_5 = None
_chart_errs: dict[str, str] = {}
_nj = int("1d" in _interval_keys) + int("15m" in _interval_keys) + int("5m" in _interval_keys)
if _nj > 0:
    try:
        _prog0 = "从 Supabase 加载看板各周期…"
        _prog_slot.progress(0.0, text=_prog0)
    except TypeError:
        _prog_slot.progress(0)
    _fut_map: dict[Any, tuple[str, str]] = {}
    _workers = min(3, _nj)
    with ThreadPoolExecutor(max_workers=_workers) as _pool:
        if "1d" in _interval_keys:
            _f = _pool.submit(
                fig_daily,
                _chart_yf,
                _chart_pick,
                chart_theme=chart_theme,
                user_avg_cost=_chart_user_avg_cost,
                cache_only=True,
            )
            _fut_map[_f] = ("1d", "日线（1d）")
        if "15m" in _interval_keys:
            _f = _pool.submit(
                fig_15m_vwap_rsi,
                _chart_yf,
                _chart_pick,
                chart_theme=chart_theme,
                user_avg_cost=_chart_user_avg_cost,
                cache_only=True,
            )
            _fut_map[_f] = ("15m", "15m（15m）")
        if "5m" in _interval_keys:
            _f = _pool.submit(
                fig_5m_vwap_rsi7,
                _chart_yf,
                _chart_pick,
                chart_theme=chart_theme,
                user_avg_cost=_chart_user_avg_cost,
                cache_only=True,
            )
            _fut_map[_f] = ("5m", "5m（5m）")
        _done = 0
        for _fut in as_completed(_fut_map):
            _kind, _lab = _fut_map[_fut]
            try:
                _fig = _fut.result()
                if _kind == "1d":
                    _fig_d = _fig
                elif _kind == "15m":
                    _fig_15 = _fig
                else:
                    _fig_5 = _fig
            except Exception as e:
                # 并行任务失败时不让整页崩溃；打印日志并串行补一次。
                print(f"[investment_calculator] 并行绘图失败 {_lab}: {e}", flush=True)
                try:
                    if _kind == "1d":
                        _fig_d = fig_daily(
                            _chart_yf,
                            _chart_pick,
                            chart_theme=chart_theme,
                            user_avg_cost=_chart_user_avg_cost,
                            cache_only=True,
                        )
                    elif _kind == "15m":
                        _fig_15 = fig_15m_vwap_rsi(
                            _chart_yf,
                            _chart_pick,
                            chart_theme=chart_theme,
                            user_avg_cost=_chart_user_avg_cost,
                            cache_only=True,
                        )
                    else:
                        _fig_5 = fig_5m_vwap_rsi7(
                            _chart_yf,
                            _chart_pick,
                            chart_theme=chart_theme,
                            user_avg_cost=_chart_user_avg_cost,
                            cache_only=True,
                        )
                except Exception as e2:
                    print(f"[investment_calculator] 串行补偿失败 {_lab}: {e2}", flush=True)
                    _chart_errs[_kind] = str(e2)
            _chart_load_progress(_prog_slot, _done, _nj, _lab)
            _done += 1
    try:
        _done_msg = "看板数据加载完成"
        _prog_slot.progress(1.0, text=_done_msg)
    except TypeError:
        _prog_slot.progress(1.0)
    print(f"[investment_calculator] {_done_msg}", flush=True)
    _prog_slot.empty()

_tab_d, _tab_15, _tab_5 = st.tabs(
    ["日线（EMA·ATR·MACD）", "15m（VWAP·RSI·MACD）", "5m（VWAP·RSI·MACD）"]
)
with _tab_d:
    if "1d" in _interval_keys and _fig_d is not None:
        st.plotly_chart(_fig_d, width="stretch")
    elif "1d" in _interval_keys:
        st.warning(f"日线图加载失败：{_chart_errs.get('1d', '未知错误（请看 Cloud logs）')}")
    else:
        st.info("未选择日线（1d），本周期不拉取数据。")
with _tab_15:
    if "15m" in _interval_keys and _fig_15 is not None:
        st.plotly_chart(_fig_15, width="stretch")
    elif "15m" in _interval_keys:
        st.warning(f"15m 图加载失败：{_chart_errs.get('15m', '未知错误（请看 Cloud logs）')}")
    else:
        st.info("未选择15分钟（15m），本周期不拉取数据。")
with _tab_5:
    if "5m" in _interval_keys and _fig_5 is not None:
        st.plotly_chart(_fig_5, width="stretch")
    elif "5m" in _interval_keys:
        st.warning(f"5m 图加载失败：{_chart_errs.get('5m', '未知错误（请看 Cloud logs）')}")
    else:
        st.info("未选择5分钟（5m），本周期不拉取数据。")

# 前端不负责同步外部行情，避免阻塞与不确定性；由独立 sync worker 负责写 Supabase

st.divider()

with st.expander("开始定投", expanded=False):
    st.subheader("💵 开始定投")
    in_col1, in_col2 = st.columns(2)
    with in_col1:
        rmb = st.number_input("每月投入（人民币）", value=5000.0)
    with in_col2:
        _ensure_fx_session_default()
        fx = st.number_input("汇率（USD/CNY）", value=float(st.session_state.def_fx), key="inp_fx")

    st.markdown("#### 输入价格")
    spot_meta = _fetch_spot_prices_meta()
    fx_meta = _fetch_usdcny_rate_meta()
    spot_sources = spot_meta["source_by_symbol"]
    st.caption(
        "数据来源标签："
        f" 汇率={fx_meta['source']}（更新时间 {fx_meta['fetched_at']}）"
        f" | VOO={spot_sources['VOO']}, QQQ={spot_sources['QQQ']}, TLT={spot_sources['TLT']}, IEI={spot_sources['IEI']}"
        f" | 001015={spot_sources['001015']}, 007994={spot_sources['007994']}"
        f"（更新时间 {spot_meta['fetched_at']}）"
    )
    
    _ensure_price_session_defaults()
    
    col_p1, col_p2, col_p3 = st.columns(3)
    with col_p1:
        voo_price = st.number_input("VOO价格", value=float(st.session_state.def_voo), key="inp_voo")
        qqq_price = st.number_input("QQQ价格", value=float(st.session_state.def_qqq), key="inp_qqq")
    with col_p2:
        tlt_price = st.number_input("TLT价格", value=float(st.session_state.def_tlt), key="inp_tlt")
        iei_price = st.number_input("IEI价格", value=float(st.session_state.def_iei), key="inp_iei")
    with col_p3:
        hs300_price = st.number_input(
            "001015 沪深300 净值",
            value=float(st.session_state.def_hs300),
            key="inp_hs300",
        )
        zz500_price = st.number_input(
            "007994 中证500 净值",
            value=float(st.session_state.def_zz500),
            key="inp_zz500",
        )

    prices_now = {
        "VOO": voo_price,
        "QQQ": qqq_price,
        "TLT": tlt_price,
        "IEI": iei_price,
        "001015": hs300_price,
        "007994": zz500_price,
    }

    if st.button("计算"):
        weights_us = {
            "VOO": _TARGET_WEIGHTS["VOO"],
            "QQQ": _TARGET_WEIGHTS["QQQ"],
            "TLT": _TARGET_WEIGHTS["TLT"],
            "IEI": _TARGET_WEIGHTS["IEI"],
        }
        _, balances, _ = _load_user_state(cloud_user_id)
        us_ratio = sum(weights_us.values())
        cny_ratio = _TARGET_WEIGHTS["001015"] + _TARGET_WEIGHTS["007994"]
        usd_month_raw = (rmb * us_ratio) / fx
        usd_total = balances["cash_usd"] + usd_month_raw

        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("📈 投资结果")

        voo_budget_usd = usd_total * (weights_us["VOO"] / us_ratio)
        qqq_budget_usd = usd_total * (weights_us["QQQ"] / us_ratio)
        tlt_budget_usd = usd_total * (weights_us["TLT"] / us_ratio)
        iei_budget_usd = usd_total * (weights_us["IEI"] / us_ratio)

        # 美股按整股估算，剩余现金滚入下月
        voo_shares = int(voo_budget_usd // voo_price) if voo_price > 0 else 0
        qqq_shares = int(qqq_budget_usd // qqq_price) if qqq_price > 0 else 0
        tlt_shares = int(tlt_budget_usd // tlt_price) if tlt_price > 0 else 0
        iei_shares = int(iei_budget_usd // iei_price) if iei_price > 0 else 0
        us_allocated_usd = (
            voo_shares * voo_price + qqq_shares * qqq_price + tlt_shares * tlt_price + iei_shares * iei_price
        )
        cash_usd_next = max(0.0, usd_total - us_allocated_usd)

        cny_total = balances["cash_cny"] + rmb * cny_ratio
        hs300_amount = cny_total * (_TARGET_WEIGHTS["001015"] / cny_ratio) if cny_ratio > 0 else 0.0
        zz500_amount = cny_total * (_TARGET_WEIGHTS["007994"] / cny_ratio) if cny_ratio > 0 else 0.0
        hs300_units = (hs300_amount / hs300_price) if hs300_price > 0 else 0.0
        zz500_units = (zz500_amount / zz500_price) if zz500_price > 0 else 0.0
        cash_cny_next = max(0.0, cny_total - hs300_amount - zz500_amount)

        res_c1, res_c2 = st.columns(2)
        with res_c1:
            st.markdown("#### 🇺🇸 美股配置")
            st.info(f"**可用美元：** {usd_total:.2f} USD （含已有现金 {balances['cash_usd']:.2f}）\n\n"
                    f"**本次买入：** -{us_allocated_usd:.2f} USD\n\n"
                    f"**结转现金：** {cash_usd_next:.2f} USD")
            
            st.markdown(f"- **VOO**：预算 {voo_budget_usd:.2f} USD → 买 **{voo_shares}** 股")
            st.markdown(f"- **QQQ**：预算 {qqq_budget_usd:.2f} USD → 买 **{qqq_shares}** 股")
            st.markdown(f"- **TLT**：预算 {tlt_budget_usd:.2f} USD → 买 **{tlt_shares}** 股")
            st.markdown(f"- **IEI**：预算 {iei_budget_usd:.2f} USD → 买 **{iei_shares}** 股")

        with res_c2:
            st.markdown("#### 🇨🇳 A股配置")
            st.info(f"**可用人民币：** {cny_total:.2f} CNY （含已有现金 {balances['cash_cny']:.2f}）\n\n"
                    f"**本次买入：** -{hs300_amount + zz500_amount:.2f} CNY\n\n"
                    f"**结转现金：** {cash_cny_next:.2f} CNY")
            
            st.markdown(f"- **001015**：{hs300_amount:.2f} CNY → 买 **{hs300_units:.3f}** 份")
            st.markdown(f"- **007994**：{zz500_amount:.2f} CNY → 买 **{zz500_units:.3f}** 份")

        calc_buys = {
            "VOO": {"shares": float(voo_shares), "price": voo_price},
            "QQQ": {"shares": float(qqq_shares), "price": qqq_price},
            "TLT": {"shares": float(tlt_shares), "price": tlt_price},
            "IEI": {"shares": float(iei_shares), "price": iei_price},
            "001015": {"shares": hs300_units, "price": hs300_price},
            "007994": {"shares": zz500_units, "price": zz500_price},
        }
        if st.button("将本月定投更新到我的持仓"):
            holdings, balances_loaded, _ = _load_user_state(cloud_user_id)
            for sym, buy in calc_buys.items():
                holdings[sym] = _merge_buy(holdings[sym], buy["shares"], buy["price"])
            balances_loaded["cash_usd"] = cash_usd_next
            balances_loaded["cash_cny"] = cash_cny_next
            save_mode = _save_user_state(cloud_user_id, holdings, balances_loaded)
            st.success(f"已更新到持仓（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

st.subheader("📦 我的持仓")
st.caption(f"当前持仓读取来源：{'云端数据库' if storage_mode == 'cloud' else '本地文件'}")

with st.expander("编辑持仓（会保存）", expanded=False):
    with st.form("holdings_edit_form"):
        for sym, meta in _ASSET_META.items():
            c1, c2 = st.columns([1, 1])
            with c1:
                shares = st.number_input(
                    f"{meta['label']} 持有数量",
                    min_value=0.0,
                    value=float(holdings[sym]["shares"]),
                    step=1.0 if sym.endswith(".SS") else 0.01,
                    key=f"edit_shares_{sym}",
                )
            with c2:
                avg_cost = st.number_input(
                    f"{meta['label']} 持仓成本({meta['currency']})",
                    min_value=0.0,
                    value=float(holdings[sym]["avg_cost"]),
                    step=0.0001,
                    key=f"edit_cost_{sym}",
                )
            holdings[sym]["shares"] = shares
            holdings[sym]["avg_cost"] = avg_cost
        st.markdown("#### 现金余额（会保存）")
        balances_for_view["cash_usd"] = st.number_input(
            "现金美元（USD）",
            min_value=0.0,
            value=float(balances_for_view.get("cash_usd", 0.0)),
            step=0.01,
            key="edit_balance_cash_usd",
        )
        balances_for_view["cash_cny"] = st.number_input(
            "剩余人民币（CNY）",
            min_value=0.0,
            value=float(balances_for_view.get("cash_cny", 0.0)),
            step=0.01,
            key="edit_balance_cash_cny",
        )
        if st.form_submit_button("保存持仓"):
            save_mode = _save_user_state(cloud_user_id, holdings, balances_for_view)
            st.success(f"持仓已保存（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

rows = []
total_cost_cny = 0.0
total_value_cny = 0.0
value_cny_by_symbol: dict[str, float] = {}
pnl_cny_by_symbol: dict[str, float] = {}
daily_change_pct_by_symbol: dict[str, float] = spot_meta.get("daily_change_pct_by_symbol", {})  # type: ignore[assignment]
for sym, meta in _ASSET_META.items():
    shares = float(holdings[sym]["shares"])
    avg_cost = float(holdings[sym]["avg_cost"])
    current = float(prices_now[sym])
    cost = shares * avg_cost
    value = shares * current
    pnl = value - cost
    pnl_pct = (pnl / cost * 100) if cost > 0 else 0.0

    if meta["currency"] == "USD":
        cost_cny = cost * fx
        value_cny = value * fx
    else:
        cost_cny = cost
        value_cny = value

    total_cost_cny += cost_cny
    total_value_cny += value_cny
    value_cny_by_symbol[sym] = value_cny
    pnl_cny_by_symbol[sym] = value_cny - cost_cny
    rows.append(
        {
            "标的": meta["label"],
            "币种": meta["currency"],
            "当日涨跌%": round(daily_change_pct_by_symbol.get(sym, 0.0), 2),
            "浮动盈亏": round(pnl, 2),
            "涨跌幅%": round(pnl_pct, 2),
            "持有数量": round(shares, 3),
            "持仓成本": round(avg_cost, 4),
            "当前价": round(current, 4),
            "持仓市值": round(value, 2),
        }
    )

st.dataframe(rows, width="stretch", hide_index=True)
total_pnl_cny = total_value_cny - total_cost_cny
total_pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny > 0 else 0.0
cash_usd = float(balances_for_view.get("cash_usd", 0.0))
cash_cny = float(balances_for_view.get("cash_cny", 0.0))
total_balance_cny = cash_usd * fx + cash_cny
total_assets_cny = total_value_cny + total_balance_cny
ratio_rows = []
for sym, meta in _ASSET_META.items():
    target = _TARGET_WEIGHTS.get(sym, 0.0)
    current = (value_cny_by_symbol.get(sym, 0.0) / total_value_cny) if total_value_cny > 0 else 0.0
    ratio_rows.append(
        {
            "标的": meta["label"],
            "目标比例%": round(target * 100, 2),
            "当前比例%": round(current * 100, 2),
            "偏离(当前-目标)%": round((current - target) * 100, 2),
        }
    )

metric_cols = st.columns(5)
metric_cols[0].metric("总成本(折合CNY)", f"¥ {total_cost_cny:,.2f}")
metric_cols[1].metric("持仓市值(折合CNY)", f"¥ {total_value_cny:,.2f}")
metric_cols[2].metric("现金余额(折合CNY)", f"¥ {total_balance_cny:,.2f}")
metric_cols[3].metric("总资产(折合CNY)", f"¥ {total_assets_cny:,.2f}")
metric_cols[4].metric(
    "总浮盈亏(折合CNY)",
    f"¥ {total_pnl_cny:,.2f}",
    delta=f"{total_pnl_pct:.2f}%",
    delta_color=theme["delta_color"],
)
st.caption(f"💵 现金明细：USD {cash_usd:,.2f} ｜ CNY {cash_cny:,.2f}")

st.markdown("<br>", unsafe_allow_html=True)
st.markdown("### 📊 今日表现")

weighted_daily_pct = (
    sum(
        (value_cny_by_symbol.get(sym, 0.0) / total_value_cny) * daily_change_pct_by_symbol.get(sym, 0.0)
        for sym in _ASSET_META
    )
    if total_value_cny > 0
    else 0.0
)
weighted_daily_color = theme["profit_color"] if weighted_daily_pct >= 0 else theme["loss_color"]
st.markdown(
    f"**当日加权涨跌**：<span style='color:{weighted_daily_color}; font-weight:700; font-size:18px;'>{weighted_daily_pct:+.2f}%</span>",
    unsafe_allow_html=True,
)

daily_cols = st.columns(len(_ASSET_META))
for i, (sym, meta) in enumerate(_ASSET_META.items()):
    d = daily_change_pct_by_symbol.get(sym, 0.0)
    c = theme["profit_color"] if d >= 0 else theme["loss_color"]
    daily_cols[i].markdown(
        f"**{meta['label']}**<br><span style='color:{c}; font-weight:800; font-size:18px;'>{d:+.2f}%</span>",
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)
st.subheader("📈 资产分布与盈亏")
usd_symbols = ("VOO", "QQQ", "TLT", "IEI")
cny_symbols = ("001015", "007994")

bond_current = value_cny_by_symbol.get("TLT", 0.0) + value_cny_by_symbol.get("IEI", 0.0)
voo_current = value_cny_by_symbol.get("VOO", 0.0)
qqq_current = value_cny_by_symbol.get("QQQ", 0.0)

ratio_denominator = total_value_cny if total_value_cny > 0 else 0.0
voo_ratio = (voo_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
qqq_ratio = (qqq_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
tlt_ratio = (value_cny_by_symbol.get("TLT", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
iei_ratio = (value_cny_by_symbol.get("IEI", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
bond_ratio = (bond_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0

voo_target = _TARGET_WEIGHTS["VOO"] * 100.0
qqq_target = _TARGET_WEIGHTS["QQQ"] * 100.0
bond_target = (_TARGET_WEIGHTS["TLT"] + _TARGET_WEIGHTS["IEI"]) * 100.0

group1_df = pd.DataFrame(
    [
        {"标的组": "VOO", "类型": "当前比例%", "成分": "VOO", "比例%": round(voo_ratio, 2)},
        {"标的组": "VOO", "类型": "目标比例%", "成分": "目标", "比例%": round(voo_target, 2)},
        {"标的组": "QQQ", "类型": "当前比例%", "成分": "QQQ", "比例%": round(qqq_ratio, 2)},
        {"标的组": "QQQ", "类型": "目标比例%", "成分": "目标", "比例%": round(qqq_target, 2)},
        {"标的组": "债券", "类型": "当前比例%", "成分": "TLT", "比例%": round(tlt_ratio, 2)},
        {"标的组": "债券", "类型": "当前比例%", "成分": "IEI", "比例%": round(iei_ratio, 2)},
        {"标的组": "债券", "类型": "目标比例%", "成分": "目标", "比例%": round(bond_target, 2)},
    ]
)

group1_chart = (
    alt.Chart(group1_df)
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的组:N", sort=["VOO", "QQQ", "债券"]),
        xOffset=alt.XOffset("类型:N", sort=["当前比例%", "目标比例%"]),
        y=alt.Y("比例%:Q", title="比例(%)"),
        color=alt.Color(
            "成分:N",
            sort=["VOO", "QQQ", "TLT", "IEI", "目标"],
            scale=alt.Scale(
                domain=["VOO", "QQQ", "TLT", "IEI", "目标"],
                range=[theme["accent"], "#60a5fa", "#f59e0b", "#fbbf24", "#94a3b8"],
            ),
        ),
        order=alt.Order("成分:N", sort="ascending"),
        tooltip=["标的组:N", "类型:N", "成分:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="VOO / QQQ / 债券（债券当前柱由 TLT+IEI 堆叠）")
)
st.altair_chart(group1_chart, width="stretch")

hs300_ratio = (value_cny_by_symbol.get("001015", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
zz500_ratio = (value_cny_by_symbol.get("007994", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
hs300_target = _TARGET_WEIGHTS["001015"] * 100.0
zz500_target = _TARGET_WEIGHTS["007994"] * 100.0

group2_df = pd.DataFrame(
    [
        {"标的组": "沪深300", "比例%": round(hs300_ratio, 2)},
        {"标的组": "中证500", "比例%": round(zz500_ratio, 2)},
    ]
)
group2_chart = (
    alt.Chart(group2_df)
    .mark_arc(innerRadius=42)
    .encode(
        theta=alt.Theta("比例%:Q"),
        color=alt.Color(
            "标的组:N",
            sort=["沪深300", "中证500"],
            scale=alt.Scale(range=[theme["accent"], "#60a5fa"]),
        ),
        tooltip=["标的组:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="沪深300 / 中证500 当前占比", width=420, height=260)
)
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.altair_chart(group2_chart, width="stretch")

usd_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in usd_symbols) + cash_usd * fx
cny_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in cny_symbols) + cash_cny
currency_denominator = total_assets_cny if total_assets_cny > 0 else 0.0
usd_ratio = (usd_value_cny / currency_denominator * 100.0) if currency_denominator > 0 else 0.0
cny_ratio = (cny_value_cny / currency_denominator * 100.0) if currency_denominator > 0 else 0.0
usd_target = sum(_TARGET_WEIGHTS[sym] for sym in usd_symbols) * 100.0
cny_target = sum(_TARGET_WEIGHTS[sym] for sym in cny_symbols) * 100.0

group3_df = pd.DataFrame(
    [
        {"资产币种": "美元资产", "比例%": round(usd_ratio, 2)},
        {"资产币种": "人民币资产", "比例%": round(cny_ratio, 2)},
    ]
)
group3_chart = (
    alt.Chart(group3_df)
    .mark_arc(innerRadius=42)
    .encode(
        theta=alt.Theta("比例%:Q"),
        color=alt.Color(
            "资产币种:N",
            sort=["美元资产", "人民币资产"],
            scale=alt.Scale(range=[theme["accent"], "#94a3b8"]),
        ),
        tooltip=["资产币种:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="美元资产 / 人民币资产当前占比", width=420, height=260)
)
with chart_col2:
    st.altair_chart(group3_chart, width="stretch")

pnl_chart_df = pd.DataFrame(
    [
        {
            "标的": _ASSET_META[sym]["label"],
            "浮盈亏(CNY)": round(pnl_cny_by_symbol[sym], 2),
            "方向": "盈利" if pnl_cny_by_symbol[sym] >= 0 else "亏损",
        }
        for sym in _ASSET_META
    ]
)
pnl_chart = (
    alt.Chart(pnl_chart_df)
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的:N", sort=None),
        y=alt.Y("浮盈亏(CNY):Q"),
        color=alt.Color(
            "方向:N",
            scale=alt.Scale(range=[theme["profit_color"], theme["loss_color"]]),
            legend=None,
        ),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(CNY):Q", format=",.2f"), "方向:N"],
    )
    .properties(title="各标的浮盈亏（折合CNY）")
)
st.altair_chart(pnl_chart, width="stretch")

st.subheader("🎯 持仓比例对比")
st.dataframe(ratio_rows, width="stretch", hide_index=True)

st.subheader("🧮 再平衡买入建议")
if total_assets_cny <= 0:
    st.info("总资产为 0，暂无法生成再平衡建议。")
else:
    st.caption("仅针对美元资产（VOO/QQQ/TLT/IEI）。基金部分按你的固定定投节奏，不纳入这里。")
    rebalance_rows: list[dict[str, Any]] = []
    for sym in ("VOO", "QQQ", "TLT", "IEI"):
        meta = _ASSET_META[sym]
        tgt_w = _TARGET_WEIGHTS[sym]
        cur_cny = value_cny_by_symbol.get(sym, 0.0)
        # 若要“买入后仍达到目标权重”，单标的所需新增资金需解：
        # (cur + x) / (total + x) = target  => x = (target*total - cur) / (1-target)
        num = tgt_w * total_assets_cny - cur_cny
        den = 1.0 - tgt_w
        gap_cny = (num / den) if den > 0 else 0.0
        if gap_cny <= 0:
            continue
        px = float(prices_now[sym])
        q = _fetch_us_etf_pe_drawdown(sym)
        pe = q.get("pe")
        dd = q.get("drawdown_pct")
        gap_usd = gap_cny / fx if fx > 0 else 0.0
        need_shares = (gap_usd / px) if px > 0 else 0.0
        whole_shares_need = int(gap_usd // px) if px > 0 else 0
        affordable_shares = int(cash_usd // px) if px > 0 else 0
        usd_needed_for_one = max(0.0, px - cash_usd) if sym in ("VOO", "QQQ") and affordable_shares < 1 else 0.0

        pe_txt = f"{float(pe):.1f}" if isinstance(pe, (int, float)) else "N/A"
        pe_band = _USD_ASSET_PE_BANDS.get(sym)
        pe_band_txt = f"{pe_band[0]:.0f} - {pe_band[1]:.0f}" if pe_band else "N/A"
        dd_txt = f"{float(dd):.2f}%" if isinstance(dd, (int, float)) else "N/A"
        if isinstance(dd, (int, float)) and float(dd) <= -10:
            market_hint = "近期回撤较大，可分批偏积极补仓"
        elif isinstance(pe, (int, float)) and float(pe) >= 30:
            market_hint = "估值偏高，建议分3-4批慢慢买"
        elif isinstance(pe, (int, float)) and float(pe) >= 24:
            market_hint = "估值略贵，建议分批买入"
        else:
            market_hint = "估值/位置中性，按目标缺口逐步补仓"
        if sym == "QQQ" and isinstance(dd, (int, float)) and float(dd) <= -12:
            market_hint = "QQQ 回撤明显，可优先于其他美元资产补仓（仍分批）"
        cash_hint = (
            f"你当前美元现金可买 {affordable_shares} 股；距离买1股还差 {usd_needed_for_one:.2f} USD"
            if sym in ("VOO", "QQQ") and affordable_shares < 1
            else f"你当前美元现金可买 {affordable_shares} 股"
        )
        rebalance_rows.append(
            {
                "标的": meta["label"],
                "目标缺口(CNY,买后口径)": round(gap_cny, 2),
                "目标缺口(USD,买后口径)": round(gap_usd, 2),
                "PE": pe_txt,
                "参考PE区间": pe_band_txt,
                "近60日回撤": dd_txt,
                "按当前价需买(股)": round(need_shares, 3),
                "整股需求(股)": whole_shares_need,
                "现金可买(股)": affordable_shares,
                "说明": f"{market_hint}；{cash_hint}",
            }
        )
    if rebalance_rows:
        st.dataframe(pd.DataFrame(rebalance_rows), width="stretch", hide_index=True)
    else:
        st.success("当前美元资产已不低于目标权重，暂无必须新增买入项。")

    st.markdown("#### 美股 VIX（CBOE）参考")
    _vix_meta = _fetch_vix_meta()
    _vix_val = float(_vix_meta["vix"])
    _vix_chg = float(_vix_meta["change_pct"])
    _vix_tag, _vix_note = _vix_regime(_vix_val)
    st.markdown(
        f"当前：`{_vix_val:.2f}`（{_vix_chg:+.2f}%）｜区间判定：**{_vix_tag}**  \n"
        f"{_vix_note}  \n"
        "参考区间：`<15 低波动` · `15-20 中性` · `20-30 偏高波动` · `>=30 高波动/恐慌`"
    )
    st.caption(f"VIX 数据源：{_vix_meta['source']}（更新时间 {_vix_meta['fetched_at']}）")
