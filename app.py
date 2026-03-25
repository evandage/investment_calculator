import re
import json
from pathlib import Path
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st

from chart_boards import (
    CHART_THEME_OPTIONS,
    fig_15m_vwap_rsi,
    fig_5m_vwap_rsi7,
    fig_daily,
    multiframe_signal_bundle,
)

# 拉取失败时的回退价（与常见区间一致）
_FALLBACK = {
    "VOO": 400.0,
    "QQQ": 500.0,
    "TLT": 90.0,
    "510300.SS": 4.0,
}

_TICKERS = {
    "voo": "VOO",
    "qqq": "QQQ",
    "tlt": "TLT",
    "hs300": "510300.SS",  # 华泰柏瑞沪深300ETF
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
_QQ_US = {"VOO": "usVOO", "QQQ": "usQQQ", "TLT": "usTLT"}
_SINA_GB = {"VOO": "gb_voo", "QQQ": "gb_qqq", "TLT": "gb_tlt"}
_SINA_CN = {"510300.SS": "sh510300"}

_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "QQQ": {"label": "QQQ", "currency": "USD"},
    "TLT": {"label": "债券(TLT)", "currency": "USD"},
    "510300.SS": {"label": "沪深300ETF", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    "VOO": 0.4,
    "QQQ": 0.2,
    "TLT": 0.2,
    "510300.SS": 0.2,
}


def _rebalance_alerts(
    value_cny_by_symbol: dict[str, float],
    total_value_cny: float,
    threshold: float = 0.05,
) -> list[dict[str, Any]]:
    """相对目标权重的偏离提示（默认阈值 5%）。"""
    if total_value_cny <= 0:
        return []
    rows: list[dict[str, Any]] = []
    for sym, tgt in _TARGET_WEIGHTS.items():
        cur = value_cny_by_symbol.get(sym, 0.0) / total_value_cny
        diff = cur - tgt
        if abs(diff) >= threshold:
            rows.append(
                {
                    "标的": _ASSET_META[sym]["label"],
                    "当前%": round(cur * 100, 2),
                    "目标%": round(tgt * 100, 2),
                    "偏离(当前-目标)%": round(diff * 100, 2),
                    "提示": "偏高，可考虑减/再平衡" if diff > 0 else "偏低，可考虑加/定投",
                }
            )
    return rows


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
        .stMetric {{
            background: {theme["card_bg"]};
            border: 1px solid rgba(148, 163, 184, 0.22);
            border-radius: 16px;
            padding: 12px 14px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
        }}
        .stButton > button, .stDownloadButton > button {{
            border-radius: 12px;
            border: 1px solid {theme["accent"]};
            box-shadow: 0 8px 20px rgba(37, 99, 235, 0.12);
        }}
        div[data-testid="stCaptionContainer"] p {{
            color: #475569;
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

    for sym in ("VOO", "QQQ", "TLT"):
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

    # A股：只用新浪（避免“价格/涨跌%口径混用”导致的错乱）
    symbols = list(_TICKERS.values())
    for sym in ("510300.SS",):
        try:
            res = _fetch_sina_cn_price_change(_SINA_CN[sym])
            if res is not None:
                p, change_pct = res
                out[sym] = p
                daily_change_pct_by_symbol[sym] = change_pct
                source_by_symbol[sym] = "新浪A股"
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
    return {"510300.SS": 0.0}


def _load_balances() -> dict[str, float]:
    if not _BALANCE_FILE.exists():
        return _default_balances()
    try:
        data = json.loads(_BALANCE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_balances()
    out = _default_balances()
    for sym in out:
        try:
            out[sym] = max(0.0, float(data.get(sym, 0.0)))
        except (TypeError, ValueError):
            pass
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
    for sym in balances:
        try:
            balances[sym] = max(0.0, float(raw.get(sym, 0.0)))
        except (TypeError, ValueError):
            continue
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


def _load_user_state(user_id: str) -> tuple[dict[str, dict[str, float]], dict[str, float], str]:
    if user_id:
        cloud = _load_from_supabase(user_id)
        if cloud is not None:
            h, b = cloud
            return h, b, "cloud"
    # Cloud 未配置或读失败时，回退本地
    return _load_holdings(), _load_balances(), "local"


def _save_user_state(
    user_id: str,
    holdings: dict[str, dict[str, float]],
    balances: dict[str, float],
) -> str:
    if user_id and _save_to_supabase(user_id, holdings, balances):
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
        "hs300": raw["510300.SS"],
    }


def _ensure_price_session_defaults() -> None:
    if st.session_state.get("_prices_initialized"):
        return
    d = _defaults_from_fetch()
    st.session_state.setdefault("def_voo", d["voo"])
    st.session_state.setdefault("def_qqq", d["qqq"])
    st.session_state.setdefault("def_tlt", d["tlt"])
    st.session_state.setdefault("def_hs300", d["hs300"])
    st.session_state["_prices_initialized"] = True


@st.cache_data(ttl=120, show_spinner=False)
def _cached_signal_bundle(symbol: str) -> dict[str, Any]:
    return multiframe_signal_bundle(symbol)


def _ensure_fx_session_default() -> None:
    if st.session_state.get("_fx_initialized"):
        return
    st.session_state.setdefault("def_fx", _fetch_usdcny_rate())
    st.session_state["_fx_initialized"] = True


st.title("📊 资产配置与定投仪表盘")
theme_name = st.sidebar.selectbox("显示主题", options=list(_UI_THEMES.keys()), index=0)
theme = _UI_THEMES[theme_name]
_apply_theme_css(theme)
chart_theme = st.sidebar.selectbox(
    "K线配色主题",
    options=list(CHART_THEME_OPTIONS),
    index=0,
    key="chart_plot_theme",
    help="Classic Light 浅色机构风；Trading Dark 暗色终端风；CN Quant 红涨绿跌略饱和。",
)
st.sidebar.caption("显示主题影响盈亏颜色；K线主题只影响技术看板配色。")

# --- 技术看板（K 线）---
_chart_symbol_labels = {meta["label"]: sym for sym, meta in _ASSET_META.items()}
st.caption(
    "看板不会静默后台自动刷新：切换选项、点「刷新市价」或浏览器刷新页面时会重新拉取行情。"
    " 主图含成交量与 Volume Profile；日线/分钟图含 ATR 带与 MACD。"
    " **决策组合包**（多周期评分+再平衡）在「总浮盈亏」指标下方展开。"
)
_chart_pick = st.selectbox(
    "看板标的",
    options=list(_chart_symbol_labels.keys()),
    index=0,
    key="chart_board_symbol",
)
_chart_yf = _chart_symbol_labels[_chart_pick]
_tab_d, _tab_15, _tab_5 = st.tabs(
    ["日线（EMA·ATR·MACD）", "15m（VWAP·RSI·MACD）", "5m（VWAP·RSI·MACD）"]
)
with _tab_d:
    st.plotly_chart(fig_daily(_chart_yf, _chart_pick, chart_theme=chart_theme), width="stretch")
with _tab_15:
    st.plotly_chart(fig_15m_vwap_rsi(_chart_yf, _chart_pick, chart_theme=chart_theme), width="stretch")
with _tab_5:
    st.plotly_chart(fig_5m_vwap_rsi7(_chart_yf, _chart_pick, chart_theme=chart_theme), width="stretch")

st.divider()
user_id = st.sidebar.text_input("用户ID（用于跨设备同步）", value="evan").strip()
if _db_conf():
    st.sidebar.caption("存储后端：Supabase")
else:
    st.sidebar.caption("存储后端：本地文件（未配置 Supabase Secrets）")

if st.button(
    "刷新市价",
    help="腾讯财经(美股)拉取现价；失败时用新浪全球。A股统一使用新浪A股。约 1 分钟内会走缓存。",
):
    _fetch_spot_prices_meta.clear()
    _fetch_usdcny_rate_meta.clear()
    d = _defaults_from_fetch()
    st.session_state.def_fx = _fetch_usdcny_rate()
    st.session_state.def_voo = d["voo"]
    st.session_state.def_qqq = d["qqq"]
    st.session_state.def_tlt = d["tlt"]
    st.session_state.def_hs300 = d["hs300"]
    for k in ("inp_fx", "inp_voo", "inp_qqq", "inp_tlt", "inp_hs300"):
        if k in st.session_state:
            del st.session_state[k]
    st.rerun()

with st.expander("开始定投", expanded=False):
    rmb = st.number_input("每月投入（人民币）", value=5000.0)
    _ensure_fx_session_default()
    fx = st.number_input("汇率（USD/CNY）", value=float(st.session_state.def_fx), key="inp_fx")

    st.subheader("输入价格")
    spot_meta = _fetch_spot_prices_meta()
    fx_meta = _fetch_usdcny_rate_meta()
    spot_sources = spot_meta["source_by_symbol"]
    st.caption(
        "数据来源标签："
        f" 汇率={fx_meta['source']}（更新时间 {fx_meta['fetched_at']}）"
        f" | VOO={spot_sources['VOO']}, QQQ={spot_sources['QQQ']}, 债券(TLT)={spot_sources['TLT']}"
        f" | 沪深300={spot_sources['510300.SS']}"
        f"（更新时间 {spot_meta['fetched_at']}）"
    )
    col_a, col_b = st.columns([1, 1])
    with col_a:
        st.empty()

    _ensure_price_session_defaults()

    voo_price = st.number_input("VOO价格", value=float(st.session_state.def_voo), key="inp_voo")
    qqq_price = st.number_input("QQQ价格", value=float(st.session_state.def_qqq), key="inp_qqq")
    tlt_price = st.number_input("TLT价格", value=float(st.session_state.def_tlt), key="inp_tlt")

    hs300_price = st.number_input("沪深300价格", value=float(st.session_state.def_hs300), key="inp_hs300")

    prices_now = {
        "VOO": voo_price,
        "QQQ": qqq_price,
        "TLT": tlt_price,
        "510300.SS": hs300_price,
    }

    if st.button("计算"):
        weights_us = {"VOO": 0.4, "QQQ": 0.2, "TLT": 0.2}
        us_ratio = sum(weights_us.values())
        usd_total_raw = (rmb * us_ratio) / fx
        usd_total = round(usd_total_raw)

        st.subheader("📈 投资结果")

        st.write("### 美股")
        voo_usd = usd_total * (0.4 / us_ratio)
        tlt_usd = usd_total * (0.2 / us_ratio)
        qqq_usd = usd_total * (0.2 / us_ratio)

        st.write(f"VOO：{voo_usd:.2f} USD → {voo_usd/voo_price:.3f} 股")
        st.write(f"QQQ：{qqq_usd:.2f} USD → {qqq_usd/qqq_price:.3f} 股")
        st.write(f"TLT：{tlt_usd:.2f} USD → {tlt_usd/tlt_price:.3f} 股")

        us_allocated_usd = voo_usd + qqq_usd + tlt_usd
        st.write(
            f"**美股美元合计：{us_allocated_usd:.2f} USD**（本月按整数美元换汇，原始应换约 {usd_total_raw:.2f} USD）"
        )

        st.write("### A股")

        _, balances, _ = _load_user_state(user_id)
        hs300_amount = rmb * 0.2
        hs300_budget = hs300_amount + balances["510300.SS"]
        hs300_lot_cost = hs300_price * 100
        hs300_lots = int(hs300_budget // hs300_lot_cost) if hs300_lot_cost > 0 else 0
        hs300_balance_next = hs300_budget - hs300_lots * hs300_lot_cost

        st.write(f"沪深300：{hs300_lots*100} 股（{hs300_lots} 手）")
        st.caption(f"A股余额结转：沪深300 结转 {hs300_balance_next:.2f} CNY")

        calc_buys = {
            "VOO": {"shares": voo_usd / voo_price, "price": voo_price},
            "QQQ": {"shares": qqq_usd / qqq_price, "price": qqq_price},
            "TLT": {"shares": tlt_usd / tlt_price, "price": tlt_price},
            "510300.SS": {"shares": hs300_lots * 100.0, "price": hs300_price},
        }
        if st.button("将本月定投更新到我的持仓"):
            holdings, balances_loaded, _ = _load_user_state(user_id)
            for sym, buy in calc_buys.items():
                holdings[sym] = _merge_buy(holdings[sym], buy["shares"], buy["price"])
            balances_loaded["510300.SS"] = hs300_balance_next
            save_mode = _save_user_state(user_id, holdings, balances_loaded)
            st.success(f"已更新到持仓（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

st.subheader("📦 我的持仓")
holdings, balances_for_view, storage_mode = _load_user_state(user_id)
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
        st.markdown("#### A股结转余额（未凑够一手的现金）")
        balances_for_view["510300.SS"] = st.number_input(
            "沪深300 结转余额（CNY）",
            min_value=0.0,
            value=float(balances_for_view.get("510300.SS", 0.0)),
            step=0.01,
            key="edit_balance_hs300",
        )
        if st.form_submit_button("保存持仓"):
            save_mode = _save_user_state(user_id, holdings, balances_for_view)
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
            "结转余额(CNY)": round(balances_for_view.get(sym, 0.0), 2)
            if meta["currency"] == "CNY"
            else 0.0,
        }
    )

st.dataframe(rows, width="stretch", hide_index=True)
total_pnl_cny = total_value_cny - total_cost_cny
total_pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny > 0 else 0.0
total_balance_cny = float(balances_for_view.get("510300.SS", 0.0))
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

metric_cols = st.columns(4)
metric_cols[0].metric("总成本(折合CNY)", f"{total_cost_cny:,.2f}")
metric_cols[1].metric("持仓市值(折合CNY)", f"{total_value_cny:,.2f}")
metric_cols[2].metric("A股结转余额(折合CNY)", f"{total_balance_cny:,.2f}")
metric_cols[3].metric("总资产(折合CNY)", f"{total_assets_cny:,.2f}")
st.metric(
    "总浮盈亏(折合CNY)",
    f"{total_pnl_cny:,.2f}",
    delta=f"{total_pnl_pct:.2f}%",
    delta_color=theme["delta_color"],
)

with st.expander("决策组合包（多周期评分 + 再平衡）", expanded=False):
    _sig = _cached_signal_bundle(_chart_yf)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("日线趋势分", f"{_sig['daily']}" if _sig["daily"] is not None else "—")
    c2.metric("15m 分", f"{_sig['m15']}" if _sig["m15"] is not None else "—")
    c3.metric("5m 分", f"{_sig['m5']}" if _sig["m5"] is not None else "—")
    c4.metric("合计分", str(_sig["total"]))
    st.caption(_sig["summary"])
    st.caption(
        "评分规则（粗）：日线 EMA20>EMA50 记 1；"
        "15m 价≥VWAP 且 RSI(14)<72 记 1；"
        "5m RSI(7)<75 记 1。仅供参考。"
    )
    if total_value_cny > 0:
        _alerts = _rebalance_alerts(value_cny_by_symbol, total_value_cny)
        if _alerts:
            st.warning("以下标的相对目标权重偏离 ≥5%，可考虑再平衡：")
            st.dataframe(_alerts, width="stretch", hide_index=True)
        else:
            st.success("各标的偏离目标权重均在 5% 阈值内（或持仓过小）。")
    else:
        st.info("暂无持仓市值，无法计算再平衡偏离。")

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
    f"当日加权涨跌：<span style='color:{weighted_daily_color}; font-weight:700; font-size:18px;'>{weighted_daily_pct:+.2f}%</span>",
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

st.subheader("📊 可视化")
chart_col1, chart_col2 = st.columns(2)

value_chart_df = pd.DataFrame(
    [
        {"标的": _ASSET_META[sym]["label"], "当前市值(CNY)": round(value_cny_by_symbol[sym], 2)}
        for sym in _ASSET_META
        if value_cny_by_symbol.get(sym, 0.0) > 0
    ]
)
if not value_chart_df.empty:
    pie_chart = (
        alt.Chart(value_chart_df)
        .mark_arc(innerRadius=55)
        .encode(
            theta=alt.Theta("当前市值(CNY):Q"),
            color=alt.Color("标的:N"),
            tooltip=["标的:N", alt.Tooltip("当前市值(CNY):Q", format=",.2f")],
        )
        .properties(title="当前持仓市值占比")
    )
    chart_col1.altair_chart(pie_chart, width="stretch")
else:
    chart_col1.info("暂无持仓市值数据可供可视化。")

ratio_chart_df = pd.DataFrame(ratio_rows).melt(
    id_vars="标的",
    value_vars=["目标比例%", "当前比例%"],
    var_name="类型",
    value_name="比例%",
)
ratio_chart = (
    alt.Chart(ratio_chart_df)
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的:N", sort=None),
        xOffset="类型:N",
        y=alt.Y("比例%:Q"),
        color=alt.Color("类型:N", scale=alt.Scale(range=[theme["accent"], "#94a3b8"])),
        tooltip=["标的:N", "类型:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="目标比例 vs 当前比例")
)
chart_col2.altair_chart(ratio_chart, width="stretch")

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
