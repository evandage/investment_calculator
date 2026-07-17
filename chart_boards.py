"""K 线看板：日线 EMA+RSI、15m/5m VWAP+RSI。

行情源：统一优先东方财富；若东方财富不可用，才兜底 yfinance。
环境变量：YFINANCE_CHART_TIMEOUT（默认 90）、YFINANCE_CHART_RETRIES（默认 4）、
YFINANCE_MIN_GAP_SECONDS（两次 yfinance 请求最小间隔，默认 15，减轻限流）、
MARKET_SYNC_MIN_SECONDS（K 线增量同步最小间隔，默认 180）。
"""

from __future__ import annotations

import logging
import os
import threading
import time
import concurrent.futures
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
from plotly.subplots import make_subplots

_EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_EASTMONEY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
}
_TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_TENCENT_HEADERS = {
    "User-Agent": _EASTMONEY_HEADERS["User-Agent"],
    "Referer": "https://finance.qq.com/",
}
_HTTP_TIMEOUT = (3, 12)
_ROOT_DIR = Path(__file__).resolve().parent
_SOURCE_CACHE_TTL_SECONDS = {"1d": 300.0, "15m": 45.0, "5m": 30.0}
_SOURCE_CACHE: dict[tuple[str, str, str], tuple[pd.DataFrame, str, float]] = {}
_MIN_FUTU_INTRADAY_HISTORY_BARS = {"15m": 3, "5m": 4}

_YF_TIMEOUT = float(os.environ.get("YFINANCE_CHART_TIMEOUT", "90"))
_YF_RETRIES = max(1, int(os.environ.get("YFINANCE_CHART_RETRIES", "4")))
_YF_MIN_GAP = float(os.environ.get("YFINANCE_MIN_GAP_SECONDS", "15"))
_YF_LAST_MONO = 0.0
_YF_PACE_LOCK = threading.Lock()
_SUPABASE_CONF: dict[str, str] | None = None
_SUPABASE_SESSION: requests.Session | None = None
_SUPABASE_READ_ONLY = bool(int(os.environ.get("SUPABASE_READ_ONLY", "0")))
_LAST_SYNC_AT: dict[tuple[str, str], float] = {}
_OHLCV_MEMORY_CACHE: dict[tuple[str, str], pd.DataFrame] = {}
_SYNC_MIN_SECONDS = max(60, int(os.environ.get("MARKET_SYNC_MIN_SECONDS", "180")))
_MARKET_DATA_PROVIDER = "tencent"
_EARNINGS_DATE_CACHE: dict[str, tuple[pd.Timestamp | None, float]] = {}
_EARNINGS_DATE_TTL_SECONDS = 6 * 60 * 60
_ETF_SYMBOLS = {"VOO", "QQQ", "SGOV"}

AVWAP_MODE_LABELS = {
    "none": "无",
    "earnings": "最近财报反应日",
    "year_start": "年初",
    "high_60d": "最近 Swing High",
    "low_60d": "最近 Swing Low",
    "gap_60d": "最近 Gap 日",
    "selloff_60d": "最近60日大跌低点",
    "rally_60d": "最近60日大涨日",
    "today_open": "今日开盘",
}


def _naive_day(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()

# Supabase 行情缓存保留期（避免分钟线无限增长导致切换标的变慢）
# - 1d：需要足够长用于 EMA200/MACD/ATR 计算（这里留 ~450 天）
# - 15m/5m：图上只看当日/昨日，缓存留一周足够
_SUPABASE_RETENTION_DAYS: dict[str, int] = {"1d": 450, "15m": 2, "5m": 2}
# 单次 GET market_bars 行数上限（过大则 JSON 解析与传输慢；日线 EMA200 约 300 根即可）
_SUPABASE_READ_LIMIT: dict[str, int] = {"1d": 900, "15m": 1600, "5m": 3600}


def _yf_pace() -> None:
    """同一进程内两次 yfinance 请求之间留出间隔，减轻 Yahoo 限流。"""
    global _YF_LAST_MONO
    with _YF_PACE_LOCK:
        now = time.monotonic()
        if _YF_LAST_MONO > 0 and (now - _YF_LAST_MONO) < _YF_MIN_GAP:
            time.sleep(_YF_MIN_GAP - (now - _YF_LAST_MONO))
        _YF_LAST_MONO = time.monotonic()


def _is_yf_rate_limited(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    return "ratelimit" in name or "too many" in msg or "rate limit" in msg


def _yf_ticker_history(import_yf: Any, symbol: str, **kwargs: Any) -> pd.DataFrame:
    """调用 Ticker.history；旧版 yfinance 无 timeout 时自动降级。"""
    t = import_yf.Ticker(symbol)
    try:
        return t.history(timeout=int(_YF_TIMEOUT), **kwargs)
    except TypeError:
        return t.history(**kwargs)


# 东财美股 secid 与行情中心分类一致（与 Yahoo  ticker 不同前缀）
_EASTMONEY_US_SECID = {
    "VOO": "107.VOO",
    "QQQ": "105.QQQ",
    "AVGO": "105.AVGO",
    "NVDA": "105.NVDA",
    "TEM": "106.TEM",
    "PLTR": "106.PLTR",
    "GOOGL": "105.GOOGL",
    "MSFT": "105.MSFT",
    "ISRG": "105.ISRG",
    "SGOV": "106.SGOV",
}
_TENCENT_US_KLINE = {
    "VOO": "usVOO.AM",
    "QQQ": "usQQQ.OQ",
    "AVGO": "usAVGO.OQ",
    "NVDA": "usNVDA.OQ",
    "TEM": "usTEM.N",
    "PLTR": "usPLTR.N",
    "GOOGL": "usGOOGL.OQ",
    "MSFT": "usMSFT.OQ",
    "ISRG": "usISRG.OQ",
    "SGOV": "usSGOV.AM",
}


_CH_FONT_FAMILY = "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif"

# 可切换 K 线主题（配色 + 网格 + hover）
CHART_THEMES: dict[str, dict[str, Any]] = {
    "Classic Light": {
        "paper": "#f6f7f9",
        "plot": "#f6f7f9",
        "grid": "rgba(100, 116, 139, 0.11)",
        "muted": "#64748b",
        "up_line": "#a63d3d",
        "up_fill": "rgba(166, 61, 61, 0.22)",
        "dn_line": "#3f6b5c",
        "dn_fill": "rgba(63, 107, 92, 0.22)",
        "vol_up": "rgba(166, 61, 61, 0.38)",
        "vol_dn": "rgba(63, 107, 92, 0.38)",
        "ema": ("#b45309", "#475569", "#6d5f8a", "#94a3b8"),
        "vwap": "#0d9488",
        "vwap_band": "rgba(13, 148, 136, 0.42)",
        "vwap_fill": "rgba(13, 148, 136, 0.055)",
        "rsi": "#6366f1",
        "rsi_ma": "#475569",
        "rsi_orange": "#b45309",
        "rsi_ob": "#ef4444",
        "rsi_mid": "#3b82f6",
        "ref_line": "rgba(148, 163, 184, 0.55)",
        "atr_upper": "rgba(139, 92, 246, 0.75)",
        "atr_lower": "rgba(139, 92, 246, 0.45)",
        "macd_line": "#2563eb",
        "macd_sig": "#f59e0b",
        "macd_pos": "rgba(37, 99, 235, 0.45)",
        "macd_neg": "rgba(220, 38, 38, 0.35)",
        "vp_bar": "rgba(99, 102, 241, 0.35)",
        "legend_bg": "rgba(246, 247, 249, 0.88)",
        "hover_bg": "rgba(255, 255, 255, 0.96)",
        "hover_border": "rgba(148, 163, 184, 0.35)",
        "spike": "rgba(100, 116, 139, 0.25)",
    },
    "Trading Dark": {
        "paper": "#0b0f14",
        "plot": "#0b0f14",
        "grid": "rgba(148, 163, 184, 0.09)",
        "muted": "#94a3b8",
        "up_line": "#4ade80",
        "up_fill": "rgba(74, 222, 128, 0.22)",
        "dn_line": "#f87171",
        "dn_fill": "rgba(248, 113, 113, 0.18)",
        "vol_up": "rgba(74, 222, 128, 0.4)",
        "vol_dn": "rgba(248, 113, 113, 0.35)",
        "ema": ("#fbbf24", "#60a5fa", "#a78bfa", "#94a3b8"),
        "vwap": "#2dd4bf",
        "vwap_band": "rgba(45, 212, 191, 0.45)",
        "vwap_fill": "rgba(45, 212, 191, 0.07)",
        "rsi": "#a78bfa",
        "rsi_ma": "#cbd5e1",
        "rsi_orange": "#fb923c",
        "rsi_ob": "#fb7185",
        "rsi_mid": "#38bdf8",
        "ref_line": "rgba(148, 163, 184, 0.45)",
        "atr_upper": "rgba(167, 139, 250, 0.85)",
        "atr_lower": "rgba(167, 139, 250, 0.5)",
        "macd_line": "#60a5fa",
        "macd_sig": "#fbbf24",
        "macd_pos": "rgba(96, 165, 250, 0.5)",
        "macd_neg": "rgba(248, 113, 113, 0.45)",
        "vp_bar": "rgba(129, 140, 248, 0.45)",
        "legend_bg": "rgba(15, 23, 42, 0.75)",
        "hover_bg": "rgba(15, 23, 42, 0.92)",
        "hover_border": "rgba(148, 163, 184, 0.28)",
        "spike": "rgba(148, 163, 184, 0.2)",
    },
    "CN Quant": {
        "paper": "#fafafa",
        "plot": "#fafafa",
        "grid": "rgba(71, 85, 105, 0.12)",
        "muted": "#475569",
        "up_line": "#dc2626",
        "up_fill": "rgba(220, 38, 38, 0.18)",
        "dn_line": "#16a34a",
        "dn_fill": "rgba(22, 163, 74, 0.18)",
        "vol_up": "rgba(220, 38, 38, 0.35)",
        "vol_dn": "rgba(22, 163, 74, 0.35)",
        "ema": ("#ca8a04", "#1d4ed8", "#7c3aed", "#64748b"),
        "vwap": "#0f766e",
        "vwap_band": "rgba(15, 118, 110, 0.45)",
        "vwap_fill": "rgba(15, 118, 110, 0.06)",
        "rsi": "#4f46e5",
        "rsi_ma": "#334155",
        "rsi_orange": "#c2410c",
        "rsi_ob": "#dc2626",
        "rsi_mid": "#2563eb",
        "ref_line": "rgba(100, 116, 139, 0.5)",
        "atr_upper": "rgba(109, 40, 217, 0.7)",
        "atr_lower": "rgba(109, 40, 217, 0.45)",
        "macd_line": "#1d4ed8",
        "macd_sig": "#d97706",
        "macd_pos": "rgba(29, 78, 216, 0.4)",
        "macd_neg": "rgba(220, 38, 38, 0.35)",
        "vp_bar": "rgba(79, 70, 229, 0.35)",
        "legend_bg": "rgba(250, 250, 250, 0.9)",
        "hover_bg": "rgba(255, 255, 255, 0.98)",
        "hover_border": "rgba(71, 85, 105, 0.3)",
        "spike": "rgba(71, 85, 105, 0.22)",
    },
}

CHART_THEME_OPTIONS: tuple[str, ...] = tuple(CHART_THEMES.keys())


def configure_market_provider(provider: str | None = None) -> str:
    """配置 K 线数据源；默认腾讯优先，东财/yfinance 作为后备。"""
    global _MARKET_DATA_PROVIDER
    p = str(provider or "tencent").strip().lower()
    if p in {"tencent", "qq", "gtimg"}:
        _MARKET_DATA_PROVIDER = "tencent"
    elif p in {"eastmoney", "em", "cn", "china", "mainland"}:
        _MARKET_DATA_PROVIDER = "eastmoney"
    elif p in {"yfinance", "yf", "yahoo", "us"}:
        _MARKET_DATA_PROVIDER = "yfinance"
    elif p in {"futu", "futunn", "opend"}:
        _MARKET_DATA_PROVIDER = "futu"
    else:
        _MARKET_DATA_PROVIDER = "tencent"
    return _MARKET_DATA_PROVIDER


def get_market_provider() -> str:
    return _MARKET_DATA_PROVIDER


configure_market_provider(None)


def get_chart_theme(name: str) -> dict[str, Any]:
    return CHART_THEMES.get(name, CHART_THEMES["Classic Light"])


def configure_market_storage(conf: dict[str, str] | None, *, read_only: bool | None = None) -> None:
    """配置 Supabase 行情存储（None 表示关闭）。"""
    global _SUPABASE_CONF, _SUPABASE_SESSION, _SUPABASE_READ_ONLY
    _SUPABASE_CONF = conf if conf and conf.get("url") and conf.get("key") else None
    if read_only is not None:
        _SUPABASE_READ_ONLY = bool(read_only)
    _SUPABASE_SESSION = None
    if _SUPABASE_CONF:
        s = requests.Session()
        s.headers.update(
            {
                "apikey": _SUPABASE_CONF["key"],
                "Authorization": f"Bearer {_SUPABASE_CONF['key']}",
            }
        )
        _SUPABASE_SESSION = s


def _candlestick_kwargs(theme: dict[str, Any]) -> dict[str, Any]:
    return {
        "increasing": dict(
            line=dict(color=theme["up_line"], width=0.85),
            fillcolor=theme["up_line"],
        ),
        "decreasing": dict(
            line=dict(color=theme["dn_line"], width=0.85),
            fillcolor=theme["dn_line"],
        ),
        "whiskerwidth": 0.38,
    }


def _extended_candlestick_kwargs(theme: dict[str, Any]) -> dict[str, Any]:
    return {
        "increasing": dict(
            line=dict(color=theme["up_line"], width=1.1),
            fillcolor="rgba(0, 0, 0, 0)",
        ),
        "decreasing": dict(
            line=dict(color=theme["dn_line"], width=1.1),
            fillcolor="rgba(0, 0, 0, 0)",
        ),
        "whiskerwidth": 0.38,
    }


def _regular_us_session_mask(index: pd.Index) -> np.ndarray:
    idx = pd.DatetimeIndex(index)
    if idx.tz is None:
        idx = idx.tz_localize(ZoneInfo("America/New_York"), ambiguous="infer", nonexistent="shift_forward")
    else:
        idx = idx.tz_convert(ZoneInfo("America/New_York"))
    minutes = idx.hour * 60 + idx.minute
    return np.asarray((minutes >= 9 * 60 + 30) & (minutes < 16 * 60))


def _candlestick_customdata(df: pd.DataFrame) -> np.ndarray:
    open_values = pd.to_numeric(df["Open"], errors="coerce")
    high_values = pd.to_numeric(df["High"], errors="coerce")
    low_values = pd.to_numeric(df["Low"], errors="coerce")
    close_values = pd.to_numeric(df["Close"], errors="coerce")
    change = close_values - open_values
    change_pct = (close_values / open_values.replace(0, np.nan) - 1.0) * 100.0
    amplitude_pct = (high_values / low_values.replace(0, np.nan) - 1.0) * 100.0
    return np.column_stack([
        change.to_numpy(dtype=float),
        change_pct.to_numpy(dtype=float),
        amplitude_pct.to_numpy(dtype=float),
    ])


def _candlestick_hovertemplate(label: str = "K") -> str:
    return (
        f"{label}<br>"
        "%{x}<br>"
        "Open: %{open:.2f}<br>"
        "High: %{high:.2f}<br>"
        "Low: %{low:.2f}<br>"
        "Close: %{close:.2f}<br>"
        "Change: %{customdata[0]:+.2f} (%{customdata[1]:+.2f}%)<br>"
        "Range: %{customdata[2]:.2f}%"
        "<extra></extra>"
    )


def _add_intraday_candlesticks(
    fig: go.Figure,
    df: pd.DataFrame,
    theme: dict[str, Any],
    regular_mask: np.ndarray,
    show_extended: bool = True,
    row: int = 1,
    col: int = 1,
    show_hover: bool = True,
) -> None:
    regular = df.loc[regular_mask]
    extended = df.loc[~regular_mask]
    regular_customdata = _candlestick_customdata(regular) if show_hover else None
    regular_hovertemplate = _candlestick_hovertemplate("Regular") if show_hover else None
    extended_customdata = _candlestick_customdata(extended) if show_hover else None
    extended_hovertemplate = _candlestick_hovertemplate("Extended") if show_hover else None
    if not regular.empty:
        fig.add_trace(
            go.Candlestick(
                x=regular.index,
                open=regular["Open"],
                high=regular["High"],
                low=regular["Low"],
                close=regular["Close"],
                customdata=regular_customdata,
                hovertemplate=regular_hovertemplate,
                name="K线",
                **_candlestick_kwargs(theme),
            ),
            row=row,
            col=col,
        )
    if show_extended and not extended.empty:
        fig.add_trace(
            go.Candlestick(
                x=extended.index,
                open=extended["Open"],
                high=extended["High"],
                low=extended["Low"],
                close=extended["Close"],
                customdata=extended_customdata,
                hovertemplate=extended_hovertemplate,
                name="扩展盘",
                **_extended_candlestick_kwargs(theme),
            ),
            row=row,
            col=col,
        )


def _apply_chart_theme(fig: go.Figure, theme: dict[str, Any]) -> None:
    right_margin = max(18, int(fig.layout.margin.r or 0))
    fig.update_layout(
        paper_bgcolor=theme["paper"],
        plot_bgcolor=theme["plot"],
        font=dict(family=_CH_FONT_FAMILY, size=11, color=theme["muted"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.006,
            xanchor="right",
            x=1,
            bgcolor=theme["legend_bg"],
            borderwidth=0,
            font=dict(size=10, color=theme["muted"]),
            itemsizing="constant",
            itemwidth=30,
        ),
        hovermode="closest",
        hoverlabel=dict(
            bgcolor=theme["hover_bg"],
            bordercolor=theme["hover_border"],
            font_size=11,
            font_family=_CH_FONT_FAMILY,
        ),
        margin=dict(l=52, r=right_margin, t=18, b=44),
        dragmode="zoom",
    )
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid"],
        zeroline=False,
        showline=False,
        tickfont=dict(size=10, color=theme["muted"]),
        showspikes=True,
        spikecolor=theme["spike"],
        spikemode="across",
        spikesnap="cursor",
        spikedash="solid",
        fixedrange=False,
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid"],
        zeroline=False,
        showline=False,
        tickfont=dict(size=10, color=theme["muted"]),
        fixedrange=False,
    )


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.droplevel(1)
    colmap = {c: c.capitalize() for c in out.columns if isinstance(c, str)}
    out = out.rename(columns=colmap)
    for need in ("Open", "High", "Low", "Close", "Volume"):
        if need not in out.columns:
            return pd.DataFrame()
    return out


def _eastmoney_secid_is_cn(secid: str) -> bool:
    return secid.startswith("0.") or secid.startswith("1.")


def _eastmoney_secid(yahoo_symbol: str) -> str | None:
    if yahoo_symbol in _EASTMONEY_US_SECID:
        return _EASTMONEY_US_SECID[yahoo_symbol]
    if yahoo_symbol.endswith(".SS"):
        return "1." + yahoo_symbol[:-3]
    if yahoo_symbol.endswith(".SZ"):
        return "0." + yahoo_symbol[:-3]
    return None


def _adjust_eastmoney_us_index(
    df: pd.DataFrame,
    interval: Literal["1d", "15m", "5m"],
) -> pd.DataFrame:
    """美股：分钟线为北京时间 → 转美东；日线仅为交易日历日，按美东日期对齐（避免 naive 午夜误当北京时间）。"""
    if df.empty:
        return df
    out = df.copy()
    if interval == "1d":
        new_idx = [
            pd.Timestamp(
                year=t.year,
                month=t.month,
                day=t.day,
                hour=12,
                minute=0,
                tz=ZoneInfo("America/New_York"),
            )
            for t in out.index
        ]
        out.index = pd.DatetimeIndex(new_idx, name="Datetime")
        return out
    if out.index.tz is not None:
        return out
    out.index = (
        out.index.tz_localize(ZoneInfo("Asia/Shanghai"), ambiguous="infer", nonexistent="shift_forward").tz_convert(
            ZoneInfo("America/New_York")
        )
    )
    return out


def _normalize_plot_time_index(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """将索引统一为本地市场时区下的“无时区时间”，避免 Plotly 轴显示偏移。"""
    if df.empty:
        return df
    tz = _market_tz(symbol)
    out = df.copy()
    if out.index.tz is None:
        out.index = out.index.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        out.index = out.index.tz_convert(tz)
    out.index = out.index.tz_localize(None)
    return out


def _fix_intraday_last_bar_volume(df: pd.DataFrame) -> pd.DataFrame:
    """修复部分源最后一根分钟量偶发返回“当日累计量”的异常。"""
    if df.empty:
        return df
    out = df.copy()
    for _, g in out.groupby(out.index.date):
        if len(g) < 4:
            continue
        idx = g.index
        vols = out.loc[idx, "Volume"].astype(float)
        last = float(vols.iloc[-1])
        prev = vols.iloc[:-1]
        prev_sum = float(prev.sum())
        prev_med = float(prev.median()) if len(prev) else 0.0
        # 若最后一根接近“前面总和”，大概率是累计量；还原为增量
        if prev_sum > 0 and last > prev_sum * 0.8:
            delta = last - prev_sum
            replacement = delta if delta > 0 else prev_med
            out.loc[idx[-1], "Volume"] = max(replacement, prev_med if prev_med > 0 else 0.0)
    return out


def _fetch_eastmoney_ohlcv(
    secid: str,
    interval: Literal["1d", "15m", "5m"],
    *,
    lmt: int | None = None,
) -> pd.DataFrame:
    """东方财富 K 线：日期/时间,开,收,高,低,量,额。"""
    klt = {"1d": "101", "15m": "15", "5m": "5"}[interval]
    cap = 1500 if interval == "1d" else 2000
    floor = 30 if interval == "1d" else 80
    n = cap if lmt is None else int(lmt)
    n = max(floor, min(cap, n))
    lmt_s = str(n)
    params = {
        "secid": secid,
        "klt": klt,
        "fqt": "1",
        "lmt": lmt_s,
        "end": "20500101",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57",
    }
    try:
        r = requests.get(
            _EASTMONEY_KLINE_URL,
            params=params,
            headers=_EASTMONEY_HEADERS,
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json().get("data") or {}
        klines = data.get("klines") or []
    except (OSError, ValueError, requests.RequestException):
        return pd.DataFrame()

    rows: list[tuple[pd.Timestamp, float, float, float, float, float]] = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        try:
            ts = pd.to_datetime(parts[0])
            o, c, h, low = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])
            v = float(parts[5])
            rows.append((ts, o, h, low, c, v))
        except (ValueError, TypeError):
            continue
    if not rows:
        return pd.DataFrame()
    idx, o, h, low, c, v = zip(*rows)
    df = pd.DataFrame(
        {"Open": o, "High": h, "Low": low, "Close": c, "Volume": v},
        index=pd.DatetimeIndex(idx, name="Datetime"),
    )
    df = df.sort_index()
    return df[~df.index.duplicated(keep="last")]


def _fetch_tencent_ohlcv(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    *,
    lmt: int | None = None,
) -> pd.DataFrame:
    """腾讯美股 K 线。当前对美股日线覆盖较好，分钟线通常不足，分钟线返回空让后备源处理。"""
    code = _TENCENT_US_KLINE.get(symbol)
    if not code or interval != "1d":
        return pd.DataFrame()
    n = 1000 if lmt is None else int(lmt)
    n = max(80, min(1000, n))
    try:
        r = requests.get(
            _TENCENT_KLINE_URL,
            params={"param": f"{code},day,,,{n},qfq"},
            headers=_TENCENT_HEADERS,
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json().get("data") or {}
        rows_raw = ((data.get(code) or {}).get("day") or []) if isinstance(data, dict) else []
    except (OSError, ValueError, requests.RequestException):
        return pd.DataFrame()

    rows: list[tuple[pd.Timestamp, float, float, float, float, float]] = []
    for row in rows_raw:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts = pd.to_datetime(row[0])
            o, c, h, low = float(row[1]), float(row[2]), float(row[3]), float(row[4])
            v = float(row[5])
            rows.append((ts, o, h, low, c, v))
        except (ValueError, TypeError):
            continue
    if not rows:
        return pd.DataFrame()
    idx, o, h, low, c, v = zip(*rows)
    new_york_noon = [
        pd.Timestamp(
            year=t.year,
            month=t.month,
            day=t.day,
            hour=12,
            minute=0,
            tz=ZoneInfo("America/New_York"),
        )
        for t in idx
    ]
    df = pd.DataFrame(
        {"Open": o, "High": h, "Low": low, "Close": c, "Volume": v},
        index=pd.DatetimeIndex(new_york_noon, name="Datetime"),
    )
    df = df.sort_index()
    return df[~df.index.duplicated(keep="last")]


def _fetch_yfinance_ohlcv(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    period: str,
) -> pd.DataFrame:
    import yfinance as yf

    ylog = logging.getLogger("yfinance")
    prev = ylog.level
    ylog.setLevel(logging.ERROR)
    last_exc: BaseException | None = None
    try:
        for attempt in range(_YF_RETRIES):
            _yf_pace()
            try:
                df = _yf_ticker_history(
                    yf,
                    symbol,
                    period=period,
                    interval=interval,
                    auto_adjust=True,
                )
            except BaseException as e:
                last_exc = e
                df = pd.DataFrame()
                if _is_yf_rate_limited(e) and attempt < _YF_RETRIES - 1:
                    wait = min(120.0, 45.0 + 35.0 * attempt)
                    logging.getLogger("chart_boards").warning(
                        "yfinance rate limited %s %s, retry after %.0fs", symbol, interval, wait
                    )
                    time.sleep(wait)
                    continue
            df = _normalize_ohlcv(df)
            if not df.empty:
                return df.dropna(how="all")
            if attempt < _YF_RETRIES - 1:
                time.sleep(1.5 * (attempt + 1))
    finally:
        ylog.setLevel(prev)
    if last_exc is not None:
        ylog.warning("yfinance %s %s: %s", symbol, interval, last_exc)
    return pd.DataFrame()


def _fetch_yfinance_ohlcv_from(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    start_naive_local: pd.Timestamp,
) -> pd.DataFrame:
    """从 start 附近起拉取（Supabase 增量用，避免反复下载整段 period）。"""
    import yfinance as yf

    tz = _market_tz(symbol)
    st = start_naive_local
    if st.tzinfo is None:
        st = st.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        st = st.tz_convert(tz)
    start_day = (st - pd.Timedelta(days=1)).date()

    ylog = logging.getLogger("yfinance")
    prev = ylog.level
    ylog.setLevel(logging.ERROR)
    last_exc: BaseException | None = None
    try:
        for attempt in range(_YF_RETRIES):
            _yf_pace()
            try:
                df = _yf_ticker_history(
                    yf,
                    symbol,
                    start=start_day,
                    interval=interval,
                    auto_adjust=True,
                )
            except BaseException as e:
                last_exc = e
                df = pd.DataFrame()
                if _is_yf_rate_limited(e) and attempt < _YF_RETRIES - 1:
                    wait = min(120.0, 45.0 + 35.0 * attempt)
                    logging.getLogger("chart_boards").warning(
                        "yfinance rate limited %s %s from %s, retry after %.0fs",
                        symbol,
                        interval,
                        start_day,
                        wait,
                    )
                    time.sleep(wait)
                    continue
            df = _normalize_ohlcv(df)
            if not df.empty:
                return df.dropna(how="all")
            if attempt < _YF_RETRIES - 1:
                time.sleep(1.5 * (attempt + 1))
    finally:
        ylog.setLevel(prev)
    if last_exc is not None:
        ylog.warning("yfinance %s %s from %s: %s", symbol, interval, start_day, last_exc)
    return pd.DataFrame()


def _period_for_incremental(interval: Literal["1d", "15m", "5m"]) -> str:
    return "60d" if interval == "1d" else "2d"


def _trim_df_for_storage(symbol: str, interval: Literal["1d", "15m", "5m"], df: pd.DataFrame) -> pd.DataFrame:
    """写入 Supabase 前裁剪到保留期，避免表无限膨胀。"""
    if df.empty:
        return df
    days = int(_SUPABASE_RETENTION_DAYS.get(interval, 0) or 0)
    if days <= 0:
        return df
    tz = _market_tz(symbol)
    cutoff = pd.Timestamp.now(tz=tz) - pd.Timedelta(days=days)
    idx = df.index
    try:
        if idx.tz is None:
            idx_local = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
        else:
            idx_local = idx.tz_convert(tz)
        mask = idx_local >= cutoff
        out = df.loc[mask].copy()
        return out if not out.empty else df.tail(1)
    except Exception:
        return df


def _eastmoney_incremental_lmt(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    cached_max_naive: pd.Timestamp,
) -> int:
    """据库里最后一根 K 估算东财 lmt，增量时少拉历史条数。"""
    tz = _market_tz(symbol)
    try:
        cm = cached_max_naive
        if cm.tzinfo is None:
            cm_local = cm.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
        else:
            cm_local = cm.tz_convert(tz)
        now = pd.Timestamp.now(tz=tz)
        delta_td = now - cm_local
        if interval == "1d":
            days = max(5, int(delta_td.total_seconds() // 86400) + 14)
            return min(1500, days + 30)
        mins = max(180, int(delta_td.total_seconds() // 60) + 360)
        if interval == "15m":
            n = mins // 15 + 100
        else:
            n = mins // 5 + 150
        return min(2000, n)
    except Exception:
        return 1500 if interval == "1d" else 2000


def _delta_ohlcv_vs_cache(latest: pd.DataFrame, cached: pd.DataFrame) -> pd.DataFrame:
    """相对库里最后一根时间戳：只保留更新/新增 K 线（用于增量 upsert）。"""
    if latest.empty or cached.empty:
        return latest
    mx = cached.index.max()
    newer = latest.loc[latest.index > mx]
    tail_update = latest.loc[latest.index == mx].tail(1)
    parts = [newer]
    if not tail_update.empty:
        parts.append(tail_update)
    out = pd.concat(parts)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def _merge_ohlcv_cached_delta(cached: pd.DataFrame, delta: pd.DataFrame) -> pd.DataFrame:
    """增量写入后合并到内存，避免再 GET 全表。"""
    if delta.empty:
        return cached
    if cached.empty:
        return delta.sort_index()
    out = pd.concat([cached, delta])
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def _yfinance_incremental_start(
    symbol: str,
    cached_max_naive: pd.Timestamp,
    interval: Literal["1d", "15m", "5m"],
) -> pd.Timestamp:
    tz = _market_tz(symbol)
    mx = cached_max_naive
    if mx.tzinfo is None:
        mloc = mx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        mloc = mx.tz_convert(tz)
    pad = pd.Timedelta(days=7) if interval == "1d" else pd.Timedelta(hours=18)
    return (mloc - pad).tz_localize(None)


def _fetch_from_source(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    fetch_period: str,
    *,
    eastmoney_lmt: int | None = None,
    yfinance_start_naive_local: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, str]:
    provider = get_market_provider()
    cache_key = (provider, symbol, interval)
    cached = _SOURCE_CACHE.get(cache_key)
    if provider != "futu" and cached and time.time() - cached[2] < _SOURCE_CACHE_TTL_SECONDS.get(interval, 30.0):
        return cached[0].copy(), cached[1]

    def _store_source(df: pd.DataFrame, source: str) -> tuple[pd.DataFrame, str]:
        if not df.empty:
            _SOURCE_CACHE[cache_key] = (df.copy(), source, time.time())
        return df, source

    def _fetch_tencent_or_eastmoney(fallback_provider: str) -> tuple[pd.DataFrame, str]:
        d = _fetch_tencent_ohlcv(symbol, interval, lmt=eastmoney_lmt)
        if not d.empty:
            if interval != "1d":
                d = _fix_intraday_last_bar_volume(d)
            return _store_source(_normalize_plot_time_index(d, symbol), "tencent")
        secid_inner = _eastmoney_secid(symbol)
        if secid_inner is not None:
            d = _fetch_eastmoney_ohlcv(secid_inner, interval, lmt=eastmoney_lmt)
            if d.empty:
                time.sleep(1.2)
                d = _fetch_eastmoney_ohlcv(secid_inner, interval, lmt=eastmoney_lmt)
            if not d.empty:
                if not _eastmoney_secid_is_cn(secid_inner):
                    d = _adjust_eastmoney_us_index(d, interval)
                if interval != "1d":
                    d = _fix_intraday_last_bar_volume(d)
                return _store_source(_normalize_plot_time_index(d, symbol), "eastmoney")
        return pd.DataFrame(), fallback_provider

    if provider == "futu":
        futu_df = pd.DataFrame()
        futu_reason = "futu_empty"
        try:
            from backend.ohlcv import _fetch_futu_ohlcv

            bars, futu_reason = _fetch_futu_ohlcv(symbol, interval)
            if bars:
                futu_df = pd.DataFrame(bars).rename(
                    columns={
                        "open": "Open",
                        "high": "High",
                        "low": "Low",
                        "close": "Close",
                        "volume": "Volume",
                    }
                )
                if interval == "1d":
                    futu_df.index = pd.to_datetime(futu_df.pop("time"), errors="coerce")
                else:
                    futu_df.index = pd.to_datetime(futu_df.pop("time"), unit="s", utc=True, errors="coerce")
                futu_df = futu_df.loc[~futu_df.index.isna(), ["Open", "High", "Low", "Close", "Volume"]]
                if not futu_df.empty:
                    if interval != "1d":
                        futu_df = _fix_intraday_last_bar_volume(futu_df)
                    min_bars = _MIN_FUTU_INTRADAY_HISTORY_BARS.get(interval, 1)
                    if interval == "1d" or len(futu_df) >= min_bars:
                        return _store_source(_normalize_plot_time_index(futu_df, symbol), "futu")
                    futu_reason = f"futu_history_short_{len(futu_df)}"
        except Exception:
            futu_reason = "futu_failed"
        fallback_df, fallback_source = _fetch_tencent_or_eastmoney(futu_reason)
        if not fallback_df.empty:
            return fallback_df, fallback_source
        if not futu_df.empty:
            return _store_source(_normalize_plot_time_index(futu_df, symbol), futu_reason)
        return fallback_df, fallback_source

    if provider == "tencent":
        return _fetch_tencent_or_eastmoney("tencent")

    secid = _eastmoney_secid(symbol) if provider in {"tencent", "eastmoney"} else None
    if secid is not None:
        d = _fetch_eastmoney_ohlcv(secid, interval, lmt=eastmoney_lmt)
        if d.empty:
            time.sleep(1.2)
            d = _fetch_eastmoney_ohlcv(secid, interval, lmt=eastmoney_lmt)
        if not d.empty:
            if not _eastmoney_secid_is_cn(secid):
                d = _adjust_eastmoney_us_index(d, interval)
            if interval != "1d":
                d = _fix_intraday_last_bar_volume(d)
            return _normalize_plot_time_index(d, symbol), "eastmoney"
    if provider != "yfinance":
        return pd.DataFrame(), provider
    if yfinance_start_naive_local is not None:
        d = _fetch_yfinance_ohlcv_from(symbol, interval, yfinance_start_naive_local)
    else:
        d = _fetch_yfinance_ohlcv(symbol, interval, fetch_period)
    if interval != "1d":
        d = _fix_intraday_last_bar_volume(d)
    return _normalize_plot_time_index(d, symbol), "yfinance"


def _supabase_headers(content_type: bool = False) -> dict[str, str]:
    if not _SUPABASE_CONF:
        return {}
    h = {
        "apikey": _SUPABASE_CONF["key"],
        "Authorization": f"Bearer {_SUPABASE_CONF['key']}",
    }
    if content_type:
        h["Content-Type"] = "application/json"
        h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return h


def _load_bars_from_supabase(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    *,
    since_utc: pd.Timestamp | None = None,
    limit: int = 10000,
) -> pd.DataFrame:
    if not _SUPABASE_CONF:
        return pd.DataFrame()
    url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars"
    params = {
        "select": "ts,open,high,low,close,volume",
        "symbol": f"eq.{symbol}",
        "interval": f"eq.{interval}",
        "order": "ts.desc" if since_utc is not None else "ts.asc",
        "limit": str(max(1, int(limit))),
    }
    if since_utc is not None:
        try:
            ts = pd.Timestamp(since_utc)
            if ts.tz is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            params["ts"] = f"gte.{ts.isoformat()}"
        except Exception:
            pass
    try:
        if _SUPABASE_SESSION is not None:
            r = _SUPABASE_SESSION.get(url, params=params, timeout=_HTTP_TIMEOUT)
        else:
            r = requests.get(url, params=params, headers=_supabase_headers(), timeout=_HTTP_TIMEOUT)
        if r.status_code >= 400:
            try:
                detail = (r.text or "").strip()
            except Exception:
                detail = ""
            logging.getLogger("supabase").warning(
                "load market_bars failed: status=%s symbol=%s interval=%s detail=%s",
                r.status_code,
                symbol,
                interval,
                detail[:500],
            )
            return pd.DataFrame()
        rows = r.json()
    except (requests.RequestException, ValueError, TypeError):
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    def _pick_col(frame: pd.DataFrame, name: str) -> str | None:
        if name in frame.columns:
            return name
        target = name.lower()
        for c in frame.columns:
            if str(c).lower() == target:
                return str(c)
        return None

    try:
        col_ts = _pick_col(df, "ts")
        col_open = _pick_col(df, "open")
        col_high = _pick_col(df, "high")
        col_low = _pick_col(df, "low")
        col_close = _pick_col(df, "close")
        col_volume = _pick_col(df, "volume")
        if not col_ts or not col_open or not col_high or not col_low or not col_close:
            return pd.DataFrame()

        ts = pd.to_datetime(df[col_ts], utc=True, errors="coerce")
        base = pd.DataFrame(
            {
                "Open": pd.to_numeric(df[col_open], errors="coerce"),
                "High": pd.to_numeric(df[col_high], errors="coerce"),
                "Low": pd.to_numeric(df[col_low], errors="coerce"),
                "Close": pd.to_numeric(df[col_close], errors="coerce"),
                "Volume": pd.to_numeric(df[col_volume], errors="coerce") if col_volume else 0.0,
            }
        )
        base["ts"] = ts
        base = base.dropna(subset=["ts", "Open", "High", "Low", "Close"])
        if base.empty:
            return pd.DataFrame()

        out = pd.DataFrame(
            {
                "Open": base["Open"].to_numpy(),
                "High": base["High"].to_numpy(),
                "Low": base["Low"].to_numpy(),
                "Close": base["Close"].to_numpy(),
                "Volume": base["Volume"].to_numpy(),
            },
            index=pd.DatetimeIndex(base["ts"].to_numpy(), name="Datetime"),
        ).sort_index()
        out = out[~out.index.duplicated(keep="last")]
        try:
            return _normalize_plot_time_index(out, symbol)
        except Exception:
            # 解析成功时宁可返回原始时区索引，也不要误报“无数据”
            return out
    except (TypeError, ValueError, KeyError):
        return pd.DataFrame()


def probe_recent_market_rows(limit: int = 20) -> list[dict[str, Any]]:
    """返回 market_bars 最近若干条 (symbol, interval, ts) 供前端诊断显示。"""
    if not _SUPABASE_CONF:
        return []
    url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars"
    params = {
        "select": "symbol,interval,ts",
        "order": "ts.desc",
        "limit": str(max(1, int(limit))),
    }
    try:
        if _SUPABASE_SESSION is not None:
            r = _SUPABASE_SESSION.get(url, params=params, timeout=_HTTP_TIMEOUT)
        else:
            r = requests.get(url, params=params, headers=_supabase_headers(), timeout=_HTTP_TIMEOUT)
        if r.status_code >= 400:
            return []
        rows = r.json()
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def probe_market_inventory(limit: int = 1000) -> list[dict[str, Any]]:
    """返回 market_bars 在给定窗口内的 (symbol, interval, rows, latest_ts) 概览。"""
    if not _SUPABASE_CONF:
        return []
    url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars"
    params = {
        "select": "symbol,interval,ts",
        "order": "ts.desc",
        "limit": str(max(1, int(limit))),
    }
    try:
        if _SUPABASE_SESSION is not None:
            r = _SUPABASE_SESSION.get(url, params=params, timeout=_HTTP_TIMEOUT)
        else:
            r = requests.get(url, params=params, headers=_supabase_headers(), timeout=_HTTP_TIMEOUT)
        if r.status_code >= 400:
            return []
        rows = r.json()
        if not isinstance(rows, list) or not rows:
            return []
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for x in rows:
            sym = str(x.get("symbol", "") or "")
            iv = str(x.get("interval", "") or "")
            ts = str(x.get("ts", "") or "")
            if not sym or not iv:
                continue
            k = (sym, iv)
            if k not in out:
                out[k] = {"symbol": sym, "interval": iv, "rows": 0, "latest_ts": ts}
            out[k]["rows"] = int(out[k]["rows"]) + 1
            if ts and (not out[k]["latest_ts"] or ts > str(out[k]["latest_ts"])):
                out[k]["latest_ts"] = ts
        return sorted(out.values(), key=lambda i: (i["symbol"], i["interval"]))
    except Exception:
        return []


def probe_symbol_interval_raw_rows(
    symbol: str,
    intervals: list[Literal["1d", "15m", "5m"]],
) -> dict[str, int]:
    """直接用 REST 原始返回统计当前 symbol 各周期行数（不经过 DataFrame 解析）。"""
    out: dict[str, int] = {k: 0 for k in intervals}
    if not _SUPABASE_CONF:
        return out
    url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars"
    for iv in intervals:
        params = {
            "select": "ts",
            "symbol": f"eq.{symbol}",
            "interval": f"eq.{iv}",
            "order": "ts.desc",
            "limit": "200",
        }
        try:
            if _SUPABASE_SESSION is not None:
                r = _SUPABASE_SESSION.get(url, params=params, timeout=_HTTP_TIMEOUT)
            else:
                r = requests.get(url, params=params, headers=_supabase_headers(), timeout=_HTTP_TIMEOUT)
            if r.status_code >= 400:
                out[iv] = -1
                continue
            rows = r.json()
            out[iv] = len(rows) if isinstance(rows, list) else 0
        except Exception:
            out[iv] = -1
    return out


def probe_market_cache_status(
    symbol: str,
    intervals: list[Literal["1d", "15m", "5m"]],
) -> dict[str, Any]:
    """探测 Supabase 连通性与各周期有效缓存情况（按可解析 OHLC 行判断）。"""
    out: dict[str, Any] = {
        "enabled": bool(_SUPABASE_CONF),
        "reachable": False,
        "error": "",
        "hits": {k: False for k in intervals},
        "rows": {k: 0 for k in intervals},
        "latest_ts": {k: "" for k in intervals},
    }
    if not _SUPABASE_CONF:
        return out
    # 先做一次最小化连通/权限探测，避免把 401/403 误报成“空数据”。
    try:
        ping_url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars"
        ping_params = {"select": "symbol", "limit": "1"}
        if _SUPABASE_SESSION is not None:
            ping = _SUPABASE_SESSION.get(ping_url, params=ping_params, timeout=_HTTP_TIMEOUT)
        else:
            ping = requests.get(ping_url, params=ping_params, headers=_supabase_headers(), timeout=_HTTP_TIMEOUT)
        if ping.status_code >= 400:
            detail = ""
            try:
                detail = (ping.text or "").strip()
            except Exception:
                detail = ""
            out["error"] = f"http {ping.status_code}: {detail[:220] or 'request failed'}"
            return out
    except Exception as e:
        out["error"] = str(e)
        return out
    try:
        for iv in intervals:
            # 用统一解析逻辑判断“是否有可绘图的有效 K 线”
            df = _load_bars_from_supabase(symbol, iv, since_utc=None, limit=200)
            n = int(len(df)) if not df.empty else 0
            out["rows"][iv] = n
            out["hits"][iv] = n > 0
            if n > 0:
                try:
                    out["latest_ts"][iv] = str(df.index.max())
                except Exception:
                    out["latest_ts"][iv] = ""
        out["reachable"] = True
    except Exception as e:
        out["error"] = str(e)
    return out


def _bars_payload(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    df: pd.DataFrame,
    source: str,
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    tz = _market_tz(symbol)
    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        idx = idx.tz_convert(tz)
    idx_utc = idx.tz_convert("UTC")
    out: list[dict[str, Any]] = []
    for ts, row in zip(idx_utc, df.itertuples()):
        out.append(
            {
                "symbol": symbol,
                "interval": interval,
                "ts": pd.Timestamp(ts).isoformat(),
                "open": float(row.Open),
                "high": float(row.High),
                "low": float(row.Low),
                "close": float(row.Close),
                "volume": float(getattr(row, "Volume", 0.0) or 0.0),
                "source": source,
            }
        )
    return out


def _upsert_bars_to_supabase(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    df: pd.DataFrame,
    source: str,
) -> int:
    if not _SUPABASE_CONF or df.empty:
        return 0
    df_trim = _trim_df_for_storage(symbol, interval, df)
    payload = _bars_payload(symbol, interval, df_trim, source)
    if not payload:
        return 0
    url = f"{_SUPABASE_CONF['url']}/rest/v1/market_bars?on_conflict=symbol,interval,ts"
    try:
        if _SUPABASE_SESSION is not None:
            r = _SUPABASE_SESSION.post(
                url,
                headers=_supabase_headers(content_type=True),
                json=payload,
                timeout=_HTTP_TIMEOUT,
            )
        else:
            r = requests.post(
                url,
                headers=_supabase_headers(content_type=True),
                json=payload,
                timeout=_HTTP_TIMEOUT,
            )
        if r.status_code >= 400:
            # 这里不直接抛异常：界面仍可展示本地拉取的图表；但必须把原因写进日志
            # 以便确认是 service_role key / RLS / 项目 URL 是否配置正确。
            try:
                detail = (r.text or "").strip()
            except Exception:
                detail = ""
            logging.getLogger("supabase").warning(
                "upsert market_bars failed: status=%s detail=%s",
                r.status_code,
                detail[:800],
            )
            return 0
        return len(payload)
    except requests.RequestException:
        return 0


def _market_tz(symbol: str) -> ZoneInfo:
    if symbol.endswith(".SS") or symbol.endswith(".SZ"):
        return ZoneInfo("Asia/Shanghai")
    return ZoneInfo("America/New_York")


def slice_intraday_today_or_yesterday(
    df: pd.DataFrame,
    symbol: str,
    *,
    min_current_bars: int = 1,
    include_previous_context: bool = False,
) -> tuple[pd.DataFrame, str]:
    """分钟 K 只保留：本地市场「当日」有 bar 则当日；否则「昨日」；再无则数据内最近交易日。"""
    if df.empty:
        return df, ""

    tz = _market_tz(symbol)
    idx = df.index
    if idx.tz is None:
        idx_local = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        idx_local = idx.tz_convert(tz)

    today = pd.Timestamp.now(tz=tz).date()
    yesterday = today - timedelta(days=1)
    bar_dates = pd.Series(idx_local.date, index=df.index)

    if (bar_dates == today).any():
        picked = today
        note = f"{today} 当日"
    elif (bar_dates == yesterday).any():
        picked = yesterday
        note = f"{yesterday} 昨日（本地无当日数据）"
    else:
        picked = bar_dates.max()
        note = f"{picked} 最近交易日"

    mask = bar_dates == picked
    if include_previous_context and int(mask.sum()) < max(1, min_current_bars):
        dates = sorted(pd.unique(bar_dates))
        if picked in dates:
            picked_pos = dates.index(picked)
            if picked_pos > 0:
                previous = dates[picked_pos - 1]
                mask = bar_dates.isin([previous, picked])
                note += f" + {previous} context"
    out = df.loc[mask].copy()
    out.index = idx_local[mask]
    return out, note


def slice_regular_intraday_with_context(
    df: pd.DataFrame,
    symbol: str,
    *,
    min_current_bars: int = 6,
    include_previous_context: bool = False,
) -> tuple[pd.DataFrame, str]:
    """Regular-session intraday view.

    Pick the latest market date from all intraday bars first, then show only
    that date's regular-session bars. This prevents premarket-only current-day
    data from falling back to the previous regular trading day when extended
    hours are hidden.
    """
    if df.empty:
        return df, ""
    tz = _market_tz(symbol)
    idx = df.index
    if idx.tz is None:
        idx_local = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        idx_local = idx.tz_convert(tz)
    all_bar_dates = pd.Series(idx_local.date, index=df.index)
    latest_data_date = all_bar_dates.max()

    regular_mask = _regular_us_session_mask(idx)
    regular = df.loc[regular_mask].copy()
    if regular.empty:
        out = df.iloc[0:0].copy()
        out.index = pd.DatetimeIndex([], name=df.index.name, tz=tz)
        return out, f"{latest_data_date} regular session not open"

    regular_idx_local = pd.DatetimeIndex(idx_local[regular_mask])
    bar_dates = pd.Series(regular_idx_local.date, index=regular.index)
    latest_mask = bar_dates == latest_data_date
    if latest_mask.any():
        out = regular.loc[latest_mask].copy()
        out.index = regular_idx_local[latest_mask.to_numpy()]
        latest_count = int(latest_mask.sum())
        selected_dates = [latest_data_date]
        if include_previous_context and latest_count < max(1, min_current_bars):
            dates = sorted(pd.unique(bar_dates))
            latest_pos = dates.index(latest_data_date)
            if latest_pos > 0:
                selected_dates = [dates[latest_pos - 1], latest_data_date]
                context_mask = bar_dates.isin(selected_dates)
                out = regular.loc[context_mask].copy()
                out.index = regular_idx_local[context_mask.to_numpy()]
        note = f"{latest_data_date} regular session"
        if len(selected_dates) > 1:
            note += f" + {selected_dates[0]} open context"
        return out, note

    if not include_previous_context:
        out = regular.iloc[0:0].copy()
        out.index = pd.DatetimeIndex([], name=regular.index.name, tz=tz)
        return out, f"{latest_data_date} regular session not open"

    dates = sorted(pd.unique(bar_dates))
    latest = dates[-1]
    selected_dates = [latest]
    latest_count = int((bar_dates == latest).sum())
    if latest_count < max(1, min_current_bars) and len(dates) > 1:
        selected_dates = [dates[-2], latest]
    mask = bar_dates.isin(selected_dates)
    out = regular.loc[mask].copy()
    out.index = regular_idx_local[mask.to_numpy()]
    note = f"{latest} regular session"
    if len(selected_dates) > 1:
        note += f" + {selected_dates[0]} open context"
    return out, note


def _shanghai_plot_index(index: pd.Index) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(index)
    if idx.tz is None:
        idx = idx.tz_localize(ZoneInfo("America/New_York"), ambiguous="infer", nonexistent="shift_forward")
    return idx.tz_convert(ZoneInfo("Asia/Shanghai")).tz_localize(None)


def _intraday_fixed_x_range(index: pd.Index, symbol: str, show_extended: bool) -> list[pd.Timestamp] | None:
    if len(index) == 0:
        return None
    tz = _market_tz(symbol)
    idx = pd.DatetimeIndex(index)
    if idx.tz is None:
        idx = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        idx = idx.tz_convert(tz)
    dates = sorted(pd.unique(idx.date))
    if not dates:
        return None
    start_date = dates[0]
    end_date = dates[-1]
    if show_extended:
        start_hour, start_minute = 4, 0
        end_hour, end_minute = 20, 0
    else:
        start_hour, start_minute = 9, 30
        end_hour, end_minute = 16, 0
    start = pd.Timestamp(
        year=start_date.year,
        month=start_date.month,
        day=start_date.day,
        hour=start_hour,
        minute=start_minute,
        tz=tz,
    )
    end = pd.Timestamp(
        year=end_date.year,
        month=end_date.month,
        day=end_date.day,
        hour=end_hour,
        minute=end_minute,
        tz=tz,
    )
    return [
        start.tz_convert(ZoneInfo("Asia/Shanghai")).tz_localize(None),
        end.tz_convert(ZoneInfo("Asia/Shanghai")).tz_localize(None),
    ]


def _intraday_xaxis_tick_options(
    fixed_x_range: list[pd.Timestamp] | None,
    show_extended: bool,
) -> dict[str, Any]:
    if not fixed_x_range:
        return {}
    return {
        "tick0": fixed_x_range[0],
        "dtick": 60 * 60 * 1000 if show_extended else 30 * 60 * 1000,
        "tickformat": "%H:%M",
    }


def _intraday_open_base_price(df: pd.DataFrame, symbol: str, show_extended: bool) -> float | None:
    if df.empty:
        return None
    tz = _market_tz(symbol)
    idx = pd.DatetimeIndex(df.index)
    if idx.tz is None:
        idx_local = idx.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    else:
        idx_local = idx.tz_convert(tz)
    dates = sorted(pd.unique(idx_local.date))
    if not dates:
        return None
    target_date = dates[-1]
    target_minutes = 4 * 60 if show_extended else 9 * 60 + 30
    minutes = idx_local.hour * 60 + idx_local.minute
    mask = np.asarray((idx_local.date == target_date) & (minutes >= target_minutes))
    if not mask.any():
        mask = np.asarray(idx_local.date == target_date)
    if not mask.any():
        return None
    base = pd.to_numeric(df.loc[mask, "Open"], errors="coerce").dropna()
    if base.empty:
        return None
    value = float(base.iloc[0])
    return value if np.isfinite(value) and value > 0 else None


def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = pd.DataFrame(
        {
            "Open": df["Open"].resample(rule, label="right", closed="right").first(),
            "High": df["High"].resample(rule, label="right", closed="right").max(),
            "Low": df["Low"].resample(rule, label="right", closed="right").min(),
            "Close": df["Close"].resample(rule, label="right", closed="right").last(),
            "Volume": df["Volume"].resample(rule, label="right", closed="right").sum(),
        }
    )
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def fetch_ohlcv(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    period: str,
    *,
    cache_only: bool = False,
) -> pd.DataFrame:
    def _remember(df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            _OHLCV_MEMORY_CACHE[(symbol, interval)] = df.copy()
        return df

    def _last_good() -> pd.DataFrame:
        cached = _OHLCV_MEMORY_CACHE.get((symbol, interval))
        return cached.copy() if cached is not None and not cached.empty else pd.DataFrame()

    def _since_utc_for_period(p: str) -> pd.Timestamp | None:
        # 根据保留期减少从 Supabase 拉取的无用历史，提升切换标的速度
        try:
            keep_days = int(_SUPABASE_RETENTION_DAYS.get(interval, 0) or 0)
            if keep_days > 0:
                # 多给 1 天缓冲，覆盖时区切换/周末
                return pd.Timestamp.now("UTC") - pd.Timedelta(days=keep_days + 1)
        except Exception:
            pass
        try:
            if isinstance(p, str) and p.endswith("d"):
                days = int(p[:-1])
                # 多给 1 天缓冲，覆盖时区切换/周末
                return pd.Timestamp.now("UTC") - pd.Timedelta(days=max(1, days + 1))
        except Exception:
            return None
        return None

    # 仅读 Supabase、不打外网（用于先快速出图，再由下一轮 rerun 做完整同步）
    if cache_only and _SUPABASE_CONF:
        since_utc = _since_utc_for_period(period)
        lim = _SUPABASE_READ_LIMIT.get(interval, 2500)
        cached = _load_bars_from_supabase(symbol, interval, since_utc=since_utc, limit=lim)
        return _remember(cached) if not cached.empty else _last_good()

    if _SUPABASE_CONF:
        since_utc = _since_utc_for_period(period)
        lim = _SUPABASE_READ_LIMIT.get(interval, 2500)
        cached = _load_bars_from_supabase(symbol, interval, since_utc=since_utc, limit=lim)
        if _SUPABASE_READ_ONLY:
            return _remember(cached) if not cached.empty else _last_good()
        now_s = time.time()
        key = (symbol, interval)
        should_sync = now_s - _LAST_SYNC_AT.get(key, 0.0) >= _SYNC_MIN_SECONDS
        need_seed = cached.empty
        if should_sync or need_seed:
            if need_seed:
                fetch_period = period
                latest, source = _fetch_from_source(symbol, interval, fetch_period)
                to_store = latest
            else:
                fetch_period = _period_for_incremental(interval)
                mx = cached.index.max()
                provider = get_market_provider()
                secid = _eastmoney_secid(symbol) if provider in {"tencent", "eastmoney"} else None
                em_lmt = _eastmoney_incremental_lmt(symbol, interval, mx) if secid else None
                yf_start = _yfinance_incremental_start(symbol, mx, interval) if provider == "yfinance" else None
                latest, source = _fetch_from_source(
                    symbol,
                    interval,
                    fetch_period,
                    eastmoney_lmt=em_lmt,
                    yfinance_start_naive_local=yf_start,
                )
                to_store = _delta_ohlcv_vs_cache(latest, cached)
            if not latest.empty:
                wrote = 0
                if not to_store.empty:
                    wrote = _upsert_bars_to_supabase(symbol, interval, to_store, source)
                if need_seed:
                    cached = _load_bars_from_supabase(symbol, interval, since_utc=since_utc, limit=lim)
                elif wrote > 0:
                    cached = _merge_ohlcv_cached_delta(cached, to_store)
            _LAST_SYNC_AT[key] = now_s
        if not cached.empty:
            return _remember(cached)

    direct, _ = _fetch_from_source(symbol, interval, period)
    return _remember(direct) if not direct.empty else _last_good()


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _volume_bar_colors(df: pd.DataFrame, theme: dict[str, Any]) -> list[str]:
    return [
        theme["vol_up"] if float(c) >= float(o) else theme["vol_dn"]
        for c, o in zip(df["Close"], df["Open"])
    ]


def _with_rgba_alpha(color: str, alpha: float) -> str:
    if color.startswith("rgba(") and color.endswith(")") and "," in color:
        prefix, _, _ = color.rpartition(",")
        return f"{prefix}, {alpha:.2f})"
    return color


def _volume_profile_by_price(
    df: pd.DataFrame,
    bins: int = 24,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    """按价格分箱聚合成交量，返回(箱中位价格, 对应成交量, 下沿, 上沿, 箱高)。

    逻辑：将每根 K 线成交量按其 high-low 区间与价格箱的重叠比例分摊。
    这比把整根 K 线成交量塞进 typical price 单个箱更稳定，尤其适合宽振幅 K 线。
    """
    if df.empty:
        e = pd.Series(dtype=float)
        return e, e, e, e, e
    low = float(df["Low"].min())
    high = float(df["High"].max())
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        e = pd.Series(dtype=float)
        return e, e, e, e, e

    bar_low = pd.to_numeric(df["Low"], errors="coerce").astype(float)
    bar_high = pd.to_numeric(df["High"], errors="coerce").astype(float)
    close = pd.to_numeric(df["Close"], errors="coerce").astype(float)
    vol = df["Volume"].fillna(0.0).astype(float)
    edges = np.linspace(low, high, bins + 1)
    if len(np.unique(edges)) < 2:
        e = pd.Series(dtype=float)
        return e, e, e, e, e

    by_bin_values = np.zeros(len(edges) - 1, dtype=float)
    for lo, hi, cls, amount in zip(bar_low, bar_high, close, vol):
        if not (np.isfinite(lo) and np.isfinite(hi) and np.isfinite(amount)) or amount <= 0:
            continue
        lo, hi = min(float(lo), float(hi)), max(float(lo), float(hi))
        if hi > lo:
            overlaps = np.maximum(0.0, np.minimum(edges[1:], hi) - np.maximum(edges[:-1], lo))
            overlap_sum = float(overlaps.sum())
            if overlap_sum > 0:
                by_bin_values += amount * overlaps / overlap_sum
                continue

        # 无实体波动或极端数据时，退化为按收盘价所在箱归集。
        ref_price = float(cls) if np.isfinite(cls) else lo
        idx = int(np.searchsorted(edges, ref_price, side="right") - 1)
        idx = max(0, min(len(by_bin_values) - 1, idx))
        by_bin_values[idx] += amount

    intervals = pd.IntervalIndex.from_breaks(edges, closed="right")
    by_bin = pd.Series(by_bin_values, index=intervals).astype(float)
    by_bin = by_bin[by_bin > 0]
    if by_bin.empty:
        e = pd.Series(dtype=float)
        return e, e, e, e, e

    lows = by_bin.index.map(lambda itv: float(itv.left))
    highs = by_bin.index.map(lambda itv: float(itv.right))
    mids = by_bin.index.map(lambda itv: float((itv.left + itv.right) / 2.0))
    widths = [max(float(hi) - float(lo), 1e-9) for lo, hi in zip(lows, highs)]
    return (
        pd.Series(mids, index=by_bin.index),
        by_bin.astype(float),
        pd.Series(lows, index=by_bin.index),
        pd.Series(highs, index=by_bin.index),
        pd.Series(widths, index=by_bin.index),
    )


def atr_series(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["High"], df["Low"], df["Close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def macd_series(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    line = ema_f - ema_s
    sig = line.ewm(span=signal, adjust=False).mean()
    hist = line - sig
    return line, sig, hist


def _macd_yaxis_range(
    m_line: pd.Series,
    m_sig: pd.Series,
    m_hist: pd.Series,
    *,
    pad_ratio: float = 0.12,
    q_low: float = 0.05,
    q_high: float = 0.95,
) -> list[float]:
    """自适应 MACD 子图纵轴范围（综合线/信号/柱），包含 0 并加留白。

    使用分位数裁剪避免极端值把范围撑太大。
    """
    vals = pd.concat([m_line, m_sig, m_hist], axis=0)
    vals = vals.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    if vals.empty:
        return [-1.0, 1.0]
    lo = float(vals.quantile(q_low))
    hi = float(vals.quantile(q_high))
    lo = min(lo, 0.0)
    hi = max(hi, 0.0)
    span = hi - lo
    if span <= 0:
        pad = max(abs(hi) * 0.2, 1.0)
    else:
        pad = span * pad_ratio
    return [lo - pad, hi + pad]


def rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_g = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_l = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def vwap_intraday(df: pd.DataFrame) -> pd.Series:
    """按交易日重置的累积 VWAP。"""
    vw, _, _ = vwap_and_bands(df)
    return vw


def vwap_and_bands(df: pd.DataFrame, n_std: float = 1.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    """按交易日重置：VWAP 与成交量加权标准差通道（上轨 VWAP+nσ、下轨 VWAP−nσ；默认 1 倍标准差）。"""
    if df.empty:
        e = pd.Series(dtype=float)
        return e, e, e
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"].fillna(0.0).astype(float)
    parts_v: list[pd.Series] = []
    parts_hi: list[pd.Series] = []
    parts_lo: list[pd.Series] = []
    for _, g in df.groupby(df.index.date):
        cum_v = vol.loc[g.index].cumsum()
        cum_pv = (tp.loc[g.index] * vol.loc[g.index]).cumsum()
        vw = cum_pv / cum_v.replace(0, np.nan)
        cum_p2v = ((tp.loc[g.index] ** 2) * vol.loc[g.index]).cumsum()
        mean_p2 = cum_p2v / cum_v.replace(0, np.nan)
        var = (mean_p2 - vw**2).clip(lower=0.0)
        sig = np.sqrt(var)
        parts_v.append(vw)
        parts_hi.append(vw + n_std * sig)
        parts_lo.append(vw - n_std * sig)
    return (
        pd.concat(parts_v).sort_index(),
        pd.concat(parts_hi).sort_index(),
        pd.concat(parts_lo).sort_index(),
    )


def _earnings_reaction_date_from_history(
    data: pd.DataFrame,
    as_of: pd.Timestamp,
) -> pd.Timestamp | None:
    """Return the latest completed earnings reaction trading day.

    Futu distinguishes the announcement date from the trading session that
    prices the news.  For an after-hours release these are different dates;
    using the announcement date would incorrectly include the full session
    before the earnings release in AVWAP.
    """
    if data is None or data.empty or "pub_trading_day_str" not in data.columns:
        return None

    frame = data.copy()
    empty_values = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    empty_numbers = pd.Series(np.nan, index=frame.index, dtype=float)
    frame["_release_day"] = pd.to_datetime(frame["pub_trading_day_str"], errors="coerce").dt.normalize()
    reaction_values = frame["trading_day_str"] if "trading_day_str" in frame.columns else empty_values
    release_times = frame["pub_time_str"] if "pub_time_str" in frame.columns else empty_values
    pub_types = frame["pub_type"] if "pub_type" in frame.columns else empty_numbers
    frame["_reaction_day"] = pd.to_datetime(reaction_values, errors="coerce").dt.normalize()
    frame["_release_time"] = pd.to_datetime(release_times, errors="coerce")
    frame["_pub_type"] = pd.to_numeric(pub_types, errors="coerce")
    frame = frame.dropna(subset=["_release_day"])
    if frame.empty:
        return None

    as_of_ts = pd.Timestamp(as_of)
    if as_of_ts.tzinfo is not None:
        as_of_ts = as_of_ts.tz_localize(None)
    as_of_day = as_of_ts.normalize()

    # The API repeats earnings metadata for each schedule row. One row per
    # release is sufficient, and keeping the first preserves its reaction day.
    frame = frame.sort_values(["_release_day", "_release_time"], na_position="last")
    frame = frame.drop_duplicates(subset=["_release_day"], keep="first")
    known_time = frame["_release_time"].notna() & (
        frame["_release_time"].dt.normalize() == frame["_release_day"]
    )
    published = (frame["_release_day"] < as_of_day) | (
        (frame["_release_day"] == as_of_day)
        & (~known_time | (frame["_release_time"] <= as_of_ts))
    )
    frame = frame.loc[published].copy()
    if frame.empty:
        return None

    # Fallback for older records without trading_day_str: premarket/in-session
    # reacts the same day; after-hours reacts on the next business day. The
    # normal path always uses Futu's actual trading day, which handles holidays.
    missing_reaction = frame["_reaction_day"].isna()
    after_hours = frame["_pub_type"] == 2
    frame.loc[missing_reaction & ~after_hours, "_reaction_day"] = frame.loc[
        missing_reaction & ~after_hours, "_release_day"
    ]
    frame.loc[missing_reaction & after_hours, "_reaction_day"] = frame.loc[
        missing_reaction & after_hours, "_release_day"
    ] + pd.offsets.BDay(1)
    frame = frame.loc[frame["_reaction_day"].notna() & (frame["_reaction_day"] <= as_of_day)]
    if frame.empty:
        return None
    latest = frame.sort_values(["_release_day", "_reaction_day"]).iloc[-1]
    return pd.Timestamp(latest["_reaction_day"]).normalize()


def _latest_earnings_date(symbol: str) -> pd.Timestamp | None:
    cached = _EARNINGS_DATE_CACHE.get(symbol)
    now = time.time()
    if cached and now - cached[1] < _EARNINGS_DATE_TTL_SECONDS:
        return cached[0]

    result: pd.Timestamp | None = None
    ctx = None
    try:
        from backend.config import FUTU_US
        from backend.market_data import futu_opend_config, is_futu_opend_available
        from futu import OpenQuoteContext, RET_OK

        code = FUTU_US.get(symbol)
        if code and is_futu_opend_available():
            host, port = futu_opend_config()
            ctx = OpenQuoteContext(host=host, port=port)
            ret, data = ctx.get_financials_earnings_price_history(code)
            if ret == RET_OK and data is not None and not data.empty:
                market_now = pd.Timestamp.now(tz=_market_tz(symbol)).tz_localize(None)
                result = _earnings_reaction_date_from_history(data, market_now)
    except Exception:
        result = None
    finally:
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass

    _EARNINGS_DATE_CACHE[symbol] = (result, now)
    return result


def latest_earnings_anchor(symbol: str) -> pd.Timestamp | None:
    """Return the cached latest earnings date for chart display-range anchoring."""
    return _latest_earnings_date(str(symbol or "").upper())


def _avwap_anchor_date(
    symbol: str,
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    mode: str,
) -> tuple[pd.Timestamp, str]:
    normalized_mode = mode if mode in AVWAP_MODE_LABELS else "earnings"
    current_day = _naive_day(intraday.index[-1])
    if normalized_mode == "today_open":
        return current_day, AVWAP_MODE_LABELS[normalized_mode]

    if normalized_mode == "year_start":
        year_start = pd.Timestamp(year=current_day.year, month=1, day=1)
        daily_dates = pd.DatetimeIndex([_naive_day(value) for value in daily.index]) if not daily.empty else pd.DatetimeIndex([])
        current_year_dates = daily_dates[(daily_dates >= year_start) & (daily_dates <= current_day)]
        anchor = _naive_day(current_year_dates.min()) if len(current_year_dates) else year_start
        return anchor, AVWAP_MODE_LABELS[normalized_mode]

    recent = daily.tail(60)
    if recent.empty:
        return current_day, AVWAP_MODE_LABELS["today_open"]

    if normalized_mode == "earnings":
        if symbol in _ETF_SYMBOLS:
            normalized_mode = "high_60d"
        else:
            earnings_date = _latest_earnings_date(symbol)
            if earnings_date is not None:
                return earnings_date, AVWAP_MODE_LABELS[normalized_mode]
            normalized_mode = "selloff_60d"

    if normalized_mode == "high_60d":
        anchor = _naive_day(recent["High"].astype(float).idxmax())
    elif normalized_mode == "low_60d":
        anchor = _naive_day(recent["Low"].astype(float).idxmin())
    elif normalized_mode == "gap_60d":
        previous_close = daily["Close"].astype(float).shift(1).reindex(recent.index)
        gap_pct = (recent["Open"].astype(float) / previous_close - 1.0).replace([np.inf, -np.inf], np.nan).dropna()
        significant_gaps = gap_pct[gap_pct.abs() >= 0.02]
        if not significant_gaps.empty:
            anchor = _naive_day(significant_gaps.index[-1])
        elif not gap_pct.empty:
            anchor = _naive_day(gap_pct.abs().idxmax())
        else:
            anchor = _naive_day(recent.index[0])
    else:
        daily_returns = recent["Close"].astype(float).pct_change()
        valid_returns = daily_returns.dropna()
        anchor = (
            _naive_day(valid_returns.idxmax() if normalized_mode == "rally_60d" else valid_returns.idxmin())
            if not valid_returns.empty
            else _naive_day(recent.index[0])
        )
    return anchor, AVWAP_MODE_LABELS[normalized_mode]


def anchored_vwap_and_bands(
    symbol: str,
    intraday: pd.DataFrame,
    mode: str,
    *,
    n_std: float = 1.0,
    cache_only: bool = False,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Timestamp, str]:
    if intraday.empty:
        empty = pd.Series(dtype=float)
        return empty, empty, empty, pd.Timestamp.now().normalize(), AVWAP_MODE_LABELS["today_open"]

    if mode == "none":
        empty = pd.Series(np.nan, index=intraday.index, dtype=float)
        return empty, empty.copy(), empty.copy(), _naive_day(intraday.index[-1]), AVWAP_MODE_LABELS["none"]

    daily = fetch_ohlcv(symbol, "1d", "5y", cache_only=cache_only)
    anchor_date, label = _avwap_anchor_date(symbol, daily, intraday, mode)
    current_day = _naive_day(intraday.index[-1])

    base_volume = 0.0
    base_pv = 0.0
    base_p2v = 0.0
    if not daily.empty and anchor_date < current_day:
        daily_dates = pd.DatetimeIndex([_naive_day(value) for value in daily.index])
        base = daily.loc[(daily_dates >= anchor_date) & (daily_dates < current_day)]
        if not base.empty:
            base_tp = (base["High"] + base["Low"] + base["Close"]) / 3.0
            base_vol = base["Volume"].fillna(0.0).astype(float).clip(lower=0.0)
            base_volume = float(base_vol.sum())
            base_pv = float((base_tp * base_vol).sum())
            base_p2v = float(((base_tp**2) * base_vol).sum())

    intraday_dates = pd.DatetimeIndex([_naive_day(value) for value in intraday.index])
    active = intraday.loc[intraday_dates >= anchor_date]
    result = pd.Series(np.nan, index=intraday.index, dtype=float)
    upper = result.copy()
    lower = result.copy()
    if active.empty:
        return result, upper, lower, anchor_date, label

    tp = (active["High"] + active["Low"] + active["Close"]) / 3.0
    vol = active["Volume"].fillna(0.0).astype(float).clip(lower=0.0)
    cum_volume = base_volume + vol.cumsum()
    cum_pv = base_pv + (tp * vol).cumsum()
    cum_p2v = base_p2v + ((tp**2) * vol).cumsum()
    avwap = cum_pv / cum_volume.replace(0, np.nan)
    variance = (cum_p2v / cum_volume.replace(0, np.nan) - avwap**2).clip(lower=0.0)
    sigma = np.sqrt(variance)
    result.loc[active.index] = avwap
    upper.loc[active.index] = avwap + n_std * sigma
    lower.loc[active.index] = avwap - n_std * sigma
    return result, upper, lower, anchor_date, label


def daily_anchored_vwap(
    symbol: str,
    daily: pd.DataFrame,
    mode: str,
) -> tuple[pd.Series, pd.Timestamp, str]:
    if daily.empty:
        return pd.Series(dtype=float), pd.Timestamp.now().normalize(), AVWAP_MODE_LABELS["earnings"]

    daily_mode = mode if mode in {"earnings", "high_60d", "selloff_60d"} else "earnings"
    anchor_date, label = _avwap_anchor_date(symbol, daily, daily, daily_mode)
    daily_dates = pd.DatetimeIndex([_naive_day(value) for value in daily.index])
    active = daily.loc[daily_dates >= anchor_date]
    result = pd.Series(np.nan, index=daily.index, dtype=float)
    if active.empty:
        return result, anchor_date, label

    typical_price = (active["High"] + active["Low"] + active["Close"]) / 3.0
    volume = active["Volume"].fillna(0.0).astype(float).clip(lower=0.0)
    result.loc[active.index] = (typical_price * volume).cumsum() / volume.cumsum().replace(0, np.nan)
    return result, anchor_date, label


def _focus_intraday_price_axis(
    fig: go.Figure,
    df: pd.DataFrame,
    avwap: pd.Series,
    atr_values: pd.Series,
    last_close: float,
    avwap_label: str,
    theme: dict[str, Any],
) -> list[float]:
    y_lo = float(df["Low"].min())
    y_hi = float(df["High"].max())
    candle_span = max(y_hi - y_lo, max(abs(last_close) * 0.005, 1e-9))
    atr_clean = atr_values.replace([np.inf, -np.inf], np.nan).dropna()
    atr_pad = float(atr_clean.iloc[-1]) if not atr_clean.empty else 0.0
    y_pad = max(atr_pad, candle_span * 0.12)
    price_y_range = [y_lo - y_pad, y_hi + y_pad]

    avwap_clean = avwap.replace([np.inf, -np.inf], np.nan).dropna()
    if avwap_clean.empty:
        return price_y_range

    current_avwap = float(avwap_clean.iloc[-1])
    if price_y_range[0] <= current_avwap <= price_y_range[1]:
        return price_y_range

    for trace in fig.data:
        if str(getattr(trace, "name", "")).startswith("AVWAP"):
            trace.visible = False

    above = current_avwap > price_y_range[1]
    distance_pct = (current_avwap / last_close - 1.0) * 100.0 if last_close > 0 else 0.0
    fig.add_annotation(
        x=df.index.max(),
        y=price_y_range[1] if above else price_y_range[0],
        xref="x1",
        yref="y1",
        text=f"{'↑' if above else '↓'} {avwap_label} AVWAP {current_avwap:.2f}（{distance_pct:+.2f}%）",
        showarrow=False,
        xanchor="right",
        yanchor="top" if above else "bottom",
        font=dict(color=theme["vwap"], size=11),
        bgcolor=theme["paper"],
        bordercolor=theme["vwap"],
        borderwidth=1,
        borderpad=4,
    )
    return price_y_range


def _add_latest_price_line(
    fig: go.Figure,
    price: float,
    theme: dict[str, Any],
    change_pct: float | None = None,
) -> None:
    if not np.isfinite(price) or price <= 0:
        return
    color = theme["rsi_mid"]
    fig.add_hline(
        y=price,
        name="latest_price_line",
        line_dash="dash",
        line_width=1.15,
        line_color=color,
        row=1,
        col=1,
    )
    change_text = f"<br>{change_pct:+.2f}%" if change_pct is not None and np.isfinite(change_pct) else ""
    fig.add_annotation(
        name="latest_price_label",
        x=1.005,
        y=price,
        xref="paper",
        yref="y1",
        text=f"最新 {price:.2f}{change_text}",
        showarrow=False,
        xanchor="left",
        yanchor="middle",
        font=dict(color=color, size=11),
        bgcolor=theme["paper"],
        bordercolor=color,
        borderwidth=1,
        borderpad=3,
    )
    fig.update_layout(margin=dict(r=76))


def multiframe_signal_bundle(symbol: str) -> dict[str, Any]:
    """多周期一致性粗评分：日线趋势 + 15m 相对 VWAP + 5m RSI 节奏。"""
    out: dict[str, Any] = {
        "daily": None,
        "m15": None,
        "m5": None,
        "total": 0,
        "summary": "",
    }
    notes: list[str] = []

    d1 = fetch_ohlcv(symbol, "1d", "5y")
    if not d1.empty and len(d1) > 55:
        c = d1["Close"]
        if float(ema(c, 20).iloc[-1]) > float(ema(c, 50).iloc[-1]):
            out["daily"] = 1
            notes.append("日线：EMA20>EMA50")
        else:
            out["daily"] = 0
            notes.append("日线：EMA20≤EMA50")

    df15 = fetch_ohlcv(symbol, "15m", "5d")
    df15, _ = slice_intraday_today_or_yesterday(df15, symbol)
    if not df15.empty:
        vw = vwap_intraday(df15)
        cl = df15["Close"]
        rl = rsi(cl, 14)
        i = -1
        if float(cl.iloc[i]) >= float(vw.iloc[i]) and float(rl.iloc[i]) < 72:
            out["m15"] = 1
            notes.append("15m：价≥VWAP 且 RSI 未过热")
        else:
            out["m15"] = 0
            notes.append("15m：未同时满足价≥VWAP 与 RSI<72")

    df5 = fetch_ohlcv(symbol, "5m", "5d")
    df5, _ = slice_intraday_today_or_yesterday(df5, symbol)
    if not df5.empty:
        rl7 = rsi(df5["Close"], 7)
        if float(rl7.iloc[-1]) < 75:
            out["m5"] = 1
            notes.append("5m：RSI(7) 未过热")
        else:
            out["m5"] = 0
            notes.append("5m：RSI(7) 偏高")

    parts = [x for x in (out["daily"], out["m15"], out["m5"]) if x is not None]
    out["total"] = int(sum(parts)) if parts else 0
    out["summary"] = "；".join(notes) if notes else "数据不足"
    return out


def sync_symbol_bars(symbol: str) -> dict[str, int]:
    """手动触发一次三周期同步（返回各周期当前可用条数）。"""
    out: dict[str, int] = {}
    for interval, period in (("1d", "5y"), ("15m", "5d"), ("5m", "5d")):
        # 手动同步：绕过 fetch_ohlcv 内的最小同步间隔节流
        _LAST_SYNC_AT[(symbol, interval)] = 0.0
        _ = fetch_ohlcv(symbol, interval, period)  # fetch_ohlcv 内部会做增量写入
        # 同步按钮展示“Supabase 侧真实表行数”（在 limit 内，避免前端窗口长度误差）
        total_df = _load_bars_from_supabase(symbol, interval, since_utc=None, limit=10000)
        out[interval] = int(len(total_df)) if not total_df.empty else 0
    return out


def fig_daily(
    symbol: str,
    display_name: str,
    *,
    chart_theme: str = "Classic Light",
    user_avg_cost: float | None = None,
    avwap_mode: str = "earnings",
    latest_price: float | None = None,
    latest_change_pct: float | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "1d", "5y", cache_only=cache_only)
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无足够日线数据（至少需要 60 根 K 线，可检查网络或标的代码）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme["muted"]),
        )
        _apply_chart_theme(fig, theme)
        return fig

    c = df["Close"]
    last_close = float(c.iloc[-1]) if len(c) else 0.0
    p_min = float(df["Low"].min())
    p_max = float(df["High"].max())

    def _cost_visible(cost: float) -> bool:
        return cost > 0

    e20, e50, e100, e200 = ema(c, 20), ema(c, 50), ema(c, 100), ema(c, 200)
    r14 = rsi(c, 14)
    atr_v = atr_series(df, 14)
    m_line, m_sig, m_hist = macd_series(c)
    daily_avwap, avwap_anchor, avwap_label = daily_anchored_vwap(symbol, df, avwap_mode)

    # 默认展示最近 N 根「交易日」K 线（非日历天），与页面 60 日回撤口径保持一致。
    _n_daily_visible = 60
    vis_df = df.tail(_n_daily_visible) if len(df) >= _n_daily_visible else df
    vis_e20 = e20.reindex(vis_df.index)
    vis_e50 = e50.reindex(vis_df.index)
    vis_e100 = e100.reindex(vis_df.index)
    vis_e200 = e200.reindex(vis_df.index)
    vis_r14 = r14.reindex(vis_df.index)
    vis_daily_avwap = daily_avwap.reindex(vis_df.index)
    vis_m_line = m_line.reindex(vis_df.index)
    vis_m_sig = m_sig.reindex(vis_df.index)
    vis_m_hist = m_hist.reindex(vis_df.index)
    vis_vol = vis_df["Volume"].fillna(0.0).astype(float)
    vis_vcols = _volume_bar_colors(vis_df, theme)
    full_vol = df["Volume"].fillna(0.0).astype(float)
    full_vcols = _volume_bar_colors(df, theme)
    _x_start = vis_df.index.min()
    _x_end = vis_df.index.max()
    _x_pad = pd.Timedelta(days=0.45)

    vp_price, vp_vol, vp_low, vp_high, vp_width = _volume_profile_by_price(vis_df, bins=26)

    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.035,
        horizontal_spacing=0.02,
        row_heights=[0.52, 0.22, 0.26],
        column_widths=[0.82, 0.18],
    )
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            customdata=_candlestick_customdata(df),
            hovertemplate=_candlestick_hovertemplate(symbol),
            name="K线",
            **_candlestick_kwargs(theme),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    _add_latest_price_line(fig, float(latest_price or last_close), theme, latest_change_pct)

    # 叠加用户“持仓成本”水平线 + 涨跌幅标注
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        cost = float(user_avg_cost)
        pct = (last_close / cost - 1.0) * 100 if cost > 0 else 0.0
        fig.add_hline(
            y=cost,
            line_dash="dashdot",
            line_width=1.8,
            line_color=theme["rsi_orange"],
            row=1,
            col=1,
        )
        fig.add_annotation(
            x=_x_end,
            y=cost,
            xref="x1",
            yref="y1",
            text=f"成本 {cost:.2f}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    for name, s, color in zip(
        ("EMA20", "EMA50", "EMA100", "EMA200"),
        (e20, e50, e100, e200),
        theme["ema"],
    ):
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=s,
                name=name,
                line=dict(width=1.15, color=color),
                opacity=0.92,
            ),
            row=1,
            col=1,
            secondary_y=False,
        )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=daily_avwap,
            name=f"AVWAP · {avwap_label}",
            line=dict(color=theme["vwap"], width=1.6),
            connectgaps=False,
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=full_vol,
            name="成交量",
            marker_color=full_vcols,
            marker_line_width=0,
            showlegend=False,
            hovertemplate="成交量: %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r14,
            name="RSI(14)",
            line=dict(color=theme["rsi"], width=1.2),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color=theme["rsi_ob"], row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color=theme["rsi_mid"], row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=theme["ref_line"],
        row=2,
        col=1,
    )

    hist_colors = [theme["macd_pos"] if float(v) >= 0 else theme["macd_neg"] for v in m_hist.fillna(0.0)]
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=m_hist,
            name="MACD柱",
            marker_color=hist_colors,
            marker_line_width=0,
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_line,
            name="MACD",
            line=dict(color=theme["macd_line"], width=1.1),
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_sig,
            name="Signal",
            line=dict(color=theme["macd_sig"], width=1.0),
        ),
        row=3,
        col=1,
    )

    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                width=vp_width.values,
                customdata=np.column_stack([vp_low.values, vp_high.values]),
                orientation="h",
                name="价位成交量",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格区间: %{customdata[0]:.4f} - %{customdata[1]:.4f}<br>区间成交量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
        meta={
            "avwap_mode": avwap_mode if avwap_mode != "today_open" else "earnings",
            "avwap_label": avwap_label,
            "avwap_anchor": avwap_anchor.strftime("%Y-%m-%d"),
        },
    )
    _apply_chart_theme(fig, theme)
    vmax_vis = float(vis_vol.max()) if len(vis_vol) else 0.0
    price_y_range = _focus_intraday_price_axis(
        fig,
        vis_df,
        vis_daily_avwap,
        atr_v.reindex(vis_df.index),
        last_close,
        avwap_label,
        theme,
    )
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        price_y_range[0] = min(price_y_range[0], float(user_avg_cost))
        price_y_range[1] = max(price_y_range[1], float(user_avg_cost))
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=price_y_range,
    )
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax_vis * 4 if vmax_vis > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    # MACD 用“可见窗口”数据算范围，避免 5 年历史极值把副图撑太大
    macd_range = _macd_yaxis_range(
        vis_m_line,
        vis_m_sig,
        vis_m_hist,
    )
    fig.update_yaxes(title_text="MACD", row=3, col=1, title_standoff=8, range=macd_range)
    # 横轴：最近 N 个交易日 + 少量边距，K 线更易辨认
    _daily_rangebreaks = [dict(bounds=["sat", "mon"])]
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], rangebreaks=_daily_rangebreaks, row=1, col=1)
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], rangebreaks=_daily_rangebreaks, row=2, col=1)
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], rangebreaks=_daily_rangebreaks, row=3, col=1)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, showgrid=False, range=price_y_range, row=1, col=2)
    return fig


def fig_15m_vwap_rsi(
    symbol: str,
    display_name: str,
    *,
    chart_theme: str = "Classic Light",
    user_avg_cost: float | None = None,
    avwap_mode: str = "earnings",
    show_extended: bool = True,
    latest_price: float | None = None,
    latest_change_pct: float | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "15m", "2d", cache_only=cache_only)
    if show_extended:
        df, _ = slice_intraday_today_or_yesterday(
            df,
            symbol,
            min_current_bars=4,
            # Extended-hours view must stay on the current trading date.
            # Do not backfill yesterday just because only a few premarket
            # bars have arrived after the extended session opens.
            include_previous_context=False,
        )
    else:
        df, _ = slice_regular_intraday_with_context(
            df,
            symbol,
            min_current_bars=4,
            include_previous_context=True,
        )
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无 15 分钟数据（腾讯/东财暂未返回该标的分时 K 线）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme["muted"]),
        )
        _apply_chart_theme(fig, theme)
        return fig

    vw, v_hi, v_lo, avwap_anchor, avwap_label = anchored_vwap_and_bands(
        symbol,
        df,
        avwap_mode,
        cache_only=cache_only,
    )
    cl = df["Close"]
    last_close = float(cl.iloc[-1]) if len(cl) else 0.0
    p_min = float(df["Low"].min())
    p_max = float(df["High"].max())

    def _cost_visible(cost: float) -> bool:
        return cost > 0

    r14 = rsi(cl, 14)
    r14_ma = ema(r14, 9)
    atr_v = atr_series(df, 14)
    m_line, m_sig, m_hist = macd_series(cl)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df, theme)

    regular_session_mask = _regular_us_session_mask(df.index)
    regular_df = df.loc[regular_session_mask]
    vp_price, vp_vol, vp_low, vp_high, vp_width = _volume_profile_by_price(regular_df, bins=24)
    volume_mask = np.isfinite(vol.to_numpy()) & (vol.to_numpy() > 0)
    if not show_extended:
        volume_mask &= regular_session_mask
    display_vol = vol.loc[volume_mask]
    display_vcols = [
        color if is_regular else _with_rgba_alpha(color, 0.20)
        for color, is_regular, is_visible in zip(vcols, regular_session_mask, volume_mask)
        if is_visible
    ]
    fixed_x_range = _intraday_fixed_x_range(df.index, symbol, show_extended)
    df.index = _shanghai_plot_index(df.index)

    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.035,
        horizontal_spacing=0.02,
        row_heights=[0.52, 0.22, 0.26],
        column_widths=[0.82, 0.18],
    )
    _add_intraday_candlesticks(fig, df, theme, regular_session_mask, show_extended)
    _add_latest_price_line(fig, float(latest_price or last_close), theme, latest_change_pct)

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            mode="lines",
            name="AVWAP+1σ",
            line=dict(color=theme["vwap_band"], width=1, dash="dot"),
            legendgroup="vwap_band",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_lo,
            mode="lines",
            name="AVWAP−1σ",
            line=dict(color=theme["vwap_band"], width=1, dash="dot"),
            fill="tonexty",
            fillcolor=theme["vwap_fill"],
            legendgroup="vwap_band",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=vw,
            mode="lines",
            name=f"AVWAP · {avwap_label}",
            line=dict(color=theme["vwap"], width=1.35),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df.index[volume_mask],
            y=display_vol,
            name="成交量",
            marker_color=display_vcols,
            marker_line_width=0,
            showlegend=False,
            hovertemplate="成交量: %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        cost = float(user_avg_cost)
        pct = (last_close / cost - 1.0) * 100 if cost > 0 else 0.0
        fig.add_hline(
            y=cost,
            line_dash="dashdot",
            line_width=1.8,
            line_color=theme["rsi_orange"],
            row=1,
            col=1,
        )
        fig.add_annotation(
            x=df.index.max(),
            y=cost,
            xref="x1",
            yref="y1",
            text=f"成本 {cost:.2f}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r14,
            name="RSI(14)",
            line=dict(color=theme["rsi"], width=1.15),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r14_ma,
            name="RSI EMA(9)",
            line=dict(color=theme["rsi_ma"], width=1.1, dash="solid"),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color=theme["rsi_ob"], row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color=theme["rsi_mid"], row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=theme["ref_line"],
        row=2,
        col=1,
    )
    hist_colors = [theme["macd_pos"] if float(v) >= 0 else theme["macd_neg"] for v in m_hist.fillna(0.0)]
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=m_hist,
            name="MACD柱",
            marker_color=hist_colors,
            marker_line_width=0,
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_line,
            name="MACD",
            line=dict(color=theme["macd_line"], width=1.1),
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_sig,
            name="Signal",
            line=dict(color=theme["macd_sig"], width=1.0),
        ),
        row=3,
        col=1,
    )
    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                width=vp_width.values,
                customdata=np.column_stack([vp_low.values, vp_high.values]),
                orientation="h",
                name="价位成交量（正常盘）",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格区间: %{customdata[0]:.4f} - %{customdata[1]:.4f}<br>区间成交量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
        meta={
            "avwap_mode": avwap_mode,
            "avwap_label": avwap_label,
            "avwap_anchor": avwap_anchor.strftime("%Y-%m-%d"),
        },
    )
    _apply_chart_theme(fig, theme)
    vmax = float(display_vol.max()) if len(display_vol) else 0.0
    price_y_range = _focus_intraday_price_axis(
        fig,
        df,
        vw,
        atr_v,
        last_close,
        avwap_label,
        theme,
    )
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        price_y_range[0] = min(price_y_range[0], float(user_avg_cost))
        price_y_range[1] = max(price_y_range[1], float(user_avg_cost))
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=price_y_range,
    )
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax * 4 if vmax > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    macd_range = _macd_yaxis_range(m_line, m_sig, m_hist)
    fig.update_yaxes(title_text="MACD", row=3, col=1, title_standoff=8, range=macd_range)
    if fixed_x_range is not None:
        tick_options = _intraday_xaxis_tick_options(fixed_x_range, show_extended)
        for _row in (1, 2, 3):
            fig.update_xaxes(range=fixed_x_range, **tick_options, row=_row, col=1)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, showgrid=False, range=price_y_range, row=1, col=2)
    return fig


def fig_5m_vwap_rsi7(
    symbol: str,
    display_name: str,
    *,
    chart_theme: str = "Classic Light",
    user_avg_cost: float | None = None,
    avwap_mode: str = "earnings",
    show_extended: bool = True,
    latest_price: float | None = None,
    latest_change_pct: float | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "5m", "2d", cache_only=cache_only)
    if show_extended:
        df, _ = slice_intraday_today_or_yesterday(
            df,
            symbol,
            min_current_bars=12,
            # Extended-hours view must stay on the current trading date.
            include_previous_context=False,
        )
    else:
        df, _ = slice_regular_intraday_with_context(
            df,
            symbol,
            min_current_bars=12,
            include_previous_context=True,
        )
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无 5 分钟数据（腾讯/东财暂未返回该标的分时 K 线）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme["muted"]),
        )
        _apply_chart_theme(fig, theme)
        return fig

    vw, v_hi, v_lo, avwap_anchor, avwap_label = anchored_vwap_and_bands(
        symbol,
        df,
        avwap_mode,
        cache_only=cache_only,
    )
    cl = df["Close"]
    last_close = float(cl.iloc[-1]) if len(cl) else 0.0
    r7 = rsi(cl, 7)
    r7_ma = ema(r7, 9)
    atr_v = atr_series(df, 14)
    m_line, m_sig, m_hist = macd_series(cl)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df, theme)

    regular_session_mask = _regular_us_session_mask(df.index)
    regular_df = df.loc[regular_session_mask]
    vp_price, vp_vol, vp_low, vp_high, vp_width = _volume_profile_by_price(regular_df, bins=24)
    volume_mask = np.isfinite(vol.to_numpy()) & (vol.to_numpy() > 0)
    if not show_extended:
        volume_mask &= regular_session_mask
    display_vol = vol.loc[volume_mask]
    display_vcols = [
        color if is_regular else _with_rgba_alpha(color, 0.20)
        for color, is_regular, is_visible in zip(vcols, regular_session_mask, volume_mask)
        if is_visible
    ]
    fixed_x_range = _intraday_fixed_x_range(df.index, symbol, show_extended)
    df.index = _shanghai_plot_index(df.index)

    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.035,
        horizontal_spacing=0.02,
        row_heights=[0.52, 0.22, 0.26],
        column_widths=[0.82, 0.18],
    )
    _add_intraday_candlesticks(fig, df, theme, regular_session_mask, show_extended)
    _add_latest_price_line(fig, float(latest_price or last_close), theme, latest_change_pct)
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            mode="lines",
            name="AVWAP+1σ",
            line=dict(color=theme["vwap_band"], width=1, dash="dot"),
            legendgroup="vwap_band",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_lo,
            mode="lines",
            name="AVWAP−1σ",
            line=dict(color=theme["vwap_band"], width=1, dash="dot"),
            fill="tonexty",
            fillcolor=theme["vwap_fill"],
            legendgroup="vwap_band",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=vw,
            mode="lines",
            name=f"AVWAP · {avwap_label}",
            line=dict(color=theme["vwap"], width=1.35),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df.index[volume_mask],
            y=display_vol,
            name="成交量",
            marker_color=display_vcols,
            marker_line_width=0,
            showlegend=False,
            hovertemplate="成交量: %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )
    p_min = float(df["Low"].min())
    p_max = float(df["High"].max())

    def _cost_visible(cost: float) -> bool:
        return cost > 0

    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        cost = float(user_avg_cost)
        pct = (last_close / cost - 1.0) * 100 if cost > 0 else 0.0
        fig.add_hline(
            y=cost,
            line_dash="dashdot",
            line_width=1.8,
            line_color=theme["rsi_orange"],
            row=1,
            col=1,
        )
        fig.add_annotation(
            x=df.index.max(),
            y=cost,
            xref="x1",
            yref="y1",
            text=f"成本 {cost:.2f}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r7,
            name="RSI(7)",
            line=dict(color=theme["rsi_orange"], width=1.15),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r7_ma,
            name="RSI EMA(9)",
            line=dict(color=theme["rsi_ma"], width=1.1, dash="solid"),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color=theme["rsi_ob"], row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color=theme["rsi_mid"], row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=theme["ref_line"],
        row=2,
        col=1,
    )
    hist_colors = [theme["macd_pos"] if float(v) >= 0 else theme["macd_neg"] for v in m_hist.fillna(0.0)]
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=m_hist,
            name="MACD柱",
            marker_color=hist_colors,
            marker_line_width=0,
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_line,
            name="MACD",
            line=dict(color=theme["macd_line"], width=1.1),
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=m_sig,
            name="Signal",
            line=dict(color=theme["macd_sig"], width=1.0),
        ),
        row=3,
        col=1,
    )
    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                width=vp_width.values,
                customdata=np.column_stack([vp_low.values, vp_high.values]),
                orientation="h",
                name="价位成交量（正常盘）",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格区间: %{customdata[0]:.4f} - %{customdata[1]:.4f}<br>区间成交量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
        meta={
            "avwap_mode": avwap_mode,
            "avwap_label": avwap_label,
            "avwap_anchor": avwap_anchor.strftime("%Y-%m-%d"),
        },
    )
    _apply_chart_theme(fig, theme)
    vmax = float(display_vol.max()) if len(display_vol) else 0.0
    price_y_range = _focus_intraday_price_axis(
        fig,
        df,
        vw,
        atr_v,
        last_close,
        avwap_label,
        theme,
    )
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        price_y_range[0] = min(price_y_range[0], float(user_avg_cost))
        price_y_range[1] = max(price_y_range[1], float(user_avg_cost))
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=price_y_range,
    )
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax * 4 if vmax > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    macd_range = _macd_yaxis_range(m_line, m_sig, m_hist)
    fig.update_yaxes(title_text="MACD", row=3, col=1, title_standoff=8, range=macd_range)
    if fixed_x_range is not None:
        tick_options = _intraday_xaxis_tick_options(fixed_x_range, show_extended)
        for _row in (1, 2, 3):
            fig.update_xaxes(range=fixed_x_range, **tick_options, row=_row, col=1)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, showgrid=False, range=price_y_range, row=1, col=2)
    return fig


def fig_global_kline_board(
    symbols: list[str],
    *,
    interval: Literal["1d", "15m", "5m"] = "5m",
    chart_theme: str = "Trading Dark",
    show_extended: bool = True,
    columns: int = 1,
    latest_quotes: dict[str, dict[str, float]] | None = None,
    user_avg_costs: dict[str, float] | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    cols = min(5, max(1, int(columns or 1)))
    rows = max(1, (len(symbols) + cols - 1) // cols)
    fig = make_subplots(
        rows=rows,
        cols=cols,
        specs=[[{"secondary_y": True} for _ in range(cols)] for _ in range(rows)],
        shared_xaxes=False,
        vertical_spacing=0.035 if cols > 1 else 0.018,
        horizontal_spacing=0.055 if cols > 1 else 0.02,
    )
    x_ranges: list[list[pd.Timestamp]] = []
    period = "5y" if interval == "1d" else "5d"

    def _load_global_symbol(symbol: str) -> tuple[str, pd.DataFrame]:
        df = fetch_ohlcv(symbol, interval, period, cache_only=cache_only)
        if interval == "15m" and df.empty:
            df5 = fetch_ohlcv(symbol, "5m", "5d", cache_only=cache_only)
            df = _resample_ohlcv(df5, "15min")
        return symbol, df

    loaded: dict[str, pd.DataFrame] = {}
    if symbols:
        max_workers = min(8, len(symbols))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_load_global_symbol, symbol): symbol for symbol in symbols}
            for future in concurrent.futures.as_completed(future_map):
                symbol = future_map[future]
                try:
                    _, df = future.result()
                except Exception:
                    df = pd.DataFrame()
                loaded[symbol] = df

    for idx, symbol in enumerate(symbols):
        row = idx // cols + 1
        col = idx % cols + 1
        axis_index = idx + 1
        axis_suffix = "" if axis_index == 1 else str(axis_index)
        df = loaded.get(symbol, pd.DataFrame())
        daily_ema: dict[str, pd.Series] = {}
        if interval != "1d":
            if show_extended:
                df, _ = slice_intraday_today_or_yesterday(df, symbol)
            else:
                df, _ = slice_regular_intraday_with_context(df, symbol, min_current_bars=12 if interval == "5m" else 4)
        elif not df.empty:
            close = df["Close"]
            daily_ema = {
                "EMA20": ema(close, 20),
            }
            df = df.tail(90).copy()

        if df.empty:
            fig.add_annotation(
                text=f"{symbol} 暂无数据",
                xref="x domain",
                yref="y domain",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(color=theme["muted"], size=11),
                row=row,
                col=col,
            )
            continue

        if interval != "1d":
            fixed_x_range = _intraday_fixed_x_range(df.index, symbol, show_extended)
            if fixed_x_range is not None:
                x_ranges.append(fixed_x_range)
            open_base_price = _intraday_open_base_price(df, symbol, show_extended)
            regular_mask = _regular_us_session_mask(df.index)
            vw, v_hi, v_lo = vwap_and_bands(df)
            df_plot = df.copy()
            df_plot.index = _shanghai_plot_index(df_plot.index)
            vw.index = df_plot.index
            v_hi.index = df_plot.index
            v_lo.index = df_plot.index
            _add_intraday_candlesticks(fig, df_plot, theme, regular_mask, show_extended, row=row, col=col, show_hover=False)
        else:
            df_plot = df.copy()
            vw = pd.Series(dtype=float)
            v_hi = pd.Series(dtype=float)
            v_lo = pd.Series(dtype=float)
            latest_open = pd.to_numeric(df_plot["Open"], errors="coerce").dropna()
            open_base_price = float(latest_open.iloc[-1]) if not latest_open.empty else None
            if open_base_price is not None and (not np.isfinite(open_base_price) or open_base_price <= 0):
                open_base_price = None
            x_ranges.append([df_plot.index.min() - pd.Timedelta(days=0.45), df_plot.index.max() + pd.Timedelta(days=0.45)])
            fig.add_trace(
                go.Candlestick(
                    x=df_plot.index,
                    open=df_plot["Open"],
                    high=df_plot["High"],
                    low=df_plot["Low"],
                    close=df_plot["Close"],
                    name=symbol,
                    increasing=dict(line=dict(color=theme["up_line"], width=1.1), fillcolor=theme["up_fill"]),
                    decreasing=dict(line=dict(color=theme["dn_line"], width=1.1), fillcolor=theme["dn_fill"]),
                    whiskerwidth=0.35,
                    showlegend=False,
                ),
                row=row,
                col=col,
            )
            for (ema_name, ema_series), ema_color in zip(daily_ema.items(), theme["ema"]):
                fig.add_trace(
                    go.Scatter(
                        x=df_plot.index,
                        y=ema_series.reindex(df_plot.index),
                        name=f"{symbol} {ema_name}",
                        line=dict(width=1.05, color=ema_color),
                        opacity=0.9,
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                    secondary_y=False,
                )

        vol = df_plot["Volume"].fillna(0.0).astype(float)
        vcols = _volume_bar_colors(df_plot, theme)
        if interval != "1d" and not vw.empty:
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=v_hi,
                    name=f"{symbol} VWAP+1σ",
                    mode="lines",
                    line=dict(color=theme["vwap_band"], width=0.8, dash="dot"),
                    legendgroup=f"{symbol}-vwap-band",
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=row,
                col=col,
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=v_lo,
                    name=f"{symbol} VWAP-1σ",
                    mode="lines",
                    line=dict(color=theme["vwap_band"], width=0.8, dash="dot"),
                    fill="tonexty",
                    fillcolor=theme["vwap_fill"],
                    legendgroup=f"{symbol}-vwap-band",
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=row,
                col=col,
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=vw,
                    name=f"{symbol} Today VWAP",
                    mode="lines",
                    line=dict(color=theme["vwap"], width=1.35),
                    connectgaps=False,
                    showlegend=False,
                ),
                row=row,
                col=col,
                secondary_y=False,
            )
        fig.add_trace(
            go.Bar(
                x=df_plot.index,
                y=vol,
                name=f"{symbol} Volume",
                marker_color=[_with_rgba_alpha(color, 0.22) for color in vcols],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="Volume %{y:,.0f}<extra></extra>",
            ),
            row=row,
            col=col,
            secondary_y=True,
        )
        fig.add_annotation(
            x=0.012,
            y=0.985,
            xref="x domain",
            yref="y domain",
            text=f"<b>{symbol}</b>",
            showarrow=False,
            xanchor="left",
            yanchor="top",
            font=dict(color=theme["rsi_mid"], size=14),
            bgcolor=theme["plot"],
            bordercolor=theme["rsi_mid"],
            borderwidth=1,
            borderpad=4,
            row=row,
            col=col,
        )
        quote = (latest_quotes or {}).get(symbol) or {}
        last_close = float(df["Close"].iloc[-1])
        price = float(quote.get("price") or last_close)
        extended_change_pct = quote.get("extended_change_pct")
        if interval == "1d":
            change_pct = quote.get("regular_change_pct", quote.get("change_pct"))
            if change_pct is None and len(df) >= 2:
                prev_close = float(df["Close"].iloc[-2])
                if prev_close > 0:
                    change_pct = (price / prev_close - 1.0) * 100.0
        elif interval != "1d" and show_extended and extended_change_pct is not None:
            change_pct = extended_change_pct
        elif interval != "1d" and not show_extended:
            change_pct = quote.get("regular_change_pct", quote.get("change_pct"))
        else:
            change_pct = quote.get("regular_change_pct", quote.get("change_pct"))
        label = f"{symbol} {price:.2f}"
        if change_pct is not None and np.isfinite(float(change_pct)):
            label += f"<br>{float(change_pct):+.2f}%"
        fig.add_hline(
            y=price,
            line_dash="dash",
            line_width=0.9,
            line_color=theme["rsi_mid"],
            row=row,
            col=col,
        )
        fig.add_annotation(
            x=0.988,
            y=0.985,
            xref="x domain",
            yref="y domain",
            text=label,
            showarrow=False,
            xanchor="right",
            yanchor="top",
            font=dict(color=theme["rsi_mid"], size=10),
            bgcolor=theme["paper"],
            bordercolor=theme["rsi_mid"],
            borderwidth=1,
            borderpad=2,
            row=row,
            col=col,
        )
        cost = float((user_avg_costs or {}).get(symbol, 0.0) or 0.0)
        if interval == "1d" and np.isfinite(cost) and cost > 0:
            fig.add_hline(
                y=cost,
                line_dash="dashdot",
                line_width=1.8,
                line_color=theme["rsi_orange"],
                row=row,
                col=col,
            )
        price_low = min(float(df["Low"].min()), cost) if interval == "1d" and cost > 0 else float(df["Low"].min())
        price_high = max(float(df["High"].max()), cost) if interval == "1d" and cost > 0 else float(df["High"].max())
        pad = max(price_high - price_low, abs(price) * 0.003, 1e-9)
        fig.update_yaxes(
            title_text="",
            range=[price_low - pad * 0.12, price_high + pad * 0.12],
            row=row,
            col=col,
            secondary_y=False,
        )
        vmax = float(vol.max()) if len(vol) else 0.0
        fig.update_yaxes(
            title_text="",
            showgrid=False,
            showticklabels=False,
            visible=False,
            range=[0, vmax * 4 if vmax > 0 else 1],
            row=row,
            col=col,
            secondary_y=True,
        )

    fig.update_layout(
        height=max(760, (310 if cols > 1 else 210) * rows),
        xaxis_rangeslider_visible=False,
        showlegend=False,
        margin=dict(l=32, r=44 if cols > 1 else 86, t=24, b=30),
        title=None,
    )
    _apply_chart_theme(fig, theme)
    board_x_range = [
        min(rng[0] for rng in x_ranges),
        max(rng[1] for rng in x_ranges),
    ] if x_ranges else None
    intraday_tick_options = _intraday_xaxis_tick_options(board_x_range, show_extended) if interval != "1d" else {}
    daily_rangebreaks = [dict(bounds=["sat", "mon"])] if interval == "1d" else None
    for row in range(1, rows + 1):
        for col in range(1, cols + 1):
            subplot_index = (row - 1) * cols + col
            if subplot_index > len(symbols):
                continue
            fig.update_xaxes(
                rangeslider_visible=False,
                range=board_x_range,
                rangebreaks=daily_rangebreaks,
                **intraday_tick_options,
                row=row,
                col=col,
            )
    return fig
