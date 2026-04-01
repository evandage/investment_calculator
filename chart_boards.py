"""K 线看板：日线 EMA+RSI、15m/5m VWAP+RSI。

行情源：A 股（.SS/.SZ）与常用美股 ETF（VOO/QQQ/TLT）优先东方财富（国内访问快，与页内腾讯/新浪现价同源生态）；
其余美股等仍走 yfinance（Yahoo）。
环境变量：YFINANCE_CHART_TIMEOUT（默认 90）、YFINANCE_CHART_RETRIES（默认 4）、
YFINANCE_MIN_GAP_SECONDS（两次 yfinance 请求最小间隔，默认 15，减轻限流）、
MARKET_SYNC_MIN_SECONDS（K 线增量同步最小间隔，默认 180）。
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import timedelta
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
_HTTP_TIMEOUT = (5, 45)

_YF_TIMEOUT = float(os.environ.get("YFINANCE_CHART_TIMEOUT", "90"))
_YF_RETRIES = max(1, int(os.environ.get("YFINANCE_CHART_RETRIES", "4")))
_YF_MIN_GAP = float(os.environ.get("YFINANCE_MIN_GAP_SECONDS", "15"))
_YF_LAST_MONO = 0.0
_YF_PACE_LOCK = threading.Lock()
_SUPABASE_CONF: dict[str, str] | None = None
_SUPABASE_SESSION: requests.Session | None = None
_SUPABASE_READ_ONLY = bool(int(os.environ.get("SUPABASE_READ_ONLY", "0")))
_LAST_SYNC_AT: dict[tuple[str, str], float] = {}
_SYNC_MIN_SECONDS = max(60, int(os.environ.get("MARKET_SYNC_MIN_SECONDS", "180")))

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
    "TLT": "105.TLT",
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
            fillcolor=theme["up_fill"],
        ),
        "decreasing": dict(
            line=dict(color=theme["dn_line"], width=0.85),
            fillcolor=theme["dn_fill"],
        ),
        "whiskerwidth": 0.38,
    }


def _apply_chart_theme(fig: go.Figure, theme: dict[str, Any]) -> None:
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
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=theme["hover_bg"],
            bordercolor=theme["hover_border"],
            font_size=11,
            font_family=_CH_FONT_FAMILY,
        ),
        margin=dict(l=52, r=18, t=18, b=44),
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
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid"],
        zeroline=False,
        showline=False,
        tickfont=dict(size=10, color=theme["muted"]),
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
    secid = _eastmoney_secid(symbol)
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
                "Open": base["Open"],
                "High": base["High"],
                "Low": base["Low"],
                "Close": base["Close"],
                "Volume": base["Volume"],
            },
            index=pd.DatetimeIndex(base["ts"], name="Datetime"),
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


def slice_intraday_today_or_yesterday(df: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame, str]:
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
    out = df.loc[mask].copy()
    out.index = idx_local[mask]
    return out, note


def fetch_ohlcv(
    symbol: str,
    interval: Literal["1d", "15m", "5m"],
    period: str,
    *,
    cache_only: bool = False,
) -> pd.DataFrame:
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
        return _load_bars_from_supabase(symbol, interval, since_utc=since_utc, limit=lim)

    if _SUPABASE_CONF:
        since_utc = _since_utc_for_period(period)
        lim = _SUPABASE_READ_LIMIT.get(interval, 2500)
        cached = _load_bars_from_supabase(symbol, interval, since_utc=since_utc, limit=lim)
        if _SUPABASE_READ_ONLY:
            return cached
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
                secid = _eastmoney_secid(symbol)
                em_lmt = _eastmoney_incremental_lmt(symbol, interval, mx) if secid else None
                yf_start = _yfinance_incremental_start(symbol, mx, interval) if secid is None else None
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
            return cached

    direct, _ = _fetch_from_source(symbol, interval, period)
    return direct


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _volume_bar_colors(df: pd.DataFrame, theme: dict[str, Any]) -> list[str]:
    return [
        theme["vol_up"] if float(c) >= float(o) else theme["vol_dn"]
        for c, o in zip(df["Close"], df["Open"])
    ]


def _volume_profile_by_price(df: pd.DataFrame, bins: int = 24) -> tuple[pd.Series, pd.Series]:
    """按价格分箱聚合成交量，返回(箱中位价格, 对应成交量)。"""
    if df.empty:
        e = pd.Series(dtype=float)
        return e, e
    low = float(df["Low"].min())
    high = float(df["High"].max())
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        e = pd.Series(dtype=float)
        return e, e

    tp = ((df["High"] + df["Low"] + df["Close"]) / 3.0).astype(float)
    vol = df["Volume"].fillna(0.0).astype(float)
    edges = np.linspace(low, high, bins + 1)
    cats = pd.cut(tp, bins=edges, include_lowest=True, duplicates="drop")
    if cats.isna().all():
        e = pd.Series(dtype=float)
        return e, e
    by_bin = vol.groupby(cats, observed=False).sum()
    mids = by_bin.index.map(lambda itv: float((itv.left + itv.right) / 2.0))
    return pd.Series(mids, index=by_bin.index), by_bin.astype(float)


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
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "1d", "5y", cache_only=cache_only)
    if df.empty or len(df) < 50:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无足够日线数据（可检查网络或标的代码）",
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
        if cost <= 0:
            return False
        # 防止异常成本线把价格轴拉爆，导致K线“看不见”。
        return (p_min * 0.5) <= cost <= (p_max * 1.5)

    e20, e50, e100, e200 = ema(c, 20), ema(c, 50), ema(c, 100), ema(c, 200)
    r14 = rsi(c, 14)
    atr_v = atr_series(df, 14)
    upper = c + 1.0 * atr_v
    lower = c - 1.0 * atr_v
    m_line, m_sig, m_hist = macd_series(c)

    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df, theme)

    # 默认展示最近 N 根「交易日」K 线（非日历天），避免周末空白导致蜡烛挤成一坨
    _n_daily_visible = 15
    vis_df = df.tail(_n_daily_visible) if len(df) >= _n_daily_visible else df
    _x_start = vis_df.index.min()
    _x_end = vis_df.index.max()
    _x_pad = pd.Timedelta(days=0.45)

    vp_price, vp_vol = _volume_profile_by_price(vis_df, bins=26)

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
            name="K线",
            **_candlestick_kwargs(theme),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

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
            text=f"成本 {cost:.4g}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    for name, s, color in zip(("EMA20", "EMA50", "EMA100", "EMA200"), (e20, e50, e100, e200), theme["ema"]):
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
            y=upper,
            name="收盘+1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_upper"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=lower,
            name="收盘−1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_lower"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Bar(
            x=df.index,
            y=vol,
            name="成交量",
            marker_color=vcols,
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
                orientation="h",
                name="价位成交量",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig, theme)
    v_vis = vis_df["Volume"].fillna(0.0).astype(float)
    vmax_vis = float(v_vis.max()) if len(v_vis) else 0.0
    y_lo = float(vis_df["Low"].min())
    y_hi = float(vis_df["High"].max())
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        y_lo = min(y_lo, float(user_avg_cost))
        y_hi = max(y_hi, float(user_avg_cost))
    atr_vis = atr_v.reindex(vis_df.index).dropna()
    y_pad = float(atr_vis.iloc[-1]) if len(atr_vis) else max((y_hi - y_lo) * 0.06, 1e-9)
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=[y_lo - y_pad, y_hi + y_pad],
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
        m_line.reindex(vis_df.index),
        m_sig.reindex(vis_df.index),
        m_hist.reindex(vis_df.index),
    )
    fig.update_yaxes(title_text="MACD", row=3, col=1, title_standoff=8, range=macd_range)
    # 横轴：最近 N 个交易日 + 少量边距，K 线更易辨认
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], row=1, col=1)
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], row=2, col=1)
    fig.update_xaxes(range=[_x_start - _x_pad, _x_end + _x_pad], row=3, col=1)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig


def fig_15m_vwap_rsi(
    symbol: str,
    display_name: str,
    *,
    chart_theme: str = "Classic Light",
    user_avg_cost: float | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "15m", "2d", cache_only=cache_only)
    df, _ = slice_intraday_today_or_yesterday(df, symbol)
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无 15 分钟数据（yfinance 对部分标的/时段有限制）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme["muted"]),
        )
        _apply_chart_theme(fig, theme)
        return fig

    vw, v_hi, v_lo = vwap_and_bands(df)
    cl = df["Close"]
    last_close = float(cl.iloc[-1]) if len(cl) else 0.0
    p_min = float(df["Low"].min())
    p_max = float(df["High"].max())

    def _cost_visible(cost: float) -> bool:
        if cost <= 0:
            return False
        return (p_min * 0.5) <= cost <= (p_max * 1.5)

    r14 = rsi(cl, 14)
    r14_ma = ema(r14, 9)
    atr_v = atr_series(df, 14)
    upper = cl + 1.0 * atr_v
    lower = cl - 1.0 * atr_v
    m_line, m_sig, m_hist = macd_series(cl)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df, theme)

    vp_price, vp_vol = _volume_profile_by_price(df, bins=24)

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
            name="K线",
            **_candlestick_kwargs(theme),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

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
            x=df.index.max(),
            y=cost,
            xref="x1",
            yref="y1",
            text=f"成本 {cost:.4g}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            name="VWAP+1σ",
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
            name="VWAP−1σ",
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
            name="VWAP",
            line=dict(color=theme["vwap"], width=1.35),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=upper,
            name="收盘+1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_upper"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=lower,
            name="收盘−1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_lower"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=vol,
            name="成交量",
            marker_color=vcols,
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
                orientation="h",
                name="价位成交量",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig, theme)
    vmax = float(vol.max()) if len(vol) else 0.0
    y_lo = float(df["Low"].min())
    y_hi = float(df["High"].max())
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        y_lo = min(y_lo, float(user_avg_cost))
        y_hi = max(y_hi, float(user_avg_cost))
    y_pad = float(atr_v.iloc[-1]) if len(atr_v.dropna()) else max((y_hi - y_lo) * 0.08, 1e-9)
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=[y_lo - y_pad, y_hi + y_pad],
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
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig


def fig_5m_vwap_rsi7(
    symbol: str,
    display_name: str,
    *,
    chart_theme: str = "Classic Light",
    user_avg_cost: float | None = None,
    cache_only: bool = False,
) -> go.Figure:
    theme = get_chart_theme(chart_theme)
    df = fetch_ohlcv(symbol, "5m", "2d", cache_only=cache_only)
    df, _ = slice_intraday_today_or_yesterday(df, symbol)
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无 5 分钟数据（yfinance 对部分标的/时段有限制）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=theme["muted"]),
        )
        _apply_chart_theme(fig, theme)
        return fig

    vw, v_hi, v_lo = vwap_and_bands(df)
    cl = df["Close"]
    last_close = float(cl.iloc[-1]) if len(cl) else 0.0
    p_min = float(df["Low"].min())
    p_max = float(df["High"].max())

    def _cost_visible(cost: float) -> bool:
        if cost <= 0:
            return False
        return (p_min * 0.5) <= cost <= (p_max * 1.5)

    r7 = rsi(cl, 7)
    r7_ma = ema(r7, 9)
    atr_v = atr_series(df, 14)
    upper = cl + 1.0 * atr_v
    lower = cl - 1.0 * atr_v
    m_line, m_sig, m_hist = macd_series(cl)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df, theme)

    vp_price, vp_vol = _volume_profile_by_price(df, bins=24)

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
            name="K线",
            **_candlestick_kwargs(theme),
        ),
        row=1,
        col=1,
        secondary_y=False,
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
            text=f"成本 {cost:.4g}（{pct:+.2f}%）",
            showarrow=False,
            font=dict(color=theme["rsi_orange"], size=11),
            bgcolor="rgba(0,0,0,0.08)",
        )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            name="VWAP+1σ",
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
            name="VWAP−1σ",
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
            name="VWAP",
            line=dict(color=theme["vwap"], width=1.35),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=upper,
            name="收盘+1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_upper"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=lower,
            name="收盘−1ATR",
            line=dict(width=1, dash="dot", color=theme["atr_lower"]),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=vol,
            name="成交量",
            marker_color=vcols,
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
                orientation="h",
                name="价位成交量",
                marker_color=theme["vp_bar"],
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=980,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig, theme)
    vmax = float(vol.max()) if len(vol) else 0.0
    y_lo = float(df["Low"].min())
    y_hi = float(df["High"].max())
    if user_avg_cost is not None and _cost_visible(float(user_avg_cost)):
        y_lo = min(y_lo, float(user_avg_cost))
        y_hi = max(y_hi, float(user_avg_cost))
    y_pad = float(atr_v.iloc[-1]) if len(atr_v.dropna()) else max((y_hi - y_lo) * 0.08, 1e-9)
    fig.update_yaxes(
        title_text="价格",
        row=1,
        col=1,
        title_standoff=8,
        secondary_y=False,
        range=[y_lo - y_pad, y_hi + y_pad],
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
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig
