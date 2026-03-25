"""K 线看板：日线 EMA+RSI、15m/5m VWAP+RSI。

行情源：A 股（.SS/.SZ）与常用美股 ETF（VOO/QQQ/TLT）优先东方财富（国内访问快，与页内腾讯/新浪现价同源生态）；
其余美股等仍走 yfinance（Yahoo）。
环境变量：YFINANCE_CHART_TIMEOUT（默认 90）、YFINANCE_CHART_RETRIES（默认 4）。
"""

from __future__ import annotations

import logging
import os
import time
from datetime import timedelta
from typing import Literal
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

# 东财美股 secid 与行情中心分类一致（与 Yahoo  ticker 不同前缀）
_EASTMONEY_US_SECID = {
    "VOO": "107.VOO",
    "QQQ": "105.QQQ",
    "TLT": "105.TLT",
}

# --- 简约图表主题：浅底、低饱和、细线 ---
_CH_FONT = "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif"
_CH_PAPER = "#f6f7f9"
_CH_GRID = "rgba(100, 116, 139, 0.11)"
_CH_MUTED = "#64748b"
# 涨 / 跌（与 A 股常见红涨绿跌一致，饱和度压低）
_CH_UP_LINE = "#a63d3d"
_CH_UP_FILL = "rgba(166, 61, 61, 0.22)"
_CH_DN_LINE = "#3f6b5c"
_CH_DN_FILL = "rgba(63, 107, 92, 0.22)"
_CH_EMA_COLORS = ("#b45309", "#475569", "#6d5f8a", "#94a3b8")
_CH_VWAP = "#0d9488"
_CH_VWAP_BAND = "rgba(13, 148, 136, 0.42)"
_CH_VWAP_FILL = "rgba(13, 148, 136, 0.055)"
_CH_RSI = "#6366f1"
_CH_RSI_MA = "#475569"
_CH_RSI_ORANGE = "#b45309"
_CH_REF_LINE = "rgba(148, 163, 184, 0.55)"


def _candlestick_kwargs() -> dict:
    return {
        "increasing": dict(
            line=dict(color=_CH_UP_LINE, width=0.85),
            fillcolor=_CH_UP_FILL,
        ),
        "decreasing": dict(
            line=dict(color=_CH_DN_LINE, width=0.85),
            fillcolor=_CH_DN_FILL,
        ),
        "whiskerwidth": 0.38,
    }


def _apply_chart_theme(fig: go.Figure) -> None:
    fig.update_layout(
        paper_bgcolor=_CH_PAPER,
        plot_bgcolor=_CH_PAPER,
        font=dict(family=_CH_FONT, size=11, color=_CH_MUTED),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.006,
            xanchor="right",
            x=1,
            bgcolor="rgba(246, 247, 249, 0.88)",
            borderwidth=0,
            font=dict(size=10, color=_CH_MUTED),
            itemsizing="constant",
            itemwidth=30,
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="rgba(255, 255, 255, 0.96)",
            bordercolor="rgba(148, 163, 184, 0.35)",
            font_size=11,
            font_family=_CH_FONT,
        ),
        margin=dict(l=52, r=18, t=18, b=44),
    )
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=_CH_GRID,
        zeroline=False,
        showline=False,
        tickfont=dict(size=10, color=_CH_MUTED),
        showspikes=True,
        spikecolor="rgba(100, 116, 139, 0.25)",
        spikemode="across",
        spikesnap="cursor",
        spikedash="solid",
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=_CH_GRID,
        zeroline=False,
        showline=False,
        tickfont=dict(size=10, color=_CH_MUTED),
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


def _fetch_eastmoney_ohlcv(
    secid: str,
    interval: Literal["1d", "15m", "5m"],
) -> pd.DataFrame:
    """东方财富 K 线：日期/时间,开,收,高,低,量,额。"""
    klt = {"1d": "101", "15m": "15", "5m": "5"}[interval]
    lmt = "1500" if interval == "1d" else "2000"
    params = {
        "secid": secid,
        "klt": klt,
        "fqt": "1",
        "lmt": lmt,
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
            try:
                df = yf.download(
                    symbol,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=True,
                    threads=False,
                    timeout=_YF_TIMEOUT,
                )
            except BaseException as e:
                last_exc = e
                df = pd.DataFrame()
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
) -> pd.DataFrame:
    secid = _eastmoney_secid(symbol)
    if secid is not None:
        df = _fetch_eastmoney_ohlcv(secid, interval)
        if not df.empty:
            if not _eastmoney_secid_is_cn(secid):
                df = _adjust_eastmoney_us_index(df, interval)
            return df
    return _fetch_yfinance_ohlcv(symbol, interval, period)


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _volume_bar_colors(df: pd.DataFrame) -> list[str]:
    """与 K 线同色相、略提高透明度。"""
    up = "rgba(166, 61, 61, 0.38)"
    down = "rgba(63, 107, 92, 0.38)"
    return [up if float(c) >= float(o) else down for c, o in zip(df["Close"], df["Open"])]


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


def fig_daily(symbol: str, display_name: str) -> go.Figure:
    df = fetch_ohlcv(symbol, "1d", "5y")
    if df.empty or len(df) < 50:
        fig = go.Figure()
        fig.add_annotation(
            text="暂无足够日线数据（可检查网络或标的代码）",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
        return fig

    c = df["Close"]
    e20, e50, e100, e200 = ema(c, 20), ema(c, 50), ema(c, 100), ema(c, 200)
    r14 = rsi(c, 14)

    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df)

    vp_price, vp_vol = _volume_profile_by_price(df, bins=26)

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.04,
        horizontal_spacing=0.02,
        row_heights=[0.72, 0.28],
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
            **_candlestick_kwargs(),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    for name, s, color in zip(
        ("EMA20", "EMA50", "EMA100", "EMA200"),
        (e20, e50, e100, e200),
        _CH_EMA_COLORS,
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
            line=dict(color=_CH_RSI, width=1.2),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color="#ef4444", row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color="#3b82f6", row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=_CH_REF_LINE,
        row=2,
        col=1,
    )
    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                orientation="h",
                name="价位成交量",
                marker_color="rgba(99, 102, 241, 0.35)",
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=860,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig)
    vmax = float(vol.max()) if len(vol) else 0.0
    fig.update_yaxes(title_text="价格", row=1, col=1, title_standoff=8, secondary_y=False)
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax * 4 if vmax > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig


def fig_15m_vwap_rsi(symbol: str, display_name: str) -> go.Figure:
    df = fetch_ohlcv(symbol, "15m", "60d")
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
        )
        return fig

    vw, v_hi, v_lo = vwap_and_bands(df)
    r14 = rsi(df["Close"], 14)
    r14_ma = ema(r14, 9)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df)

    vp_price, vp_vol = _volume_profile_by_price(df, bins=24)

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.04,
        horizontal_spacing=0.02,
        row_heights=[0.72, 0.28],
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
            **_candlestick_kwargs(),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            name="VWAP+1σ",
            line=dict(color=_CH_VWAP_BAND, width=1, dash="dot"),
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
            line=dict(color=_CH_VWAP_BAND, width=1, dash="dot"),
            fill="tonexty",
            fillcolor=_CH_VWAP_FILL,
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
            line=dict(color=_CH_VWAP, width=1.35),
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
            line=dict(color=_CH_RSI, width=1.15),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r14_ma,
            name="RSI EMA(9)",
            line=dict(color=_CH_RSI_MA, width=1.1, dash="solid"),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color="#ef4444", row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color="#3b82f6", row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=_CH_REF_LINE,
        row=2,
        col=1,
    )
    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                orientation="h",
                name="价位成交量",
                marker_color="rgba(99, 102, 241, 0.35)",
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=860,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig)
    vmax = float(vol.max()) if len(vol) else 0.0
    fig.update_yaxes(title_text="价格", row=1, col=1, title_standoff=8, secondary_y=False)
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax * 4 if vmax > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig


def fig_5m_vwap_rsi7(symbol: str, display_name: str) -> go.Figure:
    df = fetch_ohlcv(symbol, "5m", "60d")
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
        )
        return fig

    vw, v_hi, v_lo = vwap_and_bands(df)
    r7 = rsi(df["Close"], 7)
    r7_ma = ema(r7, 9)
    vol = df["Volume"].fillna(0.0).astype(float)
    vcols = _volume_bar_colors(df)

    vp_price, vp_vol = _volume_profile_by_price(df, bins=24)

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[[{"secondary_y": True}, {}], [{"colspan": 2}, None]],
        shared_yaxes=True,
        vertical_spacing=0.04,
        horizontal_spacing=0.02,
        row_heights=[0.72, 0.28],
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
            **_candlestick_kwargs(),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=v_hi,
            name="VWAP+1σ",
            line=dict(color=_CH_VWAP_BAND, width=1, dash="dot"),
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
            line=dict(color=_CH_VWAP_BAND, width=1, dash="dot"),
            fill="tonexty",
            fillcolor=_CH_VWAP_FILL,
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
            line=dict(color=_CH_VWAP, width=1.35),
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
            line=dict(color=_CH_RSI_ORANGE, width=1.15),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=r7_ma,
            name="RSI EMA(9)",
            line=dict(color=_CH_RSI_MA, width=1.1, dash="solid"),
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_width=1.6, line_color="#ef4444", row=2, col=1)
    fig.add_hline(y=35, line_dash="dash", line_width=1.6, line_color="#3b82f6", row=2, col=1)
    fig.add_hline(
        y=30,
        line_dash="dot",
        line_width=1,
        line_color=_CH_REF_LINE,
        row=2,
        col=1,
    )
    if not vp_vol.empty:
        fig.add_trace(
            go.Bar(
                x=vp_vol.values,
                y=vp_price.values,
                orientation="h",
                name="价位成交量",
                marker_color="rgba(99, 102, 241, 0.35)",
                marker_line_width=0,
                showlegend=False,
                hovertemplate="价格: %{y:.4f}<br>量: %{x:,.0f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        height=860,
        xaxis_rangeslider_visible=False,
        barmode="overlay",
    )
    _apply_chart_theme(fig)
    vmax = float(vol.max()) if len(vol) else 0.0
    fig.update_yaxes(title_text="价格", row=1, col=1, title_standoff=8, secondary_y=False)
    fig.update_yaxes(
        row=1,
        col=1,
        secondary_y=True,
        showgrid=False,
        visible=False,
        range=[0, vmax * 4 if vmax > 0 else 1],
    )
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100], title_standoff=8)
    fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    return fig
