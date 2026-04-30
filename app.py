import re
import json
import base64
import importlib
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
    initial_sidebar_state="collapsed",
)

# 拉取失败时的回退价（与常见区间一致）
_FALLBACK = {
    "VOO": 400.0,
    "AVGO": 1700.0,
    "NVDA": 1200.0,
    "GOOGL": 180.0,
    "MSFT": 420.0,
    "TLT": 90.0,
    "IEI": 115.0,
    "001015": 1.0,
    "007994": 1.0,
}

_TICKERS = {
    "voo": "VOO",
    "avgo": "AVGO",
    "nvda": "NVDA",
    "googl": "GOOGL",
    "msft": "MSFT",
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
_QQ_US = {
    "VOO": "usVOO",
    "AVGO": "usAVGO",
    "NVDA": "usNVDA",
    "GOOGL": "usGOOGL",
    "MSFT": "usMSFT",
    "TLT": "usTLT",
    "IEI": "usIEI",
}
_SINA_GB = {
    "VOO": "gb_voo",
    "AVGO": "gb_avgo",
    "NVDA": "gb_nvda",
    "GOOGL": "gb_googl",
    "MSFT": "gb_msft",
    "TLT": "gb_tlt",
    "IEI": "gb_iei",
}
_FUND_CODES = {"001015": "001015", "007994": "007994"}

_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "AVGO": {"label": "AVGO", "currency": "USD"},
    "NVDA": {"label": "NVDA", "currency": "USD"},
    "GOOGL": {"label": "GOOGL", "currency": "USD"},
    "MSFT": {"label": "MSFT", "currency": "USD"},
    "TLT": {"label": "债券(TLT)", "currency": "USD"},
    "IEI": {"label": "债券(IEI)", "currency": "USD"},
    "001015": {"label": "华夏沪深300指数增强A(001015)", "currency": "CNY"},
    "007994": {"label": "华夏中证500指数增强(007994)", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    # 目标比例：
    # 美元资产: VOO 20%, 卫星仓位四标的合计20%（AVGO/NVDA/GOOGL/MSFT=3:2:3:2）, 债券 20%（TLT/IEI各10%）
    # 人民币资产: 沪深300(001015) 20%, 中证500(007994) 20%
    "VOO": 0.20,
    "AVGO": 0.06,
    "NVDA": 0.04,
    "GOOGL": 0.06,
    "MSFT": 0.04,
    "TLT": 0.10,
    "IEI": 0.10,
    "001015": 0.20,
    "007994": 0.20,
}

_SATELLITE_SYMBOLS = ("AVGO", "NVDA", "GOOGL", "MSFT")

# 美元资产 PE 参考区间（经验口径，仅作辅助，不构成投资建议）
_USD_ASSET_PE_BANDS: dict[str, tuple[float, float]] = {
    "VOO": (18.0, 24.0),
    "AVGO": (24.0, 34.0),
    "NVDA": (28.0, 45.0),
    "GOOGL": (18.0, 28.0),
    "MSFT": (24.0, 36.0),
    "TLT": (14.0, 24.0),
    "IEI": (12.0, 22.0),
}

_DCA_DRAWDOWN_RULES: dict[str, tuple[tuple[float, str], ...]] = {
    "VOO": ((-15.0, "明显加大，可用部分备用资金"), (-10.0, "明显加大"), (-5.0, "多买一点")),
    "MSFT": ((-25.0, "重加"), (-15.0, "中加"), (-8.0, "小加")),
    "GOOGL": ((-25.0, "重加"), (-18.0, "中加"), (-10.0, "小加")),
    "NVDA": ((-30.0, "重加"), (-20.0, "中加"), (-12.0, "小加")),
    "AVGO": ((-28.0, "重加"), (-18.0, "中加"), (-10.0, "小加")),
}

_DCA_DRAWDOWN_RULE_TEXT = (
    "回撤定投规则：VOO 参考 S&P 500：-5% 多买一点、-10% 明显加大、-15% 可用部分备用资金；"
    "卫星仓位按个股阈值：MSFT -8/-15/-25，GOOGL -10/-18/-25，NVDA -12/-20/-30，AVGO -10/-18/-28。"
)


_TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
_UI_THEMES = {
    "浅色：绿跌红涨": {
        "delta_color": "normal",
        "up_base": "#7f1d1d",
        "up_peak": "#ef4444",
        "down_base": "#14532d",
        "down_peak": "#22c55e",
        "accent": "#2563eb",
        "text": "#0f172a",
        "muted_text": "#475569",
        "page_bg": "#f8fafc",
        "app_bg": "linear-gradient(135deg, rgba(248, 250, 252, 0.96), rgba(239, 246, 255, 0.92) 48%, rgba(255, 247, 237, 0.9)), repeating-linear-gradient(90deg, rgba(148, 163, 184, 0.08) 0 1px, transparent 1px 48px), repeating-linear-gradient(0deg, rgba(148, 163, 184, 0.06) 0 1px, transparent 1px 48px)",
        "header_bg": "rgba(248, 250, 252, 0.76)",
        "sidebar_bg": "rgba(248, 250, 252, 0.92)",
        "card_bg": "rgba(255, 255, 255, 0.78)",
        "panel_bg": "rgba(255, 255, 255, 0.72)",
        "border": "rgba(148, 163, 184, 0.22)",
        "shadow": "0 14px 32px rgba(15, 23, 42, 0.08)",
        "hover_shadow": "0 18px 38px rgba(15, 23, 42, 0.12)",
        "button_bg": "linear-gradient(135deg, #2563eb 0%, #0ea5e9 52%, #f59e0b 100%)",
        "button_text": "#ffffff",
        "button_border": "rgba(255, 255, 255, 0.18)",
        "button_shadow": "0 8px 22px rgba(37, 99, 235, 0.28)",
        "control_bg": "rgba(255, 255, 255, 0.86)",
    },
    "浅色：绿涨红跌": {
        "delta_color": "normal",
        "up_base": "#166534",
        "up_peak": "#22c55e",
        "down_base": "#991b1b",
        "down_peak": "#ef4444",
        "accent": "#7c3aed",
        "text": "#0f172a",
        "muted_text": "#475569",
        "page_bg": "#f8fafc",
        "app_bg": "linear-gradient(135deg, rgba(248, 250, 252, 0.96), rgba(245, 243, 255, 0.92) 48%, rgba(236, 253, 245, 0.88)), repeating-linear-gradient(90deg, rgba(148, 163, 184, 0.08) 0 1px, transparent 1px 48px), repeating-linear-gradient(0deg, rgba(148, 163, 184, 0.06) 0 1px, transparent 1px 48px)",
        "header_bg": "rgba(248, 250, 252, 0.76)",
        "sidebar_bg": "rgba(248, 250, 252, 0.92)",
        "card_bg": "rgba(255, 255, 255, 0.78)",
        "panel_bg": "rgba(255, 255, 255, 0.72)",
        "border": "rgba(148, 163, 184, 0.22)",
        "shadow": "0 14px 32px rgba(15, 23, 42, 0.08)",
        "hover_shadow": "0 18px 38px rgba(15, 23, 42, 0.12)",
        "button_bg": "linear-gradient(135deg, #7c3aed 0%, #0ea5e9 52%, #22c55e 100%)",
        "button_text": "#ffffff",
        "button_border": "rgba(255, 255, 255, 0.18)",
        "button_shadow": "0 8px 22px rgba(124, 58, 237, 0.28)",
        "control_bg": "rgba(255, 255, 255, 0.86)",
    },
    "深色：绿跌红涨": {
        "delta_color": "normal",
        "up_base": "#fecaca",
        "up_peak": "#f87171",
        "down_base": "#bbf7d0",
        "down_peak": "#4ade80",
        "accent": "#38bdf8",
        "text": "#e5edf8",
        "muted_text": "#a8b4c7",
        "page_bg": "#07111f",
        "app_bg": "linear-gradient(135deg, #07111f 0%, #111827 48%, #1f2937 100%), repeating-linear-gradient(90deg, rgba(148, 163, 184, 0.08) 0 1px, transparent 1px 48px), repeating-linear-gradient(0deg, rgba(148, 163, 184, 0.06) 0 1px, transparent 1px 48px)",
        "header_bg": "rgba(7, 17, 31, 0.78)",
        "sidebar_bg": "rgba(15, 23, 42, 0.94)",
        "card_bg": "rgba(15, 23, 42, 0.78)",
        "panel_bg": "rgba(15, 23, 42, 0.68)",
        "border": "rgba(148, 163, 184, 0.26)",
        "shadow": "0 18px 42px rgba(0, 0, 0, 0.28)",
        "hover_shadow": "0 22px 48px rgba(0, 0, 0, 0.36)",
        "button_bg": "linear-gradient(135deg, #38bdf8 0%, #6366f1 52%, #f97316 100%)",
        "button_text": "#f8fafc",
        "button_border": "rgba(226, 232, 240, 0.22)",
        "button_shadow": "0 10px 26px rgba(56, 189, 248, 0.22)",
        "control_bg": "rgba(15, 23, 42, 0.78)",
    },
    "深色：绿涨红跌": {
        "delta_color": "normal",
        "up_base": "#bbf7d0",
        "up_peak": "#4ade80",
        "down_base": "#fecaca",
        "down_peak": "#f87171",
        "accent": "#a78bfa",
        "text": "#e5edf8",
        "muted_text": "#a8b4c7",
        "page_bg": "#07111f",
        "app_bg": "linear-gradient(135deg, #07111f 0%, #111827 48%, #182235 100%), repeating-linear-gradient(90deg, rgba(148, 163, 184, 0.08) 0 1px, transparent 1px 48px), repeating-linear-gradient(0deg, rgba(148, 163, 184, 0.06) 0 1px, transparent 1px 48px)",
        "header_bg": "rgba(7, 17, 31, 0.78)",
        "sidebar_bg": "rgba(15, 23, 42, 0.94)",
        "card_bg": "rgba(15, 23, 42, 0.78)",
        "panel_bg": "rgba(15, 23, 42, 0.68)",
        "border": "rgba(148, 163, 184, 0.26)",
        "shadow": "0 18px 42px rgba(0, 0, 0, 0.28)",
        "hover_shadow": "0 22px 48px rgba(0, 0, 0, 0.36)",
        "button_bg": "linear-gradient(135deg, #a78bfa 0%, #38bdf8 52%, #22c55e 100%)",
        "button_text": "#f8fafc",
        "button_border": "rgba(226, 232, 240, 0.22)",
        "button_shadow": "0 10px 26px rgba(167, 139, 250, 0.22)",
        "control_bg": "rgba(15, 23, 42, 0.78)",
    },
}


def _auto_theme_name(now: datetime | None = None) -> str:
    current = now or datetime.now(_TZ_SHANGHAI)
    is_daytime = 7 <= current.hour < 19
    return "浅色：绿涨红跌" if is_daytime else "深色：绿涨红跌"


def _apply_theme_css(theme: dict[str, str]) -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
        
        .stApp {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --app-text: {theme["text"]};
            --app-muted-text: {theme["muted_text"]};
            --app-card-bg: {theme["card_bg"]};
            --app-panel-bg: {theme["panel_bg"]};
            --app-border: {theme["border"]};
            --app-shadow: {theme["shadow"]};
            --app-hover-shadow: {theme["hover_shadow"]};
            --app-control-bg: {theme["control_bg"]};
            color: var(--app-text);
            background: {theme["app_bg"]};
            background-color: {theme.get("page_bg", "#f8fafc")};
        }}

        .stApp [data-testid="stAppViewContainer"],
        .stApp main,
        .stApp p,
        .stApp label,
        .stApp span,
        .stApp div {{
            color: inherit;
        }}

        .block-container {{
            max-width: 1180px;
            padding-top: 3.25rem;
            padding-bottom: 2rem;
        }}

        [data-testid="stHeader"] {{
            background: {theme["header_bg"]};
            backdrop-filter: blur(16px);
        }}

        [data-testid="stSidebar"] {{
            background: {theme["sidebar_bg"]};
            border-right: 1px solid var(--app-border);
            color: var(--app-text);
        }}
        
        /* Modern metric cards */
        [data-testid="stMetric"] {{
            position: relative;
            overflow: hidden;
            min-height: 116px;
            background:
                linear-gradient(var(--app-card-bg), var(--app-card-bg)) padding-box,
                linear-gradient(135deg, rgba(37, 99, 235, 0.34), rgba(245, 158, 11, 0.22)) border-box;
            border: 1px solid transparent;
            border-radius: 8px;
            padding: 15px 16px;
            box-shadow: var(--app-shadow);
            text-align: center;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        [data-testid="stMetric"]:hover {{
            transform: translateY(-2px);
            box-shadow: var(--app-hover-shadow);
        }}
        [data-testid="stMetricValue"] {{
            font-size: clamp(1.25rem, 2.5vw, 1.8rem);
            font-weight: 800;
            color: var(--app-text);
            line-height: 1.1;
        }}
        [data-testid="stMetricLabel"] {{
            font-weight: 600;
            color: var(--app-muted-text);
            opacity: 0.92;
            font-size: 0.95rem;
        }}
        
        /* Button styling */
        .stButton > button,
        .stDownloadButton > button,
        [data-testid="stButton"] button,
        [data-testid="stDownloadButton"] button {{
            border-radius: 8px;
            border: 1px solid {theme["button_border"]};
            background: {theme["button_bg"]};
            color: {theme["button_text"]} !important;
            font-weight: 600;
            padding: 0.5rem 1.5rem;
            box-shadow: {theme["button_shadow"]};
            text-shadow: 0 1px 1px rgba(0, 0, 0, 0.18);
            transition: all 0.2s ease;
        }}
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        [data-testid="stButton"] button:hover,
        [data-testid="stDownloadButton"] button:hover {{
            transform: scale(1.02);
            border-color: {theme["button_border"]};
            color: {theme["button_text"]} !important;
            box-shadow: var(--app-hover-shadow);
        }}

        .stButton > button *,
        .stDownloadButton > button *,
        [data-testid="stButton"] button *,
        [data-testid="stButton"] button p,
        [data-testid="stButton"] button span,
        [data-testid="stDownloadButton"] button *,
        [data-testid="stDownloadButton"] button p,
        [data-testid="stDownloadButton"] button span {{
            color: {theme["button_text"]} !important;
            -webkit-text-fill-color: {theme["button_text"]} !important;
        }}
        
        /* Expander / container styling */
        [data-testid="stExpander"] {{
            background: var(--app-card-bg);
            border-radius: 8px;
            border: 1px solid var(--app-border);
            box-shadow: var(--app-shadow);
            overflow: hidden;
            margin-bottom: 1rem;
        }}
        [data-testid="stExpander"] summary {{
            font-weight: 700;
            font-size: 1.1rem;
            color: var(--app-text);
        }}
        
        /* Dataframes */
        [data-testid="stDataFrame"] {{
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid var(--app-border);
            box-shadow: var(--app-shadow);
        }}

        .stTextInput input,
        .stNumberInput input,
        .stSelectbox [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div,
        .stDateInput input,
        .stTextArea textarea {{
            background: var(--app-control-bg) !important;
            color: var(--app-text) !important;
            border-color: var(--app-border) !important;
        }}

        .daily-summary {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            margin: 0.25rem 0 0.85rem;
            padding: 12px 14px;
            border: 1px solid var(--app-border);
            border-radius: 8px;
            background: var(--app-panel-bg);
            box-shadow: var(--app-shadow);
            color: var(--app-text);
        }}

        .daily-summary-main,
        .daily-summary-values {{
            display: inline-flex;
            align-items: baseline;
            justify-content: center;
            gap: 6px;
        }}

        .daily-summary-values {{
            margin-left: 6px;
        }}

        .daily-summary strong {{
            font-weight: 800;
        }}

        .daily-card-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
            margin-bottom: 0.75rem;
            align-items: stretch;
        }}

        .daily-card {{
            position: relative;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            min-height: 104px;
            border: 1px solid var(--app-border);
            border-radius: 8px;
            padding: 12px;
            background: var(--app-panel-bg);
            box-shadow: var(--app-shadow);
            color: var(--app-text);
            text-align: center;
        }}

        .daily-card::before {{
            content: "";
            position: absolute;
            inset: 0;
            border-top: 3px solid var(--daily-color);
            opacity: 0.85;
            pointer-events: none;
        }}

        .daily-card-title {{
            min-height: 2.4em;
            color: var(--daily-color) !important;
            font-weight: 800;
            font-size: 1.02rem;
            line-height: 1.2;
            word-break: break-word;
        }}

        .daily-card-pct {{
            color: var(--daily-color) !important;
            font-weight: 800;
            font-size: 1.1rem;
            margin-top: 6px;
        }}

        .daily-card-amount {{
            color: var(--daily-color) !important;
            font-weight: 650;
            font-size: 0.83rem;
            line-height: 1.35;
            margin-top: 2px;
        }}
        
        /* Headers */
        h1, h2, h3, h4 {{
            font-weight: 800 !important;
            letter-spacing: 0 !important;
            color: var(--app-text) !important;
        }}

        .stMarkdown,
        .stMarkdown p,
        .stCaptionContainer,
        [data-testid="stMarkdownContainer"] {{
            color: var(--app-text);
        }}
        
        div[data-testid="stCaptionContainer"] p {{
            color: var(--app-muted-text);
        }}

        [data-testid="stVegaLiteChart"],
        [data-testid="stVegaLiteChart"] > div,
        .vega-embed {{
            background: transparent !important;
        }}

        [data-testid="stVegaLiteChart"] {{
            box-sizing: border-box;
            width: 100%;
            max-width: 100%;
            border: 1px solid var(--app-border);
            border-radius: 8px;
            padding: 10px;
            background: var(--app-panel-bg) !important;
            box-shadow: var(--app-shadow);
            overflow: hidden;
        }}

        [data-testid="stVegaLiteChart"] svg {{
            background: transparent !important;
            max-width: 100%;
        }}

        [data-testid="column"] {{
            min-width: 0;
        }}

        @media (max-width: 720px) {{
            .block-container {{
                padding: 3rem 0.75rem 1.5rem;
            }}

            h1 {{
                font-size: 1.55rem !important;
            }}

            h2, h3 {{
                font-size: 1.18rem !important;
            }}

            [data-testid="stMetric"] {{
                min-height: 96px;
                padding: 12px;
            }}

            [data-testid="stMetricLabel"] {{
                font-size: 0.82rem;
            }}

            .daily-summary {{
                display: flex;
                flex-direction: column;
                align-items: center;
                text-align: center;
                gap: 4px;
                padding: 11px 12px;
            }}

            .daily-summary-values {{
                margin-left: 0;
            }}

            .daily-card-grid {{
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 8px;
            }}

            .daily-card {{
                min-height: 112px;
                padding: 10px;
            }}

            .daily-card-title {{
                font-size: 0.95rem;
            }}

            .daily-card-pct {{
                font-size: 1rem;
            }}

            .daily-card-amount {{
                font-size: 0.76rem;
            }}

            .stButton > button, .stDownloadButton > button {{
                width: 100%;
                padding-left: 0.75rem;
                padding-right: 0.75rem;
            }}
        }}

        @media (max-width: 420px) {{
            .daily-card-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _theme_altair_chart(chart: alt.Chart, theme: dict[str, str]) -> alt.Chart:
    return (
        chart.configure(background="transparent")
        .configure_view(stroke=None, fill="transparent")
        .configure_title(color=theme["text"], fontSize=15, fontWeight=700, anchor="start")
        .configure_axis(
            labelColor=theme["muted_text"],
            titleColor=theme["muted_text"],
            gridColor=theme["border"],
            domainColor=theme["border"],
            tickColor=theme["border"],
        )
        .configure_legend(
            labelColor=theme["muted_text"],
            titleColor=theme["muted_text"],
            orient="bottom",
        )
    )


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    s = hex_color.strip().lstrip("#")
    if len(s) != 6:
        return (0, 0, 0)
    try:
        return (int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16))
    except ValueError:
        return (0, 0, 0)


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{max(0, min(255, r)):02x}{max(0, min(255, g)):02x}{max(0, min(255, b)):02x}"


def _lerp_color(hex_a: str, hex_b: str, t: float) -> str:
    t = max(0.0, min(1.0, float(t)))
    ra, ga, ba = _hex_to_rgb(hex_a)
    rb, gb, bb = _hex_to_rgb(hex_b)
    r = int(round(ra + (rb - ra) * t))
    g = int(round(ga + (gb - ga) * t))
    b = int(round(ba + (bb - ba) * t))
    return _rgb_to_hex(r, g, b)


def _change_color_by_pct(pct: float, cap_pct: float = 4.0, theme: dict[str, str] | None = None) -> str:
    """涨跌幅颜色梯度：绝对涨跌越大颜色越亮，越小越暗。"""
    p = float(pct)
    intensity = min(1.0, abs(p) / max(0.1, float(cap_pct)))
    palette = theme or {}
    if p >= 0:
        return _lerp_color(palette.get("up_base", "#166534"), palette.get("up_peak", "#86efac"), intensity)
    return _lerp_color(palette.get("down_base", "#991b1b"), palette.get("down_peak", "#fca5a5"), intensity)


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

    for sym in ("VOO", *_SATELLITE_SYMBOLS, "TLT", "IEI"):
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
    pe_val: float | None = None
    dd_val: float | None = None
    try:
        import yfinance as yf
    except Exception:
        yf = None  # type: ignore[assignment]

    if yf is not None:
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
            pass

    if dd_val is None:
        try:
            end_ts = int(datetime.now(_TZ_SHANGHAI).timestamp())
            start_ts = end_ts - 370 * 24 * 60 * 60
            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                f"?period1={start_ts}&period2={end_ts}&interval=1d&events=history"
            )
            r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
            r.raise_for_status()
            result = (r.json().get("chart", {}).get("result") or [None])[0]
            closes = ((result or {}).get("indicators", {}).get("quote") or [{}])[0].get("close", [])
            if closes:
                c = pd.to_numeric(pd.Series(closes), errors="coerce").dropna()
                if len(c) > 20:
                    win = c.tail(60) if len(c) >= 60 else c
                    peak = float(win.max())
                    last = float(win.iloc[-1])
                    if peak > 0:
                        dd_val = (last / peak - 1.0) * 100.0
        except Exception:
            pass

    return {"pe": pe_val, "drawdown_pct": dd_val}


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_fund_drawdown(code: str) -> float | None:
    """用东方财富历史净值估算近60条记录高点回撤。"""
    url = f"https://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={code}&page=1&per=120"
    try:
        r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
        r.raise_for_status()
        matches = re.findall(r"<tr>\\s*<td>([^<]+)</td>\\s*<td[^>]*>([^<]+)</td>", r.text)
        values: list[float] = []
        for _, nav in matches:
            try:
                values.append(float(nav))
            except (TypeError, ValueError):
                continue
        if len(values) <= 1:
            return None
        win = values[:60]
        peak = max(win)
        last = values[0]
        return (last / peak - 1.0) * 100.0 if peak > 0 else None
    except Exception:
        return None


def _fetch_asset_drawdown(sym: str, meta: dict[str, str]) -> float | None:
    if meta["currency"] == "USD":
        q = _fetch_us_etf_pe_drawdown(sym)
        dd = q.get("drawdown_pct")
        return float(dd) if isinstance(dd, (int, float)) else None
    if sym in _FUND_CODES:
        return _fetch_fund_drawdown(_FUND_CODES[sym])
    return None


def _drawdown_dca_signal(symbol: str, drawdown_pct: float | None) -> str:
    if not isinstance(drawdown_pct, (int, float)):
        return "暂无回撤数据"
    for threshold, label in _DCA_DRAWDOWN_RULES.get(symbol, ()):
        if float(drawdown_pct) <= threshold:
            return label
    return "按计划观察"


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
        "avgo": raw["AVGO"],
        "nvda": raw["NVDA"],
        "googl": raw["GOOGL"],
        "msft": raw["MSFT"],
        "tlt": raw["TLT"],
        "iei": raw["IEI"],
        "hs300": raw["001015"],
        "zz500": raw["007994"],
    }


def _ensure_price_session_defaults() -> None:
    d = _defaults_from_fetch()
    st.session_state.setdefault("def_voo", d["voo"])
    st.session_state.setdefault("def_avgo", d["avgo"])
    st.session_state.setdefault("def_nvda", d["nvda"])
    st.session_state.setdefault("def_googl", d["googl"])
    st.session_state.setdefault("def_msft", d["msft"])
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


@st.cache_resource(show_spinner=False)
def _load_chart_boards_api() -> dict[str, Any]:
    mod = importlib.import_module("chart_boards")
    return {
        "CHART_THEME_OPTIONS": getattr(mod, "CHART_THEME_OPTIONS"),
        "configure_market_storage": getattr(mod, "configure_market_storage"),
        "fig_15m_vwap_rsi": getattr(mod, "fig_15m_vwap_rsi"),
        "fig_5m_vwap_rsi7": getattr(mod, "fig_5m_vwap_rsi7"),
        "fig_daily": getattr(mod, "fig_daily"),
        "probe_market_inventory": getattr(mod, "probe_market_inventory"),
        "probe_symbol_interval_raw_rows": getattr(mod, "probe_symbol_interval_raw_rows"),
        "probe_market_cache_status": getattr(mod, "probe_market_cache_status"),
        "probe_recent_market_rows": getattr(mod, "probe_recent_market_rows"),
    }


theme_options = ["自动（白天浅色 / 晚上深色）", *list(_UI_THEMES.keys())]
theme_pick = st.sidebar.selectbox("显示主题", options=theme_options, index=0)
if theme_pick == "自动（白天浅色 / 晚上深色）":
    theme_name = _auto_theme_name()
    st.sidebar.caption(f"当前自动主题：{theme_name}")
else:
    theme_name = theme_pick
theme = _UI_THEMES[theme_name]
_apply_theme_css(theme)
chart_board_enabled = st.sidebar.toggle(
    "启用技术看板",
    value=False,
    key="chart_board_enabled",
    help="默认关闭。开启后才会加载技术看板模块与K线数据。",
)
if not chart_board_enabled:
    st.sidebar.caption("技术看板未启用：不会加载看板模块，也不会拉取看板数据。")

cloud_user_id = st.sidebar.text_input(
    "用户ID（用于跨设备同步）",
    value="evan",
    key="sidebar_user_id",
    help="配置了 Supabase 时读写云端 portfolio；否则使用本地 JSON。",
).strip()

_db = _db_conf()

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
    st.session_state.def_avgo = d["avgo"]
    st.session_state.def_nvda = d["nvda"]
    st.session_state.def_googl = d["googl"]
    st.session_state.def_msft = d["msft"]
    st.session_state.def_tlt = d["tlt"]
    st.session_state.def_iei = d["iei"]
    st.session_state.def_hs300 = d["hs300"]
    st.session_state.def_zz500 = d["zz500"]

    # 删除输入框缓存值，让下方 number_input 用新的 def_* 作为默认值。
    for k in (
        "inp_fx",
        "inp_voo",
        "inp_avgo",
        "inp_nvda",
        "inp_googl",
        "inp_msft",
        "inp_tlt",
        "inp_iei",
        "inp_hs300",
        "inp_zz500",
    ):
        if k in st.session_state:
            del st.session_state[k]

    st.success("已刷新市价")

_ensure_fx_session_default()
_ensure_price_session_defaults()
spot_meta = _fetch_spot_prices_meta()
fx_meta = _fetch_usdcny_rate_meta()
fx = float(st.session_state.get("inp_fx", st.session_state.def_fx))
prices_now = {
    "VOO": float(st.session_state.get("inp_voo", st.session_state.def_voo)),
    "AVGO": float(st.session_state.get("inp_avgo", st.session_state.def_avgo)),
    "NVDA": float(st.session_state.get("inp_nvda", st.session_state.def_nvda)),
    "GOOGL": float(st.session_state.get("inp_googl", st.session_state.def_googl)),
    "MSFT": float(st.session_state.get("inp_msft", st.session_state.def_msft)),
    "TLT": float(st.session_state.get("inp_tlt", st.session_state.def_tlt)),
    "IEI": float(st.session_state.get("inp_iei", st.session_state.def_iei)),
    "001015": float(st.session_state.get("inp_hs300", st.session_state.def_hs300)),
    "007994": float(st.session_state.get("inp_zz500", st.session_state.def_zz500)),
}

# 前端不负责同步外部行情，避免阻塞与不确定性；由独立 sync worker 负责写 Supabase

def _render_holdings_editor() -> None:
    with st.expander("编辑持仓（会保存）", expanded=False):
        with st.form("holdings_edit_form"):
            for sym, meta in _ASSET_META.items():
                c1, c2, c3 = st.columns([1, 1, 1])
                with c1:
                    shares = st.number_input(
                        f"{meta['label']} 持有数量",
                        min_value=0.0,
                        value=float(holdings[sym]["shares"]),
                        step=0.0001,
                        format="%.4f",
                        key=f"edit_shares_{sym}",
                    )
                with c2:
                    avg_cost = st.number_input(
                        f"单位成本({meta['currency']})",
                        min_value=0.0,
                        value=float(holdings[sym]["avg_cost"]),
                        step=0.0001,
                        format="%.4f",
                        key=f"edit_cost_{sym}",
                    )
                with c3:
                    total_cost_input = st.number_input(
                        f"或填总成本({meta['currency']})",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        format="%.4f",
                        key=f"edit_total_cost_{sym}",
                        help="若此处填写大于0的金额，将优先按 [总成本 ÷ 持有数量] 自动计算单位成本并覆盖保存。"
                    )
                holdings[sym]["shares"] = shares
                if total_cost_input > 0:
                    holdings[sym]["avg_cost"] = (total_cost_input / shares) if shares > 0 else 0.0
                else:
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
drawdown_pct_by_symbol: dict[str, float | None] = {}
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
    row_drawdown = _fetch_asset_drawdown(sym, meta)
    drawdown_pct_by_symbol[sym] = row_drawdown
    rows.append(
        {
            "标的": meta["label"],
            "币种": meta["currency"],
            "当日涨跌%": round(daily_change_pct_by_symbol.get(sym, 0.0), 2),
            "近60日高点回撤%": round(float(row_drawdown), 2) if isinstance(row_drawdown, (int, float)) else None,
            "浮动盈亏": round(pnl, 2),
            "涨跌幅%": round(pnl_pct, 2),
            "持有数量": round(shares, 3),
            "持仓成本": round(avg_cost, 2),
            "当前价": round(current, 4),
            "持仓市值": round(value, 2),
        }
    )

total_pnl_cny = total_value_cny - total_cost_cny
total_pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny > 0 else 0.0
cash_usd = float(balances_for_view.get("cash_usd", 0.0))
cash_cny = float(balances_for_view.get("cash_cny", 0.0))
total_balance_cny = cash_usd * fx + cash_cny
total_assets_cny = total_value_cny + total_balance_cny

st.markdown("### 📊 今日表现")

weighted_daily_pct = (
    sum(
        (value_cny_by_symbol.get(sym, 0.0) / total_value_cny) * daily_change_pct_by_symbol.get(sym, 0.0)
        for sym in _ASSET_META
    )
    if total_value_cny > 0
    else 0.0
)
weighted_daily_color = _change_color_by_pct(weighted_daily_pct, theme=theme)
# 汇总全持仓当日涨跌金额（先按各资产原币计算，再统一折算 CNY）。
total_daily_change_cny = 0.0
for sym, meta in _ASSET_META.items():
    d = float(daily_change_pct_by_symbol.get(sym, 0.0))
    d_ratio = d / 100.0
    shares_now = float(holdings.get(sym, {}).get("shares", 0.0))
    current_px = float(prices_now.get(sym, 0.0))
    current_value_native = shares_now * current_px
    if abs(1.0 + d_ratio) > 1e-9:
        daily_amount_native = current_value_native - (current_value_native / (1.0 + d_ratio))
    else:
        daily_amount_native = 0.0
    if meta["currency"] == "USD":
        total_daily_change_cny += daily_amount_native * fx
    else:
        total_daily_change_cny += daily_amount_native
total_daily_change_usd = (total_daily_change_cny / fx) if fx > 0 else 0.0
st.markdown(
    "<div class='daily-summary'>"
    f"<span class='daily-summary-main'><strong>当日加权涨跌</strong>：<span style='color:{weighted_daily_color}; font-weight:800; font-size:18px;'>{weighted_daily_pct:+.2f}%</span></span>"
    f"<span class='daily-summary-values' style='color:{weighted_daily_color}; font-weight:800;'>CNY {total_daily_change_cny:+,.2f} · USD {total_daily_change_usd:+,.2f}</span>"
    "</div>",
    unsafe_allow_html=True,
)

_daily_cards: list[str] = []
satellite_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in _SATELLITE_SYMBOLS)
satellite_daily_change_cny = 0.0
for sym in _SATELLITE_SYMBOLS:
    d = float(daily_change_pct_by_symbol.get(sym, 0.0))
    d_ratio = d / 100.0
    shares_now = float(holdings.get(sym, {}).get("shares", 0.0))
    current_px = float(prices_now.get(sym, 0.0))
    current_value_native = shares_now * current_px
    if abs(1.0 + d_ratio) > 1e-9:
        daily_amount_native = current_value_native - (current_value_native / (1.0 + d_ratio))
    else:
        daily_amount_native = 0.0
    satellite_daily_change_cny += daily_amount_native * fx
satellite_daily_pct = (
    sum(
        (value_cny_by_symbol.get(sym, 0.0) / satellite_value_cny) * daily_change_pct_by_symbol.get(sym, 0.0)
        for sym in _SATELLITE_SYMBOLS
    )
    if satellite_value_cny > 0
    else 0.0
)
satellite_daily_color = _change_color_by_pct(satellite_daily_pct, theme=theme)
satellite_daily_change_usd = (satellite_daily_change_cny / fx) if fx > 0 else 0.0
satellite_card_html = (
    f"<div class='daily-card' style='--daily-color:{satellite_daily_color};'>"
    "<div class='daily-card-title'>卫星仓位</div>"
    f"<div class='daily-card-pct'>{satellite_daily_pct:+.2f}%</div>"
    f"<div class='daily-card-amount'>CNY {satellite_daily_change_cny:+,.2f}<br>≈ USD {satellite_daily_change_usd:+,.2f}</div>"
    "</div>"
)
for sym, meta in _ASSET_META.items():
    d = daily_change_pct_by_symbol.get(sym, 0.0)
    c = _change_color_by_pct(d, theme=theme)
    shares_now = float(holdings.get(sym, {}).get("shares", 0.0))
    current_px = float(prices_now.get(sym, 0.0))
    current_value_native = shares_now * current_px
    d_ratio = d / 100.0
    # 用当前市值反推昨日市值，得到更贴近真实的当日波动金额。
    if abs(1.0 + d_ratio) > 1e-9:
        daily_amount_native = current_value_native - (current_value_native / (1.0 + d_ratio))
    else:
        daily_amount_native = 0.0
    if meta["currency"] == "USD":
        daily_amount_text = f"USD {daily_amount_native:+,.2f}<br>≈ CNY {daily_amount_native * fx:+,.2f}"
    else:
        daily_amount_text = f"CNY {daily_amount_native:+,.2f}"
    _daily_cards.append(
        f"<div class='daily-card' style='--daily-color:{c};'>"
        f"<div class='daily-card-title'>{meta['label']}</div>"
        f"<div class='daily-card-pct'>{d:+.2f}%</div>"
        f"<div class='daily-card-amount'>{daily_amount_text}</div>"
        "</div>"
    )
    if sym == "VOO":
        _daily_cards.append(satellite_card_html)
st.markdown(
    "<div class='daily-card-grid'>"
    + "".join(_daily_cards)
    + "</div>",
    unsafe_allow_html=True,
)

st.markdown("<br>", unsafe_allow_html=True)
st.subheader("📈 资产分布与盈亏")

excluded_core_pnl_symbols = _SATELLITE_SYMBOLS
pnl_chart_df = pd.DataFrame(
    [
        {
            "标的": _ASSET_META[sym]["label"],
            "浮盈亏(CNY)": round(pnl_cny_by_symbol[sym], 2),
            "方向": "盈利" if pnl_cny_by_symbol[sym] >= 0 else "亏损",
        }
        for sym in _ASSET_META
        if sym not in excluded_core_pnl_symbols
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
            scale=alt.Scale(domain=["盈利", "亏损"], range=["#16a34a", "#dc2626"]),
            legend=None,
        ),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(CNY):Q", format=",.2f"), "方向:N"],
    )
    .properties(title="核心仓位浮盈亏（不含卫星仓位，折合CNY）")
)
st.altair_chart(_theme_altair_chart(pnl_chart, theme), width="stretch")

usd_symbols = ("VOO", *_SATELLITE_SYMBOLS, "TLT", "IEI")
cny_symbols = ("001015", "007994")

bond_current = value_cny_by_symbol.get("TLT", 0.0) + value_cny_by_symbol.get("IEI", 0.0)
voo_current = value_cny_by_symbol.get("VOO", 0.0)

ratio_denominator = total_value_cny if total_value_cny > 0 else 0.0
voo_ratio = (voo_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
avgo_ratio = (value_cny_by_symbol.get("AVGO", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
nvda_ratio = (value_cny_by_symbol.get("NVDA", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
googl_ratio = (value_cny_by_symbol.get("GOOGL", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
msft_ratio = (value_cny_by_symbol.get("MSFT", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
new4_ratio = avgo_ratio + nvda_ratio + googl_ratio + msft_ratio
tlt_ratio = (value_cny_by_symbol.get("TLT", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
iei_ratio = (value_cny_by_symbol.get("IEI", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
bond_ratio = (bond_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0

voo_target = _TARGET_WEIGHTS["VOO"] * 100.0
new4_target = (
    _TARGET_WEIGHTS["AVGO"] + _TARGET_WEIGHTS["NVDA"] + _TARGET_WEIGHTS["GOOGL"] + _TARGET_WEIGHTS["MSFT"]
) * 100.0
bond_target = (_TARGET_WEIGHTS["TLT"] + _TARGET_WEIGHTS["IEI"]) * 100.0

group1_df = pd.DataFrame(
    [
        {"标的组": "VOO", "类型": "当前比例%", "成分": "VOO", "比例%": round(voo_ratio, 2)},
        {"标的组": "VOO", "类型": "目标比例%", "成分": "目标", "比例%": round(voo_target, 2)},
        {"标的组": "卫星仓位", "类型": "当前比例%", "成分": "卫星仓位", "比例%": round(new4_ratio, 2)},
        {"标的组": "卫星仓位", "类型": "目标比例%", "成分": "目标", "比例%": round(new4_target, 2)},
        {"标的组": "债券", "类型": "当前比例%", "成分": "债券", "比例%": round(bond_ratio, 2)},
        {"标的组": "债券", "类型": "目标比例%", "成分": "目标", "比例%": round(bond_target, 2)},
    ]
)

group1_chart = (
    alt.Chart(group1_df)
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的组:N", sort=["VOO", "卫星仓位", "债券"]),
        xOffset=alt.XOffset("类型:N", sort=["当前比例%", "目标比例%"]),
        y=alt.Y("比例%:Q", title="比例(%)"),
        color=alt.Color(
            "成分:N",
            sort=["VOO", "卫星仓位", "债券", "目标"],
            scale=alt.Scale(
                domain=["VOO", "卫星仓位", "债券", "目标"],
                range=[theme["accent"], "#8b5cf6", "#f59e0b", "#94a3b8"],
            ),
        ),
        order=alt.Order("成分:N", sort="ascending"),
        tooltip=["标的组:N", "类型:N", "成分:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="VOO / 卫星仓位 / 债券 当前与目标对比")
)
tech_denominator = avgo_ratio + nvda_ratio + googl_ratio + msft_ratio
tech_split_df = pd.DataFrame(
    [
        {
            "标的": "AVGO",
            "类型": "当前占卫星仓位%",
            "比例%": round((avgo_ratio / tech_denominator * 100.0) if tech_denominator > 0 else 0.0, 2),
            "浮盈亏(CNY)": round(pnl_cny_by_symbol.get("AVGO", 0.0), 2),
            "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get('AVGO', 0.0):+,.0f}",
        },
        {"标的": "AVGO", "类型": "目标占卫星仓位%", "比例%": 30.0, "浮盈亏(CNY)": None, "浮盈亏标签": ""},
        {
            "标的": "NVDA",
            "类型": "当前占卫星仓位%",
            "比例%": round((nvda_ratio / tech_denominator * 100.0) if tech_denominator > 0 else 0.0, 2),
            "浮盈亏(CNY)": round(pnl_cny_by_symbol.get("NVDA", 0.0), 2),
            "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get('NVDA', 0.0):+,.0f}",
        },
        {"标的": "NVDA", "类型": "目标占卫星仓位%", "比例%": 20.0, "浮盈亏(CNY)": None, "浮盈亏标签": ""},
        {
            "标的": "GOOGL",
            "类型": "当前占卫星仓位%",
            "比例%": round((googl_ratio / tech_denominator * 100.0) if tech_denominator > 0 else 0.0, 2),
            "浮盈亏(CNY)": round(pnl_cny_by_symbol.get("GOOGL", 0.0), 2),
            "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get('GOOGL', 0.0):+,.0f}",
        },
        {"标的": "GOOGL", "类型": "目标占卫星仓位%", "比例%": 30.0, "浮盈亏(CNY)": None, "浮盈亏标签": ""},
        {
            "标的": "MSFT",
            "类型": "当前占卫星仓位%",
            "比例%": round((msft_ratio / tech_denominator * 100.0) if tech_denominator > 0 else 0.0, 2),
            "浮盈亏(CNY)": round(pnl_cny_by_symbol.get("MSFT", 0.0), 2),
            "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get('MSFT', 0.0):+,.0f}",
        },
        {"标的": "MSFT", "类型": "目标占卫星仓位%", "比例%": 20.0, "浮盈亏(CNY)": None, "浮盈亏标签": ""},
    ]
)
tech_split_base = alt.Chart(tech_split_df)
tech_split_bars = (
    tech_split_base
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的:N", sort=["AVGO", "NVDA", "GOOGL", "MSFT"]),
        xOffset=alt.XOffset("类型:N", sort=["当前占卫星仓位%", "目标占卫星仓位%"]),
        y=alt.Y("比例%:Q", title="比例(%)"),
        color=alt.Color(
            "类型:N",
            sort=["当前占卫星仓位%", "目标占卫星仓位%"],
            scale=alt.Scale(range=["#8b5cf6", "#94a3b8"]),
        ),
        tooltip=[
            "标的:N",
            "类型:N",
            alt.Tooltip("比例%:Q", format=".2f"),
            alt.Tooltip("浮盈亏(CNY):Q", format=",.2f"),
        ],
    )
)
tech_pnl_labels = (
    alt.Chart(tech_split_df[tech_split_df["类型"] == "当前占卫星仓位%"])
    .mark_text(dy=-8, fontWeight=700, fontSize=12)
    .encode(
        x=alt.X("标的:N", sort=["AVGO", "NVDA", "GOOGL", "MSFT"]),
        xOffset=alt.XOffset("类型:N", sort=["当前占卫星仓位%", "目标占卫星仓位%"]),
        y=alt.Y("比例%:Q"),
        text=alt.Text("浮盈亏标签:N"),
        color=alt.value(theme["text"]),
    )
)
tech_split_chart = (
    (tech_split_bars + tech_pnl_labels)
    .properties(title="卫星仓位内部占比与浮盈亏（目标：AVGO/NVDA/GOOGL/MSFT = 3:2:3:2）")
)
st.altair_chart(_theme_altair_chart(tech_split_chart, theme), width="stretch")
st.altair_chart(_theme_altair_chart(group1_chart, theme), width="stretch")

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
    .properties(title="沪深300 / 中证500 当前占比", width="container", height=260)
)
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.altair_chart(_theme_altair_chart(group2_chart, theme), width="stretch")

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
    .properties(title="美元资产 / 人民币资产当前占比", width="container", height=260)
)
with chart_col2:
    st.altair_chart(_theme_altair_chart(group3_chart, theme), width="stretch")

st.markdown("<br>", unsafe_allow_html=True)
st.subheader("📦 我的持仓")
st.caption(f"当前持仓读取来源：{'云端数据库' if storage_mode == 'cloud' else '本地文件'}")
_render_holdings_editor()
st.dataframe(
    rows,
    width="stretch",
    hide_index=True,
    column_config={
        "当日涨跌%": st.column_config.NumberColumn("当日涨跌%", format="%.2f%%"),
        "近60日高点回撤%": st.column_config.NumberColumn("近60日高点回撤%", format="%.2f%%"),
        "涨跌幅%": st.column_config.NumberColumn("涨跌幅%", format="%.2f%%"),
    },
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

st.subheader("🧮 再平衡买入建议")
st.caption(_DCA_DRAWDOWN_RULE_TEXT)
if total_assets_cny <= 0:
    st.info("总资产为 0，暂无法生成再平衡建议。")
else:
    st.caption("按目标缺口和近高回撤给出买入优先级；表格只保留执行定投时最常用的信息。")
    rebalance_rows: list[dict[str, Any]] = []
    suggestion_map: dict[str, dict[str, Any]] = {}
    for sym, meta in _ASSET_META.items():
        tgt_w = _TARGET_WEIGHTS.get(sym, 0.0)
        cur_cny = value_cny_by_symbol.get(sym, 0.0)
        num = tgt_w * total_assets_cny - cur_cny
        den = 1.0 - tgt_w
        gap_cny = (num / den) if den > 0 else 0.0
        q = _fetch_us_etf_pe_drawdown(sym) if meta["currency"] == "USD" else {"pe": None}
        pe = q.get("pe")
        dd = drawdown_pct_by_symbol.get(sym)
        dca_signal = _drawdown_dca_signal(sym, dd if isinstance(dd, (int, float)) else None)
        pe_band = _USD_ASSET_PE_BANDS.get(sym)
        expensive = False
        if isinstance(pe, (int, float)) and pe_band is not None and float(pe) >= pe_band[1]:
            expensive = True
        if isinstance(pe, (int, float)) and float(pe) >= 35.0:
            expensive = True
        priority_score = max(0.0, gap_cny)
        if expensive:
            priority_score *= 0.55
        if isinstance(dd, (int, float)) and float(dd) <= -10:
            priority_score *= 1.15
        action = "暂不买入"
        note = "已高于目标或接近目标"
        if gap_cny > 0:
            if expensive:
                action = "观察"
                note = "估值偏贵，建议少量/暂缓"
            else:
                action = "优先买入"
                note = "低于目标权重，适合补仓"
        if dca_signal not in ("按计划观察", "暂无回撤数据"):
            note = f"{note}；回撤信号：{dca_signal}"
        gap_native = (gap_cny / fx) if meta["currency"] == "USD" and fx > 0 else gap_cny
        px = float(prices_now.get(sym, 0.0))
        need_units = (gap_native / px) if px > 0 else 0.0
        dd_txt = f"{float(dd):.2f}%" if isinstance(dd, (int, float)) else "N/A"
        rebalance_rows.append(
            {
                "标的": meta["label"],
                "建议动作": action,
                "优先级分数": round(priority_score, 2),
                "目标缺口(CNY)": round(gap_cny, 2),
                "按当前价需买": round(need_units, 3),
                "近60日回撤": dd_txt,
                "回撤定投档位": dca_signal,
                "说明": note,
            }
        )
        suggestion_map[sym] = {
            "gap_cny": gap_cny,
            "priority_score": priority_score,
            "action": action,
            "expensive": expensive,
        }
    rebalance_df = pd.DataFrame(rebalance_rows)
    rebalance_df = rebalance_df[
        ["标的", "建议动作", "目标缺口(CNY)", "按当前价需买", "近60日回撤", "回撤定投档位", "说明", "优先级分数"]
    ].sort_values(by=["优先级分数", "目标缺口(CNY)"], ascending=[False, False])
    if not rebalance_df.empty:
        rebalance_df.insert(0, "优先级", range(1, len(rebalance_df) + 1))
        rebalance_df = rebalance_df.drop(columns=["优先级分数"])
    st.dataframe(
        rebalance_df,
        width="stretch",
        hide_index=True,
        column_config={
            "优先级": st.column_config.NumberColumn("优先级", format="%d"),
            "目标缺口(CNY)": st.column_config.NumberColumn("目标缺口(CNY)", format="%.2f"),
            "按当前价需买": st.column_config.NumberColumn("按当前价需买", format="%.3f"),
        },
    )

    st.markdown("#### 根据再平衡建议执行本月定投")
    cfg_c1, cfg_c2 = st.columns(2)
    with cfg_c1:
        monthly_rmb = st.number_input("本月投入（CNY）", min_value=0.0, value=5000.0, step=100.0, key="rebalance_rmb")
    with cfg_c2:
        rebalance_fx = st.number_input("汇率（USD/CNY）", min_value=0.01, value=float(fx), step=0.0001, format="%.4f", key="rebalance_fx")

    if st.button("按优先级生成本月买入计划", key="rebalance_plan_btn"):
        _, balances_plan, _ = _load_user_state(cloud_user_id)
        usd_symbols = [s for s, m in _ASSET_META.items() if m["currency"] == "USD"]
        cny_symbols = [s for s, m in _ASSET_META.items() if m["currency"] == "CNY"]
        usd_ratio = sum(_TARGET_WEIGHTS[s] for s in usd_symbols)
        cny_ratio = sum(_TARGET_WEIGHTS[s] for s in cny_symbols)
        usd_budget_total = balances_plan["cash_usd"] + (monthly_rmb * usd_ratio / rebalance_fx if rebalance_fx > 0 else 0.0)
        cny_budget_total = balances_plan["cash_cny"] + monthly_rmb * cny_ratio

        usd_candidates = [s for s in usd_symbols if suggestion_map[s]["gap_cny"] > 0 and not suggestion_map[s]["expensive"]]
        if not usd_candidates:
            usd_candidates = [s for s in usd_symbols if suggestion_map[s]["gap_cny"] > 0]
        cny_candidates = [s for s in cny_symbols if suggestion_map[s]["gap_cny"] > 0]

        usd_weight_sum = sum(max(0.0, suggestion_map[s]["priority_score"]) for s in usd_candidates)
        cny_weight_sum = sum(max(0.0, suggestion_map[s]["priority_score"]) for s in cny_candidates)

        plan_buys: dict[str, dict[str, float]] = {s: {"shares": 0.0, "price": float(prices_now[s])} for s in _ASSET_META}
        usd_spent = 0.0
        cny_spent = 0.0

        for s in usd_candidates:
            px = float(prices_now[s])
            if px <= 0:
                continue
            w = max(0.0, suggestion_map[s]["priority_score"])
            alloc = usd_budget_total * (w / usd_weight_sum) if usd_weight_sum > 0 else 0.0
            shares = int(alloc // px)
            spent = shares * px
            plan_buys[s]["shares"] = float(shares)
            usd_spent += spent

        for s in cny_candidates:
            px = float(prices_now[s])
            if px <= 0:
                continue
            w = max(0.0, suggestion_map[s]["priority_score"])
            alloc = cny_budget_total * (w / cny_weight_sum) if cny_weight_sum > 0 else 0.0
            units = alloc / px
            spent = units * px
            plan_buys[s]["shares"] = float(units)
            cny_spent += spent

        cash_usd_next = max(0.0, usd_budget_total - usd_spent)
        cash_cny_next = max(0.0, cny_budget_total - cny_spent)

        st.info(
            f"美元预算 {usd_budget_total:.2f}（买入 {usd_spent:.2f}，结余 {cash_usd_next:.2f}） | "
            f"人民币预算 {cny_budget_total:.2f}（买入 {cny_spent:.2f}，结余 {cash_cny_next:.2f}）"
        )
        for s, buy in plan_buys.items():
            if buy["shares"] > 0:
                unit_txt = "股" if _ASSET_META[s]["currency"] == "USD" else "份"
                st.markdown(f"- **{_ASSET_META[s]['label']}**：买入 **{buy['shares']:.3f}** {unit_txt} @ {buy['price']:.4f}")

        if st.button("将该买入计划更新到我的持仓", key="rebalance_apply_btn"):
            holdings_apply, balances_apply, _ = _load_user_state(cloud_user_id)
            for s, buy in plan_buys.items():
                holdings_apply[s] = _merge_buy(holdings_apply[s], buy["shares"], buy["price"])
            balances_apply["cash_usd"] = cash_usd_next
            balances_apply["cash_cny"] = cash_cny_next
            save_mode = _save_user_state(cloud_user_id, holdings_apply, balances_apply)
            st.success(f"已更新到持仓（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

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

st.divider()

st.divider()

if chart_board_enabled:
    _chart_symbol_labels = {
        "VOO": "VOO",
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
    _chart_user_avg_cost: float | None = None
    try:
        _chart_hold = holdings.get(_chart_yf, {})  # type: ignore[assignment]
        _sh = float(_chart_hold.get("shares", 0.0))
        _ac = float(_chart_hold.get("avg_cost", 0.0))
        if _sh > 0 and _ac > 0:
            _chart_user_avg_cost = _ac
    except Exception:
        _chart_user_avg_cost = None

    _chart_api = _load_chart_boards_api()
    chart_theme_options = list(_chart_api["CHART_THEME_OPTIONS"])
    chart_theme = st.sidebar.selectbox(
        "K线配色主题",
        options=chart_theme_options,
        index=chart_theme_options.index("Trading Dark") if "Trading Dark" in chart_theme_options else 0,
        key="chart_plot_theme",
        help="Classic Light 浅色机构风；Trading Dark 暗色终端风（绿涨红跌）；CN Quant 红涨绿跌略饱和。",
    )
    st.sidebar.caption("显示主题影响盈亏颜色；K线主题只影响技术看板配色。")
    chart_data_mode = st.sidebar.selectbox(
        "看板数据模式",
        options=["实时拉取（默认）", "数据库缓存（Supabase）"],
        index=0,
        key="chart_data_mode",
        help="实时拉取：页面直接请求行情源；数据库缓存：仅读 Supabase 里的 K 线缓存。",
    )

    chart_cache_only = chart_data_mode == "数据库缓存（Supabase）"
    if chart_cache_only and not _db:
        st.sidebar.warning("未配置 Supabase，已自动切换为实时拉取模式。")
        chart_cache_only = False
    _chart_api["configure_market_storage"](_db if chart_cache_only else None, read_only=chart_cache_only)

    _interval_display_map = {"日线（1d）": "1d", "15分钟（15m）": "15m", "5分钟（5m）": "5m"}
    _interval_pick = st.sidebar.multiselect(
        "看板加载哪些周期（不选则不拉取数据）",
        options=list(_interval_display_map.keys()),
        default=list(_interval_display_map.keys()),
        key="chart_intervals_to_load",
        help="关闭某个周期后，该周期对应的图表不会触发 fetch_ohlcv()，通常能显著减少加载时间。",
    )
    _interval_keys = [_interval_display_map[x] for x in _interval_pick] or ["1d"]

    _prog_slot = st.empty()
    _fig_d = _fig_15 = _fig_5 = None
    _chart_errs: dict[str, str] = {}
    _nj = int("1d" in _interval_keys) + int("15m" in _interval_keys) + int("5m" in _interval_keys)
    if _nj > 0:
        _fut_map: dict[Any, tuple[str, str]] = {}
        with ThreadPoolExecutor(max_workers=min(3, _nj)) as _pool:
            if "1d" in _interval_keys:
                _fut_map[_pool.submit(_chart_api["fig_daily"], _chart_yf, _chart_pick, chart_theme=chart_theme, user_avg_cost=_chart_user_avg_cost, cache_only=chart_cache_only)] = ("1d", "日线（1d）")
            if "15m" in _interval_keys:
                _fut_map[_pool.submit(_chart_api["fig_15m_vwap_rsi"], _chart_yf, _chart_pick, chart_theme=chart_theme, user_avg_cost=_chart_user_avg_cost, cache_only=chart_cache_only)] = ("15m", "15m（15m）")
            if "5m" in _interval_keys:
                _fut_map[_pool.submit(_chart_api["fig_5m_vwap_rsi7"], _chart_yf, _chart_pick, chart_theme=chart_theme, user_avg_cost=_chart_user_avg_cost, cache_only=chart_cache_only)] = ("5m", "5m（5m）")
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
                    _chart_errs[_kind] = str(e)
        _prog_slot.empty()

    _tab_d, _tab_15, _tab_5 = st.tabs(["日线（EMA·ATR·MACD）", "15m（VWAP·RSI·MACD）", "5m（VWAP·RSI·MACD）"])
    with _tab_d:
        if "1d" in _interval_keys and _fig_d is not None:
            st.plotly_chart(_fig_d, width="stretch")
        elif "1d" in _interval_keys:
            st.warning(f"日线图加载失败：{_chart_errs.get('1d', '未知错误')}")
    with _tab_15:
        if "15m" in _interval_keys and _fig_15 is not None:
            st.plotly_chart(_fig_15, width="stretch")
        elif "15m" in _interval_keys:
            st.warning(f"15m 图加载失败：{_chart_errs.get('15m', '未知错误')}")
    with _tab_5:
        if "5m" in _interval_keys and _fig_5 is not None:
            st.plotly_chart(_fig_5, width="stretch")
        elif "5m" in _interval_keys:
            st.warning(f"5m 图加载失败：{_chart_errs.get('5m', '未知错误')}")
else:
    st.info("技术看板已关闭。点击侧边栏“启用技术看板”后才会加载看板模块与K线数据。")
