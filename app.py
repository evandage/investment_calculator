import re
import json
import base64
import importlib
import os
import time
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
    "QQQ": 400.0,
    "AVGO": 1700.0,
    "NVDA": 1200.0,
    "GOOGL": 180.0,
    "MSFT": 420.0,
    "ISRG": 450.0,
    "SGOV": 100.0,
    "001015": 1.0,
    "007994": 1.0,
}

_TICKERS = {
    "voo": "VOO",
    "qqq": "QQQ",
    "avgo": "AVGO",
    "nvda": "NVDA",
    "googl": "GOOGL",
    "msft": "MSFT",
    "isrg": "ISRG",
    "sgov": "SGOV",
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
    "QQQ": "usQQQ",
    "AVGO": "usAVGO",
    "NVDA": "usNVDA",
    "GOOGL": "usGOOGL",
    "MSFT": "usMSFT",
    "ISRG": "usISRG",
    "SGOV": "usSGOV",
}
_QQ_US_KLINE = {
    "VOO": "usVOO.AM",
    "QQQ": "usQQQ.OQ",
    "AVGO": "usAVGO.OQ",
    "NVDA": "usNVDA.OQ",
    "GOOGL": "usGOOGL.OQ",
    "MSFT": "usMSFT.OQ",
    "ISRG": "usISRG.OQ",
    "SGOV": "usSGOV.AM",
}
_SINA_GB = {
    "VOO": "gb_voo",
    "QQQ": "gb_qqq",
    "AVGO": "gb_avgo",
    "NVDA": "gb_nvda",
    "GOOGL": "gb_googl",
    "MSFT": "gb_msft",
    "ISRG": "gb_isrg",
    "SGOV": "gb_sgov",
}
_FUND_CODES = {"001015": "001015", "007994": "007994"}
_EASTMONEY_KLINE_URLS = (
    "http://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
)
_EASTMONEY_QUOTE_URLS = (
    "http://push2.eastmoney.com/api/qt/stock/get",
    "https://push2.eastmoney.com/api/qt/stock/get",
)
_DRAWDOWN_CACHE_VERSION = "tencent-eastmoney-yahoo-chart-v5"
_EASTMONEY_US_SECID = {
    "VOO": "107.VOO",
    "QQQ": "105.QQQ",
    "AVGO": "105.AVGO",
    "NVDA": "105.NVDA",
    "GOOGL": "105.GOOGL",
    "MSFT": "105.MSFT",
    "ISRG": "105.ISRG",
    "SGOV": "106.SGOV",
}
_US_MARKET_SYMBOLS = ("VOO", "QQQ", "AVGO", "NVDA", "GOOGL", "MSFT", "ISRG", "SGOV")


def _normalize_market_provider(value: str | None) -> str:
    v = str(value or "auto").strip().lower()
    if v in {"tencent", "qq", "gtimg"}:
        return "tencent"
    if v in {"eastmoney", "em", "cn", "china", "mainland"}:
        return "eastmoney"
    if v in {"yfinance", "yf", "yahoo", "us"}:
        return "yfinance"
    return "auto"


def _market_data_provider() -> str:
    return "tencent"

_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_MONTHLY_BUDGET_USAGE_FILE = Path(__file__).with_name("monthly_budget_usage.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "QQQ": {"label": "QQQ", "currency": "USD"},
    "AVGO": {"label": "AVGO", "currency": "USD"},
    "NVDA": {"label": "NVDA", "currency": "USD"},
    "GOOGL": {"label": "GOOGL", "currency": "USD"},
    "MSFT": {"label": "MSFT", "currency": "USD"},
    "ISRG": {"label": "ISRG", "currency": "USD"},
    "SGOV": {"label": "短债(SGOV)", "currency": "USD"},
    "001015": {"label": "沪深300", "currency": "CNY"},
    "007994": {"label": "中证500", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    # 目标比例：
    # 美元资产: VOO/QQQ/卫星仓位/短债(SGOV) = 4:2:2:2
    # 人民币资产: 沪深300(001015) 20%, 中证500(007994) 20%
    "VOO": 0.24,
    "QQQ": 0.12,
    "AVGO": 0.024,
    "NVDA": 0.016,
    "GOOGL": 0.024,
    "MSFT": 0.016,
    "ISRG": 0.04,
    "SGOV": 0.12,
    "001015": 0.20,
    "007994": 0.20,
}

_SATELLITE_SYMBOLS = ("AVGO", "NVDA", "GOOGL", "MSFT", "ISRG")
_SATELLITE_TARGET_PARTS = {
    "AVGO": 3,
    "NVDA": 2,
    "GOOGL": 3,
    "MSFT": 2,
    "ISRG": 5,
}

# 美元资产 PE 参考区间（经验口径，仅作辅助，不构成投资建议）
_USD_ASSET_PE_BANDS: dict[str, tuple[float, float]] = {
    "VOO": (18.0, 24.0),
    "QQQ": (24.0, 36.0),
    "ISRG": (40.0, 65.0),
    "AVGO": (24.0, 34.0),
    "NVDA": (28.0, 45.0),
    "GOOGL": (18.0, 28.0),
    "MSFT": (24.0, 36.0),
    "SGOV": (0.0, 10.0),
}

_REBALANCE_PHASE_BUILD = "建仓期"
_REBALANCE_PHASE_DCA = "长期定投期"
_REBALANCE_ALLOCATION_ROWS = [
    {"资产": "VOO", "目标占比": "40%", "策略定位": "核心长期仓"},
    {"资产": "QQQ", "目标占比": "20%", "策略定位": "科技增强仓"},
    {"资产": "卫星仓（MSFT/GOOGL/NVDA/AVGO/ISRG）", "目标占比": "20%", "策略定位": "主动超额收益"},
    {"资产": "SGOV", "目标占比": "20%", "策略定位": "弹药库/现金管理"},
]
_REBALANCE_STRATEGY_LABELS = {
    "VOO": "长期无脑定投",
    "QQQ": "动态增强，跌了多买",
    "GOOGL": "半定投化",
    "MSFT": "半定投化",
    "ISRG": "半定投化",
    "NVDA": "波动驱动加仓",
    "AVGO": "波动驱动加仓",
    "SGOV": "机会弹药库",
}
_REBALANCE_RULES: dict[str, dict[str, dict[str, Any]]] = {
    _REBALANCE_PHASE_BUILD: {
        "VOO": {"normal": (1.0, "正常建仓", "每月机械定投", "normal"), "bands": [(-15.0, 3.0, "大加", "3x 月预算 + 少量 SGOV", "large"), (-10.0, 2.0, "中加", "2x 月预算", "medium"), (-5.0, 1.5, "小加", "1.5x 月预算", "small")]},
        "QQQ": {"normal": (0.25, "少买/等回撤", "未跌满 6%：贵了少买", "normal"), "bands": [(-18.0, 4.0, "大加", "4x + 动用 SGOV", "large"), (-12.0, 2.5, "中加", "2.5x", "medium"), (-6.0, 1.5, "小加", "1.5x", "small")]},
        "MSFT": {"normal": (0.25, "慢慢建仓", "未到小加档：小额累积", "normal"), "bands": [(-22.0, 4.0, "大加", "4x", "large"), (-15.0, 2.5, "中加", "2.5x", "medium"), (-8.0, 1.5, "小加", "1.5x", "small")]},
        "GOOGL": {"normal": (0.25, "慢慢建仓", "未到小加档：小额累积", "normal"), "bands": [(-25.0, 4.0, "大加", "4x", "large"), (-18.0, 2.5, "中加", "2.5x", "medium"), (-10.0, 1.5, "小加", "1.5x", "small")]},
        "NVDA": {"normal": (0.0, "等待波动", "未到小加档：等待波动", "normal"), "bands": [(-30.0, 5.0, "大加", "5x + 明显动用 SGOV", "large"), (-20.0, 3.0, "中加", "3x", "medium"), (-12.0, 1.5, "小加", "1.5x", "small")]},
        "AVGO": {"normal": (0.0, "等待波动", "未到小加档：等待波动", "normal"), "bands": [(-28.0, 4.0, "大加", "4x", "large"), (-18.0, 2.5, "中加", "2.5x", "medium"), (-10.0, 1.5, "小加", "1.5x", "small")]},
        "ISRG": {"normal": (0.25, "慢慢建仓", "未到小加档：小额累积", "normal"), "bands": [(-28.0, 4.0, "大加", "4x", "large"), (-18.0, 2.5, "中加", "2.5x", "medium"), (-10.0, 1.5, "小加", "1.5x", "small")]},
    },
    _REBALANCE_PHASE_DCA: {
        "VOO": {"normal": (1.0, "正常定投", "正常：工资定投", "normal"), "bands": [(-15.0, 2.5, "大加", "中等动用 SGOV", "large"), (-10.0, 1.75, "明显加仓", "少量 SGOV", "medium"), (-5.0, 1.25, "多买一点", "当月 SGOV 流入", "small")]},
        "QQQ": {"normal": (0.5, "小额定投", "正常：小额定投", "normal"), "bands": [(-18.0, 2.5, "大加", "中等 SGOV", "large"), (-12.0, 1.75, "明显加仓", "少量 SGOV", "medium"), (-6.0, 1.25, "增加投入", "当月 SGOV 流入", "small")]},
        "MSFT": {"normal": (0.25, "持续小买", "正常：持续小买", "normal"), "bands": [(-22.0, 2.0, "大加", "大加", "large"), (-15.0, 1.5, "明显加", "明显加", "medium"), (-8.0, 1.0, "多买", "多买", "small")]},
        "GOOGL": {"normal": (0.25, "持续小买", "正常：持续小买", "normal"), "bands": [(-25.0, 2.0, "大加", "大加", "large"), (-18.0, 1.5, "明显加", "明显加", "medium"), (-10.0, 1.0, "多买", "多买", "small")]},
        "NVDA": {"normal": (0.0, "少量/观察", "正常：少量/观察", "normal"), "bands": [(-30.0, 2.5, "大加", "大加（AI 恐慌级）", "large"), (-20.0, 1.75, "明显加", "明显加", "medium"), (-12.0, 1.0, "小加", "小加", "small")]},
        "AVGO": {"normal": (0.1, "观察/小买", "正常：观察/小买", "normal"), "bands": [(-28.0, 2.5, "大加", "大加", "large"), (-18.0, 1.75, "明显加", "明显加", "medium"), (-10.0, 1.0, "小加", "小加", "small")]},
        "ISRG": {"normal": (0.25, "长期慢买", "正常：长期慢买", "normal"), "bands": [(-28.0, 2.0, "大加", "大加", "large"), (-18.0, 1.5, "明显加", "明显加", "medium"), (-10.0, 1.0, "多买", "多买", "small")]},
    },
}


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

        .daily-card-wide {{
            grid-column: span 2;
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

            .daily-card-wide {{
                grid-column: span 2;
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

            .daily-card-wide {{
                grid-column: span 1;
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


def _fetch_yfinance_us_price_change() -> dict[str, dict[str, float]]:
    import yfinance as yf

    out: dict[str, dict[str, float]] = {}
    for sym in _US_MARKET_SYMBOLS:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
            if hist.empty or "Close" not in hist.columns:
                continue
            closes = pd.to_numeric(hist["Close"], errors="coerce").dropna()
            if closes.empty:
                continue
            price = float(closes.iloc[-1])
            try:
                fast_price = float((ticker.fast_info or {}).get("last_price") or 0.0)
                if fast_price > 0:
                    price = fast_price
            except Exception:
                pass
            prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else price
            change_pct = ((price / prev_close - 1.0) * 100.0) if prev_close > 0 else 0.0
            if price > 0:
                out[sym] = {"price": price, "change_pct": change_pct}
        except Exception:
            continue
    return out


def _fetch_fund_nav_price_change(code: str) -> tuple[float, float, str] | None:
    """东方财富历史净值：返回(最新确认单位净值, 日增长率%, 净值日期)。"""
    url = f"https://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={code}&page=1&per=1"
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://fundf10.eastmoney.com/",
        },
    )
    text = r.text
    rows = re.findall(
        r"<tr>\s*<td>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>([^<]+)</td>",
        text,
    )
    if not rows:
        return None
    nav_date, nav, daily_pct = rows[0]
    try:
        price = float(nav)
        change_pct = float(daily_pct.strip().rstrip("%"))
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price, change_pct, nav_date


def _fetch_fund_estimated_price_change(code: str) -> tuple[float, float, str] | None:
    """东方财富基金估值：返回(盘中/收盘估值, 估算涨跌幅%, 估值时间)。仅作兜底。"""
    url = f"https://fundgz.1234567.com.cn/js/{code}.js"
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://fund.eastmoney.com/",
        },
    )
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
        gztime = str(obj.get("gztime") or "")
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price, change_pct, gztime


def _is_cn_market_open(now: datetime | None = None) -> bool:
    current = now or datetime.now(_TZ_SHANGHAI)
    if current.weekday() >= 5:
        return False
    minutes = current.hour * 60 + current.minute
    return (9 * 60 + 30 <= minutes <= 11 * 60 + 30) or (13 * 60 <= minutes <= 15 * 60)


def _fetch_fund_price_change(code: str) -> tuple[float, float, str] | None:
    """基金展示口径：交易中用估算；收盘后当日净值未披露前继续用估算。"""
    current = datetime.now(_TZ_SHANGHAI)
    today = current.strftime("%Y-%m-%d")
    nav = _fetch_fund_nav_price_change(code)
    estimate = _fetch_fund_estimated_price_change(code)

    if _is_cn_market_open(current):
        if estimate is not None:
            return estimate[0], estimate[1], "东财基金估算"
        if nav is not None:
            return nav[0], nav[1], f"东财确认净值({nav[2]})"
        return None

    if nav is not None and nav[2] >= today:
        return nav[0], nav[1], f"东财确认净值({nav[2]})"

    if estimate is not None and estimate[2].startswith(today):
        return estimate[0], estimate[1], "东财收盘估算"

    if nav is not None:
        return nav[0], nav[1], f"东财确认净值({nav[2]})"
    if estimate is not None:
        return estimate[0], estimate[1], "东财基金估算"
    return None


@st.cache_data(ttl=60, show_spinner=False)
def _fetch_spot_prices_meta() -> dict[str, object]:
    out: dict[str, float] = {}
    daily_change_pct_by_symbol: dict[str, float] = {}
    source_by_symbol: dict[str, str] = {}
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    provider = _market_data_provider()

    if provider == "yfinance":
        try:
            yf_raw = _fetch_yfinance_us_price_change()
            for sym, item in yf_raw.items():
                out[sym] = float(item["price"])
                daily_change_pct_by_symbol[sym] = float(item["change_pct"])
                source_by_symbol[sym] = "yfinance"
        except Exception:
            pass
    else:
        try:
            qq_raw = _fetch_qq_us_price_change()
            for sym, item in qq_raw.items():
                out[sym] = float(item["price"])
                daily_change_pct_by_symbol[sym] = float(item["change_pct"])
                source_by_symbol[sym] = "腾讯美股"
        except Exception:
            pass

        for sym in _US_MARKET_SYMBOLS:
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
                p, change_pct, source = res
                out[sym] = p
                daily_change_pct_by_symbol[sym] = change_pct
                source_by_symbol[sym] = source
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
        "market_data_provider": provider,
        "fetched_at": fetched_at,
    }


def _fetch_spot_prices() -> dict[str, float]:
    return dict(_fetch_spot_prices_meta()["prices"])


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_vix_meta() -> dict[str, float | str]:
    """获取美股 CBOE VIX 当前值与当日涨跌幅。"""
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    if _market_data_provider() == "yfinance":
        try:
            import yfinance as yf

            hist = yf.Ticker("^VIX").history(period="5d", interval="1d", auto_adjust=False)
            closes = pd.to_numeric(hist["Close"], errors="coerce").dropna() if not hist.empty else pd.Series(dtype=float)
            if not closes.empty:
                cur = float(closes.iloc[-1])
                prev = float(closes.iloc[-2]) if len(closes) >= 2 else cur
                pct = ((cur / prev - 1.0) * 100.0) if prev > 0 else 0.0
                if cur > 0:
                    return {"vix": cur, "change_pct": pct, "source": "yfinance ^VIX", "fetched_at": fetched_at}
        except Exception:
            pass
    for url in _EASTMONEY_QUOTE_URLS:
        try:
            r = requests.get(
                url,
                params={"secid": "167.VIX", "fields": "f43,f58,f60,f170"},
                timeout=(10, 20),
                headers={**_REQUEST_HEADERS, "Referer": "https://quote.eastmoney.com/"},
            )
            r.raise_for_status()
            data = r.json().get("data") or {}
            cur = float(data.get("f43")) / 100.0
            pct = float(data.get("f170")) / 100.0
            if cur > 0:
                return {"vix": cur, "change_pct": pct, "source": "东方财富 VIX(167.VIX)", "fetched_at": fetched_at}
        except Exception:
            continue
    return {"vix": 20.0, "change_pct": 0.0, "source": "Fallback(20.0)", "fetched_at": fetched_at}


def _vix_regime(vix: float) -> tuple[str, str]:
    if vix < 15:
        return ("低波动", "市场情绪偏乐观，风险偏好较高；注意防止过度乐观。")
    if vix < 20:
        return ("中性偏稳", "常态区间，市场波动温和，可按计划分批配置。")
    if vix < 30:
        return ("偏高波动", "不确定性上升，建议控制单次仓位、分批进场。")
    return ("高波动/恐慌", "风险事件阶段，优先仓位管理与现金流，避免一次性重仓。")


def _fetch_yahoo_chart_60d_metrics(symbol: str) -> dict[str, float | None]:
    period2 = int(time.time())
    period1 = period2 - 220 * 24 * 60 * 60
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={
                "period1": period1,
                "period2": period2,
                "interval": "1d",
                "events": "history",
            },
            timeout=(8, 18),
            headers=_REQUEST_HEADERS,
        )
        r.raise_for_status()
        result = ((r.json().get("chart") or {}).get("result") or [None])[0] or {}
        quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
        closes = [
            float(v)
            for v in (quote.get("close") or [])
            if isinstance(v, (int, float)) and float(v) > 0
        ]
        if len(closes) <= 1:
            return {"drawdown_pct": None, "rebound_pct": None}
        win = closes[-60:] if len(closes) >= 60 else closes
        peak = max(win)
        trough = min(win)
        last = closes[-1]
        drawdown = (last / peak - 1.0) * 100.0 if peak > 0 else None
        rebound = (last / trough - 1.0) * 100.0 if trough > 0 else None
        return {"drawdown_pct": drawdown, "rebound_pct": rebound}
    except Exception:
        return {"drawdown_pct": None, "rebound_pct": None}


def _fetch_tencent_us_60d_metrics(symbol: str) -> dict[str, float | None]:
    code = _QQ_US_KLINE.get(symbol)
    if not code:
        return {"drawdown_pct": None, "rebound_pct": None}
    try:
        r = requests.get(
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
            params={"param": f"{code},day,,,100,qfq"},
            timeout=(8, 18),
            headers={**_REQUEST_HEADERS, "Referer": "https://finance.qq.com/"},
        )
        r.raise_for_status()
        data = r.json().get("data") or {}
        rows = ((data.get(code) or {}).get("day") or []) if isinstance(data, dict) else []
        closes: list[float] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 3:
                continue
            try:
                close = float(row[2])
            except (TypeError, ValueError):
                continue
            if close > 0:
                closes.append(close)
        if len(closes) <= 1:
            return {"drawdown_pct": None, "rebound_pct": None}
        win = closes[-60:] if len(closes) >= 60 else closes
        peak = max(win)
        trough = min(win)
        last = closes[-1]
        drawdown = (last / peak - 1.0) * 100.0 if peak > 0 else None
        rebound = (last / trough - 1.0) * 100.0 if trough > 0 else None
        return {"drawdown_pct": drawdown, "rebound_pct": rebound}
    except Exception:
        return {"drawdown_pct": None, "rebound_pct": None}


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_us_etf_pe_drawdown(
    symbol: str,
    cache_version: str = _DRAWDOWN_CACHE_VERSION,
    provider: str | None = None,
) -> dict[str, float | None]:
    """返回美股/ETF估值与回撤指标：pe(若可得)、回撤%(近60日高点回撤，负值为回撤)。"""
    _ = cache_version
    provider = _normalize_market_provider(provider or _market_data_provider())
    pe_val: float | None = None
    dd_val: float | None = None
    rebound_val: float | None = None
    if provider != "yfinance":
        tencent_metrics = _fetch_tencent_us_60d_metrics(symbol)
        dd_val = tencent_metrics.get("drawdown_pct")
        rebound_val = tencent_metrics.get("rebound_pct")

    if provider == "yfinance":
        try:
            import yfinance as yf

            hist = yf.Ticker(symbol).history(period="6mo", interval="1d", auto_adjust=True)
            closes = pd.to_numeric(hist["Close"], errors="coerce").dropna() if not hist.empty else pd.Series(dtype=float)
            if len(closes) > 1:
                win = closes.tail(60)
                peak = float(win.max())
                last = float(closes.iloc[-1])
                if peak > 0:
                    dd_val = (last / peak - 1.0) * 100.0
                trough = float(win.min())
                if trough > 0:
                    rebound_val = (last / trough - 1.0) * 100.0
        except Exception:
            pass
        if dd_val is None or rebound_val is None:
            tencent_metrics = _fetch_tencent_us_60d_metrics(symbol)
            dd_val = dd_val if dd_val is not None else tencent_metrics.get("drawdown_pct")
            rebound_val = rebound_val if rebound_val is not None else tencent_metrics.get("rebound_pct")
        if dd_val is None or rebound_val is None:
            yahoo_metrics = _fetch_yahoo_chart_60d_metrics(symbol)
            dd_val = dd_val if dd_val is not None else yahoo_metrics.get("drawdown_pct")
            rebound_val = rebound_val if rebound_val is not None else yahoo_metrics.get("rebound_pct")
        return {"pe": pe_val, "drawdown_pct": dd_val, "rebound_pct": rebound_val}

    secid = _EASTMONEY_US_SECID.get(symbol)
    if secid and (dd_val is None or rebound_val is None):
        for url in _EASTMONEY_KLINE_URLS:
            try:
                r = requests.get(
                    url,
                    params={
                        "secid": secid,
                        "klt": "101",
                        "fqt": "1",
                        "lmt": "80",
                        "end": "20500101",
                        "fields1": "f1,f2,f3,f4,f5,f6",
                        "fields2": "f51,f52,f53,f54,f55,f56,f57",
                    },
                    timeout=(10, 20),
                    headers={**_REQUEST_HEADERS, "Referer": "https://quote.eastmoney.com/"},
                )
                r.raise_for_status()
                klines = ((r.json().get("data") or {}).get("klines") or [])
                closes: list[float] = []
                for line in klines:
                    parts = str(line).split(",")
                    if len(parts) < 3:
                        continue
                    try:
                        close = float(parts[2])
                    except (TypeError, ValueError):
                        continue
                    if close > 0:
                        closes.append(close)
                if len(closes) > 1:
                    win = closes[-60:] if len(closes) >= 60 else closes
                    peak = max(win)
                    trough = min(win)
                    last = closes[-1]
                    if peak > 0:
                        dd_val = (last / peak - 1.0) * 100.0
                    if trough > 0:
                        rebound_val = (last / trough - 1.0) * 100.0
                    if dd_val is not None or rebound_val is not None:
                        break
            except Exception:
                continue

    if dd_val is None or rebound_val is None:
        yahoo_metrics = _fetch_yahoo_chart_60d_metrics(symbol)
        dd_val = dd_val if dd_val is not None else yahoo_metrics.get("drawdown_pct")
        rebound_val = rebound_val if rebound_val is not None else yahoo_metrics.get("rebound_pct")

    return {"pe": pe_val, "drawdown_pct": dd_val, "rebound_pct": rebound_val}


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_fund_60d_metrics(code: str) -> dict[str, float | None]:
    """用东方财富历史净值估算近60条记录高点回撤与低点涨幅。"""
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
            return {"drawdown_pct": None, "rebound_pct": None}
        win = values[:60]
        peak = max(win)
        trough = min(win)
        last = values[0]
        drawdown = (last / peak - 1.0) * 100.0 if peak > 0 else None
        rebound = (last / trough - 1.0) * 100.0 if trough > 0 else None
        return {"drawdown_pct": drawdown, "rebound_pct": rebound}
    except Exception:
        return {"drawdown_pct": None, "rebound_pct": None}


def _fetch_fund_drawdown(code: str) -> float | None:
    q = _fetch_fund_60d_metrics(code)
    dd = q.get("drawdown_pct")
    return float(dd) if isinstance(dd, (int, float)) else None


def _fetch_asset_drawdown(sym: str, meta: dict[str, str]) -> float | None:
    if meta["currency"] == "USD":
        q = _fetch_us_etf_pe_drawdown(sym, _DRAWDOWN_CACHE_VERSION, _market_data_provider())
        dd = q.get("drawdown_pct")
        return float(dd) if isinstance(dd, (int, float)) else None
    if sym in _FUND_CODES:
        return _fetch_fund_drawdown(_FUND_CODES[sym])
    return None


def _fetch_asset_rebound(sym: str, meta: dict[str, str]) -> float | None:
    if meta["currency"] == "USD":
        q = _fetch_us_etf_pe_drawdown(sym, _DRAWDOWN_CACHE_VERSION, _market_data_provider())
        rebound = q.get("rebound_pct")
        return float(rebound) if isinstance(rebound, (int, float)) else None
    if sym in _FUND_CODES:
        q = _fetch_fund_60d_metrics(_FUND_CODES[sym])
        rebound = q.get("rebound_pct")
        return float(rebound) if isinstance(rebound, (int, float)) else None
    return None


def _rebalance_strategy_signal(
    symbol: str,
    drawdown_pct: float | None,
    phase: str,
) -> tuple[str, float, str, str, str]:
    strategy = _REBALANCE_STRATEGY_LABELS.get(symbol, "按计划")
    if symbol == "SGOV":
        return strategy, 0.0, "作为弹药库", "弹药库", "reserve"
    rule = _REBALANCE_RULES.get(phase, {}).get(symbol)
    if rule is None:
        return strategy, 0.0, "按计划", "暂无策略规则", "normal"
    normal_multiplier, normal_action, normal_signal, normal_intensity = rule["normal"]
    if not isinstance(drawdown_pct, (int, float)):
        return strategy, float(normal_multiplier), str(normal_action), str(normal_signal), str(normal_intensity)
    for threshold, multiplier, action, signal, intensity in rule["bands"]:
        if float(drawdown_pct) <= threshold:
            return strategy, float(multiplier), str(action), str(signal), str(intensity)
    return strategy, float(normal_multiplier), str(normal_action), str(normal_signal), str(normal_intensity)


def _rebalance_can_use_sgov_reserve(drawdowns: dict[str, float | None]) -> bool:
    for rules_by_symbol in _REBALANCE_RULES.values():
        for sym, rule in rules_by_symbol.items():
            drawdown = drawdowns.get(sym)
            if not isinstance(drawdown, (int, float)):
                continue
            for threshold, _, _, _, intensity in rule["bands"]:
                if intensity == "large" and float(drawdown) <= threshold:
                    return True
    return False



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
    return {
        "cash_usd": 0.0,
        "cash_cny": 0.0,
        "realized_usd": 0.0,
        "realized_cny": 0.0,
    }


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
    try:
        out["realized_usd"] = float(data.get("realized_usd", 0.0))
    except (TypeError, ValueError):
        out["realized_usd"] = 0.0
    try:
        out["realized_cny"] = float(data.get("realized_cny", 0.0))
    except (TypeError, ValueError):
        out["realized_cny"] = 0.0
    # 兼容旧版按标的保存的“结转余额”
    try:
        legacy_cny = max(0.0, float(data.get("001015", 0.0))) + max(0.0, float(data.get("007994", 0.0)))
    except (TypeError, ValueError):
        legacy_cny = 0.0
    if out["cash_cny"] <= 0 and legacy_cny > 0:
        out["cash_cny"] = legacy_cny
    return out


def _save_balances(balances: dict[str, float]) -> None:
    payload = _default_balances()
    payload["cash_usd"] = float(max(0.0, balances.get("cash_usd", 0.0)))
    payload["cash_cny"] = float(max(0.0, balances.get("cash_cny", 0.0)))
    payload["realized_usd"] = float(balances.get("realized_usd", 0.0))
    payload["realized_cny"] = float(balances.get("realized_cny", 0.0))
    _BALANCE_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_monthly_budget_usage_store() -> dict[str, Any]:
    if not _MONTHLY_BUDGET_USAGE_FILE.exists():
        return {}
    try:
        data = json.loads(_MONTHLY_BUDGET_USAGE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _load_monthly_budget_usage(user_id: str, month_key: str) -> dict[str, Any]:
    store = _load_monthly_budget_usage_store()
    user_key = str(user_id or "local").strip() or "local"
    raw_user = store.get(user_key)
    raw = (raw_user.get(month_key) or {}) if isinstance(raw_user, dict) else {}
    month_data = {
        "used_budget_usd": 0.0,
        "planned_new_cash_usd": 700.0,
        "bought_symbols": [],
        "updated_at": "",
    }
    if isinstance(raw, dict):
        try:
            month_data["used_budget_usd"] = max(0.0, float(raw.get("used_budget_usd", 0.0)))
        except (TypeError, ValueError):
            month_data["used_budget_usd"] = 0.0
        try:
            month_data["planned_new_cash_usd"] = max(0.0, float(raw.get("planned_new_cash_usd", 700.0)))
        except (TypeError, ValueError):
            month_data["planned_new_cash_usd"] = 700.0
        raw_symbols = raw.get("bought_symbols", [])
        if isinstance(raw_symbols, list):
            month_data["bought_symbols"] = [
                str(sym).upper()
                for sym in raw_symbols
                if str(sym).upper() in _ASSET_META and _ASSET_META[str(sym).upper()]["currency"] == "USD"
            ]
        month_data["updated_at"] = str(raw.get("updated_at", ""))
    return month_data


def _save_monthly_budget_usage(
    user_id: str,
    month_key: str,
    used_budget_usd: float,
    bought_symbols: list[str] | None = None,
    planned_new_cash_usd: float = 700.0,
) -> None:
    store = _load_monthly_budget_usage_store()
    user_key = str(user_id or "local").strip() or "local"
    user_store = store.get(user_key)
    if not isinstance(user_store, dict):
        user_store = {}
    try:
        used = max(0.0, float(used_budget_usd))
    except (TypeError, ValueError):
        used = 0.0
    try:
        planned_cash_usd = max(0.0, float(planned_new_cash_usd))
    except (TypeError, ValueError):
        planned_cash_usd = 700.0
    user_store[month_key] = {
        "used_budget_usd": used,
        "planned_new_cash_usd": planned_cash_usd,
        "bought_symbols": [
            str(sym).upper()
            for sym in (bought_symbols or [])
            if str(sym).upper() in _ASSET_META and _ASSET_META[str(sym).upper()]["currency"] == "USD"
        ],
        "updated_at": datetime.now(_TZ_SHANGHAI).isoformat(timespec="seconds"),
    }
    store[user_key] = user_store
    _MONTHLY_BUDGET_USAGE_FILE.write_text(
        json.dumps(store, ensure_ascii=False, indent=2),
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
    try:
        balances["realized_usd"] = float(raw.get("realized_usd", 0.0))
    except (TypeError, ValueError):
        balances["realized_usd"] = 0.0
    try:
        balances["realized_cny"] = float(raw.get("realized_cny", 0.0))
    except (TypeError, ValueError):
        balances["realized_cny"] = 0.0
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
        "avgo": raw["AVGO"],
        "nvda": raw["NVDA"],
        "googl": raw["GOOGL"],
        "msft": raw["MSFT"],
        "isrg": raw["ISRG"],
        "sgov": raw["SGOV"],
        "hs300": raw["001015"],
        "zz500": raw["007994"],
    }


def _ensure_price_session_defaults() -> None:
    d = _defaults_from_fetch()
    st.session_state.setdefault("def_voo", d["voo"])
    st.session_state.setdefault("def_qqq", d["qqq"])
    st.session_state.setdefault("def_avgo", d["avgo"])
    st.session_state.setdefault("def_nvda", d["nvda"])
    st.session_state.setdefault("def_googl", d["googl"])
    st.session_state.setdefault("def_msft", d["msft"])
    st.session_state.setdefault("def_isrg", d["isrg"])
    st.session_state.setdefault("def_sgov", d["sgov"])
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


def _load_chart_boards_api() -> dict[str, Any]:
    mod = importlib.reload(importlib.import_module("chart_boards"))
    configure_market_provider = getattr(mod, "configure_market_provider", None)
    if configure_market_provider is None:
        configure_market_provider = lambda provider=None: provider or "eastmoney"
    get_market_provider = getattr(mod, "get_market_provider", None)
    if get_market_provider is None:
        get_market_provider = lambda: "eastmoney"
    return {
        "CHART_THEME_OPTIONS": getattr(mod, "CHART_THEME_OPTIONS"),
        "configure_market_storage": getattr(mod, "configure_market_storage"),
        "fig_15m_vwap_rsi": getattr(mod, "fig_15m_vwap_rsi"),
        "fig_5m_vwap_rsi7": getattr(mod, "fig_5m_vwap_rsi7"),
        "fig_daily": getattr(mod, "fig_daily"),
        "configure_market_provider": configure_market_provider,
        "get_market_provider": get_market_provider,
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
st.sidebar.caption(f"行情源策略：{_market_data_provider()}")

holdings, balances_for_view, storage_mode = _load_user_state(cloud_user_id)


# --- 刷新市价（并可选同步当前K线到云端）---
refresh_left, refresh_center, refresh_right = st.columns([0.18, 0.64, 0.18])
with refresh_center:
    refresh_prices_clicked = st.button(
        "刷新市价",
        help="拉取现价并刷新默认输入；K线同步在后台定时进行。",
        use_container_width=True,
    )
if refresh_prices_clicked:
    _fetch_spot_prices_meta.clear()
    _fetch_usdcny_rate_meta.clear()
    _fetch_vix_meta.clear()
    _fetch_us_etf_pe_drawdown.clear()
    _fetch_fund_60d_metrics.clear()
    d = _defaults_from_fetch()
    st.session_state.def_fx = _fetch_usdcny_rate()
    st.session_state.def_voo = d["voo"]
    st.session_state.def_qqq = d["qqq"]
    st.session_state.def_avgo = d["avgo"]
    st.session_state.def_nvda = d["nvda"]
    st.session_state.def_googl = d["googl"]
    st.session_state.def_msft = d["msft"]
    st.session_state.def_isrg = d["isrg"]
    st.session_state.def_sgov = d["sgov"]
    st.session_state.def_hs300 = d["hs300"]
    st.session_state.def_zz500 = d["zz500"]

    # 删除输入框缓存值，让下方 number_input 用新的 def_* 作为默认值。
    for k in (
        "inp_fx",
        "inp_voo",
        "inp_qqq",
        "inp_avgo",
        "inp_nvda",
        "inp_googl",
        "inp_msft",
        "inp_isrg",
        "inp_sgov",
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
    "QQQ": float(st.session_state.get("inp_qqq", st.session_state.def_qqq)),
    "AVGO": float(st.session_state.get("inp_avgo", st.session_state.def_avgo)),
    "NVDA": float(st.session_state.get("inp_nvda", st.session_state.def_nvda)),
    "GOOGL": float(st.session_state.get("inp_googl", st.session_state.def_googl)),
    "MSFT": float(st.session_state.get("inp_msft", st.session_state.def_msft)),
    "ISRG": float(st.session_state.get("inp_isrg", st.session_state.def_isrg)),
    "SGOV": float(st.session_state.get("inp_sgov", st.session_state.def_sgov)),
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
            st.markdown("#### 已变现与现金余额（会保存）")
            balances_for_view["realized_usd"] = st.number_input(
                "已变现浮盈亏美元（USD）",
                value=float(balances_for_view.get("realized_usd", 0.0)),
                step=0.01,
                key="edit_balance_realized_usd",
            )
            balances_for_view["realized_cny"] = st.number_input(
                "已变现浮盈亏人民币（CNY）",
                value=float(balances_for_view.get("realized_cny", 0.0)),
                step=0.01,
                key="edit_balance_realized_cny",
            )
            balances_for_view["cash_usd"] = st.number_input(
                "现金美元（USD）",
                min_value=0.0,
                value=float(balances_for_view.get("cash_usd", 0.0)),
                step=0.01,
                key="edit_balance_cash_usd",
            )
            balances_for_view["cash_cny"] = st.number_input(
                "现金人民币（CNY）",
                min_value=0.0,
                value=float(balances_for_view.get("cash_cny", 0.0)),
                step=0.01,
                key="edit_balance_cash_cny",
            )
            if st.form_submit_button("保存持仓"):
                save_mode = _save_user_state(cloud_user_id, holdings, balances_for_view)
                st.success(f"持仓已保存（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")


def _render_chart_board() -> None:
    _chart_symbol_labels = {
        "VOO": "VOO",
        "QQQ": "QQQ",
        "AVGO": "AVGO",
        "NVDA": "NVDA",
        "GOOGL": "GOOGL",
        "MSFT": "MSFT",
        "ISRG": "ISRG",
        "SGOV": "SGOV",
    }
    _chart_label_options = list(_chart_symbol_labels.keys())
    _chart_pick_default_label = "VOO"
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
    chart_provider = _chart_api["configure_market_provider"](_market_data_provider())
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
    st.sidebar.caption(f"K线行情源：{chart_provider}")

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
                _fut_map[
                    _pool.submit(
                        _chart_api["fig_daily"],
                        _chart_yf,
                        _chart_pick,
                        chart_theme=chart_theme,
                        user_avg_cost=_chart_user_avg_cost,
                        cache_only=chart_cache_only,
                    )
                ] = ("1d", "日线（1d）")
            if "15m" in _interval_keys:
                _fut_map[
                    _pool.submit(
                        _chart_api["fig_15m_vwap_rsi"],
                        _chart_yf,
                        _chart_pick,
                        chart_theme=chart_theme,
                        user_avg_cost=None,
                        cache_only=chart_cache_only,
                    )
                ] = ("15m", "15m（15m）")
            if "5m" in _interval_keys:
                _fut_map[
                    _pool.submit(
                        _chart_api["fig_5m_vwap_rsi7"],
                        _chart_yf,
                        _chart_pick,
                        chart_theme=chart_theme,
                        user_avg_cost=None,
                        cache_only=chart_cache_only,
                    )
                ] = ("5m", "5m（5m）")
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
    _plotly_static_axes_config = {
        "scrollZoom": False,
        "displayModeBar": False,
        "doubleClick": False,
    }
    with _tab_d:
        if "1d" in _interval_keys and _fig_d is not None:
            st.plotly_chart(_fig_d, width="stretch", config=_plotly_static_axes_config)
        elif "1d" in _interval_keys:
            st.warning(f"日线图加载失败：{_chart_errs.get('1d', '未知错误')}")
    with _tab_15:
        if "15m" in _interval_keys and _fig_15 is not None:
            st.plotly_chart(_fig_15, width="stretch", config=_plotly_static_axes_config)
        elif "15m" in _interval_keys:
            st.warning(f"15m 图加载失败：{_chart_errs.get('15m', '未知错误')}")
    with _tab_5:
        if "5m" in _interval_keys and _fig_5 is not None:
            st.plotly_chart(_fig_5, width="stretch", config=_plotly_static_axes_config)
        elif "5m" in _interval_keys:
            st.warning(f"5m 图加载失败：{_chart_errs.get('5m', '未知错误')}")


with st.expander("技术看板", expanded=False):
    _chart_should_load = st.toggle(
        "加载技术看板",
        value=False,
        key="chart_board_load_top",
        help="开启后才会加载 K 线模块和行情数据；默认收起时不拉取 K 线。",
    )
    if _chart_should_load:
        _render_chart_board()
    else:
        st.caption("打开“加载技术看板”后显示 K 线图，默认加载日线、15分钟、5分钟。")

rows = []
usd_symbols = ("VOO", "QQQ", *_SATELLITE_SYMBOLS, "SGOV")
total_cost_cny = 0.0
total_value_cny = 0.0
value_cny_by_symbol: dict[str, float] = {}
pnl_cny_by_symbol: dict[str, float] = {}
daily_change_pct_by_symbol: dict[str, float] = spot_meta.get("daily_change_pct_by_symbol", {})  # type: ignore[assignment]
drawdown_pct_by_symbol: dict[str, float | None] = {}
rebound_pct_by_symbol: dict[str, float | None] = {}
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
    row_rebound = _fetch_asset_rebound(sym, meta)
    drawdown_pct_by_symbol[sym] = row_drawdown
    rebound_pct_by_symbol[sym] = row_rebound
    rows.append(
        {
            "标的": meta["label"],
            "币种": meta["currency"],
            "当日涨跌%": round(daily_change_pct_by_symbol.get(sym, 0.0), 2),
            "近60日高点回撤%": round(float(row_drawdown), 2) if isinstance(row_drawdown, (int, float)) else None,
            "近60日低点涨幅%": round(float(row_rebound), 2) if isinstance(row_rebound, (int, float)) else None,
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
realized_usd = float(balances_for_view.get("realized_usd", 0.0))
realized_cny = float(balances_for_view.get("realized_cny", 0.0))
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
    f"<div class='daily-card daily-card-wide' style='--daily-color:{satellite_daily_color};'>"
    "<div class='daily-card-title'>卫星仓位</div>"
    f"<div class='daily-card-pct'>{satellite_daily_pct:+.2f}%</div>"
    f"<div class='daily-card-amount'>USD {satellite_daily_change_usd:+,.2f}<br>≈ CNY {satellite_daily_change_cny:+,.2f}</div>"
    "</div>"
)
daily_card_symbols = (
    "VOO",
    "QQQ",
    "AVGO",
    "NVDA",
    "GOOGL",
    "MSFT",
    "ISRG",
    "SGOV",
    "001015",
    "007994",
)
for sym in daily_card_symbols:
    meta = _ASSET_META[sym]
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
    if sym == "QQQ":
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
            "盈亏标签": f"¥ {pnl_cny_by_symbol[sym]:+,.0f}",
            "方向": "盈利" if pnl_cny_by_symbol[sym] >= 0 else "亏损",
        }
        for sym in _ASSET_META
        if sym not in excluded_core_pnl_symbols
    ]
).sort_values("浮盈亏(CNY)", ascending=False)
pnl_chart_base = alt.Chart(pnl_chart_df)
pnl_chart_positive_bars = (
    pnl_chart_base
    .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
    .transform_filter(alt.datum["浮盈亏(CNY)"] >= 0)
    .encode(
        y=alt.Y("标的:N", sort=list(pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=240)),
        x=alt.X("浮盈亏(CNY):Q", title="浮盈亏(CNY)"),
        color=alt.value("#059669"),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(CNY):Q", format=",.2f"), "方向:N"],
    )
)
pnl_chart_negative_bars = (
    pnl_chart_base
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusBottomLeft=6)
    .transform_filter(alt.datum["浮盈亏(CNY)"] < 0)
    .encode(
        y=alt.Y("标的:N", sort=list(pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=240)),
        x=alt.X("浮盈亏(CNY):Q", title="浮盈亏(CNY)"),
        color=alt.value("#e11d48"),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(CNY):Q", format=",.2f"), "方向:N"],
    )
)
pnl_chart_positive_labels = (
    pnl_chart_base
    .mark_text(align="left", baseline="middle", dx=8, fontSize=13, fontWeight=700)
    .transform_filter(alt.datum["浮盈亏(CNY)"] >= 0)
    .encode(
        y=alt.Y("标的:N", sort=list(pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=240)),
        x=alt.X("浮盈亏(CNY):Q"),
        text="盈亏标签:N",
        color=alt.value("#047857"),
    )
)
pnl_chart_negative_labels = (
    pnl_chart_base
    .mark_text(align="right", baseline="middle", dx=-8, fontSize=13, fontWeight=700)
    .transform_filter(alt.datum["浮盈亏(CNY)"] < 0)
    .encode(
        y=alt.Y("标的:N", sort=list(pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=240)),
        x=alt.X("浮盈亏(CNY):Q"),
        text="盈亏标签:N",
        color=alt.value("#be123c"),
    )
)
pnl_chart = (
    (pnl_chart_positive_bars + pnl_chart_negative_bars + pnl_chart_positive_labels + pnl_chart_negative_labels)
    .properties(title="核心仓位浮盈亏排名（不含卫星仓位，折合CNY）", height=max(260, 46 * len(pnl_chart_df)))
    .configure_view(stroke=None)
    .configure_axisY(labelLimit=240, labelPadding=8)
)
st.altair_chart(_theme_altair_chart(pnl_chart, theme), width="stretch")

satellite_pnl_chart_df = pd.DataFrame(
    [
        {
            "标的": sym,
            "浮盈亏(USD)": round(pnl_usd, 2),
            "盈亏标签": f"$ {pnl_usd:+,.2f}",
            "方向": "盈利" if pnl_usd >= 0 else "亏损",
        }
        for sym in _SATELLITE_SYMBOLS
        for pnl_usd in [
            float(holdings.get(sym, {}).get("shares", 0.0))
            * (float(prices_now.get(sym, 0.0)) - float(holdings.get(sym, {}).get("avg_cost", 0.0)))
        ]
    ]
).sort_values("浮盈亏(USD)", ascending=False)
satellite_pnl_chart_base = alt.Chart(satellite_pnl_chart_df)
satellite_pnl_chart_positive_bars = (
    satellite_pnl_chart_base
    .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
    .transform_filter(alt.datum["浮盈亏(USD)"] >= 0)
    .encode(
        y=alt.Y("标的:N", sort=list(satellite_pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=160)),
        x=alt.X("浮盈亏(USD):Q", title="浮盈亏(USD)"),
        color=alt.value("#059669"),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(USD):Q", format=",.2f"), "方向:N"],
    )
)
satellite_pnl_chart_negative_bars = (
    satellite_pnl_chart_base
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusBottomLeft=6)
    .transform_filter(alt.datum["浮盈亏(USD)"] < 0)
    .encode(
        y=alt.Y("标的:N", sort=list(satellite_pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=160)),
        x=alt.X("浮盈亏(USD):Q", title="浮盈亏(USD)"),
        color=alt.value("#e11d48"),
        tooltip=["标的:N", alt.Tooltip("浮盈亏(USD):Q", format=",.2f"), "方向:N"],
    )
)
satellite_pnl_chart_positive_labels = (
    satellite_pnl_chart_base
    .mark_text(align="left", baseline="middle", dx=8, fontSize=13, fontWeight=700)
    .transform_filter(alt.datum["浮盈亏(USD)"] >= 0)
    .encode(
        y=alt.Y("标的:N", sort=list(satellite_pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=160)),
        x=alt.X("浮盈亏(USD):Q"),
        text="盈亏标签:N",
        color=alt.value("#047857"),
    )
)
satellite_pnl_chart_negative_labels = (
    satellite_pnl_chart_base
    .mark_text(align="right", baseline="middle", dx=-8, fontSize=13, fontWeight=700)
    .transform_filter(alt.datum["浮盈亏(USD)"] < 0)
    .encode(
        y=alt.Y("标的:N", sort=list(satellite_pnl_chart_df["标的"]), title=None, axis=alt.Axis(labelLimit=160)),
        x=alt.X("浮盈亏(USD):Q"),
        text="盈亏标签:N",
        color=alt.value("#be123c"),
    )
)
satellite_pnl_chart = (
    (
        satellite_pnl_chart_positive_bars
        + satellite_pnl_chart_negative_bars
        + satellite_pnl_chart_positive_labels
        + satellite_pnl_chart_negative_labels
    )
    .properties(title="卫星仓位浮盈亏排名（USD）", height=max(240, 42 * len(satellite_pnl_chart_df)))
    .configure_view(stroke=None)
    .configure_axisY(labelLimit=160, labelPadding=8)
)
st.altair_chart(_theme_altair_chart(satellite_pnl_chart, theme), width="stretch")

usd_target_weight_total = sum(_TARGET_WEIGHTS[sym] for sym in usd_symbols)
usd_position_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in usd_symbols)
usd_extra_value_cny = cash_usd * fx
usd_total_cny = usd_position_value_cny + usd_extra_value_cny
usd_total_usd = (usd_total_cny / fx) if fx > 0 else 0.0


def _usd_target_pct(sym: str) -> float:
    return (_TARGET_WEIGHTS[sym] / usd_target_weight_total * 100.0) if usd_target_weight_total > 0 else 0.0

voo_current = value_cny_by_symbol.get("VOO", 0.0)
qqq_current = value_cny_by_symbol.get("QQQ", 0.0)
sgov_current = value_cny_by_symbol.get("SGOV", 0.0)

ratio_denominator = usd_total_cny if usd_total_cny > 0 else 0.0
voo_ratio = (voo_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
qqq_ratio = (qqq_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
avgo_ratio = (value_cny_by_symbol.get("AVGO", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
nvda_ratio = (value_cny_by_symbol.get("NVDA", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
googl_ratio = (value_cny_by_symbol.get("GOOGL", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
msft_ratio = (value_cny_by_symbol.get("MSFT", 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
satellite_ratio_by_symbol = {
    sym: (value_cny_by_symbol.get(sym, 0.0) / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
    for sym in _SATELLITE_SYMBOLS
}
new4_ratio = sum(satellite_ratio_by_symbol.values())
sgov_ratio = (sgov_current / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0
cash_usd_ratio = (usd_extra_value_cny / ratio_denominator * 100.0) if ratio_denominator > 0 else 0.0

voo_target = _usd_target_pct("VOO")
qqq_target = _usd_target_pct("QQQ")
new4_target = (
    sum(_TARGET_WEIGHTS[sym] for sym in _SATELLITE_SYMBOLS) / usd_target_weight_total * 100.0
) if usd_target_weight_total > 0 else 0.0
sgov_target = _usd_target_pct("SGOV")

group1_df = pd.DataFrame(
    [
        {"标的组": "VOO", "类型": "当前比例%", "成分": "VOO", "比例%": round(voo_ratio, 2)},
        {"标的组": "VOO", "类型": "目标比例%", "成分": "目标", "比例%": round(voo_target, 2)},
        {"标的组": "QQQ", "类型": "当前比例%", "成分": "QQQ", "比例%": round(qqq_ratio, 2)},
        {"标的组": "QQQ", "类型": "目标比例%", "成分": "目标", "比例%": round(qqq_target, 2)},
        {"标的组": "卫星仓位", "类型": "当前比例%", "成分": "卫星仓位", "比例%": round(new4_ratio, 2)},
        {"标的组": "卫星仓位", "类型": "目标比例%", "成分": "目标", "比例%": round(new4_target, 2)},
        {"标的组": "短债", "类型": "当前比例%", "成分": "SGOV", "比例%": round(sgov_ratio, 2)},
        {"标的组": "短债", "类型": "目标比例%", "成分": "目标", "比例%": round(sgov_target, 2)},
        {"标的组": "现金", "类型": "当前比例%", "成分": "现金", "比例%": round(cash_usd_ratio, 2)},
        {"标的组": "现金", "类型": "目标比例%", "成分": "目标", "比例%": 0.0},
    ]
)

group1_chart = (
    alt.Chart(group1_df)
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的组:N", sort=["VOO", "QQQ", "卫星仓位", "短债", "现金"]),
        xOffset=alt.XOffset("类型:N", sort=["当前比例%", "目标比例%"]),
        y=alt.Y("比例%:Q", title="比例(%)"),
        color=alt.Color(
            "成分:N",
            sort=["VOO", "QQQ", "卫星仓位", "SGOV", "现金", "目标"],
            scale=alt.Scale(
                domain=["VOO", "QQQ", "卫星仓位", "SGOV", "现金", "目标"],
                range=["#2563eb", "#06b6d4", "#a855f7", "#f59e0b", "#64748b", "#94a3b8"],
            ),
        ),
        order=alt.Order("成分:N", sort="ascending"),
        tooltip=["标的组:N", "类型:N", "成分:N", alt.Tooltip("比例%:Q", format=".2f")],
    )
    .properties(title="VOO / QQQ / 卫星仓位 / 短债(SGOV) / 现金 当前与目标对比")
)
tech_denominator = sum(satellite_ratio_by_symbol.values())
satellite_target_parts_total = sum(_SATELLITE_TARGET_PARTS.values())
tech_split_df = pd.DataFrame(
    [
        row
        for sym in _SATELLITE_SYMBOLS
        for row in (
            {
                "标的": sym,
                "类型": "当前占卫星仓位%",
                "比例%": round(
                    (satellite_ratio_by_symbol[sym] / tech_denominator * 100.0) if tech_denominator > 0 else 0.0,
                    2,
                ),
                "浮盈亏(CNY)": round(pnl_cny_by_symbol.get(sym, 0.0), 2),
                "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get(sym, 0.0):+,.0f}",
            },
            {
                "标的": sym,
                "类型": "目标占卫星仓位%",
                "比例%": round(_SATELLITE_TARGET_PARTS[sym] / satellite_target_parts_total * 100.0, 2),
                "浮盈亏(CNY)": None,
                "浮盈亏标签": "",
            },
        )
    ]
)
tech_split_base = alt.Chart(tech_split_df)
tech_split_bars = (
    tech_split_base
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("标的:N", sort=list(_SATELLITE_SYMBOLS)),
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
        ],
    )
)
tech_split_chart = (
    tech_split_bars
    .properties(title="卫星仓位内部占比（目标：AVGO/NVDA/GOOGL/MSFT/ISRG = 3:2:3:2:5）")
)
st.altair_chart(_theme_altair_chart(tech_split_chart, theme), width="stretch")
st.altair_chart(_theme_altair_chart(group1_chart, theme), width="stretch")

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
        "近60日低点涨幅%": st.column_config.NumberColumn("近60日低点涨幅%", format="%.2f%%"),
        "涨跌幅%": st.column_config.NumberColumn("涨跌幅%", format="%.2f%%"),
    },
)
usd_cost_usd = sum(float(holdings[sym]["shares"]) * float(holdings[sym]["avg_cost"]) for sym in usd_symbols)
usd_position_value_usd = sum(
    float(holdings[sym]["shares"]) * float(prices_now.get(sym, 0.0))
    for sym in usd_symbols
)
usd_unrealized_pnl_usd = usd_position_value_usd - usd_cost_usd
usd_unrealized_return_pct = (usd_unrealized_pnl_usd / usd_cost_usd * 100.0) if usd_cost_usd > 0 else 0.0
usd_unrealized_asset_pct = (usd_unrealized_pnl_usd / usd_total_usd * 100.0) if usd_total_usd > 0 else 0.0
total_realized_cny = realized_usd * fx + realized_cny
total_unrealized_asset_pct = (total_pnl_cny / total_assets_cny * 100.0) if total_assets_cny > 0 else 0.0

asset_col_usd, asset_col_total = st.columns(2)
with asset_col_usd:
    st.markdown("#### 美元资产")
    usd_m1, usd_m2 = st.columns(2)
    usd_m1.metric("已变现浮盈亏", f"$ {realized_usd:,.2f}")
    usd_m2.metric(
        "未变现浮盈亏",
        f"$ {usd_unrealized_pnl_usd:,.2f}",
        delta=f"{usd_unrealized_asset_pct:.2f}%",
        delta_color=theme["delta_color"],
    )
    st.caption(
        f"成本 USD {usd_cost_usd:,.2f} ｜ 持仓市值 USD {usd_position_value_usd:,.2f} ｜ "
        f"已变现浮盈亏 USD {realized_usd:,.2f} ｜ 现金 USD {cash_usd:,.2f} ｜ "
        f"总资产 USD {usd_total_usd:,.2f} ｜ 收益率 = 未变现浮盈亏 / 美元持仓成本 = {usd_unrealized_return_pct:.2f}%"
    )

with asset_col_total:
    st.markdown("#### 总资产（折合CNY）")
    total_m1, total_m2 = st.columns(2)
    total_m1.metric("已变现浮盈亏", f"¥ {total_realized_cny:,.2f}")
    total_m2.metric(
        "未变现浮盈亏",
        f"¥ {total_pnl_cny:,.2f}",
        delta=f"{total_unrealized_asset_pct:.2f}%",
        delta_color=theme["delta_color"],
    )
    st.caption(
        f"成本 CNY {total_cost_cny:,.2f} ｜ 持仓市值 CNY {total_value_cny:,.2f} ｜ "
        f"已变现浮盈亏 CNY {total_realized_cny:,.2f} ｜ 现金 CNY {total_balance_cny:,.2f} ｜ "
        f"总资产 CNY {total_assets_cny:,.2f} ｜ 收益率 = 未变现浮盈亏 / 持仓成本 = {total_pnl_pct:.2f}%"
    )

st.markdown("<br>", unsafe_allow_html=True)
rebalance_title_col, rebalance_rule_col = st.columns([0.78, 0.22], vertical_alignment="center")
with rebalance_title_col:
    st.subheader("🧮 再平衡建议")
with rebalance_rule_col:
    with st.popover("ⓘ 算法规则", width="stretch"):
        st.markdown("#### 长期目标")
        st.dataframe(pd.DataFrame(_REBALANCE_ALLOCATION_ROWS), width="stretch", hide_index=True)
        st.markdown(
            """
#### 回撤定义
使用 60 日最高价回撤：

`drawdown = (current_price - max_60d_price) / max_60d_price`

#### 阶段判断
- **建仓期**：SGOV 明显高于 20% 目标，或权益仓位未达到目标权益仓位的 90%。
- **长期定投期**：基本达到目标仓位后，主要靠新增工资定投。

#### SGOV 规则
- 本表只使用当前已经在账户里的美元现金和 SGOV，不预估未来新增工资。
- 每月新增 5000 RMB 由前面的持仓/现金编辑录入后，再参与下一次规划。
- 建仓期先按当前持仓计算每个标的到目标的缺口，再按剩余月数拆成月度推进量。
- SGOV 超过 20% 目标的部分，可以随时挪用。
- 目标内 20% SGOV 默认保留，只有触发大加档才动用。
- 一旦触发大加/恐慌级档位，SGOV 可以全部动用，之后用后续新增资金再补回。
"""
        )
        st.markdown("#### 建仓期规则")
        st.dataframe(
            pd.DataFrame(
                [
                    {"标的": "VOO", "小加": "-5% / 1.5x", "中加": "-10% / 2x", "大加": "-15% / 3x + 少量SGOV"},
                    {"标的": "QQQ", "小加": "-6% / 1.5x", "中加": "-12% / 2.5x", "大加": "-18% / 4x + 动用SGOV"},
                    {"标的": "MSFT", "小加": "-8% / 1.5x", "中加": "-15% / 2.5x", "大加": "-22% / 4x"},
                    {"标的": "GOOGL", "小加": "-10% / 1.5x", "中加": "-18% / 2.5x", "大加": "-25% / 4x"},
                    {"标的": "NVDA", "小加": "-12% / 1.5x", "中加": "-20% / 3x", "大加": "-30% / 5x + 明显动用SGOV"},
                    {"标的": "AVGO", "小加": "-10% / 1.5x", "中加": "-18% / 2.5x", "大加": "-28% / 4x"},
                    {"标的": "ISRG", "小加": "-10% / 1.5x", "中加": "-18% / 2.5x", "大加": "-28% / 4x"},
                ]
            ),
            width="stretch",
            hide_index=True,
        )
        st.markdown("#### 长期定投期规则")
        st.dataframe(
            pd.DataFrame(
                [
                    {"标的": "VOO", "正常": "正常定投", "小加": "-5% 多买一点", "中加": "-10% 明显加仓", "大加": "-15% 大加"},
                    {"标的": "QQQ", "正常": "小额定投", "小加": "-6% 增加投入", "中加": "-12% 明显加仓", "大加": "-18% 大加"},
                    {"标的": "MSFT", "正常": "持续小买", "小加": "-8% 多买", "中加": "-15% 明显加", "大加": "-22% 大加"},
                    {"标的": "GOOGL", "正常": "持续小买", "小加": "-10% 多买", "中加": "-18% 明显加", "大加": "-25% 大加"},
                    {"标的": "NVDA", "正常": "少量/观察", "小加": "-12% 小加", "中加": "-20% 明显加", "大加": "-30% AI恐慌级"},
                    {"标的": "AVGO", "正常": "观察/小买", "小加": "-10% 小加", "中加": "-18% 明显加", "大加": "-28% 大加"},
                    {"标的": "ISRG", "正常": "长期慢买", "小加": "-10% 多买", "中加": "-18% 明显加", "大加": "-28% 大加"},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

with st.expander("再平衡买入建议", expanded=False):
    if usd_total_cny <= 0:
        st.info("美元资产总额为 0，暂无法生成再平衡建议。")
    else:
        sgov_current_usd = (value_cny_by_symbol.get("SGOV", 0.0) / fx) if fx > 0 else 0.0
        sgov_target_pct = (
            _TARGET_WEIGHTS.get("SGOV", 0.0) / usd_target_weight_total
            if usd_target_weight_total > 0
            else 0.0
        )
        equity_current_usd = (
            sum(value_cny_by_symbol.get(sym, 0.0) / fx for sym in usd_symbols if sym != "SGOV")
            if fx > 0
            else 0.0
        )
        equity_target_pct = max(0.0, 1.0 - sgov_target_pct)
        equity_current_pct = (equity_current_usd / usd_total_usd) if usd_total_usd > 0 else 0.0
        sgov_current_pct = (sgov_current_usd / usd_total_usd) if usd_total_usd > 0 else 0.0
        auto_phase = (
            _REBALANCE_PHASE_BUILD
            if sgov_current_pct > sgov_target_pct + 0.05 or equity_current_pct < equity_target_pct * 0.90
            else _REBALANCE_PHASE_DCA
        )
    
        default_build_months = 1
        now_for_build = datetime.now(_TZ_SHANGHAI)
        build_end_year = 2026
        build_end_month = 10
        if now_for_build.year < build_end_year or (
            now_for_build.year == build_end_year and now_for_build.month <= build_end_month
        ):
            default_build_months = max(
                1,
                (build_end_year - now_for_build.year) * 12 + build_end_month - now_for_build.month + 1,
            )
    
        st.session_state.setdefault("rebalance_wizard_step", "1 确认阶段")
        st.session_state.setdefault("rebalance_generated", False)
        rebalance_steps = ["1 确认阶段", "2 预算与记录", "3 生成建议"]
        current_wizard_step = str(st.session_state.get("rebalance_wizard_step", rebalance_steps[0]))
        if current_wizard_step not in rebalance_steps:
            current_wizard_step = rebalance_steps[0]
        wizard_step = st.radio(
            "再平衡买入流程",
            rebalance_steps,
            index=rebalance_steps.index(current_wizard_step),
            horizontal=True,
        )
        if wizard_step != current_wizard_step:
            st.session_state["rebalance_wizard_step"] = wizard_step
            st.session_state["rebalance_generated"] = False
    
        if wizard_step == "1 确认阶段":
            st.markdown("##### 1. 确认阶段")
            cfg_c1, cfg_c2 = st.columns(2)
            with cfg_c1:
                phase_choice = st.selectbox(
                    "策略阶段",
                    ["自动判断", _REBALANCE_PHASE_BUILD, _REBALANCE_PHASE_DCA],
                    key="rebalance_phase_choice",
                )
            with cfg_c2:
                build_month_input_kwargs: dict[str, Any] = {
                    "label": "建仓剩余月数（到11月前）",
                    "min_value": 1,
                    "max_value": 24,
                    "step": 1,
                    "key": "rebalance_build_months",
                }
                if "rebalance_build_months" not in st.session_state:
                    build_month_input_kwargs["value"] = int(default_build_months)
                build_months = st.number_input(**build_month_input_kwargs)
            st.caption(
                f"自动判断：{auto_phase}。建仓期默认按当前持仓缺口和剩余月份推进，不把平均进度当成硬性月预算。"
            )
            if st.button("确认阶段，下一步", key="rebalance_confirm_phase"):
                st.session_state["rebalance_wizard_step"] = "2 预算与记录"
                st.session_state["rebalance_generated"] = False
                st.rerun()
        else:
            phase_choice = str(st.session_state.get("rebalance_phase_choice", "自动判断"))
            build_months = int(st.session_state.get("rebalance_build_months", default_build_months))
    
        rebalance_phase = auto_phase if phase_choice == "自动判断" else phase_choice
        can_use_sgov_reserve = _rebalance_can_use_sgov_reserve(drawdown_pct_by_symbol)
        build_month_count = max(1, int(build_months))
    
        current_month_key = now_for_build.strftime("%Y-%m")
        budget_usage = _load_monthly_budget_usage(cloud_user_id, current_month_key)
        used_budget_usd = float(budget_usage.get("used_budget_usd", 0.0))
        planned_new_cash_usd = float(budget_usage.get("planned_new_cash_usd", 700.0))
        planned_total_new_cash_usd = planned_new_cash_usd * build_month_count
        planned_usd_total_usd = usd_total_usd + planned_total_new_cash_usd
        planned_sgov_target_usd = sgov_target_pct * planned_usd_total_usd
        sgov_excess_usd = max(0.0, sgov_current_usd - planned_sgov_target_usd)
        planned_sgov_gap_usd = max(0.0, planned_sgov_target_usd - sgov_current_usd)
        planned_new_cash_sgov_reserve_usd = 0.0 if can_use_sgov_reserve else min(
            planned_new_cash_usd * sgov_target_pct,
            planned_sgov_gap_usd,
        )
        planned_new_cash_deployable_usd = max(0.0, planned_new_cash_usd - planned_new_cash_sgov_reserve_usd)
        sgov_special_deploy_usd = (
            max(0.0, sgov_current_usd - sgov_excess_usd) if can_use_sgov_reserve else 0.0
        )
        current_deployable_pool_usd = max(0.0, cash_usd) + sgov_excess_usd + sgov_special_deploy_usd
        deployable_pool_usd = current_deployable_pool_usd + planned_new_cash_deployable_usd
        bought_symbols_this_month = [
            sym for sym in budget_usage.get("bought_symbols", []) if sym in usd_symbols and sym != "SGOV"
        ]
        save_execution = False
        clear_execution = False
        edited_used_budget_usd = used_budget_usd
        edited_planned_new_cash_usd = planned_new_cash_usd
        edited_bought_symbols = bought_symbols_this_month
        if wizard_step == "2 预算与记录":
            st.markdown("##### 2. 预算与本月记录")
            st.caption(
                f"记录 {current_month_key} 已经用掉的建仓预算，保存到 monthly_budget_usage.json。"
                "每月计划新投入只用于生成建议，不自动修改持仓或现金；真实买入后你继续在前面的持仓/现金区更新。"
            )
            with st.form("monthly_budget_usage_form"):
                edited_planned_new_cash_usd = st.number_input(
                    "每月计划新投入(USD)",
                    min_value=0.0,
                    value=planned_new_cash_usd,
                    step=50.0,
                    format="%.2f",
                    key=f"monthly_planned_cash_{current_month_key}",
                )
                edited_used_budget_usd = st.number_input(
                    "本月已用预算(USD)",
                    min_value=0.0,
                    value=used_budget_usd,
                    step=10.0,
                    format="%.2f",
                    key=f"monthly_budget_used_{current_month_key}",
                )
                buyable_symbols = [sym for sym in usd_symbols if sym != "SGOV"]
                edited_bought_symbols = st.multiselect(
                    "本月已经买过的标的（正常/小加不重复；中加/大加继续建议）",
                    options=buyable_symbols,
                    default=bought_symbols_this_month,
                    key=f"monthly_budget_bought_symbols_{current_month_key}",
                )
                save_exec_col, clear_exec_col = st.columns([0.25, 0.25])
                save_execution = save_exec_col.form_submit_button("保存并下一步")
                clear_execution = clear_exec_col.form_submit_button("清零本月已用")
        if save_execution:
            used_budget_usd = float(edited_used_budget_usd)
            planned_new_cash_usd = float(edited_planned_new_cash_usd)
            bought_symbols_this_month = [str(sym).upper() for sym in edited_bought_symbols]
            _save_monthly_budget_usage(
                cloud_user_id,
                current_month_key,
                used_budget_usd,
                bought_symbols_this_month,
                planned_new_cash_usd,
            )
            st.session_state["rebalance_wizard_step"] = "3 生成建议"
            st.session_state["rebalance_generated"] = False
            st.success("已保存本月预算使用。")
            st.rerun()
        if clear_execution:
            used_budget_usd = 0.0
            bought_symbols_this_month = []
            _save_monthly_budget_usage(
                cloud_user_id,
                current_month_key,
                used_budget_usd,
                bought_symbols_this_month,
                planned_new_cash_usd,
            )
            st.success("已清零本月已用预算。")
    
        strategy_rows: list[dict[str, Any]] = []
        full_rebalance_need_usd = 0.0
        for sym in usd_symbols:
            meta = _ASSET_META[sym]
            current_cny = value_cny_by_symbol.get(sym, 0.0)
            current_usd = (current_cny / fx) if fx > 0 else 0.0
            target_pct = (_TARGET_WEIGHTS.get(sym, 0.0) / usd_target_weight_total) if usd_target_weight_total > 0 else 0.0
            target_usd = target_pct * planned_usd_total_usd
            gap_usd = target_usd - current_usd
            drawdown_pct = drawdown_pct_by_symbol.get(sym)
            strategy, multiplier, action, signal, intensity = _rebalance_strategy_signal(sym, drawdown_pct, rebalance_phase)
            already_bought_this_month = sym != "SGOV" and sym in bought_symbols_this_month
            if already_bought_this_month and intensity not in {"medium", "large"}:
                continue
            base_budget_usd = max(0.0, gap_usd) / build_month_count
            raw_buy_usd = min(max(0.0, gap_usd), base_budget_usd * multiplier)
            if sym == "SGOV":
                raw_buy_usd = 0.0
                action = "作为弹药库"
                if can_use_sgov_reserve:
                    note = "触发大加档，SGOV 可以全部动用；后续用新增资金补回。"
                elif sgov_excess_usd > 0:
                    note = "超过 20% 目标和底线的部分可随时挪用；目标内 20% 继续保留。"
                else:
                    note = "目标内 20% 作为常规弹药库，只有大加档才动用。"
            elif gap_usd <= 0:
                action = "暂不买入"
                note = "当前已达到或高于目标仓位。"
            elif intensity == "large":
                note = "大加档：可以动用全部 SGOV，后续再补回弹药库。"
            elif intensity == "medium":
                note = "中加档：可少量动用 SGOV，优先使用月现金流。"
            elif intensity == "small":
                note = "小加档：主要使用当月现金流，尽量不动 SGOV。"
            else:
                note = "按当前阶段的常规月度节奏执行。"
            if already_bought_this_month and intensity in {"medium", "large"}:
                note = f"本月已买过，但现在升级到{action}档，允许继续给出加仓建议。"
            if sym != "SGOV":
                full_rebalance_need_usd += max(0.0, gap_usd)
            signal_remaining_usd = max(0.0, raw_buy_usd)
            strategy_rows.append(
                {
                    "标的": meta["label"],
                    "阶段": rebalance_phase,
                    "策略": strategy,
                    "动作": action,
                    "当前占美元资产%": round(
                        (current_usd / planned_usd_total_usd * 100.0) if planned_usd_total_usd > 0 else 0.0,
                        2,
                    ),
                    "目标占美元资产%": round(target_pct * 100.0, 2),
                    "到目标缺口(USD)": round(gap_usd, 2),
                    "60日回撤%": round(float(drawdown_pct), 2) if isinstance(drawdown_pct, (int, float)) else None,
                    "回撤档位": signal,
                    "建议买入(USD)": 0.0,
                    "说明": note,
                    "_raw_buy_usd": raw_buy_usd,
                    "_signal_remaining_usd": signal_remaining_usd,
                }
            )
    
        remaining_signal_buy_usd = sum(row["_signal_remaining_usd"] for row in strategy_rows)
        monthly_budget_usd = min(deployable_pool_usd, full_rebalance_need_usd) / build_month_count
        total_executed_usd = max(0.0, used_budget_usd)
        remaining_reference_budget_usd = max(0.0, monthly_budget_usd - total_executed_usd)
        remaining_deployable_budget_usd = max(0.0, deployable_pool_usd - total_executed_usd)
        suggested_run_budget_usd = min(
            remaining_deployable_budget_usd,
            max(remaining_reference_budget_usd, remaining_signal_buy_usd),
        )
        if wizard_step == "3 生成建议":
            st.markdown("##### 3. 生成买入建议")
            preview_c1, preview_c2, preview_c3, preview_c4 = st.columns(4)
            preview_c1.metric("计划期末资产(USD)", f"{planned_usd_total_usd:,.2f}")
            preview_c2.metric("本月新增可买(USD)", f"{planned_new_cash_deployable_usd:,.2f}")
            preview_c3.metric("本轮可用资金(USD)", f"{remaining_deployable_budget_usd:,.2f}")
            preview_c4.metric("当前信号建议(USD)", f"{remaining_signal_buy_usd:,.2f}")
            run_budget_usd = st.number_input(
                "本轮最高可用预算(USD)",
                min_value=0.0,
                max_value=max(0.0, remaining_deployable_budget_usd),
                value=float(suggested_run_budget_usd),
                step=50.0,
                format="%.2f",
                key=f"rebalance_run_budget_{current_month_key}",
                help="月度参考不是硬上限。没触发可以少用；触发中加/大加时，可以把没用掉的资金滚到本轮一起用。",
            )
            action_c1, action_c2 = st.columns([0.25, 0.75])
            if action_c1.button("生成买入建议", type="primary", key="rebalance_generate_btn"):
                st.session_state["rebalance_generated"] = True
            if action_c2.button("返回修改预算", key="rebalance_back_to_budget"):
                st.session_state["rebalance_wizard_step"] = "2 预算与记录"
                st.session_state["rebalance_generated"] = False
                st.rerun()
        else:
            run_budget_usd = float(suggested_run_budget_usd)
        strategy_budget_usd = min(run_budget_usd, remaining_signal_buy_usd)
        waiting_trigger_usd = max(0.0, remaining_deployable_budget_usd - strategy_budget_usd)
        cash_scale = (strategy_budget_usd / remaining_signal_buy_usd) if remaining_signal_buy_usd > 0 else 0.0
        for row in strategy_rows:
            buy_usd = row["_signal_remaining_usd"] * cash_scale
            row["建议买入(USD)"] = round(buy_usd, 2)
    
        strategy_df = pd.DataFrame(strategy_rows).sort_values(
            by=["_signal_remaining_usd", "到目标缺口(USD)"],
            ascending=[False, False],
        )
        strategy_df = strategy_df.drop(columns=["_raw_buy_usd", "_signal_remaining_usd"])
        if wizard_step != "3 生成建议":
            st.stop()
        if not st.session_state.get("rebalance_generated", False):
            st.info("确认本轮预算后，点击「生成买入建议」查看结果。")
            st.stop()
        st.caption(
            f"自动判断阶段：{auto_phase}；当前执行阶段：{rebalance_phase}。"
            f"本轮按当前现金/SGOV可动用资金 USD {current_deployable_pool_usd:,.2f} "
            f"+ 本月计划新投入可买部分 USD {planned_new_cash_deployable_usd:,.2f} 生成建议。"
            f"目标分母按当前美元资产 USD {usd_total_usd:,.2f} "
            f"+ 未来 {int(build_months)} 个月新投入 USD {planned_total_new_cash_usd:,.2f} "
            f"= USD {planned_usd_total_usd:,.2f} 计算。"
            f"当前计划可动用建仓池 USD {deployable_pool_usd:,.2f}，按计划期末仓位缺口和 {int(build_months)} 个月进度计算，"
            f"月度参考进度 USD {monthly_budget_usd:,.2f}。"
            f"本月已用预算 USD {total_executed_usd:,.2f}，参考剩余额度 USD {remaining_reference_budget_usd:,.2f}，"
            f"剩余可动用建仓池 USD {remaining_deployable_budget_usd:,.2f}。"
            f"本月已买过标的：{', '.join(bought_symbols_this_month) if bought_symbols_this_month else '无'}"
            "（正常/小加不重复；若跌到中加/大加会重新建议）。"
            f"SGOV 当前 USD {sgov_current_usd:,.2f}，计划期末 20% 目标 USD {planned_sgov_target_usd:,.2f}，"
            f"本月新增资金中预留给 SGOV USD {planned_new_cash_sgov_reserve_usd:,.2f}，"
            f"可随时挪用 SGOV USD {sgov_excess_usd:,.2f}，"
            f"{'已触发大加档，可额外动用目标内 SGOV USD ' + format(sgov_special_deploy_usd, ',.2f') + '（可用完，后续补回）' if can_use_sgov_reserve else '未触发大加档，目标内 20% SGOV 暂不动用'}。"
            f"当前信号建议 USD {remaining_signal_buy_usd:,.2f}；"
            f"本轮最高可用预算 USD {run_budget_usd:,.2f}；"
            f"最终建议买入 USD {strategy_budget_usd:,.2f}；"
            f"留待后续/等待触发资金 USD {waiting_trigger_usd:,.2f}。"
        )
        st.dataframe(
            strategy_df,
            width="stretch",
            hide_index=True,
            column_config={
                "当前占美元资产%": st.column_config.NumberColumn("当前占美元资产%", format="%.2f%%"),
                "目标占美元资产%": st.column_config.NumberColumn("目标占美元资产%", format="%.2f%%"),
                "到目标缺口(USD)": st.column_config.NumberColumn("到目标缺口(USD)", format="%.2f"),
                "60日回撤%": st.column_config.NumberColumn("60日回撤%", format="%.2f%%"),
                "建议买入(USD)": st.column_config.NumberColumn("建议买入(USD)", format="%.2f"),
            },
        )
    
