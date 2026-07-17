import re
import json
import base64
import importlib
import os
import socket
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

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

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
    "TEM": 60.0,
    "PLTR": 100.0,
    "GOOGL": 180.0,
    "MSFT": 420.0,
    "ISRG": 450.0,
    "SGOV": 100.0,
    "001015": 1.0,
}

_TICKERS = {
    "voo": "VOO",
    "qqq": "QQQ",
    "avgo": "AVGO",
    "nvda": "NVDA",
    "tem": "TEM",
    "pltr": "PLTR",
    "googl": "GOOGL",
    "msft": "MSFT",
    "isrg": "ISRG",
    "sgov": "SGOV",
    "hs300": "001015",  # 华夏沪深300指数增强A
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
    "TEM": "usTEM",
    "PLTR": "usPLTR",
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
    "TEM": "usTEM.N",
    "PLTR": "usPLTR.N",
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
    "TEM": "gb_tem",
    "PLTR": "gb_pltr",
    "GOOGL": "gb_googl",
    "MSFT": "gb_msft",
    "ISRG": "gb_isrg",
    "SGOV": "gb_sgov",
}
_FUTU_US = {
    "VOO": "US.VOO",
    "QQQ": "US.QQQ",
    "AVGO": "US.AVGO",
    "NVDA": "US.NVDA",
    "TEM": "US.TEM",
    "PLTR": "US.PLTR",
    "GOOGL": "US.GOOGL",
    "MSFT": "US.MSFT",
    "ISRG": "US.ISRG",
    "SGOV": "US.SGOV",
}
_FUND_CODES = {"001015": "001015"}
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
    "TEM": "106.TEM",
    "PLTR": "106.PLTR",
    "GOOGL": "105.GOOGL",
    "MSFT": "105.MSFT",
    "ISRG": "105.ISRG",
    "SGOV": "106.SGOV",
}
_US_MARKET_SYMBOLS = ("VOO", "QQQ", "AVGO", "NVDA", "TEM", "PLTR", "GOOGL", "MSFT", "ISRG", "SGOV")


def _normalize_market_provider(value: str | None) -> str:
    v = str(value or "auto").strip().lower()
    if v in {"futu", "opend", "futuapi"}:
        return "futu"
    if v in {"tencent", "qq", "gtimg"}:
        return "tencent"
    if v in {"eastmoney", "em", "cn", "china", "mainland"}:
        return "eastmoney"
    return "auto"


def _futu_opend_config() -> tuple[str, int]:
    host = os.getenv("FUTU_OPEND_HOST", "127.0.0.1").strip() or "127.0.0.1"
    try:
        port = int(os.getenv("FUTU_OPEND_PORT", "11111"))
    except (TypeError, ValueError):
        port = 11111
    return host, port


@st.cache_data(ttl=15, show_spinner=False)
def _is_futu_opend_available() -> bool:
    host, port = _futu_opend_config()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.6)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _market_data_provider() -> str:
    return "futu" if _is_futu_opend_available() else "tencent"

_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_MONTHLY_BUDGET_USAGE_FILE = Path(__file__).with_name("monthly_budget_usage.json")
_SATELLITE_TARGETS_FILE = Path(__file__).with_name("satellite_targets.json")
_SATELLITE_UNIVERSE_FILE = Path(__file__).with_name("satellite_universe.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "QQQ": {"label": "QQQ", "currency": "USD"},
    "ISRG": {"label": "ISRG", "currency": "USD"},
    "TEM": {"label": "TEM", "currency": "USD"},
    "PLTR": {"label": "PLTR", "currency": "USD"},
    "GOOGL": {"label": "GOOGL", "currency": "USD"},
    "MSFT": {"label": "MSFT", "currency": "USD"},
    "AVGO": {"label": "AVGO", "currency": "USD"},
    "NVDA": {"label": "NVDA", "currency": "USD"},
    "SGOV": {"label": "短债(SGOV)", "currency": "USD"},
    "001015": {"label": "沪深300", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    # 目标比例：
    # 美元资产: VOO/QQQ/AI卫星仓位/短债(SGOV) = 4:3:1:2
    # 人民币资产: 沪深300(001015) 20%
    "VOO": 0.24,
    "QQQ": 0.18,
    "AVGO": 0.0114,
    "NVDA": 0.0076,
    "GOOGL": 0.0114,
    "MSFT": 0.0076,
    "ISRG": 0.019,
    "TEM": 0.003,
    "PLTR": 0.0,
    "SGOV": 0.12,
    "001015": 0.20,
}

_SATELLITE_SYMBOLS = ("ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO", "NVDA")
_DEFAULT_SATELLITE_TARGET_PCTS = {
    "ISRG": 31.6666,
    "TEM": 5.0,
    "PLTR": 0.0,
    "AVGO": 19.0,
    "NVDA": 12.6667,
    "GOOGL": 19.0,
    "MSFT": 12.6667,
}


_STATIC_SATELLITE_SYMBOLS = _SATELLITE_SYMBOLS
_SATELLITE_TOTAL_WEIGHT = sum(_TARGET_WEIGHTS.get(sym, 0.0) for sym in _STATIC_SATELLITE_SYMBOLS)


def _default_satellite_universe() -> list[dict[str, Any]]:
    return [
        {
            "symbol": sym,
            "label": _ASSET_META.get(sym, {}).get("label", sym),
            "target_pct": _DEFAULT_SATELLITE_TARGET_PCTS.get(sym, 0.0),
        }
        for sym in _STATIC_SATELLITE_SYMBOLS
    ]


def _load_satellite_universe() -> list[dict[str, Any]]:
    raw: Any = None
    if _SATELLITE_UNIVERSE_FILE.exists():
        try:
            raw = json.loads(_SATELLITE_UNIVERSE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = None
    if not isinstance(raw, list):
        raw = _default_satellite_universe()

    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol", "")).strip().upper()
        if not sym or not sym.replace(".", "").replace("-", "").isalnum() or sym in seen:
            continue
        try:
            target_pct = max(0.0, float(item.get("target_pct", 0.0) or 0.0))
        except (TypeError, ValueError):
            target_pct = 0.0
        seen.add(sym)
        out.append(
            {
                "symbol": sym,
                "label": str(item.get("label") or sym),
                "target_pct": target_pct,
            }
        )
    return out or _default_satellite_universe()


def _apply_satellite_universe() -> None:
    global _SATELLITE_SYMBOLS, _US_MARKET_SYMBOLS, _DEFAULT_SATELLITE_TARGET_PCTS

    universe = _load_satellite_universe()
    configured = tuple(item["symbol"] for item in universe)
    configured_set = set(configured)
    for sym in _STATIC_SATELLITE_SYMBOLS:
        if sym not in configured_set:
            _FALLBACK.pop(sym, None)
            _TICKERS.pop(sym.lower(), None)
            _QQ_US.pop(sym, None)
            _QQ_US_KLINE.pop(sym, None)
            _SINA_GB.pop(sym, None)
            _FUTU_US.pop(sym, None)
            _EASTMONEY_US_SECID.pop(sym, None)
            _ASSET_META.pop(sym, None)
            _TARGET_WEIGHTS.pop(sym, None)

    _DEFAULT_SATELLITE_TARGET_PCTS = {}
    for item in universe:
        sym = item["symbol"]
        _FALLBACK[sym] = 0.0
        _TICKERS[sym.lower()] = sym
        _QQ_US.pop(sym, None)
        _QQ_US_KLINE.pop(sym, None)
        _SINA_GB.pop(sym, None)
        _FUTU_US[sym] = f"US.{sym}"
        _EASTMONEY_US_SECID.pop(sym, None)
        _ASSET_META[sym] = {"label": item["label"], "currency": "USD"}
        _DEFAULT_SATELLITE_TARGET_PCTS[sym] = float(item["target_pct"])
        _TARGET_WEIGHTS[sym] = _SATELLITE_TOTAL_WEIGHT * float(item["target_pct"]) / 100.0

    ordered_meta: dict[str, dict[str, str]] = {}
    for sym in ("VOO", "QQQ", *configured, "SGOV", "001015"):
        if sym in _ASSET_META:
            ordered_meta[sym] = _ASSET_META[sym]
    _ASSET_META.clear()
    _ASSET_META.update(ordered_meta)

    _SATELLITE_SYMBOLS = configured
    _US_MARKET_SYMBOLS = ("VOO", "QQQ", *_SATELLITE_SYMBOLS, "SGOV")


_apply_satellite_universe()


def _normalize_satellite_targets(raw: Any) -> dict[str, float]:
    targets = dict(_DEFAULT_SATELLITE_TARGET_PCTS)
    if isinstance(raw, dict):
        for sym in _SATELLITE_SYMBOLS:
            try:
                targets[sym] = max(0.0, float(raw.get(sym, targets.get(sym, 0.0))))
            except (TypeError, ValueError):
                continue
    total = sum(targets.values())
    if total <= 0:
        return dict(_DEFAULT_SATELLITE_TARGET_PCTS)
    return {sym: value / total * 100.0 for sym, value in targets.items() if sym in _SATELLITE_SYMBOLS}


def _load_satellite_targets() -> dict[str, float]:
    if not _SATELLITE_TARGETS_FILE.exists():
        return _normalize_satellite_targets({})
    try:
        raw = json.loads(_SATELLITE_TARGETS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _normalize_satellite_targets({})
    return _normalize_satellite_targets(raw)


def _save_satellite_targets(targets: dict[str, float]) -> None:
    _SATELLITE_TARGETS_FILE.write_text(
        json.dumps(_normalize_satellite_targets(targets), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _effective_target_weights() -> dict[str, float]:
    weights = dict(_TARGET_WEIGHTS)
    satellite_total = sum(_TARGET_WEIGHTS.get(sym, 0.0) for sym in _SATELLITE_SYMBOLS)
    satellite_targets = _load_satellite_targets()
    for sym in _SATELLITE_SYMBOLS:
        weights[sym] = satellite_total * satellite_targets.get(sym, 0.0) / 100.0
    return weights

# 美元资产 PE 参考区间（经验口径，仅作辅助，不构成投资建议）
_USD_ASSET_PE_BANDS: dict[str, tuple[float, float]] = {
    "VOO": (18.0, 24.0),
    "QQQ": (24.0, 36.0),
    "ISRG": (40.0, 65.0),
    "PLTR": (60.0, 100.0),
    "AVGO": (24.0, 34.0),
    "NVDA": (28.0, 45.0),
    "GOOGL": (18.0, 28.0),
    "MSFT": (24.0, 36.0),
    "SGOV": (0.0, 10.0),
}

_USD_ASSET_PEG_BANDS: dict[str, tuple[float, float]] = {
    "ISRG": (4.1, 7.3),
    "PLTR": (1.5, 3.0),
    "GOOGL": (1.3, 1.9),
    "MSFT": (1.5, 2.6),
    "AVGO": (0.9, 3.0),
    "NVDA": (0.3, 0.4),
}

_USD_ASSET_PS_BANDS: dict[str, tuple[float, float]] = {
    "TEM": (5.0, 9.0),
}

_REBALANCE_PHASE_BUILD = "建仓期"
_REBALANCE_PHASE_DCA = "长期定投期"
_REBALANCE_ALLOCATION_ROWS = [
    {"资产": "VOO", "目标占比": "40%", "策略定位": "核心长期仓"},
    {"资产": "QQQ", "目标占比": "30%", "策略定位": "科技增强仓"},
    {"资产": "AI卫星（可自定义标的）", "目标占比": "10%", "策略定位": "主动超额收益"},
    {"资产": "SGOV", "目标占比": "20%", "策略定位": "弹药库/现金管理"},
]
_REBALANCE_STRATEGY_LABELS = {
    "VOO": "长期无脑定投",
    "QQQ": "动态增强，跌了多买",
    "GOOGL": "半定投化",
    "MSFT": "半定投化",
    "ISRG": "半定投化",
    "TEM": "观察仓",
    "PLTR": "观察仓",
    "NVDA": "波动驱动加仓",
    "AVGO": "波动驱动加仓",
    "SGOV": "机会弹药库",
}
_REBALANCE_RULES: dict[str, dict[str, dict[str, Any]]] = {
    _REBALANCE_PHASE_BUILD: {
        "VOO": {"normal": (1.0, "正常建仓", "每月机械定投", "normal"), "bands": [(-10.0, 3.0, "大加", "3x 月预算 + 少量 SGOV", "large"), (-7.0, 2.0, "中加", "2x 月预算", "medium"), (-3.0, 1.5, "小加", "1.5x 月预算", "small")]},
        "QQQ": {"normal": (1.0, "正常建仓", "每月机械定投", "normal"), "bands": [(-10.0, 3.0, "大加", "3x + 少量 SGOV", "large"), (-7.0, 2.0, "中加", "2x", "medium"), (-3.0, 1.5, "小加", "1.5x", "small")]},
        "MSFT": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-22.0, 0.5, "大加", "大加", "large"), (-18.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "GOOGL": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-24.0, 0.5, "大加", "大加", "large"), (-19.0, 0.3, "中加", "中加", "medium"), (-11.0, 0.2, "小加", "小加", "small")]},
        "NVDA": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-21.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "AVGO": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-22.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "ISRG": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-23.0, 0.5, "大加", "大加", "large"), (-20.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
    },
    _REBALANCE_PHASE_DCA: {
        "VOO": {"normal": (1.0, "正常定投", "正常：工资定投", "normal"), "bands": [(-10.0, 2.5, "大加", "中等动用 SGOV", "large"), (-7.0, 1.75, "明显加仓", "少量 SGOV", "medium"), (-3.0, 1.25, "多买一点", "当月 SGOV 流入", "small")]},
        "QQQ": {"normal": (1.0, "正常定投", "正常：工资定投", "normal"), "bands": [(-10.0, 2.5, "大加", "中等 SGOV", "large"), (-7.0, 1.75, "明显加仓", "少量 SGOV", "medium"), (-3.0, 1.25, "多买一点", "当月 SGOV 流入", "small")]},
        "MSFT": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-22.0, 0.5, "大加", "大加", "large"), (-18.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "GOOGL": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-24.0, 0.5, "大加", "大加", "large"), (-19.0, 0.3, "中加", "中加", "medium"), (-11.0, 0.2, "小加", "小加", "small")]},
        "NVDA": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-21.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "AVGO": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-22.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "ISRG": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-23.0, 0.5, "大加", "大加", "large"), (-20.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
    },
}


_REBALANCE_INTENSITY_ORDER = {"none": 0, "normal": 1, "small": 2, "medium": 3, "large": 4}
_REBALANCE_INTENSITY_LABELS = {
    "none": "未买",
    "normal": "普通/正常",
    "small": "小加/试探",
    "medium": "中加",
    "large": "大加",
}
def _normalize_rebalance_intensity(value: Any) -> str:
    v = str(value or "none").strip().lower()
    aliases = {
        "": "none",
        "no": "none",
        "none": "none",
        "normal": "normal",
        "regular": "normal",
        "small": "small",
        "probe": "small",
        "medium": "medium",
        "large": "large",
    }
    return aliases.get(v, "none")


def _rebalance_intensity_multiplier(symbol: str, phase: str, intensity: str) -> float:
    intensity = _normalize_rebalance_intensity(intensity)
    rule = _REBALANCE_RULES.get(phase, {}).get(symbol)
    if rule is None or intensity == "none":
        return 0.0
    if intensity == "normal":
        return float(rule["normal"][0])
    for _, multiplier, _, _, band_intensity in rule["bands"]:
        if str(band_intensity) == intensity:
            return float(multiplier)
    return 0.0


def _rebalance_signal_for_intensity(symbol: str, phase: str, intensity: str) -> tuple[float, str, str, str]:
    intensity = _normalize_rebalance_intensity(intensity)
    rule = _REBALANCE_RULES.get(phase, {}).get(symbol)
    if rule is None or intensity == "none":
        return 0.0, "暂无规则", "暂无策略规则", "normal"
    normal_multiplier, normal_action, normal_signal, normal_intensity = rule["normal"]
    if intensity == "normal":
        return float(normal_multiplier), str(normal_action), str(normal_signal), str(normal_intensity)
    for _, multiplier, action, signal, band_intensity in rule["bands"]:
        if str(band_intensity) == intensity:
            return float(multiplier), str(action), str(signal), str(band_intensity)
    return float(normal_multiplier), str(normal_action), str(normal_signal), str(normal_intensity)


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
        .stFormSubmitButton > button,
        [data-testid="stButton"] button,
        [data-testid="stDownloadButton"] button,
        [data-testid="stFormSubmitButton"] button,
        .stApp button[kind="primary"],
        .stApp button[kind="secondary"],
        .stApp [data-baseweb="button"] {{
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
        .stFormSubmitButton > button:hover,
        [data-testid="stButton"] button:hover,
        [data-testid="stDownloadButton"] button:hover,
        [data-testid="stFormSubmitButton"] button:hover,
        .stApp button[kind="primary"]:hover,
        .stApp button[kind="secondary"]:hover,
        .stApp [data-baseweb="button"]:hover {{
            transform: scale(1.02);
            border-color: {theme["button_border"]};
            color: {theme["button_text"]} !important;
            box-shadow: var(--app-hover-shadow);
        }}

        .stButton > button *,
        .stDownloadButton > button *,
        .stFormSubmitButton > button *,
        [data-testid="stButton"] button *,
        [data-testid="stButton"] button p,
        [data-testid="stButton"] button span,
        [data-testid="stDownloadButton"] button *,
        [data-testid="stDownloadButton"] button p,
        [data-testid="stDownloadButton"] button span,
        [data-testid="stFormSubmitButton"] button *,
        [data-testid="stFormSubmitButton"] button p,
        [data-testid="stFormSubmitButton"] button span,
        .stApp button[kind="primary"] *,
        .stApp button[kind="secondary"] *,
        .stApp [data-baseweb="button"] * {{
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
            background: var(--app-panel-bg) !important;
            box-shadow: var(--app-shadow);
        }}

        [data-testid="stDataFrame"] > div,
        [data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
        [data-testid="stDataFrame"] [role="grid"],
        [data-testid="stDataFrame"] canvas {{
            background: var(--app-panel-bg) !important;
            color: var(--app-text) !important;
        }}

        [data-testid="stDataFrame"] button,
        [data-testid="stDataFrame"] [data-baseweb="button"] {{
            background: var(--app-control-bg) !important;
            border: 1px solid var(--app-border) !important;
            color: var(--app-text) !important;
            box-shadow: none !important;
        }}

        [data-testid="stDataFrame"] button *,
        [data-testid="stDataFrame"] [data-baseweb="button"] * {{
            color: var(--app-text) !important;
            -webkit-text-fill-color: var(--app-text) !important;
        }}

        [data-testid="stDataFrame"] input,
        [data-testid="stDataFrame"] textarea {{
            background: var(--app-control-bg) !important;
            color: var(--app-text) !important;
            border-color: var(--app-border) !important;
        }}

        .themed-table-wrap {{
            width: 100%;
            max-width: 100%;
            overflow-x: auto;
            border: 1px solid var(--app-border);
            border-radius: 8px;
            background: var(--app-panel-bg);
            box-shadow: var(--app-shadow);
            margin: 0.25rem 0 1rem;
        }}

        .themed-table {{
            width: 100%;
            border-collapse: collapse;
            color: var(--app-text);
            background: var(--app-panel-bg);
            font-size: 0.9rem;
        }}

        .themed-table thead th {{
            position: sticky;
            top: 0;
            z-index: 1;
            background: var(--app-control-bg);
            color: var(--app-text);
            border-bottom: 1px solid var(--app-border);
            padding: 10px 12px;
            text-align: left;
            font-weight: 800;
            white-space: nowrap;
        }}

        .themed-table thead th:first-child {{
            left: 0;
            z-index: 3;
        }}

        .themed-table tbody td {{
            background: transparent;
            color: var(--app-text);
            border-bottom: 1px solid var(--app-border);
            padding: 9px 12px;
            white-space: nowrap;
        }}

        .themed-table tbody td:first-child {{
            position: sticky;
            left: 0;
            z-index: 2;
            background: var(--app-panel-bg);
            font-weight: 800;
        }}

        .themed-table tbody tr:nth-child(even) td {{
            background: color-mix(in srgb, var(--app-control-bg) 46%, transparent);
        }}

        .themed-table tbody tr:nth-child(even) td:first-child {{
            background: color-mix(in srgb, var(--app-control-bg) 46%, var(--app-panel-bg));
        }}

        .themed-table tbody tr:hover td {{
            background: color-mix(in srgb, var(--app-control-bg) 74%, transparent);
        }}

        .themed-table tbody tr:hover td:first-child {{
            background: color-mix(in srgb, var(--app-control-bg) 74%, var(--app-panel-bg));
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

        .daily-card-line {{
            color: var(--daily-color) !important;
            font-weight: 700;
            line-height: 1.35;
        }}

        .daily-card-ext {{
            font-weight: 700;
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


def _render_themed_table(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    formatters: dict[str, str] | None = None,
) -> None:
    df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    green = "#16a34a"
    red = "#dc2626"

    def _cell_style(value: Any, col_name: str) -> str:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return ""
        if not pd.notna(v) or abs(v) < 1e-12:
            return ""
        color_columns = (
            "涨跌",
            "涨幅",
            "回撤",
            "浮动盈亏",
            "到目标缺口",
            "本月差额",
            "建议买入",
            "买入",
        )
        if not any(key in str(col_name) for key in color_columns):
            return ""
        color = green if v > 0 else red
        if "超买" in str(col_name):
            color = red if v > 0 else green
        return f"color: {color}; font-weight: 800;"

    def _style_frame(frame: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=frame.index, columns=frame.columns)
        is_rebalance_table = {"本月计划应买(USD)", "建议买入(USD)", "实际买入(USD)"}.issubset(frame.columns)
        for col in frame.columns:
            if is_rebalance_table:
                continue
            styles[col] = [_cell_style(value, str(col)) for value in frame[col]]
        if "本月计划应买(USD)" in frame.columns:
            plan = pd.to_numeric(frame["本月计划应买(USD)"], errors="coerce").fillna(0.0)
            for col in ("建议买入(USD)", "实际买入(USD)"):
                if col not in frame.columns:
                    continue
                values = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
                for idx, value in values.items():
                    target = float(plan.loc[idx])
                    diff = float(value) - target
                    if abs(diff) < 1e-9:
                        continue
                    color = red if diff > 0 else green
                    styles.at[idx, col] = f"color: {color}; font-weight: 900;"
        if "Forward PE" in frame.columns and "PE合理区间" in frame.columns:
            for idx, value in frame["Forward PE"].items():
                pe = _coerce_float(value)
                band_text = str(frame.at[idx, "PE合理区间"] or "")
                match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$", band_text)
                if pe is None or not match:
                    continue
                low = float(match.group(1))
                high = float(match.group(2))
                if pe > high:
                    styles.at[idx, "Forward PE"] = f"color: {red}; font-weight: 800;"
                elif pe < low:
                    styles.at[idx, "Forward PE"] = f"color: {green}; font-weight: 800;"
        if "PEG" in frame.columns and "PEG区间" in frame.columns:
            for idx, value in frame["PEG"].items():
                peg = _coerce_float(value)
                band_text = str(frame.at[idx, "PEG区间"] or "")
                match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$", band_text)
                if peg is None or not match:
                    continue
                low = float(match.group(1))
                high = float(match.group(2))
                if peg > high:
                    styles.at[idx, "PEG"] = f"color: {red}; font-weight: 800;"
                elif peg < low:
                    styles.at[idx, "PEG"] = f"color: {green}; font-weight: 800;"
        if "Forward PS" in frame.columns and "PS合理区间" in frame.columns:
            for idx, value in frame["Forward PS"].items():
                ps = _coerce_float(value)
                band_text = str(frame.at[idx, "PS合理区间"] or "")
                match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$", band_text)
                if ps is None or not match:
                    continue
                low = float(match.group(1))
                high = float(match.group(2))
                if ps > high:
                    styles.at[idx, "Forward PS"] = f"color: {red}; font-weight: 800;"
                elif ps < low:
                    styles.at[idx, "Forward PS"] = f"color: {green}; font-weight: 800;"
        if "回撤档位" in frame.columns:
            tier_styles = {
                "正常": "color:#16a34a; background-color:rgba(34,197,94,0.16); font-weight:900;",
                "小加": "color:#ca8a04; background-color:rgba(250,204,21,0.18); font-weight:900;",
                "中加": "color:#ea580c; background-color:rgba(249,115,22,0.18); font-weight:900;",
                "大加": "color:#dc2626; background-color:rgba(239,68,68,0.18); font-weight:900;",
            }
            for idx, value in frame["回撤档位"].items():
                styles.at[idx, "回撤档位"] = tier_styles.get(str(value), "color:#64748b; font-weight:800;")
        return styles

    styler = (
        df.style.hide(axis="index")
        .format(formatters or {}, na_rep="-")
        .apply(_style_frame, axis=None)
        .set_table_attributes('class="themed-table"')
    )
    st.markdown(
        f'<div class="themed-table-wrap">{styler.to_html()}</div>',
        unsafe_allow_html=True,
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


def _coerce_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not pd.notna(out):
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
    return default if pd.isna(val) else val


def _fetch_futu_us_price_change() -> dict[str, dict[str, object]]:
    if not _is_futu_opend_available():
        return {}
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception:
        return {}

    host, port = _futu_opend_config()
    ctx = None
    out: dict[str, dict[str, object]] = {}
    futu_codes = [_FUTU_US[sym] for sym in _US_MARKET_SYMBOLS]
    code_to_sym = {code: sym for sym, code in _FUTU_US.items()}
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        ret, snapshot = ctx.get_market_snapshot(futu_codes)
        if ret != RET_OK or snapshot is None or len(snapshot) == 0:
            return {}

        market_states: dict[str, str] = {}
        try:
            state_ret, state_data = ctx.get_market_state(futu_codes)
            if state_ret == RET_OK and state_data is not None and len(state_data) > 0:
                for i in range(len(state_data)):
                    row = state_data.iloc[i] if hasattr(state_data, "iloc") else state_data[i]
                    code = str(_row_get(row, "code", ""))
                    market_states[code] = str(_row_get(row, "market_state", "") or "")
        except Exception:
            pass

        for i in range(len(snapshot)):
            row = snapshot.iloc[i] if hasattr(snapshot, "iloc") else snapshot[i]
            code = str(_row_get(row, "code", ""))
            sym = code_to_sym.get(code)
            if not sym:
                continue
            last_price = _coerce_float(
                _row_get(row, "last_price")
                or _row_get(row, "cur_price")
                or _row_get(row, "price")
            )
            prev_close = _coerce_float(_row_get(row, "prev_close_price") or _row_get(row, "prev_close"))
            pre_price = _coerce_float(
                _row_get(row, "pre_market_price")
                or _row_get(row, "pre_price")
                or _row_get(row, "preMarketPrice")
            )
            pre_change_pct = _coerce_float(_row_get(row, "pre_change_rate"))
            post_price = _coerce_float(
                _row_get(row, "after_market_price")
                or _row_get(row, "after_hours_price")
                or _row_get(row, "after_price")
                or _row_get(row, "postMarketPrice")
            )
            post_change_pct = _coerce_float(_row_get(row, "after_change_rate"))
            overnight_price = _coerce_float(_row_get(row, "overnight_price"))
            overnight_change_pct = _coerce_float(_row_get(row, "overnight_change_rate"))
            market_state = market_states.get(code, "")
            regular_price = last_price
            session = _futu_market_session(market_state)
            extended_price: float | None = None
            extended_change_pct: float | None = None
            if session == "premarket":
                extended_price = pre_price
                extended_change_pct = pre_change_pct
            elif session == "overnight":
                extended_price = overnight_price
                extended_change_pct = overnight_change_pct
            elif session == "postmarket":
                extended_price = post_price
                extended_change_pct = post_change_pct

            if not regular_price or regular_price <= 0:
                continue
            base = prev_close if prev_close and prev_close > 0 else regular_price
            effective_price = (
                extended_price
                if session != "regular" and extended_price is not None and extended_price > 0
                else regular_price
            )
            out[sym] = {
                "price": effective_price,
                "change_pct": ((effective_price / base - 1.0) * 100.0) if base > 0 else 0.0,
                "regular_price": regular_price,
                "regular_change_pct": ((regular_price / base - 1.0) * 100.0) if base > 0 else 0.0,
                "session": session,
                "market_state": market_state,
                "extended_price": extended_price,
                "extended_change_pct": extended_change_pct,
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


def _is_us_regular_market_hours_now() -> bool:
    now = datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 9 * 60 + 30 <= minutes < 16 * 60


def _futu_market_session(market_state: str) -> str:
    state = re.sub(r"[^A-Z0-9]+", "_", market_state.upper()).strip("_")
    if not state:
        return "regular"
    if _is_us_regular_market_hours_now():
        return "regular"
    if ("PRE_MARKET" in state or "PREMARKET" in state) and not any(
        marker in state for marker in ("END", "CLOSE", "CLOSED")
    ):
        return "premarket"
    if "OVERNIGHT" in state and not any(marker in state for marker in ("END", "CLOSE", "CLOSED")):
        return "overnight"
    if (
        "AFTER_MARKET" in state
        or "AFTERHOURS" in state
        or "POST_MARKET" in state
        or "POSTMARKET" in state
    ) and not any(marker in state for marker in ("END", "CLOSE", "CLOSED")):
        return "postmarket"
    return "regular"


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_futu_valuation_meta(symbols: tuple[str, ...]) -> dict[str, dict[str, float]]:
    if not _is_futu_opend_available():
        return {}
    try:
        from futu import OpenQuoteContext, RET_OK
    except Exception:
        return {}

    host, port = _futu_opend_config()
    ctx = None
    out: dict[str, dict[str, float]] = {}
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        for sym in symbols:
            futu_code = _FUTU_US.get(sym)
            if not futu_code:
                continue
            try:
                ret, data = ctx.get_valuation_detail(futu_code, valuation_type=1, interval_type=8)
            except Exception:
                continue
            if ret != RET_OK or not data:
                continue
            trend = data.get("trend") or {}
            forward_pe = _coerce_float(trend.get("forward_value"))
            if forward_pe is not None and forward_pe > 0:
                metrics = {"forward_pe": forward_pe}
                peg = _peg_from_futu_valuation(forward_pe, data)
                if peg is not None and peg > 0:
                    metrics["PEG"] = peg
                out[sym] = metrics
            if sym in _USD_ASSET_PS_BANDS:
                try:
                    ps_ret, ps_data = ctx.get_valuation_detail(code, valuation_type=3, interval_type=8)
                except Exception:
                    ps_ret, ps_data = None, None
                if ps_ret == RET_OK and ps_data:
                    ps_trend = ps_data.get("trend") or {}
                    forward_ps = _coerce_float(ps_trend.get("forward_value"))
                    current_ps = _coerce_float(ps_trend.get("current_value"))
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
    return out


def _peg_from_futu_valuation(forward_pe: float, data: dict[str, Any]) -> float | None:
    growth = data.get("profit_growth_rate") or data.get("profitGrowthRate") or {}
    growth_multiple = _coerce_float(growth.get("financial_ttm_multiple") or growth.get("financialTtmMultiple"))
    year_count = _coerce_float(growth.get("year_count") or growth.get("yearCount"))
    if (growth_multiple is None or year_count is None) and isinstance(growth.get("profit_data"), list):
        profit_data = growth.get("profit_data") or []
        if len(profit_data) >= 2:
            first = _coerce_float(profit_data[0].get("finance_data_multiple"))
            last = _coerce_float(profit_data[-1].get("finance_data_multiple"))
            if first is not None and last is not None and first > 0 and last > 0:
                growth_multiple = last / first
                year_count = max(1.0, len(profit_data) / 4.0)
    if growth_multiple is None or year_count is None or growth_multiple <= 0 or year_count <= 0:
        return None
    annual_growth_pct = ((growth_multiple ** (1.0 / year_count)) - 1.0) * 100.0
    if annual_growth_pct <= 0:
        return None
    return forward_pe / annual_growth_pct


def _fetch_futu_forward_pe_meta(symbols: tuple[str, ...]) -> dict[str, float]:
    return {
        sym: metrics["forward_pe"]
        for sym, metrics in _fetch_futu_valuation_meta(symbols).items()
        if "forward_pe" in metrics
    }


def _pe_band_text(symbol: str) -> str:
    band = _USD_ASSET_PE_BANDS.get(symbol)
    if not band:
        return "-"
    return f"{band[0]:.0f}-{band[1]:.0f}"


def _peg_band_text(symbol: str) -> str:
    band = _USD_ASSET_PEG_BANDS.get(symbol)
    if not band:
        return "-"
    return f"{band[0]:.1f}-{band[1]:.1f}"


def _ps_band_text(symbol: str) -> str:
    band = _USD_ASSET_PS_BANDS.get(symbol)
    if not band:
        return "-"
    return f"{band[0]:.1f}-{band[1]:.1f}"


def _forward_pe_judgment(symbol: str, forward_pe: float | None) -> str:
    if symbol not in _SATELLITE_SYMBOLS:
        return "-"
    band = _USD_ASSET_PE_BANDS.get(symbol)
    if not band or not isinstance(forward_pe, (int, float)):
        return "缺数据"
    low, high = band
    if forward_pe < low:
        return "偏低，可优先加仓"
    if forward_pe <= high:
        return "合理，按计划执行"
    return "偏贵，放慢加仓"


def _forward_pe_rebalance_note(symbol: str, forward_pe: float | None) -> str:
    if symbol not in _SATELLITE_SYMBOLS:
        return ""
    judgment = _forward_pe_judgment(symbol, forward_pe)
    band = _pe_band_text(symbol)
    if not isinstance(forward_pe, (int, float)):
        return f"估值：Forward PE 缺数据，参考区间 {band}。"
    return f"估值：Forward PE {forward_pe:.2f}，参考区间 {band}，{judgment}。"


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
    minutes = current.hour * 60 + current.minute
    nav = _fetch_fund_nav_price_change(code)
    estimate = _fetch_fund_estimated_price_change(code)

    # The portfolio trading day rolls at 09:00, while the fund estimate often
    # does not update until several minutes later.  During that gap keep the
    # latest confirmed price for valuation, but never carry yesterday's daily
    # return into today's P&L/weighted return.
    if current.weekday() < 5 and minutes >= 9 * 60:
        if estimate is not None and estimate[2].startswith(today):
            return estimate[0], estimate[1], "东财基金估算"
        if nav is not None and nav[2] >= today:
            return nav[0], nav[1], f"东财确认净值({nav[2]})"
        if nav is not None:
            return nav[0], 0.0, "等待今日基金估值"
        if estimate is not None:
            return estimate[0], 0.0, "等待今日基金估值"
        return None

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


@st.cache_data(ttl=3, show_spinner=False)
def _fetch_spot_prices_meta() -> dict[str, object]:
    out: dict[str, float] = {}
    daily_change_pct_by_symbol: dict[str, float] = {}
    source_by_symbol: dict[str, str] = {}
    market_session_by_symbol: dict[str, str] = {}
    extended_change_pct_by_symbol: dict[str, float] = {}
    regular_price_by_symbol: dict[str, float] = {}
    regular_change_pct_by_symbol: dict[str, float] = {}
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    provider = _market_data_provider()

    if provider == "futu":
        try:
            futu_raw = _fetch_futu_us_price_change()
            for sym, item in futu_raw.items():
                out[sym] = float(item["price"])
                daily_change_pct_by_symbol[sym] = float(item["change_pct"])
                session = str(item.get("session") or "regular")
                market_session_by_symbol[sym] = session
                regular_price = _coerce_float(item.get("regular_price"))
                regular_change_pct = _coerce_float(item.get("regular_change_pct"))
                if regular_price is not None and regular_price > 0:
                    regular_price_by_symbol[sym] = regular_price
                if regular_change_pct is not None:
                    regular_change_pct_by_symbol[sym] = regular_change_pct
                source_by_symbol[sym] = "Futu OpenD" if session == "regular" else "Futu OpenD 扩展盘"
                extended_change_pct = _coerce_float(item.get("extended_change_pct"))
                if extended_change_pct is not None and session != "regular":
                    extended_change_pct_by_symbol[sym] = extended_change_pct
        except Exception:
            pass
        if not out:
            provider = "tencent"
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
                        sina_code = _SINA_GB.get(sym)
                        res = _fetch_sina_gb_price_change(sina_code) if sina_code else None
                        if res is not None:
                            p, change_pct = res
                            out[sym] = p
                            daily_change_pct_by_symbol[sym] = change_pct
                            source_by_symbol[sym] = "新浪全球"
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
                    sina_code = _SINA_GB.get(sym)
                    res = _fetch_sina_gb_price_change(sina_code) if sina_code else None
                    if res is not None:
                        p, change_pct = res
                        out[sym] = p
                        daily_change_pct_by_symbol[sym] = change_pct
                        source_by_symbol[sym] = "新浪全球"
                except Exception:
                    pass

    # 基金：用东方财富基金估值接口
    symbols = list(_TICKERS.values())
    for sym in ("001015",):
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
        "market_session_by_symbol": market_session_by_symbol,
        "extended_change_pct_by_symbol": extended_change_pct_by_symbol,
        "regular_price_by_symbol": regular_price_by_symbol,
        "regular_change_pct_by_symbol": regular_change_pct_by_symbol,
        "market_data_provider": provider,
        "fetched_at": fetched_at,
    }


def _fetch_spot_prices() -> dict[str, float]:
    return dict(_fetch_spot_prices_meta()["prices"])


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_vix_meta() -> dict[str, float | str]:
    """获取美股 CBOE VIX 当前值与当日涨跌幅。"""
    fetched_at = datetime.now(_TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
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


def _fetch_tencent_us_60d_metrics(symbol: str) -> dict[str, float | None]:
    code = _QQ_US_KLINE.get(symbol)
    if not code:
        return {"drawdown_pct": None, "rebound_pct": None, "peak": None, "trough": None}
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
        return {"drawdown_pct": drawdown, "rebound_pct": rebound, "peak": peak, "trough": trough}
    except Exception:
        return {"drawdown_pct": None, "rebound_pct": None, "peak": None, "trough": None}


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_us_etf_pe_drawdown(
    symbol: str,
    cache_version: str = _DRAWDOWN_CACHE_VERSION,
    provider: str | None = None,
) -> dict[str, float | None]:
    """返回美股/ETF估值与回撤指标：pe(若可得)、回撤%(近60日高点回撤，负值为回撤)。"""
    _ = cache_version
    pe_val: float | None = None
    dd_val: float | None = None
    rebound_val: float | None = None
    peak_val: float | None = None
    trough_val: float | None = None
    tencent_metrics = _fetch_tencent_us_60d_metrics(symbol)
    dd_val = tencent_metrics.get("drawdown_pct")
    rebound_val = tencent_metrics.get("rebound_pct")
    peak_val = tencent_metrics.get("peak")
    trough_val = tencent_metrics.get("trough")

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
                        peak_val = peak
                    if trough > 0:
                        rebound_val = (last / trough - 1.0) * 100.0
                        trough_val = trough
                    if dd_val is not None or rebound_val is not None:
                        break
            except Exception:
                continue

    return {"pe": pe_val, "drawdown_pct": dd_val, "rebound_pct": rebound_val, "peak": peak_val, "trough": trough_val}


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


def _fetch_asset_drawdown(sym: str, meta: dict[str, str], current_price: float | None = None) -> float | None:
    if meta["currency"] == "USD":
        q = _fetch_us_etf_pe_drawdown(sym, _DRAWDOWN_CACHE_VERSION, _market_data_provider())
        peak = q.get("peak")
        if isinstance(current_price, (int, float)) and current_price > 0 and isinstance(peak, (int, float)) and peak > 0:
            return (float(current_price) / float(peak) - 1.0) * 100.0
        dd = q.get("drawdown_pct")
        return float(dd) if isinstance(dd, (int, float)) else None
    if sym in _FUND_CODES:
        return _fetch_fund_drawdown(_FUND_CODES[sym])
    return None


def _fetch_asset_rebound(sym: str, meta: dict[str, str], current_price: float | None = None) -> float | None:
    if meta["currency"] == "USD":
        q = _fetch_us_etf_pe_drawdown(sym, _DRAWDOWN_CACHE_VERSION, _market_data_provider())
        trough = q.get("trough")
        if isinstance(current_price, (int, float)) and current_price > 0 and isinstance(trough, (int, float)) and trough > 0:
            return (float(current_price) / float(trough) - 1.0) * 100.0
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
        "voo_dividend_usd": 0.0,
        "sgov_dividend_usd": 0.0,
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
    try:
        out["voo_dividend_usd"] = max(0.0, float(data.get("voo_dividend_usd", 0.0)))
    except (TypeError, ValueError):
        out["voo_dividend_usd"] = 0.0
    try:
        out["sgov_dividend_usd"] = float(data.get("sgov_dividend_usd", 0.0))
    except (TypeError, ValueError):
        out["sgov_dividend_usd"] = 0.0
    # 兼容旧版按标的保存的“结转余额”
    try:
        legacy_cny = max(0.0, float(data.get("001015", 0.0)))
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
    payload["voo_dividend_usd"] = float(max(0.0, balances.get("voo_dividend_usd", 0.0)))
    payload["sgov_dividend_usd"] = float(balances.get("sgov_dividend_usd", 0.0))
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
        "bought_amount_by_symbol": {},
        "bought_intensity_by_symbol": {},
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
        raw_amounts = raw.get("bought_amount_by_symbol", {})
        if isinstance(raw_amounts, dict):
            clean_amounts: dict[str, float] = {}
            for sym, amount in raw_amounts.items():
                usym = str(sym).upper()
                if usym not in _ASSET_META or _ASSET_META[usym]["currency"] != "USD":
                    continue
                try:
                    val = max(0.0, float(amount))
                except (TypeError, ValueError):
                    val = 0.0
                if val > 0:
                    clean_amounts[usym] = val
            month_data["bought_amount_by_symbol"] = clean_amounts
            if clean_amounts:
                month_data["used_budget_usd"] = sum(clean_amounts.values())
                month_data["bought_symbols"] = sorted(set(month_data["bought_symbols"]) | set(clean_amounts.keys()))
        raw_intensity = raw.get("bought_intensity_by_symbol", {})
        if isinstance(raw_intensity, dict):
            month_data["bought_intensity_by_symbol"] = {
                str(sym).upper(): _normalize_rebalance_intensity(intensity)
                for sym, intensity in raw_intensity.items()
                if str(sym).upper() in _ASSET_META
                and _ASSET_META[str(sym).upper()]["currency"] == "USD"
                and _normalize_rebalance_intensity(intensity) != "none"
            }
        elif month_data["bought_symbols"]:
            month_data["bought_intensity_by_symbol"] = {
                str(sym).upper(): "normal"
                for sym in month_data["bought_symbols"]
            }
        month_data["updated_at"] = str(raw.get("updated_at", ""))
    return month_data


def _save_monthly_budget_usage(
    user_id: str,
    month_key: str,
    used_budget_usd: float,
    bought_symbols: list[str] | None = None,
    planned_new_cash_usd: float = 700.0,
    bought_intensity_by_symbol: dict[str, str] | None = None,
    bought_amount_by_symbol: dict[str, float] | None = None,
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
    normalized_intensity = {
        str(sym).upper(): _normalize_rebalance_intensity(intensity)
        for sym, intensity in (bought_intensity_by_symbol or {}).items()
        if str(sym).upper() in _ASSET_META
        and _ASSET_META[str(sym).upper()]["currency"] == "USD"
        and _normalize_rebalance_intensity(intensity) != "none"
    }
    normalized_symbols = [
        str(sym).upper()
        for sym in (bought_symbols or normalized_intensity.keys())
        if str(sym).upper() in _ASSET_META and _ASSET_META[str(sym).upper()]["currency"] == "USD"
    ]
    normalized_amounts: dict[str, float] = {}
    for sym, amount in (bought_amount_by_symbol or {}).items():
        usym = str(sym).upper()
        if usym not in _ASSET_META or _ASSET_META[usym]["currency"] != "USD":
            continue
        try:
            val = max(0.0, float(amount))
        except (TypeError, ValueError):
            val = 0.0
        if val > 0:
            normalized_amounts[usym] = val
    if normalized_amounts:
        normalized_symbols = sorted(set(normalized_symbols) | set(normalized_amounts.keys()))
        used = sum(normalized_amounts.values())
    user_store[month_key] = {
        "used_budget_usd": used,
        "planned_new_cash_usd": planned_cash_usd,
        "bought_symbols": normalized_symbols,
        "bought_amount_by_symbol": normalized_amounts,
        "bought_intensity_by_symbol": normalized_intensity,
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
    try:
        balances["voo_dividend_usd"] = max(0.0, float(raw.get("voo_dividend_usd", 0.0)))
    except (TypeError, ValueError):
        balances["voo_dividend_usd"] = 0.0
    try:
        balances["sgov_dividend_usd"] = float(raw.get("sgov_dividend_usd", 0.0))
    except (TypeError, ValueError):
        balances["sgov_dividend_usd"] = 0.0
    # 兼容旧版字段
    try:
        legacy_cny = max(0.0, float(raw.get("001015", 0.0)))
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
    return {sym: float(raw.get(sym, _FALLBACK.get(sym, 0.0))) for sym in _ASSET_META}


def _ensure_price_session_defaults() -> None:
    d = _defaults_from_fetch()
    for sym, value in d.items():
        st.session_state.setdefault(f"def_{sym.lower()}", value)
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

provider_labels = {
    "tencent": "腾讯（默认）",
    "futu": "Futu OpenD",
}
futu_host, futu_port = _futu_opend_config()
futu_available = _is_futu_opend_available()
effective_market_provider = _market_data_provider()
futu_status_text = "已连接" if futu_available else "未启动"
st.sidebar.caption(
    f"行情源：Futu OpenD 优先，腾讯兜底"
)
st.sidebar.caption(
    f"OpenD {futu_host}:{futu_port}：{futu_status_text} ｜ 当前：{provider_labels.get(effective_market_provider, effective_market_provider)}"
)
realtime_quotes_enabled = st.sidebar.checkbox(
    "实时刷新行情",
    value=True,
    key="realtime_quotes_enabled",
    help="基于当前数据源定时刷新页面；Futu OpenD 可用时会重新拉取快照，腾讯仅作兜底。",
)
realtime_quotes_interval_seconds = int(
    st.sidebar.number_input(
        "刷新间隔(秒)",
        min_value=3,
        max_value=60,
        value=10,
        step=1,
        key="realtime_quotes_interval_seconds",
        disabled=not realtime_quotes_enabled,
    )
)
if realtime_quotes_enabled:
    if st_autorefresh is not None:
        st_autorefresh(
            interval=realtime_quotes_interval_seconds * 1000,
            key="market_realtime_autorefresh",
        )
    else:
        st.sidebar.warning("缺少 streamlit-autorefresh，自动刷新未启用。")
previous_market_provider = st.session_state.get("_last_effective_market_data_provider")
if previous_market_provider is None:
    st.session_state["_last_effective_market_data_provider"] = effective_market_provider
elif previous_market_provider != effective_market_provider:
    _fetch_spot_prices_meta.clear()
    _fetch_vix_meta.clear()
    _fetch_us_etf_pe_drawdown.clear()
    _fetch_futu_valuation_meta.clear()
    _is_futu_opend_available.clear()
    for k in (
        "def_voo",
        "def_qqq",
        "def_avgo",
        "def_nvda",
        "def_googl",
        "def_msft",
        "def_isrg",
        "def_sgov",
        "def_hs300",
        "inp_voo",
        "inp_qqq",
        "inp_avgo",
        "inp_nvda",
        "inp_googl",
        "inp_msft",
        "inp_isrg",
        "inp_sgov",
        "inp_hs300",
        "_prices_initialized",
    ):
        st.session_state.pop(k, None)
    st.session_state["_last_effective_market_data_provider"] = effective_market_provider

_db = _db_conf()

title_col, refresh_col = st.columns([0.72, 0.28], vertical_alignment="center")
with title_col:
    st.title(f"👋 Hello, {cloud_user_id or 'Guest'}")
with refresh_col:
    refresh_prices_clicked = st.button(
        "刷新市价",
        help="拉取现价并刷新默认输入；K线同步在后台定时进行。",
        use_container_width=True,
    )

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
target_weights = _effective_target_weights()
satellite_target_pcts = _load_satellite_targets()
if refresh_prices_clicked:
    _fetch_spot_prices_meta.clear()
    _fetch_usdcny_rate_meta.clear()
    _fetch_vix_meta.clear()
    _fetch_us_etf_pe_drawdown.clear()
    _fetch_futu_valuation_meta.clear()
    _fetch_fund_60d_metrics.clear()
    d = _defaults_from_fetch()
    st.session_state.def_fx = _fetch_usdcny_rate()
    for sym, value in d.items():
        st.session_state[f"def_{sym.lower()}"] = value

    # 删除输入框缓存值，让下方 number_input 用新的 def_* 作为默认值。
    for k in ("inp_fx", *(f"inp_{sym.lower()}" for sym in _ASSET_META)):
        if k in st.session_state:
            del st.session_state[k]

    st.success("已刷新市价")

_ensure_fx_session_default()
_ensure_price_session_defaults()
spot_meta = _fetch_spot_prices_meta()
st.sidebar.caption(f"行情刷新：{spot_meta.get('fetched_at', '-')}")
fx_meta = _fetch_usdcny_rate_meta()
fx = float(st.session_state.get("inp_fx", st.session_state.def_fx))
spot_prices = spot_meta.get("prices", {})
prices_now = {
    sym: float(spot_prices.get(sym, st.session_state.get(f"def_{sym.lower()}", _FALLBACK.get(sym, 0.0))))
    for sym in _ASSET_META
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
            balances_for_view["sgov_dividend_usd"] = st.number_input(
                "SGOV 已收分红/利息（USD）",
                value=float(balances_for_view.get("sgov_dividend_usd", 0.0)),
                step=0.01,
                key="edit_balance_sgov_dividend_usd",
                help="用于修正 SGOV 月度派息除权导致的价格型浮盈亏误差；填写已收到但希望归因到 SGOV 的分红金额。",
            )
            balances_for_view["voo_dividend_usd"] = st.number_input(
                "VOO 已收分红（USD）",
                min_value=0.0,
                value=float(balances_for_view.get("voo_dividend_usd", 0.0)),
                step=0.01,
                key="edit_balance_voo_dividend_usd",
                help="填写已收到且希望计入 VOO 总回报的累计分红金额；不改变 VOO 市值或现金余额。",
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
    _chart_symbols = ("VOO", "QQQ", *_SATELLITE_SYMBOLS, "SGOV")
    _chart_symbol_labels = {sym: sym for sym in _chart_symbols}
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
    chart_provider = _chart_api["configure_market_provider"]("tencent")
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
    _avwap_mode_labels = {}
    if _chart_yf not in {"VOO", "QQQ", "SGOV"}:
        _avwap_mode_labels["最近财报日"] = "earnings"
    _avwap_mode_labels.update(
        {
            "最近60日历史高点": "high_60d",
            "最近60日大跌低点": "selloff_60d",
            "今日开盘": "today_open",
        }
    )
    _avwap_pick = st.sidebar.selectbox(
        "AVWAP 锚点",
        options=list(_avwap_mode_labels.keys()),
        key="chart_avwap_mode",
    )
    _avwap_mode = _avwap_mode_labels[_avwap_pick]

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
                        avwap_mode=_avwap_mode,
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
                        avwap_mode=_avwap_mode,
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
                        avwap_mode=_avwap_mode,
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

    _tab_d, _tab_15, _tab_5 = st.tabs(["日线（EMA·MACD）", "15m（AVWAP·RSI·MACD）", "5m（AVWAP·RSI·MACD）"])
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
source_by_symbol: dict[str, str] = spot_meta.get("source_by_symbol", {})  # type: ignore[assignment]
market_session_by_symbol: dict[str, str] = spot_meta.get("market_session_by_symbol", {})  # type: ignore[assignment]
extended_change_pct_by_symbol: dict[str, float] = spot_meta.get("extended_change_pct_by_symbol", {})  # type: ignore[assignment]
regular_price_by_symbol: dict[str, float] = spot_meta.get("regular_price_by_symbol", {})  # type: ignore[assignment]
regular_change_pct_by_symbol: dict[str, float] = spot_meta.get("regular_change_pct_by_symbol", {})  # type: ignore[assignment]
valuation_metrics_by_symbol = (
    _fetch_futu_valuation_meta(
        tuple(sym for sym in _SATELLITE_SYMBOLS if sym in _USD_ASSET_PE_BANDS or sym in _USD_ASSET_PS_BANDS)
    )
    if _market_data_provider() == "futu"
    else {}
)
forward_pe_by_symbol = {
    sym: metrics["forward_pe"]
    for sym, metrics in valuation_metrics_by_symbol.items()
    if "forward_pe" in metrics
}
drawdown_pct_by_symbol: dict[str, float | None] = {}
rebound_pct_by_symbol: dict[str, float | None] = {}
voo_dividend_usd = float(balances_for_view.get("voo_dividend_usd", 0.0))
sgov_dividend_usd = float(balances_for_view.get("sgov_dividend_usd", 0.0))
for sym, meta in _ASSET_META.items():
    shares = float(holdings[sym]["shares"])
    avg_cost = float(holdings[sym]["avg_cost"])
    current = float(prices_now[sym])
    cost = shares * avg_cost
    value = shares * current
    dividend_native = voo_dividend_usd if sym == "VOO" else sgov_dividend_usd if sym == "SGOV" else 0.0
    pnl = value - cost + dividend_native
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
    pnl_cny_by_symbol[sym] = value_cny - cost_cny + (dividend_native * fx if meta["currency"] == "USD" else dividend_native)
    row_drawdown = _fetch_asset_drawdown(sym, meta, current)
    row_rebound = _fetch_asset_rebound(sym, meta, current)
    drawdown_pct_by_symbol[sym] = row_drawdown
    rebound_pct_by_symbol[sym] = row_rebound
    rows.append(
        {
            "标的": meta["label"],
            "币种": meta["currency"],
            "当日涨跌%": round(daily_change_pct_by_symbol.get(sym, 0.0), 2),
            "近60日高点回撤%": round(float(row_drawdown), 2) if isinstance(row_drawdown, (int, float)) else None,
            "近60日低点涨幅%": round(float(row_rebound), 2) if isinstance(row_rebound, (int, float)) else None,
            "Forward PE": (
                round(float(forward_pe_by_symbol[sym]), 2)
                if sym in _SATELLITE_SYMBOLS and isinstance(forward_pe_by_symbol.get(sym), (int, float))
                else None
            ),
            "PE合理区间": _pe_band_text(sym) if sym in _SATELLITE_SYMBOLS else "-",
            "PEG": (
                round(float(valuation_metrics_by_symbol[sym]["PEG"]), 2)
                if sym in _SATELLITE_SYMBOLS
                and isinstance(valuation_metrics_by_symbol.get(sym), dict)
                and isinstance(valuation_metrics_by_symbol[sym].get("PEG"), (int, float))
                else None
            ),
            "PEG区间": _peg_band_text(sym) if sym in _SATELLITE_SYMBOLS else "-",
            "Forward PS": (
                round(float(valuation_metrics_by_symbol[sym]["forward_ps"]), 2)
                if sym in _SATELLITE_SYMBOLS
                and isinstance(valuation_metrics_by_symbol.get(sym), dict)
                and isinstance(valuation_metrics_by_symbol[sym].get("forward_ps"), (int, float))
                else None
            ),
            "PS合理区间": _ps_band_text(sym) if sym in _USD_ASSET_PS_BANDS else "-",
            "浮动盈亏": round(pnl, 2),
            "涨跌幅%": round(pnl_pct, 2),
            "持有数量": round(shares, 3),
            "持仓成本": round(avg_cost, 2),
            "当前价": round(current, 4),
            "持仓市值": round(value, 2),
        }
    )

total_pnl_cny = sum(pnl_cny_by_symbol.values())
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


def _daily_value_with_extension(
    main_text: str,
    ext_text: str | None,
    ext_pct: float | None,
) -> str:
    if not ext_text or not isinstance(ext_pct, (int, float)):
        return main_text
    ext_color = _change_color_by_pct(float(ext_pct), theme=theme)
    return f"{main_text} <span class='daily-card-ext' style='color:{ext_color};'>({ext_text})</span>"


satellite_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in _SATELLITE_SYMBOLS)
satellite_regular_value_cny = 0.0
satellite_daily_change_cny = 0.0
satellite_extended_change_cny = 0.0
for sym in _SATELLITE_SYMBOLS:
    d = float(regular_change_pct_by_symbol.get(sym, daily_change_pct_by_symbol.get(sym, 0.0)))
    d_ratio = d / 100.0
    shares_now = float(holdings.get(sym, {}).get("shares", 0.0))
    current_px = float(regular_price_by_symbol.get(sym, prices_now.get(sym, 0.0)))
    current_value_native = shares_now * current_px
    satellite_regular_value_cny += current_value_native * fx
    if abs(1.0 + d_ratio) > 1e-9:
        daily_amount_native = current_value_native - (current_value_native / (1.0 + d_ratio))
    else:
        daily_amount_native = 0.0
    satellite_daily_change_cny += daily_amount_native * fx
    ext_pct = extended_change_pct_by_symbol.get(sym)
    if isinstance(ext_pct, (int, float)):
        satellite_extended_change_cny += current_value_native * (float(ext_pct) / 100.0) * fx
satellite_daily_pct = (
    sum(
        (
            (
                float(holdings.get(sym, {}).get("shares", 0.0))
                * float(regular_price_by_symbol.get(sym, prices_now.get(sym, 0.0)))
                * fx
            )
            / satellite_regular_value_cny
        )
        * regular_change_pct_by_symbol.get(sym, daily_change_pct_by_symbol.get(sym, 0.0))
        for sym in _SATELLITE_SYMBOLS
    )
    if satellite_regular_value_cny > 0
    else 0.0
)
satellite_extended_pct = (
    (satellite_extended_change_cny / satellite_regular_value_cny * 100.0)
    if satellite_regular_value_cny > 0 and abs(satellite_extended_change_cny) > 1e-9
    else None
)
satellite_daily_color = _change_color_by_pct(satellite_daily_pct, theme=theme)
satellite_daily_change_usd = (satellite_daily_change_cny / fx) if fx > 0 else 0.0
satellite_extended_change_usd = (satellite_extended_change_cny / fx) if fx > 0 else None
satellite_card_html = (
    f"<div class='daily-card' style='--daily-color:{satellite_daily_color};'>"
    "<div class='daily-card-title'>卫星仓位</div>"
    "<div class='daily-card-amount'>"
    f"<div class='daily-card-line'>{_daily_value_with_extension(f'{satellite_daily_pct:+.2f}%', f'{satellite_extended_pct:+.2f}%' if isinstance(satellite_extended_pct, (int, float)) else None, satellite_extended_pct)}</div>"
    f"<div class='daily-card-line'>{_daily_value_with_extension(f'USD {satellite_daily_change_usd:+,.2f}', f'{satellite_extended_change_usd:+,.2f}' if isinstance(satellite_extended_change_usd, (int, float)) and isinstance(satellite_extended_pct, (int, float)) else None, satellite_extended_pct)}</div>"
    f"<div class='daily-card-line'>{_daily_value_with_extension(f'CNY {satellite_daily_change_cny:+,.2f}', f'{satellite_extended_change_cny:+,.2f}' if isinstance(satellite_extended_pct, (int, float)) else None, satellite_extended_pct)}</div>"
    "</div>"
    "</div>"
)
daily_card_symbols = (
    "VOO",
    "QQQ",
    *_SATELLITE_SYMBOLS,
    "SGOV",
    "001015",
)
for sym in daily_card_symbols:
    meta = _ASSET_META[sym]
    d = regular_change_pct_by_symbol.get(sym, daily_change_pct_by_symbol.get(sym, 0.0))
    c = _change_color_by_pct(d, theme=theme)
    shares_now = float(holdings.get(sym, {}).get("shares", 0.0))
    current_px = float(regular_price_by_symbol.get(sym, prices_now.get(sym, 0.0)))
    current_value_native = shares_now * current_px
    d_ratio = d / 100.0
    # 用当前市值反推昨日市值，得到更贴近真实的当日波动金额。
    if abs(1.0 + d_ratio) > 1e-9:
        daily_amount_native = current_value_native - (current_value_native / (1.0 + d_ratio))
    else:
        daily_amount_native = 0.0
    session = market_session_by_symbol.get(sym, "regular")
    extended_pct = extended_change_pct_by_symbol.get(sym)
    session_label = {
        "premarket": "盘前",
        "postmarket": "盘后",
        "overnight": "夜盘",
    }.get(session, "")
    ext_pct_text: str | None = None
    ext_usd_text: str | None = None
    ext_cny_text: str | None = None
    effective_px = float(prices_now.get(sym, current_px))
    price_currency = "USD" if meta["currency"] == "USD" else "CNY"
    price_decimals = 2 if meta["currency"] == "USD" else 4
    price_line = f"{price_currency} {current_px:,.{price_decimals}f}"
    if session_label and effective_px > 0 and abs(effective_px - current_px) > 1e-9:
        price_line = f"{price_line}（{effective_px:,.{price_decimals}f}）"
    if session_label and isinstance(extended_pct, (int, float)):
        ext_ratio = float(extended_pct) / 100.0
        ext_amount_native = current_value_native * ext_ratio
        ext_pct_text = f"{float(extended_pct):+.2f}%"
        if meta["currency"] == "USD":
            ext_usd_text = f"{ext_amount_native:+,.2f}"
            ext_cny_text = f"{ext_amount_native * fx:+,.2f}"
        else:
            ext_cny_text = f"{ext_amount_native:+,.2f}"
            ext_usd_text = f"{(ext_amount_native / fx):+,.2f}" if fx > 0 else None
    if meta["currency"] == "USD":
        pct_line = _daily_value_with_extension(f"{d:+.2f}%", ext_pct_text, extended_pct)
        usd_line = _daily_value_with_extension(f"USD {daily_amount_native:+,.2f}", ext_usd_text, extended_pct)
        cny_line = _daily_value_with_extension(f"CNY {daily_amount_native * fx:+,.2f}", ext_cny_text, extended_pct)
    else:
        pct_line = _daily_value_with_extension(f"{d:+.2f}%", ext_pct_text, extended_pct)
        usd_main = (daily_amount_native / fx) if fx > 0 else 0.0
        usd_line = _daily_value_with_extension(f"USD {usd_main:+,.2f}", ext_usd_text, extended_pct)
        cny_line = _daily_value_with_extension(f"CNY {daily_amount_native:+,.2f}", ext_cny_text, extended_pct)
    daily_amount_text = (
        f"<div class='daily-card-line'>{price_line}</div>"
        f"<div class='daily-card-line'>{pct_line}</div>"
        f"<div class='daily-card-line'>{usd_line}</div>"
        f"<div class='daily-card-line'>{cny_line}</div>"
    )
    _daily_cards.append(
        f"<div class='daily-card' style='--daily-color:{c};'>"
        f"<div class='daily-card-title'>{meta['label']}</div>"
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
satellite_pnl_cny = sum(pnl_cny_by_symbol.get(sym, 0.0) for sym in _SATELLITE_SYMBOLS)
pnl_chart_rows = [
    {
        "标的": _ASSET_META[sym]["label"],
        "浮盈亏(CNY)": round(pnl_cny_by_symbol[sym], 2),
        "盈亏标签": f"¥ {pnl_cny_by_symbol[sym]:+,.0f}",
        "方向": "盈利" if pnl_cny_by_symbol[sym] >= 0 else "亏损",
    }
    for sym in _ASSET_META
    if sym not in excluded_core_pnl_symbols
]
pnl_chart_rows.append(
    {
        "标的": "卫星仓位",
        "浮盈亏(CNY)": round(satellite_pnl_cny, 2),
        "盈亏标签": f"¥ {satellite_pnl_cny:+,.0f}",
        "方向": "盈利" if satellite_pnl_cny >= 0 else "亏损",
    }
)
pnl_chart_df = pd.DataFrame(pnl_chart_rows).sort_values("浮盈亏(CNY)", ascending=False)
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
    .properties(title="核心仓位浮盈亏排名（卫星仓位合并为一项，折合CNY）", height=max(260, 46 * len(pnl_chart_df)))
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

usd_target_weight_total = sum(target_weights[sym] for sym in usd_symbols)
usd_position_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in usd_symbols)
usd_extra_value_cny = cash_usd * fx
usd_total_cny = usd_position_value_cny + usd_extra_value_cny
usd_total_usd = (usd_total_cny / fx) if fx > 0 else 0.0


def _usd_target_pct(sym: str) -> float:
    return (target_weights[sym] / usd_target_weight_total * 100.0) if usd_target_weight_total > 0 else 0.0


def _usd_amount_label(value_cny: float | None) -> str:
    if value_cny is None:
        return ""
    value_usd = (value_cny / fx) if fx > 0 else 0.0
    return f"USD {value_usd:,.2f} / CNY {value_cny:,.2f}"


voo_current = value_cny_by_symbol.get("VOO", 0.0)
qqq_current = value_cny_by_symbol.get("QQQ", 0.0)
sgov_current = value_cny_by_symbol.get("SGOV", 0.0)
satellite_current = sum(value_cny_by_symbol.get(sym, 0.0) for sym in _SATELLITE_SYMBOLS)

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
    sum(target_weights[sym] for sym in _SATELLITE_SYMBOLS) / usd_target_weight_total * 100.0
) if usd_target_weight_total > 0 else 0.0
sgov_target = _usd_target_pct("SGOV")

group1_df = pd.DataFrame(
    [
        {"标的组": "VOO", "类型": "当前比例%", "成分": "VOO", "比例%": round(voo_ratio, 2), "金额": _usd_amount_label(voo_current)},
        {"标的组": "VOO", "类型": "目标比例%", "成分": "目标", "比例%": round(voo_target, 2), "金额": _usd_amount_label(usd_total_cny * voo_target / 100.0)},
        {"标的组": "QQQ", "类型": "当前比例%", "成分": "QQQ", "比例%": round(qqq_ratio, 2), "金额": _usd_amount_label(qqq_current)},
        {"标的组": "QQQ", "类型": "目标比例%", "成分": "目标", "比例%": round(qqq_target, 2), "金额": _usd_amount_label(usd_total_cny * qqq_target / 100.0)},
        {"标的组": "卫星仓位", "类型": "当前比例%", "成分": "卫星仓位", "比例%": round(new4_ratio, 2), "金额": _usd_amount_label(satellite_current)},
        {"标的组": "卫星仓位", "类型": "目标比例%", "成分": "目标", "比例%": round(new4_target, 2), "金额": _usd_amount_label(usd_total_cny * new4_target / 100.0)},
        {"标的组": "短债", "类型": "当前比例%", "成分": "SGOV", "比例%": round(sgov_ratio, 2), "金额": _usd_amount_label(sgov_current)},
        {"标的组": "短债", "类型": "目标比例%", "成分": "目标", "比例%": round(sgov_target, 2), "金额": _usd_amount_label(usd_total_cny * sgov_target / 100.0)},
        {"标的组": "现金", "类型": "当前比例%", "成分": "现金", "比例%": round(cash_usd_ratio, 2), "金额": _usd_amount_label(usd_extra_value_cny)},
        {"标的组": "现金", "类型": "目标比例%", "成分": "目标", "比例%": 0.0, "金额": _usd_amount_label(0.0)},
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
        tooltip=["标的组:N", "类型:N", "成分:N", alt.Tooltip("比例%:Q", format=".2f"), "金额:N"],
    )
    .properties(title="VOO / QQQ / 卫星仓位 / 短债(SGOV) / 现金 当前与目标对比")
)
tech_denominator = sum(satellite_ratio_by_symbol.values())
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
                "金额": _usd_amount_label(value_cny_by_symbol.get(sym, 0.0)),
                "浮盈亏(CNY)": round(pnl_cny_by_symbol.get(sym, 0.0), 2),
                "浮盈亏标签": f"¥ {pnl_cny_by_symbol.get(sym, 0.0):+,.0f}",
            },
            {
                "标的": sym,
                "类型": "目标占卫星仓位%",
                "比例%": round(satellite_target_pcts.get(sym, 0.0), 2),
                "金额": _usd_amount_label(
                    satellite_current * satellite_target_pcts.get(sym, 0.0) / 100.0
                ),
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
            "金额:N",
        ],
    )
)
tech_split_chart = (
    tech_split_bars
    .properties(title="卫星仓位内部占比（目标可在下方配置）")
)
st.altair_chart(_theme_altair_chart(tech_split_chart, theme), width="stretch")
st.altair_chart(_theme_altair_chart(group1_chart, theme), width="stretch")

with st.expander("配置卫星仓位目标比例", expanded=False):
    st.caption("这里填写的是卫星仓位内部目标百分比；保存后会自动归一化为合计 100%。")
    with st.form("satellite_targets_form"):
        target_cols = st.columns(3)
        target_inputs: dict[str, float] = {}
        for idx, sym in enumerate(_SATELLITE_SYMBOLS):
            with target_cols[idx % 3]:
                target_inputs[sym] = st.number_input(
                    sym,
                    min_value=0.0,
                    max_value=100.0,
                    value=float(satellite_target_pcts.get(sym, 0.0)),
                    step=0.5,
                    format="%.2f",
                    key=f"satellite_target_{sym}",
                )
        target_sum = sum(target_inputs.values())
        st.caption(f"当前输入合计：{target_sum:.2f}%")
        if st.form_submit_button("保存卫星目标比例"):
            _save_satellite_targets(target_inputs)
            st.success("卫星目标比例已保存。")
            st.rerun()

st.markdown("<br>", unsafe_allow_html=True)
st.subheader("📦 我的持仓")
st.caption(f"当前持仓读取来源：{'云端数据库' if storage_mode == 'cloud' else '本地文件'}")
_render_holdings_editor()
_render_themed_table(
    rows,
    formatters={
        "当日涨跌%": "{:.2f}%",
        "Forward PE": "{:.2f}",
        "PEG": "{:.2f}",
        "Forward PS": "{:.2f}",
        "近60日高点回撤%": "{:.2f}%",
        "近60日低点涨幅%": "{:.2f}%",
        "浮动盈亏": "{:.2f}",
        "涨跌幅%": "{:.2f}%",
        "持有数量": "{:.2f}",
        "持仓成本": "{:.2f}",
        "当前价": "{:.2f}",
        "持仓市值": "{:.2f}",
    },
)
usd_cost_usd = sum(float(holdings[sym]["shares"]) * float(holdings[sym]["avg_cost"]) for sym in usd_symbols)
usd_position_value_usd = sum(
    float(holdings[sym]["shares"]) * float(prices_now.get(sym, 0.0))
    for sym in usd_symbols
)
usd_unrealized_pnl_usd = usd_position_value_usd - usd_cost_usd + voo_dividend_usd + sgov_dividend_usd
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
- VOO/QQQ 建仓期按目标缺口和剩余月数拆成月度推进量。
- 卫星股以10月底预计总资产对应的目标金额为 1x；目标为 0% 的观察成员不产生系统买入建议。
- SGOV 超过 20% 目标的部分，可以随时挪用。
- 目标内 20% SGOV 默认保留，只有触发大加档才动用。
- 一旦触发大加/恐慌级档位，SGOV 可以全部动用，之后用后续新增资金再补回。
"""
        )
        st.markdown("#### 建仓期规则")
        st.dataframe(
            pd.DataFrame(
                [
                    {"标的": "VOO", "小加": "-3% / 1.5x", "中加": "-7% / 2x", "大加": "-10% / 3x + 少量SGOV"},
                    {"标的": "QQQ", "小加": "-3% / 1.5x", "中加": "-7% / 2x", "大加": "-10% / 3x + 少量SGOV"},
                    {"标的": "ISRG", "正常": "0.1x", "小加": "-15% / 0.2x", "中加": "-20% / 0.3x", "大加": "-23% / 0.5x"},
                    {"标的": "GOOGL", "正常": "0.1x", "小加": "-11% / 0.2x", "中加": "-19% / 0.3x", "大加": "-24% / 0.5x"},
                    {"标的": "MSFT", "正常": "0.1x", "小加": "-12% / 0.2x", "中加": "-18% / 0.3x", "大加": "-22% / 0.5x"},
                    {"标的": "AVGO", "正常": "0.1x", "小加": "-15% / 0.2x", "中加": "-22% / 0.3x", "大加": "-25% / 0.5x"},
                    {"标的": "NVDA", "正常": "0.1x", "小加": "-12% / 0.2x", "中加": "-21% / 0.3x", "大加": "-25% / 0.5x"},
                ]
            ),
            width="stretch",
            hide_index=True,
        )
        st.markdown("#### 长期定投期规则")
        st.dataframe(
            pd.DataFrame(
                [
                    {"标的": "VOO", "正常": "正常定投", "小加": "-3% 多买一点", "中加": "-7% 明显加仓", "大加": "-10% 大加"},
                    {"标的": "QQQ", "正常": "正常定投", "小加": "-3% 多买一点", "中加": "-7% 明显加仓", "大加": "-10% 大加"},
                    {"标的": "ISRG", "正常": "正常定投", "小加": "-15% 试探", "中加": "-20% 明显加", "大加": "-23% 大加"},
                    {"标的": "GOOGL", "正常": "持续小买", "小加": "-11% 试探", "中加": "-19% 明显加", "大加": "-24% 大加"},
                    {"标的": "MSFT", "正常": "持续小买", "小加": "-12% 试探", "中加": "-18% 明显加", "大加": "-22% 大加"},
                    {"标的": "AVGO", "正常": "观察/小买", "小加": "-15% 试探", "中加": "-22% 明显加", "大加": "-25% 大加"},
                    {"标的": "NVDA", "正常": "少量/观察", "小加": "-12% 试探", "中加": "-21% 明显加", "大加": "-25% AI恐慌级"},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

with st.expander("再平衡模块", expanded=False):
    if usd_total_cny <= 0:
        st.info("美元资产总额为 0，暂无法生成再平衡建议。")
    else:
        sgov_current_usd = (value_cny_by_symbol.get("SGOV", 0.0) / fx) if fx > 0 else 0.0
        sgov_target_pct = (
            target_weights.get("SGOV", 0.0) / usd_target_weight_total
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
        build_end_month = 9
        if now_for_build.year < build_end_year or (
            now_for_build.year == build_end_year and now_for_build.month <= build_end_month
        ):
            default_build_months = max(
                1,
                (build_end_year - now_for_build.year) * 12 + build_end_month - now_for_build.month + 1,
            )
    
        st.markdown("##### 参数设置")
        cfg_c1, cfg_c2 = st.columns(2)
        build_months_key = "rebalance_build_months_until_sep_2026"
        with cfg_c1:
            phase_choice = st.selectbox(
                "策略阶段",
                ["自动判断", _REBALANCE_PHASE_BUILD, _REBALANCE_PHASE_DCA],
                key="rebalance_phase_choice",
            )
        with cfg_c2:
            build_month_input_kwargs: dict[str, Any] = {
                "label": "建仓剩余月数（到10月前）",
                "min_value": 1,
                "max_value": 24,
                "step": 1,
                "key": build_months_key,
            }
            if build_months_key not in st.session_state:
                build_month_input_kwargs["value"] = int(default_build_months)
            build_months = st.number_input(**build_month_input_kwargs)
        st.caption(
            f"自动判断：{auto_phase}。建仓期默认按当前持仓缺口和剩余月份推进，不把平均进度当成硬性月预算。"
        )
        wizard_step = "4 买入确认"
    
        rebalance_phase = auto_phase if phase_choice == "自动判断" else phase_choice
        can_use_sgov_reserve = _rebalance_can_use_sgov_reserve(drawdown_pct_by_symbol)
        build_month_count = max(1, int(build_months))
    
        current_month_key = now_for_build.strftime("%Y-%m")
        budget_usage = _load_monthly_budget_usage(cloud_user_id, current_month_key)
        used_budget_usd = float(budget_usage.get("used_budget_usd", 0.0))
        planned_new_cash_usd = float(budget_usage.get("planned_new_cash_usd", 700.0))
        future_cash_month_count = max(0, build_month_count - 1)
        planned_future_new_cash_usd = planned_new_cash_usd * future_cash_month_count
        planned_usd_total_usd = usd_total_usd + planned_future_new_cash_usd
        planned_sgov_target_usd = sgov_target_pct * planned_usd_total_usd
        sgov_excess_usd = max(0.0, sgov_current_usd - planned_sgov_target_usd)
        planned_sgov_gap_usd = max(0.0, planned_sgov_target_usd - sgov_current_usd)
        planned_future_cash_sgov_reserve_usd = 0.0 if can_use_sgov_reserve else min(
            planned_future_new_cash_usd * sgov_target_pct,
            planned_sgov_gap_usd,
        )
        planned_future_cash_deployable_usd = max(
            0.0,
            planned_future_new_cash_usd - planned_future_cash_sgov_reserve_usd,
        )
        sgov_special_deploy_usd = (
            max(0.0, sgov_current_usd - sgov_excess_usd) if can_use_sgov_reserve else 0.0
        )
        current_deployable_pool_usd = max(0.0, cash_usd) + sgov_excess_usd + sgov_special_deploy_usd
        deployable_pool_usd = current_deployable_pool_usd
        bought_symbols_this_month = [
            sym for sym in budget_usage.get("bought_symbols", []) if sym in usd_symbols and sym != "SGOV"
        ]
        raw_bought_amounts = budget_usage.get("bought_amount_by_symbol", {})
        bought_amount_by_symbol = {
            str(sym).upper(): max(0.0, float(amount))
            for sym, amount in (raw_bought_amounts if isinstance(raw_bought_amounts, dict) else {}).items()
            if str(sym).upper() in usd_symbols and str(sym).upper() != "SGOV"
        }
        if bought_amount_by_symbol:
            used_budget_usd = sum(bought_amount_by_symbol.values())
        raw_bought_intensity = budget_usage.get("bought_intensity_by_symbol", {})
        bought_intensity_by_symbol = {
            str(sym).upper(): _normalize_rebalance_intensity(intensity)
            for sym, intensity in (raw_bought_intensity if isinstance(raw_bought_intensity, dict) else {}).items()
            if str(sym).upper() in usd_symbols and str(sym).upper() != "SGOV"
        }
        for sym in bought_symbols_this_month:
            bought_intensity_by_symbol.setdefault(sym, "normal")
        save_execution = False
        budget_back = False
        edited_planned_new_cash_usd = planned_new_cash_usd
        edited_bought_amount_by_symbol = dict(bought_amount_by_symbol)
        edited_bought_intensity_by_symbol = dict(bought_intensity_by_symbol)
        st.markdown("##### 预算设置")
        st.caption(
            f"设置 {current_month_key} 的未来资金假设，保存到 monthly_budget_usage.json。"
            "下月起每月计划新投入只用于计划期末目标分母；本月已经到账的钱请手动写到现金或持仓里。"
        )
        with st.form("monthly_budget_usage_form"):
            edited_planned_new_cash_usd = st.number_input(
                "下月起每月计划新投入(USD)",
                min_value=0.0,
                value=planned_new_cash_usd,
                step=50.0,
                format="%.2f",
                key=f"monthly_planned_cash_{current_month_key}",
                help="从下个月开始计入未来总资金；本月已入金请手动录入现金/持仓，避免重复计算。",
            )
            edited_used_budget_usd = used_budget_usd
            save_execution = st.form_submit_button("保存预算并刷新建议", type="primary")
        if budget_back:
            st.rerun()
        if save_execution:
            used_budget_usd = float(edited_used_budget_usd)
            planned_new_cash_usd = float(edited_planned_new_cash_usd)
            bought_amount_by_symbol = {
                str(sym).upper(): max(0.0, float(amount))
                for sym, amount in edited_bought_amount_by_symbol.items()
                if max(0.0, float(amount)) > 0
            }
            bought_intensity_by_symbol = {
                str(sym).upper(): _normalize_rebalance_intensity(intensity)
                for sym, intensity in edited_bought_intensity_by_symbol.items()
                if _normalize_rebalance_intensity(intensity) != "none"
            }
            bought_symbols_this_month = sorted(set(bought_intensity_by_symbol.keys()) | set(bought_amount_by_symbol.keys()))
            _save_monthly_budget_usage(
                cloud_user_id,
                current_month_key,
                used_budget_usd,
                bought_symbols_this_month,
                planned_new_cash_usd,
                bought_intensity_by_symbol,
                bought_amount_by_symbol,
            )
            st.success("已保存预算设置。")
            st.rerun()
    
        strategy_rows: list[dict[str, Any]] = []
        full_rebalance_need_usd = 0.0
        for sym in usd_symbols:
            meta = _ASSET_META[sym]
            current_cny = value_cny_by_symbol.get(sym, 0.0)
            current_usd = (current_cny / fx) if fx > 0 else 0.0
            amount_already_bought_usd = bought_amount_by_symbol.get(sym, 0.0)
            is_satellite = sym in _SATELLITE_SYMBOLS
            if sym == "SGOV" or is_satellite:
                planning_current_usd = current_usd
            else:
                planning_current_usd = max(0.0, current_usd - amount_already_bought_usd)
            target_pct = (target_weights.get(sym, 0.0) / usd_target_weight_total) if usd_target_weight_total > 0 else 0.0
            target_usd = target_pct * planned_usd_total_usd
            gap_usd = target_usd - planning_current_usd
            drawdown_pct = drawdown_pct_by_symbol.get(sym)
            strategy, multiplier, action, signal, intensity = _rebalance_strategy_signal(sym, drawdown_pct, rebalance_phase)
            previous_intensity = _normalize_rebalance_intensity(bought_intensity_by_symbol.get(sym, "none"))
            current_intensity = _normalize_rebalance_intensity(intensity)
            if (
                sym != "SGOV"
                and not is_satellite
                and _REBALANCE_INTENSITY_ORDER.get(previous_intensity, 0)
                > _REBALANCE_INTENSITY_ORDER.get(current_intensity, 0)
            ):
                multiplier, action, signal, intensity = _rebalance_signal_for_intensity(
                    sym,
                    rebalance_phase,
                    previous_intensity,
                )
                action = f"维持本月已确认{_REBALANCE_INTENSITY_LABELS.get(previous_intensity, '已买')}档"
            previous_multiplier = (
                0.0
                if is_satellite
                else _rebalance_intensity_multiplier(sym, rebalance_phase, previous_intensity)
            )
            additional_multiplier = max(0.0, float(multiplier) - previous_multiplier)
            already_bought_this_month = sym != "SGOV" and not is_satellite and previous_intensity != "none"
            forward_pe = forward_pe_by_symbol.get(sym)
            pe_band = _USD_ASSET_PE_BANDS.get(sym)
            valuation_split_factor = 1.0
            if (
                sym in _SATELLITE_SYMBOLS
                and isinstance(forward_pe, (int, float))
                and pe_band is not None
                and float(forward_pe) > float(pe_band[1])
            ):
                valuation_split_factor = 0.5
            base_budget_usd = (
                max(0.0, target_usd)
                if is_satellite
                else max(0.0, gap_usd) / build_month_count
            )
            planned_tier_buy_usd = (
                min(max(0.0, gap_usd), base_budget_usd * float(multiplier))
                if is_satellite
                else min(max(0.0, gap_usd), base_budget_usd * float(multiplier))
            )
            if sym == "VOO" and rebalance_phase == _REBALANCE_PHASE_BUILD:
                planned_tier_buy_usd = max(planned_tier_buy_usd, float(prices_now.get("VOO", 0.0)))
            valuation_adjusted_planned_tier_buy_usd = planned_tier_buy_usd * valuation_split_factor
            raw_buy_usd = (
                planned_tier_buy_usd * valuation_split_factor
                if is_satellite
                else min(
                    max(0.0, gap_usd),
                    base_budget_usd * additional_multiplier * valuation_split_factor,
                )
            )
            suggested_cap_usd = (
                planned_tier_buy_usd * valuation_split_factor
                if is_satellite
                else raw_buy_usd
            )
            if already_bought_this_month:
                raw_buy_usd = max(0.0, valuation_adjusted_planned_tier_buy_usd - amount_already_bought_usd)
                additional_multiplier = (raw_buy_usd / base_budget_usd) if base_budget_usd > 0 else 0.0
            if already_bought_this_month and raw_buy_usd <= 1e-9:
                raw_buy_usd = 0.0
                additional_multiplier = 0.0
            if sym == "SGOV":
                planned_tier_buy_usd = 0.0
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
                note = (
                    f"卫星股以10月底目标金额为 1x，当前按 {float(multiplier):.1f}x 计算，并取不超过实时缺口。"
                    if is_satellite
                    else "按当前阶段的常规月度节奏执行。"
                )
            if is_satellite and "10月底目标金额为 1x" not in note:
                note = f"{note} 卫星股以10月底目标金额为 1x，当前按 {float(multiplier):.1f}x 计算，并取不超过实时缺口。"
            if valuation_split_factor < 1.0:
                note = (
                    f"{note} Forward PE 高于合理区间上沿，本月按 50% 分批买入；"
                    "剩余额度等估值回落或下次复查。"
                )
            if already_bought_this_month:
                previous_label = _REBALANCE_INTENSITY_LABELS.get(previous_intensity, "已买")
                current_label = _REBALANCE_INTENSITY_LABELS.get(_normalize_rebalance_intensity(intensity), str(action))
                if raw_buy_usd <= 1e-9:
                    note = (
                        f"本月已执行到{previous_label}档，已买 USD {amount_already_bought_usd:,.2f}；"
                        "当前无需系统建议买入。若仍想加仓，可在买入确认里手动填写金额/股数。"
                    )
                elif intensity == "normal":
                    note = (
                        f"本月已执行到{previous_label}档，已买 USD {amount_already_bought_usd:,.2f}；"
                        "当前仍为 normal，本次补齐本月 normal 剩余额度。"
                    )
                else:
                    note = (
                        f"本月已执行到{previous_label}档；当前为{current_label}档，"
                        f"本次只补买档位差额（{additional_multiplier:.2f}x）。"
                    )
            valuation_note = _forward_pe_rebalance_note(sym, forward_pe)
            if valuation_note:
                note = f"{note} {valuation_note}"
            if sym != "SGOV":
                full_rebalance_need_usd += max(0.0, gap_usd)
            signal_remaining_usd = max(0.0, raw_buy_usd)
            if sym == "VOO" and rebalance_phase == _REBALANCE_PHASE_BUILD and sym not in bought_symbols_this_month:
                signal_remaining_usd = max(signal_remaining_usd, float(prices_now.get("VOO", 0.0)))
            strategy_rows.append(
                {
                    "标的": meta["label"],
                    "_symbol": sym,
                    "阶段": rebalance_phase,
                    "策略": strategy,
                    "动作": action,
                    "目前占比%": round(
                        (current_usd / planned_usd_total_usd * 100.0) if planned_usd_total_usd > 0 else 0.0,
                        2,
                    ),
                    "目标占比%": round(target_pct * 100.0, 2),
                    "月初口径到目标缺口(USD)": round(gap_usd, 2),
                    "本月计划应买(USD)": round(planned_tier_buy_usd, 2),
                    "实际买入(USD)": round(amount_already_bought_usd, 2),
                    "本月差额(应买-已买 USD)": round(
                        gap_usd if is_satellite else planned_tier_buy_usd - amount_already_bought_usd,
                        2,
                    ),
                    "60日回撤%": round(float(drawdown_pct), 2) if isinstance(drawdown_pct, (int, float)) else None,
                    "Forward PE": round(float(forward_pe), 2) if isinstance(forward_pe, (int, float)) else None,
                    "PE合理区间": _pe_band_text(sym) if sym in _SATELLITE_SYMBOLS else "-",
                    "估值判断": _forward_pe_judgment(sym, forward_pe) if sym in _SATELLITE_SYMBOLS else "-",
                    "回撤档位": signal,
                    "建议买入(USD)": 0.0,
                    "说明": note,
                    "_raw_buy_usd": raw_buy_usd,
                    "_signal_remaining_usd": signal_remaining_usd,
                    "_suggested_cap_usd": suggested_cap_usd,
                    "_intensity": intensity,
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
        st.markdown("##### 买入建议")
        run_budget_usd = float(suggested_run_budget_usd)
        strategy_budget_usd = min(run_budget_usd, remaining_signal_buy_usd)
        cash_scale = (strategy_budget_usd / remaining_signal_buy_usd) if remaining_signal_buy_usd > 0 else 0.0
        for row in strategy_rows:
            buy_usd = row["_signal_remaining_usd"] * cash_scale
            sym = str(row.get("_symbol", ""))
            if sym in _SATELLITE_SYMBOLS:
                buy_usd = min(float(row.get("_suggested_cap_usd", 0.0)), buy_usd)
            current_price_usd = float(prices_now.get(sym, 0.0))
            if (
                sym == "VOO"
                and rebalance_phase == _REBALANCE_PHASE_BUILD
                and current_price_usd > 0
                and float(row.get("_signal_remaining_usd", 0.0)) > 0
            ):
                buy_usd = max(buy_usd, current_price_usd)
            buy_shares = (buy_usd / current_price_usd) if current_price_usd > 0 else 0.0
            row["建议买入(USD)"] = round(buy_usd, 2)
            row["建议买入(股)"] = round(buy_shares, 4)
        actual_strategy_budget_usd = sum(float(row.get("建议买入(USD)", 0.0)) for row in strategy_rows)
        waiting_trigger_usd = max(0.0, remaining_deployable_budget_usd - actual_strategy_budget_usd)
    
        strategy_df = pd.DataFrame(strategy_rows).sort_values(
            by=["_signal_remaining_usd", "月初口径到目标缺口(USD)"],
            ascending=[False, False],
        )
        strategy_df = strategy_df.drop(columns=["_symbol", "_raw_buy_usd", "_signal_remaining_usd", "_suggested_cap_usd", "_intensity"])
        strategy_columns = [
            "标的",
            "阶段",
            "策略",
            "动作",
            "本月计划应买(USD)",
            "建议买入(USD)",
            "实际买入(USD)",
            "目前占比%",
            "目标占比%",
            "60日回撤%",
            "本月差额(应买-已买 USD)",
            "估值判断",
            "回撤档位",
            "说明",
        ]
        strategy_df = strategy_df[[col for col in strategy_columns if col in strategy_df.columns]]
        if wizard_step not in {"3 生成建议", "4 买入确认"}:
            st.stop()
        bought_tier_text = "无"
        bought_record_symbols = sorted(set(bought_intensity_by_symbol.keys()) | set(bought_amount_by_symbol.keys()))
        if bought_record_symbols:
            bought_tier_text = ", ".join(
                (
                    f"{sym}:"
                    f"{_REBALANCE_INTENSITY_LABELS.get(_normalize_rebalance_intensity(bought_intensity_by_symbol.get(sym, 'none')), '未买')}"
                    f"/USD {bought_amount_by_symbol.get(sym, 0.0):,.2f}"
                )
                for sym in bought_record_symbols
            )
        st.caption(
            f"自动判断阶段：{auto_phase}；当前执行阶段：{rebalance_phase}。"
            f"本轮按当前现金/SGOV可动用资金 USD {current_deployable_pool_usd:,.2f} "
            "生成建议；下月起计划新投入不进入本轮可用资金。"
            f"目标分母按当前美元资产 USD {usd_total_usd:,.2f} "
            f"+ 下月起未来 {int(future_cash_month_count)} 个月新投入 USD {planned_future_new_cash_usd:,.2f} "
            f"= USD {planned_usd_total_usd:,.2f} 计算。"
            f"当前计划可动用建仓池 USD {deployable_pool_usd:,.2f}，按计划期末仓位缺口和 {int(build_months)} 个月进度计算，"
            f"月度参考进度 USD {monthly_budget_usd:,.2f}。"
            f"本月已用预算 USD {total_executed_usd:,.2f}，参考剩余额度 USD {remaining_reference_budget_usd:,.2f}，"
            f"剩余可动用建仓池 USD {remaining_deployable_budget_usd:,.2f}。"
            f"本月已执行档位：{bought_tier_text}。"
            "若后续跌到更深档，系统只按档位倍数差额给出补买建议。"
            f"SGOV 当前 USD {sgov_current_usd:,.2f}，计划期末 20% 目标 USD {planned_sgov_target_usd:,.2f}，"
            f"未来新增资金中计划预留给 SGOV USD {planned_future_cash_sgov_reserve_usd:,.2f}，"
            f"可随时挪用 SGOV USD {sgov_excess_usd:,.2f}，"
            f"{'已触发大加档，可额外动用目标内 SGOV USD ' + format(sgov_special_deploy_usd, ',.2f') + '（可用完，后续补回）' if can_use_sgov_reserve else '未触发大加档，目标内 20% SGOV 暂不动用'}。"
            f"最终建议买入 USD {actual_strategy_budget_usd:,.2f}；"
            f"留待后续/等待触发资金 USD {waiting_trigger_usd:,.2f}。"
        )
        _render_themed_table(
            strategy_df,
            formatters={
                "目前占比%": "{:.2f}%",
                "目标占比%": "{:.2f}%",
                "60日回撤%": "{:.2f}%",
                "本月计划应买(USD)": "{:.2f}",
                "实际买入(USD)": "{:.2f}",
                "本月差额(应买-已买 USD)": "{:.2f}",
                "建议买入(USD)": "{:.2f}",
            },
        )
        if wizard_step == "4 买入确认":
            st.markdown("##### 买入确认")
            st.caption("按实际成交填写。默认值来自上方建议；保存后会同步本月已买记录和持仓均价。")
            execution_rows = [
                row for row in strategy_rows
                if str(row.get("_symbol", "")) != "SGOV"
            ]
            if not execution_rows:
                st.info("本轮没有需要确认的买入建议。")
            else:
                _, clear_col = st.columns([0.72, 0.28])
                clear_execution_records = clear_col.button(
                    "清零待确认买入",
                    key=f"clear_rebalance_execution_{current_month_key}",
                    help="只把当前买入确认表单里的金额和股数清零，不影响本月已买记录和持仓。",
                    use_container_width=True,
                )
                if clear_execution_records:
                    for row in execution_rows:
                        sym = str(row.get("_symbol", ""))
                        if not sym:
                            continue
                        st.session_state[f"rebalance_exec_amount_{current_month_key}_{sym}"] = 0.0
                        st.session_state[f"rebalance_exec_shares_{current_month_key}_{sym}"] = 0.0
                    st.success("已清零当前待确认的买入金额和股数；本月已买记录未改变。")
                with st.form("rebalance_execution_confirm_form"):
                    execution_inputs: dict[str, dict[str, float | str]] = {}
                    st.caption("建议为 0 的标的也会显示；如需强行买入，手动填写金额和股数即可。成交均价按 买入金额 / 买入股数 自动计算。")
                    header_cols = st.columns([0.16, 0.28, 0.28, 0.28])
                    header_cols[0].markdown("**标的**")
                    header_cols[1].markdown("**当前档位**")
                    header_cols[2].markdown("**买入金额(USD)**")
                    header_cols[3].markdown("**买入股数**")
                    for row in execution_rows:
                        sym = str(row.get("_symbol", ""))
                        suggested_usd = float(row.get("建议买入(USD)", 0.0))
                        current_price = float(prices_now.get(sym, 0.0))
                        suggested_shares = float(row.get("建议买入(股)", 0.0))
                        if suggested_shares <= 0 and current_price > 0:
                            suggested_shares = suggested_usd / current_price
                        tier_options = list(_REBALANCE_INTENSITY_LABELS.keys())
                        tier_labels = [_REBALANCE_INTENSITY_LABELS[key] for key in tier_options]
                        tier_label_to_key = {label: key for key, label in _REBALANCE_INTENSITY_LABELS.items()}
                        default_tier = _normalize_rebalance_intensity(row.get("_intensity", "normal"))
                        default_label = _REBALANCE_INTENSITY_LABELS.get(default_tier, _REBALANCE_INTENSITY_LABELS["normal"])
                        label_col, tier_col, amount_col, shares_col = st.columns([0.16, 0.28, 0.28, 0.28])
                        with label_col:
                            st.markdown(f"**{sym}**")
                        with tier_col:
                            selected_label = st.selectbox(
                                "当前档位",
                                options=tier_labels,
                                index=tier_labels.index(default_label) if default_label in tier_labels else tier_labels.index(_REBALANCE_INTENSITY_LABELS["normal"]),
                                key=f"rebalance_exec_tier_{current_month_key}_{sym}",
                                label_visibility="collapsed",
                            )
                        with amount_col:
                            actual_usd = st.number_input(
                                "买入金额(USD)",
                                min_value=0.0,
                                value=max(0.0, suggested_usd),
                                step=10.0,
                                format="%.2f",
                                key=f"rebalance_exec_amount_{current_month_key}_{sym}",
                                label_visibility="collapsed",
                            )
                        with shares_col:
                            actual_shares = st.number_input(
                                "买入股数",
                                min_value=0.0,
                                value=max(0.0, suggested_shares),
                                step=0.0001,
                                format="%.4f",
                                key=f"rebalance_exec_shares_{current_month_key}_{sym}",
                                label_visibility="collapsed",
                            )
                        execution_inputs[sym] = {
                            "amount": float(actual_usd),
                            "shares": float(actual_shares),
                            "intensity": tier_label_to_key[selected_label],
                        }
                    _, save_col = st.columns([0.72, 0.28])
                    execution_back = False
                    save_execution_confirm = save_col.form_submit_button("确认并同步持仓", type="primary")
                if save_execution_confirm:
                    updated_holdings = {
                        sym: dict(item)
                        for sym, item in holdings.items()
                    }
                    updated_balances = dict(balances_for_view)
                    updated_amounts = dict(bought_amount_by_symbol)
                    updated_intensity = dict(bought_intensity_by_symbol)
                    total_execution_usd = 0.0
                    executed_symbols: list[str] = []
                    for sym, item in execution_inputs.items():
                        amount = max(0.0, float(item.get("amount", 0.0)))
                        shares = max(0.0, float(item.get("shares", 0.0)))
                        price = (amount / shares) if amount > 0 and shares > 0 else 0.0
                        if amount <= 0 or shares <= 0 or price <= 0:
                            continue
                        updated_holdings[sym] = _merge_buy(updated_holdings.get(sym, {"shares": 0.0, "avg_cost": price}), shares, price)
                        updated_amounts[sym] = max(0.0, float(updated_amounts.get(sym, 0.0))) + amount
                        new_intensity = _normalize_rebalance_intensity(item.get("intensity", "normal"))
                        old_intensity = _normalize_rebalance_intensity(updated_intensity.get(sym, "none"))
                        if _REBALANCE_INTENSITY_ORDER.get(new_intensity, 0) > _REBALANCE_INTENSITY_ORDER.get(old_intensity, 0):
                            updated_intensity[sym] = new_intensity
                        executed_symbols.append(sym)
                        total_execution_usd += amount
                    if total_execution_usd <= 0:
                        st.warning("没有填写有效成交，未保存。")
                    else:
                        updated_balances["cash_usd"] = max(
                            0.0,
                            float(updated_balances.get("cash_usd", 0.0)) - total_execution_usd,
                        )
                        updated_symbols = sorted(set(updated_intensity.keys()) | set(updated_amounts.keys()))
                        _save_monthly_budget_usage(
                            cloud_user_id,
                            current_month_key,
                            sum(updated_amounts.values()),
                            updated_symbols,
                            planned_new_cash_usd,
                            updated_intensity,
                            updated_amounts,
                        )
                        save_mode = _save_user_state(cloud_user_id, updated_holdings, updated_balances)
                        st.success(
                            f"已同步买入确认：{', '.join(executed_symbols)}，合计 USD {total_execution_usd:,.2f}。"
                            f"持仓已保存到{'云端数据库' if save_mode == 'cloud' else '本地文件'}。"
                        )
                        if total_execution_usd > float(balances_for_view.get("cash_usd", 0.0)):
                            st.warning("本次买入金额超过当前现金，已将现金扣到 0；如果动用了 SGOV，请手动更新 SGOV/现金。")
                        st.rerun()

