from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
HOLDINGS_FILE = ROOT_DIR / "holdings.json"
BALANCES_FILE = ROOT_DIR / "balances.json"
MONTHLY_USAGE_FILE = ROOT_DIR / "monthly_budget_usage.json"
SATELLITE_TARGETS_FILE = ROOT_DIR / "satellite_targets.json"
SATELLITE_UNIVERSE_FILE = ROOT_DIR / "satellite_universe.json"
PORTFOLIO_HISTORY_FILE = ROOT_DIR / "portfolio_history.json"
TRADE_RECORDS_FILE = ROOT_DIR / "trades.json"

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}
HTTP_TIMEOUT = (3, 12)

FALLBACK_PRICES = {
    "VOO": 400.0,
    "QQQ": 400.0,
    "ISRG": 450.0,
    "TEM": 60.0,
    "PLTR": 100.0,
    "GOOGL": 180.0,
    "MSFT": 420.0,
    "AVGO": 170.0,
    "NVDA": 120.0,
    "SGOV": 100.0,
    "001015": 1.0,
}

ASSET_META = {
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

TARGET_WEIGHTS = {
    "VOO": 0.24,
    "QQQ": 0.18,
    "ISRG": 0.019,
    "GOOGL": 0.0114,
    "MSFT": 0.0076,
    "AVGO": 0.0114,
    "NVDA": 0.0076,
    "TEM": 0.003,
    "PLTR": 0.0,
    "SGOV": 0.12,
    "001015": 0.20,
}

SATELLITE_SYMBOLS = ("ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO", "NVDA")
USD_SYMBOLS = ("VOO", "QQQ", *SATELLITE_SYMBOLS, "SGOV")
ALL_SYMBOLS = tuple(ASSET_META.keys())
DEFAULT_SATELLITE_TARGET_PCTS = {
    "ISRG": 31.6666,
    "TEM": 5.0,
    "PLTR": 0.0,
    "GOOGL": 19.0,
    "MSFT": 12.6667,
    "AVGO": 19.0,
    "NVDA": 12.6667,
}

PE_BANDS = {
    "VOO": (18.0, 24.0),
    "QQQ": (24.0, 36.0),
    "ISRG": (40.0, 65.0),
    "PLTR": (60.0, 100.0),
    "GOOGL": (18.0, 28.0),
    "MSFT": (24.0, 36.0),
    "AVGO": (24.0, 34.0),
    "NVDA": (28.0, 45.0),
    "SGOV": (0.0, 10.0),
}

PEG_BANDS = {
    "ISRG": (4.1, 7.3),
    "PLTR": (1.5, 3.0),
    "GOOGL": (1.3, 1.9),
    "MSFT": (1.5, 2.6),
    "AVGO": (0.9, 3.0),
    "NVDA": (0.3, 0.4),
}

PS_BANDS = {
    "TEM": (5.0, 9.0),
}

QQ_US = {
    "VOO": "usVOO",
    "QQQ": "usQQQ",
    "ISRG": "usISRG",
    "TEM": "usTEM",
    "PLTR": "usPLTR",
    "GOOGL": "usGOOGL",
    "MSFT": "usMSFT",
    "AVGO": "usAVGO",
    "NVDA": "usNVDA",
    "SGOV": "usSGOV",
}

SINA_GB = {
    "VOO": "gb_voo",
    "QQQ": "gb_qqq",
    "ISRG": "gb_isrg",
    "TEM": "gb_tem",
    "PLTR": "gb_pltr",
    "GOOGL": "gb_googl",
    "MSFT": "gb_msft",
    "AVGO": "gb_avgo",
    "NVDA": "gb_nvda",
    "SGOV": "gb_sgov",
}

FUTU_US = {
    "VOO": "US.VOO",
    "QQQ": "US.QQQ",
    "ISRG": "US.ISRG",
    "TEM": "US.TEM",
    "PLTR": "US.PLTR",
    "GOOGL": "US.GOOGL",
    "MSFT": "US.MSFT",
    "AVGO": "US.AVGO",
    "NVDA": "US.NVDA",
    "SGOV": "US.SGOV",
}


_STATIC_SATELLITE_SYMBOLS = ("ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO", "NVDA")
_SATELLITE_TOTAL_WEIGHT = sum(TARGET_WEIGHTS.get(sym, 0.0) for sym in _STATIC_SATELLITE_SYMBOLS)


def _default_satellite_universe() -> list[dict[str, Any]]:
    return [
        {
            "symbol": sym,
            "label": ASSET_META.get(sym, {}).get("label", sym),
            "target_pct": DEFAULT_SATELLITE_TARGET_PCTS.get(sym, 0.0),
        }
        for sym in _STATIC_SATELLITE_SYMBOLS
    ]


def _normalize_satellite_universe(raw: Any) -> list[dict[str, Any]]:
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


def _load_satellite_universe() -> list[dict[str, Any]]:
    raw: Any = None
    if SATELLITE_UNIVERSE_FILE.exists():
        try:
            raw = json.loads(SATELLITE_UNIVERSE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = None
    return _normalize_satellite_universe(raw)


def load_satellite_universe_config() -> list[dict[str, Any]]:
    return [dict(item) for item in _load_satellite_universe()]


def save_satellite_universe_config(items: Any) -> list[dict[str, Any]]:
    normalized = _normalize_satellite_universe(items)
    SATELLITE_UNIVERSE_FILE.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    _apply_satellite_universe()
    return load_satellite_universe_config()


def _apply_satellite_universe() -> None:
    global SATELLITE_SYMBOLS, USD_SYMBOLS, ALL_SYMBOLS, DEFAULT_SATELLITE_TARGET_PCTS

    universe = _load_satellite_universe()
    configured = tuple(item["symbol"] for item in universe)
    configured_set = set(configured)
    for sym in _STATIC_SATELLITE_SYMBOLS:
        if sym not in configured_set:
            FALLBACK_PRICES.pop(sym, None)
            ASSET_META.pop(sym, None)
            TARGET_WEIGHTS.pop(sym, None)
            QQ_US.pop(sym, None)
            SINA_GB.pop(sym, None)
            FUTU_US.pop(sym, None)

    DEFAULT_SATELLITE_TARGET_PCTS = {}
    for item in universe:
        sym = item["symbol"]
        FALLBACK_PRICES[sym] = 0.0
        ASSET_META[sym] = {"label": item["label"], "currency": "USD"}
        DEFAULT_SATELLITE_TARGET_PCTS[sym] = float(item["target_pct"])
        TARGET_WEIGHTS[sym] = _SATELLITE_TOTAL_WEIGHT * float(item["target_pct"]) / 100.0
        QQ_US.pop(sym, None)
        SINA_GB.pop(sym, None)
        FUTU_US[sym] = f"US.{sym}"

    ordered_meta: dict[str, dict[str, str]] = {}
    for sym in ("VOO", "QQQ", *configured, "SGOV", "001015"):
        if sym in ASSET_META:
            ordered_meta[sym] = ASSET_META[sym]
    ASSET_META.clear()
    ASSET_META.update(ordered_meta)

    SATELLITE_SYMBOLS = configured
    USD_SYMBOLS = ("VOO", "QQQ", *SATELLITE_SYMBOLS, "SGOV")
    ALL_SYMBOLS = tuple(ASSET_META.keys())


_apply_satellite_universe()

FUND_CODES = {"001015": "001015"}

REBALANCE_PHASE_BUILD = "建仓期"
REBALANCE_PHASE_DCA = "长期定投期"

REBALANCE_RULES = {
    REBALANCE_PHASE_BUILD: {
        "VOO": {"normal": (1.0, "正常建仓", "每月机械定投", "normal"), "bands": [(-10.0, 3.0, "大加", "3x", "large"), (-7.0, 2.0, "中加", "2x", "medium"), (-3.0, 1.5, "小加", "1.5x", "small")]},
        "QQQ": {"normal": (1.0, "正常建仓", "每月按目标推进", "normal"), "bands": [(-13.0, 4.0, "大加", "4x", "large"), (-10.0, 2.5, "中加", "2.5x", "medium"), (-5.0, 1.5, "小加", "1.5x", "small")]},
        "ISRG": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-23.0, 0.5, "大加", "大加", "large"), (-20.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "GOOGL": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-24.0, 0.5, "大加", "大加", "large"), (-19.0, 0.3, "中加", "中加", "medium"), (-11.0, 0.2, "小加", "小加", "small")]},
        "MSFT": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-22.0, 0.5, "大加", "大加", "large"), (-18.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "AVGO": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-22.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "NVDA": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-21.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
    },
    REBALANCE_PHASE_DCA: {
        "VOO": {"normal": (1.0, "正常定投", "正常", "normal"), "bands": [(-10.0, 2.5, "大加", "2.5x", "large"), (-7.0, 1.75, "明显加仓", "1.75x", "medium"), (-3.0, 1.25, "多买一点", "1.25x", "small")]},
        "QQQ": {"normal": (0.5, "小额定投", "正常", "normal"), "bands": [(-13.0, 2.5, "大加", "2.5x", "large"), (-10.0, 1.75, "明显加仓", "1.75x", "medium"), (-5.0, 1.25, "增加投入", "1.25x", "small")]},
        "ISRG": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-23.0, 0.5, "大加", "大加", "large"), (-20.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "GOOGL": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-24.0, 0.5, "大加", "大加", "large"), (-19.0, 0.3, "中加", "中加", "medium"), (-11.0, 0.2, "小加", "小加", "small")]},
        "MSFT": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-22.0, 0.5, "大加", "大加", "large"), (-18.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
        "AVGO": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-22.0, 0.3, "中加", "中加", "medium"), (-15.0, 0.2, "小加", "小加", "small")]},
        "NVDA": {"normal": (0.1, "正常", "正常", "normal"), "bands": [(-25.0, 0.5, "大加", "大加", "large"), (-21.0, 0.3, "中加", "中加", "medium"), (-12.0, 0.2, "小加", "小加", "small")]},
    },
}

INTENSITY_ORDER = {"none": 0, "normal": 1, "small": 2, "medium": 3, "large": 4}
INTENSITY_LABELS = {"none": "未买", "normal": "普通/正常", "small": "小加/试探", "medium": "中加", "large": "大加"}
