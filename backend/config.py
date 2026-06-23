from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
HOLDINGS_FILE = ROOT_DIR / "holdings.json"
BALANCES_FILE = ROOT_DIR / "balances.json"
MONTHLY_USAGE_FILE = ROOT_DIR / "monthly_budget_usage.json"

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
    "ISRG": 0.02,
    "GOOGL": 0.012,
    "MSFT": 0.008,
    "AVGO": 0.012,
    "NVDA": 0.008,
    "SGOV": 0.12,
    "001015": 0.20,
}

SATELLITE_SYMBOLS = ("ISRG", "GOOGL", "MSFT", "AVGO", "NVDA")
USD_SYMBOLS = ("VOO", "QQQ", *SATELLITE_SYMBOLS, "SGOV")
ALL_SYMBOLS = tuple(ASSET_META.keys())

PE_BANDS = {
    "VOO": (18.0, 24.0),
    "QQQ": (24.0, 36.0),
    "ISRG": (40.0, 65.0),
    "GOOGL": (18.0, 28.0),
    "MSFT": (24.0, 36.0),
    "AVGO": (24.0, 34.0),
    "NVDA": (28.0, 45.0),
    "SGOV": (0.0, 10.0),
}

QQ_US = {
    "VOO": "usVOO",
    "QQQ": "usQQQ",
    "ISRG": "usISRG",
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
    "GOOGL": "US.GOOGL",
    "MSFT": "US.MSFT",
    "AVGO": "US.AVGO",
    "NVDA": "US.NVDA",
    "SGOV": "US.SGOV",
}

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
