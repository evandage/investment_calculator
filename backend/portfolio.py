from __future__ import annotations

import json
import time
import re
import threading
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .drawdown_episodes import (
    advance_episode_on_close,
    ensure_threshold_snapshot,
    intraday_warning,
)

from .config import (
    ALL_SYMBOLS,
    ASSET_META,
    FUND_CODES,
    INTENSITY_LABELS,
    INTENSITY_ORDER,
    PE_BANDS,
    PEG_BANDS,
    PS_BANDS,
    REBALANCE_PHASE_BUILD,
    REBALANCE_RULES,
    SATELLITE_SYMBOLS,
    TARGET_WEIGHTS,
    TZ_SHANGHAI,
    USD_SYMBOLS,
    load_satellite_universe_config,
)
from .market_data import fetch_quotes
from .storage import (
    load_closed_satellite_pnl,
    load_drawdown_episode_store,
    load_monthly_usage,
    load_fx_conversion_records,
    load_portfolio_history,
    load_trade_records,
    load_satellite_targets,
    load_user_state,
    save_monthly_usage,
    save_drawdown_episode_store,
    save_fx_conversion_records,
    save_portfolio_history,
    save_trade_records,
    save_user_state,
)


BUILD_TARGET_YEAR = 2026
BUILD_TARGET_MONTH = 10
MIDTERM_ELECTION_DATE = date(2026, 11, 3)
_DRAWDOWN_CACHE: dict[str, tuple[dict[str, Any], float]] = {}
_DRAWDOWN_CACHE_TTL_SECONDS = 300
_EPISODE_STATE_LOCK = threading.Lock()
NY_TZ = ZoneInfo("America/New_York")
PERFORMANCE_WRITE_HOUR = 8
US_MARKET_CLOSE_MINUTE = 16 * 60
PERFORMANCE_HISTORY_START_DATE = "2026-07-07"
PERFORMANCE_CHART_START_DATE = "2026-07-08"
PERFORMANCE_CHART_BASELINE_DATE = "2026-07-08"
FUND_HISTORY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://fundf10.eastmoney.com/",
}


def effective_target_weights() -> dict[str, float]:
    weights = dict(TARGET_WEIGHTS)
    satellite_total = sum(TARGET_WEIGHTS.get(sym, 0.0) for sym in SATELLITE_SYMBOLS)
    satellite_targets = load_satellite_targets()
    for sym in SATELLITE_SYMBOLS:
        weights[sym] = satellite_total * float(satellite_targets.get(sym, 0.0)) / 100.0
    return weights


def default_build_months(now: datetime | None = None) -> int:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.year > BUILD_TARGET_YEAR or (current.year == BUILD_TARGET_YEAR and current.month > BUILD_TARGET_MONTH):
        return 1
    return max(1, (BUILD_TARGET_YEAR - current.year) * 12 + BUILD_TARGET_MONTH - current.month + 1)


def future_month_keys(now: datetime, future_cash_months: int) -> list[str]:
    keys: list[str] = []
    year = now.year
    month = now.month
    for _ in range(max(0, future_cash_months)):
        month += 1
        if month > 12:
            year += 1
            month = 1
        keys.append(f"{year}-{month:02d}")
    return keys


def rebalance_rules_payload(
    build_months: int,
    future_cash_months: int,
    planned_new_cash_usd: float,
    future_cash_total_usd: float = 0.0,
) -> dict[str, Any]:
    satellite_items = load_satellite_universe_config()
    satellite_names = " / ".join(item["symbol"] for item in satellite_items) or "-"
    zero_target_names = " / ".join(item["symbol"] for item in satellite_items if float(item.get("target_pct") or 0.0) == 0.0)
    zero_target_note = f"{zero_target_names} 当前目标为 0%，不产生系统买入建议。" if zero_target_names else "目标为 0% 的观察成员不产生系统买入建议。"
    return {
        "title": "再平衡算法规则",
        "build_target": f"{BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}",
        "build_months": build_months,
        "future_cash_months": future_cash_months,
        "planned_new_cash_usd": planned_new_cash_usd,
        "future_cash_total_usd": future_cash_total_usd,
        "sections": [
            {
                "heading": "长期目标",
                "items": [
                    "美元资产目标：VOO 40% / QQQ 30% / AI卫星仓位 10% / 短债(SGOV) 20%。",
                    f"卫星仓位内部目标：{satellite_names}；{zero_target_note}",
                    "A股基金按 Dashboard 目标占比展示，但当前买入建议只处理美元标的。",
                ],
            },
            {
                "heading": "建仓期分母",
                "items": [
                    f"建仓期默认计算到 {BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}，当前共 {build_months} 个月。",
                    f"未来入金纳入目标分母，共 {future_cash_months} 个月，每月 USD {planned_new_cash_usd:,.2f}；本轮实际建议仍只使用当前可释放资金。",
                    "目标分母 = USD 现金 + SGOV 当前金额 + 各美元标的持仓成本金额 + 未来入金；是否接近目标优先看成本占比，而不是市值占比。",
                ],
            },
            {
                "heading": "建议金额",
                "items": [
                    "成本缺口按月初成本口径计算，即目标金额减去当前成本扣除本月已买后的金额。",
                    "VOO/QQQ 按剩余月份折算为每周基准，再乘档位倍率计算本轮计划应买；个股按目标金额 × 0.1 × 档位倍率计算一手。",
                    f"卫星股以10月底目标金额为 1x；{zero_target_note}",
                    "估值/追高系数不改变计划应买金额，只影响本轮建议买入；全部标的统一按近 60 个交易日高点回撤定档，近 5 日涨幅偏热时只做备注提示。",
                    "建议买入总额受 USD 现金与 SGOV 安全线以上可释放额度限制；可动用资金不足时按比例缩放计划应买。",
                ],
            },
            {
                "heading": "档位规则",
                "items": [
                    "盘中价格只产生预警；只有最新完整交易日收盘价可以正式触发档位。同一回撤周期每档只确认一次。",
                    "回撤周期开始时绑定当月档位快照；周期内冻结。月末生成的新快照只供下一个回撤周期使用。",
                    "基础档位取全历史60日回撤的65% / 85% / 95%分位数；仅以上月末波动状态乘0.95 / 1.00 / 1.05，并在单次回撤周期内冻结。执行层再按独立触发频率校准档位名称。",
                    "VOO：正常 1x，小加 -3.5% / 1.5x，中加 -6.5% / 2.5x，大加 -13% / 4x。",
                    "QQQ：正常 1x，小加 -4% / 1.25x，中加 -9% / 2x，大加 -16.5% / 3x。",
                    "ISRG：正常 0.1x，小加 -8.5% / 0.2x，中加 -16% / 0.3x，大加 -25% / 0.5x；-25%过去10年独立触发4次，约2.5年一次。",
                    "TEM：复核；自身样本档位 -31% / -40.5% / -52.5% 仅供观察。同行组未确认，同行与收缩后最终档位为空，不自动控制买入。",
                    "PLTR：正常 0.1x，小加 -20.5% / 0.2x，中加 -31% / 0.3x，大加 -40.5% / 0.5x。",
                    "GOOGL：正常 0.1x，小加 -6.5% / 0.2x，中加 -14% / 0.3x，大加 -20% / 0.5x。",
                    "MSFT：正常 0.1x，小加 -5.5% / 0.2x，中加 -11.5% / 0.3x，大加 -18.5% / 0.5x。",
                    "AVGO：正常 0.1x，小加 -8% / 0.2x，中加 -14.5% / 0.3x，大加 -22% / 0.5x。",
                ],
            },
        ],
    }


def normalize_intensity(value: Any) -> str:
    v = str(value or "none").strip().lower()
    return {
        "": "none",
        "none": "none",
        "manual_review_only": "manual_review_only",
        "normal": "normal",
        "regular": "normal",
        # Legacy QQQ-only tiers are normalized to the closest standard tier
        # so old monthly records remain readable without creating new tiers.
        "probe": "small",
        "dip": "small",
        "month_end": "medium",
        "month-end": "medium",
        "monthend": "medium",
        "small": "small",
        "medium": "medium",
        "large": "large",
    }.get(v, "none")


def intensity_rank(value: Any) -> int:
    return {
        "none": 0,
        "normal": 1,
        "small": 4,
        "medium": 5,
        "large": 6,
    }.get(normalize_intensity(value), 0)


def intensity_label(value: Any) -> str:
    intensity = normalize_intensity(value)
    return INTENSITY_LABELS.get(intensity, str(intensity))


def intensity_multiplier(symbol: str, phase: str, intensity: str) -> float:
    intensity = normalize_intensity(intensity)
    rule = REBALANCE_RULES.get(phase, {}).get(symbol)
    if not rule or intensity == "none":
        return 0.0
    if intensity == "normal":
        return float(rule["normal"][0])
    for _, multiplier, _, _, band_intensity in rule["bands"]:
        if band_intensity == intensity:
            return float(multiplier)
    return 0.0


def signal_for_drawdown(symbol: str, drawdown_pct: float | None, phase: str) -> tuple[float, str, str, str]:
    rule = REBALANCE_RULES.get(phase, {}).get(symbol)
    if not rule:
        return 0.0, "暂无规则", "暂无规则", "normal"
    if rule.get("mode") == "manual_review_only":
        return 0.0, "复核", "复核", "manual_review_only"
    normal = rule["normal"]
    if not isinstance(drawdown_pct, (int, float)):
        return float(normal[0]), str(normal[1]), str(normal[2]), str(normal[3])
    for threshold, multiplier, action, signal, intensity in rule["bands"]:
        if float(drawdown_pct) <= threshold:
            return float(multiplier), str(action), str(signal), str(intensity)
    return float(normal[0]), str(normal[1]), str(normal[2]), str(normal[3])


def signal_for_historical_position(symbol: str, item: dict[str, Any], phase: str) -> tuple[float, str, str, str]:
    return signal_for_drawdown(symbol, item.get("drawdown_pct"), phase)


def signal_for_intensity(symbol: str, phase: str, intensity: str) -> tuple[float, str, str, str]:
    rule = REBALANCE_RULES.get(phase, {}).get(symbol)
    intensity = normalize_intensity(intensity)
    if rule and rule.get("mode") == "manual_review_only":
        return 0.0, "复核", "复核", "manual_review_only"
    if not rule or intensity == "none":
        return 0.0, "暂无规则", "暂无规则", "normal"
    if intensity == "normal":
        normal = rule["normal"]
        return float(normal[0]), str(normal[1]), str(normal[2]), str(normal[3])
    for _, multiplier, action, signal, band_intensity in rule["bands"]:
        if band_intensity == intensity:
            return float(multiplier), str(action), str(signal), str(band_intensity)
    return signal_for_intensity(symbol, phase, "normal")


def pe_band_text(symbol: str) -> str:
    band = PE_BANDS.get(symbol)
    return "-" if not band else f"{band[0]:.0f}-{band[1]:.0f}"


def peg_band_text(symbol: str) -> str:
    band = PEG_BANDS.get(symbol)
    return "-" if not band else f"{band[0]:.1f}-{band[1]:.1f}"


def ps_band_text(symbol: str) -> str:
    band = PS_BANDS.get(symbol)
    return "-" if not band else f"{band[0]:.1f}-{band[1]:.1f}"


def pe_judgment(symbol: str, forward_pe: float | None) -> str:
    band = PE_BANDS.get(symbol)
    if symbol not in SATELLITE_SYMBOLS or not band:
        return "-"
    if not isinstance(forward_pe, (int, float)):
        return "缺数据"
    low, high = band
    if forward_pe < low:
        return "偏低"
    if forward_pe <= high:
        return "合理"
    return "偏贵"


def fx_conversion_summary(records: list[dict[str, Any]], fallback_fx: float) -> dict[str, float]:
    total_cny = sum(max(0.0, float(row.get("cny_amount", 0.0) or 0.0)) for row in records)
    total_usd = sum(max(0.0, float(row.get("usd_amount", 0.0) or 0.0)) for row in records)
    avg_rate = total_cny / total_usd if total_usd > 0 else fallback_fx
    return {
        "total_cny": total_cny,
        "total_usd": total_usd,
        "avg_rate": avg_rate,
    }


def valuation_split_for_row(symbol: str, item: dict[str, Any]) -> tuple[float, str]:
    fpe = item.get("forward_pe")
    band = PE_BANDS.get(symbol)
    peg = item.get("peg")
    peg_band = PEG_BANDS.get(symbol)
    if symbol in SATELLITE_SYMBOLS:
        ps = item.get("forward_ps") or item.get("ps")
        ps_band = PS_BANDS.get(symbol)
        high_fpe = symbol not in PS_BANDS and isinstance(fpe, (int, float)) and band and fpe > band[1]
        high_ps = symbol in PS_BANDS and isinstance(ps, (int, float)) and ps_band and ps > ps_band[1]
        high_peg = isinstance(peg, (int, float)) and peg_band and peg > peg_band[1]
        if high_fpe or high_ps or high_peg:
            signals = []
            if high_fpe:
                signals.append("Forward PE")
            if high_ps:
                signals.append("PS")
            if high_peg:
                signals.append("PEG")
            return 0.5, f"{' / '.join(signals)} 高于合理区间，估值系数 0.50。"

    recent_5d = item.get("recent_5d_pct")
    if symbol in {"VOO", "QQQ"} and isinstance(recent_5d, (int, float)):
        if recent_5d >= 4.0:
            return 0.5, f"{symbol} 近 5 个交易日涨幅 {recent_5d:.2f}%，短期偏热，估值系数 0.50。"
        if recent_5d >= 3.0:
            return 0.75, f"{symbol} 近 5 个交易日涨幅 {recent_5d:.2f}%，本轮稍微分批，估值系数 0.75。"
    return 1.0, ""


def historical_probability_note(symbol: str, item: dict[str, Any], intensity: str) -> str:
    if symbol not in {"PLTR", "TEM"}:
        return ""
    drawdown = item.get("drawdown_pct")
    recent_5d = item.get("recent_5d_pct")
    rebound = item.get("rebound_pct")
    parts = []
    if isinstance(drawdown, (int, float)):
        parts.append(f"60日高点回撤 {float(drawdown):.2f}%")
    if isinstance(recent_5d, (int, float)):
        parts.append(f"近5日涨跌 {float(recent_5d):+.2f}%")
    if isinstance(rebound, (int, float)):
        parts.append(f"较60日低点反弹 {float(rebound):+.2f}%")
    if not parts:
        return f"{symbol} 暂无足够历史波动数据，按 normal 观察。"
    if normalize_intensity(intensity) == "manual_review_only":
        return f"{symbol} 仅展示历史位置，不自动定档（{'；'.join(parts)}）。"
    label = INTENSITY_LABELS.get(normalize_intensity(intensity), "normal")
    return f"{symbol} 按历史涨跌位置定档：{label}（{'；'.join(parts)}）。"


def fetch_60d_metrics(symbol: str, current_price: float | None = None) -> dict[str, Any]:
    now = time.time()
    completed_day = completed_performance_day()
    cached = _DRAWDOWN_CACHE.get(symbol)
    if (
        cached
        and now - cached[1] < _DRAWDOWN_CACHE_TTL_SECONDS
        and cached[0].get("expected_completed_day") == completed_day
    ):
        metrics = dict(cached[0])
    else:
        metrics = {
            "drawdown_pct": None,
            "confirmed_drawdown_pct": None,
            "intraday_drawdown_pct": None,
            "rebound_pct": None,
            "recent_5d_pct": None,
            "peak": None,
            "trough": None,
            "prev_5d": None,
            "confirmed_close_date": None,
            "confirmed_close_price": None,
            "expected_completed_day": completed_day,
        }
        try:
            from .ohlcv import fetch_ohlcv

            payload = fetch_ohlcv(symbol, "1d")
            bars = payload.get("bars") if isinstance(payload, dict) else []
            completed_bars = [
                bar
                for bar in (bars or [])
                if isinstance(bar, dict)
                and str(bar.get("time") or "") <= completed_day
                and float(bar.get("close") or 0) > 0
            ][-60:]
            closes = [float(bar.get("close")) for bar in completed_bars]
            if closes:
                peak = max(closes)
                trough = min(closes)
                last = closes[-1]
                prev_5d = closes[-6] if len(closes) >= 6 else None
                confirmed_date = str(completed_bars[-1].get("time") or "")
                confirmed_drawdown = (last / peak - 1.0) * 100.0 if peak > 0 else None
                metrics = {
                    "drawdown_pct": confirmed_drawdown,
                    "confirmed_drawdown_pct": confirmed_drawdown,
                    "intraday_drawdown_pct": confirmed_drawdown,
                    "rebound_pct": (last / trough - 1.0) * 100.0 if trough > 0 else None,
                    "recent_5d_pct": (last / prev_5d - 1.0) * 100.0 if prev_5d and prev_5d > 0 else None,
                    "peak": peak,
                    "trough": trough,
                    "prev_5d": prev_5d,
                    "confirmed_close_date": confirmed_date,
                    "confirmed_close_price": last,
                    "expected_completed_day": completed_day,
                }
        except Exception:
            metrics = {
                "drawdown_pct": None,
                "confirmed_drawdown_pct": None,
                "intraday_drawdown_pct": None,
                "rebound_pct": None,
                "recent_5d_pct": None,
                "peak": None,
                "trough": None,
                "prev_5d": None,
                "confirmed_close_date": None,
                "confirmed_close_price": None,
                "expected_completed_day": completed_day,
            }
        if metrics.get("peak") and metrics.get("trough"):
            _DRAWDOWN_CACHE[symbol] = (dict(metrics), now)

    price = float(current_price or 0.0)
    peak = metrics.get("peak")
    trough = metrics.get("trough")
    if price > 0 and isinstance(peak, (int, float)) and peak > 0:
        preview_peak = max(float(peak), price)
        metrics["intraday_drawdown_pct"] = (price / preview_peak - 1.0) * 100.0
    if price > 0 and isinstance(trough, (int, float)) and trough > 0:
        metrics["rebound_pct"] = (price / float(trough) - 1.0) * 100.0
    prev_5d = metrics.get("prev_5d")
    if price > 0 and isinstance(prev_5d, (int, float)) and prev_5d > 0:
        metrics["recent_5d_pct"] = (price / float(prev_5d) - 1.0) * 100.0
    metrics["intraday_price"] = price if price > 0 else None
    return metrics


def _daily_amount(value: float, pct: float) -> float:
    ratio = pct / 100.0
    if abs(1.0 + ratio) <= 1e-9:
        return 0.0
    return value - value / (1.0 + ratio)


def _fmt_usd_compact(value: float) -> str:
    return f"{float(value):,.0f}" if abs(float(value)) >= 100 else f"{float(value):,.2f}"


def _fmt_usd_exact(value: float) -> str:
    return f"{float(value):,.2f}"


def trade_cost_basis(record: dict[str, Any]) -> float:
    cost_basis = max(0.0, float(record.get("cost_basis", 0.0) or 0.0))
    if cost_basis > 0:
        return cost_basis
    shares = max(0.0, float(record.get("shares", 0.0) or 0.0))
    prev_avg_cost = max(0.0, float(record.get("prev_avg_cost", 0.0) or 0.0))
    if shares > 0 and prev_avg_cost > 0:
        return shares * prev_avg_cost
    return max(0.0, float(record.get("amount_usd", 0.0) or 0.0))


def _currency_value_to_cny(value: float, currency: str, fx: float) -> float:
    return value * fx if currency == "USD" else value


def _quote_price_line(symbol: str, quote: dict[str, Any]) -> str:
    currency = ASSET_META[symbol]["currency"]
    regular = float(quote.get("regular_price") or quote.get("price") or 0.0)
    decimals = 2 if currency == "USD" else 4
    text = f"{currency} {regular:,.{decimals}f}"
    effective = float(quote.get("price") or regular)
    if quote.get("session") != "regular" and effective > 0 and abs(effective - regular) > 1e-9:
        text += f"（{effective:,.{decimals}f}）"
    return text


def treemap_daily_pct(quote: dict[str, Any], regular_pct: float) -> float:
    extended_pct = coerce_optional_float(quote.get("extended_change_pct"))
    if extended_pct is None or str(quote.get("session") or "").lower() in {"regular", "closed"}:
        return regular_pct
    # 拓展盘涨跌幅以收盘价为基准；复利相乘后才是当前价格相对昨日收盘的完整变动。
    return ((1.0 + regular_pct / 100.0) * (1.0 + extended_pct / 100.0) - 1.0) * 100.0


def build_visualizations(
    rows: list[dict[str, Any]],
    balances: dict[str, float],
    value_cny_by_symbol: dict[str, float],
    fx: float,
) -> dict[str, Any]:
    target_weights = effective_target_weights()
    row_by_symbol = {row["symbol"]: row for row in rows}
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS)
    usd_cash_cny = float(balances.get("cash_usd", 0.0)) * fx
    allocation_total_cny = usd_total_cny + usd_cash_cny
    satellite_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in SATELLITE_SYMBOLS)

    pnl_rank = []
    satellite_pnl_rank = []
    satellite_pnl_cny = 0.0
    satellite_pnl_usd = 0.0
    for row in rows:
        sym = row["symbol"]
        if sym in SATELLITE_SYMBOLS:
            satellite_pnl_cny += float(row["pnl_cny"])
            satellite_pnl_usd += float(row["pnl"])
            satellite_pnl_rank.append(
                {
                    "symbol": sym,
                    "label": row["label"],
                    "pnl": row["pnl"],
                    "pnl_cny": row["pnl_cny"],
                    "currency": row["currency"],
                }
            )
        elif sym in ("VOO", "QQQ", "SGOV", "001015"):
            pnl_cny = float(row["pnl_cny"])
            pnl_usd = float(row["pnl"]) if row["currency"] == "USD" else (pnl_cny / fx if fx > 0 else 0.0)
            pnl_rank.append(
                {
                    "symbol": sym,
                    "label": row["label"],
                    "pnl_usd": pnl_usd,
                    "pnl_cny": pnl_cny,
                    "currency": "CNY",
                }
            )
    for sym, archived in load_closed_satellite_pnl().items():
        if sym in SATELLITE_SYMBOLS:
            continue
        pnl_usd = float(archived.get("pnl_usd", 0.0) or 0.0)
        satellite_pnl_rank.append(
            {
                "symbol": sym,
                "label": f"{archived.get('label') or sym}*",
                "pnl": pnl_usd,
                "pnl_cny": pnl_usd * fx,
                "currency": "USD",
                "archived": True,
            }
        )
    pnl_rank.append(
        {
            "symbol": "SATELLITE",
            "label": "卫星仓位",
            "pnl_usd": satellite_pnl_usd,
            "pnl_cny": satellite_pnl_cny,
            "currency": "CNY",
        }
    )
    pnl_rank.sort(key=lambda item: item["pnl_usd"], reverse=True)
    satellite_pnl_rank.sort(key=lambda item: item["pnl"], reverse=True)

    allocation_order = [
        ("VOO", "VOO", ["VOO"]),
        ("QQQ", "QQQ", ["QQQ"]),
        ("SATELLITE", "卫星仓位", list(SATELLITE_SYMBOLS)),
        ("SGOV", "短债(SGOV)", ["SGOV"]),
        ("CASH", "现金", []),
    ]
    target_map = {
        "VOO": target_weights["VOO"],
        "QQQ": target_weights["QQQ"],
        "SATELLITE": sum(target_weights[sym] for sym in SATELLITE_SYMBOLS),
        "SGOV": target_weights["SGOV"],
        "CASH": 0.0,
    }
    allocation_target_total = sum(target_map.values())
    allocation_compare = []
    for key, label, symbols in allocation_order:
        amount_cny = usd_cash_cny if key == "CASH" else sum(value_cny_by_symbol.get(sym, 0.0) for sym in symbols)
        normalized_target = target_map[key] / allocation_target_total if allocation_target_total > 0 else 0.0
        target_cny = allocation_total_cny * normalized_target if allocation_total_cny > 0 else 0.0
        allocation_compare.append(
            {
                "key": key,
                "label": label,
                "current_pct": amount_cny / allocation_total_cny * 100.0 if allocation_total_cny > 0 else 0.0,
                "target_pct": normalized_target * 100.0,
                "current_usd": amount_cny / fx if fx > 0 else 0.0,
                "target_usd": target_cny / fx if fx > 0 else 0.0,
            }
        )

    satellite_weight_total = sum(target_weights[sym] for sym in SATELLITE_SYMBOLS)
    satellite_split = []
    for sym in SATELLITE_SYMBOLS:
        amount_cny = value_cny_by_symbol.get(sym, 0.0)
        row = row_by_symbol.get(sym, {})
        target_pct = target_weights[sym] / satellite_weight_total * 100.0 if satellite_weight_total > 0 else 0.0
        satellite_split.append(
            {
                "symbol": sym,
                "label": row.get("label", sym),
                "current_pct": amount_cny / satellite_total_cny * 100.0 if satellite_total_cny > 0 else 0.0,
                "target_pct": target_pct,
                "current_usd": amount_cny / fx if fx > 0 else 0.0,
                "target_usd": (satellite_total_cny * target_pct / 100.0) / fx if fx > 0 else 0.0,
            }
        )

    return {
        "pnl_rank": pnl_rank,
        "satellite_pnl_rank": satellite_pnl_rank,
        "allocation_compare": allocation_compare,
        "satellite_split": satellite_split,
    }


def date_range(start: str, end: str) -> list[str]:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    days: list[str] = []
    current = start_date
    while current <= end_date:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def previous_day(day: str) -> str:
    return (date.fromisoformat(day) - timedelta(days=1)).isoformat()


def is_weekday(day: str) -> bool:
    return date.fromisoformat(day).weekday() < 5


def completed_performance_day(now: datetime | None = None) -> str:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    ny_now = current.astimezone(NY_TZ)
    ny_day = ny_now.date()
    ny_minutes = ny_now.hour * 60 + ny_now.minute
    if ny_now.weekday() < 5 and ny_minutes >= US_MARKET_CLOSE_MINUTE:
        return ny_day.isoformat()
    previous = ny_day - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous.isoformat()


def previous_market_open_day(day: str, histories: dict[str, dict[str, float]]) -> str:
    previous = previous_day(day)
    candidates = [
        close_day
        for prices in histories.values()
        for close_day in prices
        if close_day <= previous
    ]
    if candidates:
        return max(candidates)
    current = date.fromisoformat(day) - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.isoformat()


def close_on_or_before(prices: dict[str, float], day: str) -> float | None:
    candidates = [key for key in prices if key <= day]
    if not candidates:
        return None
    return prices[max(candidates)]


def close_on(prices: dict[str, float], day: str) -> float | None:
    return prices.get(day)


def is_completed_trading_day(day: str, histories: dict[str, dict[str, float]], symbols: set[str]) -> bool:
    return any(close_on(histories.get(sym, {}), day) is not None for sym in symbols)


def market_union_open_symbols(day: str, histories: dict[str, dict[str, float]]) -> list[str]:
    return [sym for sym in ("001015", "VOO", "QQQ") if close_on(histories.get(sym, {}), day) is not None]


def fetch_us_close_history(symbol: str) -> dict[str, float]:
    try:
        from .ohlcv import fetch_ohlcv

        payload = fetch_ohlcv(symbol, "1d")
    except Exception:
        return {}
    out: dict[str, float] = {}
    for bar in payload.get("bars") or []:
        day = str(bar.get("time") or "")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
            continue
        try:
            close = float(bar.get("close") or 0.0)
        except (TypeError, ValueError):
            continue
        if close > 0:
            out[day] = close
    return out


def fetch_fund_close_history(symbol: str) -> dict[str, float]:
    code = FUND_CODES.get(symbol, symbol)
    prices: dict[str, float] = {}
    for page in range(1, 5):
        url = f"http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={code}&page={page}&per=100"
        try:
            response = requests.get(url, headers=FUND_HISTORY_HEADERS, timeout=(5, 20))
            response.encoding = "utf-8"
        except requests.RequestException:
            break
        rows = re.findall(
            r"<tr>\s*<td>(\d{4}-\d{2}-\d{2})</td>\s*<td[^>]*>([0-9.]+)</td>",
            response.text,
            flags=re.I,
        )
        if not rows:
            break
        for day, nav in rows:
            try:
                price = float(nav)
            except (TypeError, ValueError):
                continue
            if price > 0:
                prices[day] = price
    # The legacy F10DataApi endpoint may return only `var apidata=`. The
    # fund page's own net-worth series contains the same official daily NAVs
    # and also supplements a legacy response that has not published its newest
    # rows yet.
    try:
        response = requests.get(
            f"https://fund.eastmoney.com/pingzhongdata/{code}.js",
            headers=FUND_HISTORY_HEADERS,
            timeout=(5, 20),
        )
        response.encoding = "utf-8"
        match = re.search(r"var\s+Data_netWorthTrend\s*=\s*(\[.*?\]);", response.text, flags=re.S)
        trend = json.loads(match.group(1)) if match else []
    except (requests.RequestException, json.JSONDecodeError, TypeError, ValueError):
        trend = []
    for item in trend:
        try:
            day = datetime.fromtimestamp(float(item["x"]) / 1000.0, TZ_SHANGHAI).date().isoformat()
            price = float(item["y"])
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if price > 0:
            prices[day] = price
    return prices


def fetch_close_histories(symbols: set[str]) -> dict[str, dict[str, float]]:
    histories: dict[str, dict[str, float]] = {}
    for sym in sorted(symbols):
        histories[sym] = fetch_us_close_history(sym) if sym in USD_SYMBOLS else fetch_fund_close_history(sym)
    return histories


def completed_daily_pct_for_symbol(symbol: str, day: str, histories: dict[str, dict[str, float]]) -> float | None:
    prices = histories.get(symbol) or {}
    today_price = close_on(prices, day)
    prev_price = close_on_or_before(prices, previous_day(day))
    if not today_price or not prev_price or prev_price <= 0:
        return None
    return (today_price / prev_price - 1.0) * 100.0


def completed_portfolio_daily_pct(
    holdings_snapshot: dict[str, dict[str, float]],
    day: str,
    histories: dict[str, dict[str, float]],
    fx: float,
    day_trades: list[dict[str, Any]] | None = None,
    symbols: set[str] | None = None,
    native_usd: bool = False,
) -> tuple[float, dict[str, float], float, float]:
    symbol_daily_pct: dict[str, float] = {}
    symbol_basis: dict[str, float] = {}
    symbol_pnl: dict[str, float] = {}
    prev_day = previous_day(day)

    trades_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for trade in day_trades or []:
        if str(trade.get("trade_date") or "")[:10] != day:
            continue
        sym = str(trade.get("symbol", "")).upper()
        if not sym:
            continue
        trades_by_symbol.setdefault(sym, []).append(trade)

    for sym, holding in holdings_snapshot.items():
        if symbols is not None and sym not in symbols:
            continue
        close_price = close_on(histories.get(sym, {}), day)
        prev_price = close_on_or_before(histories.get(sym, {}), prev_day)
        if prev_price is None:
            prev_price = float(holding.get("avg_cost", 0.0) or 0.0)
        if not close_price or prev_price <= 0:
            continue

        close_shares = max(0.0, float(holding.get("shares", 0.0) or 0.0))
        old_shares = close_shares
        buy_lots: list[tuple[float, float]] = []
        sell_lots: list[tuple[float, float]] = []
        for trade in trades_by_symbol.get(sym, []):
            shares = max(0.0, float(trade.get("shares", 0.0) or 0.0))
            amount = max(0.0, float(trade.get("amount_usd", 0.0) or 0.0))
            if shares <= 0 or amount <= 0:
                continue
            price = amount / shares
            if str(trade.get("action", "buy")).lower() == "sell":
                sell_lots.append((shares, price))
                old_shares += shares
            else:
                buy_lots.append((shares, price))
                old_shares -= shares
        old_shares = max(0.0, old_shares)

        multiplier = 1.0 if native_usd else (fx if sym in USD_SYMBOLS else 1.0)
        remaining_old_shares = old_shares
        basis = 0.0
        pnl = 0.0

        for shares, price in sell_lots:
            sold_old_shares = min(remaining_old_shares, shares)
            basis += sold_old_shares * prev_price * multiplier
            pnl += sold_old_shares * (price - prev_price) * multiplier
            remaining_old_shares -= sold_old_shares

        basis += remaining_old_shares * prev_price * multiplier
        pnl += remaining_old_shares * (close_price - prev_price) * multiplier

        for shares, price in buy_lots:
            basis += shares * price * multiplier
            pnl += shares * (close_price - price) * multiplier

        if basis <= 0:
            continue
        symbol_basis[sym] = basis
        symbol_pnl[sym] = pnl

    total_basis = sum(symbol_basis.values())
    if total_basis <= 0:
        return 0.0, symbol_daily_pct, 0.0, 0.0
    total_pnl = 0.0
    for sym, basis in symbol_basis.items():
        pnl = symbol_pnl.get(sym, 0.0)
        daily_pct = pnl / basis * 100.0
        symbol_daily_pct[sym] = daily_pct
        total_pnl += pnl
    return total_pnl / total_basis * 100.0, symbol_daily_pct, total_pnl, total_basis


def holding_pnl_pct_for_snapshot(
    holdings_snapshot: dict[str, dict[str, float]],
    prices: dict[str, float],
    fx: float,
    symbols: set[str] | None = None,
    native_usd: bool = False,
) -> dict[str, float]:
    total_value = 0.0
    total_cost = 0.0
    for sym, holding in holdings_snapshot.items():
        if symbols is not None and sym not in symbols:
            continue
        price = float(prices.get(sym) or 0.0)
        shares = max(0.0, float(holding.get("shares", 0.0) or 0.0))
        avg_cost = max(0.0, float(holding.get("avg_cost", 0.0) or 0.0))
        if price <= 0 or shares <= 0 or avg_cost <= 0:
            continue
        multiplier = 1.0 if native_usd else (fx if sym in USD_SYMBOLS else 1.0)
        total_value += shares * price * multiplier
        total_cost += shares * avg_cost * multiplier
    total_pnl = total_value - total_cost
    return {
        "pct": total_pnl / total_cost * 100.0 if total_cost > 0 else 0.0,
        "amount_cny": total_pnl,
        "cost_cny": total_cost,
        "value_cny": total_value,
    }


def cash_balances_for_history_day(
    history_day: str,
    balances: dict[str, float],
    trades: list[dict[str, Any]],
) -> tuple[float, float]:
    """Rewind current cash by trades that happened after a historical snapshot."""
    cash_usd = float(balances.get("cash_usd", 0.0) or 0.0)
    cash_cny = float(balances.get("cash_cny", 0.0) or 0.0)
    for trade in trades:
        trade_day = str(trade.get("trade_date") or "")[:10]
        if not trade_day or trade_day <= history_day:
            continue
        symbol = str(trade.get("symbol") or "").upper()
        amount = max(0.0, float(trade.get("amount_usd", 0.0) or 0.0))
        direction = 1.0 if str(trade.get("action") or "").lower() == "buy" else -1.0
        if symbol in USD_SYMBOLS:
            cash_usd += direction * amount
        else:
            cash_cny += direction * amount
    return max(0.0, cash_usd), max(0.0, cash_cny)


def total_pnl_for_history_snapshot(
    row: dict[str, Any],
    balances: dict[str, float],
    trades: list[dict[str, Any]],
    usd_cost_fx: float,
    fallback_fx: float,
) -> dict[str, float]:
    """Add FX P&L and cash to a historical holding-only P&L snapshot."""
    snapshot = row.get("holdings_snapshot") or {}
    usd_holding_cost = sum(
        max(0.0, float(holding.get("shares", 0.0) or 0.0))
        * max(0.0, float(holding.get("avg_cost", 0.0) or 0.0))
        for symbol, holding in snapshot.items()
        if symbol in USD_SYMBOLS
    )
    cny_holding_cost = sum(
        max(0.0, float(holding.get("shares", 0.0) or 0.0))
        * max(0.0, float(holding.get("avg_cost", 0.0) or 0.0))
        for symbol, holding in snapshot.items()
        if symbol not in USD_SYMBOLS
    )
    fx_rate = coerce_optional_float(row.get("fx_rate"))
    holding_cost_at_market_fx = coerce_optional_float(row.get("holding_cost_cny"))
    if fx_rate is None and usd_holding_cost > 0 and holding_cost_at_market_fx is not None:
        fx_rate = (holding_cost_at_market_fx - cny_holding_cost) / usd_holding_cost
    # Older rows can contain a holding cost captured before same-day trades while
    # their snapshot is post-trade. Reject the resulting impossible inferred FX.
    if fx_rate is None or fx_rate <= 0 or abs(fx_rate - fallback_fx) > 0.5:
        fx_rate = fallback_fx

    history_day = str(row.get("date") or "")[:10]
    cash_usd, cash_cny = cash_balances_for_history_day(history_day, balances, trades)
    price_pnl_cny = float(row.get("holding_pnl_cny", 0.0) or 0.0)
    fx_pnl_cny = (usd_holding_cost + cash_usd) * (fx_rate - usd_cost_fx)
    total_pnl_cny = price_pnl_cny + fx_pnl_cny
    total_return_basis_cny = cny_holding_cost + (usd_holding_cost + cash_usd) * usd_cost_fx + cash_cny
    return {
        "total_pnl_cny": total_pnl_cny,
        "total_return_basis_cny": total_return_basis_cny,
        "total_pnl_pct": total_pnl_cny / total_return_basis_cny * 100.0 if total_return_basis_cny > 0 else 0.0,
        "fx_pnl_cny": fx_pnl_cny,
        "fx_rate": fx_rate,
        "cash_usd": cash_usd,
        "cash_cny": cash_cny,
    }


def annotate_trade_close_effects(
    trades: list[dict[str, Any]],
    quotes: dict[str, Any],
    fx: float,
    history_day: str,
) -> list[dict[str, Any]]:
    symbols = {str(trade.get("symbol", "")).upper() for trade in trades if trade.get("symbol")}
    histories = fetch_close_histories(symbols)
    annotated: list[dict[str, Any]] = []
    for trade in trades:
        item = dict(trade)
        sym = str(item.get("symbol", "")).upper()
        trade_day = str(item.get("trade_date") or item.get("date") or "")[:10]
        close_price = close_on(histories.get(sym, {}), trade_day)
        if close_price is None and trade_day == history_day:
            quote = quotes.get(sym) or {}
            try:
                close_price = float(quote.get("regular_price") or quote.get("price") or 0.0)
            except (TypeError, ValueError):
                close_price = None
        try:
            shares = max(0.0, float(item.get("shares", 0.0) or 0.0))
            trade_price = float(item.get("price") or 0.0)
        except (TypeError, ValueError):
            shares = 0.0
            trade_price = 0.0
        if close_price and close_price > 0 and shares > 0 and trade_price > 0:
            if str(item.get("action", "buy")).lower() == "sell":
                effect_native = shares * (trade_price - close_price)
            else:
                effect_native = shares * (close_price - trade_price)
            item["close_price"] = close_price
            item["close_effect"] = effect_native
            item["close_effect_cny"] = effect_native * fx if sym in USD_SYMBOLS else effect_native
        annotated.append(item)
    return annotated


def current_portfolio_daily_pct(
    user_id: str,
    holdings: dict[str, dict[str, float]],
    quotes: dict[str, Any],
    fx: float,
    day: str,
    now: datetime,
) -> tuple[float, dict[str, float], float, float]:
    trades = load_trade_records(user_id)
    finalized_rows = [row for row in load_portfolio_history(user_id) if row.get("finalized")]
    holdings_snapshot = holdings_snapshot_for_day(day, holdings, finalized_rows, trades)
    trades_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        if str(trade.get("trade_date") or "")[:10] != day:
            continue
        sym = str(trade.get("symbol", "")).upper()
        if sym:
            trades_by_symbol.setdefault(sym, []).append(trade)

    symbol_daily_pct: dict[str, float] = {}
    total_basis = 0.0
    total_pnl = 0.0
    for sym, holding in holdings_snapshot.items():
        quote = quotes.get(sym) or {}
        try:
            close_price = float(quote.get("price") or 0.0)
            prev_price = float(quote.get("prev_close") or 0.0)
        except (TypeError, ValueError):
            continue
        if close_price <= 0 or prev_price <= 0:
            continue

        close_shares = max(0.0, float(holding.get("shares", 0.0) or 0.0))
        old_shares = close_shares
        buy_lots: list[tuple[float, float]] = []
        sell_lots: list[tuple[float, float]] = []
        for trade in trades_by_symbol.get(sym, []):
            shares = max(0.0, float(trade.get("shares", 0.0) or 0.0))
            amount = max(0.0, float(trade.get("amount_usd", 0.0) or 0.0))
            if shares <= 0 or amount <= 0:
                continue
            trade_price = amount / shares
            if str(trade.get("action", "buy")).lower() == "sell":
                sell_lots.append((shares, trade_price))
                old_shares += shares
            else:
                buy_lots.append((shares, trade_price))
                old_shares -= shares
        old_shares = max(0.0, old_shares)

        multiplier = fx if sym in USD_SYMBOLS else 1.0
        remaining_old_shares = old_shares
        basis = 0.0
        pnl = 0.0
        for shares, trade_price in sell_lots:
            sold_old_shares = min(remaining_old_shares, shares)
            basis += sold_old_shares * prev_price * multiplier
            pnl += sold_old_shares * (trade_price - prev_price) * multiplier
            remaining_old_shares -= sold_old_shares
        basis += remaining_old_shares * prev_price * multiplier
        pnl += remaining_old_shares * (close_price - prev_price) * multiplier
        for shares, trade_price in buy_lots:
            basis += shares * trade_price * multiplier
            pnl += shares * (close_price - trade_price) * multiplier
        if basis <= 0:
            continue
        symbol_daily_pct[sym] = pnl / basis * 100.0
        total_basis += basis
        total_pnl += pnl

    if total_basis <= 0:
        return 0.0, symbol_daily_pct, 0.0, 0.0
    return total_pnl / total_basis * 100.0, symbol_daily_pct, total_pnl, total_basis


def apply_trade_to_holdings(
    holdings: dict[str, dict[str, float]],
    trade: dict[str, Any],
    *,
    allow_oversell: bool = False,
) -> tuple[dict[str, dict[str, float]], float]:
    sym = str(trade.get("symbol", "")).upper()
    if sym not in holdings:
        return holdings, 0.0
    action = str(trade.get("action", "buy")).lower()
    shares = max(0.0, float(trade.get("shares", 0.0) or 0.0))
    amount = max(0.0, float(trade.get("amount_usd", 0.0) or 0.0))
    if shares <= 0 or amount <= 0:
        return holdings, 0.0
    old = holdings[sym]
    old_shares = float(old.get("shares", 0.0) or 0.0)
    old_cost = float(old.get("avg_cost", 0.0) or 0.0)
    if action == "sell":
        if shares > old_shares + 1e-9 and not allow_oversell:
            raise ValueError(f"{sym} 卖出股数 {shares:g} 超过当前持仓 {old_shares:g}")
        sell_shares = min(shares, old_shares)
        new_shares = max(0.0, old_shares - sell_shares)
        cost_basis = max(0.0, float(trade.get("cost_basis", 0.0) or 0.0))
        if cost_basis <= 0:
            cost_basis = sell_shares * old_cost
        realized = amount - cost_basis
        remaining_cost = max(0.0, old_shares * old_cost - cost_basis)
        holdings[sym] = {"shares": new_shares, "avg_cost": remaining_cost / new_shares if new_shares > 1e-9 else 0.0}
        return holdings, realized
    price = amount / shares
    new_shares = old_shares + shares
    holdings[sym] = {"shares": new_shares, "avg_cost": (old_shares * old_cost + shares * price) / new_shares}
    return holdings, 0.0


def rewind_trade_from_holdings(
    holdings: dict[str, dict[str, float]],
    trade: dict[str, Any],
) -> dict[str, dict[str, float]]:
    """Undo one recorded trade from an end-of-period holdings snapshot."""
    sym = str(trade.get("symbol", "")).upper()
    action = str(trade.get("action", "buy")).lower()
    shares = max(0.0, float(trade.get("shares", 0.0) or 0.0))
    amount = max(0.0, float(trade.get("amount_usd", 0.0) or 0.0))
    if not sym or shares <= 0 or amount <= 0:
        return holdings

    current = holdings.get(sym, {"shares": 0.0, "avg_cost": 0.0})
    current_shares = max(0.0, float(current.get("shares", 0.0) or 0.0))
    current_avg_cost = max(0.0, float(current.get("avg_cost", 0.0) or 0.0))
    prev_avg_cost = max(0.0, float(trade.get("prev_avg_cost", 0.0) or 0.0))

    if action == "sell":
        previous_shares = current_shares + shares
        cost_basis = max(0.0, float(trade.get("cost_basis", 0.0) or 0.0))
        if prev_avg_cost <= 0 and previous_shares > 0:
            prev_avg_cost = (current_shares * current_avg_cost + cost_basis) / previous_shares
    else:
        previous_shares = max(0.0, current_shares - shares)
        if prev_avg_cost <= 0 and previous_shares > 0:
            prev_avg_cost = max(0.0, (current_shares * current_avg_cost - amount) / previous_shares)

    holdings[sym] = {
        "shares": previous_shares,
        "avg_cost": prev_avg_cost if previous_shares > 1e-9 else 0.0,
    }
    return holdings


def holdings_snapshot_for_day(
    day: str,
    current_holdings: dict[str, dict[str, float]],
    finalized_rows: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    previous_rows = [row for row in finalized_rows if row.get("finalized") and str(row.get("date", "")) < day and row.get("holdings_snapshot")]
    if previous_rows:
        base = previous_rows[-1]["holdings_snapshot"]
    else:
        base = current_holdings
    snapshot = {sym: {"shares": float(item.get("shares", 0.0) or 0.0), "avg_cost": float(item.get("avg_cost", 0.0) or 0.0)} for sym, item in base.items()}

    # current_holdings already contains every recorded trade up to today.  When
    # there is no earlier finalized snapshot, replaying trades up to ``day``
    # would count them twice.  Instead, rewind only trades after the requested
    # day so the first generated snapshot has the correct end-of-day holdings.
    if not previous_rows:
        future_trades = [
            trade
            for trade in trades
            if str(trade.get("trade_date") or "")[:10] > day
        ]
        future_trades.sort(
            key=lambda trade: (
                str(trade.get("trade_date") or "")[:10],
                str(trade.get("created_at") or ""),
                str(trade.get("id") or ""),
            ),
            reverse=True,
        )
        for trade in future_trades:
            snapshot = rewind_trade_from_holdings(snapshot, trade)
        return snapshot

    start_date = previous_rows[-1]["date"] if previous_rows else ""
    for trade in trades:
        trade_date = str(trade.get("trade_date") or "")[:10]
        if (not start_date or trade_date > start_date) and trade_date <= day:
            snapshot, _ = apply_trade_to_holdings(snapshot, trade, allow_oversell=True)
    return snapshot


def invalidate_performance_history_from(user_id: str, start_day: str) -> None:
    if not start_day:
        return
    rows = [row for row in load_portfolio_history(user_id) if str(row.get("date", "")) < start_day]
    save_portfolio_history(user_id, rows)


def ensure_completed_performance_history(
    user_id: str,
    holdings: dict[str, dict[str, float]],
    fx: float,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    loaded_rows = load_portfolio_history(user_id)
    rows = [row for row in loaded_rows if row.get("finalized")]
    if len(rows) != len(loaded_rows):
        save_portfolio_history(user_id, rows)
    completed_day = completed_performance_day(now)
    finalized_dates = {row["date"] for row in rows if row.get("finalized")}
    latest_finalized_date = max(finalized_dates, default="")
    start_day = PERFORMANCE_HISTORY_START_DATE if not latest_finalized_date else (date.fromisoformat(latest_finalized_date) + timedelta(days=1)).isoformat()

    rows_by_date = {row["date"]: row for row in rows}
    trades = load_trade_records(user_id)
    _, balances, _ = load_user_state(user_id)
    usd_cost_fx = float(fx_conversion_summary(load_fx_conversion_records(user_id), fx)["avg_rate"] or fx)
    symbols = set(holdings) | {str(trade.get("symbol", "")).upper() for trade in trades} | {"001015", "VOO", "QQQ"}
    histories = fetch_close_histories(symbols)
    repaired_market_history = False
    for row in rows:
        day = str(row.get("date") or "")[:10]
        if not day:
            continue
        available_symbols = market_union_open_symbols(day, histories)
        recorded_symbols = set(row.get("market_open_symbols") or [])
        benchmark_prices = dict(row.get("benchmark_prices") or {})
        benchmark_daily_pct = dict(row.get("benchmark_daily_pct") or {})
        needs_repair = any(
            sym not in benchmark_prices
            or sym not in benchmark_daily_pct
            or (sym not in recorded_symbols and coerce_optional_float(benchmark_daily_pct.get(sym)) == 0.0)
            for sym in available_symbols
        )
        if not needs_repair:
            continue

        for sym in available_symbols:
            price = close_on(histories.get(sym, {}), day)
            daily_pct = completed_daily_pct_for_symbol(sym, day, histories)
            if price is not None:
                benchmark_prices[sym] = price
            if daily_pct is not None:
                benchmark_daily_pct[sym] = daily_pct
        row["benchmark_prices"] = benchmark_prices
        row["benchmark_daily_pct"] = benchmark_daily_pct
        row["market_open_symbols"] = sorted(recorded_symbols | set(available_symbols))
        repaired_market_history = True
    if repaired_market_history:
        save_portfolio_history(user_id, sorted(rows, key=lambda row: row["date"]))

    backfilled = False
    for row in rows:
        if row.get("usd_return_pct") is not None or not row.get("holdings_snapshot"):
            continue
        day = str(row.get("date") or "")[:10]
        if not day:
            continue
        snapshot = row.get("holdings_snapshot") or {}
        holding_prices = {
            sym: price
            for sym in snapshot
            if (price := close_on(histories.get(sym, {}), day) or close_on_or_before(histories.get(sym, {}), day)) is not None
        }
        usd_holding_pnl = holding_pnl_pct_for_snapshot(snapshot, holding_prices, fx, set(USD_SYMBOLS), True)
        usd_daily_pct, _, holding_daily_pnl_usd, holding_daily_basis_usd = completed_portfolio_daily_pct(
            snapshot,
            day,
            histories,
            fx,
            trades,
            set(USD_SYMBOLS),
            True,
        )
        row["usd_return_pct"] = usd_holding_pnl["pct"]
        row["usd_pnl_usd"] = usd_holding_pnl["amount_cny"]
        row["usd_cost_usd"] = usd_holding_pnl["cost_cny"]
        row["usd_value_usd"] = usd_holding_pnl["value_cny"]
        row["usd_daily_pct"] = usd_daily_pct
        row["usd_daily_pnl_usd"] = holding_daily_pnl_usd
        row["usd_daily_basis_usd"] = holding_daily_basis_usd
        backfilled = True
    if backfilled:
        save_portfolio_history(user_id, sorted(rows, key=lambda row: row["date"]))
    missing_days = [
        day
        for day in date_range(start_day, completed_day)
        if day not in finalized_dates and market_union_open_symbols(day, histories)
    ]
    history_backfilled = False
    for row in rows:
        if row.get("total_pnl_cny") is not None and row.get("total_return_basis_cny") is not None:
            continue
        row.update(total_pnl_for_history_snapshot(row, balances, trades, usd_cost_fx, fx))
        history_backfilled = True
    if history_backfilled:
        save_portfolio_history(user_id, sorted(rows, key=lambda row: row["date"]))
    if not missing_days:
        return rows
    for day in missing_days:
        holdings_snapshot = holdings_snapshot_for_day(day, holdings, sorted(rows_by_date.values(), key=lambda item: item["date"]), trades)
        portfolio_daily_pct, symbol_daily_pct, holding_daily_pnl_cny, holding_daily_basis_cny = completed_portfolio_daily_pct(holdings_snapshot, day, histories, fx, trades)
        usd_daily_pct, _, holding_daily_pnl_usd, holding_daily_basis_usd = completed_portfolio_daily_pct(
            holdings_snapshot,
            day,
            histories,
            fx,
            trades,
            set(USD_SYMBOLS),
            True,
        )
        benchmark_daily_pct = {
            sym: daily_pct
            for sym in ("001015", "VOO", "QQQ")
            for daily_pct in [completed_daily_pct_for_symbol(sym, day, histories)]
            if daily_pct is not None
        }
        benchmark_prices = {
            sym: price
            for sym in ("001015", "VOO", "QQQ")
            if (price := close_on(histories.get(sym, {}), day) or close_on_or_before(histories.get(sym, {}), day)) is not None
        }
        holding_prices = {
            sym: price
            for sym in holdings_snapshot
            if (price := close_on(histories.get(sym, {}), day) or close_on_or_before(histories.get(sym, {}), day)) is not None
        }
        holding_pnl = holding_pnl_pct_for_snapshot(holdings_snapshot, holding_prices, fx)
        usd_holding_pnl = holding_pnl_pct_for_snapshot(holdings_snapshot, holding_prices, fx, set(USD_SYMBOLS), True)
        day_cash_flow_cny = sum(
            max(0.0, float(trade.get("amount_usd", 0.0) or 0.0)) * (fx if str(trade.get("symbol", "")).upper() in USD_SYMBOLS else 1.0)
            for trade in trades
            if str(trade.get("trade_date") or "")[:10] == day
        )
        rows_by_date[day] = {
            "date": day,
            "portfolio_daily_pct": portfolio_daily_pct,
            "portfolio_return_pct": 0.0,
            "holding_pnl_pct": holding_pnl["pct"],
            "holding_pnl_cny": holding_pnl["amount_cny"],
            "holding_cost_cny": holding_pnl["cost_cny"],
            "fx_rate": fx,
            "holding_daily_pnl_pct": portfolio_daily_pct,
            "holding_daily_pnl_cny": holding_daily_pnl_cny,
            "holding_daily_basis_cny": holding_daily_basis_cny,
            "usd_return_pct": usd_holding_pnl["pct"],
            "usd_pnl_usd": usd_holding_pnl["amount_cny"],
            "usd_cost_usd": usd_holding_pnl["cost_cny"],
            "usd_value_usd": usd_holding_pnl["value_cny"],
            "usd_daily_pct": usd_daily_pct,
            "usd_daily_pnl_usd": holding_daily_pnl_usd,
            "usd_daily_basis_usd": holding_daily_basis_usd,
            "cash_flow_cny": day_cash_flow_cny,
            "cash_flow_flag": day_cash_flow_cny > 0,
            "total_assets_cny": 0.0,
            "total_cost_cny": 0.0,
            "benchmark_prices": benchmark_prices,
            "benchmark_daily_pct": benchmark_daily_pct,
            "market_open_symbols": market_union_open_symbols(day, histories),
            "symbol_daily_pct": symbol_daily_pct,
            "holdings_snapshot": holdings_snapshot,
            "estimated_symbols": [],
            "finalized": True,
            "updated_at": datetime.combine(date.fromisoformat(day), datetime.min.time(), TZ_SHANGHAI).replace(hour=PERFORMANCE_WRITE_HOUR).isoformat(timespec="seconds"),
        }
        rows_by_date[day].update(total_pnl_for_history_snapshot(rows_by_date[day], balances, trades, usd_cost_fx, fx))
    rows = sorted(rows_by_date.values(), key=lambda row: row["date"])
    save_portfolio_history(user_id, rows)
    return rows


def start_performance_history_scheduler(user_id: str = "evan") -> None:
    last_checked_day = ""
    while True:
        now = datetime.now(TZ_SHANGHAI)
        check_key = now.date().isoformat()
        if now.hour >= PERFORMANCE_WRITE_HOUR and check_key != last_checked_day:
            try:
                holdings, _, _ = load_user_state(user_id)
                market = fetch_quotes()
                fx = float((market.get("fx") or {}).get("rate") or 7.2)
                ensure_completed_performance_history(user_id, holdings, fx, now)
                last_checked_day = check_key
            except Exception:
                pass
        time.sleep(60)


def build_performance_history(
    user_id: str,
    quotes: dict[str, Any],
    holdings: dict[str, dict[str, float]],
    fx: float,
    total_assets_cny: float,
    total_cost_cny: float,
    total_return_basis_cny: float,
    portfolio_return_pct: float,
    portfolio_daily_pct: float,
    holding_pnl_cny: float,
    total_pnl_cny: float,
    fx_pnl_cny: float,
    cash_usd: float,
    cash_cny: float,
    holding_daily_pnl_cny: float,
    holding_daily_basis_cny: float,
    usd_return_pct: float,
    usd_pnl_usd: float,
    usd_cost_usd: float,
    usd_daily_pct: float,
    usd_daily_pnl_usd: float,
    usd_daily_basis_usd: float,
    cash_flow_cny: float,
) -> dict[str, Any]:
    now = datetime.now(TZ_SHANGHAI)
    today = performance_history_date(now)
    benchmark_symbols = ("001015", "VOO", "QQQ")
    required_history_symbols = list(ALL_SYMBOLS)
    history_quotes_usable = all(is_history_quote_usable(quotes.get(sym)) for sym in required_history_symbols)
    benchmark_prices = {}
    benchmark_daily_pct: dict[str, float] = {}
    estimated_symbols: list[str] = []
    # CSI300 has its own completion clock. Once Beijing reaches 15:00, use
    # the completed daily bar directly, even though the US benchmark day is
    # still open and the portfolio history row remains provisional.
    benchmark_histories = fetch_close_histories(benchmark_symbols)
    for sym in benchmark_symbols:
        if not is_history_quote_usable(quotes.get(sym)):
            continue
        if sym == "001015" and is_china_daily_close_ready(today, now):
            close_price = close_on(benchmark_histories.get(sym, {}), today)
            close_daily_pct = completed_daily_pct_for_symbol(sym, today, benchmark_histories)
            if close_price is not None and close_daily_pct is not None:
                benchmark_prices[sym] = close_price
                benchmark_daily_pct[sym] = close_daily_pct
                continue
        if is_symbol_daily_history_estimated(sym, today, now, quotes.get(sym)):
            estimated_symbols.append(sym)
        try:
            quote = quotes.get(sym) or {}
            price = float(quote.get("price") or 0.0)
        except (TypeError, ValueError):
            price = 0.0
        if price > 0:
            benchmark_prices[sym] = price
        try:
            benchmark_daily_pct[sym] = daily_pct_for_current_history_quote(sym, today, now, quote)
        except (TypeError, ValueError):
            pass

    rows = [
        row
        for row in ensure_completed_performance_history(user_id, holdings, fx, now)
        if row.get("finalized") and (row.get("market_open_symbols") or is_weekday(row["date"]))
    ]
    current = {
        "date": today,
        "portfolio_daily_pct": portfolio_daily_pct,
        "portfolio_return_pct": portfolio_return_pct,
        "holding_pnl_pct": portfolio_return_pct,
        "holding_pnl_cny": holding_pnl_cny,
        "holding_cost_cny": total_cost_cny,
        "total_pnl_cny": total_pnl_cny,
        "total_return_basis_cny": total_return_basis_cny,
        "fx_pnl_cny": fx_pnl_cny,
        "fx_rate": fx,
        "cash_usd": cash_usd,
        "cash_cny": cash_cny,
        "holding_daily_pnl_pct": portfolio_daily_pct,
        "holding_daily_pnl_cny": holding_daily_pnl_cny,
        "holding_daily_basis_cny": holding_daily_basis_cny,
        "usd_return_pct": usd_return_pct,
        "usd_pnl_usd": usd_pnl_usd,
        "usd_cost_usd": usd_cost_usd,
        "usd_daily_pct": usd_daily_pct,
        "usd_daily_pnl_usd": usd_daily_pnl_usd,
        "usd_daily_basis_usd": usd_daily_basis_usd,
        "cash_flow_cny": cash_flow_cny,
        "cash_flow_flag": cash_flow_cny > 0,
        "total_assets_cny": total_assets_cny,
        "total_cost_cny": total_cost_cny,
        "benchmark_prices": benchmark_prices,
        "benchmark_daily_pct": benchmark_daily_pct,
        "market_open_symbols": sorted(benchmark_daily_pct.keys()),
        "estimated_symbols": estimated_symbols,
        "holdings_snapshot": holdings,
        "finalized": False,
        "updated_at": now.isoformat(timespec="seconds"),
    }
    rows_by_date = {row["date"]: row for row in rows}
    # Keep today's asset snapshot on weekends too, so the latest total/USD
    # returns use the same current state as the summary cards. Closed
    # benchmarks carry a zero daily return and remain flat.
    rows_by_date[today] = current
    rows = sorted(
        (row for row in rows_by_date.values() if str(row.get("date", "")) >= PERFORMANCE_CHART_START_DATE),
        key=lambda row: row["date"],
    )
    points: list[dict[str, Any]] = []
    cumulative = {
        "001015": 1.0,
        "VOO": 1.0,
        "QQQ": 1.0,
    }
    if rows:
        baseline_row = next(
            (row for row in rows if str(row.get("date", "")) == PERFORMANCE_CHART_BASELINE_DATE),
            {},
        )
        baseline_total_pnl_cny = coerce_optional_float(baseline_row.get("total_pnl_cny"))
        baseline_total_basis_cny = coerce_optional_float(baseline_row.get("total_return_basis_cny"))
        baseline_total_return_pct = coerce_optional_float(baseline_row.get("portfolio_return_pct")) or 0.0
        if baseline_total_pnl_cny is not None and baseline_total_basis_cny is not None and baseline_total_basis_cny > 0:
            baseline_total_return_pct = baseline_total_pnl_cny / baseline_total_basis_cny * 100.0
        baseline_point = {
            "date": PERFORMANCE_CHART_BASELINE_DATE,
            # 总资产和美元资产保留基准日的实际记录；基准指数从该日归零。
            "portfolio_return_pct": baseline_total_return_pct,
            "portfolio_daily_pct": coerce_optional_float(baseline_row.get("holding_daily_pnl_pct")) or coerce_optional_float(baseline_row.get("portfolio_daily_pct")) or 0.0,
            "holding_pnl_cny": coerce_optional_float(baseline_row.get("holding_pnl_cny")),
            "holding_cost_cny": coerce_optional_float(baseline_row.get("holding_cost_cny")),
            "total_pnl_cny": baseline_total_pnl_cny,
            "total_return_basis_cny": baseline_total_basis_cny,
            "fx_pnl_cny": coerce_optional_float(baseline_row.get("fx_pnl_cny")),
            "usd_return_pct": coerce_optional_float(baseline_row.get("usd_return_pct")) or 0.0,
            "usd_daily_pct": coerce_optional_float(baseline_row.get("usd_daily_pct")) or 0.0,
            "usd_pnl_usd": coerce_optional_float(baseline_row.get("usd_pnl_usd")),
            "cash_flow_cny": coerce_optional_float(baseline_row.get("cash_flow_cny")) or 0.0,
            "cash_flow_flag": bool(baseline_row.get("cash_flow_flag")),
            "market_open_symbols": list(benchmark_symbols),
        }
        for sym in benchmark_symbols:
            baseline_point[f"{sym}_return_pct"] = 0.0
            baseline_point[f"{sym}_daily_pct"] = 0.0
        points.append(baseline_point)

    for row_index, row in enumerate(rows):
        if str(row.get("date", "")) <= PERFORMANCE_CHART_BASELINE_DATE:
            continue
        portfolio_daily = coerce_optional_float(row.get("portfolio_daily_pct"))
        holding_daily = coerce_optional_float(row.get("holding_daily_pnl_pct"))
        if holding_daily is not None:
            portfolio_daily = holding_daily
        if portfolio_daily is None:
            portfolio_daily = 0.0
        total_pnl_pct = coerce_optional_float(row.get("portfolio_return_pct"))
        total_pnl_cny = coerce_optional_float(row.get("total_pnl_cny"))
        total_return_basis_cny = coerce_optional_float(row.get("total_return_basis_cny"))
        if total_pnl_cny is not None and total_return_basis_cny is not None and total_return_basis_cny > 0:
            total_pnl_pct = total_pnl_cny / total_return_basis_cny * 100.0
        if total_pnl_pct is None:
            continue
        point = {
            "date": row["date"],
            "portfolio_return_pct": total_pnl_pct,
            "portfolio_daily_pct": portfolio_daily,
            "holding_pnl_cny": coerce_optional_float(row.get("holding_pnl_cny")),
            "holding_cost_cny": coerce_optional_float(row.get("holding_cost_cny")),
            "total_pnl_cny": total_pnl_cny,
            "total_return_basis_cny": total_return_basis_cny,
            "fx_pnl_cny": coerce_optional_float(row.get("fx_pnl_cny")),
            "holding_daily_pnl_cny": coerce_optional_float(row.get("holding_daily_pnl_cny")),
            "holding_daily_basis_cny": coerce_optional_float(row.get("holding_daily_basis_cny")),
            "usd_return_pct": coerce_optional_float(row.get("usd_return_pct")),
            "usd_daily_pct": coerce_optional_float(row.get("usd_daily_pct")),
            "usd_pnl_usd": coerce_optional_float(row.get("usd_pnl_usd")),
            "usd_cost_usd": coerce_optional_float(row.get("usd_cost_usd")),
            "usd_daily_pnl_usd": coerce_optional_float(row.get("usd_daily_pnl_usd")),
            "usd_daily_basis_usd": coerce_optional_float(row.get("usd_daily_basis_usd")),
            "cash_flow_cny": coerce_optional_float(row.get("cash_flow_cny")) or 0.0,
            "cash_flow_flag": bool(row.get("cash_flow_flag")),
            "market_open_symbols": row.get("market_open_symbols") or [],
            "symbol_daily_pct": row.get("symbol_daily_pct") or {},
            "holdings_snapshot": row.get("holdings_snapshot") or {},
        }
        daily_pcts = row.get("benchmark_daily_pct") or {}
        if isinstance(daily_pcts, dict):
            for sym in benchmark_symbols:
                daily_pct = coerce_optional_float(daily_pcts.get(sym))
                if daily_pct is None:
                    point[f"{sym}_return_pct"] = None
                    point[f"{sym}_daily_pct"] = None
                    continue
                cumulative[sym] *= 1.0 + daily_pct / 100.0
                point[f"{sym}_return_pct"] = (cumulative[sym] - 1.0) * 100.0
                point[f"{sym}_daily_pct"] = daily_pct
        points.append(point)

    return {
        "points": points,
        "started_on": points[0]["date"] if points else today,
        "updated_at": rows[-1].get("updated_at", "") if rows else "",
        "date_rule": "北京时间 06:00 切换投资日；凌晨美股交易归入前一投资日",
        "return_rule": "曲线按每日涨跌幅复利累计；当日未完整交易部分使用估值、盘前或夜盘作预计",
        "estimated_symbols": rows[-1].get("estimated_symbols", []) if rows else [],
        "benchmark_labels": {"001015": "沪深300", "VOO": "VOO", "QQQ": "QQQ"},
    }


def performance_history_date(now: datetime | None = None) -> str:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    if current.hour < 6:
        current = current - timedelta(days=1)
    return current.date().isoformat()


def is_symbol_daily_history_ready(symbol: str, investment_day: str, now: datetime | None = None) -> bool:
    if symbol in USD_SYMBOLS:
        return is_us_daily_history_ready(symbol, investment_day, now)
    return True


def is_symbol_daily_history_estimated(
    symbol: str,
    investment_day: str,
    now: datetime | None = None,
    quote: Any = None,
) -> bool:
    if symbol in USD_SYMBOLS:
        return not is_us_daily_history_ready(symbol, investment_day, now)
    if symbol == "001015":
        if is_china_daily_close_ready(investment_day, now):
            return False
        source = str((quote or {}).get("source") or "")
        return "估算" in source or "估值" in source
    return False


def is_us_daily_history_ready(_: str, investment_day: str, now: datetime | None = None) -> bool:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    ny_now = current.astimezone(NY_TZ)
    if ny_now.date().isoformat() != investment_day:
        return False
    if ny_now.weekday() >= 5:
        return False
    return ny_now.hour * 60 + ny_now.minute >= 9 * 60 + 30


def is_history_quote_usable(quote: Any) -> bool:
    if not isinstance(quote, dict):
        return False
    try:
        price = float(quote.get("price") or 0.0)
    except (TypeError, ValueError):
        return False
    if price <= 0:
        return False
    source = str(quote.get("source") or "").lower()
    return "fallback" not in source


def coerce_optional_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def fund_daily_pct_for_day(quote: dict[str, Any], investment_day: str) -> float:
    """Return a fund estimate only when the provider says it is for this day."""
    quote_day = str(quote.get("quote_date") or quote.get("quote_time") or "")[:10]
    if quote_day != investment_day:
        return 0.0
    return coerce_optional_float(quote.get("regular_change_pct", quote.get("change_pct"))) or 0.0


def history_daily_pct_for_symbol(symbol: str, quote: dict[str, Any], investment_day: str, now: datetime) -> float:
    # Quote providers commonly retain Friday's change over the weekend. Do not
    # count that old move again as the current investment day's return.
    if symbol not in USD_SYMBOLS and not is_weekday(investment_day):
        return 0.0
    if symbol == "001015":
        # 沪深300按北京时间交易日计算：09:00 后可显示当日估算，
        # 15:00 收盘后继续沿用当天最终报价，直到次日 09:00 切换。
        current = now.astimezone(TZ_SHANGHAI) if now.tzinfo else now.replace(tzinfo=TZ_SHANGHAI)
        if current.date().isoformat() != investment_day:
            return 0.0
        minutes = current.hour * 60 + current.minute
        if minutes < 9 * 60:
            return 0.0
        return fund_daily_pct_for_day(quote, investment_day)
    session = str(quote.get("session") or "").lower()
    # During an extended session, use the regular-session close as the base
    # price (that close is normalized to 1). The provider's extended return is
    # already measured against that base, so it represents the current USD P&L
    # directly; do not compound it with the regular-session return.
    if symbol in USD_SYMBOLS and session != "regular":
        extended_pct = coerce_optional_float(quote.get("extended_change_pct"))
        if extended_pct is not None and session != "closed":
            return extended_pct
        regular_pct = coerce_optional_float(quote.get("regular_change_pct"))
        if regular_pct is not None:
            return regular_pct
    try:
        return float(quote.get("change_pct", 0.0))
    except (TypeError, ValueError):
        return 0.0


def carried_completed_daily_pct(
    symbol: str,
    quote: dict[str, Any],
    completed_row: dict[str, Any] | None,
) -> float:
    """Return the last completed session move for closed-day display only."""
    row = completed_row or {}
    symbol_pcts = row.get("symbol_daily_pct") or {}
    benchmark_pcts = row.get("benchmark_daily_pct") or {}
    for values in (symbol_pcts, benchmark_pcts):
        if isinstance(values, dict):
            pct = coerce_optional_float(values.get(symbol))
            if pct is not None:
                return pct

    pct = coerce_optional_float(quote.get("regular_change_pct", quote.get("change_pct")))
    if pct is not None and abs(pct) > 1e-12:
        return pct
    try:
        price = float(quote.get("regular_price") or quote.get("price") or 0.0)
        prev_close = float(quote.get("prev_close") or 0.0)
    except (TypeError, ValueError):
        return pct or 0.0
    if price > 0 and prev_close > 0:
        return (price / prev_close - 1.0) * 100.0
    return pct or 0.0


def is_china_daily_close_ready(investment_day: str, now: datetime | None = None) -> bool:
    """Whether the A-share daily benchmark can be confirmed for this date."""
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    current = current.astimezone(TZ_SHANGHAI)
    if current.date().isoformat() != str(investment_day)[:10] or current.weekday() >= 5:
        return False
    return current.hour * 60 + current.minute >= 15 * 60


def daily_pct_for_current_history_quote(symbol: str, investment_day: str, now: datetime, quote: dict[str, Any]) -> float:
    if symbol in USD_SYMBOLS and str(quote.get("session") or "").lower() == "closed":
        return 0.0
    return history_daily_pct_for_symbol(symbol, quote, investment_day, now)


def build_dashboard(user_id: str = "evan") -> dict[str, Any]:
    market = fetch_quotes()
    target_weights = effective_target_weights()
    quotes = market["quotes"]
    fx = float(market["fx"]["rate"])
    holdings, balances, storage_mode = load_user_state(user_id)
    fx_conversions = load_fx_conversion_records(user_id)
    fx_conversion_stats = fx_conversion_summary(fx_conversions, fx)
    usd_cost_fx = float(fx_conversion_stats["avg_rate"] or fx)
    raw_holdings = holdings
    history_now = datetime.now(TZ_SHANGHAI)
    history_day = performance_history_date(history_now)
    finalized_rows = [row for row in load_portfolio_history(user_id) if row.get("finalized")]
    completed_day = completed_performance_day(history_now)
    latest_completed_row = max(
        (row for row in finalized_rows if str(row.get("date") or "") <= completed_day),
        key=lambda row: str(row.get("date") or ""),
        default=None,
    )
    carry_completed_daily = not is_weekday(history_day)
    trades = load_trade_records(user_id)
    holdings = raw_holdings
    for sym in ALL_SYMBOLS:
        holdings.setdefault(sym, raw_holdings.get(sym, {"shares": 0.0, "avg_cost": 0.0}))
    forward_pe = market.get("forward_pe", {})
    valuation_metrics = market.get("valuation_metrics", {})
    rows: list[dict[str, Any]] = []
    value_cny_by_symbol: dict[str, float] = {}
    total_value_cny = 0.0
    total_cost_cny = 0.0
    for sym in ALL_SYMBOLS:
        meta = ASSET_META[sym]
        holding = holdings[sym]
        quote = quotes[sym]
        shares = float(holding["shares"])
        avg_cost = float(holding["avg_cost"])
        price = float(quote.get("price") or 0.0)
        if price <= 0:
            price = next(
                (
                    candidate
                    for candidate in (
                        float(quote.get("regular_price") or 0.0),
                        float(quote.get("prev_close") or 0.0),
                        avg_cost,
                    )
                    if candidate > 0
                ),
                0.0,
            )
        value = shares * price
        cost = shares * avg_cost
        value_cny = value * fx if meta["currency"] == "USD" else value
        cost_cny = cost * usd_cost_fx if meta["currency"] == "USD" else cost
        dividend_usd = (
            float(balances.get("voo_dividend_usd", 0.0))
            if sym == "VOO"
            else float(balances.get("sgov_dividend_usd", 0.0)) if sym == "SGOV" else 0.0
        )
        pnl = value - cost + dividend_usd
        pnl_cny = value_cny - cost_cny + dividend_usd * fx
        total_value_cny += value_cny
        total_cost_cny += cost_cny
        value_cny_by_symbol[sym] = value_cny
        metrics = valuation_metrics.get(sym, {}) if isinstance(valuation_metrics, dict) else {}
        fpe = metrics.get("forward_pe", forward_pe.get(sym)) if isinstance(metrics, dict) else forward_pe.get(sym)
        peg = metrics.get("peg") if isinstance(metrics, dict) else None
        forward_ps = metrics.get("forward_ps") if isinstance(metrics, dict) else None
        ps = metrics.get("ps") if isinstance(metrics, dict) else None
        sixty_day = fetch_60d_metrics(sym, price) if meta["currency"] == "USD" else {}
        rows.append(
            {
                "symbol": sym,
                "label": meta["label"],
                "currency": meta["currency"],
                "shares": shares,
                "avg_cost": avg_cost,
                "price": price,
                "regular_price": quote.get("regular_price"),
                "session": quote.get("session", "regular"),
                "source": quote.get("source", ""),
                "value": value,
                "value_cny": value_cny,
                "pnl": pnl,
                "pnl_cny": pnl_cny,
                "attributed_dividend_usd": dividend_usd,
                "pnl_pct": pnl / cost * 100.0 if cost > 0 else 0.0,
                "daily_pct": float(quote.get("regular_change_pct", quote.get("change_pct", 0.0))),
                "effective_daily_pct": (
                    carried_completed_daily_pct(sym, quote, latest_completed_row)
                    if carry_completed_daily
                    else history_daily_pct_for_symbol(sym, quote, history_day, history_now)
                ),
                "extended_pct": quote.get("extended_change_pct"),
                "drawdown_pct": sixty_day.get("drawdown_pct"),
                "confirmed_drawdown_pct": sixty_day.get("confirmed_drawdown_pct"),
                "intraday_drawdown_pct": sixty_day.get("intraday_drawdown_pct"),
                "confirmed_close_date": sixty_day.get("confirmed_close_date"),
                "confirmed_close_price": sixty_day.get("confirmed_close_price"),
                "intraday_price": sixty_day.get("intraday_price"),
                "rebound_pct": sixty_day.get("rebound_pct"),
                "recent_5d_pct": sixty_day.get("recent_5d_pct"),
                "forward_pe": fpe,
                "pe_band": pe_band_text(sym) if sym in SATELLITE_SYMBOLS else "-",
                "peg": peg,
                "peg_band": peg_band_text(sym) if sym in SATELLITE_SYMBOLS else "-",
                "forward_ps": forward_ps,
                "ps": ps,
                "ps_band": ps_band_text(sym) if sym in PS_BANDS else None,
                "pe_judgment": pe_judgment(sym, fpe),
            }
        )

    cash_usd = float(balances.get("cash_usd", 0.0))
    cash_cny = float(balances.get("cash_cny", 0.0))
    cash_total_cny = cash_usd * fx + cash_cny
    total_assets_cny = total_value_cny + cash_total_cny

    daily_cards = []
    card_by_symbol: dict[str, dict[str, Any]] = {}
    for sym in ("VOO", "QQQ", *SATELLITE_SYMBOLS, "SGOV", "001015"):
        quote = quotes[sym]
        holding = holdings[sym]
        shares = float(holding["shares"])
        currency = ASSET_META[sym]["currency"]
        regular_price = float(quote.get("regular_price") or quote.get("price") or 0.0)
        regular_value = shares * regular_price
        regular_value_cny = regular_value * fx if currency == "USD" else regular_value
        current_value = shares * float(quote.get("price") or regular_price or 0.0)
        current_value_cny = current_value * fx if currency == "USD" else current_value
        regular_pct = (
            carried_completed_daily_pct(sym, quote, latest_completed_row)
            if carry_completed_daily
            else (
                fund_daily_pct_for_day(quote, history_day)
                if sym == "001015"
                else float(quote.get("regular_change_pct", quote.get("change_pct", 0.0)))
            )
        )
        summary_pct = (
            regular_pct
            if carry_completed_daily
            else history_daily_pct_for_symbol(sym, quote, history_day, history_now)
        )
        regular_change_cny = _daily_amount(regular_value_cny, regular_pct)
        change_cny = _daily_amount(current_value_cny, summary_pct)
        extended_pct = (
            None
            if carry_completed_daily
            else quote.get("extended_change_pct") if quote.get("session") != "regular" else None
        )
        effective_pct = regular_pct if carry_completed_daily else treemap_daily_pct(quote, regular_pct)
        extended_change_cny = None
        if isinstance(extended_pct, (int, float)):
            extended_change = regular_value * (float(extended_pct) / 100.0)
            extended_change_cny = extended_change * fx if currency == "USD" else extended_change
        card = {
                "symbol": sym,
                "label": ASSET_META[sym]["label"],
                "session": quote.get("session", "regular"),
                "price_line": _quote_price_line(sym, quote),
                "regular_pct": regular_pct,
                "summary_pct": summary_pct,
                "effective_pct": effective_pct,
                "extended_pct": extended_pct,
                "regular_change_usd": regular_change_cny / fx if fx > 0 else 0.0,
                "regular_change_cny": regular_change_cny,
                "change_usd": change_cny / fx if fx > 0 else 0.0,
                "change_cny": change_cny,
                "extended_change_usd": extended_change_cny / fx if extended_change_cny is not None and fx > 0 else None,
                "extended_change_cny": extended_change_cny,
            }
        daily_cards.append(card)
        card_by_symbol[sym] = card

    satellite_value_cny = sum(
        float(holdings[sym]["shares"]) * float(quotes[sym].get("regular_price") or quotes[sym].get("price") or 0.0) * fx
        for sym in SATELLITE_SYMBOLS
    )
    satellite_regular_pct = (
        sum(
            (
                float(holdings[sym]["shares"])
                * float(quotes[sym].get("regular_price") or quotes[sym].get("price") or 0.0)
                * fx
            )
            / satellite_value_cny
            * float(quotes[sym].get("regular_change_pct", quotes[sym].get("change_pct", 0.0)))
            for sym in SATELLITE_SYMBOLS
        )
        if satellite_value_cny > 0
        else 0.0
    )
    satellite_change_cny = sum(float(card_by_symbol[sym].get("change_cny") or 0.0) for sym in SATELLITE_SYMBOLS)
    satellite_regular_change_cny = sum(float(card_by_symbol[sym].get("regular_change_cny") or 0.0) for sym in SATELLITE_SYMBOLS)
    satellite_summary_pct = (
        satellite_change_cny / sum(value_cny_by_symbol.get(sym, 0.0) for sym in SATELLITE_SYMBOLS) * 100.0
        if sum(value_cny_by_symbol.get(sym, 0.0) for sym in SATELLITE_SYMBOLS) > 0
        else 0.0
    )
    satellite_extended_change_cny = (
        sum(float(card_by_symbol[sym].get("extended_change_cny") or 0.0) for sym in SATELLITE_SYMBOLS)
        if any(card_by_symbol[sym].get("extended_change_cny") is not None for sym in SATELLITE_SYMBOLS)
        else None
    )
    satellite_extended_pct = (
        satellite_extended_change_cny / satellite_value_cny * 100.0
        if satellite_extended_change_cny is not None and satellite_value_cny > 0
        else None
    )
    satellite_card = {
        "symbol": "SATELLITE",
        "label": "卫星仓位",
        "price_line": "",
        "regular_pct": satellite_regular_pct,
        "summary_pct": satellite_summary_pct,
        "effective_pct": satellite_regular_pct,
        "extended_pct": satellite_extended_pct,
        "regular_change_usd": satellite_regular_change_cny / fx if fx > 0 else 0.0,
        "regular_change_cny": satellite_regular_change_cny,
        "change_usd": satellite_change_cny / fx if fx > 0 else 0.0,
        "change_cny": satellite_change_cny,
        "extended_change_usd": satellite_extended_change_cny / fx if satellite_extended_change_cny is not None and fx > 0 else None,
        "extended_change_cny": satellite_extended_change_cny,
        "wide": False,
    }
    daily_cards.insert(2, satellite_card)

    def accounting_daily_pct(sym: str) -> float:
        if carry_completed_daily:
            return 0.0
        return history_daily_pct_for_symbol(sym, quotes[sym], history_day, history_now)

    history_weighted_daily_pct = (
        sum(
            (value_cny_by_symbol[s] / total_value_cny)
            * accounting_daily_pct(s)
            for s in ALL_SYMBOLS
        )
        if total_value_cny > 0
        else 0.0
    )
    weighted_daily_pct = history_weighted_daily_pct
    weighted_daily_change_cny = sum(
        _daily_amount(value_cny_by_symbol.get(s, 0.0), accounting_daily_pct(s))
        for s in ALL_SYMBOLS
    )
    usd_value_usd = sum(value_cny_by_symbol.get(s, 0.0) / fx for s in USD_SYMBOLS) if fx > 0 else 0.0
    usd_cost_usd = sum(
        float(holdings[s].get("shares", 0.0) or 0.0) * float(holdings[s].get("avg_cost", 0.0) or 0.0)
        for s in USD_SYMBOLS
    )
    attributed_dividend_usd = float(balances.get("voo_dividend_usd", 0.0)) + float(
        balances.get("sgov_dividend_usd", 0.0)
    )
    usd_pnl_usd = usd_value_usd - usd_cost_usd + attributed_dividend_usd
    usd_return_pct = usd_pnl_usd / usd_cost_usd * 100.0 if usd_cost_usd > 0 else 0.0
    usd_daily_pnl_usd = sum(
        _daily_amount(value_cny_by_symbol.get(s, 0.0) / fx if fx > 0 else 0.0, accounting_daily_pct(s))
        for s in USD_SYMBOLS
    )
    usd_daily_basis_usd = usd_value_usd
    usd_daily_pct = (
        usd_daily_pnl_usd / (usd_value_usd - usd_daily_pnl_usd) * 100.0
        if usd_value_usd - usd_daily_pnl_usd > 0
        else 0.0
    )
    holding_pnl_cny = sum(float(row.get("pnl_cny", 0.0)) for row in rows)
    usd_cash_fx_pnl_cny = cash_usd * (fx - usd_cost_fx)
    total_pnl_cny = holding_pnl_cny + usd_cash_fx_pnl_cny
    total_return_basis_cny = total_cost_cny + cash_usd * usd_cost_fx + cash_cny
    total_pnl_pct = total_pnl_cny / total_return_basis_cny * 100.0 if total_return_basis_cny > 0 else 0.0
    usd_fx_pnl_cny = (usd_cost_usd + cash_usd) * (fx - usd_cost_fx)
    current_rows = [
        row
        for row in rows
        if float(row.get("shares", 0.0) or 0.0) > 1e-9
        or float(target_weights.get(str(row.get("symbol", "")).upper(), 0.0) or 0.0) > 1e-9
    ]
    today_cash_flow_cny = sum(
        max(0.0, float(trade.get("amount_usd", 0.0) or 0.0)) * (fx if str(trade.get("symbol", "")).upper() in USD_SYMBOLS else 1.0)
        for trade in trades
        if str(trade.get("trade_date") or "")[:10] == history_day
    )
    performance_history = build_performance_history(
        user_id,
        quotes,
        holdings,
        fx,
        total_assets_cny,
        total_cost_cny,
        total_return_basis_cny,
        total_pnl_pct,
        history_weighted_daily_pct,
        total_pnl_cny - usd_fx_pnl_cny,
        total_pnl_cny,
        usd_fx_pnl_cny,
        cash_usd,
        cash_cny,
        weighted_daily_change_cny,
        total_value_cny,
        usd_return_pct,
        usd_pnl_usd,
        usd_cost_usd,
        usd_daily_pct,
        usd_daily_pnl_usd,
        usd_daily_basis_usd,
        today_cash_flow_cny,
    )

    # The dashboard's total-asset daily weighting must represent one completed
    # trading day.  Do not mix China's already-closed session with live US
    # quotes before the US regular session has closed; use the latest finalized
    # history point instead.  That point includes 001015, VOO, and QQQ together.
    latest_completed_point = next(
        (
            point
            for point in reversed(performance_history.get("points") or [])
            if str(point.get("date") or "") <= completed_day
        ),
        None,
    )
    completed_weighted_change = coerce_optional_float(
        (latest_completed_point or {}).get("holding_daily_pnl_cny")
    )
    completed_weighted_basis = coerce_optional_float(
        (latest_completed_point or {}).get("holding_daily_basis_cny")
    )
    if completed_weighted_change is not None and completed_weighted_basis and completed_weighted_basis > 0:
        weighted_daily_change_cny = completed_weighted_change
        weighted_daily_pct = completed_weighted_change / completed_weighted_basis * 100.0

    # Build the live daily weighting using the user's trading-day definition:
    # 09:00 A-share open through the next US regular close.  Before the US
    # regular session opens, use US overnight/premarket data; during 05:00-
    # 09:00 only that US overnight/premarket component is allowed to move.
    shanghai_now = history_now.astimezone(TZ_SHANGHAI)
    overnight_window = 5 <= shanghai_now.hour < 9
    live_daily_change_cny = 0.0
    for sym in ALL_SYMBOLS:
        quote = quotes.get(sym) or {}
        if sym == "001015":
            pct = 0.0 if overnight_window else fund_daily_pct_for_day(quote, history_day)
        else:
            session = str(quote.get("session") or "").lower()
            if overnight_window:
                pct = coerce_optional_float(quote.get("extended_change_pct")) or 0.0
            elif session == "regular":
                pct = coerce_optional_float(
                    quote.get("regular_change_pct", quote.get("change_pct"))
                ) or 0.0
            else:
                pct = coerce_optional_float(quote.get("extended_change_pct")) or 0.0
        live_daily_change_cny += _daily_amount(value_cny_by_symbol.get(sym, 0.0), pct)
    live_daily_basis_cny = total_value_cny - live_daily_change_cny
    if not carry_completed_daily and live_daily_basis_cny > 0:
        weighted_daily_change_cny = live_daily_change_cny
        weighted_daily_pct = live_daily_change_cny / live_daily_basis_cny * 100.0

    return {
        "user_id": user_id,
        "storage_mode": storage_mode,
        "market": market,
        "holdings": current_rows,
        "balances": balances,
        "fx_conversions": fx_conversions,
        "summary": {
            "fx": fx,
            "avg_fx_rate": usd_cost_fx,
            "fx_conversion_total_cny": fx_conversion_stats["total_cny"],
            "fx_conversion_total_usd": fx_conversion_stats["total_usd"],
            "usd_fx_pnl_cny": usd_fx_pnl_cny,
            "total_value_cny": total_value_cny,
            "total_cost_cny": total_cost_cny,
            "total_return_basis_cny": total_return_basis_cny,
            "holding_pnl_cny": holding_pnl_cny,
            "usd_cash_fx_pnl_cny": usd_cash_fx_pnl_cny,
            "cash_total_cny": cash_total_cny,
            "total_assets_cny": total_assets_cny,
            "total_pnl_cny": total_pnl_cny,
            "total_pnl_pct": total_pnl_pct,
            "weighted_daily_pct": weighted_daily_pct,
            "weighted_daily_change_cny": weighted_daily_change_cny,
            "weighted_daily_change_usd": weighted_daily_change_cny / fx if fx > 0 else 0.0,
            "daily_as_of": (
                str((latest_completed_row or {}).get("date") or completed_day)
                if carry_completed_daily
                else history_day
            ),
            "daily_carried_forward": carry_completed_daily,
        },
        "daily_cards": daily_cards,
        "visualizations": build_visualizations(current_rows, balances, value_cny_by_symbol, fx),
        "targets": target_weights,
        "satellite_targets": load_satellite_targets(),
        "satellite_universe": load_satellite_universe_config(),
        "performance_history": performance_history,
        "rebalance": build_rebalance_v2(user_id, rows, balances, market, value_cny_by_symbol, fx),
        "trades": annotate_trade_close_effects(trades, quotes, fx, history_day),
    }


def evaluate_drawdown_episode_signals(
    user_id: str,
    holding_rows: list[dict[str, Any]],
    *,
    phase: str,
    month_key: str,
    now: datetime,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    created_at = now.isoformat(timespec="seconds")
    with _EPISODE_STATE_LOCK:
        store = load_drawdown_episode_store(user_id)
        snapshots = store.setdefault("threshold_snapshots", {})
        episodes = store.setdefault("episodes", {})
        changed = False
        for item in holding_rows:
            symbol = str(item.get("symbol") or "").upper()
            rule = REBALANCE_RULES.get(phase, {}).get(symbol)
            if not rule:
                continue
            current_snapshot, snapshot_created = ensure_threshold_snapshot(
                store,
                symbol=symbol,
                phase=phase,
                month_key=month_key,
                rule=rule,
                created_at=created_at,
            )
            changed = changed or snapshot_created
            previous_state = episodes.get(symbol)
            next_state, confirmed, state_changed = advance_episode_on_close(
                symbol=symbol,
                state=previous_state if isinstance(previous_state, dict) else None,
                current_snapshot=current_snapshot,
                snapshots=snapshots,
                confirmed_close_date=item.get("confirmed_close_date"),
                confirmed_close_price=item.get("confirmed_close_price"),
                confirmed_drawdown_pct=item.get("confirmed_drawdown_pct"),
            )
            episodes[symbol] = next_state
            changed = changed or state_changed or previous_state is None
            bound_snapshot_id = next_state.get("threshold_snapshot_id")
            bound_snapshot = snapshots.get(bound_snapshot_id) if bound_snapshot_id else current_snapshot
            if not isinstance(bound_snapshot, dict):
                bound_snapshot = current_snapshot
            warning = intraday_warning(
                symbol=symbol,
                intraday_drawdown_pct=item.get("intraday_drawdown_pct"),
                current_price=item.get("intraday_price"),
                session=str(item.get("session") or "unknown"),
                state=next_state,
                current_snapshot=current_snapshot,
                snapshots=snapshots,
                as_of=created_at,
            )
            output[symbol] = {
                "intraday_warning": warning,
                "confirmed_signal": confirmed,
                "episode_state": next_state,
                "threshold_snapshot": bound_snapshot,
                "current_month_threshold_snapshot": current_snapshot,
            }
        if changed:
            save_drawdown_episode_store(user_id, store)
    return output


def build_rebalance_v2(
    user_id: str,
    holding_rows: list[dict[str, Any]],
    balances: dict[str, float],
    market: dict[str, Any],
    value_cny_by_symbol: dict[str, float],
    fx: float,
    phase: str = REBALANCE_PHASE_BUILD,
    build_months: int | None = None,
) -> dict[str, Any]:
    now = datetime.now(TZ_SHANGHAI)
    days_until_midterm = max(0, (MIDTERM_ELECTION_DATE - now.date()).days)
    weeks_until_midterm = max(1, (days_until_midterm + 6) // 7)
    build_month_count = int(build_months or default_build_months(now))
    future_cash_months = max(0, build_month_count - 1)
    month_key = now.strftime("%Y-%m")
    usage = load_monthly_usage(user_id, month_key)
    planned_new_cash_usd = float(usage["planned_new_cash_usd"])
    budget_months = future_month_keys(now, future_cash_months)
    saved_budget_by_month = dict(usage.get("planned_cash_by_month", {}))
    future_cash_by_month = {
        key: float(saved_budget_by_month.get(key, planned_new_cash_usd))
        for key in budget_months
    }
    future_cash_total_usd = sum(future_cash_by_month.values())
    bought_amounts = dict(usage.get("bought_amount_by_symbol", {}))
    sold_amounts = dict(usage.get("sold_amount_by_symbol", {}))
    bought_intensities = {sym: normalize_intensity(v) for sym, v in usage.get("bought_intensity_by_symbol", {}).items()}
    monthly_trade_records = [
        record
        for record in load_trade_records(user_id)
        if str(record.get("trade_date") or "")[:7] == month_key
    ]
    bought_cost_by_symbol: dict[str, float] = {}
    sold_cost_by_symbol: dict[str, float] = {}
    for record in monthly_trade_records:
        sym = str(record.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        if str(record.get("action", "buy")).lower() == "sell":
            sold_cost_by_symbol[sym] = sold_cost_by_symbol.get(sym, 0.0) + trade_cost_basis(record)
        else:
            bought_cost_by_symbol[sym] = bought_cost_by_symbol.get(sym, 0.0) + trade_cost_basis(record)
    target_weights = effective_target_weights()

    usd_weight_total = sum(target_weights[s] for s in USD_SYMBOLS)
    cash_usd = float(balances.get("cash_usd", 0.0))
    cost_usd_by_symbol: dict[str, float] = {}
    for item in holding_rows:
        sym = str(item.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        shares = max(0.0, float(item.get("shares", 0.0) or 0.0))
        avg_cost = max(0.0, float(item.get("avg_cost", 0.0) or 0.0))
        cost_usd_by_symbol[sym] = shares * avg_cost
    sgov_target_pct = target_weights["SGOV"] / usd_weight_total if usd_weight_total > 0 else 0.0
    sgov_current_usd = value_cny_by_symbol.get("SGOV", 0.0) / fx if fx > 0 else 0.0
    holding_cost_total_usd = sum(cost_usd_by_symbol.values())
    planned_total_usd = cash_usd + sgov_current_usd + holding_cost_total_usd + future_cash_total_usd
    planned_total_formula = (
        f"分母 USD {_fmt_usd_exact(planned_total_usd)} = "
        f"非SGOV持仓成本 USD {_fmt_usd_exact(holding_cost_total_usd)} + "
        f"USD现金 USD {_fmt_usd_exact(cash_usd)} + "
        f"SGOV USD {_fmt_usd_exact(sgov_current_usd)} + "
        f"未来资金 USD {_fmt_usd_exact(future_cash_total_usd)}"
    )
    planned_sgov_target_usd = sgov_target_pct * planned_total_usd
    sgov_excess_usd = max(0.0, sgov_current_usd - planned_sgov_target_usd)
    episode_signals = evaluate_drawdown_episode_signals(
        user_id,
        holding_rows,
        phase=phase,
        month_key=month_key,
        now=now,
    )
    rows: list[dict[str, Any]] = []
    full_rebalance_need_usd = 0.0
    has_large_trigger = False
    for item in holding_rows:
        sym = item["symbol"]
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        current_usd = item["value_cny"] / fx if fx > 0 else 0.0
        cost_usd = cost_usd_by_symbol.get(sym, 0.0)
        already = float(bought_amounts.get(sym, 0.0))
        already_sold = float(sold_amounts.get(sym, 0.0))
        is_satellite = sym in SATELLITE_SYMBOLS
        bought_cost = bought_cost_by_symbol.get(sym, already)
        sold_cost = sold_cost_by_symbol.get(sym, 0.0)
        month_start_cost_usd = max(0.0, cost_usd - bought_cost + sold_cost)
        target_pct = target_weights[sym] / usd_weight_total if usd_weight_total > 0 else 0.0
        target_usd = target_pct * planned_total_usd
        planning_cost_usd = cost_usd if is_satellite else month_start_cost_usd
        raw_gap = target_usd - planning_cost_usd
        target_tolerance_usd = max(5.0, target_usd * 0.01)
        gap = 0.0 if abs(raw_gap) <= target_tolerance_usd else raw_gap
        raw_actual_gap = target_usd - cost_usd
        actual_gap = 0.0 if abs(raw_actual_gap) <= target_tolerance_usd else raw_actual_gap
        drawdown_pct = item.get("confirmed_drawdown_pct", item.get("drawdown_pct"))
        episode_payload = episode_signals.get(sym, {})
        confirmed_signal = episode_payload.get("confirmed_signal") or {}
        threshold_snapshot = episode_payload.get("threshold_snapshot") or {}
        monthly_threshold_snapshot = episode_payload.get("current_month_threshold_snapshot") or threshold_snapshot
        validation = monthly_threshold_snapshot.get("validation") or {}
        validation_alerts = list(validation.get("alerts") or [])
        confirmed_tier = str(confirmed_signal.get("tier") or "normal")
        multiplier, action, signal, intensity = signal_for_intensity(sym, phase, confirmed_tier)
        rebalance_rule = REBALANCE_RULES.get(phase, {}).get(sym, {})
        review_mode = rebalance_rule.get("mode")
        tier_diagnostics = rebalance_rule.get("diagnostics")
        previous = normalize_intensity(bought_intensities.get(sym, "none"))

        if normalize_intensity(intensity) == "large":
            has_large_trigger = True

        if is_satellite:
            tier_multiplier = float(multiplier) / 0.1 if float(multiplier) > 0 else 0.0
            base_budget = max(0.0, target_usd) * 0.1
            tier_plan = base_budget * tier_multiplier
            gap_capped_plan = min(max(0.0, gap), tier_plan)
            planned = gap_capped_plan
            planned_formula_parts = [
                f"目标金额 {_fmt_usd_compact(target_usd)} = 分母 {_fmt_usd_compact(planned_total_usd)} x 目标 {target_pct * 100.0:.2f}%",
                f"一手 {_fmt_usd_compact(tier_plan)} = 目标金额 {_fmt_usd_compact(target_usd)} x 0.1 x 档位倍率 {tier_multiplier:.0f}x",
                f"计划应买 {_fmt_usd_compact(planned)} = min(成本缺口, 档位计划)",
            ]
        else:
            weekly_core = sym in {"VOO", "QQQ"}
            cadence_periods = weeks_until_midterm if weekly_core else build_month_count
            base_budget = max(0.0, gap) / max(1, cadence_periods)
            planned = min(max(0.0, gap), base_budget * multiplier)
            planned_formula_parts = [
                f"目标金额 {_fmt_usd_compact(target_usd)} = 分母 {_fmt_usd_compact(planned_total_usd)} x 目标 {target_pct * 100.0:.2f}%",
                f"月初成本 {_fmt_usd_compact(month_start_cost_usd)} = 当前成本 {_fmt_usd_compact(cost_usd)} - 本月买入成本 {_fmt_usd_compact(bought_cost)} + 本月卖出释放成本 {_fmt_usd_compact(sold_cost)}",
                f"成本缺口 {_fmt_usd_compact(gap)} = 目标 {_fmt_usd_compact(target_usd)} - 月初成本 {_fmt_usd_compact(month_start_cost_usd)}",
                f"实际差值 {_fmt_usd_compact(actual_gap)} = 目标 {_fmt_usd_compact(target_usd)} - 当前成本 {_fmt_usd_compact(cost_usd)}",
                (
                    f"每周基准 {_fmt_usd_compact(base_budget)} = 成本缺口 / {weeks_until_midterm}周（至 {MIDTERM_ELECTION_DATE.isoformat()} 中期选举）"
                    if weekly_core
                    else f"月度基准 {_fmt_usd_compact(base_budget)} = 成本缺口 / {build_month_count}月"
                ),
                f"计划应买 {_fmt_usd_compact(planned)} = min(成本缺口, {'每周' if weekly_core else '月度'}基准 x {float(multiplier):.2f}x)",
            ]
        planned_formula = "；".join(planned_formula_parts)

        previous_multiplier = 0.0 if is_satellite else intensity_multiplier(sym, phase, previous)
        additional_multiplier = max(0.0, float(multiplier) - previous_multiplier)
        already_bought_this_month = not is_satellite and previous != "none"
        fpe = item.get("forward_pe")
        split, split_note = valuation_split_for_row(sym, item)
        net_bought = already - already_sold
        raw_planned = planned
        raw_suggested = (
            gap_capped_plan * split
            if is_satellite
            else min(max(0.0, gap), base_budget * additional_multiplier) * split
        )
        suggested_cap = gap_capped_plan * split if is_satellite else raw_suggested
        if already_bought_this_month and raw_suggested <= 1e-9:
            raw_suggested = 0.0
            additional_multiplier = 0.0
        if gap <= 0 and sym != "VOO":
            raw_suggested = 0.0
            action = "暂不买入"

        note_parts: list[str] = []
        if review_mode == "manual_review_only":
            raw_suggested = 0.0
            suggested_cap = 0.0
            action = "复核"
            note_parts.append("未配置经确认的可靠同行组；自身短历史档位仅供观察，系统不会自动建议买入。")
        elif raw_suggested > 0:
            note_parts.append(
                f"卫星股以10月底目标金额为 1x，当前按 {float(multiplier):.1f}x 计算，并取不超过实时缺口。"
                if is_satellite
                else "按目标缺口和当前阶段的月度推进节奏执行。"
            )
        elif already > 0:
            note_parts.append(f"本月已买 USD {already:,.2f}，当前无需系统补买；仍可手动确认。")
        elif raw_gap > 0 and gap == 0:
            note_parts.append("按成本口径已接近目标仓位，价格下跌不触发重复加仓。")
        elif gap <= 0:
            note_parts.append("当前成本仓位已达到或高于目标，系统建议不买。")
        else:
            note_parts.append("当前可动用资金不足或档位额度已用完，先保留观察。")
        if split_note:
            note_parts.append(
                f"{split_note} 估值约束仍保留，但不恢复月度固定额度。"
                if is_satellite
                else f"{split_note} 计划金额仍按建仓节奏买够。"
            )
        history_note = historical_probability_note(sym, item, intensity)
        if history_note:
            note_parts.append(history_note)
        if already_bought_this_month:
            previous_label = INTENSITY_LABELS.get(previous, "已买")
            current_label = INTENSITY_LABELS.get(normalize_intensity(intensity), str(action))
            if raw_suggested <= 1e-9:
                note_parts.append(f"本月已执行到{previous_label}档，已买 USD {already:,.2f}；当前无需系统建议买入。")
            elif normalize_intensity(intensity) == "normal":
                note_parts.append(f"本月已执行到{previous_label}档，当前仍为 normal，本次补齐本月 normal 剩余额度。")
            else:
                note_parts.append(f"本月已执行到{previous_label}档；当前为{current_label}档，本次只补买档位差额（{additional_multiplier:.2f}x）。")
        full_rebalance_need_usd += max(0.0, gap)

        rows.append(
            {
                "symbol": sym,
                "label": item["label"],
                "phase": phase,
                "action": action,
                "signal": signal,
                "intensity": intensity,
                "review_mode": review_mode,
                "tier_diagnostics": tier_diagnostics,
                "intraday_warning": episode_payload.get("intraday_warning"),
                "confirmed_signal": episode_payload.get("confirmed_signal"),
                "episode_state": episode_payload.get("episode_state"),
                "threshold_snapshot": threshold_snapshot,
                "monthly_threshold_snapshot": monthly_threshold_snapshot,
                "walk_forward_warning": {
                    "active": validation.get("status") == "attention" and bool(validation_alerts),
                    "status": validation.get("status") or "ok",
                    "count": len(validation_alerts),
                    "messages": validation_alerts,
                    "review_message": validation.get("review_message"),
                    "diagnostic_count": int(validation.get("diagnostic_count") or 0),
                    "diagnostics": list(validation.get("diagnostics") or []),
                    "statistics": monthly_threshold_snapshot.get("walk_forward") or {},
                    "policy": monthly_threshold_snapshot.get("validation_policy") or "warning_only",
                },
                "planned_buy_usd": raw_planned,
                "raw_planned_buy_usd": raw_planned,
                "planned_buy_formula": planned_formula,
                "target_usd": target_usd,
                "base_budget_usd": base_budget,
                "signal_multiplier": float(multiplier),
                "suggested_buy_usd": raw_suggested,
                "raw_suggested_buy_usd": raw_suggested,
                "suggested_cap_usd": suggested_cap,
                "suggested_sell_usd": max(0.0, -actual_gap),
                "actual_bought_usd": already,
                "actual_sold_usd": already_sold,
                "net_bought_usd": net_bought,
                "planned_after_valuation_usd": suggested_cap if is_satellite else raw_planned * split,
                "buy_difference_usd": actual_gap,
                "actual_gap_usd": actual_gap,
                "month_start_value_usd": month_start_cost_usd,
                "month_start_cost_usd": month_start_cost_usd,
                "current_cost_usd": cost_usd,
                "month_bought_cost_usd": bought_cost,
                "month_sold_cost_usd": sold_cost,
                "gap_usd": gap,
                "drawdown_pct": drawdown_pct,
                "intraday_drawdown_pct": item.get("intraday_drawdown_pct"),
                "confirmed_close_date": item.get("confirmed_close_date"),
                "confirmed_close_price": item.get("confirmed_close_price"),
                "recent_5d_pct": item.get("recent_5d_pct"),
                "target_pct": target_pct * 100.0,
                "current_pct": current_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "month_start_pct": month_start_cost_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "current_cost_pct": cost_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "forward_pe": fpe,
                "pe_band": pe_band_text(sym),
                "forward_ps": item.get("forward_ps"),
                "ps": item.get("ps"),
                "ps_band": ps_band_text(sym) if sym in PS_BANDS else None,
                "peg": item.get("peg"),
                "peg_band": peg_band_text(sym),
                "valuation_split_factor": split,
                "note": " ".join(note_parts),
            }
        )

    sgov_available_usd = sgov_excess_usd
    if sgov_available_usd > 1e-9:
        sgov_price = float(market["quotes"].get("SGOV", {}).get("price") or 0.0)
        sgov_sell_note = (
            "大档位触发，但 SGOV 仍保留最低安全线，只释放安全线以上部分。"
            if has_large_trigger
            else "SGOV 高于目标安全线，建议先卖出超出目标的部分作为子弹。"
        )
        rows.append(
            {
                "symbol": "SGOV",
                "label": "SGOV",
                "phase": phase,
                "action": "卖出",
                "signal": "卖出",
                "intensity": "sell",
                "planned_buy_usd": 0.0,
                "raw_planned_buy_usd": 0.0,
                "planned_buy_formula": f"目标 {_fmt_usd_compact(planned_sgov_target_usd)}，当前 {_fmt_usd_compact(sgov_current_usd)}",
                "target_usd": planned_sgov_target_usd,
                "base_budget_usd": 0.0,
                "signal_multiplier": 0.0,
                "suggested_buy_usd": 0.0,
                "raw_suggested_buy_usd": 0.0,
                "suggested_cap_usd": 0.0,
                "suggested_sell_usd": sgov_available_usd,
                "suggested_sell_shares": sgov_available_usd / sgov_price if sgov_price > 0 else 0.0,
                "actual_bought_usd": float(bought_amounts.get("SGOV", 0.0)),
                "actual_sold_usd": float(sold_amounts.get("SGOV", 0.0)),
                "net_bought_usd": float(bought_amounts.get("SGOV", 0.0)) - float(sold_amounts.get("SGOV", 0.0)),
                "planned_after_valuation_usd": 0.0,
                "buy_difference_usd": -sgov_available_usd,
                "month_start_value_usd": sgov_current_usd,
                "gap_usd": planned_sgov_target_usd - sgov_current_usd,
                "drawdown_pct": None,
                "recent_5d_pct": None,
                "target_pct": sgov_target_pct * 100.0,
                "current_pct": sgov_current_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "forward_pe": None,
                "pe_band": "-",
                "valuation_split_factor": 1.0,
                "note": sgov_sell_note,
            }
        )
    deployable_pool_usd = cash_usd + sgov_available_usd
    remaining_deployable_usd = deployable_pool_usd
    raw_total = sum(float(row["raw_suggested_buy_usd"]) for row in rows)
    monthly_budget_usd = min(deployable_pool_usd, full_rebalance_need_usd) / max(1, build_month_count)
    weekly_budget_usd = min(deployable_pool_usd, full_rebalance_need_usd) / max(1, weeks_until_midterm)
    remaining_reference_budget_usd = monthly_budget_usd
    suggested_run_budget_usd = min(remaining_deployable_usd, raw_total)
    strategy_budget_usd = suggested_run_budget_usd
    scale = strategy_budget_usd / raw_total if raw_total > 0 else 0.0
    for row in rows:
        row["planned_buy_usd"] = row["raw_planned_buy_usd"]
        scaled_suggestion = float(row["raw_suggested_buy_usd"]) * scale
        if row["symbol"] in SATELLITE_SYMBOLS:
            scaled_suggestion = min(float(row.get("suggested_cap_usd", 0.0)), scaled_suggestion)
        row["suggested_buy_usd"] = scaled_suggestion
        row["buy_difference_usd"] = float(
            row.get(
                "actual_gap_usd",
                float(row.get("planned_buy_usd", 0.0)) - float(row.get("net_bought_usd", 0.0)),
            )
        )
        price = float(market["quotes"].get(row["symbol"], {}).get("price") or 0.0)
        row["suggested_buy_shares"] = row["suggested_buy_usd"] / price if price > 0 else 0.0
        row["suggested_sell_shares"] = float(row.get("suggested_sell_usd") or 0.0) / price if price > 0 else float(row.get("suggested_sell_shares") or 0.0)
    strategy_budget_usd = sum(float(row["suggested_buy_usd"]) for row in rows)
    episode_store = load_drawdown_episode_store(user_id)
    monthly_recalculation = (episode_store.get("monthly_recalculations") or {}).get(month_key)

    return {
        "month_key": month_key,
        "planned_new_cash_usd": planned_new_cash_usd,
        "future_cash_by_month": future_cash_by_month,
        "future_cash_total_usd": future_cash_total_usd,
        "base_planned_total_usd": planned_total_usd,
        "build_target": f"{BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}",
        "build_months": build_month_count,
        "weeks_until_midterm": weeks_until_midterm,
        "midterm_election_date": MIDTERM_ELECTION_DATE.isoformat(),
        "future_cash_months": future_cash_months,
        "deployable_pool_usd": deployable_pool_usd,
        "remaining_deployable_usd": remaining_deployable_usd,
        "monthly_budget_usd": monthly_budget_usd,
        "weekly_budget_usd": weekly_budget_usd,
        "remaining_reference_budget_usd": remaining_reference_budget_usd,
        "strategy_budget_usd": strategy_budget_usd,
        "suggestion_scale": scale,
        "sgov_excess_usd": sgov_excess_usd,
        "sgov_available_usd": sgov_available_usd,
        "sgov_large_trigger_enabled": has_large_trigger,
        "planned_total_usd": planned_total_usd,
        "planned_total_formula": planned_total_formula,
        "rules": rebalance_rules_payload(build_month_count, future_cash_months, planned_new_cash_usd, future_cash_total_usd),
        "monthly_recalculation": monthly_recalculation,
        "usage": usage,
        "intraday_warning": {symbol: payload.get("intraday_warning") for symbol, payload in episode_signals.items()},
        "confirmed_signal": {symbol: payload.get("confirmed_signal") for symbol, payload in episode_signals.items()},
        "episode_state": {symbol: payload.get("episode_state") for symbol, payload in episode_signals.items()},
        "threshold_snapshot": {symbol: payload.get("threshold_snapshot") for symbol, payload in episode_signals.items()},
        "rows": rows,
    }


def build_rebalance(
    user_id: str,
    holding_rows: list[dict[str, Any]],
    balances: dict[str, float],
    market: dict[str, Any],
    value_cny_by_symbol: dict[str, float],
    fx: float,
    phase: str = REBALANCE_PHASE_BUILD,
    build_months: int = 12,
) -> dict[str, Any]:
    now = datetime.now(TZ_SHANGHAI)
    month_key = now.strftime("%Y-%m")
    usage = load_monthly_usage(user_id, month_key)
    planned_new_cash_usd = float(usage["planned_new_cash_usd"])
    bought_amounts = dict(usage.get("bought_amount_by_symbol", {}))
    bought_intensities = {sym: normalize_intensity(v) for sym, v in usage.get("bought_intensity_by_symbol", {}).items()}
    target_weights = effective_target_weights()
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS)
    usd_total_usd = usd_total_cny / fx if fx > 0 else 0.0
    planned_total_usd = usd_total_usd + planned_new_cash_usd * max(0, build_months - 1)
    usd_weight_total = sum(target_weights[s] for s in USD_SYMBOLS)
    rows = []
    for item in holding_rows:
        sym = item["symbol"]
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        current_usd = item["value_cny"] / fx if fx > 0 else 0.0
        already = float(bought_amounts.get(sym, 0.0))
        is_satellite = sym in SATELLITE_SYMBOLS
        planning_current = current_usd if is_satellite else max(0.0, current_usd - already)
        target_pct = target_weights[sym] / usd_weight_total if usd_weight_total > 0 else 0.0
        target_usd = target_pct * planned_total_usd
        gap = target_usd - planning_current
        multiplier, action, signal, intensity = signal_for_historical_position(sym, item, phase)
        previous = normalize_intensity(bought_intensities.get(sym, "none"))
        base_budget = max(0.0, target_usd) if is_satellite else max(0.0, gap) / max(1, build_months)
        planned = base_budget * multiplier if is_satellite else min(max(0.0, gap), base_budget * multiplier)
        fpe = item.get("forward_pe")
        band = PE_BANDS.get(sym)
        split = 0.5 if sym in SATELLITE_SYMBOLS and isinstance(fpe, (int, float)) and band and fpe > band[1] else 1.0
        suggested = min(max(0.0, gap), planned) * split if is_satellite else max(0.0, planned * split - already)
        rows.append(
            {
                "symbol": sym,
                "label": item["label"],
                "phase": phase,
                "action": action,
                "signal": signal,
                "intensity": intensity,
                "planned_buy_usd": planned,
                "suggested_buy_usd": suggested,
                "actual_bought_usd": already,
                "gap_usd": gap,
                "target_pct": target_pct * 100.0,
                "current_pct": current_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "forward_pe": fpe,
                "pe_band": pe_band_text(sym),
                "peg": item.get("peg"),
                "peg_band": peg_band_text(sym),
                "note": "Forward PE 偏高，建议分批买入。" if split < 1.0 else "按当前规则执行。",
            }
        )
    return {
        "month_key": month_key,
        "planned_new_cash_usd": planned_new_cash_usd,
        "usage": usage,
        "rows": rows,
    }


def save_rebalance_budget(user_id: str, planned_cash_by_month: dict[str, float]) -> dict[str, Any]:
    now = datetime.now(TZ_SHANGHAI)
    month_key = now.strftime("%Y-%m")
    usage = load_monthly_usage(user_id, month_key)
    clean: dict[str, float] = {}
    for month, amount in planned_cash_by_month.items():
        key = str(month).strip()
        if not key:
            continue
        clean[key] = max(0.0, float(amount or 0.0))
    first_value = next(iter(clean.values()), float(usage["planned_new_cash_usd"]))
    save_monthly_usage(
        user_id,
        month_key,
        planned_new_cash_usd=first_value,
        planned_cash_by_month=clean,
        bought_amount_by_symbol=dict(usage.get("bought_amount_by_symbol", {})),
        bought_intensity_by_symbol=dict(usage.get("bought_intensity_by_symbol", {})),
        sold_amount_by_symbol=dict(usage.get("sold_amount_by_symbol", {})),
    )
    return {"saved": True, "month_key": month_key, "planned_cash_by_month": clean}


def add_fx_conversion_record(user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(TZ_SHANGHAI)
    converted_date = str(payload.get("converted_date") or payload.get("date") or now.date().isoformat()).strip()[:10]
    try:
        date.fromisoformat(converted_date)
    except ValueError as exc:
        raise ValueError("购汇日期无效") from exc
    cny_amount = max(0.0, float(payload.get("cny_amount", 0.0) or 0.0))
    usd_amount = max(0.0, float(payload.get("usd_amount", 0.0) or 0.0))
    if cny_amount <= 0 or usd_amount <= 0:
        raise ValueError("请填写人民币金额和美元金额")
    records = load_fx_conversion_records(user_id)
    records.append(
        {
            "id": f"{now.strftime('%Y%m%d%H%M%S')}-{len(records)}-FX",
            "converted_date": converted_date,
            "cny_amount": cny_amount,
            "usd_amount": usd_amount,
            "rate": cny_amount / usd_amount,
            "note": str(payload.get("note") or ""),
            "created_at": now.isoformat(timespec="seconds"),
        }
    )
    save_fx_conversion_records(user_id, records)
    invalidate_performance_history_from(user_id, converted_date)
    return {"saved": True, "records": load_fx_conversion_records(user_id)}


def delete_fx_conversion_record(user_id: str, record_id: str) -> dict[str, Any]:
    records = load_fx_conversion_records(user_id)
    target = next((record for record in records if str(record.get("id")) == str(record_id)), None)
    if target is None:
        raise ValueError("购汇记录不存在")
    remaining = [record for record in records if str(record.get("id")) != str(record_id)]
    save_fx_conversion_records(user_id, remaining)
    converted_date = str(target.get("converted_date") or "")[:10]
    if converted_date:
        invalidate_performance_history_from(user_id, converted_date)
    return {"deleted": True, "record_id": record_id, "records": load_fx_conversion_records(user_id)}


def confirm_trades(user_id: str, executions: list[dict[str, Any]]) -> dict[str, Any]:
    holdings, balances, _ = load_user_state(user_id)
    now = datetime.now(TZ_SHANGHAI)
    month_key = now.strftime("%Y-%m")
    usage = load_monthly_usage(user_id, month_key)
    amounts = dict(usage.get("bought_amount_by_symbol", {}))
    sold_amounts = dict(usage.get("sold_amount_by_symbol", {}))
    intensities = dict(usage.get("bought_intensity_by_symbol", {}))
    total_bought = 0.0
    total_sold = 0.0
    realized_pnl = 0.0
    for item in executions:
        sym = str(item.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        action = str(item.get("action", "buy")).lower()
        if action not in {"buy", "sell"}:
            raise ValueError(f"{sym} 的交易方向无效")
        amount = max(0.0, float(item.get("amount_usd", 0.0) or 0.0))
        shares = max(0.0, float(item.get("shares", 0.0) or 0.0))
        if amount <= 0 or shares <= 0:
            continue
        price = amount / shares
        old = holdings[sym]
        old_shares = float(old["shares"])
        old_cost = float(old["avg_cost"])
        if action == "sell":
            if shares > old_shares + 1e-9:
                raise ValueError(f"{sym} 卖出股数 {shares:g} 超过当前持仓 {old_shares:g}")
            new_shares = max(0.0, old_shares - shares)
            sale_pnl = amount - shares * old_cost
            if new_shares > 1e-9:
                remaining_cost = max(0.0, old_shares * old_cost - amount)
                new_avg_cost = remaining_cost / new_shares
                holdings[sym] = {"shares": new_shares, "avg_cost": new_avg_cost}
                realized_pnl += max(0.0, amount - old_shares * old_cost)
            else:
                holdings[sym] = {"shares": 0.0, "avg_cost": 0.0}
                realized_pnl += sale_pnl
            sold_amounts[sym] = float(sold_amounts.get(sym, 0.0)) + amount
            total_sold += amount
            continue

        new_shares = old_shares + shares
        holdings[sym] = {"shares": new_shares, "avg_cost": (old_shares * old_cost + shares * price) / new_shares}
        amounts[sym] = float(amounts.get(sym, 0.0)) + amount
        new_intensity = normalize_intensity(item.get("intensity", "normal"))
        old_intensity = normalize_intensity(intensities.get(sym, "none"))
        if INTENSITY_ORDER.get(new_intensity, 0) > INTENSITY_ORDER.get(old_intensity, 0):
            intensities[sym] = new_intensity
        total_bought += amount
    balances["cash_usd"] = max(0.0, float(balances.get("cash_usd", 0.0)) - total_bought + total_sold)
    balances["realized_usd"] = float(balances.get("realized_usd", 0.0)) + realized_pnl
    save_user_state(user_id, holdings, balances)
    save_monthly_usage(
        user_id,
        month_key,
        planned_new_cash_usd=float(usage["planned_new_cash_usd"]),
        planned_cash_by_month=dict(usage.get("planned_cash_by_month", {})),
        bought_amount_by_symbol=amounts,
        bought_intensity_by_symbol=intensities,
        sold_amount_by_symbol=sold_amounts,
    )
    return {
        "saved": True,
        "total_bought_usd": total_bought,
        "total_sold_usd": total_sold,
        "realized_pnl_usd": realized_pnl,
        "month_key": month_key,
    }


def confirm_trades(user_id: str, executions: list[dict[str, Any]]) -> dict[str, Any]:
    holdings, balances, _ = load_user_state(user_id)
    now = datetime.now(TZ_SHANGHAI)
    records = load_trade_records(user_id)
    usage_by_month: dict[str, dict[str, Any]] = {}
    total_bought = 0.0
    total_sold = 0.0
    realized_pnl = 0.0
    earliest_trade_date = ""
    touched_months: set[str] = set()
    available_usd = float(balances.get("cash_usd", 0.0) or 0.0)
    available_cny = float(balances.get("cash_cny", 0.0) or 0.0)
    for item in executions:
        sym = str(item.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS and sym != "001015":
            continue
        is_cny_trade = sym == "001015"
        tracks_monthly_usage = sym != "SGOV" and not is_cny_trade
        action = str(item.get("action", "buy")).lower()
        if action not in {"buy", "sell"}:
            raise ValueError(f"{sym} 的交易方向无效")
        amount = max(0.0, float(item.get("amount_usd", 0.0) or 0.0))
        shares = max(0.0, float(item.get("shares", 0.0) or 0.0))
        if amount <= 0 or shares <= 0:
            continue
        trade_date = str(item.get("trade_date") or item.get("date") or now.date().isoformat()).strip()[:10]
        try:
            date.fromisoformat(trade_date)
        except ValueError as exc:
            raise ValueError(f"{sym} 交易日期无效") from exc
        month_key = trade_date[:7]
        usage = usage_by_month.get(month_key)
        if usage is None:
            usage = load_monthly_usage(user_id, month_key)
            usage_by_month[month_key] = usage
        amounts = dict(usage.get("bought_amount_by_symbol", {}))
        sold_amounts = dict(usage.get("sold_amount_by_symbol", {}))
        intensities = dict(usage.get("bought_intensity_by_symbol", {}))
        prev_avg_cost = float(holdings.get(sym, {}).get("avg_cost", 0.0) or 0.0)
        if action == "sell":
            old_shares = float(holdings.get(sym, {}).get("shares", 0.0) or 0.0)
            if shares > old_shares + 1e-9:
                raise ValueError(f"{sym} 卖出股数 {shares:g} 超过当前持仓 {old_shares:g}")
            old_cost = prev_avg_cost
            cost_basis = shares * old_cost
            sale_pnl = amount - cost_basis
            holdings, _ = apply_trade_to_holdings(holdings, {"symbol": sym, "action": action, "amount_usd": amount, "shares": shares})
            new_avg_cost = float(holdings.get(sym, {}).get("avg_cost", 0.0) or 0.0)
            if is_cny_trade:
                available_cny += amount
                balances["realized_cny"] = float(balances.get("realized_cny", 0.0)) + sale_pnl
            else:
                available_usd += amount
                realized_pnl += sale_pnl
                if tracks_monthly_usage:
                    sold_amounts[sym] = float(sold_amounts.get(sym, 0.0)) + amount
                total_sold += amount
        else:
            if is_cny_trade:
                if amount > available_cny + 1e-9:
                    raise ValueError(f"{sym} 买入金额 {amount:,.2f} CNY 超过当前人民币现金 {available_cny:,.2f} CNY")
            elif amount > available_usd + 1e-9:
                raise ValueError(f"{sym} 买入金额 {amount:,.2f} USD 超过当前美元现金 {available_usd:,.2f} USD")
            holdings, _ = apply_trade_to_holdings(holdings, {"symbol": sym, "action": action, "amount_usd": amount, "shares": shares})
            new_avg_cost = float(holdings.get(sym, {}).get("avg_cost", 0.0) or 0.0)
            cost_basis = amount
            sale_pnl = 0.0
            if is_cny_trade:
                available_cny -= amount
            else:
                available_usd -= amount
                if tracks_monthly_usage:
                    amounts[sym] = float(amounts.get(sym, 0.0)) + amount
                    new_intensity = normalize_intensity(item.get("intensity", "normal"))
                    old_intensity = normalize_intensity(intensities.get(sym, "none"))
                    if intensity_rank(new_intensity) > intensity_rank(old_intensity):
                        intensities[sym] = new_intensity
                total_bought += amount
        usage["bought_amount_by_symbol"] = amounts
        usage["sold_amount_by_symbol"] = sold_amounts
        usage["bought_intensity_by_symbol"] = intensities
        touched_months.add(month_key)
        records.append(
            {
                "id": f"{now.strftime('%Y%m%d%H%M%S')}-{len(records)}-{sym}",
                "trade_date": trade_date,
                "symbol": sym,
                "action": action,
                "amount_usd": amount,
                "shares": shares,
                "price": amount / shares,
                "cost_basis": cost_basis,
                "realized_pnl": sale_pnl,
                "prev_avg_cost": prev_avg_cost,
                "new_avg_cost": new_avg_cost,
                "intensity": normalize_intensity(item.get("intensity", "normal")),
                "created_at": now.isoformat(timespec="seconds"),
            }
        )
        earliest_trade_date = trade_date if not earliest_trade_date else min(earliest_trade_date, trade_date)
    balances["cash_usd"] = max(0.0, available_usd)
    balances["cash_cny"] = max(0.0, available_cny)
    balances["realized_usd"] = float(balances.get("realized_usd", 0.0)) + realized_pnl
    save_user_state(user_id, holdings, balances)
    save_trade_records(user_id, records)
    for month_key in sorted(touched_months):
        usage = usage_by_month[month_key]
        save_monthly_usage(
            user_id,
            month_key,
            planned_new_cash_usd=float(usage["planned_new_cash_usd"]),
            planned_cash_by_month=dict(usage.get("planned_cash_by_month", {})),
            bought_amount_by_symbol=dict(usage.get("bought_amount_by_symbol", {})),
            bought_intensity_by_symbol=dict(usage.get("bought_intensity_by_symbol", {})),
            sold_amount_by_symbol=dict(usage.get("sold_amount_by_symbol", {})),
        )
    if earliest_trade_date:
        invalidate_performance_history_from(user_id, earliest_trade_date)
    return {
        "saved": True,
        "total_bought_usd": total_bought,
        "total_sold_usd": total_sold,
        "realized_pnl_usd": realized_pnl,
        "month_key": now.strftime("%Y-%m"),
        "trades": load_trade_records(user_id),
    }


def _rebuild_month_usage_from_records(user_id: str, month_key: str, records: list[dict[str, Any]]) -> None:
    usage = load_monthly_usage(user_id, month_key)
    bought_amounts: dict[str, float] = {}
    sold_amounts: dict[str, float] = {}
    intensities: dict[str, str] = {}
    for record in records:
        if str(record.get("trade_date") or "")[:7] != month_key:
            continue
        sym = str(record.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        amount = max(0.0, float(record.get("amount_usd", 0.0) or 0.0))
        if str(record.get("action", "buy")).lower() == "sell":
            sold_amounts[sym] = sold_amounts.get(sym, 0.0) + amount
            continue
        bought_amounts[sym] = bought_amounts.get(sym, 0.0) + amount
        new_intensity = normalize_intensity(record.get("intensity", "normal"))
        old_intensity = normalize_intensity(intensities.get(sym, "none"))
        if intensity_rank(new_intensity) > intensity_rank(old_intensity):
            intensities[sym] = new_intensity
    save_monthly_usage(
        user_id,
        month_key,
        planned_new_cash_usd=float(usage["planned_new_cash_usd"]),
        planned_cash_by_month=dict(usage.get("planned_cash_by_month", {})),
        bought_amount_by_symbol=bought_amounts,
        bought_intensity_by_symbol=intensities,
        sold_amount_by_symbol=sold_amounts,
    )


def delete_trade_record(user_id: str, trade_id: str) -> dict[str, Any]:
    records = load_trade_records(user_id)
    target = next((record for record in records if str(record.get("id")) == str(trade_id)), None)
    if target is None:
        raise ValueError("交易记录不存在")

    holdings, balances, _ = load_user_state(user_id)
    sym = str(target.get("symbol", "")).upper()
    action = str(target.get("action", "buy")).lower()
    amount = max(0.0, float(target.get("amount_usd", 0.0) or 0.0))
    shares = max(0.0, float(target.get("shares", 0.0) or 0.0))
    is_cny_trade = sym == "001015"
    if sym not in holdings or amount <= 0 or shares <= 0:
        raise ValueError("交易记录无法撤销")

    current = holdings[sym]
    current_shares = float(current.get("shares", 0.0) or 0.0)
    current_cost = float(current.get("avg_cost", 0.0) or 0.0)
    if action == "buy":
        if current_shares + 1e-9 < shares:
            raise ValueError(f"{sym} 当前持仓不足，无法撤销该次买入")
        new_shares = max(0.0, current_shares - shares)
        remaining_cost = max(0.0, current_shares * current_cost - amount)
        holdings[sym] = {
            "shares": new_shares,
            "avg_cost": remaining_cost / new_shares if new_shares > 1e-9 else 0.0,
        }
        if is_cny_trade:
            balances["cash_cny"] = float(balances.get("cash_cny", 0.0) or 0.0) + amount
        else:
            balances["cash_usd"] = float(balances.get("cash_usd", 0.0) or 0.0) + amount
    elif action == "sell":
        cost_basis = float(target.get("cost_basis", 0.0) or 0.0)
        avg_restore_cost = cost_basis / shares if cost_basis > 0 else current_cost
        new_shares = current_shares + shares
        holdings[sym] = {
            "shares": new_shares,
            "avg_cost": (current_shares * current_cost + shares * avg_restore_cost) / new_shares if new_shares > 1e-9 else 0.0,
        }
        if is_cny_trade:
            current_cash = float(balances.get("cash_cny", 0.0) or 0.0)
            if amount > current_cash + 1e-9:
                raise ValueError(f"人民币现金不足，无法撤销该次卖出")
            balances["cash_cny"] = current_cash - amount
            balances["realized_cny"] = float(balances.get("realized_cny", 0.0) or 0.0) - float(target.get("realized_pnl", 0.0) or 0.0)
        else:
            current_cash = float(balances.get("cash_usd", 0.0) or 0.0)
            if amount > current_cash + 1e-9:
                raise ValueError(f"美元现金不足，无法撤销该次卖出")
            balances["cash_usd"] = current_cash - amount
            balances["realized_usd"] = float(balances.get("realized_usd", 0.0) or 0.0) - float(target.get("realized_pnl", 0.0) or 0.0)
    else:
        raise ValueError("交易方向无效")

    remaining_records = [record for record in records if str(record.get("id")) != str(trade_id)]
    save_user_state(user_id, holdings, balances)
    save_trade_records(user_id, remaining_records)
    month_key = str(target.get("trade_date") or "")[:7]
    if month_key:
        _rebuild_month_usage_from_records(user_id, month_key, remaining_records)
    trade_date = str(target.get("trade_date") or "")[:10]
    if trade_date:
        invalidate_performance_history_from(user_id, trade_date)
    return {"deleted": True, "trade_id": trade_id, "trades": load_trade_records(user_id)}
