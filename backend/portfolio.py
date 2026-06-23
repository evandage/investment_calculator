from __future__ import annotations

import time
from calendar import monthrange
from datetime import datetime
from typing import Any

from .config import (
    ALL_SYMBOLS,
    ASSET_META,
    INTENSITY_LABELS,
    INTENSITY_ORDER,
    PE_BANDS,
    REBALANCE_PHASE_BUILD,
    REBALANCE_RULES,
    SATELLITE_SYMBOLS,
    TARGET_WEIGHTS,
    TZ_SHANGHAI,
    USD_SYMBOLS,
)
from .market_data import fetch_quotes
from .storage import load_monthly_usage, load_user_state, save_monthly_usage, save_user_state


BUILD_TARGET_YEAR = 2026
BUILD_TARGET_MONTH = 10
_DRAWDOWN_CACHE: dict[str, tuple[dict[str, float | None], float]] = {}
_DRAWDOWN_CACHE_TTL_SECONDS = 21600
QQQ_MONTH_END_WINDOW_DAYS = 5


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
                    "卫星仓位内部目标：ISRG / GOOGL / MSFT / AVGO / NVDA = 5 / 3 / 2 / 3 / 2。",
                    "A股基金按 Dashboard 目标占比展示，但当前买入建议只处理美元标的。",
                ],
            },
            {
                "heading": "建仓期分母",
                "items": [
                    f"建仓期默认计算到 {BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}，当前共 {build_months} 个月。",
                    f"未来入金按下个月起计算，共 {future_cash_months} 个月，每月 USD {planned_new_cash_usd:,.2f}。",
                    "目标分母 = 当前美元持仓 + USD 现金 + 未来每月计划入金；SGOV 已包含在当前美元资产内，超过 20% 的部分只进入可动用资金池，不重复加进目标分母。",
                ],
            },
            {
                "heading": "建议金额",
                "items": [
                    "先用月初口径持仓计算缺口：月初口径 = 当前持仓金额 - 本月已买金额。",
                    "再按档位倍率计算本月计划应买，VOO 建仓期至少按 1 股规划。",
                    "估值/追高系数不改变计划应买金额，只影响本轮建议买入；卫星股看 Forward PE，VOO/QQQ 看近 5 个交易日涨幅。",
                    "可动用资金不足时按比例缩放计划应买；建议买入 = max(0, 计划应买 - 本月已买)。",
                ],
            },
            {
                "heading": "档位规则",
                "items": [
                    "VOO：小加 -3% / 1.5x，中加 -7% / 2x，大加 -10% / 3x。",
                    "QQQ：小加 -5% / 1.5x，中加 -10% / 2.5x，大加 -13% / 4x。",
                    "ISRG：小加 -15% / 1.5x，中加 -20% / 2x，大加 -23% / 3.5x。",
                    "GOOGL：小加 -11% / 1.5x，中加 -19% / 2.5x，大加 -24% / 4x。",
                    "MSFT：小加 -12% / 1.5x，中加 -18% / 2.5x，大加 -22% / 4x。",
                    "AVGO：小加 -15% / 1.5x，中加 -22% / 2.5x，大加 -25% / 4x。",
                    "NVDA：小加 -12% / 1.5x，中加 -21% / 3x，大加 -25% / 5x。",
                ],
            },
        ],
    }


def normalize_intensity(value: Any) -> str:
    v = str(value or "none").strip().lower()
    return {
        "": "none",
        "none": "none",
        "normal": "normal",
        "regular": "normal",
        "probe": "probe",
        "dip": "probe",
        "month_end": "month_end",
        "month-end": "month_end",
        "monthend": "month_end",
        "small": "small",
        "medium": "medium",
        "large": "large",
    }.get(v, "none")


def intensity_rank(value: Any) -> int:
    return {
        "none": 0,
        "normal": 1,
        "probe": 2,
        "month_end": 3,
        "small": 4,
        "medium": 5,
        "large": 6,
    }.get(normalize_intensity(value), 0)


def intensity_label(value: Any) -> str:
    intensity = normalize_intensity(value)
    return {
        "probe": "QQQ -2%分批",
        "month_end": "QQQ月底补齐",
    }.get(intensity, INTENSITY_LABELS.get(intensity, str(intensity)))


def is_month_end_window(now: datetime) -> bool:
    last_day = monthrange(now.year, now.month)[1]
    return now.day >= max(1, last_day - QQQ_MONTH_END_WINDOW_DAYS + 1)


def qqq_build_signal(drawdown_pct: float | None, now: datetime | None = None) -> tuple[float, str, str, str]:
    if isinstance(drawdown_pct, (int, float)):
        drawdown = float(drawdown_pct)
        if drawdown <= -13.0:
            return 4.0, "QQQ大加", "4x", "large"
        if drawdown <= -10.0:
            return 2.5, "QQQ中加", "2.5x", "medium"
        if drawdown <= -5.0:
            return 1.5, "QQQ小加", "1.5x", "small"
        if drawdown <= -2.0:
            return 0.8, "QQQ -2%分批", "0.8x", "probe"
    if now is not None and is_month_end_window(now):
        return 1.0, "QQQ月底补齐", "1.0x", "month_end"
    return 0.6, "QQQ月初分批", "0.6x", "normal"


def intensity_multiplier(symbol: str, phase: str, intensity: str) -> float:
    intensity = normalize_intensity(intensity)
    if symbol == "QQQ" and phase == REBALANCE_PHASE_BUILD:
        return {
            "none": 0.0,
            "normal": 0.6,
            "probe": 0.8,
            "month_end": 1.0,
            "small": 1.5,
            "medium": 2.5,
            "large": 4.0,
        }.get(intensity, 0.0)
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
    if symbol == "QQQ" and phase == REBALANCE_PHASE_BUILD:
        return qqq_build_signal(drawdown_pct)
    rule = REBALANCE_RULES.get(phase, {}).get(symbol)
    if not rule:
        return 0.0, "暂无规则", "暂无规则", "normal"
    normal = rule["normal"]
    if not isinstance(drawdown_pct, (int, float)):
        return float(normal[0]), str(normal[1]), str(normal[2]), str(normal[3])
    for threshold, multiplier, action, signal, intensity in rule["bands"]:
        if float(drawdown_pct) <= threshold:
            return float(multiplier), str(action), str(signal), str(intensity)
    return float(normal[0]), str(normal[1]), str(normal[2]), str(normal[3])


def signal_for_intensity(symbol: str, phase: str, intensity: str) -> tuple[float, str, str, str]:
    rule = REBALANCE_RULES.get(phase, {}).get(symbol)
    intensity = normalize_intensity(intensity)
    if symbol == "QQQ" and phase == REBALANCE_PHASE_BUILD:
        multiplier = intensity_multiplier(symbol, phase, intensity)
        action, signal = {
            "normal": ("QQQ月初分批", "0.6x"),
            "probe": ("QQQ -2%分批", "0.8x"),
            "month_end": ("QQQ月底补齐", "1.0x"),
            "small": ("QQQ小加", "1.5x"),
            "medium": ("QQQ中加", "2.5x"),
            "large": ("QQQ大加", "4x"),
        }.get(intensity, ("暂无规则", "暂无规则"))
        return multiplier, action, signal, intensity
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


def valuation_split_for_row(symbol: str, item: dict[str, Any]) -> tuple[float, str]:
    fpe = item.get("forward_pe")
    band = PE_BANDS.get(symbol)
    if symbol in SATELLITE_SYMBOLS and isinstance(fpe, (int, float)) and band and fpe > band[1]:
        return 0.5, "Forward PE 高于合理区间，建议执行时分批，估值/追高系数 0.50。"

    recent_5d = item.get("recent_5d_pct")
    if symbol == "VOO" and isinstance(recent_5d, (int, float)):
        if recent_5d >= 3.0:
            return 0.5, f"VOO 近 5 个交易日涨幅 {recent_5d:.2f}%，短期偏热，建议分批，估值/追高系数 0.50。"
        if recent_5d >= 2.0:
            return 0.75, f"VOO 近 5 个交易日涨幅 {recent_5d:.2f}%，本轮稍微分批，估值/追高系数 0.75。"
    if symbol == "QQQ" and isinstance(recent_5d, (int, float)):
        if recent_5d >= 4.0:
            return 0.5, f"QQQ 近 5 个交易日涨幅 {recent_5d:.2f}%，短期偏热，建议分批，估值/追高系数 0.50。"
        if recent_5d >= 3.0:
            return 0.75, f"QQQ 近 5 个交易日涨幅 {recent_5d:.2f}%，本轮稍微分批，估值/追高系数 0.75。"
    return 1.0, ""


def fetch_60d_metrics(symbol: str, current_price: float | None = None) -> dict[str, float | None]:
    now = time.time()
    cached = _DRAWDOWN_CACHE.get(symbol)
    if cached and now - cached[1] < _DRAWDOWN_CACHE_TTL_SECONDS:
        metrics = dict(cached[0])
    else:
        metrics = {"drawdown_pct": None, "rebound_pct": None, "recent_5d_pct": None, "peak": None, "trough": None, "prev_5d": None}
        try:
            from .ohlcv import fetch_ohlcv

            payload = fetch_ohlcv(symbol, "1d")
            bars = payload.get("bars") if isinstance(payload, dict) else []
            closes = [float(bar.get("close")) for bar in (bars or [])[-60:] if isinstance(bar, dict) and float(bar.get("close") or 0) > 0]
            if closes:
                peak = max(closes)
                trough = min(closes)
                last = closes[-1]
                prev_5d = closes[-6] if len(closes) >= 6 else None
                metrics = {
                    "drawdown_pct": (last / peak - 1.0) * 100.0 if peak > 0 else None,
                    "rebound_pct": (last / trough - 1.0) * 100.0 if trough > 0 else None,
                    "recent_5d_pct": (last / prev_5d - 1.0) * 100.0 if prev_5d and prev_5d > 0 else None,
                    "peak": peak,
                    "trough": trough,
                    "prev_5d": prev_5d,
                }
        except Exception:
            metrics = {"drawdown_pct": None, "rebound_pct": None, "recent_5d_pct": None, "peak": None, "trough": None, "prev_5d": None}
        if metrics.get("peak") and metrics.get("trough"):
            _DRAWDOWN_CACHE[symbol] = (dict(metrics), now)

    price = float(current_price or 0.0)
    peak = metrics.get("peak")
    trough = metrics.get("trough")
    if price > 0 and isinstance(peak, (int, float)) and peak > 0:
        metrics["drawdown_pct"] = (price / float(peak) - 1.0) * 100.0
    if price > 0 and isinstance(trough, (int, float)) and trough > 0:
        metrics["rebound_pct"] = (price / float(trough) - 1.0) * 100.0
    prev_5d = metrics.get("prev_5d")
    if price > 0 and isinstance(prev_5d, (int, float)) and prev_5d > 0:
        metrics["recent_5d_pct"] = (price / float(prev_5d) - 1.0) * 100.0
    return metrics


def _daily_amount(value: float, pct: float) -> float:
    ratio = pct / 100.0
    if abs(1.0 + ratio) <= 1e-9:
        return 0.0
    return value - value / (1.0 + ratio)


def _fmt_usd_compact(value: float) -> str:
    return f"{float(value):,.0f}" if abs(float(value)) >= 100 else f"{float(value):,.2f}"


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


def build_visualizations(
    rows: list[dict[str, Any]],
    balances: dict[str, float],
    value_cny_by_symbol: dict[str, float],
    fx: float,
) -> dict[str, Any]:
    row_by_symbol = {row["symbol"]: row for row in rows}
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS)
    usd_cash_cny = float(balances.get("cash_usd", 0.0)) * fx
    allocation_total_cny = usd_total_cny + usd_cash_cny
    satellite_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in SATELLITE_SYMBOLS)

    pnl_rank = []
    satellite_pnl_rank = []
    satellite_pnl_cny = 0.0
    for row in rows:
        sym = row["symbol"]
        if sym in SATELLITE_SYMBOLS:
            satellite_pnl_cny += float(row["pnl_cny"])
            satellite_pnl_rank.append(
                {
                    "symbol": sym,
                    "label": row["label"],
                    "pnl": row["pnl"],
                    "pnl_cny": row["pnl_cny"],
                    "currency": row["currency"],
                }
            )
        elif sym in ("VOO", "QQQ", "SGOV", "001015", "006382"):
            pnl_cny = float(row["pnl_cny"])
            if sym == "SGOV":
                pnl_cny += float(balances.get("sgov_dividend_usd", 0.0)) * fx
            pnl_rank.append(
                {
                    "symbol": sym,
                    "label": row["label"],
                    "pnl_cny": pnl_cny,
                    "currency": "CNY",
                }
            )
    pnl_rank.append({"symbol": "SATELLITE", "label": "卫星仓位", "pnl_cny": satellite_pnl_cny, "currency": "CNY"})
    pnl_rank.sort(key=lambda item: item["pnl_cny"], reverse=True)
    satellite_pnl_rank.sort(key=lambda item: item["pnl"], reverse=True)

    allocation_order = [
        ("VOO", "VOO", ["VOO"]),
        ("QQQ", "QQQ", ["QQQ"]),
        ("SATELLITE", "卫星仓位", list(SATELLITE_SYMBOLS)),
        ("SGOV", "短债(SGOV)", ["SGOV"]),
        ("CASH", "现金", []),
    ]
    target_map = {
        "VOO": TARGET_WEIGHTS["VOO"],
        "QQQ": TARGET_WEIGHTS["QQQ"],
        "SATELLITE": sum(TARGET_WEIGHTS[sym] for sym in SATELLITE_SYMBOLS),
        "SGOV": TARGET_WEIGHTS["SGOV"],
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

    satellite_weight_total = sum(TARGET_WEIGHTS[sym] for sym in SATELLITE_SYMBOLS)
    satellite_split = []
    for sym in SATELLITE_SYMBOLS:
        amount_cny = value_cny_by_symbol.get(sym, 0.0)
        row = row_by_symbol.get(sym, {})
        target_pct = TARGET_WEIGHTS[sym] / satellite_weight_total * 100.0 if satellite_weight_total > 0 else 0.0
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


def build_dashboard(user_id: str = "evan") -> dict[str, Any]:
    market = fetch_quotes()
    quotes = market["quotes"]
    fx = float(market["fx"]["rate"])
    holdings, balances, storage_mode = load_user_state(user_id)
    forward_pe = market.get("forward_pe", {})

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
        price = float(quote["price"])
        value = shares * price
        cost = shares * avg_cost
        value_cny = value * fx if meta["currency"] == "USD" else value
        cost_cny = cost * fx if meta["currency"] == "USD" else cost
        pnl = value - cost
        pnl_cny = value_cny - cost_cny
        total_value_cny += value_cny
        total_cost_cny += cost_cny
        value_cny_by_symbol[sym] = value_cny
        fpe = forward_pe.get(sym)
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
                "pnl_pct": pnl / cost * 100.0 if cost > 0 else 0.0,
                "daily_pct": float(quote.get("regular_change_pct", quote.get("change_pct", 0.0))),
                "effective_daily_pct": float(quote.get("change_pct", 0.0)),
                "extended_pct": quote.get("extended_change_pct"),
                "drawdown_pct": sixty_day.get("drawdown_pct"),
                "rebound_pct": sixty_day.get("rebound_pct"),
                "recent_5d_pct": sixty_day.get("recent_5d_pct"),
                "forward_pe": fpe,
                "pe_band": pe_band_text(sym) if sym in SATELLITE_SYMBOLS else "-",
                "pe_judgment": pe_judgment(sym, fpe),
            }
        )

    cash_usd = float(balances.get("cash_usd", 0.0))
    cash_cny = float(balances.get("cash_cny", 0.0))
    cash_total_cny = cash_usd * fx + cash_cny
    total_assets_cny = total_value_cny + cash_total_cny

    daily_cards = []
    card_by_symbol: dict[str, dict[str, Any]] = {}
    for sym in ("VOO", "QQQ", *SATELLITE_SYMBOLS, "SGOV", "001015", "006382"):
        quote = quotes[sym]
        holding = holdings[sym]
        shares = float(holding["shares"])
        currency = ASSET_META[sym]["currency"]
        regular_price = float(quote.get("regular_price") or quote.get("price") or 0.0)
        regular_value = shares * regular_price
        regular_value_cny = regular_value * fx if currency == "USD" else regular_value
        regular_pct = float(quote.get("regular_change_pct", quote.get("change_pct", 0.0)))
        change_cny = _daily_amount(regular_value_cny, regular_pct)
        extended_pct = quote.get("extended_change_pct") if quote.get("session") != "regular" else None
        extended_change_cny = None
        if isinstance(extended_pct, (int, float)):
            extended_change = regular_value * (float(extended_pct) / 100.0)
            extended_change_cny = extended_change * fx if currency == "USD" else extended_change
        card = {
                "symbol": sym,
                "label": ASSET_META[sym]["label"],
                "price_line": _quote_price_line(sym, quote),
                "regular_pct": regular_pct,
                "extended_pct": extended_pct,
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
        "extended_pct": satellite_extended_pct,
        "change_usd": satellite_change_cny / fx if fx > 0 else 0.0,
        "change_cny": satellite_change_cny,
        "extended_change_usd": satellite_extended_change_cny / fx if satellite_extended_change_cny is not None and fx > 0 else None,
        "extended_change_cny": satellite_extended_change_cny,
        "wide": True,
    }
    daily_cards.insert(2, satellite_card)

    weighted_daily_pct = (
        sum((value_cny_by_symbol[s] / total_value_cny) * float(quotes[s].get("change_pct", 0.0)) for s in ALL_SYMBOLS)
        if total_value_cny > 0
        else 0.0
    )
    weighted_daily_change_cny = sum(
        _daily_amount(value_cny_by_symbol.get(s, 0.0), float(quotes[s].get("change_pct", 0.0)))
        for s in ALL_SYMBOLS
    )

    return {
        "user_id": user_id,
        "storage_mode": storage_mode,
        "market": market,
        "holdings": rows,
        "balances": balances,
        "summary": {
            "fx": fx,
            "total_value_cny": total_value_cny,
            "total_cost_cny": total_cost_cny,
            "cash_total_cny": cash_total_cny,
            "total_assets_cny": total_assets_cny,
            "total_pnl_cny": total_value_cny - total_cost_cny,
            "total_pnl_pct": (total_value_cny - total_cost_cny) / total_cost_cny * 100.0 if total_cost_cny > 0 else 0.0,
            "weighted_daily_pct": weighted_daily_pct,
            "weighted_daily_change_cny": weighted_daily_change_cny,
            "weighted_daily_change_usd": weighted_daily_change_cny / fx if fx > 0 else 0.0,
        },
        "daily_cards": daily_cards,
        "visualizations": build_visualizations(rows, balances, value_cny_by_symbol, fx),
        "targets": TARGET_WEIGHTS,
        "rebalance": build_rebalance_v2(user_id, rows, balances, market, value_cny_by_symbol, fx),
    }


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

    usd_weight_total = sum(TARGET_WEIGHTS[s] for s in USD_SYMBOLS)
    cash_usd = float(balances.get("cash_usd", 0.0))
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS) + cash_usd * fx
    usd_total_usd = usd_total_cny / fx if fx > 0 else 0.0
    sgov_target_pct = TARGET_WEIGHTS["SGOV"] / usd_weight_total if usd_weight_total > 0 else 0.0
    sgov_current_usd = value_cny_by_symbol.get("SGOV", 0.0) / fx if fx > 0 else 0.0
    planned_total_usd = usd_total_usd + future_cash_total_usd
    planned_sgov_target_usd = sgov_target_pct * planned_total_usd
    sgov_excess_usd = max(0.0, sgov_current_usd - planned_sgov_target_usd)
    used_budget_usd = sum(float(v) for v in bought_amounts.values())

    rows: list[dict[str, Any]] = []
    full_rebalance_need_usd = 0.0
    has_large_trigger = False
    for item in holding_rows:
        sym = item["symbol"]
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        current_usd = item["value_cny"] / fx if fx > 0 else 0.0
        already = float(bought_amounts.get(sym, 0.0))
        already_sold = float(sold_amounts.get(sym, 0.0))
        planning_current = max(0.0, current_usd - already + already_sold)
        target_pct = TARGET_WEIGHTS[sym] / usd_weight_total if usd_weight_total > 0 else 0.0
        target_usd = target_pct * planned_total_usd
        gap = target_usd - planning_current
        drawdown_pct = item.get("drawdown_pct")
        multiplier, action, signal, intensity = signal_for_drawdown(sym, drawdown_pct, phase)
        if sym == "QQQ" and phase == REBALANCE_PHASE_BUILD and intensity_rank(intensity) < intensity_rank("small"):
            month_end_multiplier, month_end_action, month_end_signal, month_end_intensity = qqq_build_signal(drawdown_pct, now)
            if month_end_multiplier > multiplier:
                multiplier, action, signal, intensity = month_end_multiplier, month_end_action, month_end_signal, month_end_intensity
        previous = normalize_intensity(bought_intensities.get(sym, "none"))
        if intensity_rank(previous) > intensity_rank(intensity):
            multiplier, action, signal, intensity = signal_for_intensity(sym, phase, previous)
            action = f"保持本月已确认{INTENSITY_LABELS.get(previous, '已买')}档"

        if normalize_intensity(intensity) == "large":
            has_large_trigger = True

        base_budget = max(0.0, gap) / max(1, build_month_count)
        planned = min(max(0.0, gap), base_budget * multiplier)
        planned_formula_parts = [
            f"目标 {_fmt_usd_compact(target_usd)} - 月初 {_fmt_usd_compact(planning_current)} = 缺口 {_fmt_usd_compact(gap)}",
            f"缺口 / {build_month_count}月 = {_fmt_usd_compact(base_budget)}",
            f"x {float(multiplier):.2f}档 = {_fmt_usd_compact(base_budget * multiplier)}",
            f"取不超过缺口 => {_fmt_usd_compact(min(max(0.0, gap), base_budget * multiplier))}",
        ]
        if sym == "VOO" and phase == REBALANCE_PHASE_BUILD:
            planned_formula_parts.append(f"VOO 至少 1 股 => {_fmt_usd_compact(planned)}")
        planned_formula = "；".join(planned_formula_parts)

        previous_multiplier = intensity_multiplier(sym, phase, previous)
        additional_multiplier = max(0.0, float(multiplier) - previous_multiplier)
        already_bought_this_month = previous != "none"
        fpe = item.get("forward_pe")
        split, split_note = valuation_split_for_row(sym, item)
        net_bought = already - already_sold
        raw_planned = planned
        raw_suggested = min(max(0.0, gap), base_budget * additional_multiplier) * split
        if already_bought_this_month:
            raw_suggested = max(0.0, raw_planned * split - already)
            additional_multiplier = raw_suggested / base_budget if base_budget > 0 else 0.0
        if already_bought_this_month and raw_suggested <= 1e-9:
            raw_suggested = 0.0
            additional_multiplier = 0.0
        if gap <= 0 and sym != "VOO":
            raw_suggested = 0.0
            action = "暂不买入"

        note_parts: list[str] = []
        if raw_suggested > 0:
            note_parts.append("按目标缺口和当前阶段的月度推进节奏执行。")
        elif already > 0:
            note_parts.append(f"本月已买 USD {already:,.2f}，当前无需系统补买；仍可手动确认。")
        elif gap <= 0:
            note_parts.append("当前仓位已达到或高于目标，系统建议不买。")
        else:
            note_parts.append("当前可动用资金不足或档位额度已用完，先保留观察。")
        if split_note:
            note_parts.append(f"{split_note} 计划金额仍按建仓节奏买够。")
        if sym == "VOO" and phase == REBALANCE_PHASE_BUILD:
            note_parts.append("建仓期 VOO 至少按 1 股规划。")
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
                "planned_buy_usd": raw_planned,
                "raw_planned_buy_usd": raw_planned,
                "planned_buy_formula": planned_formula,
                "target_usd": target_usd,
                "base_budget_usd": base_budget,
                "signal_multiplier": float(multiplier),
                "suggested_buy_usd": raw_suggested,
                "raw_suggested_buy_usd": raw_suggested,
                "actual_bought_usd": already,
                "actual_sold_usd": already_sold,
                "net_bought_usd": net_bought,
                "planned_after_valuation_usd": raw_planned * split,
                "buy_difference_usd": raw_planned * split - net_bought,
                "month_start_value_usd": planning_current,
                "gap_usd": gap,
                "drawdown_pct": drawdown_pct,
                "recent_5d_pct": item.get("recent_5d_pct"),
                "target_pct": target_pct * 100.0,
                "current_pct": current_usd / planned_total_usd * 100.0 if planned_total_usd > 0 else 0.0,
                "forward_pe": fpe,
                "pe_band": pe_band_text(sym),
                "valuation_split_factor": split,
                "note": " ".join(note_parts),
            }
        )

    sgov_available_usd = sgov_current_usd if has_large_trigger else sgov_excess_usd
    deployable_pool_usd = cash_usd + sgov_available_usd
    remaining_deployable_usd = max(0.0, deployable_pool_usd - used_budget_usd)
    raw_total = sum(float(row["raw_suggested_buy_usd"]) for row in rows)
    monthly_budget_usd = min(deployable_pool_usd, full_rebalance_need_usd) / max(1, build_month_count)
    remaining_reference_budget_usd = max(0.0, monthly_budget_usd - used_budget_usd)
    suggested_run_budget_usd = min(remaining_deployable_usd, max(remaining_reference_budget_usd, raw_total))
    strategy_budget_usd = min(suggested_run_budget_usd, raw_total)
    scale = strategy_budget_usd / raw_total if raw_total > 0 else 0.0
    for row in rows:
        row["planned_buy_usd"] = row["raw_planned_buy_usd"]
        row["suggested_buy_usd"] = float(row["raw_suggested_buy_usd"]) * scale
        price = float(market["quotes"].get(row["symbol"], {}).get("price") or 0.0)
        row["suggested_buy_shares"] = row["suggested_buy_usd"] / price if price > 0 else 0.0

    return {
        "month_key": month_key,
        "planned_new_cash_usd": planned_new_cash_usd,
        "future_cash_by_month": future_cash_by_month,
        "future_cash_total_usd": future_cash_total_usd,
        "base_planned_total_usd": planned_total_usd,
        "build_target": f"{BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}",
        "build_months": build_month_count,
        "future_cash_months": future_cash_months,
        "deployable_pool_usd": deployable_pool_usd,
        "remaining_deployable_usd": remaining_deployable_usd,
        "monthly_budget_usd": monthly_budget_usd,
        "remaining_reference_budget_usd": remaining_reference_budget_usd,
        "strategy_budget_usd": strategy_budget_usd,
        "suggestion_scale": scale,
        "sgov_excess_usd": sgov_excess_usd,
        "sgov_available_usd": sgov_available_usd,
        "sgov_large_trigger_enabled": has_large_trigger,
        "planned_total_usd": planned_total_usd,
        "rules": rebalance_rules_payload(build_month_count, future_cash_months, planned_new_cash_usd, future_cash_total_usd),
        "usage": usage,
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
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS)
    usd_total_usd = usd_total_cny / fx if fx > 0 else 0.0
    planned_total_usd = usd_total_usd + planned_new_cash_usd * max(0, build_months - 1)
    usd_weight_total = sum(TARGET_WEIGHTS[s] for s in USD_SYMBOLS)
    rows = []
    for item in holding_rows:
        sym = item["symbol"]
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        current_usd = item["value_cny"] / fx if fx > 0 else 0.0
        already = float(bought_amounts.get(sym, 0.0))
        planning_current = max(0.0, current_usd - already)
        target_pct = TARGET_WEIGHTS[sym] / usd_weight_total if usd_weight_total > 0 else 0.0
        target_usd = target_pct * planned_total_usd
        gap = target_usd - planning_current
        drawdown = None
        multiplier, action, signal, intensity = signal_for_drawdown(sym, drawdown, phase)
        previous = normalize_intensity(bought_intensities.get(sym, "none"))
        if intensity_rank(previous) > intensity_rank(intensity):
            multiplier, action, signal, intensity = signal_for_intensity(sym, phase, previous)
            action = f"维持本月已确认{INTENSITY_LABELS.get(previous, '已买')}档"
        base_budget = max(0.0, gap) / max(1, build_months)
        planned = min(max(0.0, gap), base_budget * multiplier)
        fpe = item.get("forward_pe")
        band = PE_BANDS.get(sym)
        split = 0.5 if sym in SATELLITE_SYMBOLS and isinstance(fpe, (int, float)) and band and fpe > band[1] else 1.0
        suggested = max(0.0, planned * split - already)
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
