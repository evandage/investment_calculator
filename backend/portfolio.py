from __future__ import annotations

import time
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
                    "美元资产目标：VOO 40% / QQQ 20% / 卫星仓位 20% / 短债(SGOV) 20%。",
                    "卫星仓位内部目标：ISRG / GOOGL / MSFT / AVGO / NVDA = 5 / 3 / 2 / 3 / 2。",
                    "A股基金按 Dashboard 目标占比展示，但当前买入建议只处理美元标的。",
                ],
            },
            {
                "heading": "建仓期分母",
                "items": [
                    f"建仓期默认计算到 {BUILD_TARGET_YEAR}-{BUILD_TARGET_MONTH:02d}，当前共 {build_months} 个月。",
                    f"未来入金按下个月起计算，共 {future_cash_months} 个月，每月 USD {planned_new_cash_usd:,.2f}。",
                    "目标分母 = 当前美元资产 + 未来每月计划入金；当前月已到账资金请写入现金或持仓，避免重复计算。",
                ],
            },
            {
                "heading": "建议金额",
                "items": [
                    "先用月初口径持仓计算缺口：月初口径 = 当前持仓金额 - 本月已买金额。",
                    "再按档位倍率计算本月计划应买，VOO 建仓期至少按 1 股规划。",
                    "Forward PE 不改变计划应买金额，只给出估值分批系数；建仓期仍按计划买够。",
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
        "small": "small",
        "probe": "small",
        "medium": "medium",
        "large": "large",
    }.get(v, "none")


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


def fetch_60d_metrics(symbol: str, current_price: float | None = None) -> dict[str, float | None]:
    now = time.time()
    cached = _DRAWDOWN_CACHE.get(symbol)
    if cached and now - cached[1] < _DRAWDOWN_CACHE_TTL_SECONDS:
        metrics = dict(cached[0])
    else:
        metrics = {"drawdown_pct": None, "rebound_pct": None, "peak": None, "trough": None}
        try:
            import chart_boards

            chart_boards.configure_market_provider("tencent")
            df = chart_boards.fetch_ohlcv(symbol, "1d", "3mo", cache_only=False)
            if df is not None and not df.empty and "Close" in df.columns:
                closes = [float(v) for v in df["Close"].dropna().tail(60).tolist() if float(v) > 0]
                if closes:
                    peak = max(closes)
                    trough = min(closes)
                    last = closes[-1]
                    metrics = {
                        "drawdown_pct": (last / peak - 1.0) * 100.0 if peak > 0 else None,
                        "rebound_pct": (last / trough - 1.0) * 100.0 if trough > 0 else None,
                        "peak": peak,
                        "trough": trough,
                    }
        except Exception:
            metrics = {"drawdown_pct": None, "rebound_pct": None, "peak": None, "trough": None}
        _DRAWDOWN_CACHE[symbol] = (dict(metrics), now)

    price = float(current_price or 0.0)
    peak = metrics.get("peak")
    trough = metrics.get("trough")
    if price > 0 and isinstance(peak, (int, float)) and peak > 0:
        metrics["drawdown_pct"] = (price / float(peak) - 1.0) * 100.0
    if price > 0 and isinstance(trough, (int, float)) and trough > 0:
        metrics["rebound_pct"] = (price / float(trough) - 1.0) * 100.0
    return metrics


def _daily_amount(value: float, pct: float) -> float:
    ratio = pct / 100.0
    if abs(1.0 + ratio) <= 1e-9:
        return 0.0
    return value - value / (1.0 + ratio)


def _currency_value_to_cny(value: float, currency: str, fx: float) -> float:
    return value * fx if currency == "USD" else value


def _quote_price_line(symbol: str, quote: dict[str, Any]) -> str:
    currency = ASSET_META[symbol]["currency"]
    regular = float(quote.get("regular_price") or quote.get("price") or 0.0)
    effective = float(quote.get("price") or regular)
    decimals = 2 if currency == "USD" else 4
    text = f"{currency} {regular:,.{decimals}f}"
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
            pnl_rank.append(
                {
                    "symbol": sym,
                    "label": row["label"],
                    "pnl_cny": row["pnl_cny"],
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
        effective_price = float(quote.get("price") or regular_price)
        regular_value = shares * regular_price
        regular_value_cny = regular_value * fx if currency == "USD" else regular_value
        regular_pct = float(quote.get("regular_change_pct", quote.get("change_pct", 0.0)))
        change_cny = _daily_amount(regular_value_cny, regular_pct)
        extended_change = shares * (effective_price - regular_price) if quote.get("session") != "regular" else None
        extended_change_cny = (
            extended_change * fx if extended_change is not None and currency == "USD" else extended_change
        )
        card = {
                "symbol": sym,
                "label": ASSET_META[sym]["label"],
                "price_line": _quote_price_line(sym, quote),
                "regular_pct": regular_pct,
                "extended_pct": quote.get("extended_change_pct"),
                "change_usd": change_cny / fx if fx > 0 else 0.0,
                "change_cny": change_cny,
                "extended_change_usd": (
                    extended_change if currency == "USD" else extended_change_cny / fx
                )
                if extended_change_cny is not None and fx > 0
                else None,
                "extended_change_cny": extended_change_cny,
            }
        daily_cards.append(card)
        card_by_symbol[sym] = card

    satellite_value_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in SATELLITE_SYMBOLS)
    satellite_regular_pct = (
        sum(
            value_cny_by_symbol.get(sym, 0.0)
            / satellite_value_cny
            * float(quotes[sym].get("regular_change_pct", quotes[sym].get("change_pct", 0.0)))
            for sym in SATELLITE_SYMBOLS
        )
        if satellite_value_cny > 0
        else 0.0
    )
    satellite_extended_values = [card_by_symbol[sym].get("extended_pct") for sym in SATELLITE_SYMBOLS]
    satellite_extended_pct = (
        sum(
            value_cny_by_symbol.get(sym, 0.0) / satellite_value_cny * float(card_by_symbol[sym].get("extended_pct"))
            for sym in SATELLITE_SYMBOLS
            if isinstance(card_by_symbol[sym].get("extended_pct"), (int, float))
        )
        if satellite_value_cny > 0 and any(isinstance(v, (int, float)) for v in satellite_extended_values)
        else None
    )
    satellite_change_cny = sum(float(card_by_symbol[sym].get("change_cny") or 0.0) for sym in SATELLITE_SYMBOLS)
    satellite_extended_change_cny = (
        sum(float(card_by_symbol[sym].get("extended_change_cny") or 0.0) for sym in SATELLITE_SYMBOLS)
        if any(card_by_symbol[sym].get("extended_change_cny") is not None for sym in SATELLITE_SYMBOLS)
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
        "extended_change_usd": satellite_extended_change_cny / fx
        if satellite_extended_change_cny is not None and fx > 0
        else None,
        "extended_change_cny": satellite_extended_change_cny,
        "wide": True,
    }
    daily_cards.insert(2, satellite_card)

    weighted_daily_pct = (
        sum((value_cny_by_symbol[s] / total_value_cny) * float(quotes[s].get("change_pct", 0.0)) for s in ALL_SYMBOLS)
        if total_value_cny > 0
        else 0.0
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
    bought_intensities = {sym: normalize_intensity(v) for sym, v in usage.get("bought_intensity_by_symbol", {}).items()}

    usd_weight_total = sum(TARGET_WEIGHTS[s] for s in USD_SYMBOLS)
    usd_total_cny = sum(value_cny_by_symbol.get(sym, 0.0) for sym in USD_SYMBOLS)
    usd_total_usd = usd_total_cny / fx if fx > 0 else 0.0
    planned_total_usd = usd_total_usd + future_cash_total_usd
    sgov_target_pct = TARGET_WEIGHTS["SGOV"] / usd_weight_total if usd_weight_total > 0 else 0.0
    sgov_current_usd = value_cny_by_symbol.get("SGOV", 0.0) / fx if fx > 0 else 0.0
    planned_sgov_target_usd = sgov_target_pct * planned_total_usd
    sgov_excess_usd = max(0.0, sgov_current_usd - planned_sgov_target_usd)
    cash_usd = float(balances.get("cash_usd", 0.0))
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
        planning_current = max(0.0, current_usd - already)
        target_pct = TARGET_WEIGHTS[sym] / usd_weight_total if usd_weight_total > 0 else 0.0
        target_usd = target_pct * planned_total_usd
        gap = target_usd - planning_current
        drawdown_pct = item.get("drawdown_pct")
        multiplier, action, signal, intensity = signal_for_drawdown(sym, drawdown_pct, phase)
        previous = normalize_intensity(bought_intensities.get(sym, "none"))
        if INTENSITY_ORDER.get(previous, 0) > INTENSITY_ORDER.get(intensity, 0):
            multiplier, action, signal, intensity = signal_for_intensity(sym, phase, previous)
            action = f"保持本月已确认{INTENSITY_LABELS.get(previous, '已买')}档"

        if normalize_intensity(intensity) == "large":
            has_large_trigger = True

        base_budget = max(0.0, gap) / max(1, build_month_count)
        planned = min(max(0.0, gap), base_budget * multiplier)
        if sym == "VOO" and phase == REBALANCE_PHASE_BUILD:
            planned = max(planned, float(market["quotes"][sym]["price"]))

        previous_multiplier = intensity_multiplier(sym, phase, previous)
        additional_multiplier = max(0.0, float(multiplier) - previous_multiplier)
        already_bought_this_month = previous != "none"
        fpe = item.get("forward_pe")
        band = PE_BANDS.get(sym)
        split = 0.5 if sym in SATELLITE_SYMBOLS and isinstance(fpe, (int, float)) and band and fpe > band[1] else 1.0
        raw_planned = planned
        raw_suggested = min(max(0.0, gap), base_budget * additional_multiplier)
        if already_bought_this_month:
            raw_suggested = max(0.0, raw_planned - already)
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
        if split < 1.0:
            note_parts.append("Forward PE 高于合理区间，建议执行时分批，估值提示系数 0.50；计划金额仍按建仓节奏买够。")
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
                "suggested_buy_usd": raw_suggested,
                "raw_suggested_buy_usd": raw_suggested,
                "actual_bought_usd": already,
                "month_start_value_usd": planning_current,
                "gap_usd": gap,
                "drawdown_pct": drawdown_pct,
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
        if (
            row["symbol"] == "VOO"
            and phase == REBALANCE_PHASE_BUILD
            and float(row["raw_suggested_buy_usd"]) > 0
            and float(market["quotes"].get(row["symbol"], {}).get("price") or 0.0) > 0
        ):
            row["suggested_buy_usd"] = max(row["suggested_buy_usd"], float(market["quotes"][row["symbol"]]["price"]))
        price = float(market["quotes"].get(row["symbol"], {}).get("price") or 0.0)
        row["suggested_buy_shares"] = row["suggested_buy_usd"] / price if price > 0 else 0.0

    return {
        "month_key": month_key,
        "planned_new_cash_usd": planned_new_cash_usd,
        "future_cash_by_month": future_cash_by_month,
        "future_cash_total_usd": future_cash_total_usd,
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
        if INTENSITY_ORDER.get(previous, 0) > INTENSITY_ORDER.get(intensity, 0):
            multiplier, action, signal, intensity = signal_for_intensity(sym, phase, previous)
            action = f"维持本月已确认{INTENSITY_LABELS.get(previous, '已买')}档"
        base_budget = max(0.0, gap) / max(1, build_months)
        planned = min(max(0.0, gap), base_budget * multiplier)
        if sym == "VOO":
            planned = max(planned, float(market["quotes"][sym]["price"]))
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
    )
    return {"saved": True, "month_key": month_key, "planned_cash_by_month": clean}


def confirm_buys(user_id: str, executions: list[dict[str, Any]]) -> dict[str, Any]:
    holdings, balances, _ = load_user_state(user_id)
    now = datetime.now(TZ_SHANGHAI)
    month_key = now.strftime("%Y-%m")
    usage = load_monthly_usage(user_id, month_key)
    amounts = dict(usage.get("bought_amount_by_symbol", {}))
    intensities = dict(usage.get("bought_intensity_by_symbol", {}))
    total = 0.0
    for item in executions:
        sym = str(item.get("symbol", "")).upper()
        if sym not in USD_SYMBOLS or sym == "SGOV":
            continue
        amount = max(0.0, float(item.get("amount_usd", 0.0) or 0.0))
        shares = max(0.0, float(item.get("shares", 0.0) or 0.0))
        if amount <= 0 or shares <= 0:
            continue
        price = amount / shares
        old = holdings[sym]
        old_shares = float(old["shares"])
        old_cost = float(old["avg_cost"])
        new_shares = old_shares + shares
        holdings[sym] = {"shares": new_shares, "avg_cost": (old_shares * old_cost + shares * price) / new_shares}
        amounts[sym] = float(amounts.get(sym, 0.0)) + amount
        new_intensity = normalize_intensity(item.get("intensity", "normal"))
        old_intensity = normalize_intensity(intensities.get(sym, "none"))
        if INTENSITY_ORDER.get(new_intensity, 0) > INTENSITY_ORDER.get(old_intensity, 0):
            intensities[sym] = new_intensity
        total += amount
    balances["cash_usd"] = max(0.0, float(balances.get("cash_usd", 0.0)) - total)
    save_user_state(user_id, holdings, balances)
    save_monthly_usage(
        user_id,
        month_key,
        planned_new_cash_usd=float(usage["planned_new_cash_usd"]),
        planned_cash_by_month=dict(usage.get("planned_cash_by_month", {})),
        bought_amount_by_symbol=amounts,
        bought_intensity_by_symbol=intensities,
    )
    return {"saved": True, "total_usd": total, "month_key": month_key}
