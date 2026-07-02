from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .config import (
    ALL_SYMBOLS,
    ASSET_META,
    BALANCES_FILE,
    DEFAULT_SATELLITE_TARGET_PCTS,
    FALLBACK_PRICES,
    HOLDINGS_FILE,
    MONTHLY_USAGE_FILE,
    PORTFOLIO_HISTORY_FILE,
    SATELLITE_SYMBOLS,
    SATELLITE_TARGETS_FILE,
    TZ_SHANGHAI,
    TRADE_RECORDS_FILE,
)


def _read_json(path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def default_holdings() -> dict[str, dict[str, float]]:
    return {sym: {"shares": 0.0, "avg_cost": float(FALLBACK_PRICES[sym])} for sym in ALL_SYMBOLS}


def normalize_holdings(raw: Any) -> dict[str, dict[str, float]]:
    out = default_holdings()
    if not isinstance(raw, dict):
        return out
    for sym in ALL_SYMBOLS:
        item = raw.get(sym, {})
        if not isinstance(item, dict):
            continue
        try:
            out[sym] = {
                "shares": max(0.0, float(item.get("shares", 0.0))),
                "avg_cost": max(0.0, float(item.get("avg_cost", out[sym]["avg_cost"]))),
            }
        except (TypeError, ValueError):
            continue
    return out


def load_holdings() -> dict[str, dict[str, float]]:
    return normalize_holdings(_read_json(HOLDINGS_FILE, {}))


def save_holdings(holdings: dict[str, dict[str, float]]) -> None:
    _write_json(HOLDINGS_FILE, normalize_holdings(holdings))


def default_balances() -> dict[str, float]:
    return {
        "cash_usd": 0.0,
        "cash_cny": 0.0,
        "realized_usd": 0.0,
        "realized_cny": 0.0,
        "sgov_dividend_usd": 0.0,
    }


def normalize_balances(raw: Any) -> dict[str, float]:
    out = default_balances()
    if not isinstance(raw, dict):
        return out
    for key in out:
        try:
            value = float(raw.get(key, out[key]))
        except (TypeError, ValueError):
            value = out[key]
        out[key] = max(0.0, value) if key.startswith("cash_") or key == "sgov_dividend_usd" else value
    return out


def load_balances() -> dict[str, float]:
    return normalize_balances(_read_json(BALANCES_FILE, {}))


def save_balances(balances: dict[str, float]) -> None:
    _write_json(BALANCES_FILE, normalize_balances(balances))


def normalize_satellite_targets(raw: Any) -> dict[str, float]:
    out = dict(DEFAULT_SATELLITE_TARGET_PCTS)
    if isinstance(raw, dict):
        for sym in SATELLITE_SYMBOLS:
            try:
                value = float(raw.get(sym, out.get(sym, 0.0)))
            except (TypeError, ValueError):
                continue
            out[sym] = max(0.0, value)
    total = sum(out.values())
    if total <= 0:
        return dict(DEFAULT_SATELLITE_TARGET_PCTS)
    return {sym: value / total * 100.0 for sym, value in out.items() if sym in SATELLITE_SYMBOLS}


def load_satellite_targets() -> dict[str, float]:
    return normalize_satellite_targets(_read_json(SATELLITE_TARGETS_FILE, {}))


def save_satellite_targets(targets: dict[str, float]) -> None:
    _write_json(SATELLITE_TARGETS_FILE, normalize_satellite_targets(targets))


def load_user_state(_: str = "evan") -> tuple[dict[str, dict[str, float]], dict[str, float], str]:
    return load_holdings(), load_balances(), "local"


def save_user_state(_: str, holdings: dict[str, dict[str, float]], balances: dict[str, float]) -> str:
    save_holdings(holdings)
    save_balances(balances)
    return "local"


def load_monthly_usage_store() -> dict[str, Any]:
    raw = _read_json(MONTHLY_USAGE_FILE, {})
    return raw if isinstance(raw, dict) else {}


def load_monthly_usage(user_id: str, month_key: str) -> dict[str, Any]:
    store = load_monthly_usage_store()
    user_key = str(user_id or "local").strip() or "local"
    raw_user = store.get(user_key)
    raw = raw_user.get(month_key, {}) if isinstance(raw_user, dict) else {}
    out = {
        "used_budget_usd": 0.0,
        "planned_new_cash_usd": 700.0,
        "planned_cash_by_month": {},
        "bought_symbols": [],
        "bought_amount_by_symbol": {},
        "sold_amount_by_symbol": {},
        "bought_intensity_by_symbol": {},
        "updated_at": "",
    }
    if not isinstance(raw, dict):
        return out
    try:
        out["planned_new_cash_usd"] = max(0.0, float(raw.get("planned_new_cash_usd", 700.0)))
    except (TypeError, ValueError):
        pass
    month_budget: dict[str, float] = {}
    for month, amount in (raw.get("planned_cash_by_month") or {}).items():
        key = str(month).strip()
        if not key:
            continue
        try:
            month_budget[key] = max(0.0, float(amount))
        except (TypeError, ValueError):
            continue
    out["planned_cash_by_month"] = month_budget
    amounts: dict[str, float] = {}
    for sym, amount in (raw.get("bought_amount_by_symbol") or {}).items():
        usym = str(sym).upper()
        if usym not in ASSET_META or ASSET_META[usym]["currency"] != "USD":
            continue
        try:
            value = max(0.0, float(amount))
        except (TypeError, ValueError):
            continue
        if value > 0:
            amounts[usym] = value
    out["bought_amount_by_symbol"] = amounts
    sold_amounts: dict[str, float] = {}
    for sym, amount in (raw.get("sold_amount_by_symbol") or {}).items():
        usym = str(sym).upper()
        if usym not in ASSET_META or ASSET_META[usym]["currency"] != "USD":
            continue
        try:
            value = max(0.0, float(amount))
        except (TypeError, ValueError):
            continue
        if value > 0:
            sold_amounts[usym] = value
    out["sold_amount_by_symbol"] = sold_amounts
    intensities = {}
    for sym, intensity in (raw.get("bought_intensity_by_symbol") or {}).items():
        usym = str(sym).upper()
        if usym in ASSET_META and ASSET_META[usym]["currency"] == "USD":
            intensities[usym] = str(intensity or "none")
    out["bought_intensity_by_symbol"] = intensities
    symbols = set(str(s).upper() for s in raw.get("bought_symbols", []) if str(s).upper() in ASSET_META)
    symbols |= set(amounts) | set(intensities)
    out["bought_symbols"] = sorted(symbols)
    out["used_budget_usd"] = sum(amounts.values()) if amounts else max(0.0, float(raw.get("used_budget_usd", 0.0) or 0.0))
    out["updated_at"] = str(raw.get("updated_at", ""))
    return out


def save_monthly_usage(
    user_id: str,
    month_key: str,
    *,
    planned_new_cash_usd: float,
    planned_cash_by_month: dict[str, float] | None = None,
    bought_amount_by_symbol: dict[str, float],
    bought_intensity_by_symbol: dict[str, str],
    sold_amount_by_symbol: dict[str, float] | None = None,
) -> None:
    store = load_monthly_usage_store()
    user_key = str(user_id or "local").strip() or "local"
    user_store = store.get(user_key)
    if not isinstance(user_store, dict):
        user_store = {}
    clean_amounts = {
        str(sym).upper(): max(0.0, float(amount))
        for sym, amount in bought_amount_by_symbol.items()
        if str(sym).upper() in ASSET_META and ASSET_META[str(sym).upper()]["currency"] == "USD" and float(amount) > 0
    }
    clean_intensity = {
        str(sym).upper(): str(intensity or "none")
        for sym, intensity in bought_intensity_by_symbol.items()
        if str(sym).upper() in ASSET_META and ASSET_META[str(sym).upper()]["currency"] == "USD" and str(intensity or "none") != "none"
    }
    clean_sold_amounts = {
        str(sym).upper(): max(0.0, float(amount))
        for sym, amount in (sold_amount_by_symbol or {}).items()
        if str(sym).upper() in ASSET_META and ASSET_META[str(sym).upper()]["currency"] == "USD" and float(amount) > 0
    }
    clean_month_budget = {
        str(month): max(0.0, float(amount))
        for month, amount in (planned_cash_by_month or {}).items()
        if str(month).strip()
    }
    symbols = sorted(set(clean_amounts) | set(clean_intensity))
    user_store[month_key] = {
        "used_budget_usd": sum(clean_amounts.values()),
        "planned_new_cash_usd": max(0.0, float(planned_new_cash_usd)),
        "planned_cash_by_month": clean_month_budget,
        "bought_symbols": symbols,
        "bought_amount_by_symbol": clean_amounts,
        "sold_amount_by_symbol": clean_sold_amounts,
        "bought_intensity_by_symbol": clean_intensity,
        "updated_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
    }
    store[user_key] = user_store
    _write_json(MONTHLY_USAGE_FILE, store)


def load_portfolio_history(user_id: str = "evan") -> list[dict[str, Any]]:
    raw = _read_json(PORTFOLIO_HISTORY_FILE, {})
    user_key = str(user_id or "local").strip() or "local"
    rows = raw.get(user_key, []) if isinstance(raw, dict) else []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        date = str(item.get("date", "")).strip()
        if not date:
            continue
        clean: dict[str, Any] = {"date": date}
        for key in ("portfolio_return_pct", "portfolio_daily_pct", "total_assets_cny", "total_cost_cny"):
            try:
                clean[key] = float(item.get(key, 0.0))
            except (TypeError, ValueError):
                clean[key] = 0.0
        benchmarks: dict[str, float] = {}
        raw_benchmarks = item.get("benchmark_prices", {})
        if isinstance(raw_benchmarks, dict):
            for sym, value in raw_benchmarks.items():
                try:
                    price = float(value)
                except (TypeError, ValueError):
                    continue
                if price > 0:
                    benchmarks[str(sym).upper()] = price
        clean["benchmark_prices"] = benchmarks
        benchmark_daily_pct: dict[str, float] = {}
        raw_daily_pct = item.get("benchmark_daily_pct", {})
        if isinstance(raw_daily_pct, dict):
            for sym, value in raw_daily_pct.items():
                try:
                    benchmark_daily_pct[str(sym).upper()] = float(value)
                except (TypeError, ValueError):
                    continue
        clean["benchmark_daily_pct"] = benchmark_daily_pct
        symbol_daily_pct: dict[str, float] = {}
        raw_symbol_daily_pct = item.get("symbol_daily_pct", {})
        if isinstance(raw_symbol_daily_pct, dict):
            for sym, value in raw_symbol_daily_pct.items():
                try:
                    symbol_daily_pct[str(sym).upper()] = float(value)
                except (TypeError, ValueError):
                    continue
        clean["symbol_daily_pct"] = symbol_daily_pct
        holdings_snapshot: dict[str, dict[str, float]] = {}
        raw_holdings_snapshot = item.get("holdings_snapshot", {})
        if isinstance(raw_holdings_snapshot, dict):
            for sym, value in raw_holdings_snapshot.items():
                if not isinstance(value, dict):
                    continue
                try:
                    shares = max(0.0, float(value.get("shares", 0.0)))
                    avg_cost = max(0.0, float(value.get("avg_cost", 0.0)))
                except (TypeError, ValueError):
                    continue
                if shares > 0:
                    holdings_snapshot[str(sym).upper()] = {"shares": shares, "avg_cost": avg_cost}
        clean["holdings_snapshot"] = holdings_snapshot
        clean["finalized"] = bool(item.get("finalized", False))
        estimated_symbols = item.get("estimated_symbols", [])
        clean["estimated_symbols"] = [
            str(sym).upper()
            for sym in estimated_symbols
            if isinstance(estimated_symbols, list) and str(sym).strip()
        ]
        clean["updated_at"] = str(item.get("updated_at", ""))
        out.append(clean)
    return sorted(out, key=lambda row: row["date"])


def save_portfolio_history(user_id: str, rows: list[dict[str, Any]]) -> None:
    raw = _read_json(PORTFOLIO_HISTORY_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local").strip() or "local"
    store[user_key] = rows
    _write_json(PORTFOLIO_HISTORY_FILE, store)


def load_trade_records(user_id: str = "evan") -> list[dict[str, Any]]:
    raw = _read_json(TRADE_RECORDS_FILE, {})
    user_key = str(user_id or "local").strip() or "local"
    rows = raw.get(user_key, []) if isinstance(raw, dict) else []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        if symbol not in ASSET_META:
            continue
        action = str(item.get("action", "buy")).lower()
        if action not in {"buy", "sell"}:
            continue
        try:
            shares = max(0.0, float(item.get("shares", 0.0)))
            amount = max(0.0, float(item.get("amount_usd", item.get("amount", 0.0))))
        except (TypeError, ValueError):
            continue
        if shares <= 0 or amount <= 0:
            continue
        trade_date = str(item.get("trade_date") or item.get("date") or "").strip()
        if not trade_date:
            continue
        out.append(
            {
                "id": str(item.get("id") or f"{trade_date}-{symbol}-{action}-{len(out)}"),
                "trade_date": trade_date[:10],
                "symbol": symbol,
                "action": action,
                "amount_usd": amount,
                "shares": shares,
                "price": amount / shares if shares > 0 else 0.0,
                "intensity": str(item.get("intensity") or "normal"),
                "created_at": str(item.get("created_at") or ""),
            }
        )
    return sorted(out, key=lambda row: (row["trade_date"], row["created_at"], row["id"]))


def save_trade_records(user_id: str, rows: list[dict[str, Any]]) -> None:
    raw = _read_json(TRADE_RECORDS_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local").strip() or "local"
    store[user_key] = load_trade_records_from_rows(rows)
    _write_json(TRADE_RECORDS_FILE, store)


def load_trade_records_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        action = str(item.get("action", "buy")).lower()
        if symbol not in ASSET_META or action not in {"buy", "sell"}:
            continue
        try:
            shares = max(0.0, float(item.get("shares", 0.0)))
            amount = max(0.0, float(item.get("amount_usd", item.get("amount", 0.0))))
        except (TypeError, ValueError):
            continue
        trade_date = str(item.get("trade_date") or item.get("date") or "").strip()[:10]
        if shares <= 0 or amount <= 0 or not trade_date:
            continue
        normalized.append(
            {
                "id": str(item.get("id") or f"{trade_date}-{symbol}-{action}-{len(normalized)}"),
                "trade_date": trade_date,
                "symbol": symbol,
                "action": action,
                "amount_usd": amount,
                "shares": shares,
                "price": amount / shares if shares > 0 else 0.0,
                "intensity": str(item.get("intensity") or "normal"),
                "created_at": str(item.get("created_at") or ""),
            }
        )
    return normalized
