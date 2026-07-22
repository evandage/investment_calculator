from __future__ import annotations

import json
import hashlib
from datetime import datetime
from typing import Any

from .config import (
    ALL_SYMBOLS,
    ASSET_META,
    BALANCES_FILE,
    CLOSED_SATELLITE_PNL_FILE,
    DEFAULT_SATELLITE_TARGET_PCTS,
    DRAWDOWN_EPISODES_FILE,
    FALLBACK_PRICES,
    HOLDINGS_FILE,
    FX_CONVERSION_RECORDS_FILE,
    MONTHLY_USAGE_FILE,
    PORTFOLIO_HISTORY_FILE,
    PORTFOLIO_SNAPSHOT_LEDGER_FILE,
    PORTFOLIO_ADJUSTMENTS_FILE,
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
        "cash_cost_basis_usd": 0.0,
        "cash_cost_basis_cny": 0.0,
        "realized_usd": 0.0,
        "realized_cny": 0.0,
        "voo_dividend_usd": 0.0,
        "sgov_dividend_usd": 0.0,
    }


def normalize_balances(raw: Any) -> dict[str, float]:
    out = default_balances()
    if not isinstance(raw, dict):
        return out
    for key in ("cash_usd", "cash_cny", "realized_usd", "realized_cny", "voo_dividend_usd", "sgov_dividend_usd"):
        try:
            value = float(raw.get(key, out[key]))
        except (TypeError, ValueError):
            value = out[key]
        out[key] = max(0.0, value) if key.startswith("cash_") or key.endswith("_dividend_usd") else value
    # Cash balance and principal basis are deliberately separate.  A basis can
    # be negative when previously realized profit has already been reinvested;
    # keeping that negative residual is what makes buys and sells basis-neutral.
    inferred_basis = {
        "cash_cost_basis_usd": (
            out["cash_usd"]
            - out["realized_usd"]
            - out["voo_dividend_usd"]
            - out["sgov_dividend_usd"]
        ),
        "cash_cost_basis_cny": out["cash_cny"] - out["realized_cny"],
    }
    for key, fallback in inferred_basis.items():
        try:
            out[key] = float(raw[key]) if key in raw else fallback
        except (TypeError, ValueError):
            out[key] = fallback
    return out


def load_balances() -> dict[str, float]:
    return normalize_balances(_read_json(BALANCES_FILE, {}))


def save_balances(balances: dict[str, float]) -> None:
    _write_json(BALANCES_FILE, normalize_balances(balances))


def load_portfolio_adjustments(user_id: str = "evan") -> list[dict[str, Any]]:
    raw = _read_json(PORTFOLIO_ADJUSTMENTS_FILE, {})
    rows = raw.get(str(user_id or "local"), []) if isinstance(raw, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def record_portfolio_adjustment(
    user_id: str,
    kind: str,
    effective_date: str,
    before: dict[str, Any],
    after: dict[str, Any],
    reason: str = "manual_edit",
    metadata: dict[str, Any] | None = None,
    allow_noop: bool = False,
) -> dict[str, Any] | None:
    if before == after and not allow_noop:
        return None
    raw = _read_json(PORTFOLIO_ADJUSTMENTS_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local")
    rows = store.get(user_key, [])
    if not isinstance(rows, list):
        rows = []
    recorded_at = datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")
    record = {
        "id": f"adj-{recorded_at.replace(':', '').replace('-', '')}-{len(rows) + 1}",
        "kind": str(kind),
        "effective_date": str(effective_date)[:10],
        "recorded_at": recorded_at,
        "reason": str(reason),
        "before": before,
        "after": after,
    }
    if metadata:
        record.update(dict(metadata))
    rows.append(record)
    store[user_key] = rows
    _write_json(PORTFOLIO_ADJUSTMENTS_FILE, store)
    return record


def load_portfolio_snapshot_ledger(user_id: str = "evan") -> list[dict[str, Any]]:
    raw = _read_json(PORTFOLIO_SNAPSHOT_LEDGER_FILE, {})
    rows = raw.get(str(user_id or "local"), []) if isinstance(raw, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _snapshot_checksum(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sync_snapshot_ledger(user_id: str, rows: list[dict[str, Any]]) -> None:
    raw = _read_json(PORTFOLIO_SNAPSHOT_LEDGER_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local")
    ledger = store.get(user_key, [])
    if not isinstance(ledger, list):
        ledger = []
    changed = False
    for row in rows:
        if not isinstance(row, dict) or not row.get("finalized") or not row.get("date"):
            continue
        payload = json.loads(json.dumps(row, ensure_ascii=False))
        checksum = _snapshot_checksum(payload)
        same_day = [item for item in ledger if isinstance(item, dict) and item.get("date") == row["date"]]
        latest = max(same_day, key=lambda item: int(item.get("revision", 0) or 0), default=None)
        if latest and latest.get("checksum") == checksum:
            continue
        revision = int((latest or {}).get("revision", 0) or 0) + 1
        recorded_at = datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")
        ledger.append(
            {
                "snapshot_id": f"{row['date']}-r{revision}",
                "date": row["date"],
                "revision": revision,
                "recorded_at": recorded_at,
                "reason": "legacy_import" if latest is None else "recalculation",
                "supersedes_snapshot_id": (latest or {}).get("snapshot_id"),
                "checksum": checksum,
                "payload": payload,
            }
        )
        changed = True
    if changed:
        store[user_key] = ledger
        _write_json(PORTFOLIO_SNAPSHOT_LEDGER_FILE, store)


def replace_snapshot_ledger_with_corrected_history(
    user_id: str,
    rows: list[dict[str, Any]],
    reason: str = "corrected_history_baseline",
) -> dict[str, Any]:
    """Replace one user's ledger with corrected finalized snapshots.

    This is an explicit maintenance operation, not the normal append-only save
    path.  Preserve the previous complete ledger beside the live file before
    installing one corrected baseline revision per date.
    """
    raw = _read_json(PORTFOLIO_SNAPSHOT_LEDGER_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    timestamp = datetime.now(TZ_SHANGHAI).strftime("%Y%m%d-%H%M%S")
    backup_path = PORTFOLIO_SNAPSHOT_LEDGER_FILE.with_name(
        f"{PORTFOLIO_SNAPSHOT_LEDGER_FILE.stem}.backup-{timestamp}{PORTFOLIO_SNAPSHOT_LEDGER_FILE.suffix}"
    )
    if PORTFOLIO_SNAPSHOT_LEDGER_FILE.exists():
        _write_json(backup_path, store)

    recorded_at = datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")
    corrected: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("date") or "")):
        if not isinstance(row, dict) or not row.get("finalized") or not row.get("date"):
            continue
        payload = json.loads(json.dumps(row, ensure_ascii=False))
        day = str(row["date"])[:10]
        corrected.append(
            {
                "snapshot_id": f"{day}-corrected-r1",
                "date": day,
                "revision": 1,
                "recorded_at": recorded_at,
                "reason": str(reason),
                "supersedes_snapshot_id": None,
                "checksum": _snapshot_checksum(payload),
                "payload": payload,
            }
        )
    user_key = str(user_id or "local")
    store[user_key] = corrected
    _write_json(PORTFOLIO_SNAPSHOT_LEDGER_FILE, store)
    return {
        "user_id": user_key,
        "snapshot_count": len(corrected),
        "backup_path": str(backup_path) if backup_path.exists() else None,
    }


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


def normalize_closed_satellite_pnl(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for raw_symbol, item in raw.items():
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or not isinstance(item, dict):
            continue
        try:
            pnl_usd = float(item.get("pnl_usd", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        out[symbol] = {
            "symbol": symbol,
            "label": str(item.get("label") or symbol),
            "pnl_usd": pnl_usd,
            "closed_at": str(item.get("closed_at") or ""),
            "included_in_realized": bool(item.get("included_in_realized", False)),
        }
    return out


def load_closed_satellite_pnl() -> dict[str, dict[str, Any]]:
    return normalize_closed_satellite_pnl(_read_json(CLOSED_SATELLITE_PNL_FILE, {}))


def save_closed_satellite_pnl(rows: dict[str, dict[str, Any]]) -> None:
    _write_json(CLOSED_SATELLITE_PNL_FILE, normalize_closed_satellite_pnl(rows))


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


def load_drawdown_episode_store(user_id: str = "evan") -> dict[str, Any]:
    raw = _read_json(DRAWDOWN_EPISODES_FILE, {})
    user_key = str(user_id or "local").strip() or "local"
    user_state = raw.get(user_key, {}) if isinstance(raw, dict) else {}
    if not isinstance(user_state, dict):
        user_state = {}
    snapshots = user_state.get("threshold_snapshots", {})
    preferred = user_state.get("preferred_threshold_snapshots", {})
    recalculations = user_state.get("monthly_recalculations", {})
    episodes = user_state.get("episodes", {})
    return {
        "threshold_snapshots": dict(snapshots) if isinstance(snapshots, dict) else {},
        "preferred_threshold_snapshots": dict(preferred) if isinstance(preferred, dict) else {},
        "monthly_recalculations": dict(recalculations) if isinstance(recalculations, dict) else {},
        "episodes": dict(episodes) if isinstance(episodes, dict) else {},
        "updated_at": str(user_state.get("updated_at") or ""),
    }


def save_drawdown_episode_store(user_id: str, state: dict[str, Any]) -> None:
    raw = _read_json(DRAWDOWN_EPISODES_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local").strip() or "local"
    store[user_key] = {
        "threshold_snapshots": dict(state.get("threshold_snapshots") or {}),
        "preferred_threshold_snapshots": dict(state.get("preferred_threshold_snapshots") or {}),
        "monthly_recalculations": dict(state.get("monthly_recalculations") or {}),
        "episodes": dict(state.get("episodes") or {}),
        "updated_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
    }
    _write_json(DRAWDOWN_EPISODES_FILE, store)


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
        if "holding_pnl_pct" in item:
            try:
                clean["holding_pnl_pct"] = float(item.get("holding_pnl_pct", 0.0))
            except (TypeError, ValueError):
                clean["holding_pnl_pct"] = None
        if "holding_daily_pnl_pct" in item:
            try:
                clean["holding_daily_pnl_pct"] = float(item.get("holding_daily_pnl_pct", 0.0))
            except (TypeError, ValueError):
                clean["holding_daily_pnl_pct"] = None
        for key in ("security_daily_pnl_pct", "total_daily_pnl_pct"):
            if key in item:
                try:
                    clean[key] = float(item.get(key, 0.0))
                except (TypeError, ValueError):
                    clean[key] = None
        for key in (
            "holding_pnl_cny",
            "holding_cost_cny",
            "total_pnl_cny",
            "total_return_basis_cny",
            "fx_pnl_cny",
            "realized_pnl_cny",
            "realized_usd",
            "realized_cny",
            "fx_rate",
            "cash_usd",
            "cash_cny",
            "cash_cost_basis_usd",
            "cash_cost_basis_cny",
            "holding_daily_pnl_cny",
            "holding_daily_basis_cny",
            "security_daily_pnl_cny",
            "security_daily_basis_cny",
            "total_daily_pnl_cny",
            "total_daily_basis_cny",
            "usd_return_pct",
            "usd_pnl_usd",
            "usd_cost_usd",
            "usd_value_usd",
            "usd_daily_pct",
            "usd_daily_pnl_usd",
            "usd_daily_basis_usd",
            "cash_flow_cny",
        ):
            if key in item:
                try:
                    clean[key] = float(item.get(key, 0.0))
                except (TypeError, ValueError):
                    clean[key] = 0.0
        if "cash_flow_flag" in item:
            clean["cash_flow_flag"] = bool(item.get("cash_flow_flag"))
        if "pnl_basis_version" in item:
            try:
                clean["pnl_basis_version"] = int(item.get("pnl_basis_version", 0))
            except (TypeError, ValueError):
                clean["pnl_basis_version"] = 0
        for key in ("snapshot_schema_version",):
            if key in item:
                try:
                    clean[key] = int(item.get(key, 0))
                except (TypeError, ValueError):
                    clean[key] = 0
        for key in ("calculation_version", "price_source", "fx_source", "revised_at"):
            if key in item:
                clean[key] = str(item.get(key) or "")
        for key in ("voo_dividend_usd", "sgov_dividend_usd"):
            if key in item:
                try:
                    clean[key] = float(item.get(key, 0.0) or 0.0)
                except (TypeError, ValueError):
                    clean[key] = 0.0
        closing_prices: dict[str, float] = {}
        for sym, value in (item.get("closing_prices") or {}).items():
            try:
                closing_prices[str(sym).upper()] = float(value)
            except (TypeError, ValueError):
                continue
        if closing_prices:
            clean["closing_prices"] = closing_prices
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
        for field in ("symbol_daily_pct", "symbol_market_pct", "symbol_position_pct"):
            normalized_pct: dict[str, float] = {}
            raw_pct = item.get(field, {})
            if isinstance(raw_pct, dict):
                for sym, value in raw_pct.items():
                    try:
                        normalized_pct[str(sym).upper()] = float(value)
                    except (TypeError, ValueError):
                        continue
            clean[field] = normalized_pct
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
    _sync_snapshot_ledger(user_id, rows)


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
            cost_basis = float(item.get("cost_basis", 0.0) or 0.0)
            realized_pnl = float(item.get("realized_pnl", 0.0) or 0.0)
            prev_avg_cost = float(item.get("prev_avg_cost", 0.0) or 0.0)
            new_avg_cost = float(item.get("new_avg_cost", 0.0) or 0.0)
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
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "prev_avg_cost": prev_avg_cost,
                "new_avg_cost": new_avg_cost,
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
            cost_basis = float(item.get("cost_basis", 0.0) or 0.0)
            realized_pnl = float(item.get("realized_pnl", 0.0) or 0.0)
            prev_avg_cost = float(item.get("prev_avg_cost", 0.0) or 0.0)
            new_avg_cost = float(item.get("new_avg_cost", 0.0) or 0.0)
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
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "prev_avg_cost": prev_avg_cost,
                "new_avg_cost": new_avg_cost,
                "intensity": str(item.get("intensity") or "normal"),
                "created_at": str(item.get("created_at") or ""),
            }
        )
    return normalized


def load_fx_conversion_records(user_id: str = "evan") -> list[dict[str, Any]]:
    raw = _read_json(FX_CONVERSION_RECORDS_FILE, {})
    user_key = str(user_id or "local").strip() or "local"
    rows = raw.get(user_key, []) if isinstance(raw, dict) else []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        converted_date = str(item.get("converted_date") or item.get("date") or "").strip()[:10]
        if not converted_date:
            continue
        try:
            cny_amount = max(0.0, float(item.get("cny_amount", 0.0) or 0.0))
            usd_amount = max(0.0, float(item.get("usd_amount", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
        if cny_amount <= 0 or usd_amount <= 0:
            continue
        out.append(
            {
                "id": str(item.get("id") or f"{converted_date}-{len(out)}"),
                "converted_date": converted_date,
                "cny_amount": cny_amount,
                "usd_amount": usd_amount,
                "rate": cny_amount / usd_amount,
                "note": str(item.get("note") or ""),
                "created_at": str(item.get("created_at") or ""),
            }
        )
    return sorted(out, key=lambda row: (row["converted_date"], row["created_at"], row["id"]))


def save_fx_conversion_records(user_id: str, rows: list[dict[str, Any]]) -> None:
    raw = _read_json(FX_CONVERSION_RECORDS_FILE, {})
    store = raw if isinstance(raw, dict) else {}
    user_key = str(user_id or "local").strip() or "local"
    store[user_key] = load_fx_conversion_records_from_rows(rows)
    _write_json(FX_CONVERSION_RECORDS_FILE, store)


def load_fx_conversion_records_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        converted_date = str(item.get("converted_date") or item.get("date") or "").strip()[:10]
        if not converted_date:
            continue
        try:
            cny_amount = max(0.0, float(item.get("cny_amount", 0.0) or 0.0))
            usd_amount = max(0.0, float(item.get("usd_amount", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
        if cny_amount <= 0 or usd_amount <= 0:
            continue
        normalized.append(
            {
                "id": str(item.get("id") or f"{converted_date}-{len(normalized)}"),
                "converted_date": converted_date,
                "cny_amount": cny_amount,
                "usd_amount": usd_amount,
                "rate": cny_amount / usd_amount,
                "note": str(item.get("note") or ""),
                "created_at": str(item.get("created_at") or ""),
            }
        )
    return normalized
