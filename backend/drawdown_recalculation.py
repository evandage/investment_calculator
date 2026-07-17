from __future__ import annotations

import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable

from analysis import drawdown_thresholds as thresholds_analysis

from .config import REBALANCE_RULES, TZ_SHANGHAI
from .storage import load_drawdown_episode_store, save_drawdown_episode_store


MONTHLY_QUANTILES = (0.65, 0.85, 0.95)
TIER_NAMES = ("small", "medium", "large")
_RECALCULATION_LOCK = threading.Lock()


def previous_month_end(month_key: str) -> date:
    year, month = (int(part) for part in month_key.split("-"))
    first = date(year, month, 1)
    return first - timedelta(days=1)


def current_month_key(now: datetime | None = None) -> str:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    return current.astimezone(TZ_SHANGHAI).strftime("%Y-%m")


def _pct_thresholds(values: list[float] | None) -> dict[str, float]:
    if not values:
        return {}
    return {tier: -round(float(value) * 100.0, 1) for tier, value in zip(TIER_NAMES, values)}


def _threshold_ci_pct(values: list[list[float]]) -> dict[str, list[float]]:
    output: dict[str, list[float]] = {}
    for tier, interval in zip(TIER_NAMES, values or []):
        if len(interval) != 2:
            continue
        low, high = float(interval[0]), float(interval[1])
        output[tier] = [-round(high * 100.0, 2), -round(low * 100.0, 2)]
    return output


def build_validation_summary(result: dict[str, Any]) -> dict[str, Any]:
    """Promote only decision-relevant validation failures to the main UI."""
    diagnostics = list(dict.fromkeys(result.get("warnings") or []))
    mode = str(result.get("execution_mode") or "automatic")
    if mode == "manual_review_only":
        return {
            "status": "review",
            "alerts": [],
            "review_message": "历史与同行样本不足，继续人工复核",
            "diagnostics": diagnostics,
            "diagnostic_count": len(diagnostics),
        }

    statistics = (result.get("walk_forward") or {}).get("statistics") or {}
    alerts: list[str] = []
    insufficient_tiers: list[str] = []
    tier_labels = {"small": "小加", "medium": "中加", "large": "大加"}
    for tier in TIER_NAMES:
        stats = statistics.get(tier) or {}
        sample_count = int(stats.get("sample_count") or 0)
        # A rare large tier is expected to have few observations, but fewer
        # than three independent events cannot support an outcome judgment.
        if sample_count < 3:
            insufficient_tiers.append(tier_labels[tier])
            continue
        medians = stats.get("forward_return_median_pct") or {}
        median_60 = medians.get("60")
        median_120 = medians.get("120")
        if median_60 is not None and median_120 is not None and float(median_60) < 0 and float(median_120) < 0:
            alerts.append(
                f"{tier_labels[tier]}触发后60日和120日中位收益均为负"
            )
        ci_120 = (stats.get("forward_return_ci90_pct") or {}).get("120") or [None, None]
        if len(ci_120) == 2 and ci_120[1] is not None and float(ci_120[1]) < 0:
            alerts.append(f"{tier_labels[tier]}触发后120日收益90%区间整体低于0")
    if insufficient_tiers:
        alerts.insert(0, f"{'、'.join(insufficient_tiers)}独立触发样本不足3个")
    alerts = list(dict.fromkeys(alerts))
    return {
        "status": "attention" if alerts else "ok",
        "alerts": alerts,
        "review_message": None,
        "diagnostics": diagnostics,
        "diagnostic_count": len(diagnostics),
    }


def calculate_monthly_results(
    as_of: date,
    *,
    bootstrap_reps: int = 2000,
    fetcher: Callable[..., dict[str, Any]] | None = None,
    analyzer: Callable[..., dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    config = thresholds_analysis.Config(as_of=as_of, bootstrap_reps=bootstrap_reps)
    history_fetcher = fetcher or thresholds_analysis.fetch_qfq_daily
    ticker_analyzer = analyzer or thresholds_analysis.analyze_ticker
    histories = history_fetcher(thresholds_analysis.TICKERS, config)
    return [
        ticker_analyzer(ticker, histories[ticker], config, index * 10000)
        for index, ticker in enumerate(thresholds_analysis.TICKERS)
    ]


def install_monthly_results(
    store: dict[str, Any],
    *,
    effective_month: str,
    as_of: date,
    results: list[dict[str, Any]],
    created_at: str,
) -> None:
    snapshots = store.setdefault("threshold_snapshots", {})
    preferred = store.setdefault("preferred_threshold_snapshots", {})
    for result in results:
        symbol = str(result.get("ticker") or "").upper()
        if not symbol:
            continue
        for phase, phase_rules in REBALANCE_RULES.items():
            rule = phase_rules.get(symbol)
            if not isinstance(rule, dict):
                continue
            snapshot_key = f"{symbol}:{phase}:{effective_month}"
            snapshot_id = f"{snapshot_key}:auto:{as_of.isoformat()}"
            snapshot = {
                "id": snapshot_id,
                "symbol": symbol,
                "phase": phase,
                "effective_month": effective_month,
                "created_at": created_at,
                "mode": str(result.get("execution_mode") or rule.get("mode") or "automatic"),
                "thresholds_pct": _pct_thresholds(result.get("thresholds")),
                "base_quantile_thresholds_pct": _pct_thresholds(result.get("base_thresholds")),
                "threshold_ci90_pct": _threshold_ci_pct(result.get("ci90") or []),
                "quantiles": list(MONTHLY_QUANTILES),
                "vol_regime": result.get("vol_regime"),
                "vol_multiplier": result.get("vol_multiplier"),
                "history_days": result.get("history_days"),
                "as_of_date": result.get("as_of_date"),
                "confidence_by_tier": result.get("confidence_by_tier") or {},
                "walk_forward": result.get("walk_forward") or {},
                "warnings": list(dict.fromkeys(result.get("warnings") or [])),
                "validation": build_validation_summary(result),
                "execution_overrides": result.get("execution_overrides") or {},
                "calculation_kind": "monthly_auto",
                "validation_policy": "warning_only",
            }
            if snapshot["mode"] == "manual_review_only":
                snapshot["thresholds_pct"] = {}
            snapshots[snapshot_id] = snapshot
            preferred[snapshot_key] = snapshot_id


def run_monthly_recalculation(
    user_id: str = "evan",
    *,
    now: datetime | None = None,
    force: bool = False,
    bootstrap_reps: int = 2000,
    calculator: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(TZ_SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TZ_SHANGHAI)
    current = current.astimezone(TZ_SHANGHAI)
    effective_month = current_month_key(current)
    as_of = previous_month_end(effective_month)

    with _RECALCULATION_LOCK:
        store = load_drawdown_episode_store(user_id)
        runs = store.setdefault("monthly_recalculations", {})
        existing = runs.get(effective_month) or {}
        if not force and existing.get("status") == "success":
            return dict(existing)
        runs[effective_month] = {
            "effective_month": effective_month,
            "as_of_date": as_of.isoformat(),
            "status": "running",
            "started_at": current.isoformat(timespec="seconds"),
        }
        save_drawdown_episode_store(user_id, store)
        try:
            calculate = calculator or calculate_monthly_results
            results = calculate(as_of, bootstrap_reps=bootstrap_reps)
            # Calculation can take several seconds. Reload before writing so
            # dashboard-triggered episode updates made meanwhile are retained.
            store = load_drawdown_episode_store(user_id)
            runs = store.setdefault("monthly_recalculations", {})
            install_monthly_results(
                store,
                effective_month=effective_month,
                as_of=as_of,
                results=results,
                created_at=current.isoformat(timespec="seconds"),
            )
            validations = [build_validation_summary(result) for result in results]
            attention_symbol_count = sum(item.get("status") == "attention" for item in validations)
            review_symbol_count = sum(item.get("status") == "review" for item in validations)
            alert_count = sum(len(item.get("alerts") or []) for item in validations)
            diagnostic_count = sum(int(item.get("diagnostic_count") or 0) for item in validations)
            run = {
                "effective_month": effective_month,
                "as_of_date": as_of.isoformat(),
                "status": "success",
                "completed_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
                "symbols": [result.get("ticker") for result in results],
                "warning_count": alert_count,
                "attention_symbol_count": attention_symbol_count,
                "review_symbol_count": review_symbol_count,
                "diagnostic_count": diagnostic_count,
                "quantiles": list(MONTHLY_QUANTILES),
                "validation_policy": "warning_only",
            }
            runs[effective_month] = run
            store["updated_at"] = run["completed_at"]
            save_drawdown_episode_store(user_id, store)
            return run
        except Exception as exc:
            store = load_drawdown_episode_store(user_id)
            runs = store.setdefault("monthly_recalculations", {})
            run = {
                "effective_month": effective_month,
                "as_of_date": as_of.isoformat(),
                "status": "error",
                "failed_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
                "error": str(exc),
                "quantiles": list(MONTHLY_QUANTILES),
                "validation_policy": "warning_only",
            }
            runs[effective_month] = run
            save_drawdown_episode_store(user_id, store)
            return run


def start_monthly_drawdown_scheduler(user_id: str = "evan") -> None:
    """Recalculate once per month after prior-month US data is available."""
    while True:
        current = datetime.now(TZ_SHANGHAI)
        # 07:00 Shanghai is safely after the prior US regular close. If the
        # service was offline at the boundary, a later restart catches up.
        if current.hour >= 7:
            run_monthly_recalculation(user_id, now=current)
        time.sleep(900)
