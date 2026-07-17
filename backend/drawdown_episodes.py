from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any


TIER_ORDER = ("small", "medium", "large")


def thresholds_from_rule(rule: dict[str, Any]) -> dict[str, float]:
    thresholds: dict[str, float] = {}
    for threshold, _multiplier, _action, _signal, intensity in rule.get("bands", []):
        tier = str(intensity or "").lower()
        if tier in TIER_ORDER:
            thresholds[tier] = float(threshold)
    return thresholds


def ensure_threshold_snapshot(
    store: dict[str, Any],
    *,
    symbol: str,
    phase: str,
    month_key: str,
    rule: dict[str, Any],
    created_at: str,
) -> tuple[dict[str, Any], bool]:
    snapshots = store.setdefault("threshold_snapshots", {})
    snapshot_key = f"{symbol}:{phase}:{month_key}"
    preferred = store.setdefault("preferred_threshold_snapshots", {})
    preferred_id = preferred.get(snapshot_key)
    if preferred_id and isinstance(snapshots.get(preferred_id), dict):
        return deepcopy(snapshots[preferred_id]), False
    snapshot_id = snapshot_key
    existing = snapshots.get(snapshot_id)
    if isinstance(existing, dict):
        return deepcopy(existing), False
    snapshot = {
        "id": snapshot_id,
        "symbol": symbol,
        "phase": phase,
        "effective_month": month_key,
        "created_at": created_at,
        "mode": str(rule.get("mode") or "automatic"),
        "thresholds_pct": thresholds_from_rule(rule),
    }
    snapshots[snapshot_id] = snapshot
    return deepcopy(snapshot), True


def default_episode_state(symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "episode_active": False,
        "episode_id": None,
        "threshold_snapshot_id": None,
        "small_triggered": False,
        "medium_triggered": False,
        "large_triggered": False,
        "trigger_date": None,
        "trigger_dates": {},
        "recovery_streak": 0,
        "last_processed_close_date": None,
        "last_confirmed_drawdown_pct": None,
        "last_confirmed_signal": None,
        "ended_at": None,
        "end_reason": None,
    }


def _snapshot_for_episode(
    state: dict[str, Any],
    current_snapshot: dict[str, Any],
    snapshots: dict[str, Any],
) -> dict[str, Any]:
    snapshot_id = state.get("threshold_snapshot_id")
    frozen = snapshots.get(snapshot_id) if snapshot_id else None
    return deepcopy(frozen) if isinstance(frozen, dict) else deepcopy(current_snapshot)


def _highest_crossed_tier(
    drawdown_pct: float,
    thresholds: dict[str, float],
    state: dict[str, Any],
    *,
    only_untriggered: bool,
) -> str | None:
    crossed: list[str] = []
    for tier in TIER_ORDER:
        threshold = thresholds.get(tier)
        if threshold is None or drawdown_pct > float(threshold):
            continue
        if only_untriggered and bool(state.get(f"{tier}_triggered")):
            continue
        crossed.append(tier)
    return crossed[-1] if crossed else None


def intraday_warning(
    *,
    symbol: str,
    intraday_drawdown_pct: float | None,
    current_price: float | None,
    session: str,
    state: dict[str, Any],
    current_snapshot: dict[str, Any],
    snapshots: dict[str, Any],
    as_of: str,
) -> dict[str, Any]:
    snapshot = _snapshot_for_episode(state, current_snapshot, snapshots)
    thresholds = dict(snapshot.get("thresholds_pct") or {})
    is_intraday = str(session or "").lower() != "closed"
    tier = None
    if is_intraday and intraday_drawdown_pct is not None and snapshot.get("mode") != "manual_review_only":
        tier = _highest_crossed_tier(
            float(intraday_drawdown_pct),
            thresholds,
            state,
            only_untriggered=bool(state.get("episode_active")),
        )
    return {
        "active": tier is not None,
        "symbol": symbol,
        "tier": tier,
        "drawdown_pct": intraday_drawdown_pct,
        "price": current_price,
        "session": str(session or "unknown"),
        "as_of": as_of,
        "threshold_snapshot_id": snapshot.get("id"),
        "confirmed": False,
        "message": f"盘中达到{tier}档，等待收盘确认" if tier else None,
    }


def advance_episode_on_close(
    *,
    symbol: str,
    state: dict[str, Any] | None,
    current_snapshot: dict[str, Any],
    snapshots: dict[str, Any],
    confirmed_close_date: str | None,
    confirmed_close_price: float | None,
    confirmed_drawdown_pct: float | None,
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    next_state = {**default_episode_state(symbol), **deepcopy(state or {})}
    previous_state = deepcopy(next_state)
    if not confirmed_close_date or confirmed_drawdown_pct is None:
        return next_state, _public_confirmed_signal(next_state, newly_triggered=False), False
    if next_state.get("last_processed_close_date") == confirmed_close_date:
        return next_state, _public_confirmed_signal(next_state, newly_triggered=False), False

    next_state["last_processed_close_date"] = confirmed_close_date
    next_state["last_confirmed_drawdown_pct"] = float(confirmed_drawdown_pct)
    active_snapshot = _snapshot_for_episode(next_state, current_snapshot, snapshots)
    thresholds = dict(active_snapshot.get("thresholds_pct") or {})
    automatic = active_snapshot.get("mode") != "manual_review_only" and "small" in thresholds

    if next_state.get("episode_active") and automatic:
        small_threshold = float(thresholds["small"])
        if float(confirmed_drawdown_pct) >= -1e-9:
            _end_episode(next_state, confirmed_close_date, "new_60d_high")
        else:
            within_half = float(confirmed_drawdown_pct) >= small_threshold / 2.0
            next_state["recovery_streak"] = int(next_state.get("recovery_streak") or 0) + 1 if within_half else 0
            if next_state["recovery_streak"] >= 10:
                _end_episode(next_state, confirmed_close_date, "recovered_within_half_small_for_10_days")

    newly_triggered_tier: str | None = None
    if automatic and not next_state.get("episode_active"):
        start_tier = _highest_crossed_tier(
            float(confirmed_drawdown_pct), thresholds, next_state, only_untriggered=False
        )
        if start_tier is not None:
            next_state["episode_active"] = True
            next_state["threshold_snapshot_id"] = current_snapshot.get("id")
            next_state["episode_id"] = f"{symbol}:{confirmed_close_date}:{current_snapshot.get('id')}"
            next_state["trigger_date"] = confirmed_close_date
            next_state["trigger_dates"] = {}
            next_state["recovery_streak"] = 0
            next_state["ended_at"] = None
            next_state["end_reason"] = None
            newly_triggered_tier = start_tier
            _mark_crossed_tiers(next_state, thresholds, float(confirmed_drawdown_pct), confirmed_close_date)
    elif automatic and next_state.get("episode_active"):
        frozen_snapshot = _snapshot_for_episode(next_state, current_snapshot, snapshots)
        frozen_thresholds = dict(frozen_snapshot.get("thresholds_pct") or {})
        newly_triggered_tier = _highest_crossed_tier(
            float(confirmed_drawdown_pct),
            frozen_thresholds,
            next_state,
            only_untriggered=True,
        )
        if newly_triggered_tier is not None:
            _mark_crossed_tiers(
                next_state,
                frozen_thresholds,
                float(confirmed_drawdown_pct),
                confirmed_close_date,
            )

    if newly_triggered_tier is not None:
        signal_id = f"{next_state['episode_id']}:{newly_triggered_tier}"
        next_state["last_confirmed_signal"] = {
            "id": signal_id,
            "symbol": symbol,
            "tier": newly_triggered_tier,
            "trigger_date": confirmed_close_date,
            "close_price": confirmed_close_price,
            "drawdown_pct": float(confirmed_drawdown_pct),
            "threshold_snapshot_id": next_state.get("threshold_snapshot_id"),
            "confirmed": True,
        }

    changed = next_state != previous_state
    return next_state, _public_confirmed_signal(next_state, newly_triggered=newly_triggered_tier is not None), changed


def _mark_crossed_tiers(
    state: dict[str, Any],
    thresholds: dict[str, float],
    drawdown_pct: float,
    trigger_date: str,
) -> None:
    trigger_dates = dict(state.get("trigger_dates") or {})
    for tier in TIER_ORDER:
        threshold = thresholds.get(tier)
        if threshold is None or drawdown_pct > float(threshold) or state.get(f"{tier}_triggered"):
            continue
        state[f"{tier}_triggered"] = True
        trigger_dates[tier] = trigger_date
    state["trigger_dates"] = trigger_dates


def _end_episode(state: dict[str, Any], close_date: str, reason: str) -> None:
    symbol = str(state.get("symbol") or "")
    last_processed = state.get("last_processed_close_date")
    last_drawdown = state.get("last_confirmed_drawdown_pct")
    ended = default_episode_state(symbol)
    ended["last_processed_close_date"] = last_processed
    ended["last_confirmed_drawdown_pct"] = last_drawdown
    ended["ended_at"] = close_date
    ended["end_reason"] = reason
    state.clear()
    state.update(ended)


def _public_confirmed_signal(state: dict[str, Any], *, newly_triggered: bool) -> dict[str, Any]:
    signal = deepcopy(state.get("last_confirmed_signal"))
    if not isinstance(signal, dict):
        return {
            "active": False,
            "id": None,
            "tier": None,
            "confirmed": True,
            "newly_triggered": False,
        }
    signal["active"] = True
    signal["newly_triggered"] = bool(newly_triggered)
    return signal


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")
