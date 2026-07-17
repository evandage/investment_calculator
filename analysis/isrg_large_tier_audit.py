from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from futu import OpenQuoteContext, RET_OK

try:
    from .drawdown_thresholds import Config, fetch_qfq_daily
except ImportError:  # Direct script execution.
    from drawdown_thresholds import Config, fetch_qfq_daily


@dataclass(frozen=True)
class AuditConfig:
    threshold: float = 0.21
    recovery_fraction: float = 0.50
    recovery_days: int = 10
    earnings_gap_cutoff: float = 0.05
    forward_windows: tuple[int, ...] = (60, 120)


def fixed_threshold_events(dd60: pd.Series, config: AuditConfig) -> list[int]:
    """Return first threshold touch in each independent drawdown episode."""
    events: list[int] = []
    episode_active = False
    recovery_count = 0
    values = dd60.to_numpy()
    for pos, dd in enumerate(values):
        if not np.isfinite(dd):
            continue
        if episode_active:
            if dd <= 1e-12:
                episode_active = False
                recovery_count = 0
            else:
                recovery_count = recovery_count + 1 if dd < config.threshold * config.recovery_fraction else 0
                if recovery_count >= config.recovery_days:
                    episode_active = False
                    recovery_count = 0
        if not episode_active and dd >= config.threshold:
            events.append(pos)
            episode_active = True
            recovery_count = 0
    return events


def fetch_earnings_events() -> pd.DataFrame:
    context = OpenQuoteContext(host="127.0.0.1", port=11111)
    try:
        ret, frame = context.get_financials_earnings_price_history("US.ISRG")
        if ret != RET_OK:
            raise RuntimeError(f"Futu earnings history request failed: {frame}")
    finally:
        context.close()
    columns = [
        "period_text",
        "pub_trading_day_str",
        "trading_day_str",
        "open_price",
        "last_close_price",
    ]
    unique = frame.drop_duplicates("period_text", keep="first")[columns].copy()
    unique["response_date"] = pd.to_datetime(unique["trading_day_str"], errors="coerce")
    unique["opening_gap"] = (
        pd.to_numeric(unique["open_price"], errors="coerce")
        / pd.to_numeric(unique["last_close_price"], errors="coerce")
        - 1
    )
    return unique.dropna(subset=["response_date"]).sort_values("response_date")


def event_outcome(close: pd.Series, position: int, windows: tuple[int, ...]) -> dict[str, Any]:
    base = float(close.iloc[position])
    output: dict[str, Any] = {
        "date": close.index[position].date().isoformat(),
        "trigger_price": base,
    }
    for window in windows:
        if position + window >= len(close):
            output[f"return_{window}d"] = None
            output[f"mae_{window}d"] = None
            continue
        path = close.iloc[position + 1 : position + window + 1] / base - 1
        output[f"return_{window}d"] = float(close.iloc[position + window] / base - 1)
        output[f"mae_{window}d"] = float(path.min())
    return output


def median_available(events: list[dict[str, Any]], field: str) -> float | None:
    values = [event[field] for event in events if event.get(field) is not None]
    return float(np.median(values)) if values else None


def compare_thresholds(
    close: pd.Series,
    dd60: pd.Series,
    thresholds: tuple[float, ...],
) -> list[dict[str, Any]]:
    span_years = (close.index[-1] - close.index[0]).days / 365.2425
    comparisons: list[dict[str, Any]] = []
    for threshold in thresholds:
        candidate = AuditConfig(threshold=threshold)
        positions = fixed_threshold_events(dd60, candidate)
        outcomes = [event_outcome(close, pos, candidate.forward_windows) for pos in positions]
        comparisons.append(
            {
                "threshold": threshold,
                "event_count": len(outcomes),
                "events_per_year": len(outcomes) / span_years,
                "years_per_event": span_years / len(outcomes) if outcomes else None,
                "median_return_60d": median_available(outcomes, "return_60d"),
                "median_return_120d": median_available(outcomes, "return_120d"),
                "median_mae_60d": median_available(outcomes, "mae_60d"),
                "median_mae_120d": median_available(outcomes, "mae_120d"),
                "sample_60d": sum(x["return_60d"] is not None for x in outcomes),
                "sample_120d": sum(x["return_120d"] is not None for x in outcomes),
            }
        )
    return comparisons


def main() -> None:
    audit = AuditConfig()
    history_config = Config(as_of=date(2026, 7, 17))
    close = fetch_qfq_daily(("ISRG",), history_config)["ISRG"]
    peak60 = close.rolling(60, min_periods=60).max()
    dd60 = 1 - close / peak60
    event_positions = fixed_threshold_events(dd60, audit)
    outcomes = [event_outcome(close, pos, audit.forward_windows) for pos in event_positions]

    span_years = (close.index[-1] - close.index[0]).days / 365.2425
    earnings = fetch_earnings_events()
    coverage_start = earnings["response_date"].min()
    coverage_end = earnings["response_date"].max()
    covered = [
        event for event in outcomes
        if coverage_start <= pd.Timestamp(event["date"]) <= coverage_end
    ]
    earnings_by_date = {
        row.response_date.normalize(): row
        for row in earnings.itertuples(index=False)
    }
    jump_events: list[dict[str, Any]] = []
    for event in covered:
        earnings_event = earnings_by_date.get(pd.Timestamp(event["date"]).normalize())
        if earnings_event is None or not np.isfinite(earnings_event.opening_gap):
            continue
        if abs(float(earnings_event.opening_gap)) >= audit.earnings_gap_cutoff:
            jump_events.append(
                {
                    "trigger_date": event["date"],
                    "period": earnings_event.period_text,
                    "opening_gap": float(earnings_event.opening_gap),
                }
            )

    payload = {
        "ticker": "ISRG",
        "source": "Futu OpenD QFQ daily close and earnings price history",
        "history_start": close.index[0].date().isoformat(),
        "history_end": close.index[-1].date().isoformat(),
        "threshold": audit.threshold,
        "episode_definition": {
            "trigger": "first DD60 >= 21%",
            "reset": "new 60-day high, or DD60 < 10.5% for 10 trading days",
        },
        "independent_event_count": len(outcomes),
        "history_years": span_years,
        "events_per_year": len(outcomes) / span_years,
        "years_per_event": span_years / len(outcomes) if outcomes else None,
        "median_forward_return_60d": median_available(outcomes, "return_60d"),
        "median_forward_return_120d": median_available(outcomes, "return_120d"),
        "median_mae_60d": median_available(outcomes, "mae_60d"),
        "median_mae_120d": median_available(outcomes, "mae_120d"),
        "forward_sample_count_60d": sum(x["return_60d"] is not None for x in outcomes),
        "forward_sample_count_120d": sum(x["return_120d"] is not None for x in outcomes),
        "earnings_jump_definition": "trigger on earnings response date and abs(open / prior close - 1) >= 5%",
        "earnings_coverage_start": coverage_start.date().isoformat(),
        "earnings_coverage_end": coverage_end.date().isoformat(),
        "covered_trigger_count": len(covered),
        "earnings_jump_trigger_count": len(jump_events),
        "earnings_jump_share": len(jump_events) / len(covered) if covered else None,
        "earnings_jump_events": jump_events,
        "candidate_comparison": compare_thresholds(
            close,
            dd60,
            (0.21, 0.23, 0.25, 0.275, 0.30, 0.325, 0.35),
        ),
        "events": outcomes,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
