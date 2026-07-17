from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from futu import AuType, KLType, OpenQuoteContext, RET_OK, Session


TICKERS = ("VOO", "QQQ", "ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO")
QUANTILES = np.array([0.65, 0.85, 0.95])
TIER_NAMES = ("small", "medium", "large")
REGIME_NAMES = {0: "low", 1: "normal", 2: "high"}
REGIME_CN = {"low": "低波动", "normal": "正常波动", "high": "高波动"}
VOL_MULTIPLIERS = {0: 0.95, 1: 1.00, 2: 1.05}
FREQUENCY_TARGETS = {
    "small": (2.0, 4.0),
    "medium": (0.8, 1.5),
    "large": (0.25, 0.5),
}


@dataclass(frozen=True)
class Config:
    as_of: date
    lookback_years: int = 10
    drawdown_window: int = 60
    rv_window: int = 20
    vol_rank_window: int = 756
    min_vol_rank_history: int = 252
    min_training_days: int = 500
    block_length: int = 20
    bootstrap_reps: int = 2000
    cooldown_days: int = 5
    recovery_days: int = 10
    random_seed: int = 20260717


def fetch_qfq_daily(tickers: tuple[str, ...], config: Config) -> dict[str, pd.Series]:
    start = config.as_of - timedelta(days=366 * config.lookback_years)
    context = OpenQuoteContext(host="127.0.0.1", port=11111)
    output: dict[str, pd.Series] = {}
    try:
        for ticker in tickers:
            pages: list[pd.DataFrame] = []
            page_key = None
            while True:
                ret, frame, page_key = context.request_history_kline(
                    f"US.{ticker}",
                    start=start.isoformat(),
                    end=config.as_of.isoformat(),
                    ktype=KLType.K_DAY,
                    autype=AuType.QFQ,
                    max_count=1000,
                    page_req_key=page_key,
                    session=Session.NONE,
                )
                if ret != RET_OK:
                    raise RuntimeError(f"Futu history request failed for {ticker}: {frame}")
                pages.append(frame)
                if page_key is None:
                    break
            data = pd.concat(pages, ignore_index=True)
            data["time_key"] = pd.to_datetime(data["time_key"])
            series = (
                data.drop_duplicates("time_key", keep="last")
                .set_index("time_key")["close"]
                .astype(float)
                .sort_index()
            )
            output[ticker] = series[series.index.date <= config.as_of]
    finally:
        context.close()
    return output


def trailing_percentile_rank(values: np.ndarray, window: int, min_history: int) -> np.ndarray:
    ranks = np.full(len(values), np.nan, dtype=float)
    for idx, value in enumerate(values):
        if not np.isfinite(value):
            continue
        history = values[max(0, idx - window) : idx]
        history = history[np.isfinite(history)]
        if len(history) < min_history:
            continue
        ranks[idx] = (np.sum(history < value) + 0.5 * np.sum(history == value)) / len(history)
    return ranks


def regime_code(percentile: float) -> int:
    if percentile < 1 / 3:
        return 0
    if percentile < 2 / 3:
        return 1
    return 2


def previous_month_end_position(index: pd.DatetimeIndex, position: int) -> int | None:
    current_month = index[position].to_period("M")
    candidates = np.flatnonzero(index[: position + 1].to_period("M") < current_month)
    return int(candidates[-1]) if len(candidates) else None


def adjusted_quantiles(drawdowns: np.ndarray, multiplier: float) -> np.ndarray:
    values = drawdowns[np.isfinite(drawdowns)]
    if not len(values):
        raise ValueError("Insufficient valid drawdown observations")
    return np.quantile(values, QUANTILES) * multiplier


def moving_block_bootstrap(
    drawdowns: np.ndarray,
    multiplier: float,
    config: Config,
    seed_offset: int,
) -> np.ndarray:
    dd = drawdowns[np.isfinite(drawdowns)]
    n = len(dd)
    block = min(config.block_length, n)
    if n < block:
        raise ValueError("Insufficient observations for moving-block bootstrap")
    rng = np.random.default_rng(config.random_seed + seed_offset)
    draws = np.empty((config.bootstrap_reps, 3), dtype=float)
    block_count = math.ceil(n / block)
    max_start = n - block
    offsets = np.arange(block)
    for rep in range(config.bootstrap_reps):
        starts = rng.integers(0, max_start + 1, size=block_count)
        indices = (starts[:, None] + offsets).ravel()[:n]
        draws[rep] = adjusted_quantiles(dd[indices], multiplier)
    return draws


def round_half_percent(value: float) -> float:
    return math.floor(value * 200 + 0.5) / 200


def clean_thresholds(values: np.ndarray) -> np.ndarray:
    cleaned = np.array([round_half_percent(float(value)) for value in values], dtype=float)
    cleaned[1] = max(cleaned[1], cleaned[0] + 0.03)
    cleaned[2] = max(cleaned[2], cleaned[1] + 0.05)
    return cleaned


def threshold_snapshot(
    dd60: np.ndarray,
    vol_rank: np.ndarray,
    index: pd.DatetimeIndex,
    position: int,
    config: Config,
) -> tuple[np.ndarray, int, float, int] | None:
    """Calculate a no-lookahead monthly snapshot using only prior-month data."""
    month_end = previous_month_end_position(index, position)
    if month_end is None or month_end + 1 < config.min_training_days:
        return None
    rank = vol_rank[month_end]
    if not np.isfinite(rank):
        return None
    regime = regime_code(float(rank))
    multiplier = VOL_MULTIPLIERS[regime]
    raw = adjusted_quantiles(dd60[: month_end + 1], multiplier)
    return clean_thresholds(raw), regime, multiplier, month_end


def walk_forward_frequency(
    dd60: pd.Series,
    vol_rank: np.ndarray,
    config: Config,
) -> dict[str, Any]:
    values = dd60.to_numpy()
    index = dd60.index
    events: dict[str, list[str]] = {name: [] for name in TIER_NAMES}
    episode_active = False
    frozen: np.ndarray | None = None
    triggered = [False, False, False]
    last_trigger_position = -10_000
    recovery_count = 0
    first_test_position: int | None = None
    snapshot_cache: dict[pd.Period, tuple[np.ndarray, int, float, int] | None] = {}

    for pos, dd in enumerate(values):
        if not np.isfinite(dd):
            continue
        month = index[pos].to_period("M")
        if month not in snapshot_cache:
            snapshot_cache[month] = threshold_snapshot(values, vol_rank, index, pos, config)
        snapshot = snapshot_cache[month]
        if snapshot is None:
            continue
        if first_test_position is None:
            first_test_position = pos

        if episode_active and frozen is not None:
            if dd <= 1e-12:
                episode_active = False
            else:
                recovery_count = recovery_count + 1 if dd < frozen[0] / 2 else 0
                if recovery_count >= config.recovery_days:
                    episode_active = False
            if not episode_active:
                frozen = None
                triggered = [False, False, False]
                recovery_count = 0

        if not episode_active:
            current_thresholds = snapshot[0]
            if dd >= current_thresholds[0]:
                episode_active = True
                frozen = current_thresholds.copy()
                triggered = [True, False, False]
                events["small"].append(index[pos].date().isoformat())
                last_trigger_position = pos
            continue

        assert frozen is not None
        if pos - last_trigger_position < config.cooldown_days:
            continue
        # Only one new tier can fire on a day. A jump across several tiers is
        # therefore staged at least five trading days apart.
        for tier_idx in (1, 2):
            if not triggered[tier_idx] and dd >= frozen[tier_idx]:
                triggered[tier_idx] = True
                events[TIER_NAMES[tier_idx]].append(index[pos].date().isoformat())
                last_trigger_position = pos
                break

    if first_test_position is None:
        years = 0.0
    else:
        calendar_days = max((index[-1] - index[first_test_position]).days, 1)
        years = calendar_days / 365.2425
    annual_frequency = {
        name: (len(dates) / years if years > 0 else None) for name, dates in events.items()
    }
    return {
        "test_start": index[first_test_position].date().isoformat() if first_test_position is not None else None,
        "test_years": years,
        "events": events,
        "event_counts": {name: len(dates) for name, dates in events.items()},
        "annual_frequency": annual_frequency,
    }


def tier_confidences(
    history_days: int,
    estimates: np.ndarray,
    ci_low: np.ndarray,
    ci_high: np.ndarray,
    event_counts: dict[str, int],
) -> tuple[dict[str, str], list[str]]:
    warnings: list[str] = []
    relative_width = np.divide(
        ci_high - ci_low,
        estimates,
        out=np.full(3, np.inf),
        where=estimates > 0,
    )
    confidences: dict[str, str] = {}
    for idx, name in enumerate(TIER_NAMES):
        if history_days < 756 or relative_width[idx] > 0.30:
            confidence = "low"
        elif relative_width[idx] > 0.20:
            confidence = "medium"
        else:
            confidence = "high"
        if name == "large" and event_counts[name] < 30 and confidence == "high":
            confidence = "medium"
        confidences[name] = confidence

    if history_days < 500:
        warnings.append("有效历史少于500个交易日")
    if history_days < 756:
        warnings.append("历史不足3年，未配置同行组；各档置信度下调为low")
    for idx, name in enumerate(TIER_NAMES):
        if relative_width[idx] > 0.30:
            warnings.append(f"{name}档Bootstrap 90%区间相对宽度超过30%")
    if event_counts["large"] < 30:
        warnings.append("独立大档事件少于30次，大档置信度不得为high")
    return confidences, warnings


def frequency_warnings(walk_forward: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    frequencies = walk_forward["annual_frequency"]
    for name in TIER_NAMES:
        frequency = frequencies[name]
        if frequency is None:
            warnings.append(f"{name}档没有足够walk-forward测试期")
            continue
        low, high = FREQUENCY_TARGETS[name]
        if not low <= frequency <= high:
            warnings.append(
                f"{name}档年均独立触发{frequency:.2f}次，不在目标{low:g}-{high:g}次内"
            )
    return warnings


def analyze_ticker(ticker: str, close: pd.Series, config: Config, seed_offset: int) -> dict[str, Any]:
    peak60 = close.rolling(config.drawdown_window, min_periods=config.drawdown_window).max()
    dd60 = 1 - close / peak60
    log_return = np.log(close / close.shift(1))
    rv20 = log_return.rolling(config.rv_window, min_periods=config.rv_window).std(ddof=1) * np.sqrt(252)
    vol_rank = trailing_percentile_rank(
        rv20.to_numpy(), config.vol_rank_window, config.min_vol_rank_history
    )

    month_end = previous_month_end_position(close.index, len(close) - 1)
    if month_end is None or not np.isfinite(vol_rank[month_end]):
        raise ValueError(f"{ticker} has no valid previous-month volatility state")
    prior_month_regime = regime_code(float(vol_rank[month_end]))
    multiplier = VOL_MULTIPLIERS[prior_month_regime]
    point_estimate = adjusted_quantiles(dd60.to_numpy(), multiplier)
    bootstrap = moving_block_bootstrap(dd60.to_numpy(), multiplier, config, seed_offset)
    boot_median = np.median(bootstrap, axis=0)
    ci_low = np.quantile(bootstrap, 0.05, axis=0)
    ci_high = np.quantile(bootstrap, 0.95, axis=0)
    thresholds = clean_thresholds(boot_median)

    walk_forward = walk_forward_frequency(dd60, vol_rank, config)
    confidences, warnings = tier_confidences(
        len(close), boot_median, ci_low, ci_high, walk_forward["event_counts"]
    )
    warnings.extend(frequency_warnings(walk_forward))
    if thresholds[0] < 0.03:
        warnings.append("小加档浅于3%")
    if thresholds[2] > 0.60:
        warnings.append("大加档深于60%")

    return {
        "ticker": ticker,
        "as_of_date": close.index[-1].date().isoformat(),
        "source": "Futu OpenD QFQ daily close",
        "history_start": close.index[0].date().isoformat(),
        "history_days": int(len(close)),
        "effective_drawdown_samples": int(np.isfinite(dd60.to_numpy()).sum()),
        "current_price": float(close.iloc[-1]),
        "current_dd60": float(dd60.iloc[-1]),
        "current_rv20": float(rv20.iloc[-1]),
        "vol_state_date": close.index[month_end].date().isoformat(),
        "vol_percentile_at_prior_month_end": float(vol_rank[month_end]),
        "vol_regime": REGIME_NAMES[prior_month_regime],
        "vol_regime_cn": REGIME_CN[REGIME_NAMES[prior_month_regime]],
        "vol_multiplier": multiplier,
        "raw_estimate": point_estimate.tolist(),
        "bootstrap_median": boot_median.tolist(),
        "thresholds": thresholds.tolist(),
        "ci90": np.column_stack([ci_low, ci_high]).tolist(),
        "confidence_by_tier": confidences,
        "walk_forward": walk_forward,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate full-history 60-day drawdown quantiles with lagged volatility adjustment."
    )
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--bootstrap-reps", type=int, default=2000)
    args = parser.parse_args()
    config = Config(as_of=date.fromisoformat(args.as_of), bootstrap_reps=args.bootstrap_reps)
    histories = fetch_qfq_daily(TICKERS, config)
    results = [
        analyze_ticker(ticker, histories[ticker], config, idx * 10000)
        for idx, ticker in enumerate(TICKERS)
    ]
    payload = {
        "method": {
            "drawdown": "1 - QFQ close / rolling 60-trading-day high",
            "base_quantiles": QUANTILES.tolist(),
            "volatility_adjustment": {
                "source": "previous calendar month-end RV20 percentile versus up to 756 prior observations",
                "low": 0.95,
                "normal": 1.00,
                "high": 1.05,
            },
            "episode": "thresholds freeze at small-tier trigger; each tier once; 5-day tier cooldown",
            "reset": "new 60-day high, or DD60 below half of frozen small tier for 10 trading days",
            "bootstrap": f"moving block bootstrap, block=20, reps={config.bootstrap_reps}, CI=90%",
            "cleaning": "round to 0.5 percentage point; medium>=small+3pp; large>=medium+5pp",
            "frequency_targets_per_year": FREQUENCY_TARGETS,
        },
        "results": results,
    }
    print("RESULT_JSON_BEGIN")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print("RESULT_JSON_END")


if __name__ == "__main__":
    main()
