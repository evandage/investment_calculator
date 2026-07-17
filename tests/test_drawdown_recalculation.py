from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from analysis.drawdown_thresholds import walk_forward_outcome_statistics
from backend.config import TZ_SHANGHAI
from backend.drawdown_episodes import ensure_threshold_snapshot
from backend.drawdown_recalculation import build_validation_summary, install_monthly_results, run_monthly_recalculation


def sample_result(symbol: str = "VOO") -> dict:
    return {
        "ticker": symbol,
        "as_of_date": "2026-07-31",
        "execution_mode": "automatic",
        "thresholds": [0.035, 0.09, 0.16],
        "base_thresholds": [0.04, 0.09, 0.16],
        "ci90": [[0.03, 0.05], [0.07, 0.11], [0.13, 0.20]],
        "vol_regime": "normal",
        "vol_multiplier": 1.0,
        "history_days": 2000,
        "confidence_by_tier": {"small": "high", "medium": "medium", "large": "low"},
        "walk_forward": {
            "annual_frequency": {"small": 2.0, "medium": 1.0, "large": 0.3},
            "statistics": {"small": {"sample_count": 12}},
        },
        "warnings": ["large档样本偏少"],
        "execution_overrides": {"small": 0.035},
    }


class MonthlyRecalculationTests(unittest.TestCase):
    def test_raw_diagnostics_do_not_become_main_table_alerts(self):
        result = sample_result()
        result["walk_forward"]["statistics"] = {
            tier: {
                "sample_count": 5,
                "forward_return_median_pct": {"60": 3.0, "120": 8.0},
                "forward_return_ci90_pct": {"120": [-4.0, 15.0]},
            }
            for tier in ("small", "medium", "large")
        }
        result["warnings"] = [
            "large档Bootstrap 90%区间相对宽度超过30%",
            "独立大档事件少于30次，大档置信度不得为high",
        ]
        validation = build_validation_summary(result)
        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["alerts"], [])
        self.assertEqual(validation["diagnostic_count"], 2)

    def test_walk_forward_statistics_include_returns_mae_and_confidence_intervals(self):
        close = pd.Series([100.0 + index for index in range(150)])
        stats = walk_forward_outcome_statistics(close, [5, 10], seed=7)
        self.assertEqual(stats["sample_count"], 2)
        self.assertIsNotNone(stats["forward_return_median_pct"]["20"])
        self.assertIsNotNone(stats["forward_return_median_pct"]["60"])
        self.assertIsNotNone(stats["forward_return_median_pct"]["120"])
        self.assertEqual(len(stats["forward_return_ci90_pct"]["60"]), 2)
        self.assertIsNotNone(stats["mae_120d_median_pct"])

    def test_install_creates_preferred_snapshot_with_warning_only_validation(self):
        store = {"threshold_snapshots": {}, "episodes": {}}
        install_monthly_results(
            store,
            effective_month="2026-08",
            as_of=datetime(2026, 7, 31).date(),
            results=[sample_result()],
            created_at="2026-08-01T07:00:00+08:00",
        )
        snapshot, created = ensure_threshold_snapshot(
            store,
            symbol="VOO",
            phase="建仓期",
            month_key="2026-08",
            rule={"bands": []},
            created_at="later",
        )
        self.assertFalse(created)
        self.assertEqual(snapshot["thresholds_pct"], {"small": -3.5, "medium": -9.0, "large": -16.0})
        self.assertEqual(snapshot["quantiles"], [0.65, 0.85, 0.95])
        self.assertEqual(snapshot["validation_policy"], "warning_only")
        self.assertEqual(snapshot["warnings"], ["large档样本偏少"])

    def test_monthly_run_is_idempotent_after_success(self):
        state = {
            "threshold_snapshots": {},
            "preferred_threshold_snapshots": {},
            "monthly_recalculations": {},
            "episodes": {},
        }
        calls = []

        def load(_user_id):
            return deepcopy(state)

        def save(_user_id, value):
            state.clear()
            state.update(deepcopy(value))

        def calculate(as_of, bootstrap_reps=2000):
            calls.append((as_of.isoformat(), bootstrap_reps))
            return [sample_result()]

        now = datetime(2026, 8, 1, 7, 5, tzinfo=TZ_SHANGHAI)
        with patch("backend.drawdown_recalculation.load_drawdown_episode_store", side_effect=load), patch(
            "backend.drawdown_recalculation.save_drawdown_episode_store", side_effect=save
        ):
            first = run_monthly_recalculation(now=now, calculator=calculate, bootstrap_reps=50)
            second = run_monthly_recalculation(now=now, calculator=calculate, bootstrap_reps=50)

        self.assertEqual(first["status"], "success")
        self.assertEqual(second["status"], "success")
        self.assertEqual(calls, [("2026-07-31", 50)])
        self.assertEqual(state["monthly_recalculations"]["2026-08"]["quantiles"], [0.65, 0.85, 0.95])


if __name__ == "__main__":
    unittest.main()
