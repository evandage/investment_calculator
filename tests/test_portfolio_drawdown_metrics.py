from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

from backend import portfolio
from backend.config import TZ_SHANGHAI


class ConfirmedCloseMetricTests(unittest.TestCase):
    def setUp(self):
        portfolio._DRAWDOWN_CACHE.clear()

    def tearDown(self):
        portfolio._DRAWDOWN_CACHE.clear()

    @patch("backend.ohlcv.fetch_ohlcv")
    @patch("backend.portfolio.completed_performance_day", return_value="2026-07-16")
    def test_partial_current_day_bar_is_not_used_for_confirmation(self, _completed_day, fetch_ohlcv):
        fetch_ohlcv.return_value = {
            "bars": [
                {"time": "2026-07-15", "close": 100.0},
                {"time": "2026-07-16", "close": 90.0},
                {"time": "2026-07-17", "close": 70.0},
            ]
        }
        metrics = portfolio.fetch_60d_metrics("ISRG", current_price=75.0)
        self.assertEqual(metrics["confirmed_close_date"], "2026-07-16")
        self.assertEqual(metrics["confirmed_close_price"], 90.0)
        self.assertAlmostEqual(metrics["confirmed_drawdown_pct"], -10.0)
        self.assertAlmostEqual(metrics["intraday_drawdown_pct"], -25.0)
        self.assertAlmostEqual(metrics["drawdown_pct"], -10.0)

    @patch("backend.ohlcv.fetch_ohlcv")
    @patch("backend.portfolio.completed_performance_day", return_value="2026-07-16")
    def test_intraday_new_high_is_only_a_zero_drawdown_preview(self, _completed_day, fetch_ohlcv):
        fetch_ohlcv.return_value = {
            "bars": [
                {"time": "2026-07-15", "close": 100.0},
                {"time": "2026-07-16", "close": 90.0},
            ]
        }
        metrics = portfolio.fetch_60d_metrics("VOO", current_price=110.0)
        self.assertAlmostEqual(metrics["confirmed_drawdown_pct"], -10.0)
        self.assertAlmostEqual(metrics["intraday_drawdown_pct"], 0.0)

    @patch("backend.ohlcv.fetch_ohlcv")
    @patch("backend.portfolio.completed_performance_day", return_value="2026-07-16")
    def test_active_episode_anchor_does_not_expire_after_sixty_days(self, _completed_day, fetch_ohlcv):
        fetch_ohlcv.return_value = {
            "bars": [
                {"time": f"2026-05-{day:02d}", "close": 80.0}
                for day in range(1, 29)
            ]
            + [
                {"time": "2026-07-15", "close": 75.0},
                {"time": "2026-07-16", "close": 70.0},
            ]
        }
        state = {
            "episode_active": True,
            "anchor_date": "2026-01-05",
            "anchor_price": 100.0,
            "metric_version": "episode_peak_v2",
        }

        metrics = portfolio.fetch_drawdown_metrics("VOO", current_price=70.0, episode_state=state)

        self.assertEqual(metrics["anchor_date"], "2026-01-05")
        self.assertEqual(metrics["anchor_price"], 100.0)
        self.assertAlmostEqual(metrics["confirmed_drawdown_pct"], -30.0)










    def test_closed_day_display_can_reconstruct_unstored_symbol_return(self):
        quote = {
            "regular_price": 98.0,
            "prev_close": 100.0,
            "regular_change_pct": 0.0,
        }
        self.assertAlmostEqual(
            portfolio.carried_completed_daily_pct("SGOV", quote, None),
            -2.0,
        )

    def test_closed_day_display_includes_last_extended_hours_move(self):
        quote = {
            "session": "closed",
            "regular_change_pct": -2.0,
            "extended_change_pct": 1.0,
        }
        expected = ((1.0 - 0.02) * (1.0 + 0.01) - 1.0) * 100.0
        self.assertAlmostEqual(
            portfolio.closed_display_daily_pct("VOO", quote, None),
            expected,
        )


if __name__ == "__main__":
    unittest.main()
