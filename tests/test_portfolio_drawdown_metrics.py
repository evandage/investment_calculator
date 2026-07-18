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

    def test_csi300_keeps_same_day_return_after_china_close(self):
        quote = {
            "quote_date": "2026-07-17",
            "regular_change_pct": -3.6,
            "change_pct": -3.6,
        }
        after_close = datetime(2026, 7, 17, 15, 20, tzinfo=TZ_SHANGHAI)
        before_close = datetime(2026, 7, 17, 14, 59, tzinfo=TZ_SHANGHAI)
        self.assertAlmostEqual(
            portfolio.history_daily_pct_for_symbol("001015", quote, "2026-07-17", after_close),
            -3.6,
        )
        self.assertAlmostEqual(
            portfolio.history_daily_pct_for_symbol("001015", quote, "2026-07-17", before_close),
            -3.6,
        )
        self.assertFalse(portfolio.is_symbol_daily_history_estimated("001015", "2026-07-17", after_close, quote))

    def test_closed_day_display_carries_latest_completed_symbol_return(self):
        completed_row = {
            "date": "2026-07-17",
            "symbol_daily_pct": {"VOO": -0.95, "001015": -3.28},
            "benchmark_daily_pct": {"QQQ": -1.31},
        }
        stale_quote = {
            "price": 101.0,
            "prev_close": 100.0,
            "regular_change_pct": 0.0,
        }
        self.assertAlmostEqual(
            portfolio.carried_completed_daily_pct("VOO", stale_quote, completed_row),
            -0.95,
        )
        self.assertAlmostEqual(
            portfolio.carried_completed_daily_pct("QQQ", stale_quote, completed_row),
            -1.31,
        )

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


if __name__ == "__main__":
    unittest.main()
