from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from backend import portfolio
from backend.config import TZ_SHANGHAI


class ConfirmedCloseMetricTests(unittest.TestCase):
    def setUp(self):
        portfolio._DRAWDOWN_CACHE.clear()
        portfolio._FUND_HISTORY_CACHE.clear()

    def tearDown(self):
        portfolio._DRAWDOWN_CACHE.clear()
        portfolio._FUND_HISTORY_CACHE.clear()

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

    @patch("backend.portfolio.requests.get")
    def test_fund_history_reuses_cached_prices(self, get):
        history_response = Mock()
        history_response.text = "<tr><td>2026-07-23</td><td>2.4010</td></tr>"
        history_response.encoding = ""
        get.return_value = history_response

        first = portfolio.fetch_fund_close_history("001015")
        request_count = get.call_count
        second = portfolio.fetch_fund_close_history("001015")

        self.assertEqual(first, {"2026-07-23": 2.401})
        self.assertEqual(second, first)
        self.assertEqual(get.call_count, request_count)

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

    def test_csi300_stale_estimate_is_replaced_by_official_fund_nav(self):
        stale_quote = {
            "symbol": "001015",
            "price": 2.2654,
            "quote_date": "2026-07-17",
            "regular_change_pct": -3.6,
            "source": "东方财富基金估算",
        }
        prices = {"2026-07-17": 2.273, "2026-07-20": 2.313}
        quote = portfolio.quote_with_official_fund_nav("001015", stale_quote, "2026-07-20", prices)
        self.assertEqual(quote["price"], 2.313)
        self.assertEqual(quote["quote_date"], "2026-07-20")
        self.assertEqual(quote["source"], "东方财富基金净值")
        self.assertAlmostEqual(quote["regular_change_pct"], (2.313 / 2.273 - 1.0) * 100.0)

    def test_csi300_keeps_estimate_until_official_nav_is_published(self):
        stale_quote = {"symbol": "001015", "price": 2.2654, "quote_date": "2026-07-17"}
        quote = portfolio.quote_with_official_fund_nav(
            "001015",
            stale_quote,
            "2026-07-20",
            {"2026-07-17": 2.273},
        )
        self.assertEqual(quote, stale_quote)

    def test_csi300_preopen_uses_previous_confirmed_close(self):
        preopen_estimate = {
            "symbol": "001015",
            "price": 2.3078,
            "regular_price": 2.3078,
            "quote_date": "2026-07-22",
            "regular_change_pct": -3.24,
            "source": "preopen estimate",
        }
        completed_row = {
            "date": "2026-07-21",
            "closing_prices": {"001015": 2.385},
        }

        quote = portfolio.quote_with_previous_fund_close(
            "001015", preopen_estimate, completed_row
        )

        self.assertEqual(quote["price"], 2.385)
        self.assertEqual(quote["regular_price"], 2.385)
        self.assertEqual(quote["regular_change_pct"], 0.0)
        self.assertEqual(quote["quote_date"], "2026-07-21")
        self.assertEqual(quote["source"], "上一交易日确认净值")

    def test_csi300_preopen_fallback_keeps_quote_without_confirmed_close(self):
        estimate = {"symbol": "001015", "price": 2.3078}

        quote = portfolio.quote_with_previous_fund_close("001015", estimate, None)

        self.assertEqual(quote, estimate)

    def test_csi300_pending_uses_latest_official_close_before_today(self):
        stale_estimate = {
            "symbol": "001015",
            "price": 2.3078,
            "regular_price": 2.3078,
            "quote_date": "2026-07-20",
            "regular_change_pct": 1.53,
            "source": "stale estimate",
        }

        quote = portfolio.quote_with_previous_fund_close(
            "001015",
            stale_estimate,
            None,
            {"2026-07-22": 2.385, "2026-07-23": 2.401},
            "2026-07-24",
        )

        self.assertEqual(quote["price"], 2.401)
        self.assertEqual(quote["regular_price"], 2.401)
        self.assertEqual(quote["regular_change_pct"], 0.0)
        self.assertEqual(quote["quote_date"], "2026-07-23")
        self.assertEqual(quote["source"], "上一交易日确认净值")

    def test_csi300_performance_curve_uses_510330_daily_return(self):
        histories = {
            "001015": {"2026-07-23": 2.30, "2026-07-24": 2.40},
            "510330.SS": {"2026-07-23": 4.00, "2026-07-24": 3.92},
        }

        daily_pct = portfolio.performance_benchmark_daily_pct(
            "001015",
            "2026-07-24",
            histories,
        )

        self.assertAlmostEqual(daily_pct, -2.0)

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
