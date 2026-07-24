from __future__ import annotations

import unittest
from unittest.mock import patch

from backend.market_data import (
    cache_fund_quote,
    fetch_direct_fund_quote,
    _apply_futu_ticker_price,
    _build_futu_quote,
    _merge_futu_subscription_quote,
    _parse_sina_fx_daily_history,
    _parse_sina_fund_estimate,
)


class ExtendedQuoteStabilityTests(unittest.TestCase):
    def test_fund_cache_does_not_allow_an_older_quote_to_replace_today(self):
        with (
            patch("backend.market_data._load_fund_quotes_cache"),
            patch("backend.market_data._save_fund_quotes_cache"),
            patch.dict("backend.market_data._FUND_QUOTES_CACHE", {}, clear=True),
        ):
            today = cache_fund_quote(
                "001015",
                {"price": 2.33, "quote_date": "2026-07-24", "source": "provider estimate"},
            )
            selected = cache_fund_quote(
                "001015",
                {"price": 2.30, "quote_date": "2026-07-20", "source": "stale estimate"},
            )

        self.assertEqual(today["price"], 2.33)
        self.assertEqual(selected["price"], 2.33)
        self.assertEqual(selected["quote_date"], "2026-07-24")

    @patch("backend.market_data.fetch_sina_fund_estimate")
    @patch("backend.market_data.fetch_fund_quote")
    def test_direct_fund_quote_prefers_same_day_eastmoney(self, eastmoney, sina):
        eastmoney.return_value = {
            "price": 2.33,
            "quote_date": "2026-07-24",
            "source": "eastmoney",
        }

        selected = fetch_direct_fund_quote("001015", "2026-07-24")

        self.assertEqual(selected["source"], "eastmoney")
        sina.assert_not_called()

    @patch("backend.market_data.cache_fund_quote")
    @patch("backend.market_data.fetch_sina_fund_estimate")
    @patch("backend.market_data.fetch_fund_quote")
    def test_direct_fund_quote_uses_same_day_sina_when_eastmoney_is_stale(
        self,
        eastmoney,
        sina,
        cache,
    ):
        eastmoney.return_value = {"price": 2.30, "quote_date": "2026-07-20"}
        sina.return_value = {
            "price": 2.33,
            "quote_date": "2026-07-24",
            "source": "sina",
        }
        cache.side_effect = lambda _code, quote: quote

        selected = fetch_direct_fund_quote("001015", "2026-07-24")

        self.assertEqual(selected["source"], "sina")
        cache.assert_called_once_with("001015", sina.return_value)

    def test_same_day_official_nav_replaces_estimate_in_cache(self):
        with (
            patch("backend.market_data._load_fund_quotes_cache"),
            patch("backend.market_data._save_fund_quotes_cache"),
            patch.dict("backend.market_data._FUND_QUOTES_CACHE", {}, clear=True),
        ):
            cache_fund_quote(
                "001015",
                {"price": 2.33, "quote_date": "2026-07-24", "source": "provider estimate"},
            )
            selected = cache_fund_quote(
                "001015",
                {
                    "price": 2.34,
                    "quote_date": "2026-07-24",
                    "source": "\u4e1c\u65b9\u8d22\u5bcc\u57fa\u91d1\u51c0\u503c",
                },
            )

        self.assertEqual(selected["price"], 2.34)

    def test_china_etf_quote_never_uses_us_extended_session_fields(self):
        quote = _build_futu_quote(
            "510330.SS",
            {
                "last_price": 4.908,
                "prev_close_price": 4.846,
                "open_price": 4.868,
                "overnight_price": 4.908,
                "overnight_change_rate": 0.0,
            },
        )

        self.assertIsNotNone(quote)
        self.assertEqual(quote["session"], "regular")
        self.assertIsNone(quote["extended_price"])
        self.assertIsNone(quote["extended_change_pct"])
        self.assertAlmostEqual(quote["change_pct"], (4.908 / 4.846 - 1.0) * 100.0)

    def test_china_etf_ticker_updates_regular_change(self):
        quote = {
            "session": "overnight",
            "regular_price": 4.908,
            "prev_close": 4.846,
            "extended_price": 4.908,
            "extended_change_pct": 0.0,
        }

        updated = _apply_futu_ticker_price(quote, "510330.SS", 4.91)

        self.assertEqual(updated["session"], "regular")
        self.assertEqual(updated["regular_price"], 4.91)
        self.assertIsNone(updated["extended_price"])
        self.assertIsNone(updated["extended_change_pct"])
        self.assertAlmostEqual(updated["change_pct"], (4.91 / 4.846 - 1.0) * 100.0)

    def test_sina_fund_estimate_parser_uses_daily_change_field(self):
        text = (
            'var hq_str_fu_001015="华夏沪深300指数增强A,10:04:00,2.3053,'
            '2.3130,2.3130,-0.3544,-0.3329,2026-07-21,2.3327,0.8517";'
        )
        quote = _parse_sina_fund_estimate("001015", text)
        self.assertIsNotNone(quote)
        self.assertEqual(quote["quote_date"], "2026-07-21")
        self.assertEqual(quote["quote_time"], "2026-07-21 10:04:00")
        self.assertEqual(quote["source"], "新浪基金估值")
        self.assertAlmostEqual(quote["price"], 2.3053)
        self.assertAlmostEqual(quote["regular_change_pct"], -0.3329)

    def test_sina_fx_history_parser_uses_daily_close(self):
        text = (
            'var _fx_susdcny=("2026-07-17,6.7727,6.7617,6.7823,6.7752,'
            '|2026-07-20,6.7732,6.7534,6.7761,6.7716,");'
        )
        prices = _parse_sina_fx_daily_history(text)
        self.assertEqual(prices, {"2026-07-17": 6.7752, "2026-07-20": 6.7716})

    def test_sparse_quote_push_keeps_last_extended_value_in_same_session(self):
        previous = {
            "session": "postmarket",
            "regular_price": 100.0,
            "prev_close": 98.0,
            "extended_price": 101.0,
            "extended_change_pct": 1.0,
        }
        incoming = {
            "session": "postmarket",
            "regular_price": 100.0,
            "prev_close": 98.0,
            "price": 100.0,
            "extended_price": None,
            "extended_change_pct": None,
        }

        merged = _merge_futu_subscription_quote(previous, incoming)

        self.assertEqual(merged["extended_price"], 101.0)
        self.assertEqual(merged["extended_change_pct"], 1.0)
        self.assertEqual(merged["price"], 101.0)

    def test_closed_session_freezes_last_extended_value(self):
        previous = {
            "session": "postmarket",
            "extended_price": 101.0,
            "extended_change_pct": 1.0,
        }
        incoming = {
            "session": "closed",
            "price": 100.0,
            "extended_price": None,
            "extended_change_pct": None,
        }

        merged = _merge_futu_subscription_quote(previous, incoming)

        self.assertEqual(merged["extended_price"], 101.0)
        self.assertEqual(merged["extended_change_pct"], 1.0)
        self.assertEqual(merged["price"], 101.0)

    def test_extended_ticker_updates_extended_fields_against_regular_close(self):
        quote = {
            "session": "premarket",
            "regular_price": 100.0,
            "prev_close": 99.0,
        }

        updated = _apply_futu_ticker_price(quote, "VOO", 102.0)

        self.assertEqual(updated["extended_price"], 102.0)
        self.assertAlmostEqual(updated["extended_change_pct"], 2.0)
        self.assertAlmostEqual(updated["change_pct"], (102.0 / 99.0 - 1.0) * 100.0)

    def test_regular_ticker_clears_old_extended_fields(self):
        quote = {
            "session": "regular",
            "regular_price": 100.0,
            "prev_close": 99.0,
            "extended_price": 101.0,
            "extended_change_pct": 1.0,
        }

        updated = _apply_futu_ticker_price(quote, "VOO", 102.0)

        self.assertEqual(updated["regular_price"], 102.0)
        self.assertIsNone(updated["extended_price"])
        self.assertIsNone(updated["extended_change_pct"])

    def test_closed_ticker_refreshes_frozen_extended_snapshot(self):
        quote = {
            "session": "closed",
            "regular_price": 100.0,
            "prev_close": 99.0,
            "extended_price": 101.0,
            "extended_change_pct": 1.0,
        }

        updated = _apply_futu_ticker_price(quote, "VOO", 102.0)

        self.assertEqual(updated["extended_price"], 102.0)
        self.assertAlmostEqual(updated["extended_change_pct"], 2.0)


if __name__ == "__main__":
    unittest.main()
