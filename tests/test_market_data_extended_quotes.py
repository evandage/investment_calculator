from __future__ import annotations

import unittest
from unittest.mock import patch

from backend.market_data import (
    _apply_futu_ticker_price,
    _build_futu_quote,
    _merge_futu_subscription_quote,
    _parse_sina_fx_daily_history,
    _futu_subscription_groups,
)


class ExtendedQuoteStabilityTests(unittest.TestCase):





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
