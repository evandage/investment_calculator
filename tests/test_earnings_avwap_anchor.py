import unittest

import pandas as pd

from chart_boards import _earnings_reaction_date_from_history


class EarningsAvwapAnchorTests(unittest.TestCase):
    def test_after_hours_uses_next_reaction_trading_day(self):
        data = pd.DataFrame(
            [
                {
                    "pub_trading_day_str": "2026-07-16",
                    "pub_time_str": "2026-07-16 17:00:00",
                    "pub_type": 2,
                    "trading_day_str": "2026-07-17",
                }
            ]
        )

        anchor = _earnings_reaction_date_from_history(data, pd.Timestamp("2026-07-17 09:30:00"))

        self.assertEqual(anchor, pd.Timestamp("2026-07-17"))

    def test_premarket_uses_same_trading_day(self):
        data = pd.DataFrame(
            [
                {
                    "pub_trading_day_str": "2026-07-17",
                    "pub_time_str": "2026-07-17 08:00:00",
                    "pub_type": 1,
                    "trading_day_str": "2026-07-17",
                }
            ]
        )

        anchor = _earnings_reaction_date_from_history(data, pd.Timestamp("2026-07-17 09:30:00"))

        self.assertEqual(anchor, pd.Timestamp("2026-07-17"))

    def test_future_release_does_not_replace_previous_anchor(self):
        data = pd.DataFrame(
            [
                {
                    "pub_trading_day_str": "2026-04-16",
                    "pub_time_str": "2026-04-16 17:00:00",
                    "pub_type": 2,
                    "trading_day_str": "2026-04-17",
                },
                {
                    "pub_trading_day_str": "2026-07-17",
                    "pub_time_str": "2026-07-17 17:00:00",
                    "pub_type": 2,
                    "trading_day_str": "2026-07-20",
                },
            ]
        )

        anchor = _earnings_reaction_date_from_history(data, pd.Timestamp("2026-07-17 12:00:00"))

        self.assertEqual(anchor, pd.Timestamp("2026-04-17"))

    def test_after_hours_fallback_advances_when_reaction_field_is_missing(self):
        data = pd.DataFrame(
            [
                {
                    "pub_trading_day_str": "2026-07-16",
                    "pub_time_str": "2026-07-16 17:00:00",
                    "pub_type": 2,
                    "trading_day_str": None,
                }
            ]
        )

        anchor = _earnings_reaction_date_from_history(data, pd.Timestamp("2026-07-17 09:30:00"))

        self.assertEqual(anchor, pd.Timestamp("2026-07-17"))


if __name__ == "__main__":
    unittest.main()
