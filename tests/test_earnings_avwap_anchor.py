import unittest

import pandas as pd

from chart_boards import _avwap_anchor_date, _earnings_reaction_date_from_history


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

    def test_cycle_high_uses_persisted_cycle_start_date(self):
        index = pd.date_range("2026-07-01", periods=8, freq="D")
        daily = pd.DataFrame(
            {
                "High": [10, 11, 12, 11, 10, 9, 10, 11],
                "Low": [9, 8, 7, 6, 5, 7, 8, 9],
                "Close": [9.5, 10, 11, 10, 9, 8, 9, 10],
            },
            index=index,
        )

        anchor, label = _avwap_anchor_date(
            "VOO",
            daily,
            daily,
            "cycle_high",
            cycle_start_date="2026-07-03",
        )

        self.assertEqual(anchor, pd.Timestamp("2026-07-03"))
        self.assertEqual(label, "周期高点")

    def test_cycle_low_is_lowest_low_after_cycle_high(self):
        index = pd.date_range("2026-07-01", periods=8, freq="D")
        daily = pd.DataFrame(
            {
                "High": [10, 11, 12, 11, 10, 9, 10, 11],
                "Low": [9, 8, 7, 6, 5, 7, 8, 9],
                "Close": [9.5, 10, 11, 10, 9, 8, 9, 10],
            },
            index=index,
        )

        anchor, label = _avwap_anchor_date(
            "VOO",
            daily,
            daily,
            "cycle_low",
            cycle_start_date="2026-07-03",
        )

        self.assertEqual(anchor, pd.Timestamp("2026-07-05"))
        self.assertEqual(label, "周期低点")

    def test_custom_anchor_moves_weekend_to_next_trading_day(self):
        index = pd.to_datetime(["2026-07-03", "2026-07-06", "2026-07-07"])
        daily = pd.DataFrame(
            {
                "High": [10, 11, 12],
                "Low": [8, 9, 10],
                "Close": [9, 10, 11],
            },
            index=index,
        )

        anchor, label = _avwap_anchor_date(
            "VOO",
            daily,
            daily,
            "custom",
            custom_anchor_date="2026-07-04",
        )

        self.assertEqual(anchor, pd.Timestamp("2026-07-06"))
        self.assertEqual(label, "自定义日期")


if __name__ == "__main__":
    unittest.main()
