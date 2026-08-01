import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from backend.main import _timestamp_for_lightweight
from backend.ohlcv import _displayed_intraday_bars, _freeze_closed_intraday_bars, _merge_realtime_bar, _trading_day_bars


class TestOhlcvFreeze(unittest.TestCase):
    def test_requested_market_date_is_selected(self):
        bars = [
            {"time": 1785457800, "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1},
            {"time": 1785544200, "open": 12, "high": 13, "low": 11, "close": 12, "volume": 2},
        ]

        selected = _trading_day_bars("VOO", bars, "2026-07-31")

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["close"], 12)

    def test_extended_session_excludes_overnight_bars_for_selected_day(self):
        ny = ZoneInfo("America/New_York")

        def bar(hour, minute):
            return {
                "time": int(datetime(2026, 7, 30, hour, minute, tzinfo=ny).timestamp()),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10,
                "volume": 1,
            }

        selected = _displayed_intraday_bars(
            "VOO",
            [bar(0, 0), bar(3, 55), bar(4, 0), bar(9, 30), bar(15, 55), bar(19, 55), bar(20, 0)],
            True,
            "2026-07-30",
        )

        self.assertEqual(
            [datetime.fromtimestamp(row["time"], ny).strftime("%H:%M") for row in selected],
            ["04:00", "09:30", "15:55", "19:55"],
        )

    def test_naive_us_chart_timestamp_uses_new_york_timezone(self):
        timestamp = _timestamp_for_lightweight(datetime(2026, 7, 30, 9, 30), "5m", "VOO")
        rendered = datetime.fromtimestamp(timestamp, ZoneInfo("America/New_York"))

        self.assertEqual(rendered.strftime("%F %R"), "2026-07-30 09:30")

    def test_closed_bars_are_kept_and_latest_bar_can_update(self):
        previous = [
            {"time": 100, "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1},
            {"time": 200, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 2},
        ]
        incoming = [
            {"time": 100, "open": 10, "high": 99, "low": 1, "close": 50, "volume": 9},
            {"time": 200, "open": 10, "high": 13, "low": 9, "close": 12, "volume": 3},
            {"time": 300, "open": 12, "high": 14, "low": 11, "close": 13, "volume": 4},
        ]

        merged = _freeze_closed_intraday_bars(previous, incoming)

        self.assertEqual([bar["time"] for bar in merged], [100, 200, 300])
        self.assertEqual(merged[0]["close"], 10)
        self.assertEqual(merged[1]["close"], 12)
        self.assertEqual(merged[2]["close"], 13)

    @patch("backend.ohlcv.get_futu_subscription_kline")
    def test_stale_subscription_bar_cannot_rewrite_history(self, pushed):
        pushed.return_value = {
            "time_key": "1970-01-01 00:02:30",
            "open": 10,
            "high": 20,
            "low": 5,
            "close": 19,
            "volume": 99,
        }
        bars = [{"time": 300, "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1}]

        merged = _merge_realtime_bar(bars, "VOO", "5m")

        self.assertEqual(merged, bars)


if __name__ == "__main__":
    unittest.main()
