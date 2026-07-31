import unittest
from unittest.mock import patch

from backend.ohlcv import _freeze_closed_intraday_bars, _merge_realtime_bar


class TestOhlcvFreeze(unittest.TestCase):
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
