from datetime import datetime
import unittest
from zoneinfo import ZoneInfo

from backend.ohlcv import _futu_ts_to_lightweight


def _local_time(timestamp: int, timezone: str) -> datetime:
    return datetime.fromtimestamp(timestamp, ZoneInfo(timezone))


class FutuKlineTimestampTests(unittest.TestCase):
    def test_futu_one_minute_bar_uses_interval_start_for_us_market(self):
        timestamp = _futu_ts_to_lightweight("2026-08-13 09:31:00", "1m", "VOO")

        self.assertIsInstance(timestamp, int)
        self.assertEqual(_local_time(timestamp, "America/New_York").strftime("%H:%M"), "09:30")


    def test_futu_fifteen_minute_bar_uses_interval_start_for_us_market(self):
        timestamp = _futu_ts_to_lightweight("2026-07-17 09:45:00", "15m", "VOO")

        self.assertIsInstance(timestamp, int)
        self.assertEqual(_local_time(timestamp, "America/New_York").strftime("%H:%M"), "09:30")


    def test_futu_daily_bar_keeps_its_trading_date(self):
        self.assertEqual(
            _futu_ts_to_lightweight("2026-07-17 00:00:00", "1d", "VOO"),
            "2026-07-17",
        )


if __name__ == "__main__":
    unittest.main()
