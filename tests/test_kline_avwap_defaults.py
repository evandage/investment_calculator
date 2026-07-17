import unittest

from backend.main import _default_avwap_mode


class KlineAvwapDefaultTests(unittest.TestCase):
    def test_intraday_intervals_default_to_today_open(self):
        self.assertEqual(_default_avwap_mode("5m", "VOO"), "today_open")
        self.assertEqual(_default_avwap_mode("15m", "ISRG"), "today_open")

    def test_daily_interval_keeps_symbol_specific_default(self):
        self.assertEqual(_default_avwap_mode("1d", "VOO"), "year_start")
        self.assertEqual(_default_avwap_mode("1d", "ISRG"), "earnings")


if __name__ == "__main__":
    unittest.main()
