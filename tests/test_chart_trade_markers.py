from __future__ import annotations

import unittest

import pandas as pd

from backend.main import _trade_markers_for_chart


class ChartTradeMarkerTests(unittest.TestCase):
    def test_aggregates_buy_and_sell_records_by_day_and_action(self):
        frame = pd.DataFrame(
            {"Close": [100.0, 101.0]},
            index=pd.to_datetime(["2026-07-06", "2026-07-07"]),
        )
        candles = [
            {"time": "2026-07-06", "close": 100.0},
            {"time": "2026-07-07", "close": 101.0},
        ]
        records = [
            {"symbol": "VOO", "trade_date": "2026-07-06", "action": "buy", "shares": 2, "amount_usd": 198},
            {"symbol": "VOO", "trade_date": "2026-07-06", "action": "buy", "shares": 1, "amount_usd": 102},
            {"symbol": "VOO", "trade_date": "2026-07-06", "action": "sell", "shares": 1, "amount_usd": 101},
            {"symbol": "QQQ", "trade_date": "2026-07-06", "action": "buy", "shares": 4, "amount_usd": 800},
            {"symbol": "VOO", "trade_date": "2026-06-01", "action": "buy", "shares": 1, "amount_usd": 90},
        ]

        markers = _trade_markers_for_chart("VOO", "1d", frame, candles, records)

        self.assertEqual(len(markers), 1)
        marker = markers[0]
        self.assertEqual((marker["text"], marker["position"]), ("Ⓣ", "aboveBar"))
        self.assertEqual(marker["action"], "trade")
        self.assertEqual(marker["count"], 3)
        self.assertEqual(marker["buy_count"], 2)
        self.assertEqual(marker["buy_shares"], 3.0)
        self.assertEqual(marker["buy_amount"], 300.0)
        self.assertEqual(marker["buy_average_price"], 100.0)
        self.assertEqual(marker["sell_count"], 1)
        self.assertEqual(marker["sell_shares"], 1.0)
        self.assertEqual(marker["sell_average_price"], 101.0)

    def test_intraday_marker_uses_first_visible_bar_for_trade_date(self):
        index = pd.to_datetime(["2026-07-06 09:30", "2026-07-06 09:35"])
        frame = pd.DataFrame({"Close": [100.0, 101.0]}, index=index)
        candles = [
            {"time": 1000, "close": 100.0},
            {"time": 1300, "close": 101.0},
        ]
        records = [
            {"symbol": "VOO", "trade_date": "2026-07-06", "action": "buy", "shares": 1, "amount_usd": 100},
        ]

        markers = _trade_markers_for_chart("VOO", "5m", frame, candles, records)

        self.assertEqual(markers[0]["time"], 1000)


if __name__ == "__main__":
    unittest.main()
