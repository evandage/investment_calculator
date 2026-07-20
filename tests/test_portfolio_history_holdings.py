from __future__ import annotations

import unittest

from backend.portfolio import holdings_snapshot_for_day


class HistoricalHoldingsSnapshotTests(unittest.TestCase):
    def test_first_snapshot_does_not_reapply_trade_already_in_current_holdings(self):
        current = {"001015": {"shares": 12230.51, "avg_cost": 2.3648485968287503}}
        trades = [
            {
                "id": "fund-buy",
                "trade_date": "2026-07-03",
                "created_at": "2026-07-08T00:00:00+08:00",
                "symbol": "001015",
                "action": "buy",
                "amount_usd": 4000.0,
                "shares": 1680.07,
                "prev_avg_cost": 2.3623,
            }
        ]

        snapshot = holdings_snapshot_for_day("2026-07-08", current, [], trades)

        self.assertAlmostEqual(snapshot["001015"]["shares"], 12230.51)
        self.assertAlmostEqual(snapshot["001015"]["avg_cost"], 2.3648485968287503)

    def test_first_snapshot_rewinds_only_future_trades(self):
        current = {"001015": {"shares": 12230.51, "avg_cost": 2.3648485968287503}}
        trades = [
            {
                "id": "fund-buy",
                "trade_date": "2026-07-03",
                "created_at": "2026-07-08T00:00:00+08:00",
                "symbol": "001015",
                "action": "buy",
                "amount_usd": 4000.0,
                "shares": 1680.07,
                "prev_avg_cost": 2.3623,
            }
        ]

        snapshot = holdings_snapshot_for_day("2026-07-02", current, [], trades)

        self.assertAlmostEqual(snapshot["001015"]["shares"], 10550.44)
        self.assertAlmostEqual(snapshot["001015"]["avg_cost"], 2.3623)

    def test_subsequent_snapshot_still_replays_new_trade_once(self):
        previous = {
            "date": "2026-07-02",
            "finalized": True,
            "holdings_snapshot": {"001015": {"shares": 10550.44, "avg_cost": 2.3623}},
        }
        trades = [
            {
                "id": "fund-buy",
                "trade_date": "2026-07-03",
                "created_at": "2026-07-08T00:00:00+08:00",
                "symbol": "001015",
                "action": "buy",
                "amount_usd": 4000.0,
                "shares": 1680.07,
                "prev_avg_cost": 2.3623,
            }
        ]

        snapshot = holdings_snapshot_for_day("2026-07-03", {}, [previous], trades)

        self.assertAlmostEqual(snapshot["001015"]["shares"], 12230.51)
        self.assertAlmostEqual(snapshot["001015"]["avg_cost"], 2.3648485968287503)


if __name__ == "__main__":
    unittest.main()
