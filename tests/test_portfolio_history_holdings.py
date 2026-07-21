from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from backend.portfolio import (
    current_holdings_pnl_for_history_day,
    fund_daily_status,
    historical_holding_pnl,
    holdings_snapshot_for_day,
    reconcile_current_book_daily_pnl,
)


class HistoricalHoldingsSnapshotTests(unittest.TestCase):
    def test_reconciled_daily_pnl_bridges_adjacent_cumulative_points(self):
        rows = [
            {"date": "2026-07-20", "total_pnl_cny": -1900.0, "total_return_basis_cny": 90000.0},
            {"date": "2026-07-21", "total_pnl_cny": -1750.0, "total_return_basis_cny": 90000.0},
        ]
        reconciled = reconcile_current_book_daily_pnl(rows)
        self.assertAlmostEqual(reconciled[1]["holding_daily_pnl_cny"], 150.0)
        self.assertAlmostEqual(
            reconciled[0]["total_pnl_cny"] + reconciled[1]["holding_daily_pnl_cny"],
            reconciled[1]["total_pnl_cny"],
        )
        self.assertTrue(reconciled[1]["daily_pnl_reconciled"])

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

    def test_subsequent_snapshot_can_add_a_symbol_missing_from_previous_row(self):
        previous = {
            "date": "2026-07-07",
            "finalized": True,
            "holdings_snapshot": {"VOO": {"shares": 1.0, "avg_cost": 600.0}},
        }
        current = {
            "VOO": {"shares": 1.0, "avg_cost": 600.0},
            "PLTR": {"shares": 0.2, "avg_cost": 125.0},
        }
        trades = [
            {
                "trade_date": "2026-07-08",
                "symbol": "PLTR",
                "action": "buy",
                "amount_usd": 25.0,
                "shares": 0.2,
            }
        ]

        snapshot = holdings_snapshot_for_day("2026-07-08", current, [previous], trades)

        self.assertAlmostEqual(snapshot["PLTR"]["shares"], 0.2)
        self.assertAlmostEqual(snapshot["PLTR"]["avg_cost"], 125.0)

    def test_historical_usd_pnl_uses_live_dividend_basis(self):
        pnl = historical_holding_pnl(
            {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
            {"VOO": 110.0},
            7.0,
            {"voo_dividend_usd": 2.0, "sgov_dividend_usd": 1.0},
            {"VOO"},
            True,
        )

        self.assertAlmostEqual(pnl["amount_cny"], 13.0)
        self.assertAlmostEqual(pnl["pct"], 13.0)

    def test_current_book_history_pnl_keeps_current_cash_and_cost_basis(self):
        pnl = current_holdings_pnl_for_history_day(
            "2026-07-20",
            {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
            {"VOO": 90.0},
            {"cash_usd": 10.0, "cash_cny": 0.0},
            7.0,
            6.8,
        )

        self.assertAlmostEqual(pnl["usd_pnl_usd"], -10.0)
        self.assertAlmostEqual(pnl["total_pnl_cny"], -48.0)
        self.assertAlmostEqual(pnl["total_return_basis_cny"], 748.0)

    def test_current_book_history_includes_frozen_closed_position_pnl(self):
        pnl = current_holdings_pnl_for_history_day(
            "2026-07-20",
            {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
            {"VOO": 105.0},
            {"cash_usd": 0.0, "cash_cny": 0.0},
            7.0,
            7.0,
            -10.76,
        )

        self.assertAlmostEqual(pnl["usd_pnl_usd"], -5.76)
        self.assertAlmostEqual(pnl["holding_pnl_cny"], -40.32)
        self.assertAlmostEqual(pnl["total_pnl_cny"], -40.32)

    def test_manual_holding_adjustment_is_applied_after_trades(self):
        previous = {
            "date": "2026-07-20",
            "finalized": True,
            "holdings_snapshot": {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
        }
        adjustments = [
            {
                "kind": "holdings",
                "effective_date": "2026-07-21",
                "recorded_at": "2026-07-21T10:00:00+08:00",
                "before": {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
                "after": {"VOO": {"shares": 1.5, "avg_cost": 101.0}},
            }
        ]

        snapshot = holdings_snapshot_for_day(
            "2026-07-21",
            {"VOO": {"shares": 1.5, "avg_cost": 101.0}},
            [previous],
            [],
            adjustments,
        )

        self.assertAlmostEqual(snapshot["VOO"]["shares"], 1.5)
        self.assertAlmostEqual(snapshot["VOO"]["avg_cost"], 101.0)

    def test_stale_fund_estimate_is_pending_after_open(self):
        status = fund_daily_status(
            {"quote_date": "2026-07-20", "source": "东方财富基金估算"},
            "2026-07-21",
            datetime(2026, 7, 21, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )

        self.assertEqual(status, "pending")

    def test_same_day_fund_estimate_is_used_during_session(self):
        status = fund_daily_status(
            {"quote_date": "2026-07-21", "source": "东方财富基金估算"},
            "2026-07-21",
            datetime(2026, 7, 21, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )

        self.assertEqual(status, "estimated")


if __name__ == "__main__":
    unittest.main()
