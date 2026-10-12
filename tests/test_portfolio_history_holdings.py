from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from backend import portfolio
from backend.portfolio import (
    cash_balances_for_history_day,
    balances_for_history_day,
    completed_daily_pct_for_symbol,
    completed_portfolio_daily_pct,
    compound_satellite_returns,
    current_holdings_pnl_for_history_day,
    daily_fx_change_cny,
    historical_holding_pnl,
    merge_recorded_close_histories,
    holdings_snapshot_for_day,
    reconcile_current_book_daily_pnl,
    total_pnl_for_history_snapshot,
)


class HistoricalHoldingsSnapshotTests(unittest.TestCase):
    def test_satellite_daily_return_uses_saved_previous_close_and_full_basket_basis(self):
        histories = {"TEM": {"2026-09-23": 76.59}, "GOOGL": {}}
        merge_recorded_close_histories(histories, [
            {"date": "2026-09-22", "closing_prices": {"TEM": 77.19, "GOOGL": 351.16}},
        ])
        snapshot = {
            "TEM": {"shares": 2.0, "avg_cost": 52.0},
            "GOOGL": {"shares": 1.0, "avg_cost": 300.0},
        }

        pct, by_symbol, pnl, basis = completed_portfolio_daily_pct(
            snapshot, "2026-09-23", histories, 1.0, symbols={"TEM", "GOOGL"}, native_usd=True
        )

        self.assertAlmostEqual(pnl, 2.0 * (76.59 - 77.19))
        self.assertAlmostEqual(basis, 2.0 * 77.19 + 351.16)
        self.assertAlmostEqual(pct, pnl / basis * 100.0)
        self.assertAlmostEqual(by_symbol["GOOGL"], 0.0)

    def test_satellite_curve_compounds_forward_from_baseline(self):
        points = [
            {"date": "2026-09-21", "satellite_daily_pct": 5.0},
            {"date": "2026-09-22", "satellite_daily_pct": 10.0},
            {"date": "2026-09-23", "satellite_daily_pct": -5.0},
        ]

        compound_satellite_returns(points)

        self.assertAlmostEqual(points[0]["satellite_return_pct"], 0.0)
        self.assertAlmostEqual(points[1]["satellite_return_pct"], 10.0)
        self.assertAlmostEqual(points[2]["satellite_return_pct"], 4.5)

    def test_missing_previous_close_cannot_turn_cost_gain_into_daily_gain(self):
        histories = {
            "TEM": {"2026-09-23": 76.59},
            "GOOGL": {"2026-09-22": 350.0, "2026-09-23": 353.5},
        }
        snapshot = {
            "TEM": {"shares": 2.0, "avg_cost": 50.0},
            "GOOGL": {"shares": 1.0, "avg_cost": 200.0},
        }

        pct, by_symbol, pnl, basis = completed_portfolio_daily_pct(
            snapshot, "2026-09-23", histories, 1.0, symbols={"TEM", "GOOGL"}, native_usd=True
        )

        self.assertAlmostEqual(by_symbol["TEM"], 0.0)
        self.assertAlmostEqual(pnl, 3.5)
        self.assertAlmostEqual(basis, 2.0 * 76.59 + 350.0)
        self.assertAlmostEqual(pct, pnl / basis * 100.0)

        snapshot["TEM"]["avg_cost"] = 10.0
        changed_cost_pct, _, _, _ = completed_portfolio_daily_pct(
            snapshot, "2026-09-23", histories, 1.0, symbols={"TEM", "GOOGL"}, native_usd=True
        )
        self.assertAlmostEqual(changed_cost_pct, pct)

    def test_new_buy_uses_trade_price_when_previous_close_is_missing(self):
        pct, by_symbol, pnl, basis = completed_portfolio_daily_pct(
            {"TEM": {"shares": 2.0, "avg_cost": 50.0}},
            "2026-09-23",
            {"TEM": {"2026-09-23": 55.0}},
            1.0,
            [{"trade_date": "2026-09-23", "symbol": "TEM", "action": "buy", "shares": 2.0, "amount_usd": 100.0}],
            symbols={"TEM"},
            native_usd=True,
        )

        self.assertAlmostEqual(by_symbol["TEM"], 10.0)
        self.assertAlmostEqual(pnl, 10.0)
        self.assertAlmostEqual(basis, 100.0)
        self.assertAlmostEqual(pct, 10.0)


    def test_cny_sale_profit_is_not_counted_as_cash_principal(self):
        result = total_pnl_for_history_snapshot(
            {"date": "2026-07-21", "holdings_snapshot": {}, "holding_pnl_cny": 0.0, "fx_rate": 7.0},
            {
                "cash_cny": 1085.25,
                "cash_usd": 0.0,
                "cash_cost_basis_cny": 1000.0,
                "cash_cost_basis_usd": 0.0,
                "realized_cny": 85.25,
                "realized_usd": 0.0,
            },
            [],
            6.8,
            7.0,
        )

        self.assertAlmostEqual(result["total_pnl_cny"], 85.25)
        self.assertAlmostEqual(result["total_return_basis_cny"], 1000.0)

    def test_usd_realized_profit_fx_is_not_counted_twice(self):
        result = total_pnl_for_history_snapshot(
            {"date": "2026-07-21", "holdings_snapshot": {}, "holding_pnl_cny": 0.0, "fx_rate": 7.0},
            {
                "cash_usd": 110.0,
                "cash_cny": 0.0,
                "cash_cost_basis_usd": 100.0,
                "cash_cost_basis_cny": 0.0,
                "realized_usd": 10.0,
                "realized_cny": 0.0,
            },
            [],
            6.8,
            7.0,
        )

        self.assertAlmostEqual(result["fx_pnl_cny"], 20.0)
        self.assertAlmostEqual(result["realized_pnl_cny"], 70.0)
        self.assertAlmostEqual(result["total_pnl_cny"], 90.0)
        self.assertAlmostEqual(result["total_return_basis_cny"], 680.0)


    def test_daily_fx_change_uses_previous_usd_assets_and_cash(self):
        pnl_cny, exposure_usd, previous_fx = daily_fx_change_cny(
            6.7569,
            {"fx_rate": 6.7711, "usd_value_usd": 5849.379133, "cash_usd": 2727.4},
        )

        self.assertAlmostEqual(exposure_usd, 8576.779133)
        self.assertAlmostEqual(previous_fx, 6.7711)
        self.assertAlmostEqual(pnl_cny, -121.7902636886)

    def test_daily_fx_change_does_not_use_cny_assets_or_cost_basis(self):
        pnl_cny, exposure_usd, _ = daily_fx_change_cny(
            7.1,
            {
                "fx_rate": 7.0,
                "usd_value_usd": 1000.0,
                "cash_usd": 200.0,
                "holding_cost_cny": 500000.0,
                "total_return_basis_cny": 800000.0,
            },
        )

        self.assertAlmostEqual(exposure_usd, 1200.0)
        self.assertAlmostEqual(pnl_cny, 120.0)

    def test_cash_history_rewinds_from_current_trusted_balance(self):
        balances = {"cash_usd": 2727.4, "cash_cny": 28.02}
        adjustments = [
            {
                "kind": "balances",
                "effective_date": "2026-07-21",
                "reconstruct_from_date": "2026-07-16",
                "recorded_at": "2026-07-21T13:14:12+08:00",
                "after": balances,
            }
        ]
        trades = [
            {"trade_date": "2026-07-17", "symbol": "TEM", "action": "buy", "amount_usd": 36.0},
            {"trade_date": "2026-07-17", "symbol": "PLTR", "action": "buy", "amount_usd": 15.0},
            {"trade_date": "2026-07-17", "symbol": "QQQ", "action": "buy", "amount_usd": 50.0},
            {"trade_date": "2026-07-20", "symbol": "TEM", "action": "buy", "amount_usd": 20.0},
            {"trade_date": "2026-07-20", "symbol": "ISRG", "action": "buy", "amount_usd": 40.0},
        ]

        self.assertEqual(cash_balances_for_history_day("2026-07-16", balances, trades, adjustments), (2888.4, 28.02))
        self.assertEqual(cash_balances_for_history_day("2026-07-17", balances, trades, adjustments), (2787.4, 28.02))
        self.assertEqual(cash_balances_for_history_day("2026-07-20", balances, trades, adjustments), (2727.4, 28.02))
        self.assertEqual(cash_balances_for_history_day("2026-07-21", balances, trades, adjustments), (2727.4, 28.02))

    def test_cash_history_preserves_old_snapshot_before_reconstruction_window(self):
        adjustments = [
            {
                "kind": "balances",
                "effective_date": "2026-07-21",
                "reconstruct_from_date": "2026-07-16",
                "after": {"cash_usd": 2727.4, "cash_cny": 28.02},
            }
        ]

        cash = cash_balances_for_history_day(
            "2026-07-15",
            {"cash_usd": 2727.4, "cash_cny": 28.02},
            [],
            adjustments,
            3000.0,
            20.0,
        )

        self.assertEqual(cash, (3000.0, 20.0))

    def test_reconciled_daily_pnl_bridges_adjacent_cumulative_points(self):
        rows = [
            {"date": "2026-07-20", "total_pnl_cny": -1900.0, "total_return_basis_cny": 90000.0, "holding_daily_pnl_cny": 12.0},
            {"date": "2026-07-21", "total_pnl_cny": -1750.0, "total_return_basis_cny": 90000.0, "holding_daily_pnl_cny": 25.0},
        ]
        reconciled = reconcile_current_book_daily_pnl(rows)
        self.assertAlmostEqual(reconciled[1]["holding_daily_pnl_cny"], 25.0)
        self.assertAlmostEqual(reconciled[1]["total_daily_pnl_cny"], 150.0)
        self.assertAlmostEqual(
            reconciled[0]["total_pnl_cny"] + reconciled[1]["total_daily_pnl_cny"],
            reconciled[1]["total_pnl_cny"],
        )
        self.assertTrue(reconciled[1]["daily_pnl_reconciled"])

    def test_latest_same_day_cash_anchor_wins(self):
        adjustments = [
            {
                "kind": "balances",
                "effective_date": "2026-07-21",
                "reconstruct_from_date": "2026-07-16",
                "recorded_at": "2026-07-21T09:00:00+08:00",
                "after": {"cash_usd": 2727.4, "cash_cny": 28.02},
            },
            {
                "kind": "balances",
                "effective_date": "2026-07-21",
                "reconstruct_from_date": "2026-07-16",
                "recorded_at": "2026-07-21T16:00:00+08:00",
                "after": {"cash_usd": 111.93, "cash_cny": 10090.11635},
            },
        ]

        self.assertEqual(
            cash_balances_for_history_day("2026-07-21", {}, [], adjustments),
            (111.93, 10090.11635),
        )


    def test_market_return_is_separate_from_trade_aware_position_return(self):
        histories = {"TEM": {"2026-07-09": 100.0, "2026-07-10": 90.0}}
        snapshot = {"TEM": {"shares": 2.0, "avg_cost": 95.0}}
        trades = [
            {
                "trade_date": "2026-07-10",
                "symbol": "TEM",
                "action": "buy",
                "shares": 1.0,
                "amount_usd": 95.0,
            }
        ]

        market_pct = completed_daily_pct_for_symbol("TEM", "2026-07-10", histories)
        _, position_pcts, _, _ = completed_portfolio_daily_pct(
            snapshot, "2026-07-10", histories, 1.0, trades
        )

        self.assertAlmostEqual(market_pct, -10.0)
        self.assertAlmostEqual(position_pcts["TEM"], -15.0 / 195.0 * 100.0)




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

    def test_current_book_history_includes_realized_pnl_in_cumulative_total(self):
        pnl = current_holdings_pnl_for_history_day(
            "2026-07-20",
            {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
            {"VOO": 105.0},
            {"cash_usd": 0.0, "cash_cny": 0.0, "realized_usd": 2.0, "realized_cny": 3.0},
            7.0,
            7.0,
        )

        self.assertAlmostEqual(pnl["usd_pnl_usd"], 7.0)
        self.assertAlmostEqual(pnl["holding_pnl_cny"], 52.0)
        self.assertAlmostEqual(pnl["total_pnl_cny"], 52.0)

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





if __name__ == "__main__":
    unittest.main()
