import unittest
from unittest.mock import patch

from backend.portfolio import add_fx_conversion_record, delete_fx_conversion_record


class TestFxConversionCash(unittest.TestCase):
    def test_add_conversion_moves_cash_between_currencies(self):
        holdings = {"VOO": {"shares": 1, "avg_cost": 100}}
        balances = {
            "cash_cny": 15553.0,
            "cash_usd": 20.085,
            "cash_cost_basis_cny": 14396.151160969588,
            "cash_cost_basis_usd": -9.703280602822133,
        }
        saved = {}
        with (
            patch("backend.portfolio.load_user_state", return_value=(holdings, balances, "local")),
            patch("backend.portfolio.load_fx_conversion_records", side_effect=[[], []]),
            patch("backend.portfolio.save_user_state", side_effect=lambda _user, _holdings, value: saved.update(value)),
            patch("backend.portfolio.save_fx_conversion_records"),
            patch("backend.portfolio.record_portfolio_adjustment"),
            patch("backend.portfolio.invalidate_performance_history_from"),
        ):
            result = add_fx_conversion_record("evan", {
                "converted_date": "2026-08-03",
                "cny_amount": 15553,
                "usd_amount": 2290.02,
            })

        self.assertTrue(result["saved"])
        self.assertEqual(saved["cash_cny"], 0.0)
        self.assertAlmostEqual(saved["cash_usd"], 2310.105)
        self.assertAlmostEqual(saved["cash_cost_basis_cny"], -1156.848839030412)
        self.assertAlmostEqual(saved["cash_cost_basis_usd"], 2280.316719397178)

    def test_delete_applied_conversion_reverses_cash_move(self):
        target = {
            "id": "fx-1",
            "converted_date": "2026-08-03",
            "cny_amount": 15553,
            "usd_amount": 2290.02,
            "balance_applied": True,
        }
        balances = {
            "cash_cny": 0.0,
            "cash_usd": 2310.105,
            "cash_cost_basis_cny": -1156.848839030412,
            "cash_cost_basis_usd": 2280.316719397178,
        }
        saved = {}
        with (
            patch("backend.portfolio.load_fx_conversion_records", side_effect=[[target], []]),
            patch("backend.portfolio.load_user_state", return_value=({}, balances, "local")),
            patch("backend.portfolio.save_user_state", side_effect=lambda _user, _holdings, value: saved.update(value)),
            patch("backend.portfolio.save_fx_conversion_records"),
            patch("backend.portfolio.record_portfolio_adjustment"),
            patch("backend.portfolio.invalidate_performance_history_from"),
        ):
            result = delete_fx_conversion_record("evan", "fx-1")

        self.assertTrue(result["deleted"])
        self.assertEqual(saved["cash_cny"], 15553.0)
        self.assertAlmostEqual(saved["cash_usd"], 20.085)


if __name__ == "__main__":
    unittest.main()
