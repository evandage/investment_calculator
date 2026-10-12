import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend import config, profiles, storage


class ProfileStorageTests(unittest.TestCase):
    def test_switch_keeps_legacy_evan_data_and_separates_nave(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holdings_file = root / "holdings.json"
            balances_file = root / "balances.json"
            trades_file = root / "trades.json"
            holdings_file.write_text(json.dumps({"VOO": {"shares": 3, "avg_cost": 400}}))
            balances_file.write_text(json.dumps({"cash_usd": 125}))
            trades_file.write_text(json.dumps({"evan": [{"symbol": "VOO", "action": "buy", "shares": 1, "amount_usd": 400, "trade_date": "2026-01-01"}]}))
            with (
                patch.object(profiles, "STATE_FILE", root / "profiles.json"),
                patch.object(profiles, "PROFILE_DIR", root / "profiles"),
                patch.object(storage, "HOLDINGS_FILE", holdings_file),
                patch.object(storage, "BALANCES_FILE", balances_file),
                patch.object(storage, "TRADE_RECORDS_FILE", trades_file),
            ):
                self.assertEqual(profiles.active_user_id(), "evan")
                self.assertEqual(storage.load_holdings()["VOO"]["shares"], 3)
                self.assertEqual(storage.load_balances()["cash_usd"], 125)
                self.assertEqual(len(storage.load_trade_records()), 1)
                profiles.switch_profile("nave")
                self.assertEqual(storage.load_holdings()["VOO"]["shares"], 0)
                self.assertEqual(storage.load_balances()["cash_usd"], 0)
                self.assertEqual(storage.load_trade_records(), [])
                nave_holdings = storage.load_holdings()
                nave_holdings["VOO"]["shares"] = 7
                storage.save_holdings(nave_holdings)
                nave_balances = storage.load_balances()
                nave_balances["cash_usd"] = 90
                storage.save_balances(nave_balances)
                profiles.switch_profile("evan")
                self.assertEqual(storage.load_holdings()["VOO"]["shares"], 3)
                self.assertEqual(storage.load_balances()["cash_usd"], 125)
                self.assertEqual(len(storage.load_trade_records()), 1)
                self.assertEqual(storage.load_holdings("nave")["VOO"]["shares"], 7)
                self.assertEqual(storage.load_balances("nave")["cash_usd"], 90)

    def test_satellite_universe_changes_with_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(profiles, "STATE_FILE", root / "profiles.json"),
                patch.object(profiles, "PROFILE_DIR", root / "profiles"),
                patch.object(config, "SATELLITE_UNIVERSE_FILE", root / "satellite_universe.json"),
            ):
                try:
                    profiles.switch_profile("nave")
                    config.save_satellite_universe_config([{"symbol": "AMD", "label": "AMD", "target_pct": 100}])
                    self.assertIn("AMD", config.SATELLITE_SYMBOLS)
                    profiles.switch_profile("evan")
                    config._apply_satellite_universe()
                    self.assertNotIn("AMD", config.SATELLITE_SYMBOLS)
                    profiles.switch_profile("nave")
                    config._apply_satellite_universe()
                    self.assertIn("AMD", config.SATELLITE_SYMBOLS)
                finally:
                    profiles.switch_profile("evan")
            config._apply_satellite_universe()


if __name__ == "__main__":
    unittest.main()
