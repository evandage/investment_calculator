import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend import config, main, profiles, storage


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


def test_api_keeps_concurrent_profile_trades_and_universes_separate(tmp_path, monkeypatch):
    monkeypatch.setattr(profiles, "PROFILE_DIR", tmp_path / "profiles")
    monkeypatch.setattr(profiles, "STATE_FILE", tmp_path / "profiles.json")
    monkeypatch.setattr(config, "SATELLITE_UNIVERSE_FILE", tmp_path / "satellite_universe.json")
    for name in (
        "HOLDINGS_FILE", "BALANCES_FILE", "TRADE_RECORDS_FILE", "MONTHLY_USAGE_FILE",
        "DRAWDOWN_EPISODES_FILE", "PORTFOLIO_ADJUSTMENTS_FILE", "PORTFOLIO_HISTORY_FILE",
        "PORTFOLIO_SNAPSHOT_LEDGER_FILE",
    ):
        monkeypatch.setattr(storage, name, tmp_path / getattr(storage, name).name)

    for user_id, symbol in (("evan", "AAA"), ("nave", "BBB")):
        with config.portfolio_profile(user_id):
            config.save_satellite_universe_config([{"symbol": symbol, "target_pct": 100}])
    monkeypatch.setattr(main, "build_dashboard", lambda user_id: {"user_id": user_id})

    async def request(path, user_id=None, payload=None):
        headers = [] if user_id is None else [(b"x-portfolio-profile", user_id.encode())]
        if payload is not None:
            headers.append((b"content-type", b"application/json"))
        messages = []

        async def receive():
            return {"type": "http.request", "body": json.dumps(payload).encode()}

        async def send(message):
            messages.append(message)

        await main.app({
            "type": "http", "method": "GET" if payload is None else "POST",
            "path": path, "query_string": b"", "headers": headers,
        }, receive, send)
        return messages[0]["status"], dict(messages[0]["headers"]), json.loads(messages[1]["body"])

    async def trade(user_id, symbol, other_user_id):
        status, _, response = await request("/api/rebalance/confirm", user_id, {
            "user_id": other_user_id,
            "executions": [{"symbol": symbol, "amount_usd": 50, "shares": 1, "trade_date": "2026-11-25"}],
        })
        assert status == 200
        assert response["dashboard"]["user_id"] == user_id

    async def verify():
        assert (await request("/api/holdings"))[0] == 400
        assert "active_user_id" not in (await request("/api/profiles"))[2]
        await asyncio.gather(trade("evan", "AAA", "nave"), trade("nave", "BBB", "evan"))
        for user_id, symbol in (("nave", "BBB"), ("evan", "AAA")):
            _, headers, trades = await request("/api/trades", user_id)
            assert headers[b"cache-control"] == b"no-store"
            assert [row["symbol"] for row in trades["trades"]] == [symbol]
            holdings = (await request("/api/holdings", user_id))[2]["holdings"]
            assert set(holdings) == {"VOO", "QQQ", "SGOV", symbol}
            assert holdings[symbol]["shares"] == 1

    asyncio.run(verify())
    assert not profiles.STATE_FILE.exists()


if __name__ == "__main__":
    unittest.main()
