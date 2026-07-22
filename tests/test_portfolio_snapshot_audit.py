from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend import storage


class PortfolioSnapshotAuditTests(unittest.TestCase):
    def test_legacy_balances_infer_cash_cost_basis_without_clamping_negative(self):
        normalized = storage.normalize_balances({
            "cash_usd": 5.0,
            "cash_cny": 1085.25,
            "realized_usd": 10.0,
            "realized_cny": 85.25,
            "voo_dividend_usd": 2.0,
            "sgov_dividend_usd": 1.0,
        })

        self.assertAlmostEqual(normalized["cash_cost_basis_usd"], -8.0)
        self.assertAlmostEqual(normalized["cash_cost_basis_cny"], 1000.0)

    def test_finalized_snapshot_revisions_are_append_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            history_file = Path(temp_dir) / "history.json"
            ledger_file = Path(temp_dir) / "ledger.json"
            with (
                patch.object(storage, "PORTFOLIO_HISTORY_FILE", history_file),
                patch.object(storage, "PORTFOLIO_SNAPSHOT_LEDGER_FILE", ledger_file),
            ):
                first = {"date": "2026-07-20", "finalized": True, "total_pnl_cny": -100.0}
                corrected = {"date": "2026-07-20", "finalized": True, "total_pnl_cny": -90.0}

                storage.save_portfolio_history("evan", [first])
                storage.save_portfolio_history("evan", [first])
                storage.save_portfolio_history("evan", [corrected])
                ledger = storage.load_portfolio_snapshot_ledger("evan")

                self.assertEqual(len(ledger), 2)
                self.assertEqual(ledger[0]["snapshot_id"], "2026-07-20-r1")
                self.assertEqual(ledger[1]["snapshot_id"], "2026-07-20-r2")
                self.assertEqual(ledger[1]["supersedes_snapshot_id"], "2026-07-20-r1")
                self.assertEqual(ledger[0]["payload"]["total_pnl_cny"], -100.0)

    def test_manual_adjustment_preserves_before_and_after(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            adjustment_file = Path(temp_dir) / "adjustments.json"
            with patch.object(storage, "PORTFOLIO_ADJUSTMENTS_FILE", adjustment_file):
                record = storage.record_portfolio_adjustment(
                    "evan",
                    "holdings",
                    "2026-07-21",
                    {"VOO": {"shares": 1.0, "avg_cost": 100.0}},
                    {"VOO": {"shares": 1.5, "avg_cost": 101.0}},
                )

                self.assertIsNotNone(record)
                loaded = storage.load_portfolio_adjustments("evan")
                self.assertEqual(len(loaded), 1)
                self.assertEqual(loaded[0]["before"]["VOO"]["shares"], 1.0)
                self.assertEqual(loaded[0]["after"]["VOO"]["shares"], 1.5)

    def test_exact_anchor_can_be_recorded_without_position_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            adjustment_file = Path(temp_dir) / "adjustments.json"
            with patch.object(storage, "PORTFOLIO_ADJUSTMENTS_FILE", adjustment_file):
                holdings = {"QQQ": {"shares": 2.2163, "avg_cost": 720.85}}
                record = storage.record_portfolio_adjustment(
                    "evan",
                    "holdings",
                    "2026-07-22",
                    holdings,
                    holdings,
                    "exact_holdings_anchor_reconciliation",
                    allow_noop=True,
                )

                self.assertIsNotNone(record)
                self.assertEqual(record["before"], record["after"])

    def test_corrected_history_replaces_ledger_and_keeps_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_file = Path(temp_dir) / "ledger.json"
            with patch.object(storage, "PORTFOLIO_SNAPSHOT_LEDGER_FILE", ledger_file):
                storage._write_json(
                    ledger_file,
                    {"evan": [{"snapshot_id": "2026-07-20-r1", "date": "2026-07-20"}]},
                )
                rows = [
                    {
                        "date": "2026-07-20",
                        "finalized": True,
                        "snapshot_schema_version": 4,
                        "fx_rate": 6.7716,
                    }
                ]
                result = storage.replace_snapshot_ledger_with_corrected_history("evan", rows)

                ledger = storage.load_portfolio_snapshot_ledger("evan")
                self.assertEqual(result["snapshot_count"], 1)
                self.assertEqual(ledger[0]["snapshot_id"], "2026-07-20-corrected-r1")
                self.assertEqual(ledger[0]["payload"]["fx_rate"], 6.7716)
                backup_path = Path(result["backup_path"])
                self.assertTrue(backup_path.exists())
                self.assertIn("2026-07-20-r1", backup_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
