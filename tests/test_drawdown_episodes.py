from __future__ import annotations

import unittest

from backend.drawdown_episodes import (
    advance_episode_on_close,
    default_episode_state,
    ensure_threshold_snapshot,
    intraday_warning,
)


def rule(small: float = -5.0, medium: float = -10.0, large: float = -15.0):
    return {
        "normal": (1.0, "正常", "1x", "normal"),
        "bands": [
            (large, 4.0, "大加", "4x", "large"),
            (medium, 2.5, "中加", "2.5x", "medium"),
            (small, 1.5, "小加", "1.5x", "small"),
        ],
    }


class DrawdownEpisodeTests(unittest.TestCase):
    def setUp(self):
        self.store = {"threshold_snapshots": {}, "episodes": {}}
        self.january, _ = ensure_threshold_snapshot(
            self.store,
            symbol="VOO",
            phase="建仓期",
            month_key="2026-01",
            rule=rule(),
            created_at="2026-01-31T16:05:00-05:00",
        )

    def advance(self, state, day, drawdown, snapshot=None, close=100.0):
        return advance_episode_on_close(
            symbol="VOO",
            state=state,
            current_snapshot=snapshot or self.january,
            snapshots=self.store["threshold_snapshots"],
            confirmed_close_date=day,
            confirmed_close_price=close,
            confirmed_drawdown_pct=drawdown,
        )

    def test_intraday_warning_does_not_mutate_episode(self):
        state = default_episode_state("VOO")
        before = dict(state)
        warning = intraday_warning(
            symbol="VOO",
            intraday_drawdown_pct=-6.0,
            current_price=94.0,
            session="regular",
            state=state,
            current_snapshot=self.january,
            snapshots=self.store["threshold_snapshots"],
            as_of="2026-02-02T10:30:00-05:00",
        )
        self.assertTrue(warning["active"])
        self.assertEqual(warning["tier"], "small")
        self.assertEqual(state, before)
        self.assertFalse(state["episode_active"])

    def test_close_confirms_once_and_refresh_is_idempotent(self):
        state, first_signal, changed = self.advance(None, "2026-02-02", -6.0)
        self.assertTrue(changed)
        self.assertTrue(first_signal["newly_triggered"])
        self.assertEqual(first_signal["tier"], "small")
        self.assertTrue(state["episode_active"])
        self.assertTrue(state["small_triggered"])

        repeated, repeated_signal, repeated_changed = self.advance(state, "2026-02-02", -7.0)
        self.assertFalse(repeated_changed)
        self.assertFalse(repeated_signal["newly_triggered"])
        self.assertEqual(repeated_signal["id"], first_signal["id"])
        self.assertEqual(repeated, state)

    def test_each_tier_triggers_only_once(self):
        state, small, _ = self.advance(None, "2026-02-02", -6.0)
        state, medium, _ = self.advance(state, "2026-02-03", -11.0)
        self.assertEqual(medium["tier"], "medium")
        self.assertTrue(state["small_triggered"])
        self.assertTrue(state["medium_triggered"])
        self.assertFalse(state["large_triggered"])

        state_again, same_signal, _ = self.advance(state, "2026-02-04", -12.0)
        self.assertFalse(same_signal["newly_triggered"])
        self.assertEqual(same_signal["id"], medium["id"])
        self.assertEqual(state_again["trigger_dates"]["medium"], "2026-02-03")

        state_large, large, _ = self.advance(state_again, "2026-02-05", -16.0)
        self.assertEqual(large["tier"], "large")
        self.assertTrue(state_large["large_triggered"])

    def test_gap_to_large_marks_all_crossed_tiers_in_one_event(self):
        state, signal, _ = self.advance(None, "2026-02-02", -16.0)
        self.assertEqual(signal["tier"], "large")
        self.assertTrue(state["small_triggered"])
        self.assertTrue(state["medium_triggered"])
        self.assertTrue(state["large_triggered"])

    def test_new_month_snapshot_does_not_change_active_episode(self):
        state, _, _ = self.advance(None, "2026-01-30", -6.0)
        february, created = ensure_threshold_snapshot(
            self.store,
            symbol="VOO",
            phase="建仓期",
            month_key="2026-02",
            rule=rule(-3.0, -7.0, -12.0),
            created_at="2026-02-01T00:00:00-05:00",
        )
        self.assertTrue(created)
        state, signal, _ = self.advance(state, "2026-02-02", -8.0, snapshot=february)
        self.assertEqual(state["threshold_snapshot_id"], self.january["id"])
        self.assertEqual(signal["tier"], "small")
        self.assertFalse(signal["newly_triggered"])
        self.assertFalse(state["medium_triggered"])

    def test_existing_snapshot_is_immutable(self):
        same, created = ensure_threshold_snapshot(
            self.store,
            symbol="VOO",
            phase="建仓期",
            month_key="2026-01",
            rule=rule(-1.0, -2.0, -3.0),
            created_at="2026-01-31T23:59:00-05:00",
        )
        self.assertFalse(created)
        self.assertEqual(same["thresholds_pct"]["small"], -5.0)

    def test_episode_ends_at_new_60_day_high(self):
        state, _, _ = self.advance(None, "2026-02-02", -6.0)
        state, signal, _ = self.advance(state, "2026-02-03", 0.0)
        self.assertFalse(state["episode_active"])
        self.assertEqual(state["end_reason"], "new_60d_high")
        self.assertFalse(signal["active"])

    def test_episode_ends_after_ten_recovery_closes(self):
        state, _, _ = self.advance(None, "2026-02-01", -6.0)
        for day in range(2, 12):
            state, _signal, _ = self.advance(state, f"2026-02-{day:02d}", -2.0)
        self.assertFalse(state["episode_active"])
        self.assertEqual(state["end_reason"], "recovered_within_half_small_for_10_days")


if __name__ == "__main__":
    unittest.main()
