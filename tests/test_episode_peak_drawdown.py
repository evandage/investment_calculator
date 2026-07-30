from __future__ import annotations

import unittest

import pandas as pd

from analysis.drawdown_thresholds import episode_peak_drawdown


class EpisodePeakDrawdownTests(unittest.TestCase):
    def test_peak_does_not_disappear_when_day_sixty_one_arrives(self):
        close = pd.Series(
            [100.0] + [90.0] * 70,
            index=pd.date_range("2026-01-01", periods=71, freq="B"),
        )

        drawdown = episode_peak_drawdown(close)

        self.assertAlmostEqual(float(drawdown.iloc[-1]), 0.10)


if __name__ == "__main__":
    unittest.main()
