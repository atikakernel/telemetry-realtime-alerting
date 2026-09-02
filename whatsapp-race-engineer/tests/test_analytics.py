from __future__ import annotations

import unittest

from app.analytics import analyze_session
from app.sample_data import build_sample_session


class AnalyticsTests(unittest.TestCase):
    def test_sample_session_produces_reasonable_metrics(self) -> None:
        summary = analyze_session(build_sample_session(points=180), session_id="test-session")

        self.assertEqual(summary.session_id, "test-session")
        self.assertGreater(summary.avg_speed_kmh, 140.0)
        self.assertGreater(summary.max_speed_kmh, 230.0)
        self.assertGreater(summary.max_rpm, 7000)
        self.assertGreaterEqual(summary.overrev_events, 1)
        self.assertTrue(summary.recommendations)
        self.assertIn(summary.driving_style, {"aggressive", "balanced", "defensive", "smooth"})


if __name__ == "__main__":
    unittest.main()
