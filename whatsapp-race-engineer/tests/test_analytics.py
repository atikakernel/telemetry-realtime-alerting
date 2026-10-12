from __future__ import annotations

import unittest

from app.analytics import analyze_session, compare_sessions
from app.models import TelemetryPoint
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


def _session(speed: float, rpm: int, *, brake: float = 0.0, points: int = 20) -> list[TelemetryPoint]:
    return [
        TelemetryPoint(
            timestamp=float(index),
            packetId=index,
            rpms=rpm,
            speedKmh=speed,
            gear=5,
            throttle=0.8,
            brake=brake,
            car="Ferrari 296 GT3",
            track="Monza",
        )
        for index in range(points)
    ]


class CompareSessionsTests(unittest.TestCase):
    def test_faster_cleaner_session_is_improved(self) -> None:
        baseline = analyze_session(_session(180.0, 7900), session_id="base")
        current = analyze_session(_session(195.0, 7200), session_id="now")

        comparison = compare_sessions(baseline, current)

        self.assertEqual(comparison.verdict, "improved")
        self.assertIn("avg_speed_kmh", comparison.improved_metrics)
        self.assertIn("overrev_events", comparison.improved_metrics)
        self.assertEqual(comparison.deltas["avg_speed_kmh"], 15.0)
        self.assertTrue(comparison.highlights)

    def test_slower_session_with_more_overrevs_regresses(self) -> None:
        baseline = analyze_session(_session(195.0, 7200), session_id="base")
        current = analyze_session(_session(180.0, 7900), session_id="now")

        comparison = compare_sessions(baseline, current)

        self.assertEqual(comparison.verdict, "regressed")
        self.assertIn("avg_speed_kmh", comparison.regressed_metrics)
        self.assertIn("overrev_events", comparison.regressed_metrics)

    def test_identical_sessions_are_stable(self) -> None:
        session = _session(190.0, 7200)
        comparison = compare_sessions(
            analyze_session(session, session_id="a"),
            analyze_session(session, session_id="b"),
        )

        self.assertEqual(comparison.verdict, "stable")
        self.assertEqual(comparison.improved_metrics, [])
        self.assertEqual(comparison.regressed_metrics, [])
