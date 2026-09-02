from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from .models import TelemetryPoint, TelemetrySummary


GENERATED_DIR = Path(__file__).resolve().parents[1] / "generated"
GENERATED_DIR.mkdir(parents=True, exist_ok=True)


def render_trend_chart(session: list[TelemetryPoint], summary: TelemetrySummary) -> Path:
    ordered = sorted(session, key=lambda point: point.timestamp)
    origin = ordered[0].timestamp
    times = [point.timestamp - origin for point in ordered]
    speeds = [point.speedKmh for point in ordered]
    rpms = [point.rpms for point in ordered]
    throttles = [point.throttle * 100 for point in ordered]
    brakes = [point.brake * 100 for point in ordered]
    gears = [point.gear for point in ordered]

    figure, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    figure.patch.set_facecolor("#f7f6f2")

    axes[0].plot(times, speeds, color="#0f766e", linewidth=2.2, label="Speed km/h")
    rpm_axis = axes[0].twinx()
    rpm_axis.plot(times, rpms, color="#b91c1c", linewidth=1.8, alpha=0.85, label="RPM")
    axes[0].set_ylabel("Speed")
    rpm_axis.set_ylabel("RPM")
    axes[0].grid(alpha=0.18)
    axes[0].set_title(f"{summary.car} @ {summary.track} | Session {summary.session_id}")

    axes[1].plot(times, throttles, color="#2563eb", linewidth=2, label="Throttle %")
    axes[1].plot(times, brakes, color="#ea580c", linewidth=2, label="Brake %")
    axes[1].set_ylabel("Inputs %")
    axes[1].set_ylim(-2, 102)
    axes[1].grid(alpha=0.18)
    axes[1].legend(loc="upper right")

    axes[2].step(times, gears, where="mid", color="#7c3aed", linewidth=2.1, label="Gear")
    axes[2].set_ylabel("Gear")
    axes[2].set_xlabel("Seconds")
    axes[2].set_ylim(0.5, 6.5)
    axes[2].grid(alpha=0.18)

    figure.suptitle(
        "ACC WhatsApp Race Engineer | Text + telemetry image response",
        fontsize=15,
        fontweight="bold",
        y=0.98,
    )
    figure.tight_layout()

    output_path = GENERATED_DIR / f"{summary.session_id}_chart.png"
    figure.savefig(output_path, dpi=170, bbox_inches="tight")
    plt.close(figure)
    return output_path


def _draw_box(ax: plt.Axes, x: float, y: float, width: float, height: float, title: str, lines: list[str], color: str) -> None:
    box = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        linewidth=1.5,
        edgecolor="#1f2937",
        facecolor=color,
    )
    ax.add_patch(box)
    ax.text(x + 0.02, y + height - 0.05, title, fontsize=12, fontweight="bold", color="#111827")
    for index, line in enumerate(lines):
        ax.text(x + 0.02, y + height - 0.11 - index * 0.055, line, fontsize=10.5, color="#111827")


def _draw_arrow(ax: plt.Axes, start: tuple[float, float], end: tuple[float, float]) -> None:
    arrow = FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=16, linewidth=1.7, color="#374151")
    ax.add_patch(arrow)


def render_insight_diagram(summary: TelemetrySummary) -> Path:
    figure, ax = plt.subplots(figsize=(12, 7))
    figure.patch.set_facecolor("#fffdf8")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    _draw_box(
        ax,
        0.05,
        0.60,
        0.26,
        0.24,
        "Session",
        [
            f"Car: {summary.car}",
            f"Track: {summary.track}",
            f"Style: {summary.driving_style}",
            f"Avg speed: {summary.avg_speed_kmh} km/h",
        ],
        "#dbeafe",
    )
    _draw_box(
        ax,
        0.37,
        0.60,
        0.24,
        0.24,
        "Powertrain",
        [
            f"Avg RPM: {summary.avg_rpm}",
            f"Max RPM: {summary.max_rpm}",
            f"Overrev: {summary.overrev_events}",
        ],
        "#fee2e2",
    )
    _draw_box(
        ax,
        0.67,
        0.60,
        0.28,
        0.24,
        "Driver Inputs",
        [
            f"Throttle avg: {summary.avg_throttle}",
            f"Brake avg: {summary.avg_brake}",
            f"Overlap: {summary.throttle_brake_overlap_ratio}",
        ],
        "#dcfce7",
    )
    _draw_box(
        ax,
        0.23,
        0.18,
        0.25,
        0.22,
        "Risk Flags",
        [
            f"Heavy braking: {summary.heavy_braking_events}",
            f"Style: {summary.driving_style}",
        ],
        "#fde68a",
    )
    _draw_box(
        ax,
        0.56,
        0.18,
        0.33,
        0.22,
        "Coach Actions",
        summary.recommendations[:2],
        "#e9d5ff",
    )

    _draw_arrow(ax, (0.31, 0.72), (0.37, 0.72))
    _draw_arrow(ax, (0.61, 0.72), (0.67, 0.72))
    _draw_arrow(ax, (0.50, 0.60), (0.35, 0.40))
    _draw_arrow(ax, (0.77, 0.60), (0.73, 0.40))
    _draw_arrow(ax, (0.48, 0.29), (0.56, 0.29))

    ax.text(
        0.5,
        0.93,
        "ACC insight diagram for WhatsApp delivery",
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold",
        color="#111827",
    )
    ax.text(
        0.5,
        0.08,
        "WhatsApp cannot render a live diagram format directly, so the bot sends this PNG instead.",
        ha="center",
        va="center",
        fontsize=10,
        color="#374151",
    )

    output_path = GENERATED_DIR / f"{summary.session_id}_diagram.png"
    figure.savefig(output_path, dpi=170, bbox_inches="tight")
    plt.close(figure)
    return output_path
