from __future__ import annotations

from statistics import mean
from uuid import uuid4

from .models import TelemetryPoint, TelemetrySummary


def _infer_driving_style(
    avg_throttle: float,
    avg_brake: float,
    overlap_ratio: float,
    heavy_braking_events: int,
) -> str:
    if overlap_ratio > 0.08 or heavy_braking_events > 12:
        return "aggressive"
    if avg_throttle > 0.64 and avg_brake < 0.16:
        return "smooth"
    if avg_brake > 0.23:
        return "defensive"
    return "balanced"


def _build_recommendations(
    overrev_events: int,
    heavy_braking_events: int,
    overlap_ratio: float,
    avg_speed_kmh: float,
) -> list[str]:
    recommendations: list[str] = []

    if overrev_events > 6:
        recommendations.append("Baja un cambio de marcha mas temprano para evitar sobre regimen al final de recta.")
    if heavy_braking_events > 10:
        recommendations.append("Tu frenada esta fuerte en alta velocidad; prueba soltar freno antes para estabilizar el coche.")
    if overlap_ratio > 0.07:
        recommendations.append("Hay demasiada superposicion entre throttle y brake; limpia la transicion para ganar consistencia.")
    if avg_speed_kmh < 165:
        recommendations.append("La velocidad media esta baja para Monza; revisa salidas de curva y traccion.")

    if not recommendations:
        recommendations.append("La sesion se ve estable; enfocate en repetir tus puntos de frenada y mantener salida limpia.")

    return recommendations


def analyze_session(session: list[TelemetryPoint], session_id: str | None = None) -> TelemetrySummary:
    if len(session) < 5:
        raise ValueError("Telemetry session must include at least five points.")

    ordered = sorted(session, key=lambda point: point.timestamp)
    timestamps = [point.timestamp for point in ordered]
    speeds = [point.speedKmh for point in ordered]
    rpms = [point.rpms for point in ordered]
    throttles = [point.throttle for point in ordered]
    brakes = [point.brake for point in ordered]

    duration_seconds = max(1.0, timestamps[-1] - timestamps[0])
    avg_speed_kmh = round(mean(speeds), 2)
    max_speed_kmh = round(max(speeds), 2)
    avg_rpm = round(mean(rpms), 2)
    max_rpm = max(rpms)
    avg_throttle = round(mean(throttles), 3)
    avg_brake = round(mean(brakes), 3)
    overrev_events = sum(1 for rpm in rpms if rpm > 7500)
    heavy_braking_events = sum(
        1 for point in ordered if point.brake > 0.78 and point.speedKmh > 145.0
    )
    overlap_events = sum(
        1 for point in ordered if point.throttle > 0.24 and point.brake > 0.24
    )
    throttle_brake_overlap_ratio = round(overlap_events / len(ordered), 3)

    driving_style = _infer_driving_style(
        avg_throttle=avg_throttle,
        avg_brake=avg_brake,
        overlap_ratio=throttle_brake_overlap_ratio,
        heavy_braking_events=heavy_braking_events,
    )
    recommendations = _build_recommendations(
        overrev_events=overrev_events,
        heavy_braking_events=heavy_braking_events,
        overlap_ratio=throttle_brake_overlap_ratio,
        avg_speed_kmh=avg_speed_kmh,
    )

    reference = ordered[-1]
    return TelemetrySummary(
        session_id=session_id or uuid4().hex[:12],
        duration_seconds=round(duration_seconds, 2),
        avg_speed_kmh=avg_speed_kmh,
        max_speed_kmh=max_speed_kmh,
        avg_rpm=avg_rpm,
        max_rpm=max_rpm,
        avg_throttle=avg_throttle,
        avg_brake=avg_brake,
        overrev_events=overrev_events,
        heavy_braking_events=heavy_braking_events,
        throttle_brake_overlap_ratio=throttle_brake_overlap_ratio,
        driving_style=driving_style,
        car=reference.car,
        track=reference.track,
        recommendations=recommendations,
    )
