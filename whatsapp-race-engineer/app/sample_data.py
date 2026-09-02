from __future__ import annotations

import math
import time

from .models import TelemetryPoint


def _gear_for_speed(speed_kmh: float) -> int:
    if speed_kmh < 45:
        return 1
    if speed_kmh < 75:
        return 2
    if speed_kmh < 110:
        return 3
    if speed_kmh < 150:
        return 4
    if speed_kmh < 210:
        return 5
    return 6


def build_sample_session(points: int = 180) -> list[TelemetryPoint]:
    base_timestamp = time.time() - points
    session: list[TelemetryPoint] = []

    for index in range(points):
        lap_slot = index % 60

        if lap_slot < 18:
            throttle = min(1.0, 0.82 + (lap_slot / 18.0) * 0.18)
            brake = 0.0
            speed = 155 + lap_slot * 5.6 + math.sin(index / 5.0) * 6
        elif lap_slot < 27:
            braking_progress = (lap_slot - 18) / 9.0
            throttle = max(0.08, 0.65 - braking_progress * 0.55)
            brake = min(1.0, 0.35 + braking_progress * 0.65)
            speed = 255 - braking_progress * 145 + math.cos(index / 3.0) * 4
        elif lap_slot < 38:
            corner_progress = (lap_slot - 27) / 11.0
            throttle = 0.18 + corner_progress * 0.25
            brake = max(0.0, 0.45 - corner_progress * 0.42)
            speed = 102 + corner_progress * 22 + math.sin(index / 4.0) * 3
        else:
            exit_progress = (lap_slot - 38) / 22.0
            throttle = min(1.0, 0.55 + exit_progress * 0.45)
            brake = 0.0 if exit_progress > 0.25 else 0.18 - exit_progress * 0.72
            speed = 126 + exit_progress * 116 + math.sin(index / 6.0) * 5

        speed = max(58.0, min(speed, 279.0))
        gear = _gear_for_speed(speed)
        rpms = int(1800 + speed * 17 + throttle * 850 - brake * 180)

        if lap_slot in {16, 17, 46, 47}:
            rpms += 650

        session.append(
            TelemetryPoint(
                timestamp=base_timestamp + index,
                packetId=index,
                rpms=max(2500, rpms),
                speedKmh=round(speed, 2),
                gear=gear,
                throttle=round(max(0.0, min(throttle, 1.0)), 3),
                brake=round(max(0.0, min(brake, 1.0)), 3),
                car="Ferrari 296 GT3",
                track="Monza",
            )
        )

    return session
