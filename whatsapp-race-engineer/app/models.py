from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TelemetryPoint(BaseModel):
    timestamp: float = Field(..., description="Unix timestamp in seconds")
    packetId: int = Field(0, ge=0)
    rpms: int = Field(0, ge=0)
    speedKmh: float = Field(0.0, ge=0.0)
    gear: int = Field(0, ge=0)
    throttle: float = Field(0.0, ge=0.0, le=1.0)
    brake: float = Field(0.0, ge=0.0, le=1.0)
    car: str = "Unknown"
    track: str = "Unknown"


class TelemetrySummary(BaseModel):
    session_id: str
    duration_seconds: float
    avg_speed_kmh: float
    max_speed_kmh: float
    avg_rpm: float
    max_rpm: int
    avg_throttle: float
    avg_brake: float
    overrev_events: int
    heavy_braking_events: int
    throttle_brake_overlap_ratio: float
    driving_style: str
    car: str
    track: str
    recommendations: list[str]


class WhatsAppMessage(BaseModel):
    type: str
    body: str | None = None
    media_url: str | None = None
    caption: str | None = None


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3)
    session_id: str | None = None
    session: list[TelemetryPoint] | None = None


class QueryResponse(BaseModel):
    answer: str
    summary: TelemetrySummary
    chart_url: str
    diagram_url: str
    whatsapp_messages: list[WhatsAppMessage]
    debug: dict[str, Any]
