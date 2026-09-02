from __future__ import annotations

import os

import httpx

from .models import TelemetrySummary


SYSTEM_PROMPT = """
You are a race engineer for Assetto Corsa Competizione.
Reply in concise Spanish.
Use the telemetry metrics provided by the user.
Mention concrete numbers.
Keep the answer under 120 words.
Sound practical and supportive.
""".strip()

PREFERRED_MODELS = (
    "qwen3:8b",
    "qwen3",
    "gpt-oss:20b",
    "gemma3:12b",
    "gemma3:4b",
    "llama3.1:8b",
    "hermes3:8b",
    "hermes3",
    "llama3.2:3b",
    "deepseek-r1:7b",
)


class OllamaNarrator:
    def __init__(self) -> None:
        self.base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
        self.model = os.getenv("OLLAMA_MODEL", "auto")
        self.timeout_seconds = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "45"))
        self.last_model_used = "unresolved"

    def compose_answer(self, question: str, summary: TelemetrySummary) -> tuple[str, str]:
        try:
            answer = self._call_ollama(question, summary)
            return answer, "ollama"
        except Exception:
            return self._fallback_answer(question, summary), "fallback"

    def _resolve_model(self) -> str:
        configured = self.model.strip()
        if configured and configured.lower() != "auto":
            return configured

        available = set(self._list_models())
        for candidate in PREFERRED_MODELS:
            if candidate in available:
                return candidate

        if available:
            return sorted(available)[0]

        raise ValueError("No models available in Ollama.")

    def _list_models(self) -> list[str]:
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.get(f"{self.base_url}/api/tags")
            response.raise_for_status()
            data = response.json()

        models = data.get("models", [])
        return [model.get("name", "") for model in models if model.get("name")]

    def _call_ollama(self, question: str, summary: TelemetrySummary) -> str:
        model_name = self._resolve_model()
        self.last_model_used = model_name
        payload = {
            "model": model_name,
            "stream": False,
            # Qwen3 thinks by default; we want the direct answer only (faster,
            # no <think> leakage). Ignored by non-reasoning models.
            "think": False,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Pregunta: {question}\n"
                        f"Carro: {summary.car}\n"
                        f"Pista: {summary.track}\n"
                        f"Duracion: {summary.duration_seconds} segundos\n"
                        f"Velocidad media: {summary.avg_speed_kmh} km/h\n"
                        f"Velocidad maxima: {summary.max_speed_kmh} km/h\n"
                        f"RPM medio: {summary.avg_rpm}\n"
                        f"RPM maximo: {summary.max_rpm}\n"
                        f"Throttle medio: {summary.avg_throttle}\n"
                        f"Brake medio: {summary.avg_brake}\n"
                        f"Eventos sobre regimen: {summary.overrev_events}\n"
                        f"Eventos frenada fuerte: {summary.heavy_braking_events}\n"
                        f"Ratio throttle-brake overlap: {summary.throttle_brake_overlap_ratio}\n"
                        f"Estilo: {summary.driving_style}\n"
                        f"Recomendaciones: {' | '.join(summary.recommendations)}"
                    ),
                },
            ],
            "options": {"num_ctx": 16384},
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/api/chat", json=payload)
            response.raise_for_status()
            data = response.json()

        content = data.get("message", {}).get("content", "").strip()
        if not content:
            raise ValueError("Ollama returned an empty response.")
        return content

    def _fallback_answer(self, question: str, summary: TelemetrySummary) -> str:
        focus = "general"
        lowered = question.lower()
        if "freno" in lowered or "brake" in lowered:
            focus = "braking"
        elif "rpm" in lowered or "regimen" in lowered or "motor" in lowered:
            focus = "engine"
        elif "velocidad" in lowered or "speed" in lowered:
            focus = "speed"

        base = (
            f"En {summary.track} con el {summary.car}, tu sesion promedia {summary.avg_speed_kmh} km/h "
            f"y llega a {summary.max_speed_kmh} km/h. El pico de motor fue {summary.max_rpm} rpm "
            f"con {summary.overrev_events} eventos de sobre regimen."
        )

        if focus == "braking":
            detail = (
                f" Veo {summary.heavy_braking_events} frenadas fuertes y un brake medio de {summary.avg_brake}. "
                f"Eso sugiere que estas entrando agresivo en algunas zonas."
            )
        elif focus == "engine":
            detail = (
                f" El motor esta tocando el limite con {summary.overrev_events} eventos; "
                f"conviene subir cambio apenas antes del corte."
            )
        elif focus == "speed":
            detail = (
                f" La velocidad media de {summary.avg_speed_kmh} km/h esta bien para una sesion demo, "
                f"pero todavia hay margen saliendo de curva."
            )
        else:
            detail = (
                f" Tu estilo sale como {summary.driving_style} y el overlap throttle-brake es "
                f"{summary.throttle_brake_overlap_ratio}, asi que hay bastante informacion para coach visual."
            )

        closing = f" Recomendacion principal: {summary.recommendations[0]}"
        return f"{base}{detail}{closing}"
