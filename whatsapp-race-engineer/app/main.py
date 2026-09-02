from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .analytics import analyze_session
from .llm import OllamaNarrator
from .models import QueryRequest, QueryResponse, TelemetryPoint, WhatsAppMessage
from .preview import build_whatsapp_preview_html
from .sample_data import build_sample_session
from .visuals import GENERATED_DIR, render_insight_diagram, render_trend_chart
from .whatsapp import IncomingWhatsAppMessage, WhatsAppCloudClient


app = FastAPI(
    title="ACC WhatsApp Race Engineer",
    description="Telemetry chatbot backend that responds with text plus visual artifacts for WhatsApp delivery.",
    version="0.1.0",
)
app.mount("/generated", StaticFiles(directory=GENERATED_DIR), name="generated")

narrator = OllamaNarrator()
whatsapp_client = WhatsAppCloudClient()


@dataclass
class DemoArtifacts:
    summary: object
    answer: str
    narrator_mode: str
    resolved_model: str
    chart_path: Path
    diagram_path: Path


def _build_whatsapp_messages(
    answer: str,
    chart_url: str,
    diagram_url: str,
) -> list[WhatsAppMessage]:
    return [
        WhatsAppMessage(type="text", body=answer),
        WhatsAppMessage(
            type="image",
            media_url=chart_url,
            caption="Grafico de telemetria: velocidad, RPM, throttle, brake y marcha.",
        ),
        WhatsAppMessage(
            type="image",
            media_url=diagram_url,
            caption="Diagrama de sesion con riesgos y acciones sugeridas.",
        ),
    ]


def _generate_demo_artifacts(
    *,
    question: str,
    session_id: str | None = None,
    session: list[TelemetryPoint] | None = None,
) -> DemoArtifacts:
    active_session = session or build_sample_session()

    try:
        summary = analyze_session(active_session, session_id=session_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    chart_path = render_trend_chart(active_session, summary)
    diagram_path = render_insight_diagram(summary)
    answer, narrator_mode = narrator.compose_answer(question, summary)
    resolved_model = narrator.last_model_used if narrator_mode == "ollama" else "fallback"

    return DemoArtifacts(
        summary=summary,
        answer=answer,
        narrator_mode=narrator_mode,
        resolved_model=resolved_model,
        chart_path=chart_path,
        diagram_path=diagram_path,
    )


def _build_demo_response(
    *,
    question: str,
    request: Request,
    session_id: str | None = None,
    session: list[TelemetryPoint] | None = None,
) -> QueryResponse:
    artifacts = _generate_demo_artifacts(
        question=question,
        session_id=session_id,
        session=session,
    )
    active_session = session or build_sample_session()

    chart_url = str(request.url_for("generated", path=artifacts.chart_path.name))
    diagram_url = str(request.url_for("generated", path=artifacts.diagram_path.name))

    return QueryResponse(
        answer=artifacts.answer,
        summary=artifacts.summary,
        chart_url=chart_url,
        diagram_url=diagram_url,
        whatsapp_messages=_build_whatsapp_messages(artifacts.answer, chart_url, diagram_url),
        debug={
            "narrator_mode": artifacts.narrator_mode,
            "resolved_model": artifacts.resolved_model,
            "points": len(active_session),
            "question": question,
        },
    )


def _deliver_demo_whatsapp_response(message: IncomingWhatsAppMessage) -> None:
    if not whatsapp_client.is_configured():
        return

    artifacts = _generate_demo_artifacts(question=message.text)

    whatsapp_client.send_text(
        recipient=message.sender,
        body=artifacts.answer,
        reply_to_message_id=message.message_id,
    )

    chart_media_id = whatsapp_client.upload_media(artifacts.chart_path)
    whatsapp_client.send_image(
        recipient=message.sender,
        media_id=chart_media_id,
        caption="Grafico de telemetria: velocidad, RPM, throttle, brake y marcha.",
    )

    diagram_media_id = whatsapp_client.upload_media(artifacts.diagram_path)
    whatsapp_client.send_image(
        recipient=message.sender,
        media_id=diagram_media_id,
        caption="Diagrama de sesion con riesgos y acciones sugeridas.",
    )


@app.get("/health")
def health() -> dict[str, str]:
    resolved_model = narrator.last_model_used
    if resolved_model == "unresolved":
        try:
            resolved_model = narrator._resolve_model()
        except Exception:
            resolved_model = "unavailable"
    return {
        "status": "ok",
        "generated_dir": str(GENERATED_DIR),
        "default_model": narrator.model,
        "resolved_model": resolved_model,
        "ollama_base_url": narrator.base_url,
        "whatsapp_configured": str(whatsapp_client.is_configured()).lower(),
    }


@app.get("/demo/sample-session")
def sample_session() -> list[TelemetryPoint]:
    return build_sample_session()


@app.get("/demo/preview", response_class=HTMLResponse)
def demo_preview(
    request: Request,
    question: str = "Como voy de frenada y sobre regimen en esta sesion?",
) -> HTMLResponse:
    payload = _build_demo_response(question=question, request=request)
    html = build_whatsapp_preview_html(
        question=question,
        answer=payload.answer,
        chart_url=payload.chart_url,
        diagram_url=payload.diagram_url,
        summary=payload.summary,
        narrator_mode=str(payload.debug["narrator_mode"]),
        resolved_model=str(payload.debug["resolved_model"]),
    )
    return HTMLResponse(content=html)


@app.get("/whatsapp/webhook")
def whatsapp_webhook_verify(request: Request):
    challenge = whatsapp_client.verify_challenge(
        mode=request.query_params.get("hub.mode"),
        token=request.query_params.get("hub.verify_token"),
        challenge=request.query_params.get("hub.challenge"),
    )
    if challenge is None:
        raise HTTPException(status_code=403, detail="Webhook verification failed.")
    return HTMLResponse(content=challenge)


@app.post("/whatsapp/webhook")
async def whatsapp_webhook_receive(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    incoming_messages = whatsapp_client.extract_text_messages(payload)

    if not incoming_messages:
        return {"status": "ignored", "reason": "no_text_messages"}

    if not whatsapp_client.is_configured():
        return {"status": "ignored", "reason": "whatsapp_not_configured", "messages": len(incoming_messages)}

    for message in incoming_messages:
        background_tasks.add_task(_deliver_demo_whatsapp_response, message)

    return {"status": "accepted", "messages": len(incoming_messages)}


@app.post("/demo/query", response_model=QueryResponse)
def demo_query(payload: QueryRequest, request: Request) -> QueryResponse:
    return _build_demo_response(
        question=payload.question,
        request=request,
        session_id=payload.session_id,
        session=payload.session,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
