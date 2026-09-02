from __future__ import annotations

from html import escape

from .models import TelemetrySummary


def _multiline(text: str) -> str:
    return escape(text).replace("\n", "<br>")


def build_whatsapp_preview_html(
    *,
    question: str,
    answer: str,
    chart_url: str,
    diagram_url: str,
    summary: TelemetrySummary,
    narrator_mode: str,
    resolved_model: str,
) -> str:
    chips = [
        ("Pista", summary.track),
        ("Coche", summary.car),
        ("Vel. media", f"{summary.avg_speed_kmh} km/h"),
        ("RPM max", f"{summary.max_rpm}"),
        ("Estilo", summary.driving_style),
        ("Modelo", resolved_model),
    ]
    chip_html = "".join(
        f'<div class="chip"><span>{escape(label)}</span><strong>{escape(value)}</strong></div>'
        for label, value in chips
    )
    recommendations_html = "".join(
        f"<li>{escape(item)}</li>" for item in summary.recommendations
    )

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ACC WhatsApp Preview</title>
  <style>
    :root {{
      --bg: #efe7db;
      --paper: #faf6ef;
      --ink: #1f2a1f;
      --muted: #5f6c5f;
      --accent: #0e8f6f;
      --accent-dark: #065f46;
      --bot: #ffffff;
      --user: #d9fdd3;
      --panel: rgba(255, 252, 246, 0.78);
      --line: rgba(31, 42, 31, 0.12);
      --shadow: 0 24px 60px rgba(52, 55, 43, 0.18);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "Avenir Next", "Trebuchet MS", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(14, 143, 111, 0.18), transparent 28%),
        radial-gradient(circle at bottom right, rgba(245, 158, 11, 0.16), transparent 24%),
        linear-gradient(135deg, #ece4d8 0%, #f6efe5 46%, #e7f3ec 100%);
      display: grid;
      place-items: center;
      padding: 28px;
    }}

    .layout {{
      width: min(1180px, 100%);
      display: grid;
      grid-template-columns: 340px minmax(320px, 430px) 1fr;
      gap: 22px;
      align-items: start;
    }}

    .panel {{
      background: var(--panel);
      backdrop-filter: blur(14px);
      border: 1px solid rgba(255,255,255,0.55);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 22px;
    }}

    .hero h1 {{
      margin: 0 0 10px;
      font-size: 2rem;
      line-height: 1.02;
      letter-spacing: -0.03em;
    }}

    .hero p {{
      margin: 0 0 16px;
      color: var(--muted);
      line-height: 1.5;
    }}

    .badge-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 18px;
    }}

    .badge {{
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 0.82rem;
      background: rgba(14, 143, 111, 0.11);
      color: var(--accent-dark);
      border: 1px solid rgba(14, 143, 111, 0.16);
    }}

    .chip-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}

    .chip {{
      padding: 12px 14px;
      border-radius: 18px;
      background: rgba(255,255,255,0.78);
      border: 1px solid var(--line);
    }}

    .chip span {{
      display: block;
      font-size: 0.72rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }}

    .chip strong {{
      font-size: 0.96rem;
      line-height: 1.2;
    }}

    .recommendations {{
      margin-top: 18px;
      padding: 16px 18px;
      border-radius: 22px;
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
    }}

    .recommendations h2 {{
      margin: 0 0 10px;
      font-size: 1rem;
    }}

    .recommendations ul {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      line-height: 1.45;
    }}

    .phone-shell {{
      padding: 16px;
      border-radius: 36px;
      background:
        linear-gradient(180deg, #20352a 0%, #142a21 100%);
      box-shadow: var(--shadow);
      position: sticky;
      top: 24px;
    }}

    .phone-screen {{
      border-radius: 28px;
      overflow: hidden;
      background:
        radial-gradient(circle at top, rgba(255,255,255,0.18), transparent 22%),
        linear-gradient(180deg, #e6ddd2 0%, #ece4d6 100%);
      border: 1px solid rgba(255,255,255,0.22);
    }}

    .chat-header {{
      padding: 14px 16px;
      background: linear-gradient(180deg, #0b6a53 0%, #075e54 100%);
      color: #f7fff8;
    }}

    .chat-header small {{
      display: block;
      opacity: 0.86;
      margin-top: 2px;
    }}

    .chat-body {{
      padding: 16px 14px 22px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      background-image:
        linear-gradient(rgba(255,255,255,0.14) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.12) 1px, transparent 1px);
      background-size: 24px 24px;
    }}

    .message {{
      max-width: 88%;
      padding: 11px 13px;
      border-radius: 18px;
      box-shadow: 0 10px 24px rgba(24, 34, 28, 0.08);
      line-height: 1.45;
      position: relative;
    }}

    .message.user {{
      background: var(--user);
      align-self: flex-end;
      border-bottom-right-radius: 6px;
    }}

    .message.bot {{
      background: var(--bot);
      align-self: flex-start;
      border-bottom-left-radius: 6px;
    }}

    .message small {{
      display: block;
      color: var(--muted);
      margin-top: 6px;
      font-size: 0.72rem;
    }}

    .image-card {{
      background: var(--bot);
      padding: 8px;
      border-radius: 20px;
      border-bottom-left-radius: 6px;
      max-width: 92%;
      align-self: flex-start;
      box-shadow: 0 14px 28px rgba(24, 34, 28, 0.10);
    }}

    .image-card img {{
      display: block;
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
    }}

    .image-card .caption {{
      padding: 10px 8px 6px;
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.35;
    }}

    .side-note h2 {{
      margin: 0 0 10px;
      font-size: 1.15rem;
    }}

    .side-note p {{
      margin: 0 0 14px;
      color: var(--muted);
      line-height: 1.5;
    }}

    .mini-card {{
      padding: 16px;
      border-radius: 20px;
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
      margin-bottom: 12px;
    }}

    .mini-card strong {{
      display: block;
      margin-bottom: 6px;
      font-size: 0.96rem;
    }}

    .mini-card a {{
      color: var(--accent-dark);
      text-decoration: none;
    }}

    @media (max-width: 1080px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}

      .phone-shell {{
        position: static;
        max-width: 430px;
        margin: 0 auto;
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <section class="panel hero">
      <div class="badge-row">
        <div class="badge">Preview con datos sintéticos</div>
        <div class="badge">Modo narrador: {escape(narrator_mode)}</div>
      </div>
      <h1>Así se vería tu race engineer en WhatsApp</h1>
      <p>Esta vista usa una sesión demo de ACC y simula la conversación final con el piloto. Ya incluye texto, gráfico de telemetría y diagrama de sesión como imágenes enviables.</p>
      <div class="chip-grid">{chip_html}</div>
      <div class="recommendations">
        <h2>Qué detectó el bot</h2>
        <ul>{recommendations_html}</ul>
      </div>
    </section>

    <section class="phone-shell">
      <div class="phone-screen">
        <div class="chat-header">
          <strong>ACC Race Engineer</strong>
          <small>en línea • vista previa demo</small>
        </div>
        <div class="chat-body">
          <div class="message user">
            {_multiline(question)}
            <small>10:42</small>
          </div>
          <div class="message bot">
            Analicé tu última sesión demo y te mando coaching con soporte visual.
            <small>10:42</small>
          </div>
          <div class="message bot">
            {_multiline(answer)}
            <small>10:43</small>
          </div>
          <div class="image-card">
            <img src="{escape(chart_url)}" alt="Gráfico de telemetría">
            <div class="caption">Gráfico con velocidad, RPM, throttle, brake y marcha para explicar la recomendación.</div>
          </div>
          <div class="image-card">
            <img src="{escape(diagram_url)}" alt="Diagrama de sesión">
            <div class="caption">Diagrama-resumen para que el piloto vea riesgos, causas y acciones sin abrir otra app.</div>
          </div>
        </div>
      </div>
    </section>

    <aside class="panel side-note">
      <h2>Qué estamos validando</h2>
      <p>Antes de conectar el canal real, esta pantalla nos deja revisar si la narrativa, el formato y el tipo de imágenes sí tienen sentido dentro de una conversación de WhatsApp.</p>
      <div class="mini-card">
        <strong>Pregunta usada</strong>
        <span>{_multiline(question)}</span>
      </div>
      <div class="mini-card">
        <strong>Payload útil para el canal</strong>
        <span>El endpoint sigue devolviendo JSON en <code>/demo/query</code>, así que OpenClaw o un webhook propio pueden tomar el texto y las URLs de media sin rehacer la lógica.</span>
      </div>
      <div class="mini-card">
        <strong>Assets generados</strong>
        <span><a href="{escape(chart_url)}" target="_blank">Abrir gráfico PNG</a><br><a href="{escape(diagram_url)}" target="_blank">Abrir diagrama PNG</a></span>
      </div>
    </aside>
  </div>
</body>
</html>
"""
