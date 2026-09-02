# ACC WhatsApp Race Engineer

This MVP answers a practical question: can a WhatsApp chatbot do more than plain text for telemetry?

Yes. The bot can send:

- A natural-language coaching reply.
- A telemetry trend chart as a PNG image.
- A session insight diagram as a PNG image.

The implementation is designed for your existing ACC telemetry flow. It reuses the packet shape already present in `producer/acc_producer.py` and exposes a lightweight API that can be connected to:

- `OpenClaw` as the WhatsApp-facing gateway.
- `Ollama` as the local LLM runtime.
- `Qwen3-8B` as the default local model for the narration layer (~5 GB, strong Spanish, Apache 2.0).

## Why this stack

- `OpenClaw` is a channel and agent platform, not a model. It already documents WhatsApp support and local-model support through Ollama.
- `Ollama` is the easiest way to run the narration model locally and expose an API.
- `llama.cpp` is still excellent, but for this MVP it would add operational work without giving us a better WhatsApp integration story.

If your Ollama instance runs in Docker and exposes `11434`, the service can use it directly from WSL with no extra networking layer.

## What the API does

`POST /demo/query` accepts either:

- A real ACC telemetry session as JSON events.
- No session at all, in which case it uses a synthetic ACC-like sample session.

The API returns:

- `answer`: short coaching answer in Spanish.
- `summary`: structured telemetry metrics.
- `chart_url`: PNG chart with RPM, speed, throttle, brake, and gear.
- `diagram_url`: PNG diagram summarizing the session.
- `whatsapp_messages`: ready-to-map payload objects for text plus images.

## Project layout

```text
whatsapp-race-engineer/
├── app/
│   ├── analytics.py
│   ├── llm.py
│   ├── main.py
│   ├── models.py
│   ├── sample_data.py
│   └── visuals.py
├── tests/
│   └── test_analytics.py
├── .env.example
└── requirements.txt
```

## Quickstart

```bash
cd ~/proyectos/sim-racing-telemetry/whatsapp-race-engineer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --port 8001
```

Port defaults to `8000` and is overridable via the `PORT` env var. `8001` is used here because Airbyte/`abctl` typically owns `8000` on a data-engineering workstation.

If you do not have Ollama running yet, the service still works with a deterministic fallback narrator.

## Docker Ollama note

The default `.env.example` uses:

```bash
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=auto
```

That covers Ollama in a Docker container with port `11434` published, when this service runs on the WSL host. If **both** run in Docker on the same network (e.g. your `ollama` container), point at the container name instead:

```bash
OLLAMA_BASE_URL=http://ollama:11434
```

`auto` means:

- Prefer `qwen3:8b` when installed (`ollama pull qwen3:8b`).
- Otherwise pick the best installed model in this order: `qwen3`, `gpt-oss:20b`, `gemma3:12b` / `gemma3:4b`, `llama3.1:8b`, then legacy `hermes3:8b` / `llama3.2:3b`.
- Reasoning output is disabled (`think: false`) so Qwen3 answers directly without `<think>` blocks.

That makes the service work immediately with a Docker Desktop Ollama container that already has models pulled.

## Try it

```bash
curl -X POST http://127.0.0.1:8000/demo/query \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Como voy de frenada y sobre regimen en esta sesion?"
  }'
```

You should receive JSON with the text answer and URLs for two generated images.

To see a visual WhatsApp-like mockup in the browser:

```bash
xdg-open "http://127.0.0.1:8000/demo/preview"
```

You can also change the sample prompt:

```bash
http://127.0.0.1:8000/demo/preview?question=Donde%20pierdo%20tiempo%20en%20esta%20sesion
```

## Example OpenClaw direction

This service is designed to sit behind OpenClaw in one of two ways:

1. OpenClaw handles WhatsApp and calls this API as a tool or skill.
2. This API talks directly to the WhatsApp Cloud API while OpenClaw is introduced later.

The best fit for your idea is option 1, because OpenClaw already abstracts the WhatsApp channel and Ollama wiring.

## Example OpenClaw model config

```yaml
models:
  ollama:
    base_url: http://localhost:11434
    default_model: qwen3:8b

agents:
  acc-race-engineer:
    model: ollama/qwen3:8b
    channels:
      - whatsapp
```

## WhatsApp media note

WhatsApp cannot render Mermaid directly, so diagrams should be sent as images. This MVP already does that by generating PNGs in `generated/`.

## WhatsApp Cloud API bridge

The service now includes:

- `GET /whatsapp/webhook` for Meta webhook verification.
- `POST /whatsapp/webhook` for receiving inbound text messages.
- Automatic response with:
  - one text coaching message
  - one uploaded telemetry chart image
  - one uploaded insight diagram image

Add these variables to `.env`:

```bash
WHATSAPP_ACCESS_TOKEN=...
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_VERIFY_TOKEN=acc-race-engineer-demo
WHATSAPP_GRAPH_API_VERSION=v23.0
```

## What I need from you to test with your real WhatsApp

1. A Meta WhatsApp Cloud API app or test number already created.
2. The `WHATSAPP_ACCESS_TOKEN`.
3. The `WHATSAPP_PHONE_NUMBER_ID`.
4. Your phone added as an allowed recipient in the Meta test setup.
5. A public HTTPS URL pointing to this app so Meta can reach `/whatsapp/webhook`.

For the public URL, the easiest options are a tunnel such as Cloudflare Tunnel or ngrok.

## Next integration step with your live ACC pipeline

The simplest production path is:

1. Keep your current producer sending ACC packets.
2. Persist a rolling session window to a JSONL file, Redis, or Kafka consumer cache.
3. Have the WhatsApp bot request the latest session slice before answering.
4. Generate text plus image attachments and send them back through WhatsApp.
