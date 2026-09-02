from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass
class IncomingWhatsAppMessage:
    sender: str
    message_id: str
    text: str


class WhatsAppCloudClient:
    def __init__(self) -> None:
        self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
        self.verify_token = os.getenv("WHATSAPP_VERIFY_TOKEN", "acc-race-engineer-demo").strip()
        self.graph_api_version = os.getenv("WHATSAPP_GRAPH_API_VERSION", "v23.0").strip()

    @property
    def base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_api_version}"

    def is_configured(self) -> bool:
        return bool(self.access_token and self.phone_number_id)

    def can_verify_webhook(self) -> bool:
        return bool(self.verify_token)

    def verify_challenge(self, mode: str | None, token: str | None, challenge: str | None) -> str | None:
        if mode == "subscribe" and token and challenge and token == self.verify_token:
            return challenge
        return None

    def extract_text_messages(self, payload: dict[str, Any]) -> list[IncomingWhatsAppMessage]:
        extracted: list[IncomingWhatsAppMessage] = []

        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                for message in value.get("messages", []):
                    if message.get("type") != "text":
                        continue

                    sender = str(message.get("from", "")).strip()
                    message_id = str(message.get("id", "")).strip()
                    text = str(message.get("text", {}).get("body", "")).strip()

                    if sender and message_id and text:
                        extracted.append(
                            IncomingWhatsAppMessage(
                                sender=sender,
                                message_id=message_id,
                                text=text,
                            )
                        )

        return extracted

    def send_text(self, recipient: str, body: str, reply_to_message_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "text",
            "text": {
                "body": body,
                "preview_url": False,
            },
        }
        if reply_to_message_id:
            payload["context"] = {"message_id": reply_to_message_id}
        return self._post_json(f"/{self.phone_number_id}/messages", payload)

    def upload_media(self, file_path: Path, mime_type: str = "image/png") -> str:
        with file_path.open("rb") as file_handle, httpx.Client(timeout=120.0) as client:
            response = client.post(
                f"{self.base_url}/{self.phone_number_id}/media",
                headers=self._auth_headers(),
                data={"messaging_product": "whatsapp"},
                files={"file": (file_path.name, file_handle, mime_type)},
            )
            response.raise_for_status()
            data = response.json()

        media_id = str(data.get("id", "")).strip()
        if not media_id:
            raise ValueError("WhatsApp Cloud API did not return a media id.")
        return media_id

    def send_image(
        self,
        recipient: str,
        media_id: str,
        caption: str,
        reply_to_message_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "image",
            "image": {
                "id": media_id,
                "caption": caption,
            },
        }
        if reply_to_message_id:
            payload["context"] = {"message_id": reply_to_message_id}
        return self._post_json(f"/{self.phone_number_id}/messages", payload)

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=120.0) as client:
            response = client.post(
                f"{self.base_url}{path}",
                headers={
                    **self._auth_headers(),
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    def _auth_headers(self) -> dict[str, str]:
        if not self.access_token:
            raise ValueError("WHATSAPP_ACCESS_TOKEN is not configured.")
        return {"Authorization": f"Bearer {self.access_token}"}
