from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.main import app, whatsapp_client
from app.whatsapp import IncomingWhatsAppMessage


class WhatsAppTests(unittest.TestCase):
    def test_extract_text_messages(self) -> None:
        payload = {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "messages": [
                                    {
                                        "from": "573001112233",
                                        "id": "wamid.demo",
                                        "type": "text",
                                        "text": {"body": "como voy en esta sesion"},
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }

        messages = whatsapp_client.extract_text_messages(payload)
        self.assertEqual(
            messages,
            [IncomingWhatsAppMessage(sender="573001112233", message_id="wamid.demo", text="como voy en esta sesion")],
        )

    def test_webhook_verification(self) -> None:
        client = TestClient(app)
        original_token = whatsapp_client.verify_token
        whatsapp_client.verify_token = "demo-token"

        try:
            response = client.get(
                "/whatsapp/webhook",
                params={
                    "hub.mode": "subscribe",
                    "hub.verify_token": "demo-token",
                    "hub.challenge": "12345",
                },
            )
        finally:
            whatsapp_client.verify_token = original_token

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "12345")

    def test_unconfigured_webhook_post_is_ignored(self) -> None:
        client = TestClient(app)
        original_access_token = whatsapp_client.access_token
        original_phone_number_id = whatsapp_client.phone_number_id
        whatsapp_client.access_token = ""
        whatsapp_client.phone_number_id = ""

        try:
            response = client.post(
                "/whatsapp/webhook",
                json={
                    "entry": [
                        {
                            "changes": [
                                {
                                    "value": {
                                        "messages": [
                                            {
                                                "from": "573001112233",
                                                "id": "wamid.demo",
                                                "type": "text",
                                                "text": {"body": "hola"},
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                },
            )
        finally:
            whatsapp_client.access_token = original_access_token
            whatsapp_client.phone_number_id = original_phone_number_id

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["reason"], "whatsapp_not_configured")
