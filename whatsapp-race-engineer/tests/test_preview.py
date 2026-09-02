from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


class PreviewTests(unittest.TestCase):
    def test_preview_renders_whatsapp_like_page(self) -> None:
        client = TestClient(app)

        with patch("app.main.narrator.compose_answer", return_value=("Respuesta demo del ingeniero.", "fallback")):
            response = client.get("/demo/preview")

        self.assertEqual(response.status_code, 200)
        self.assertIn("ACC WhatsApp Preview", response.text)
        self.assertIn("Respuesta demo del ingeniero.", response.text)
        self.assertIn("_chart.png", response.text)
        self.assertIn("_diagram.png", response.text)


if __name__ == "__main__":
    unittest.main()
