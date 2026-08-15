"""Registration payload contract for the Zulip REST client."""

from __future__ import annotations

import json
from typing import Any

from prime_zulip.client import ZulipClient


async def test_register_queue_declares_zulip12_capabilities(monkeypatch):
    """Zulip 12 rejects registration unless notification_settings_null is present.

    The server returns HTTP 400 with:
      client_capabilities["notification_settings_null"] field is missing: Field required
    """
    client = ZulipClient("https://zulip.example.com", "bot@example.com", "key")
    captured: dict[str, Any] = {}

    async def fake_post(path: str, **kwargs: Any) -> dict[str, Any]:
        captured["path"] = path
        captured["data"] = kwargs["data"]
        return {"result": "success", "queue_id": "queue-1", "last_event_id": -1}

    monkeypatch.setattr(client, "_post", fake_post)

    await client.register_queue()

    assert captured["path"] == "/register"
    capabilities = json.loads(captured["data"]["client_capabilities"])
    assert capabilities["notification_settings_null"] is True
    assert capabilities["stream_typing_notifications"] is True
