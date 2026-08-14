from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from prime_zulip.auth import Allowlist
from prime_zulip.bridge import _build_send_payload, _direct_recipient_ids
from prime_zulip.client import ZulipClient
from prime_zulip.events import categorize_event


def allowlist() -> Allowlist:
    return Allowlist(user_ids={10, 11}, emails={"a@example.com", "b@example.com"})


def dm_message_event(*, event_id: int = 1, message_id: int = 100, sender_id: int = 10) -> dict[str, Any]:
    people = [
        {"id": 99, "email": "bot@example.com", "full_name": "Bot"},
        {"id": 10, "email": "a@example.com", "full_name": "A"},
        {"id": 11, "email": "b@example.com", "full_name": "B"},
    ]
    return {
        "id": event_id,
        "type": "message",
        "message": {
            "id": message_id,
            "type": "private",
            "sender_id": sender_id,
            "sender_email": "a@example.com" if sender_id == 10 else "b@example.com",
            "sender_full_name": "A" if sender_id == 10 else "B",
            "display_recipient": list(reversed(people)) if sender_id == 11 else people,
            "content": f"message {message_id}",
            "timestamp": 1,
            "recipient_id": 5,
        },
    }


def typing_event(*, op: str = "start", sender_id: int = 10, email: str = "a@example.com") -> dict[str, Any]:
    return {
        "id": 7,
        "type": "typing",
        "op": op,
        "message_type": "direct",
        "sender": {"user_id": sender_id, "email": email},
        "recipients": [
            {"user_id": 11, "email": "b@example.com"},
            {"user_id": 99, "email": "bot@example.com"},
            {"user_id": 10, "email": "a@example.com"},
        ],
    }


def parse(raw: dict[str, Any]):
    return categorize_event(raw, "99", "bot@example.com", {"bot"}, allowlist())


def test_group_dm_has_one_canonical_key_and_all_reply_recipients() -> None:
    first = parse(dm_message_event(sender_id=10))
    second = parse(dm_message_event(sender_id=11, event_id=2, message_id=101))
    assert first and second and first.message and second.message
    assert first.message.chat_id == second.message.chat_id == "dm:10,11,99"
    assert first.message.metadata["zulip_recipient_user_ids"] == [10, 11, 99]
    assert _direct_recipient_ids(first.message.chat_id, first.message.metadata) == [10, 11]
    payload = _build_send_payload(first.message.chat_id, "answer", first.message.metadata, first.message.message_id)
    assert payload["type"] == "direct"
    assert json.loads(payload["to"]) == [10, 11]


def test_typing_is_canonical_and_authorized_independently() -> None:
    event = parse(typing_event())
    assert event and event.typing
    assert event.typing.chat_id == "dm:10,11,99"
    assert event.typing.op == "start"

    # An unauthorized user cannot manipulate an authorized sender's batch.
    raw = typing_event(sender_id=12, email="outsider@example.com")
    assert parse(raw) is None
    assert parse(typing_event(sender_id=99, email="bot@example.com")) is None


def test_stream_typing_requires_canonical_topic() -> None:
    raw = {
        "type": "typing", "op": "stop", "message_type": "stream",
        "sender": {"user_id": 10, "email": "a@example.com"},
        "stream_id": 42, "topic": "ops",
    }
    event = parse(raw)
    assert event and event.typing and event.typing.chat_id == "stream:42:topic:ops"
    raw.pop("topic")
    assert parse(raw) is None


@pytest.mark.asyncio
async def test_register_queue_requests_typing_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, Any] = {}
    client = ZulipClient("https://example.test", "bot@example.com", "key")

    async def fake_request(path: str, **kwargs: Any) -> dict[str, Any]:
        recorded.update(kwargs["data"])
        return {"queue_id": "q", "last_event_id": -1}

    monkeypatch.setattr(client, "_post", fake_request)
    await client.register_queue()
    assert "typing" in json.loads(recorded["event_types"])
    assert json.loads(recorded["client_capabilities"])["stream_typing_notifications"] is True
