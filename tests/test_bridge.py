"""Queue re-registration behavior for the bridge poll loop."""

from __future__ import annotations

import asyncio

from prime_zulip.bridge import REREGISTER_BACKOFF_SECONDS, PrimeZulipBridge
from prime_zulip.client import BadEventQueueError


def _bridge() -> PrimeZulipBridge:
    return PrimeZulipBridge(
        site="https://zulip.example.com",
        email="bot@example.com",
        api_key="key",
    )


class _FakeClient:
    def __init__(self, *, register_payload=None, error=None):
        self.register_payload = register_payload or {
            "queue_id": "q-new",
            "last_event_id": -1,
        }
        self.error = error
        self.register_calls = 0
        self.get_events_calls: list[tuple[str, int]] = []

    async def register_queue(self):
        self.register_calls += 1
        return self.register_payload

    async def get_events(self, queue_id, last_event_id):
        self.get_events_calls.append((queue_id, last_event_id))
        if self.error is not None:
            raise self.error
        return {"result": "success", "events": []}


async def test_reregister_resets_watermark_to_new_queue():
    """Event ids are queue-local; the old queue's watermark must not carry over.

    Regression for #23: `max()` kept the stale watermark, so the first poll on
    the fresh queue asked for an id it does not hold, Zulip answered
    "Event N was not in this queue", and the bridge re-registered forever.
    """
    bridge = _bridge()
    bridge._event_queue = asyncio.Queue()
    bridge._client = _FakeClient()
    bridge._queue_id = "q-old"
    bridge._last_event_id = 193  # high watermark of the dead queue

    await bridge._reregister_queue()

    assert bridge._queue_id == "q-new"
    assert bridge._last_event_id == -1
    # Consumers learn typing state is unrecoverable across queues.
    assert bridge._event_queue.get_nowait().type == "queue_reset"


async def test_poll_after_reregister_uses_new_queue_watermark():
    """The first poll on the replacement queue starts from its own watermark."""
    bridge = _bridge()
    bridge._event_queue = asyncio.Queue()
    client = _FakeClient()
    bridge._client = client
    bridge._queue_id = "q-old"
    bridge._last_event_id = 193

    await bridge._reregister_queue()
    await bridge._poll_once()

    assert client.get_events_calls == [("q-new", -1)]


async def test_stale_queue_poll_backs_off_after_reregister(monkeypatch):
    """A failing re-register must not let the poll loop spin without pause."""
    bridge = _bridge()
    bridge._event_queue = asyncio.Queue()
    bridge._client = _FakeClient(
        error=BadEventQueueError("Event 193 was not in this queue")
    )
    bridge._queue_id = "q-old"
    bridge._last_event_id = 193

    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("prime_zulip.bridge.asyncio.sleep", fake_sleep)
    await bridge._poll_once()

    assert sleeps == [REREGISTER_BACKOFF_SECONDS]
