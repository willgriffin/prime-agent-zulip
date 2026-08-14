from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from prime_zulip.events import TypingEvent, ZulipEvent
from prime_zulip.messages import ZulipMessage
from prime_zulip.relay import BurstRelay, RelayConfig, format_batch


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def advance(self, seconds: float) -> None:
        self.now += seconds
        # Wake fake-clock schedulers without waiting for their real timeout.
        await asyncio.sleep(0)


class FakeBridge:
    def __init__(self) -> None:
        self.replies: list[tuple[ZulipMessage, str]] = []
        self.typing: list[tuple[str, str]] = []

    async def typing_start(self, chat_id: str, metadata: Any = None) -> None:
        self.typing.append(("start", chat_id))

    async def typing_stop(self, chat_id: str, metadata: Any = None) -> None:
        self.typing.append(("stop", chat_id))

    async def reply(self, message: ZulipMessage, answer: str) -> None:
        self.replies.append((message, answer))


class FakePrime:
    def __init__(self) -> None:
        self.prompts: list[str] = []
        self.active = 0
        self.max_active = 0
        self.release: asyncio.Event | None = None

    async def ask(self, prompt: str) -> str:
        self.prompts.append(prompt)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.release is not None:
                await self.release.wait()
            return f"answer {len(self.prompts)}"
        finally:
            self.active -= 1


def message(mid: int, *, chat: str = "dm:10,99", sender: int = 10, content: str | None = None) -> ZulipMessage:
    return ZulipMessage(
        message_id=mid, sender_id=sender, sender_email=f"{sender}@example.com",
        sender_full_name=f"User {sender}", content=content or f"m{mid}", raw_content=content or f"m{mid}",
        chat_id=chat, chat_name=chat, is_dm=chat.startswith("dm:"), is_stream=chat.startswith("stream:"),
        is_mentioned=True, metadata={"zulip_recipient_user_ids": [10, 99], "zulip_bot_user_id": 99},
    )


def message_event(mid: int, **kwargs: Any) -> ZulipEvent:
    return ZulipEvent(type="message", message=message(mid, **kwargs), event_id=mid)


def typing(op: str, *, chat: str = "dm:10,99", sender: int = 10) -> ZulipEvent:
    return ZulipEvent(type="typing", typing=TypingEvent(op, sender, f"{sender}@example.com", chat, chat.startswith("dm:")))


async def settle() -> None:
    for _ in range(8):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_quiet_timer_resets_and_stop_resumes() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        await relay.record(message_event(1))
        await clock.advance(4)
        await relay.record(message_event(2))
        await clock.advance(4)
        await settle()
        assert prime.prompts == []
        await relay.record(typing("start"))
        await clock.advance(4)
        assert prime.prompts == []
        await relay.record(typing("stop"))
        await clock.advance(5)
        await settle()
        assert len(prime.prompts) == 1
        assert prime.prompts[0].index("m1") < prime.prompts[0].index("m2")
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_missing_stop_cannot_exceed_maximum() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        await relay.record(message_event(1))
        await relay.record(typing("start"))
        await clock.advance(19)
        await relay.record(ZulipEvent(type="queue_reset"))
        await settle()
        assert not prime.prompts
        await clock.advance(1)
        await relay.record(ZulipEvent(type="queue_reset"))
        await settle()
        assert len(prime.prompts) == 1
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_typing_alone_does_not_create_or_invoke() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        await relay.record(typing("start"))
        await relay.record(typing("stop"))
        await clock.advance(30)
        await settle()
        assert relay.pending_count == 0
        assert prime.prompts == []
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_disabled_mode_is_immediate_and_deduplicates_replay() -> None:
    bridge, prime = FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(0, 0))
    try:
        event = message_event(1)
        await relay.record(event)
        await relay.record(event)
        await settle()
        assert prime.prompts == ["m1"]
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_slow_ask_serializes_and_arrivals_form_next_turn() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    prime.release = asyncio.Event()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        await relay.record(message_event(1))
        await clock.advance(5)
        await settle()
        assert prime.active == 1
        # Poll ingestion remains prompt while ask 1 is blocked.
        await asyncio.wait_for(relay.record(message_event(2)), timeout=0.1)
        await clock.advance(1)
        await asyncio.wait_for(relay.record(message_event(3)), timeout=0.1)
        await clock.advance(5)  # deadline passes while this key is in flight
        await relay.record(ZulipEvent(type="queue_reset"))
        await settle()
        assert len(prime.prompts) == 1
        prime.release.set()
        await settle()
        assert len(prime.prompts) == 2
        assert "m2" in prime.prompts[1] and "m3" in prime.prompts[1]
        assert prime.max_active == 1
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_one_scheduler_and_worker_bound_many_conversations() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        for i in range(100):
            await relay.record(message_event(i + 1, chat=f"dm:{i},99"))
        await settle()
        owned = [task for task in asyncio.all_tasks() if task.get_name().startswith("zulip-")]
        assert {task.get_name() for task in owned} == {"zulip-burst-scheduler", "zulip-ask-worker"}
        assert relay.pending_count == 100
    finally:
        await relay.close()
    await settle()
    assert relay.pending_count == relay.ready_count == 0


@pytest.mark.asyncio
async def test_queue_reset_clears_transient_typing_but_preserves_messages() -> None:
    clock, bridge, prime = FakeClock(), FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(5, 20), clock=clock)
    try:
        await relay.record(message_event(1))
        await relay.record(typing("start"))
        await relay.record(ZulipEvent(type="queue_reset"))
        assert not next(iter(relay._pending.values())).typing_users
        await clock.advance(5)
        await settle()
        assert prime.prompts == ["m1"]
    finally:
        await relay.close()


def test_batch_group_dm_labels_speakers_and_preserves_order() -> None:
    first = message(1, chat="dm:10,11,99", sender=10, content="first")
    first.metadata["zulip_recipient_user_ids"] = [10, 11, 99]
    second = replace(first, message_id=2, sender_id=11, sender_full_name="User 11", content="second")
    prompt = format_batch([first, second])
    assert prompt.index("User 10:\nfirst") < prompt.index("User 11:\nsecond")


@pytest.mark.asyncio
async def test_disabled_mode_dispatches_each_message_during_inflight_ask() -> None:
    bridge, prime = FakeBridge(), FakePrime()
    prime.release = asyncio.Event()
    relay = BurstRelay(bridge, prime, RelayConfig(0, 0))
    try:
        await relay.record(message_event(1))
        await settle()
        assert prime.prompts == ["m1"]
        # Messages 2 and 3 arrive while ask 1 is blocked. Each must later
        # dispatch as its own one-message turn, never one combined prompt.
        await relay.record(message_event(2))
        await relay.record(message_event(3))
        await settle()
        assert len(prime.prompts) == 1
        prime.release.set()
        await settle()
        assert prime.prompts == ["m1", "m2", "m3"]
        assert prime.max_active == 1
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_seen_messages_bounded_and_recent_wins() -> None:
    bridge, prime = FakeBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(0, 0, seen_messages_limit=3))
    try:
        for i in range(3):
            await relay.record(message_event(i + 1))
            await settle()
        assert len(relay._seen_messages) == 3
        # A replay of a still-recent id is still deduped.
        await relay.record(message_event(2))
        await settle()
        assert len(relay._seen_messages) == 3
        # Inserting beyond the cap evicts the least recently used id (1);
        # the replay of 2 made 1 the oldest entry.
        await relay.record(message_event(4))
        assert 1 not in relay._seen_messages
        assert sorted(relay._seen_messages) == [2, 3, 4]
        # Id 1 fell off the window, so a replay is treated as new traffic.
        await relay.record(message_event(1))
        await settle()
        assert prime.prompts.count("m1") == 2
    finally:
        await relay.close()


@pytest.mark.asyncio
async def test_typing_stop_attempted_when_worker_cancelled_during_typing_start() -> None:
    bridge, prime = HangingTypingStartBridge(), FakePrime()
    relay = BurstRelay(bridge, prime, RelayConfig(0, 0))
    try:
        await relay.record(message_event(1))
        await settle()
        assert bridge.started
    finally:
        # close() cancels the worker while it is parked on typing_start.
        await relay.close()
    assert bridge.stopped
    assert bridge.typing == [("stop", "dm:10,99")]


class HangingTypingStartBridge(FakeBridge):
    started = False
    stopped = False

    async def typing_start(self, chat_id: str, metadata: Any = None) -> None:
        self.started = True
        await asyncio.Event().wait()

    async def typing_stop(self, chat_id: str, metadata: Any = None) -> None:
        self.stopped = True
        self.typing.append(("stop", chat_id))
