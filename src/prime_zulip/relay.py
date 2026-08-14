"""Nonblocking inbound burst coalescing for the Prime relay."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Callable

from .bridge import PrimeZulipBridge
from .events import TypingEvent, ZulipEvent
from .messages import ZulipMessage
from .prime import PrimeClient, PrimeError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelayConfig:
    """Timing policy for one inbound thought burst."""

    debounce_seconds: float = 5.0
    max_wait_seconds: float = 20.0
    seen_messages_limit: int = 4096

    def __post_init__(self) -> None:
        if self.debounce_seconds < 0 or self.max_wait_seconds < 0:
            raise ValueError("debounce timings must be non-negative")
        if self.seen_messages_limit < 1:
            raise ValueError("seen_messages_limit must be at least 1")
        if self.debounce_seconds == 0:
            object.__setattr__(self, "max_wait_seconds", 0.0)
        elif self.max_wait_seconds < self.debounce_seconds:
            object.__setattr__(self, "max_wait_seconds", self.debounce_seconds)


@dataclass
class _PendingBatch:
    first_at: float
    active_at: float
    messages: list[tuple[int, ZulipMessage]] = field(default_factory=list)
    typing_users: set[int] = field(default_factory=set)


@dataclass
class _ReadyBatch:
    chat_id: str
    messages: list[ZulipMessage]


class BurstRelay:
    """Buffer by conversation, but execute Prime turns through one worker.

    Timers are represented by one scheduler task regardless of conversation
    count. Ready batches enter one queue consumed by one ask worker, so a busy
    global Prime session never creates an unbounded set of tasks waiting on
    ``PrimeClient.ask``.
    """

    def __init__(
        self,
        bridge: PrimeZulipBridge,
        prime: PrimeClient,
        config: RelayConfig | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.bridge = bridge
        self.prime = prime
        self.config = config or RelayConfig()
        self._clock = clock
        self._pending: dict[str, _PendingBatch] = {}
        self._ready: deque[_ReadyBatch] = deque()
        # LRU-bounded so a long-lived bridge does not leak message ids; a
        # replayed id only needs to stay known until the queue could still
        # redeliver it, which is minutes, not process lifetime.
        self._seen_messages: OrderedDict[int, None] = OrderedDict()
        self._condition = asyncio.Condition()
        self._scheduler_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._inflight_chat_id: str | None = None
        self._closed = False

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def ready_count(self) -> int:
        return len(self._ready)

    def _now(self) -> float:
        if self._clock is not None:
            return self._clock()
        return asyncio.get_running_loop().time()

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("relay is closed")
        if self._scheduler_task is None:
            self._scheduler_task = asyncio.create_task(
                self._scheduler(), name="zulip-burst-scheduler"
            )
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(
                self._ask_worker(), name="zulip-ask-worker"
            )

    async def close(self) -> None:
        """Cancel timers/work, discarding in-memory batches by design."""
        self._closed = True
        tasks = [
            task
            for task in (self._scheduler_task, self._worker_task)
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._scheduler_task = self._worker_task = None
        async with self._condition:
            self._pending.clear()
            self._ready.clear()
            self._condition.notify_all()

    async def record(self, event: ZulipEvent) -> None:
        """Record one parsed event without waiting for a timer or Prime."""
        if self._closed:
            return
        await self.start()
        async with self._condition:
            if event.type == "queue_reset":
                for batch in self._pending.values():
                    if batch.typing_users:
                        # Clearing a stuck typing block is itself activity:
                        # restart the quiet window so a batch held for a long
                        # time is not flushed the instant the blockage clears
                        # (the reset may have dropped further typing events).
                        # The max-wait deadline is untouched, so a missing
                        # stop still cannot starve the batch past its cap.
                        batch.typing_users.clear()
                        batch.active_at = self._now()
                self._condition.notify_all()
                return
            if event.message is not None:
                self._record_message(event.message, event.event_id)
            elif event.typing is not None:
                self._record_typing(event.typing)
            self._condition.notify_all()

    def _record_message(self, message: ZulipMessage, event_id: int | None) -> None:
        if message.message_id in self._seen_messages:
            self._seen_messages.move_to_end(message.message_id)
            return
        self._seen_messages[message.message_id] = None
        while len(self._seen_messages) > self.config.seen_messages_limit:
            self._seen_messages.popitem(last=False)
        now = self._now()
        batch = self._pending.get(message.chat_id)
        if batch is None:
            batch = self._pending[message.chat_id] = _PendingBatch(now, now)
        else:
            batch.active_at = now
        order_event_id = event_id if event_id is not None else 2**63 - 1
        batch.messages.append((order_event_id, message))

    def _record_typing(self, typing: TypingEvent) -> None:
        # Typing alone never creates state or an ask.
        batch = self._pending.get(typing.chat_id)
        if batch is None or not batch.messages:
            return
        if typing.op == "start":
            batch.typing_users.add(typing.sender_id)
        else:
            batch.typing_users.discard(typing.sender_id)
        # Start/repeat and stop both return to the ordinary quiet deadline.
        # The max-wait deadline is immutable, so missing stop cannot starve.
        batch.active_at = self._now()

    def _deadline(self, batch: _PendingBatch) -> float:
        quiet_deadline = (
            float("inf")
            if batch.typing_users
            else batch.active_at + self.config.debounce_seconds
        )
        return min(
            quiet_deadline,
            batch.first_at + self.config.max_wait_seconds,
        )

    async def _scheduler(self) -> None:
        while True:
            async with self._condition:
                now = self._now()
                due = [
                    key
                    for key, batch in self._pending.items()
                    if key != self._inflight_chat_id
                    and (
                        self.config.debounce_seconds == 0
                        or self._deadline(batch) <= now
                    )
                ]
                for key in due:
                    batch = self._pending.pop(key)
                    ordered = [
                        item[1]
                        for item in sorted(
                            batch.messages,
                            key=lambda item: (item[0], item[1].message_id),
                        )
                    ]
                    if not ordered:
                        continue
                    if self.config.debounce_seconds == 0:
                        # Disabled mode documents immediate one-message turns:
                        # a burst that accumulated while this chat's ask was
                        # in flight must not collapse into one combined prompt.
                        # Queue one ready entry per message, in order, so each
                        # becomes its own Prime turn.
                        for item in ordered:
                            self._ready.append(_ReadyBatch(key, [item]))
                    else:
                        self._ready.append(_ReadyBatch(key, ordered))
                if due:
                    self._condition.notify_all()
                    continue
                if not self._pending:
                    await self._condition.wait()
                    continue
                eligible = [
                    self._deadline(batch)
                    for key, batch in self._pending.items()
                    if key != self._inflight_chat_id
                ]
                if not eligible:
                    # Every pending batch belongs to the in-flight chat and
                    # cannot fire while it runs. Waiting without a timeout is
                    # safe: the worker notifies when the in-flight ask ends.
                    # (Folding the in-flight deadline into the timeout below
                    # would compute 0 for an elapsed deadline and busy-spin.)
                    await self._condition.wait()
                    continue
                timeout = max(0.0, min(eligible) - now)
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    pass

    async def _ask_worker(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(lambda: bool(self._ready))
                ready = self._ready.popleft()
                self._inflight_chat_id = ready.chat_id
            try:
                await self._relay_batch(ready.messages)
            finally:
                async with self._condition:
                    self._inflight_chat_id = None
                    self._condition.notify_all()

    async def _relay_batch(self, messages: list[ZulipMessage]) -> None:
        last = messages[-1]
        prompt = format_batch(messages)
        # Content-free: conversation text must not leak into terminal/logs.
        logger.info(
            "relaying prompt to prime: chat=%s messages=%d last_sender=%s prompt_len=%d",
            last.chat_id,
            len(messages),
            last.sender_full_name or last.sender_email,
            len(prompt),
        )
        current = asyncio.current_task()
        try:
            await self.bridge.typing_start(last.chat_id, last.metadata)
            pinger = asyncio.create_task(
                _typing_pinger(self.bridge, last.chat_id, last.metadata),
                name="zulip-outbound-typing",
            )
            try:
                answer = await self.prime.ask(prompt)
            except PrimeError as exc:
                logger.error("prime-agent failed: %s", exc)
                answer = f":warning: prime-agent could not answer: {exc}"
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("unexpected relay failure")
                answer = f":warning: bridge error: {exc}"
            finally:
                pinger.cancel()
                try:
                    await pinger
                except asyncio.CancelledError:
                    # Awaiting the just-cancelled pinger raises its
                    # cancellation; only swallow that, never the worker's
                    # own, or close() could never collect this task.
                    if current is None or current.cancelling() == 0:
                        pass
                    else:
                        raise
        finally:
            # Runs even when the worker is cancelled mid-typing_start: an
            # indicator left "on" pins the whole conversation's UI, while a
            # stop after a start that never landed is harmless. suppress
            # (not Exception) never swallows a pending cancellation, and a
            # failing stop must not mask the original exception either.
            with contextlib.suppress(Exception):
                await self.bridge.typing_stop(last.chat_id, last.metadata)

        if not answer.strip():
            answer = "_(prime-agent finished without a text response)_"
        await self.bridge.reply(last, answer)


TYPING_REFRESH_INTERVAL = 7.0


async def _typing_pinger(
    bridge: PrimeZulipBridge,
    chat_id: str,
    metadata: dict[str, object] | None,
) -> None:
    while True:
        await asyncio.sleep(TYPING_REFRESH_INTERVAL)
        try:
            await bridge.typing_start(chat_id, metadata)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("typing refresh failed: %s", exc)


def format_batch(messages: list[ZulipMessage]) -> str:
    """Join a burst with stable boundaries and labels when identity matters."""
    if not messages:
        return ""
    sender_ids = {message.sender_id for message in messages}
    label = len(sender_ids) > 1 or (
        messages[0].is_dm
        and len(messages[0].metadata.get("zulip_recipient_user_ids") or []) > 2
    )
    parts: list[str] = []
    for message in messages:
        body = message.content
        if label:
            name = message.sender_full_name or message.sender_email or str(message.sender_id)
            body = f"{name}:\n{body}"
        parts.append(body)
    return "\n\n--- Zulip message boundary ---\n\n".join(parts)
