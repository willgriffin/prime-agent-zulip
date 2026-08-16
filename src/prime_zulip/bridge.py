"""Prime Zulip Bridge — the main entry point for Prime Agent Zulip integration."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .auth import Allowlist
from .client import BadEventQueueError, ZulipAPIError, ZulipClient
from .events import ZulipEvent, categorize_event
from .messages import DEFAULT_MAX_CHARS, ZulipMessage, split_message

logger = logging.getLogger(__name__)

POLL_ERROR_SLEEP = 5.0

# Pause after a queue re-register attempt before polling again. Re-registering
# replaces the queue; if it just failed (rate limit, 5xx) the stale queue is
# still in place and an immediate poll repeats the same failure.
REREGISTER_BACKOFF_SECONDS = 2.0


@dataclass
class BridgeConfig:
    """Configuration for the Prime Zulip Bridge."""

    site_url: str
    email: str
    api_key: str
    allowlist: Allowlist
    bot_names: set[str]  # names the bot responds to (for @mention detection)
    max_message_chars: int = DEFAULT_MAX_CHARS
    longpoll_timeout: int = 90
    attachment_max_bytes: int = 25_000_000
    attachment_max_count: int = 5


class PrimeZulipBridge:
    """Persistent Zulip bridge for a Prime Agent.

    Connects to Zulip via the Events API, long-polls for incoming messages and
    reactions, and provides methods to reply with full Markdown, attachments,
    and reactions.

    Usage::

        bridge = PrimeZulipBridge(
            site="https://zulip.example.com",
            email="agent@example.com",
            api_key="...",
            allowed_user_ids={8},
        )

        async with bridge:
            async for event in bridge.events():
                if event.message:
                    await bridge.reply(event.message, "Hello!")
                elif event.reaction:
                    await bridge.reply_text(
                        event.message_id,
                        f"Thanks for the {event.reaction.emoji_name}!"
                    )
    """

    def __init__(
        self,
        *,
        site: str,
        email: str,
        api_key: str,
        allowed_user_ids: set[int] | None = None,
        allowed_emails: set[str] | None = None,
        require_both: bool = False,
        bot_names: set[str] | None = None,
        max_message_chars: int = DEFAULT_MAX_CHARS,
        attachment_max_bytes: int = 25_000_000,
        attachment_max_count: int = 5,
    ):
        allowlist = Allowlist(
            user_ids=allowed_user_ids or set(),
            emails=allowed_emails or set(),
            require_both=require_both,
        )
        if bot_names is None:
            local_part = email.split("@", 1)[0] if email else ""
            bot_names = {local_part, local_part.replace("_", " "), local_part.replace("-", " ")}

        self.config = BridgeConfig(
            site_url=site.rstrip("/"),
            email=email,
            api_key=api_key,
            allowlist=allowlist,
            bot_names=bot_names,
            max_message_chars=max_message_chars,
            attachment_max_bytes=attachment_max_bytes,
            attachment_max_count=attachment_max_count,
        )

        self._client: ZulipClient | None = None
        self._queue_id: str | None = None
        self._last_event_id: int = -1
        self._bot_user_id: str | None = None
        self._bot_email: str = email
        self._stop: asyncio.Event | None = None
        self._poll_task: asyncio.Task | None = None
        self._event_queue: asyncio.Queue[ZulipEvent] | None = None

    # ── context manager ───────────────────────────────────────────

    async def __aenter__(self) -> PrimeZulipBridge:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        """Initialize the HTTP client, register an event queue, and start polling."""
        if not self.config.allowlist.is_configured:
            if self.config.allowlist.require_both:
                logger.warning(
                    "Zulip bridge: requireBoth is set but only one of the user-ID / email "
                    "allowlists is populated — all messages will be rejected"
                )
            else:
                logger.warning(
                    "Zulip bridge: no allowlist configured — all messages will be rejected"
                )

        self._client = ZulipClient(
            site_url=self.config.site_url,
            email=self.config.email,
            api_key=self.config.api_key,
        )

        # Discover bot identity
        try:
            profile = await self._client.get_profile()
            self._bot_user_id = str(profile.get("user_id", ""))
            self._bot_email = profile.get("email", self._bot_email)
            logger.info(
                "Zulip bridge authenticated as %s (user %s)",
                profile.get("full_name", self._bot_email),
                self._bot_user_id,
            )
        except Exception as exc:
            logger.error("Zulip bridge failed to fetch profile: %s", exc)
            raise

        # Register event queue
        queue_data = await self._client.register_queue()
        self._queue_id = queue_data["queue_id"]
        self._last_event_id = max(int(queue_data.get("last_event_id", -1)), self._last_event_id)

        logger.info("Zulip bridge connected — queue %s", self._queue_id)

        # Start polling
        self._stop = asyncio.Event()
        self._event_queue = asyncio.Queue()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="zulip-poll")

    async def disconnect(self) -> None:
        """Stop polling, delete the event queue, and close the client."""
        if self._stop:
            self._stop.set()

        if self._poll_task:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None

        if self._client and self._queue_id:
            try:
                await self._client.delete_queue(self._queue_id)
            except Exception:
                pass

        if self._client:
            await self._client.close()
            self._client = None

        self._queue_id = None
        self._stop = None
        self._event_queue = None
        logger.info("Zulip bridge disconnected")

    # ── event stream ──────────────────────────────────────────────

    async def events(self) -> AsyncIterator[ZulipEvent]:
        """Async generator yielding Zulip events as they arrive."""
        if self._event_queue is None:
            raise RuntimeError("Bridge not connected — call connect() or use 'async with'")
        while True:
            event = await self._event_queue.get()
            yield event

    # ── sending ───────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to_message_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[int]:
        """Send a Markdown message to a Zulip conversation.

        Returns list of sent message IDs (multiple if content was split).
        """
        if self._client is None:
            raise RuntimeError("Bridge not connected")

        meta = metadata or {}
        chunks = split_message(content, self.config.max_message_chars)
        message_ids: list[int] = []

        for chunk in chunks:
            payload = _build_send_payload(chat_id, chunk, meta, reply_to_message_id)
            resp = await self._client.send_message(payload)
            msg_id = resp.get("id")
            if msg_id is not None:
                message_ids.append(int(msg_id))

        return message_ids

    async def reply(
        self,
        message: ZulipMessage,
        content: str,
    ) -> list[int]:
        """Reply to a received message in its conversation."""
        return await self.send(
            message.chat_id,
            content,
            reply_to_message_id=message.message_id,
            metadata=message.metadata,
        )

    async def send_dm(
        self,
        user_id: int,
        content: str,
    ) -> list[int]:
        """Send a direct message to a user by ID."""
        return await self.send(f"dm:{user_id}", content)

    async def send_stream(
        self,
        stream_id: int,
        topic: str,
        content: str,
    ) -> list[int]:
        """Send a message to a stream topic."""
        return await self.send(f"stream:{stream_id}:topic:{topic}", content)

    async def edit(self, message_id: int, content: str) -> None:
        """Edit an existing message."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        await self._client.edit_message(message_id, content)

    async def delete_message(self, message_id: int) -> None:
        """Delete a message."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        await self._client.delete_message(message_id)

    # ── typing ────────────────────────────────────────────────────

    async def typing_start(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        """Show typing indicator in a conversation."""
        await self._send_typing_op(chat_id, "start", metadata)

    async def typing_stop(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        """Stop typing indicator in a conversation."""
        await self._send_typing_op(chat_id, "stop", metadata)

    async def _send_typing_op(
        self,
        chat_id: str,
        op: str,
        metadata: dict[str, Any] | None,
    ) -> None:
        if self._client is None:
            return
        meta = metadata or {}
        try:
            if chat_id.startswith("stream:"):
                parts = chat_id.removeprefix("stream:").split(":topic:")
                stream_id = int(parts[0]) if len(parts) > 0 else meta.get("zulip_stream_id")
                topic = parts[1] if len(parts) > 1 else meta.get("zulip_topic")
                if stream_id and topic:
                    await self._client.send_typing(op=op, type="stream", to=[int(stream_id)], topic=topic)
            elif chat_id.startswith("dm:"):
                participant_ids = _direct_recipient_ids(chat_id, meta)
                if participant_ids:
                    await self._client.send_typing(
                        op=op, type="direct", to=participant_ids
                    )
        except Exception as exc:
            logger.debug("Zulip typing %s failed: %s", op, exc)

    # ── reactions ─────────────────────────────────────────────────

    async def react(self, message_id: int, emoji: str, emoji_code: str | None = None) -> None:
        """Add an emoji reaction to a message."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        await self._client.add_reaction(message_id, emoji, emoji_code)

    async def unreact(self, message_id: int, emoji: str, emoji_code: str | None = None) -> None:
        """Remove an emoji reaction from a message."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        await self._client.remove_reaction(message_id, emoji, emoji_code)

    # ── attachments ───────────────────────────────────────────────

    async def upload(self, file_path: str | Path) -> dict[str, Any]:
        """Upload a file to Zulip. Returns {uri, url}."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        return await self._client.upload_file(str(file_path))

    async def send_with_image(
        self,
        chat_id: str,
        content: str,
        image_path: str | Path,
        *,
        reply_to_message_id: int | None = None,
    ) -> list[int]:
        """Send a message with an inline image attachment."""
        upload_result = await self.upload(image_path)
        image_url = upload_result.get("url") or upload_result.get("uri", "")
        full_content = f"{content}\n\n![image]({image_url})"
        return await self.send(chat_id, full_content, reply_to_message_id=reply_to_message_id)

    # ── info ──────────────────────────────────────────────────────

    async def get_streams(self) -> list[dict[str, Any]]:
        """Get all visible streams."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        return await self._client.get_streams()

    async def get_topics(self, stream_id: int) -> list[dict[str, Any]]:
        """Get topics in a stream."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        return await self._client.get_topics(stream_id)

    async def get_users(self) -> list[dict[str, Any]]:
        """Get all users in the organization."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        return await self._client.get_users()

    # ── message history ───────────────────────────────────────────

    async def get_recent(
        self,
        anchor: str = "newest",
        num_before: int = 50,
        narrow: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch recent messages, optionally narrowed."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        resp = await self._client.get_messages(anchor=anchor, num_before=num_before, narrow=narrow)
        return resp.get("messages", [])

    async def mark_read(self, message_ids: list[int]) -> None:
        """Mark messages as read."""
        if self._client is None:
            raise RuntimeError("Bridge not connected")
        await self._client.update_message_flags(add="read", messages=message_ids)

    # ── internal poll loop ────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while not (self._stop and self._stop.is_set()):
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Zulip poll error: %s", exc)
                if self._stop:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=POLL_ERROR_SLEEP)
                    except asyncio.TimeoutError:
                        continue

    async def _poll_once(self) -> None:
        if self._client is None or self._queue_id is None:
            return

        try:
            payload = await self._client.get_events(self._queue_id, self._last_event_id)
        except BadEventQueueError:
            logger.warning("Zulip event queue expired — re-registering")
            await self._reregister_queue()
            # A re-register that fails (rate limit, 5xx) leaves the stale
            # queue in place; polling again immediately just re-raises the
            # same error and spins. Back off before the next attempt.
            await asyncio.sleep(REREGISTER_BACKOFF_SECONDS)
            return

        if payload.get("result") == "error":
            code = str(payload.get("code", "")).upper()
            msg = str(payload.get("msg", "")).lower()
            if code == "BAD_EVENT_QUEUE_ID" or "bad event queue" in msg:
                logger.warning("Zulip event queue expired — re-registering")
                await self._reregister_queue()
                return
            raise ZulipAPIError(payload.get("msg") or payload.get("code") or "events error")

        events = payload.get("events") or []
        highest_event_id = self._last_event_id

        for event in events:
            event_id = event.get("id")
            if event_id is not None:
                try:
                    event_id = int(event_id)
                    if highest_event_id is None or event_id > highest_event_id:
                        highest_event_id = event_id
                except (TypeError, ValueError):
                    pass

            zulip_event = categorize_event(
                event,
                self._bot_user_id,
                self._bot_email,
                self.config.bot_names,
                self.config.allowlist,
            )
            if zulip_event is not None and self._event_queue is not None:
                await self._event_queue.put(zulip_event)

        self._last_event_id = highest_event_id

    async def _reregister_queue(self) -> None:
        try:
            # Typing is transient and cannot be reconstructed across queues.
            # Notify consumers before any event from the replacement queue.
            if self._event_queue is not None:
                await self._event_queue.put(ZulipEvent(type="queue_reset"))
            queue_data = await self._client.register_queue()
            self._queue_id = queue_data["queue_id"]
            # Event ids are queue-local: the new queue numbers its events
            # from its own last_event_id, so the previous queue's watermark
            # must NOT carry over. Keeping it (max()) made every post-
            # recovery poll ask the fresh queue for an id it does not hold,
            # returning "Event N was not in this queue" and re-registering
            # forever (#23).
            self._last_event_id = int(queue_data.get("last_event_id", -1))
            logger.info("Zulip bridge re-registered queue %s", self._queue_id)
        except Exception as exc:
            logger.error("Zulip bridge failed to re-register queue: %s", exc)


def _build_send_payload(
    chat_id: str,
    content: str,
    metadata: dict[str, Any],
    reply_to_message_id: int | None,
) -> dict[str, Any]:
    """Build a Zulip message send payload from a chat_id."""
    payload: dict[str, Any] = {"content": content}

    if reply_to_message_id:
        # Zulip doesn't have a reply_to parameter; we use the topic for streams
        # For DMs, the reply is just a new message in the same DM
        pass

    if chat_id.startswith("stream:"):
        parts = chat_id.removeprefix("stream:").split(":topic:")
        stream_to = parts[0]
        topic = parts[1] if len(parts) > 1 else ""
        payload["type"] = "stream"
        payload["to"] = stream_to
        payload["topic"] = topic
    elif chat_id.startswith("dm:"):
        recipient_ids = _direct_recipient_ids(chat_id, metadata)
        payload["type"] = "direct"
        if recipient_ids:
            payload["to"] = json.dumps(recipient_ids)
        else:
            # Fall back to email only for legacy/non-canonical callers.
            sender_email = metadata.get("zulip_sender_email", "")
            payload["to"] = json.dumps([sender_email])
    else:
        raise ValueError(f"Unsupported chat_id: {chat_id}")

    return payload


def _direct_recipient_ids(
    chat_id: str,
    metadata: dict[str, Any],
) -> list[int]:
    """Return all non-bot DM participants for replies and typing operations."""
    raw_ids = metadata.get("zulip_recipient_user_ids")
    if isinstance(raw_ids, list):
        candidates = raw_ids
    else:
        candidates = chat_id.removeprefix("dm:").split(",")
    bot_id = metadata.get("zulip_bot_user_id")
    result: set[int] = set()
    for value in candidates:
        try:
            user_id = int(value)
        except (TypeError, ValueError):
            continue
        if bot_id is not None and str(user_id) == str(bot_id):
            continue
        result.add(user_id)
    if not result and metadata.get("zulip_sender_id") is not None:
        try:
            result.add(int(metadata["zulip_sender_id"]))
        except (TypeError, ValueError):
            pass
    return sorted(result)

