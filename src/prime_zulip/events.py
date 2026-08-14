"""Zulip event processing — message/reaction routing and categorization."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .auth import Allowlist
from .messages import ZulipMessage, strip_leading_mention

logger = logging.getLogger(__name__)

DIRECT_MESSAGE_TYPES = {"direct", "private", "dm"}
MENTION_FLAGS = {"mentioned", "wildcard_mentioned"}
UNSUPPORTED_EVENT_OPS = {
    "delete", "deleted", "edit", "edited",
    "update", "update_message", "remove",
}


@dataclass
class ReactionEvent:
    """A reaction added to or removed from a message."""

    message_id: int
    user_id: int
    user_email: str
    emoji_name: str
    emoji_code: str | None
    op: str  # "add" or "remove"


@dataclass
class TypingEvent:
    """An authorized typing notification for one canonical conversation."""

    op: str
    sender_id: int
    sender_email: str
    chat_id: str
    is_dm: bool
    stream_id: int | None = None
    topic: str | None = None


@dataclass
class ZulipEvent:
    """A normalized inbound event recorded from Zulip's event queue."""

    type: str  # "message", "reaction", "typing", or "queue_reset"
    message: ZulipMessage | None = None
    reaction: ReactionEvent | None = None
    typing: TypingEvent | None = None
    event_id: int | None = None


def categorize_event(
    event: dict[str, Any],
    bot_user_id: str | None,
    bot_email: str,
    bot_names: set[str],
    allowlist: Allowlist,
) -> ZulipEvent | None:
    """Convert a raw Zulip event dict into a ZulipEvent or None if ignored."""
    event_type = event.get("type")

    event_id = _event_id(event)

    if event_type == "message":
        msg = _parse_message_event(event, bot_user_id, bot_email, bot_names, allowlist)
        if msg is None:
            return None
        return ZulipEvent(type="message", message=msg, event_id=event_id)

    if event_type == "reaction":
        reaction = _parse_reaction_event(event, bot_user_id, allowlist)
        if reaction is None:
            return None
        return ZulipEvent(type="reaction", reaction=reaction, event_id=event_id)

    if event_type == "typing":
        typing = _parse_typing_event(event, bot_user_id, bot_email, allowlist)
        if typing is None:
            return None
        return ZulipEvent(type="typing", typing=typing, event_id=event_id)

    return None


def _parse_message_event(
    event: dict[str, Any],
    bot_user_id: str | None,
    bot_email: str,
    bot_names: set[str],
    allowlist: Allowlist,
) -> ZulipMessage | None:
    if _is_unsupported_event(event):
        return None

    message = event.get("message")
    if not isinstance(message, dict):
        return None

    # Skip own messages
    if _is_self_message(message, bot_user_id, bot_email):
        return None

    msg_type = str(message.get("type", "")).lower()
    is_dm = msg_type in DIRECT_MESSAGE_TYPES
    is_stream = msg_type == "stream"
    if not is_dm and not is_stream:
        return None

    # Authorization
    sender_id = message.get("sender_id")
    sender_email = str(message.get("sender_email", "")).lower()
    if not allowlist.allows(user_id=sender_id, email=sender_email):
        return None

    raw_content = str(message.get("content", ""))
    content = raw_content

    # For streams, check mention
    is_mentioned = True  # for DMs
    if is_stream:
        flags = {str(f).lower() for f in (message.get("flags") or [])}
        is_mentioned = bool(flags & MENTION_FLAGS)
        if not is_mentioned:
            return None  # don't respond to unmentioned stream messages
        content = strip_leading_mention(content, bot_names)

    # Build chat_id for routing
    participant_ids: tuple[int, ...] = ()
    if is_dm:
        chat_id, chat_name, participant_ids = _dm_routing(message, bot_user_id)
    else:
        chat_id, chat_name = _stream_routing(message)

    return ZulipMessage(
        message_id=int(message.get("id", 0)),
        sender_id=int(sender_id or 0),
        sender_email=sender_email,
        sender_full_name=str(message.get("sender_full_name", "")),
        content=content,
        raw_content=raw_content,
        chat_id=chat_id,
        chat_name=chat_name,
        is_dm=is_dm,
        is_stream=is_stream,
        is_mentioned=is_mentioned,
        stream_name=str(message.get("display_recipient", "")) if is_stream else None,
        topic=str(message.get("topic") or message.get("subject") or ""),
        stream_id=message.get("stream_id"),
        timestamp=message.get("timestamp"),
        metadata={
            "zulip_message_id": message.get("id"),
            "zulip_sender_id": sender_id,
            "zulip_sender_email": sender_email,
            "zulip_stream_id": message.get("stream_id"),
            "zulip_topic": message.get("topic") or message.get("subject"),
            "zulip_recipient_id": message.get("recipient_id"),
            "zulip_recipient_user_ids": list(participant_ids),
            "zulip_bot_user_id": _int_or_none(bot_user_id),
        },
    )


def _parse_typing_event(
    event: dict[str, Any],
    bot_user_id: str | None,
    bot_email: str,
    allowlist: Allowlist,
) -> TypingEvent | None:
    """Parse typing without letting it bypass or modify message authorization."""
    op = str(event.get("op", "")).lower()
    if op not in {"start", "stop"}:
        return None

    sender = event.get("sender") if isinstance(event.get("sender"), dict) else {}
    sender_id = _int_or_none(sender.get("user_id") or sender.get("id") or event.get("sender_id"))
    sender_email = str(sender.get("email") or event.get("sender_email") or "").lower()
    if sender_id is None:
        return None
    if str(sender_id) == str(bot_user_id) or (
        sender_email and sender_email == bot_email.lower()
    ):
        return None
    if not allowlist.allows(user_id=sender_id, email=sender_email):
        return None

    message_type = str(event.get("message_type") or event.get("type_name") or "").lower()
    if message_type in DIRECT_MESSAGE_TYPES:
        recipients = event.get("recipients")
        participants = recipients if isinstance(recipients, list) else []
        participant_ids = _participant_ids(participants)
        participant_ids.add(sender_id)
        bot_id = _int_or_none(bot_user_id)
        if bot_id is not None:
            participant_ids.add(bot_id)
        if not participant_ids:
            return None
        chat_id = "dm:" + ",".join(str(uid) for uid in sorted(participant_ids))
        return TypingEvent(
            op=op,
            sender_id=sender_id,
            sender_email=sender_email,
            chat_id=chat_id,
            is_dm=True,
        )

    if message_type in {"stream", "channel"}:
        stream_id = _int_or_none(event.get("stream_id"))
        topic = str(event.get("topic") or "")
        if stream_id is None or not topic:
            return None
        return TypingEvent(
            op=op,
            sender_id=sender_id,
            sender_email=sender_email,
            chat_id=f"stream:{stream_id}:topic:{topic}",
            is_dm=False,
            stream_id=stream_id,
            topic=topic,
        )

    return None


def _event_id(event: dict[str, Any]) -> int | None:
    return _int_or_none(event.get("id"))


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _recipient_user_id(recipient: dict[str, Any]) -> int | None:
    return _int_or_none(recipient.get("user_id") or recipient.get("id"))


def _participant_ids(recipients: list[Any]) -> set[int]:
    return {
        uid
        for recipient in recipients
        if isinstance(recipient, dict)
        for uid in [_recipient_user_id(recipient)]
        if uid is not None
    }


def _parse_reaction_event(
    event: dict[str, Any],
    bot_user_id: str | None,
    allowlist: Allowlist,
) -> ReactionEvent | None:
    """Parse a reaction event, skipping self-reactions and unauthorized users."""
    op = str(event.get("op", "")).lower()
    if op not in {"add", "add_reaction"}:
        return None

    user = event.get("user") or {}
    if isinstance(user, dict):
        user_id = user.get("user_id") or user.get("id")
        user_email = user.get("email", "")
    else:
        user_id = event.get("user_id")
        user_email = event.get("user_email", "")

    # Skip self-reactions
    if bot_user_id is not None and str(user_id) == bot_user_id:
        return None

    if not allowlist.allows(user_id=user_id, email=str(user_email).lower()):
        return None

    return ReactionEvent(
        message_id=int(event.get("message_id", 0)),
        user_id=int(user_id or 0),
        user_email=str(user_email).lower(),
        emoji_name=str(event.get("emoji_name") or event.get("emoji", "")),
        emoji_code=event.get("emoji_code"),
        op="add" if op in {"add", "add_reaction"} else "remove",
    )


def _dm_routing(
    message: dict[str, Any],
    bot_user_id: str | None,
) -> tuple[str, str, tuple[int, ...]]:
    """Build a canonical participant-set key and display name for a DM."""
    display_recipient = message.get("display_recipient")
    participants = display_recipient if isinstance(display_recipient, list) else []
    participant_ids = _participant_ids(participants)
    sender_id = _int_or_none(message.get("sender_id"))
    bot_id = _int_or_none(bot_user_id)
    participant_ids.update(uid for uid in (sender_id, bot_id) if uid is not None)
    ordered_ids = tuple(sorted(participant_ids))
    chat_id = "dm:" + ",".join(str(uid) for uid in ordered_ids) if ordered_ids else "dm:unknown"

    names = [
        str(d.get("full_name") or d.get("email") or "")
        for d in participants
        if isinstance(d, dict) and _recipient_user_id(d) != bot_id
    ]
    chat_name = ", ".join(n for n in names if n)
    if not chat_name:
        chat_name = str(
            message.get("sender_full_name") or message.get("sender_email") or "Zulip DM"
        )

    return chat_id, chat_name, ordered_ids


def _stream_routing(message: dict[str, Any]) -> tuple[str, str]:
    """Build routing key and display name for a stream message."""
    stream_id = message.get("stream_id")
    stream_name = str(message.get("display_recipient") or "unknown")
    topic = str(message.get("topic") or message.get("subject") or "general")
    chat_id = f"stream:{stream_id}:topic:{topic}"
    chat_name = f"{stream_name} / {topic}"
    return chat_id, chat_name


def _is_unsupported_event(event: dict[str, Any]) -> bool:
    ops = {
        str(event.get(k, "")).lower()
        for k in ("op", "operation", "update_type")
        if event.get(k)
    }
    message = event.get("message", {})
    if isinstance(message, dict):
        ops.update(
            str(message.get(k, "")).lower()
            for k in ("op", "operation", "update_type")
            if message.get(k)
        )
    return bool(ops & UNSUPPORTED_EVENT_OPS)


def _is_self_message(
    message: dict[str, Any],
    bot_user_id: str | None,
    bot_email: str,
) -> bool:
    sender_email = str(message.get("sender_email", "")).lower()
    if sender_email and sender_email == bot_email.lower():
        return True
    if bot_user_id is not None:
        return str(message.get("sender_id")) == bot_user_id
    return False
