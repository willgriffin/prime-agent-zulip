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
class ZulipEvent:
    """A received Zulip event — either a message or a reaction on a tracked message."""

    type: str  # "message" or "reaction"
    message: ZulipMessage | None = None
    reaction: ReactionEvent | None = None


def categorize_event(
    event: dict[str, Any],
    bot_user_id: str | None,
    bot_email: str,
    bot_names: set[str],
    allowlist: Allowlist,
) -> ZulipEvent | None:
    """Convert a raw Zulip event dict into a ZulipEvent or None if ignored."""
    event_type = event.get("type")

    if event_type == "message":
        msg = _parse_message_event(event, bot_user_id, bot_email, bot_names, allowlist)
        if msg is None:
            return None
        return ZulipEvent(type="message", message=msg)

    if event_type == "reaction":
        reaction = _parse_reaction_event(event, bot_user_id, allowlist)
        if reaction is None:
            return None
        return ZulipEvent(type="reaction", reaction=reaction)

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
    if is_dm:
        chat_id, chat_name = _dm_routing(message, bot_user_id)
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
        },
    )


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
) -> tuple[str, str]:
    """Build routing key and display name for a DM."""
    sender_id = message.get("sender_id")
    chat_id = f"dm:{sender_id}" if sender_id is not None else "dm:unknown"

    display_recipient = message.get("display_recipient")
    if isinstance(display_recipient, list):
        names = [d.get("full_name", d.get("email", "")) for d in display_recipient if isinstance(d, dict)]
        # Exclude bot from display name
        if bot_user_id:
            names = [
                d.get("full_name", d.get("email", ""))
                for d in display_recipient
                if isinstance(d, dict) and str(d.get("id")) != bot_user_id
            ]
        chat_name = ", ".join(n for n in names if n) or "Zulip DM"
    else:
        chat_name = str(message.get("sender_full_name") or message.get("sender_email") or "Zulip DM")

    return chat_id, chat_name


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
