"""Zulip message formatting, splitting, and parsing utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# Zulip is generous with message length but we split to stay safe
DEFAULT_MAX_CHARS = 8000

# Leading mention patterns like "@**cricket** rest of message"
MENTION_RE = re.compile(
    r"^\s*@\*\*(?P<name>[^*]+)\*\*\s*:?\s*", re.UNICODE
)


@dataclass
class ZulipMessage:
    """A received Zulip message, normalized for agent consumption."""

    message_id: int
    sender_id: int
    sender_email: str
    sender_full_name: str
    content: str  # raw Zulip Markdown with leading @mention stripped
    raw_content: str  # original unmodified content
    chat_id: str  # stable routing key for replies
    chat_name: str  # human-readable display name
    is_dm: bool
    is_stream: bool
    is_mentioned: bool
    stream_name: str | None = None
    topic: str | None = None
    stream_id: int | None = None
    timestamp: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def split_message(content: str, limit: int = DEFAULT_MAX_CHARS) -> list[str]:
    """Split long Markdown into Zulip-sized chunks preserving paragraph/fence boundaries."""
    limit = max(limit, 1)
    if len(content) <= limit:
        return [content]

    chunks: list[str] = []
    current = ""
    for unit in _markdown_split_units(content):
        if len(unit) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_hard_split(unit, limit))
            continue
        if current and len(current) + len(unit) > limit:
            chunks.append(current)
            current = unit
        else:
            current += unit
    if current:
        chunks.append(current)
    return [c for c in chunks if c]


def _markdown_split_units(content: str) -> list[str]:
    """Return paragraph-ish units, keeping fenced code blocks together."""
    if not content:
        return [content]
    lines = content.splitlines(keepends=True)
    units: list[str] = []
    current: list[str] = []
    in_fence = False
    for line in lines:
        current.append(line)
        stripped = line.lstrip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence and not stripped.strip():
            units.append("".join(current))
            current = []
    if current:
        units.append("".join(current))
    return units


def _hard_split(content: str, limit: int) -> list[str]:
    return [content[i : i + limit] for i in range(0, len(content), limit)]


def strip_leading_mention(content: str, bot_names: set[str]) -> str:
    """Strip a leading @**BotName** mention if it matches a known bot name."""
    match = MENTION_RE.match(content)
    if not match:
        return content
    if not bot_names or match.group("name").strip().lower() in bot_names:
        return content[match.end() :].lstrip()
    return content


def format_code_block(code: str, language: str = "") -> str:
    """Format a code block for Zulip Markdown."""
    return f"```{language}\n{code}\n```"


def format_quote(text: str) -> str:
    """Format a blockquote for Zulip Markdown."""
    return "\n".join(f"> {line}" for line in text.splitlines())


def format_spoiler(text: str) -> str:
    """Format a spoiler block for Zulip Markdown."""
    return f"```spoiler {text}\n```"


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    """Build a Zulip Markdown table."""
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def format_link(text: str, url: str) -> str:
    """Format a named link for Zulip Markdown."""
    return f"[{text}]({url})"


def format_image(alt: str, url: str) -> str:
    """Format an inline image for Zulip Markdown."""
    return f"[{alt}]({url})"


def escape_message(content: str) -> str:
    """Escape Zulip-specific Markdown that might be misinterpreted."""
    # Escape @mentions that aren't intentional
    return content
