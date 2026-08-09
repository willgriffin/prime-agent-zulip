"""Zulip bridge authorization — user/email allowlists for inbound messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Allowlist:
    """Controls which Zulip users can interact with the bridge.

    At least one allowlist must be populated, otherwise all inbound messages
    are rejected.  Supports user IDs and emails; matches are exact after
    stripping / lowercasing.
    """

    user_ids: set[int] = field(default_factory=set)
    emails: set[str] = field(default_factory=set)

    @classmethod
    def from_env(
        cls,
        *,
        user_ids_env: str = "ZULIP_ALLOWED_USER_IDS",
        emails_env: str = "ZULIP_ALLOWED_EMAILS",
    ) -> Allowlist:
        """Build an Allowlist from comma-separated environment variables."""
        import os

        return cls(
            user_ids=_parse_int_set(os.environ.get(user_ids_env, "")),
            emails=_parse_str_set(os.environ.get(emails_env, "")),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.user_ids or self.emails)

    def allows(self, *, user_id: int | None = None, email: str | None = None) -> bool:
        if not self.is_configured:
            return False
        if user_id is not None and user_id in self.user_ids:
            return True
        if email is not None and email.lower().strip() in self.emails:
            return True
        return False


def _parse_int_set(raw: str) -> set[int]:
    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            pass
    return result


def _parse_str_set(raw: str) -> set[str]:
    return {s.strip().lower() for s in raw.split(",") if s.strip()}
