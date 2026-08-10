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

    By default a sender is admitted when *either* identifier matches. Set
    ``require_both`` to demand both. That is stricter than it first looks: the
    two identifiers are not independent, because a Zulip user ID is stable
    while an email address can be reassigned to a new person by an
    administrator. Requiring both means a reassigned address cannot inherit
    the previous holder's access on its own.
    """

    user_ids: set[int] = field(default_factory=set)
    emails: set[str] = field(default_factory=set)
    require_both: bool = False

    @classmethod
    def from_env(
        cls,
        *,
        user_ids_env: str = "ZULIP_ALLOWED_USER_IDS",
        emails_env: str = "ZULIP_ALLOWED_EMAILS",
        require_both_env: str = "ZULIP_ALLOW_REQUIRE_BOTH",
    ) -> Allowlist:
        """Build an Allowlist from comma-separated environment variables."""
        import os

        return cls(
            user_ids=_parse_int_set(os.environ.get(user_ids_env, "")),
            emails=_parse_str_set(os.environ.get(emails_env, "")),
            require_both=_parse_bool(os.environ.get(require_both_env, "")),
        )

    @property
    def is_configured(self) -> bool:
        """Whether the allowlist can admit anyone at all.

        Under ``require_both`` an unpopulated list can never be satisfied, so
        both must be populated for the allowlist to be usable. Reporting
        "configured" when one is empty would describe a policy that rejects
        every sender as if it were working.
        """
        if self.require_both:
            return bool(self.user_ids and self.emails)
        return bool(self.user_ids or self.emails)

    def allows(self, *, user_id: int | None = None, email: str | None = None) -> bool:
        if not self.is_configured:
            return False

        matches_user_id = user_id is not None and user_id in self.user_ids
        matches_email = email is not None and email.lower().strip() in self.emails

        if self.require_both:
            return matches_user_id and matches_email
        return matches_user_id or matches_email


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


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}
