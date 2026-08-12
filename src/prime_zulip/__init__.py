"""
Prime Agent Zulip Bridge — persistent chat integration for Prime Agent sessions.

Provides a lightweight async bridge that connects a Prime Agent to a Zulip
organization via the Zulip Events API. Agents receive DMs and stream @mentions
and reply with full Markdown, code blocks, images, and reactions.

Usage:
    from prime_zulip import PrimeZulipBridge

    bridge = PrimeZulipBridge(
        site="https://zulip.example.com",
        email="agent@example.com",
        api_key="...",
        allowed_user_ids={8},
    )

    async with bridge:
        async for event in bridge.events():
            await bridge.reply(event, "Got it!")
"""

from .bridge import PrimeZulipBridge, ZulipEvent, ZulipMessage
from .auth import Allowlist
from .client import BadEventQueueError, ZulipAPIError
from .prime import PrimeClient, PrimeConfig, PrimeError

__all__ = [
    "PrimeZulipBridge",
    "ZulipEvent",
    "ZulipMessage",
    "Allowlist",
    "BadEventQueueError",
    "ZulipAPIError",
    "PrimeClient",
    "PrimeConfig",
    "PrimeError",
]
