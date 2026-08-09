"""CLI entry point for the Prime Agent Zulip bridge.

Usage:
    prime-zulip listen           Poll Zulip and print events
    prime-zulip send <msg>       Send a one-shot message to the home channel
    prime-zulip dm <user> <msg>  Send a DM to a user
    prime-zulip status           Check connection status
    prime-zulip heartbeat        Run as a Prime Agent heartbeat worker
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from . import PrimeZulipBridge, ZulipEvent


def _env_config() -> dict[str, Any]:
    """Load bridge config from environment variables and config file."""
    # Try config file first
    config_file = Path.home() / ".config" / "prime-zulip" / "env"
    if config_file.exists():
        for line in config_file.read_text().splitlines():
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and val and key not in os.environ:
                    os.environ[key] = val

    site = os.environ.get("ZULIP_SITE", os.environ.get("ZULIP_SITE_URL", ""))
    email = os.environ.get("ZULIP_EMAIL", os.environ.get("ZULIP_BOT_EMAIL", ""))
    api_key = os.environ.get("ZULIP_API_KEY", "")

    user_ids_raw = os.environ.get("ZULIP_ALLOWED_USER_IDS", "")
    user_ids = {int(x.strip()) for x in user_ids_raw.split(",") if x.strip().isdigit()}

    emails_raw = os.environ.get("ZULIP_ALLOWED_EMAILS", "")
    emails = {x.strip().lower() for x in emails_raw.split(",") if x.strip()}

    home_channel = os.environ.get("ZULIP_HOME_CHANNEL", "")
    home_dm_user = None
    if home_channel.startswith("dm:"):
        uid = home_channel.removeprefix("dm:")
        if uid.isdigit():
            home_dm_user = int(uid)

    return {
        "site": site,
        "email": email,
        "api_key": api_key,
        "user_ids": user_ids,
        "emails": emails,
        "home_dm_user": home_dm_user,
    }


async def _cmd_listen() -> None:
    """Listen for Zulip events and print them."""
    config = _env_config()
    bridge = PrimeZulipBridge(
        site=config["site"],
        email=config["email"],
        api_key=config["api_key"],
        allowed_user_ids=config["user_ids"],
        allowed_emails=config["emails"],
    )

    print(f"Connecting to {config['site']} as {config['email']}...")
    async with bridge:
        print("Connected! Listening for messages...")
        async for event in bridge.events():
            if event.message:
                msg = event.message
                print(f"\n[{msg.sender_full_name}] {msg.content[:200]}")
                # Echo back for now
                await bridge.reply(msg, f"Received: {msg.content[:500]}")
            elif event.reaction:
                r = event.reaction
                print(f"\nReaction: {r.user_email} {r.op} {r.emoji_name} on msg {r.message_id}")


async def _cmd_send(message: str) -> None:
    """Send a one-shot message."""
    config = _env_config()
    if not config["home_dm_user"]:
        print("No ZULIP_HOME_CHANNEL dm: user configured", file=sys.stderr)
        sys.exit(1)

    bridge = PrimeZulipBridge(
        site=config["site"],
        email=config["email"],
        api_key=config["api_key"],
        allowed_user_ids=config["user_ids"],
        allowed_emails=config["emails"],
    )

    async with bridge:
        ids = await bridge.send_dm(config["home_dm_user"], message)
        print(f"Sent message(s): {ids}")


async def _cmd_dm(user_id: str, message: str) -> None:
    """Send a DM to a specific user."""
    config = _env_config()

    bridge = PrimeZulipBridge(
        site=config["site"],
        email=config["email"],
        api_key=config["api_key"],
        allowed_user_ids=config["user_ids"],
        allowed_emails=config["emails"],
    )

    async with bridge:
        uid = int(user_id)
        ids = await bridge.send_dm(uid, message)
        print(f"Sent message(s): {ids}")


async def _cmd_status() -> None:
    """Check connection status."""
    config = _env_config()
    if not config["site"] or not config["email"] or not config["api_key"]:
        print("Not configured — set ZULIP_SITE, ZULIP_EMAIL, ZULIP_API_KEY")
        sys.exit(1)

    bridge = PrimeZulipBridge(
        site=config["site"],
        email=config["email"],
        api_key=config["api_key"],
    )

    async with bridge:
        print(f"Site: {config['site']}")
        print(f"Bot: {bridge._bot_email} (ID: {bridge._bot_user_id})")
        print(f"Queue: {bridge._queue_id}")
        print(f"Allowlist: {bridge.config.allowlist}")
        print("Connection: OK")


async def _cmd_heartbeat() -> None:
    """Run as a Prime Agent heartbeat worker.

    Reads ~/.prime/zulip/state.json for pending work.
    On each tick: check Zulip for new messages, process one if available,
    and write state.
    """
    state_dir = Path.home() / ".prime" / "zulip"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / "state.json"

    config = _env_config()
    bridge = PrimeZulipBridge(
        site=config["site"],
        email=config["email"],
        api_key=config["api_key"],
        allowed_user_ids=config["user_ids"],
        allowed_emails=config["emails"],
    )

    async with bridge:
        # Process one event with a timeout
        try:
            async for event in bridge.events():
                if event.message:
                    msg = event.message
                    print(json.dumps({
                        "type": "message",
                        "sender": msg.sender_full_name,
                        "content": msg.content[:200],
                        "chat_id": msg.chat_id,
                    }))
                    # Write state for the agent to pick up
                    state_file.write_text(json.dumps({
                        "last_message_id": msg.message_id,
                        "last_sender": msg.sender_full_name,
                        "last_content": msg.content,
                        "chat_id": msg.chat_id,
                        "timestamp": msg.timestamp,
                    }))
                break  # Process one event per heartbeat tick
        except asyncio.TimeoutError:
            pass


def main() -> None:
    """CLI entry point."""
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(1)

    cmd = args[0]

    if cmd == "listen":
        asyncio.run(_cmd_listen())
    elif cmd == "send":
        if len(args) < 2:
            print("Usage: prime-zulip send <message>", file=sys.stderr)
            sys.exit(1)
        asyncio.run(_cmd_send(" ".join(args[1:])))
    elif cmd == "dm":
        if len(args) < 3:
            print("Usage: prime-zulip dm <user_id> <message>", file=sys.stderr)
            sys.exit(1)
        asyncio.run(_cmd_dm(args[1], " ".join(args[2:])))
    elif cmd == "status":
        asyncio.run(_cmd_status())
    elif cmd == "heartbeat":
        asyncio.run(_cmd_heartbeat())
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
