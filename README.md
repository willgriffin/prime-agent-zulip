# Prime Agent Zulip Bridge

Persistent Zulip chat integration for Prime Agent sessions. Connects a Prime
Agent to a Zulip organization via the Zulip Events API, handling DMs, stream
@mentions, reactions, typing indicators, attachments, and message editing.

## Features

- **Persistent polling** — long-polls Zulip's event queue; auto re-registers on expiry
- **Full Markdown** — code blocks, tables, quotes, spoilers, links, images
- **DMs & streams** — responds to DMs and stream @mentions; replies stay in-context
- **Reactions** — add/remove emoji reactions on messages
- **Typing indicators** — show/hide native Zulip typing status
- **Attachments** — upload files and inline images
- **Message editing** — edit or delete sent messages
- **Authorization** — user-ID and email allowlists gate all inbound messages
- **Heartbeat mode** — run as a Prime Agent heartbeat worker for persistence
- **Async API** — full `asyncio`/`httpx`-native; works as a library or CLI

## Installation

```bash
pip install git+https://github.com/willgriffin/prime-agent-zulip.git
```

Or via `uv`:

```bash
uv pip install git+https://github.com/willgriffin/prime-agent-zulip.git
```

## Quick Start

### CLI

Set environment variables:

```bash
export ZULIP_SITE="https://chat.happyvertical.com"
export ZULIP_EMAIL="cricket@happyvertical.com"
export ZULIP_API_KEY="your-api-key"
export ZULIP_ALLOWED_USER_IDS="8"
export ZULIP_HOME_CHANNEL="dm:8"
```

Check status:

```bash
prime-zulip status
```

Listen for messages:

```bash
prime-zulip listen
```

Send a one-shot DM:

```bash
prime-zulip send "Hello from Prime Agent!"
```

Run as a heartbeat worker:

```bash
prime-zulip heartbeat
```

### Python API

```python
from prime_zulip import PrimeZulipBridge

bridge = PrimeZulipBridge(
    site="https://chat.happyvertical.com",
    email="cricket@happyvertical.com",
    api_key="...",
    allowed_user_ids={8},
)

async with bridge:
    # Listen for events
    async for event in bridge.events():
        if event.message:
            msg = event.message
            print(f"From {msg.sender_full_name}: {msg.content}")

            # Reply in the same conversation
            await bridge.reply(msg, "Got it!")

            # Add a reaction
            await bridge.react(msg.message_id, "thumbs_up")

        elif event.reaction:
            r = event.reaction
            print(f"{r.user_email} reacted with {r.emoji_name}")

    # Send a DM directly
    await bridge.send_dm(8, "Heads up!")

    # Send to a stream
    await bridge.send_stream(42, "general", "Hello stream!")

    # Send with an image
    await bridge.send_with_image("dm:8", "Check this out:", "/path/to/image.png")

    # Edit a message
    await bridge.edit(12345, "Updated content")

    # Show typing indicator
    await bridge.typing_start("dm:8")
    # ... do work ...
    await bridge.typing_stop("dm:8")
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ZULIP_SITE` / `ZULIP_SITE_URL` | Yes | Zulip organization URL |
| `ZULIP_EMAIL` / `ZULIP_BOT_EMAIL` | Yes | Bot/agent email address |
| `ZULIP_API_KEY` | Yes | Zulip API key |
| `ZULIP_ALLOWED_USER_IDS` | Yes* | Comma-separated allowed user IDs |
| `ZULIP_ALLOWED_EMAILS` | Yes* | Comma-separated allowed emails |
| `ZULIP_HOME_CHANNEL` | No | Default DM target, e.g. `dm:8` |
| `ZULIP_ALLOW_ALL_USERS` | No | Set to `true` to allow all users (dev only) |

*At least one allowlist must be configured, or all inbound messages are rejected.

## Heartbeat Integration

For persistent operation with Prime Agent, register a heartbeat:

```python
# In your Prime Agent session:
await rlm_heartbeat.create(
    name="zulip-check",
    command="prime-zulip heartbeat",
    interval_seconds=30,
)
```

The heartbeat writes incoming message state to `~/.prime/zulip/state.json`
for the agent to pick up on its next turn.

## License

MIT
