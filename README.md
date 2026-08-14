# Prime Agent Zulip Bridge

Persistent Zulip chat integration for Prime Agent sessions. Connects a Prime
Agent to a Zulip organization via the Zulip Events API, handling DMs, stream
@mentions, reactions, typing indicators, attachments, and message editing.

## Features

- **Persistent polling** — long-polls Zulip's event queue; auto re-registers on expiry
- **Thought-burst batching** — waits for a quiet pause, honors typing status, and caps the wait
- **Full Markdown** — code blocks, tables, quotes, spoilers, links, images
- **DMs & streams** — responds to DMs and stream @mentions; replies stay in-context
- **Reactions** — add/remove emoji reactions on messages
- **Typing indicators** — show/hide native Zulip typing status
- **Attachments** — upload files and inline images
- **Message editing** — edit or delete sent messages
- **Prime relay** — messages go to a live `prime-agent --mode rpc` session and its answer comes back
- **Authorization** — fail-closed user-ID and email allowlists, optionally requiring both
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
export ZULIP_SITE="https://zulip.example.com"
export ZULIP_EMAIL="agent@example.com"
export ZULIP_API_KEY="your-api-key"
export ZULIP_ALLOWED_USER_IDS="8"
export ZULIP_HOME_CHANNEL="dm:8"

# Optional: require both identifiers to match, not either
export ZULIP_ALLOW_REQUIRE_BOTH="true"
export ZULIP_ALLOWED_EMAILS="you@example.com"
```

Check status:

```bash
prime-zulip status
```

Relay messages to Prime and reply with its answers:

```bash
prime-zulip listen
```

This starts one `prime-agent --mode rpc` subprocess and keeps it for the life
of the process, so the conversation carries across messages. Point it at a
specific build with `PRIME_AGENT_BIN` when `prime-agent` is not on `PATH`.

By default, incoming messages in the same DM/group DM or stream topic are
collected until five seconds of quiet, then sent to Prime as one ordered turn.
Typing start/repeat extends that quiet period only when a message is already
pending; typing stop resumes it, and a hard 20-second cap prevents a missing
stop event from starving the turn. Typing notifications by themselves never
invoke Prime. Set `PRIME_ZULIP_DEBOUNCE_SECONDS=0` to restore immediate
one-message behavior: each message becomes its own Prime turn, including
messages that pile up while a previous turn is still in flight (they are
dispatched one by one, not collapsed into one combined prompt).

Batching is at-most-once and in-memory: a process restart discards pending
fragments rather than replaying old ones, and a Zulip queue re-registration
clears stale typing state while replaying caught-up messages through the same
dedupe and ordering path.

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
    site="https://zulip.example.com",
    email="agent@example.com",
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

Every value can also be supplied through `~/.config/prime-zulip/env` as
`KEY=value` lines, which is what lets a NixOS module inject configuration
without putting anything on the command line. Real environment variables win
over the file.

### Zulip

| Variable | Required | Description |
|----------|----------|-------------|
| `ZULIP_SITE` / `ZULIP_SITE_URL` | Yes | Zulip organization URL |
| `ZULIP_EMAIL` / `ZULIP_BOT_EMAIL` | Yes | Bot/agent email address |
| `ZULIP_API_KEY` | Yes | Zulip API key |
| `ZULIP_ALLOWED_USER_IDS` | Yes* | Comma-separated allowed user IDs |
| `ZULIP_ALLOWED_EMAILS` | Yes* | Comma-separated allowed emails |
| `ZULIP_ALLOW_REQUIRE_BOTH` | No | Require **both** identifiers to match, not either |
| `ZULIP_HOME_CHANNEL` | No | Default DM target, e.g. `dm:8` |

*At least one allowlist must be configured, or all inbound messages are
rejected. Under `ZULIP_ALLOW_REQUIRE_BOTH` **both** must be populated — an
empty list can never satisfy an AND, so a half-configured strict allowlist
rejects everyone and `prime-zulip status` says so rather than reporting itself
healthy.

`requireBoth` is worth more than it looks. A Zulip user ID is stable, but an
administrator can move an email address to a different account. Under the
default OR, that new account inherits access the moment it holds the address;
under AND it does not, because the ID still will not match.

There is no allow-all switch, by design. The allowlist is fail-closed and has
no bypass.

### Prime Agent

| Variable | Required | Description |
|----------|----------|-------------|
| `PRIME_AGENT_BIN` | No | Path to `prime-agent` (default: found on `PATH`) |
| `PRIME_AGENT_ARGS` | No | Extra arguments, space-separated |
| `PRIME_AGENT_CWD` | No | Working directory for the agent |
| `PRIME_AGENT_SESSION_DIR` | No | Custom session storage directory |
| `PRIME_AGENT_NO_SESSION` | No | Set truthy to disable session persistence |
| `PRIME_AGENT_START_TIMEOUT` | No | Seconds to allow for startup (default 30) |
| `PRIME_AGENT_SEND_TIMEOUT` | No | Seconds to allow for writing a prompt (default 30) |
| `PRIME_AGENT_RESPONSE_TIMEOUT` | No | Seconds to allow per answer (default 600) |
| `PRIME_AGENT_STREAMING_BEHAVIOR` | No | `streamingBehavior` sent on prompts: `followUp` (default; queue a prompt racing an in-flight stream as a follow-up), `steer`, or empty to disable |
| `PRIME_ZULIP_DEBOUNCE_SECONDS` | No | Quiet time before dispatch; non-negative seconds, default `5`, `0` disables batching |
| `PRIME_ZULIP_DEBOUNCE_MAX_WAIT_SECONDS` | No | Hard cap from the first message; non-negative seconds, default `20`; values below quiet time are raised to it |
| `PRIME_ZULIP_SEEN_MESSAGES_LIMIT` | No | Dedupe memory bound: most recent message ids kept, default `4096`; a replayed id is re-delivered once it falls off this window |

No credential is ever placed in the agent's argv — argv is world-readable via
`/proc`. What the agent needs reaches it through the inherited environment.

`ZULIP_API_KEY` is **stripped** from that environment before launch. The agent
has tool and shell access and its answers are relayed verbatim into Zulip, so
leaving the bot's own credential reachable would mean "print your environment"
publishes it to the chat — and for a stream mention, to everyone in the stream
rather than only the allowlisted sender. Nothing in prime-agent needs it.

## How the Prime integration works

`prime-zulip listen` runs one long-lived `prime-agent --mode rpc` subprocess
and relays authorized Zulip messages to it, posting the assistant's reply back
into the same conversation with a typing indicator while it works.

**RPC mode is not an alternative to daemon mode.** On a machine already running
`prime-agent --mode daemon`, an RPC process attaches to that supervisor as a
client — prime-agent's own `docs/daemon.md` describes the daemon as internal
infrastructure, with interactive, print, JSON and RPC being client behaviours
that "retain their public I/O contracts". The bridge therefore does not replace
a running daemon and must not be configured to start a second one.

Prompts are **serialised**: one in flight at a time. The protocol rejects a
prompt sent mid-stream unless it carries a `streamingBehavior`, and serialising
is also the only way a reply can be attributed to the prompt that caused it.
Messages arriving while Prime is answering are polled immediately and form the
next conversation batch; one scheduler and one worker bound task growth even
when many conversations are active.

If the agent dies, the next message restarts it. Failures are reported into the
Zulip conversation instead of killing the listener, because a bridge that dies
quietly on one bad turn is indistinguishable from one that was never running.

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

## Development

```bash
pip install -e ".[dev]"
pytest
```

The Prime integration tests drive a real subprocess against
`tests/stub_prime_agent.py`, which speaks the actual JSONL protocol. That is
deliberate: the failures worth catching are framing and lifecycle failures that
a mocked transport would define out of existence.

## License

MIT
