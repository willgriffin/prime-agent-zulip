"""Prime Agent RPC client — speaks prime-agent's JSONL protocol over a subprocess.

`prime-agent --mode rpc` is a *client* behaviour of the agent, not a server of
its own: on a machine where the daemon supervisor is already running, an RPC
process attaches to it. Starting one here therefore does not compete with a
`--mode daemon` unit and must not be confused for replacing it.

The protocol is documented in prime-agent's `docs/rpc.md`. Two details from it
drive the shape of this module:

* **Framing is strict JSONL with LF as the only delimiter.** The upstream docs
  call out by name that generic line readers are not protocol-compliant,
  because they also split on ``U+2028``/``U+2029`` -- both of which are legal
  unescaped inside JSON strings, and both of which show up in text pasted from
  the web. So this reads bytes and splits on ``b"\\n"`` itself rather than
  iterating the stream as text.

* **A prompt sent while the agent is streaming is rejected** unless it carries
  a ``streamingBehavior``. Rather than guess a behaviour per message, this
  client serialises: one prompt at a time, under a lock. A second operator
  message waits for the first answer instead of racing it, which is also the
  only way a reply can be attributed to the prompt that caused it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from collections import deque
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_COMMAND = "prime-agent"
DEFAULT_START_TIMEOUT = 30.0
DEFAULT_RESPONSE_TIMEOUT = 600.0

# Bounded separately from the answer: a write that blocks is a stuck reader on
# the far end, and the answer timeout cannot rescue it because the send happens
# under the same lock every later prompt queues on.
DEFAULT_SEND_TIMEOUT = 30.0

# Default streamingBehavior for prompts: a prompt that lands while the agent
# is still streaming an earlier answer is queued as a follow-up rather than
# rejected. "followUp" (not "steer") matches the bridge's one-turn-at-a-time
# serialisation: the follow-up runs only after the current turn fully stops.
DEFAULT_STREAMING_BEHAVIOR = "followUp"

# Stripped from the agent's environment before launch.
#
# The agent has tool and shell access and its output is relayed verbatim back
# into Zulip -- and for a stream mention that reply is visible to everyone in
# the stream, not just the allowlisted sender. So the bot's own Zulip
# credential must not be reachable from inside it: "print your environment"
# would otherwise publish the key to chat. Nothing in prime-agent needs it;
# the bridge is the only Zulip client here.
SCRUBBED_ENV = frozenset({"ZULIP_API_KEY"})

# Read chunk for the stdout pump. Assistant turns are far larger than a line,
# so this is a throughput knob, not a correctness one.
_READ_CHUNK = 65536


class PrimeError(RuntimeError):
    """The agent rejected a command, died, or failed to answer in time."""


@dataclass
class PrimeConfig:
    """How to launch and talk to prime-agent.

    Everything here is injectable from the environment so a NixOS module can
    supply it without putting anything on the command line.
    """

    command: str = DEFAULT_COMMAND
    extra_args: list[str] = field(default_factory=list)
    cwd: str | None = None
    session_dir: str | None = None
    no_session: bool = False
    start_timeout: float = DEFAULT_START_TIMEOUT
    send_timeout: float = DEFAULT_SEND_TIMEOUT
    response_timeout: float = DEFAULT_RESPONSE_TIMEOUT
    # Sent on every prompt so a message that races an in-flight stream is
    # queued as a follow-up instead of rejected (docs/rpc.md). Empty disables.
    streaming_behavior: str = DEFAULT_STREAMING_BEHAVIOR
    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> PrimeConfig:
        """Build a config from ``PRIME_*`` environment variables.

        No secret is ever placed in argv. What the agent does need reaches it
        through the inherited environment -- minus everything in
        :data:`SCRUBBED_ENV`, which the launch strips before exec.
        """
        src = os.environ if environ is None else environ

        raw_args = src.get("PRIME_AGENT_ARGS", "").strip()
        extra_args = raw_args.split() if raw_args else []

        return cls(
            command=src.get("PRIME_AGENT_BIN", DEFAULT_COMMAND),
            extra_args=extra_args,
            cwd=src.get("PRIME_AGENT_CWD") or None,
            session_dir=src.get("PRIME_AGENT_SESSION_DIR") or None,
            no_session=_truthy(src.get("PRIME_AGENT_NO_SESSION", "")),
            start_timeout=_float(src.get("PRIME_AGENT_START_TIMEOUT"), DEFAULT_START_TIMEOUT),
            send_timeout=_float(src.get("PRIME_AGENT_SEND_TIMEOUT"), DEFAULT_SEND_TIMEOUT),
            response_timeout=_float(
                src.get("PRIME_AGENT_RESPONSE_TIMEOUT"), DEFAULT_RESPONSE_TIMEOUT
            ),
            streaming_behavior=src.get(
                "PRIME_AGENT_STREAMING_BEHAVIOR", DEFAULT_STREAMING_BEHAVIOR
            ),
        )

    def argv(self) -> list[str]:
        argv = [self.command, "--mode", "rpc"]
        if self.no_session:
            argv.append("--no-session")
        if self.session_dir:
            argv += ["--session-dir", self.session_dir]
        argv += self.extra_args
        return argv


class PrimeClient:
    """A single long-lived ``prime-agent --mode rpc`` subprocess.

    The agent keeps one conversation across calls, so the Zulip operator gets
    continuity rather than a cold agent per message.
    """

    def __init__(self, config: PrimeConfig | None = None):
        self.config = config or PrimeConfig()
        self._proc: asyncio.subprocess.Process | None = None
        self._events: asyncio.Queue[dict[str, Any]] | None = None
        self._held: deque[dict[str, Any]] = deque()
        self._pump: asyncio.Task[None] | None = None
        self._stderr_pump: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._counter = 0

    # ── lifecycle ─────────────────────────────────────────────────

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def __aenter__(self) -> PrimeClient:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        """Launch the agent. A no-op if it is already up."""
        if self.is_running:
            return

        argv = self.config.argv()
        if shutil.which(argv[0]) is None and not os.path.isabs(argv[0]):
            raise PrimeError(
                f"prime-agent binary {argv[0]!r} not found on PATH; "
                "set PRIME_AGENT_BIN to an absolute path"
            )

        env = {k: v for k, v in os.environ.items() if k not in SCRUBBED_ENV}
        env.update(self.config.env)

        logger.info("starting prime-agent: %s", " ".join(argv))
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.config.cwd,
                env=env,
            )
        except OSError as exc:
            raise PrimeError(f"could not start prime-agent: {exc}") from exc

        self._events = asyncio.Queue()
        self._held = deque()
        self._pump = asyncio.create_task(self._read_stdout(), name="prime-stdout")
        self._stderr_pump = asyncio.create_task(self._read_stderr(), name="prime-stderr")

    async def stop(self) -> None:
        """Close stdin so the agent exits on EOF, then reap it."""
        proc, self._proc = self._proc, None

        pumps = [t for t in (self._pump, self._stderr_pump) if t is not None]
        for task in pumps:
            task.cancel()
        self._pump = self._stderr_pump = None

        if proc is not None and proc.returncode is None:
            # The documented shutdown is EOF on stdin: "accepts prompts until
            # EOF". Kill only if it ignores that.
            try:
                if proc.stdin is not None and not proc.stdin.is_closing():
                    proc.stdin.close()
            except (BrokenPipeError, ConnectionResetError):
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("prime-agent ignored EOF; killing")
                proc.kill()
                await proc.wait()

        # Await the cancellations rather than merely requesting them. Leaving
        # them pending both produces "Task was destroyed but it is pending"
        # noise and means a later start() could install a fresh queue while a
        # previous-generation pump is still technically alive.
        if pumps:
            await asyncio.gather(*pumps, return_exceptions=True)

        self._events = None
        logger.info("prime-agent stopped")

    # ── asking ────────────────────────────────────────────────────

    async def ask(self, message: str) -> str:
        """Send a prompt and return the assistant's text for that run.

        Serialised: concurrent callers queue rather than interleave, so the
        text returned always belongs to the prompt that was passed in.
        """
        async with self._lock:
            if not self.is_running:
                await self.start()
            return await self._ask_locked(message)

    async def _ask_locked(self, message: str) -> str:
        assert self._proc is not None and self._events is not None

        # Drain anything left over from a previous run so a late event cannot
        # be mistaken for this run's output.
        self._drain()

        self._counter += 1
        request_id = f"zulip-{self._counter}"
        command: dict[str, Any] = {"id": request_id, "type": "prompt", "message": message}
        if self.config.streaming_behavior:
            command["streamingBehavior"] = self.config.streaming_behavior
        try:
            await asyncio.wait_for(
                self._send(command),
                timeout=self.config.send_timeout,
            )
        except asyncio.TimeoutError:
            raise PrimeError(
                f"prime-agent did not accept input within {self.config.send_timeout:.0f}s"
            ) from None

        parts: list[str] = []
        deadline = asyncio.get_running_loop().time() + self.config.response_timeout
        query_counter = 0

        while True:
            event = await self._next_event(deadline)

            if event is _EOF:
                raise PrimeError("prime-agent exited before answering")

            kind = event.get("type")

            # A rejected prompt is the one failure reported as a response;
            # anything after acceptance arrives as events instead.
            if kind == "response" and event.get("id") == request_id:
                if not event.get("success", False):
                    raise PrimeError(
                        f"prime-agent rejected the prompt: "
                        f"{event.get('error') or event.get('message') or 'no reason given'}"
                    )
                continue

            if kind == "message_end":
                parts.extend(_assistant_text(event.get("message")))
                continue

            if kind != "agent_end":
                continue

            # Prefer the terminal summary when it carries messages: it is
            # the authoritative list for the run, and using it avoids
            # double-counting a message we already saw end.
            summary = _messages_text(event.get("messages"))
            text = (summary if summary else "\n\n".join(p for p in parts if p)).strip()
            logger.info(
                "prime-agent request %s reached %s with %d assistant text chars",
                request_id,
                kind,
                len(text),
            )
            if text:
                return text

            query_counter += 1
            state_id = f"{request_id}-state-{query_counter}"
            state = await self._query_state(state_id, deadline)
            continuation = _has_continuation(state)
            logger.info(
                "prime-agent request %s had an empty %s; state %s",
                request_id,
                kind,
                _state_brief(state),
            )
            if continuation is True:
                continue
            if continuation is False:
                return ""
            raise PrimeError(
                "prime-agent reached an empty agent_end, but its completion state "
                "could not be read"
            )

    async def _next_event(self, deadline: float) -> dict[str, Any]:
        assert self._events is not None
        if self._held:
            # Parked while a get_state answer was in flight; replay in
            # receive order before anything newer from the stream. popleft()
            # keeps draining O(1) per event instead of list.pop(0)'s O(n).
            return self._held.popleft()
        return await self._queued_event(deadline)

    async def _queued_event(self, deadline: float) -> dict[str, Any]:
        assert self._events is not None
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise PrimeError(
                f"prime-agent did not finish within {self.config.response_timeout:.0f}s"
            )
        try:
            return await asyncio.wait_for(self._events.get(), timeout=remaining)
        except asyncio.TimeoutError:
            raise PrimeError(
                f"prime-agent did not finish within {self.config.response_timeout:.0f}s"
            ) from None

    async def _query_state(self, request_id: str, deadline: float) -> dict[str, Any] | None:
        """Ask the agent for its state without losing events.

        Ordinary 0.7.1 RPC events carry no prompt-correlation ID, so an empty
        ``agent_end`` is ambiguous: it can be the final quiet turn, or a
        session-action boundary with another cycle queued immediately behind
        it. ``get_state`` is the only exposed discriminator. Events received
        while that response is in flight are parked on ``self._held`` and
        replayed first by ``_next_event``. The stdout pump keeps writing to
        the one queue it bound at start, so events that arrive after the
        state answer stay visible to this same run.
        """
        try:
            await asyncio.wait_for(
                self._send({"id": request_id, "type": "get_state"}),
                timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            )
        except (PrimeError, asyncio.TimeoutError):
            return None

        while True:
            # Read the live queue only: replaying ``self._held`` here would
            # re-park the same events forever.
            event = await self._queued_event(deadline)
            if event is _EOF:
                raise PrimeError("prime-agent exited before answering")
            if event.get("type") == "response" and event.get("id") == request_id:
                if not event.get("success", False):
                    return None
                data = event.get("data")
                return data if isinstance(data, dict) else None
            self._held.append(event)

    async def _send(self, command: dict[str, Any]) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None or proc.returncode is not None:
            raise PrimeError("prime-agent is not running")
        payload = (json.dumps(command) + "\n").encode()
        try:
            proc.stdin.write(payload)
            await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise PrimeError(f"prime-agent closed its input: {exc}") from exc

    def _drain(self) -> None:
        self._held.clear()
        if self._events is None:
            return
        while True:
            try:
                self._events.get_nowait()
            except asyncio.QueueEmpty:
                return

    # ── stream pumps ──────────────────────────────────────────────

    async def _read_stdout(self) -> None:
        """Split the agent's stdout on LF only and queue each JSON record.

        Deliberately not `async for line in stream` / `readline`: per
        `docs/rpc.md` a reader that also breaks on Unicode line separators is
        not protocol-compliant, and Python's text-mode line iteration
        additionally rewrites bare CR. Bytes in, LF out.
        """
        # Bind to this generation's process *and* queue. Reading either off
        # the instance later would let a pump that outlives its own stop()
        # write into a successor's queue.
        proc = self._proc
        events = self._events
        if proc is None or proc.stdout is None or events is None:
            return

        buffer = bytearray()
        try:
            while True:
                chunk = await proc.stdout.read(_READ_CHUNK)
                if not chunk:
                    break
                buffer.extend(chunk)
                while True:
                    index = buffer.find(b"\n")
                    if index < 0:
                        break
                    raw = bytes(buffer[:index])
                    del buffer[: index + 1]
                    self._offer(events, raw)

            if buffer:
                self._offer(events, bytes(buffer))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("prime-agent stdout pump failed: %s", exc)
        finally:
            events.put_nowait(_EOF)

    def _offer(self, events: asyncio.Queue[dict[str, Any]], raw: bytes) -> None:
        # Tolerate CRLF input by stripping one trailing CR, as the spec asks.
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        if not raw.strip():
            return
        try:
            event = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("prime-agent emitted a non-JSON line; ignoring")
            return
        if isinstance(event, dict):
            events.put_nowait(event)

    async def _read_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").rstrip()
                if text:
                    logger.warning("prime-agent: %s", text)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            pass


# Sentinel queued when the agent's stdout closes, so a waiting `ask` fails
# immediately instead of sitting until its timeout.
_EOF: dict[str, Any] = {"type": "__eof__"}


def _has_continuation(state: dict[str, Any] | None) -> bool | None:
    """Classify whether an empty ``agent_end`` has runnable continuation work.

    ``True`` means wait for the next cycle, ``False`` means the boundary is
    demonstrably quiet, and ``None`` means the state did not carry enough
    information to make that call (including when ``_query_state`` could not
    read any state at all). Queued *steering* is deliberately not a
    continuation here: while this client holds its own lock no accepted prompt
    of ours is streaming to be steered, so unrelated steering cannot become
    this answer.
    """
    if not isinstance(state, dict):
        return None
    if (
        state.get("isStreaming") is True
        or state.get("isCompacting") is True
        or state.get("isBashRunning") is True
        or state.get("isRunningTools") is True
    ):
        return True

    actions = state.get("sessionActions")
    if isinstance(actions, dict):
        active = actions.get("active")
        if isinstance(active, dict) and active:
            return True
        # followUps can represent unrelated autonomous work while this client has
        # exclusive prompt lock; they are not reliable evidence that this ask is
        # not finished.

    try:
        unfinished = int(state.get("unfinishedActionCount", 0))
    except (TypeError, ValueError):
        return None
    if unfinished > 0:
        return True

    # isStreaming=false with no unfinished actions is a quiescent
    # boundary even when the server omits optional booleans such as
    # isCompacting.
    if state.get("isStreaming") is False:
        return False
    return None


def _state_brief(state: dict[str, Any] | None) -> str:
    """Content-safe state summary for logs: booleans/counts only, no prompts."""
    if not isinstance(state, dict):
        return "unavailable"
    actions = state.get("sessionActions")
    active = actions.get("active") if isinstance(actions, dict) else None
    try:
        followups = len(actions.get("followUps") or []) if isinstance(actions, dict) else 0
    except TypeError:
        followups = 0
    return (
        "isStreaming=%r isCompacting=%r isBashRunning=%r isRunningTools=%r "
        "queuedActionCount=%r unfinishedActionCount=%r followUps=%d active=%s"
        % (
            state.get("isStreaming"),
            state.get("isCompacting"),
            state.get("isBashRunning"),
            state.get("isRunningTools"),
            state.get("queuedActionCount"),
            state.get("unfinishedActionCount"),
            followups,
            "yes" if isinstance(active, dict) and active else "no",
        )
    )


def _assistant_text(message: Any) -> list[str]:
    """Extract text blocks from an assistant message, ignoring thinking/tools."""
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return []
    content = message.get("content")
    if isinstance(content, str):
        return [content] if content.strip() else []
    if not isinstance(content, list):
        return []
    out: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                out.append(text)
    return out


def _messages_text(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""
    parts: list[str] = []
    for message in messages:
        parts.extend(_assistant_text(message))
    return "\n\n".join(parts).strip()


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float(value: str | None, default: float) -> float:
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default
