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
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_COMMAND = "prime-agent"
DEFAULT_START_TIMEOUT = 30.0
DEFAULT_RESPONSE_TIMEOUT = 600.0

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
    response_timeout: float = DEFAULT_RESPONSE_TIMEOUT
    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> PrimeConfig:
        """Build a config from ``PRIME_*`` environment variables.

        No secret is ever placed in argv -- anything sensitive reaches the
        agent through the inherited environment, which is why `env` is merged
        over `os.environ` rather than replacing it.
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
            response_timeout=_float(
                src.get("PRIME_AGENT_RESPONSE_TIMEOUT"), DEFAULT_RESPONSE_TIMEOUT
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

        env = {**os.environ, **self.config.env}

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
        self._pump = asyncio.create_task(self._read_stdout(), name="prime-stdout")
        self._stderr_pump = asyncio.create_task(self._read_stderr(), name="prime-stderr")

    async def stop(self) -> None:
        """Close stdin so the agent exits on EOF, then reap it."""
        proc, self._proc = self._proc, None

        for task in (self._pump, self._stderr_pump):
            if task is not None:
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
        await self._send({"id": request_id, "type": "prompt", "message": message})

        parts: list[str] = []
        deadline = asyncio.get_running_loop().time() + self.config.response_timeout

        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise PrimeError(
                    f"prime-agent did not finish within {self.config.response_timeout:.0f}s"
                )

            try:
                event = await asyncio.wait_for(self._events.get(), timeout=remaining)
            except asyncio.TimeoutError:
                raise PrimeError(
                    f"prime-agent did not finish within {self.config.response_timeout:.0f}s"
                ) from None

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

            if kind == "agent_end":
                # Prefer the terminal summary when it carries messages: it is
                # the authoritative list for the run, and using it avoids
                # double-counting a message we already saw end.
                summary = _messages_text(event.get("messages"))
                text = summary if summary else "\n\n".join(p for p in parts if p)
                return text.strip()

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
        proc = self._proc
        if proc is None or proc.stdout is None:
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
                    self._offer(raw)

            if buffer:
                self._offer(bytes(buffer))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("prime-agent stdout pump failed: %s", exc)
        finally:
            if self._events is not None:
                self._events.put_nowait(_EOF)

    def _offer(self, raw: bytes) -> None:
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
        if isinstance(event, dict) and self._events is not None:
            self._events.put_nowait(event)

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
