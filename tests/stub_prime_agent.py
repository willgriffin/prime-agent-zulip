#!/usr/bin/env python3
"""A stand-in for `prime-agent --mode rpc`, speaking the real JSONL protocol.

Behaviour is driven by the prompt text so one stub can cover the happy path,
rejection, silence, death and the framing edge cases. Kept deliberately dumb:
it reads bytes, splits on LF, and writes LF-terminated JSON, exactly like the
contract in prime-agent's `docs/rpc.md`.
"""

from __future__ import annotations

import json
import sys


def emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


QUIET_STATE = {
    "isStreaming": False,
    "isCompacting": False,
    "isBashRunning": False,
    "isRunningTools": False,
    "queuedActionCount": 0,
    "unfinishedActionCount": 0,
    "sessionActions": {"queuedCount": 0, "steering": [], "followUps": []},
}

# Set by continue scenarios once their intermediate boundary has been emitted;
# the bridge's get_state query at that boundary must see runnable work.
STATE: dict | None = None
# Set when a scenario has emitted its empty boundary and still owes a
# continuation. The continuation is emitted *after* answering the bridge's
# get_state, reproducing the production interleaving where agent_start
# follows the empty agent_end while the state query is in flight.
PENDING: str | None = None


def assistant(text: str) -> dict:
    return {
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "stopReason": "stop",
    }


def handle(command: dict) -> None:
    global STATE, PENDING
    if command.get("type") == "get_state":
        # Continue scenarios update this after their first boundary. All other
        # prompts are answered before a state query exists, so the default is
        # the quiet state that makes a true empty completion stay empty.
        if STATE == "ERROR":
            emit(
                {
                    "id": command.get("id"),
                    "type": "response",
                    "command": "get_state",
                    "success": False,
                    "error": "stubbed state failure",
                }
            )
        else:
            emit(
                {
                    "id": command.get("id"),
                    "type": "response",
                    "command": "get_state",
                    "success": True,
                    "data": STATE if STATE is not None else QUIET_STATE,
                }
            )
        pending, PENDING = PENDING, None
        if pending == "CONTINUE_TEXT":
            # The continued root cycle begins only once the state answer has
            # been written, exactly as the daemon emitted agent_start ~22ms
            # after the boundary in the incident.
            STATE = QUIET_STATE
            emit({"type": "agent_start"})
            final = assistant("continued answer")
            emit({"type": "message_end", "message": final})
            emit({"type": "agent_end", "messages": [final]})
        elif pending == "CONTINUE_UNRELATED":
            STATE = QUIET_STATE
            emit({"type": "agent_start"})
            unrelated = assistant("unrelated answer")
            emit({"type": "message_end", "message": unrelated})
            emit({"type": "agent_end", "messages": [unrelated]})
        return

    if command.get("type") != "prompt":
        emit({"id": command.get("id"), "type": "response", "command": command.get("type"), "success": True})
        return

    message = str(command.get("message", ""))
    request_id = command.get("id")

    if message == "REJECT":
        emit(
            {
                "id": request_id,
                "type": "response",
                "command": "prompt",
                "success": False,
                "error": "stubbed rejection",
            }
        )
        return

    emit({"id": request_id, "type": "response", "command": "prompt", "success": True})

    if message == "DIE":
        sys.exit(3)

    if message == "HANG":
        # Accepted, then never finishes: exercises the response timeout.
        for line in sys.stdin.buffer:  # pragma: no cover - blocks until killed
            pass
        return

    emit({"type": "agent_start"})

    if message == "SHOW_BEHAVIOR":
        # Echo the raw payload field so tests can assert what the client sent.
        behavior = command.get("streamingBehavior") or "none"
        msg = assistant(f"behavior: {behavior}")
        emit({"type": "message_end", "message": msg})
        emit({"type": "agent_end", "messages": [msg]})
        return

    if message == "TOOL_ONLY":
        # A turn that runs a tool and says nothing. Valid, and must not be
        # reported as an assistant reply.
        emit(
            {
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "no prose for this one"},
                        {"type": "toolCall", "id": "c1", "name": "bash", "arguments": {}},
                    ],
                },
            }
        )
        emit({"type": "agent_end", "messages": []})
        return

    if message == "CONTINUE_TEXT":
        # Production regression shape: a tool-only root boundary with no text,
        # while actionable work is still queued and the next root cycle continues.
        emit(
            {
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "checking state"},
                        {"type": "toolCall", "id": "c1", "name": "bash", "arguments": {}},
                    ],
                    "stopReason": "toolUse",
                },
            }
        )
        emit({"type": "agent_end", "messages": []})
        STATE = {
            **QUIET_STATE,
            "isStreaming": True,
            "unfinishedActionCount": 1,
            "sessionActions": {
                "queuedCount": 1,
                "steering": [],
                "followUps": [{"label": "continue"}],
            },
        }
        emit({"type": "agent_start"})
        PENDING = "CONTINUE_TEXT"
        return

    if message == "CONTINUE_UNRELATED":
        # Empty boundary, then a follow-up cycle that belongs to other session
        # work. State correlation cannot prove ownership in RPC 0.7.1; after a
        # demonstrably quiescent boundary the answer is still the local empty
        # one rather than the unrelated prose.
        emit({"type": "agent_end", "messages": []})
        STATE = {
            **QUIET_STATE,
            "sessionActions": {
                "queuedCount": 1,
                "steering": [],
                "followUps": [{"label": "someone else's continuation"}],
            },
        }
        emit({"type": "agent_start"})
        PENDING = "CONTINUE_UNRELATED"
        return

    if message == "STATE_ERROR":
        # Empty boundary, but state cannot be read: fail explicitly rather
        # than publishing an empty answer that might be premature.
        emit({"type": "agent_end", "messages": []})
        STATE = "ERROR"
        return

    if message == "MULTIPART":
        first = assistant("part one")
        second = assistant("part two")
        emit({"type": "message_end", "message": first})
        emit({"type": "message_end", "message": second})
        emit({"type": "agent_end", "messages": [first, second]})
        return

    if message == "SEPARATORS":
        # U+2028 and U+2029 are legal *unescaped* inside a JSON string, and a
        # reader that treats them as line breaks splits this record into
        # fragments that are not valid JSON. ensure_ascii=False below is the
        # point: the default escapes them, which would test nothing.
        text = "before middle after"
        msg = assistant(text)
        for record in (
            {"type": "message_end", "message": msg},
            {"type": "agent_end", "messages": [msg]},
        ):
            sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        return

    if message == "NOISE":
        # Blank lines and a non-JSON line must be skipped, not fatal.
        sys.stdout.write("\n")
        sys.stdout.write("this is not json\n")
        sys.stdout.flush()
        msg = assistant("survived the noise")
        emit({"type": "message_end", "message": msg})
        emit({"type": "agent_end", "messages": [msg]})
        return

    if message == "CRLF":
        # Emit one record with CRLF framing; the trailing CR must be stripped.
        msg = assistant("crlf ok")
        sys.stdout.write(json.dumps({"type": "message_end", "message": msg}) + "\r\n")
        sys.stdout.write(json.dumps({"type": "agent_end", "messages": [msg]}) + "\r\n")
        sys.stdout.flush()
        return

    msg = assistant(f"echo: {message}")
    emit({"type": "message_end", "message": msg})
    emit({"type": "agent_end", "messages": [msg]})


def main() -> None:
    buffer = bytearray()
    while True:
        chunk = sys.stdin.buffer.read1(65536)
        if not chunk:
            break
        buffer.extend(chunk)
        while True:
            index = buffer.find(b"\n")
            if index < 0:
                break
            raw = bytes(buffer[:index])
            del buffer[: index + 1]
            if raw.endswith(b"\r"):
                raw = raw[:-1]
            if not raw.strip():
                continue
            handle(json.loads(raw.decode()))


if __name__ == "__main__":
    main()
