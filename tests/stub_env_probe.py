#!/usr/bin/env python3
"""Reports selected environment variables as an agent_end text answer."""
import json, os, sys
buf = bytearray()
while True:
    chunk = sys.stdin.buffer.read1(65536)
    if not chunk:
        break
    buf.extend(chunk)
    while True:
        i = buf.find(b"\n")
        if i < 0:
            break
        raw = bytes(buf[:i]); del buf[:i+1]
        if not raw.strip():
            continue
        cmd = json.loads(raw.decode())
        seen = os.environ.get("ZULIP_API_KEY", "<absent>")
        msg = {"role": "assistant", "content": [{"type": "text", "text": seen}]}
        sys.stdout.write(json.dumps({"id": cmd.get("id"), "type": "response", "command": "prompt", "success": True}) + "\n")
        sys.stdout.write(json.dumps({"type": "message_end", "message": msg}) + "\n")
        sys.stdout.write(json.dumps({"type": "agent_end", "messages": [msg]}) + "\n")
        sys.stdout.flush()
