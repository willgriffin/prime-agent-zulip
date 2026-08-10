"""Integration tests for the prime-agent RPC client, against a stubbed Prime.

These drive a real subprocess speaking the real JSONL protocol, because the
failures worth catching here are framing and lifecycle failures that a mocked
transport would define out of existence.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from prime_zulip.prime import PrimeClient, PrimeConfig, PrimeError

STUB = Path(__file__).parent / "stub_prime_agent.py"


def stub_config(**overrides) -> PrimeConfig:
    config = PrimeConfig(command=sys.executable, extra_args=[])
    # The stub stands in for the whole `prime-agent --mode rpc` argv, so drive
    # it through the interpreter rather than letting argv() prepend flags the
    # stub does not understand.
    config.argv = lambda: [sys.executable, str(STUB)]  # type: ignore[method-assign]
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


class TestRoundTrip:
    async def test_prompt_returns_assistant_text(self):
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("hello") == "echo: hello"

    async def test_conversation_persists_across_asks(self):
        """One subprocess serves many messages — the operator gets continuity."""
        async with PrimeClient(stub_config()) as prime:
            first = await prime.ask("one")
            pid_after_first = prime._proc.pid
            second = await prime.ask("two")
            assert first == "echo: one"
            assert second == "echo: two"
            assert prime._proc.pid == pid_after_first

    async def test_multiple_assistant_messages_are_joined(self):
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("MULTIPART") == "part one\n\npart two"

    async def test_tool_only_turn_yields_empty_string(self):
        """Thinking and toolCall blocks are not prose and must not be relayed."""
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("TOOL_ONLY") == ""


class TestFraming:
    async def test_unicode_line_separators_survive(self):
        """U+2028/U+2029 inside a JSON string must not be treated as newlines.

        `docs/rpc.md` calls this out by name. A generic line reader splits the
        record into invalid-JSON fragments and loses the turn.
        """
        async with PrimeClient(stub_config()) as prime:
            answer = await prime.ask("SEPARATORS")
        assert answer == "before middle after"

    async def test_crlf_framing_is_accepted(self):
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("CRLF") == "crlf ok"

    async def test_blank_and_non_json_lines_are_skipped(self):
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("NOISE") == "survived the noise"


class TestFailures:
    async def test_rejected_prompt_raises(self):
        async with PrimeClient(stub_config()) as prime:
            with pytest.raises(PrimeError, match="rejected"):
                await prime.ask("REJECT")

    async def test_agent_death_raises_rather_than_hanging(self):
        async with PrimeClient(stub_config()) as prime:
            with pytest.raises(PrimeError, match="exited"):
                await prime.ask("DIE")

    async def test_timeout_is_enforced(self):
        async with PrimeClient(stub_config(response_timeout=0.5)) as prime:
            with pytest.raises(PrimeError, match="did not finish"):
                await prime.ask("HANG")

    async def test_missing_binary_is_reported_clearly(self):
        config = PrimeConfig(command="prime-agent-does-not-exist")
        with pytest.raises(PrimeError, match="not found on PATH"):
            await PrimeClient(config).start()

    async def test_restarts_after_death(self):
        """A dead agent must not wedge the bridge for every later message."""
        prime = PrimeClient(stub_config())
        await prime.start()
        try:
            with pytest.raises(PrimeError):
                await prime.ask("DIE")
            assert not prime.is_running
            assert await prime.ask("back up") == "echo: back up"
        finally:
            await prime.stop()


class TestSerialisation:
    async def test_concurrent_asks_do_not_interleave(self):
        """Answers must belong to their own prompts.

        Sending a second prompt mid-stream is a protocol error unless it
        carries a streamingBehavior; serialising sidesteps that and keeps
        attribution correct.
        """
        import asyncio

        async with PrimeClient(stub_config()) as prime:
            answers = await asyncio.gather(
                prime.ask("a"), prime.ask("b"), prime.ask("c")
            )
        assert sorted(answers) == ["echo: a", "echo: b", "echo: c"]


class TestConfig:
    def test_argv_defaults_to_rpc_mode(self):
        assert PrimeConfig().argv() == ["prime-agent", "--mode", "rpc"]

    def test_argv_carries_session_options(self):
        config = PrimeConfig(no_session=True, session_dir="/tmp/s", extra_args=["--offline"])
        assert config.argv() == [
            "prime-agent",
            "--mode",
            "rpc",
            "--no-session",
            "--session-dir",
            "/tmp/s",
            "--offline",
        ]

    def test_from_env_reads_prime_variables(self):
        config = PrimeConfig.from_env(
            {
                "PRIME_AGENT_BIN": "/nix/store/x/bin/prime-agent",
                "PRIME_AGENT_ARGS": "--offline --verbose",
                "PRIME_AGENT_CWD": "/var/lib/prime",
                "PRIME_AGENT_NO_SESSION": "yes",
                "PRIME_AGENT_RESPONSE_TIMEOUT": "42",
            }
        )
        assert config.command == "/nix/store/x/bin/prime-agent"
        assert config.extra_args == ["--offline", "--verbose"]
        assert config.cwd == "/var/lib/prime"
        assert config.no_session is True
        assert config.response_timeout == 42.0

    def test_no_secret_is_placed_in_argv(self):
        """Credentials reach the agent through the environment, never argv.

        argv is world-readable through /proc on the machines this runs on.
        """
        config = PrimeConfig.from_env(
            {"PRIME_AGENT_BIN": "prime-agent", "ZULIP_API_KEY": "super-secret"}
        )
        assert "super-secret" not in " ".join(config.argv())

    @pytest.mark.parametrize("raw", ["0", "-1", "notanumber", ""])
    def test_bad_timeouts_fall_back_to_default(self, raw):
        config = PrimeConfig.from_env({"PRIME_AGENT_RESPONSE_TIMEOUT": raw})
        assert config.response_timeout > 0
