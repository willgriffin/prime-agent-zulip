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

    async def test_prompt_carries_default_streaming_behavior(self):
        """Follow-up prompts are queued, not rejected, when an answer is streaming."""
        async with PrimeClient(stub_config()) as prime:
            assert await prime.ask("SHOW_BEHAVIOR") == "behavior: followUp"

    async def test_streaming_behavior_is_overridable(self):
        async with PrimeClient(stub_config(streaming_behavior="steer")) as prime:
            assert await prime.ask("SHOW_BEHAVIOR") == "behavior: steer"

    async def test_streaming_behavior_can_be_omitted(self):
        async with PrimeClient(stub_config(streaming_behavior="")) as prime:
            assert await prime.ask("SHOW_BEHAVIOR") == "behavior: none"

    async def test_empty_boundary_with_continuation_waits_for_later_text(self):
        """The incident shape: tool-only agent_end, state still busy, then prose.

        The stub emits the continuation agent_start/message_end/agent_end *after*
        answering the bridge's get_state query, reproducing the production
        interleaving where agent_start followed the empty boundary ~22ms
        later. If _query_state ever orphaned the stdout pump queue this test
        would hang and exceed the 3 s cap instead of returning in ms.
        """
        async with PrimeClient(stub_config(response_timeout=3.0)) as prime:
            assert await prime.ask("CONTINUE_TEXT") == "continued answer"

    async def test_quiescent_empty_boundary_does_not_capture_unrelated_activity(self):
        """A true no-text completion returns empty before unrelated queued work."""
        async with PrimeClient(stub_config(response_timeout=3.0)) as prime:
            assert await prime.ask("CONTINUE_UNRELATED") == ""

    async def test_unreadable_state_at_empty_boundary_is_explicit(self):
        async with PrimeClient(stub_config(response_timeout=3.0)) as prime:
            with pytest.raises(PrimeError, match="completion state"):
                await prime.ask("STATE_ERROR")


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


class TestEnvironmentScrubbing:
    """The agent must not be able to read the bridge's Zulip credential.

    It has tool and shell access, and `_relay` posts its answer straight back
    into Zulip -- for a stream mention, to everyone in the stream rather than
    only the allowlisted sender. So "print your environment" must not publish
    the bot's own key.
    """

    async def test_zulip_api_key_is_not_visible_to_the_agent(self, monkeypatch):
        monkeypatch.setenv("ZULIP_API_KEY", "SUPER-SECRET-KEY")

        probe = Path(__file__).parent / "stub_env_probe.py"
        config = PrimeConfig(command=sys.executable)
        config.argv = lambda: [sys.executable, str(probe)]  # type: ignore[method-assign]

        async with PrimeClient(config) as prime:
            seen = await prime.ask("what is in your environment?")

        assert seen == "<absent>"
        assert "SUPER-SECRET-KEY" not in seen

    async def test_unrelated_variables_still_reach_the_agent(self, monkeypatch):
        """Scrubbing is a deny list, not a sandbox — the agent still needs a PATH."""
        from prime_zulip.prime import SCRUBBED_ENV

        assert "PATH" not in SCRUBBED_ENV
        assert "ZULIP_API_KEY" in SCRUBBED_ENV


class TestLifecycleHygiene:
    async def test_stop_leaves_no_pending_pump_tasks(self):
        """After stop(), no pump task is still pending.

        Honest scope: this asserts the end state, not the mechanism. It does
        **not** discriminate the `await asyncio.gather(*pumps)` in `stop()` --
        removing that line leaves this green, because `stop()` also awaits
        `proc.wait()`, and that yield is enough for the pumps to finish on
        their own. The gather is kept as hygiene rather than because a test
        forces it; a review that assumes this test guards it would be wrong.
        """
        prime = PrimeClient(stub_config())
        await prime.start()
        pumps = [prime._pump, prime._stderr_pump]
        await prime.stop()

        assert all(task.done() for task in pumps if task is not None)

    async def test_stop_then_start_gets_a_clean_queue(self):
        prime = PrimeClient(stub_config())
        await prime.start()
        assert await prime.ask("first") == "echo: first"
        await prime.stop()
        await prime.start()
        try:
            assert await prime.ask("second") == "echo: second"
        finally:
            await prime.stop()

    def test_send_timeout_is_configurable_and_separate(self):
        from prime_zulip.prime import DEFAULT_SEND_TIMEOUT

        assert PrimeConfig().send_timeout == DEFAULT_SEND_TIMEOUT
        config = PrimeConfig.from_env({"PRIME_AGENT_SEND_TIMEOUT": "7"})
        assert config.send_timeout == 7.0
        # Bounding the write separately matters because the answer deadline
        # cannot rescue a blocked send: it happens under the same lock.
        assert config.send_timeout != config.response_timeout
