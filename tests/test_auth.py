"""Allowlist policy — the gate every inbound message passes through."""

from __future__ import annotations

import pytest

from prime_zulip.auth import Allowlist

ALICE_ID = 8
ALICE_EMAIL = "alice@example.com"


class TestFailClosed:
    """An unconfigured allowlist admits nobody. This is the property that matters."""

    def test_empty_allowlist_rejects_everyone(self):
        allowlist = Allowlist()
        assert not allowlist.is_configured
        assert not allowlist.allows(user_id=ALICE_ID, email=ALICE_EMAIL)

    def test_empty_allowlist_rejects_even_with_no_identifiers(self):
        assert not Allowlist().allows()


class TestEitherMode:
    """Default: either identifier is enough."""

    def test_user_id_alone_admits(self):
        allowlist = Allowlist(user_ids={ALICE_ID})
        assert allowlist.allows(user_id=ALICE_ID)
        assert allowlist.allows(user_id=ALICE_ID, email="stranger@example.com")

    def test_email_alone_admits(self):
        allowlist = Allowlist(emails={ALICE_EMAIL})
        assert allowlist.allows(email=ALICE_EMAIL)
        assert allowlist.allows(user_id=999, email=ALICE_EMAIL)

    def test_neither_matching_rejects(self):
        allowlist = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL})
        assert not allowlist.allows(user_id=999, email="stranger@example.com")

    def test_email_match_is_case_and_space_insensitive(self):
        allowlist = Allowlist(emails={ALICE_EMAIL})
        assert allowlist.allows(email="  ALICE@Example.COM  ")


class TestRequireBoth:
    """`requireBoth` turns the OR into an AND."""

    def test_both_matching_admits(self):
        allowlist = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL}, require_both=True)
        assert allowlist.allows(user_id=ALICE_ID, email=ALICE_EMAIL)

    def test_user_id_alone_is_not_enough(self):
        allowlist = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL}, require_both=True)
        assert not allowlist.allows(user_id=ALICE_ID)
        assert not allowlist.allows(user_id=ALICE_ID, email="stranger@example.com")

    def test_email_alone_is_not_enough(self):
        allowlist = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL}, require_both=True)
        assert not allowlist.allows(email=ALICE_EMAIL)
        assert not allowlist.allows(user_id=999, email=ALICE_EMAIL)

    def test_reassigned_email_cannot_inherit_access(self):
        """The reason requireBoth exists.

        A Zulip admin can move an allowlisted address onto a different
        account. Under OR that new account is admitted immediately; under AND
        it is not, because the stable user ID still does not match.
        """
        allowlist = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL})
        assert allowlist.allows(user_id=4242, email=ALICE_EMAIL)

        strict = Allowlist(user_ids={ALICE_ID}, emails={ALICE_EMAIL}, require_both=True)
        assert not strict.allows(user_id=4242, email=ALICE_EMAIL)

    @pytest.mark.parametrize(
        "user_ids,emails",
        [
            ({ALICE_ID}, set()),
            (set(), {ALICE_EMAIL}),
            (set(), set()),
        ],
    )
    def test_half_configured_is_not_configured(self, user_ids, emails):
        """Under AND, an empty list can never be satisfied — so say so.

        Reporting `is_configured` here would describe a policy that rejects
        every sender as though it were working.
        """
        allowlist = Allowlist(user_ids=user_ids, emails=emails, require_both=True)
        assert not allowlist.is_configured
        assert not allowlist.allows(user_id=ALICE_ID, email=ALICE_EMAIL)


class TestFromEnv:
    def test_parses_lists_and_flag(self, monkeypatch):
        monkeypatch.setenv("ZULIP_ALLOWED_USER_IDS", "8, 9 ,notanint")
        monkeypatch.setenv("ZULIP_ALLOWED_EMAILS", "Alice@Example.com , bob@example.com")
        monkeypatch.setenv("ZULIP_ALLOW_REQUIRE_BOTH", "true")

        allowlist = Allowlist.from_env()

        assert allowlist.user_ids == {8, 9}
        assert allowlist.emails == {"alice@example.com", "bob@example.com"}
        assert allowlist.require_both is True

    def test_require_both_defaults_off(self, monkeypatch):
        monkeypatch.delenv("ZULIP_ALLOW_REQUIRE_BOTH", raising=False)
        monkeypatch.setenv("ZULIP_ALLOWED_USER_IDS", "8")
        assert Allowlist.from_env().require_both is False

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "yes", "on", " On "])
    def test_truthy_spellings(self, monkeypatch, raw):
        monkeypatch.setenv("ZULIP_ALLOW_REQUIRE_BOTH", raw)
        assert Allowlist.from_env().require_both is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", "", "maybe"])
    def test_falsy_spellings(self, monkeypatch, raw):
        monkeypatch.setenv("ZULIP_ALLOW_REQUIRE_BOTH", raw)
        assert Allowlist.from_env().require_both is False
