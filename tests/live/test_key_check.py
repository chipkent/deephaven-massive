"""Tests of the live tests' key check (`tests/_live/key_check.py`)."""

from __future__ import annotations

import pytest

from tests._live import key_check


def test_a_key_under_ci_is_never_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("MASSIVE_API_KEY", "not-a-real-key")
    with pytest.raises(pytest.skip.Exception, match="live tests never run in CI"):
        key_check.live_key()


def test_a_blank_ci_lets_the_key_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CI", " ")
    monkeypatch.setenv("MASSIVE_API_KEY", " not-a-real-key\n")
    assert key_check.live_key() == "not-a-real-key"


@pytest.mark.parametrize("value", [None, "", "  "])
def test_an_unset_or_blank_variable_skips(
    value: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    if value is None:
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    else:
        monkeypatch.setenv("MASSIVE_API_KEY", value)
    with pytest.raises(pytest.skip.Exception, match="MASSIVE_API_KEY is not set"):
        key_check.required_env("MASSIVE_API_KEY")
