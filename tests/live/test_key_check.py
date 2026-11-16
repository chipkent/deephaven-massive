"""Tests of the live tests' key check and their plans fixture (`tests/_live/key_check.py`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._live import key_check, plans

if TYPE_CHECKING:
    from collections.abc import Iterator

_PLANS = plans.declared_plans("stocks=starter")


@pytest.fixture
def asked(pytestconfig: pytest.Config) -> Iterator[list[str]]:
    """Put a stand-in source of plans in the run's stash for the test, recording each key asked.

    Yields:
        The keys the source was asked for, in order.
    """
    keys: list[str] = []

    def source(config: pytest.Config, api_key: str) -> plans.LivePlans:
        assert config is pytestconfig
        keys.append(api_key)
        return _PLANS

    saved = pytestconfig.stash[key_check.PLANS_SOURCE]
    pytestconfig.stash[key_check.PLANS_SOURCE] = source
    try:
        yield keys
    finally:
        pytestconfig.stash[key_check.PLANS_SOURCE] = saved


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


def test_the_plans_are_never_asked_for_under_ci(
    asked: list[str], pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("MASSIVE_API_KEY", "not-a-real-key")
    with pytest.raises(pytest.skip.Exception, match="live tests never run in CI"):
        key_check.run_plans(pytestconfig)
    assert asked == []


def test_the_plans_are_never_asked_for_without_a_key(
    asked: list[str], pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    with pytest.raises(pytest.skip.Exception, match="MASSIVE_API_KEY is not set"):
        key_check.run_plans(pytestconfig)
    assert asked == []


def test_the_source_gives_the_plans_of_the_key(
    asked: list[str], pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY", "not-a-real-key")
    assert key_check.run_plans(pytestconfig) is _PLANS
    assert asked == ["not-a-real-key"]
