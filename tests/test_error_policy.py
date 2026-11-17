"""Tests for `_error_policy.py`: the error policy, its defaults, and how a call's resolves."""

import dataclasses
from dataclasses import fields

import pytest

import deephaven_massive as dm
from deephaven_massive._error_policy import (
    DEFAULT_POLICY,
    ErrorPolicy,
    ResolvedPolicy,
    check_policy,
    policy_hint,
    resolve_policy,
)

CAUSES = [f.name for f in dataclasses.fields(ErrorPolicy)]


def _settings(policy: ErrorPolicy | ResolvedPolicy) -> dict[str, object]:
    return dataclasses.asdict(policy)


def test_the_policy_is_exported_from_the_package() -> None:
    assert dm.ErrorPolicy is ErrorPolicy


def test_the_resolved_policy_has_every_cause_of_the_policy() -> None:
    # A cause added to one class and not the other would never resolve.
    assert [f.name for f in dataclasses.fields(ResolvedPolicy)] == CAUSES


def test_a_new_policy_sets_nothing() -> None:
    assert set(_settings(ErrorPolicy()).values()) == {None}


def test_the_defaults_raise_on_a_refused_venue_and_log_everything_else() -> None:
    defaults = _settings(ErrorPolicy.DEFAULTS)
    assert defaults.pop("refused_venues") is dm.OnError.RAISE
    assert set(defaults.values()) == {dm.OnError.LOG}


def test_the_defaults_are_no_field_of_the_policy() -> None:
    assert "DEFAULTS" not in CAUSES


@pytest.mark.parametrize("cause", CAUSES)
def test_a_named_cause_is_set_and_the_others_are_not(cause: str) -> None:
    policy = ErrorPolicy(**{cause: dm.OnError.RAISE})
    settings = _settings(policy)
    assert settings.pop(cause) is dm.OnError.RAISE
    assert set(settings.values()) == {None}


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_the_first_argument_sets_every_cause_left_unnamed(setting: dm.OnError) -> None:
    policy = ErrorPolicy(setting, malformed_rows=dm.OnError.IGNORE)
    settings = _settings(policy)
    assert settings.pop("malformed_rows") is dm.OnError.IGNORE
    assert set(settings.values()) == {setting}


def test_the_first_argument_is_positional_only() -> None:
    with pytest.raises(TypeError, match="every"):
        ErrorPolicy(every=dm.OnError.RAISE)  # type: ignore[call-arg]  # the point: refused


@pytest.mark.parametrize("cause", CAUSES)
def test_a_cause_must_be_the_enum(cause: str) -> None:
    # A plain string equal to a member's value is refused too: `OnError` is a `StrEnum`.
    with pytest.raises(TypeError, match=f"{cause} must be dm.OnError, e.g. dm.OnError.RAISE"):
        ErrorPolicy(**{cause: "raise"})  # type: ignore[arg-type]  # the point: refused at run time


def test_the_setting_for_every_cause_must_be_the_enum() -> None:
    with pytest.raises(TypeError, match=r"the setting for every cause must be dm\.OnError"):
        ErrorPolicy("log")  # type: ignore[arg-type]  # the point: refused at run time


def test_a_policy_is_frozen_and_hashable() -> None:
    policy = ErrorPolicy(malformed_rows=dm.OnError.RAISE)
    with pytest.raises(dataclasses.FrozenInstanceError):
        policy.malformed_rows = dm.OnError.LOG  # type: ignore[misc]  # the point: frozen
    assert policy == ErrorPolicy(malformed_rows=dm.OnError.RAISE)
    assert hash(policy) == hash(ErrorPolicy(malformed_rows=dm.OnError.RAISE))
    assert policy != ErrorPolicy()


def test_the_repr_shows_only_the_causes_set() -> None:
    assert repr(ErrorPolicy()) == "dm.ErrorPolicy()"
    shown = repr(ErrorPolicy(malformed_rows=dm.OnError.RAISE, refused_venues=dm.OnError.LOG))
    assert shown == (
        "dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE, refused_venues=dm.OnError.LOG)"
    )


_LAYERS = [
    (None, None, None),  # the default
    (None, dm.OnError.IGNORE, dm.OnError.IGNORE),  # the session's
    (dm.OnError.IGNORE, dm.OnError.RAISE, dm.OnError.IGNORE),  # the call's
    (dm.OnError.LOG, None, dm.OnError.LOG),  # the call's over the default
]
"""Each case: the call's setting, the session's, and the expected one (None for the default)."""


@pytest.mark.parametrize("cause", CAUSES)
@pytest.mark.parametrize("layers", _LAYERS)
def test_each_cause_resolves_from_the_call_then_the_session_then_the_defaults(
    cause: str, layers: tuple[dm.OnError | None, dm.OnError | None, dm.OnError | None]
) -> None:
    call, session, expected = layers
    resolved = resolve_policy(ErrorPolicy(**{cause: call}), ErrorPolicy(**{cause: session}))
    default = _settings(ErrorPolicy.DEFAULTS)[cause]
    assert _settings(resolved)[cause] is (default if expected is None else expected)


def test_a_call_without_a_policy_takes_the_session_policy() -> None:
    session = ErrorPolicy(dm.OnError.IGNORE)
    assert set(_settings(resolve_policy(None, session)).values()) == {dm.OnError.IGNORE}


def test_the_default_policy_is_the_defaults_resolved() -> None:
    assert _settings(DEFAULT_POLICY) == _settings(ErrorPolicy.DEFAULTS)


def test_check_policy_returns_a_policy() -> None:
    policy = ErrorPolicy()
    assert check_policy(policy) is policy


def test_check_policy_shows_how_to_set_every_cause_to_a_bare_member() -> None:
    with pytest.raises(TypeError, match=r"pass dm.ErrorPolicy\(dm.OnError.RAISE\)"):
        check_policy(dm.OnError.RAISE)


@pytest.mark.parametrize("value", [None, "raise", {"malformed_rows": "raise"}])
def test_check_policy_refuses_anything_else(value: object) -> None:
    with pytest.raises(TypeError, match=r"on_error must be dm\.ErrorPolicy, such as .*, not "):
        check_policy(value)


def test_the_hint_is_a_sentence_naming_the_policy_to_pass() -> None:
    assert policy_hint(dm.OnError.LOG, "malformed_rows") == (
        "Pass on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.LOG) to the call or to "
        "dm.MassiveConfig."
    )


def test_the_hint_names_every_cause_it_is_given_in_order() -> None:
    assert policy_hint(dm.OnError.LOG, "unreadable_keys", "unconvertible_values") == (
        "Pass on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG, "
        "unconvertible_values=dm.OnError.LOG) to the call or to dm.MassiveConfig."
    )


def test_the_hint_starts_with_its_purpose_when_given_one() -> None:
    hint = policy_hint(
        dm.OnError.LOG, "malformed_rows", purpose="To skip such lines and keep the rest of the file"
    )
    assert hint == (
        "To skip such lines and keep the rest of the file, pass "
        "on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.LOG) to the call or to dm.MassiveConfig."
    )


@pytest.mark.parametrize("name", [field.name for field in fields(ErrorPolicy)])
@pytest.mark.parametrize("setting", list(dm.OnError))
def test_the_policy_the_hint_names_sets_the_cause(name: str, setting: dm.OnError) -> None:
    # The advice works for every cause: the keyword the hint names is one the policy takes.
    assert f"dm.ErrorPolicy({name}=dm.OnError.{setting.name})" in policy_hint(setting, name)
    assert getattr(dm.ErrorPolicy(**{name: setting}), name) is setting
