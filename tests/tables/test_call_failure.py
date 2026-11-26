"""Tests for whether one symbol's failure fails its whole call or table, and with which error."""

from __future__ import annotations

import dataclasses

import pytest

import deephaven_massive as dm
from deephaven_massive._error_policy import DEFAULT_POLICY
from deephaven_massive._errors import (
    FeedRefusalError,
    RowDataError,
    UnavailableSymbolError,
    WithheldSymbolError,
)
from deephaven_massive._tables.call_failure import call_failure, fails_call

_REFUSAL = (
    "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 403: "
    '"You are not entitled to this data".'
)
"""Massive's refusal of AAPL, as the REST client words it."""


@pytest.mark.parametrize(
    ("setting", "fails"),
    [(dm.OnError.RAISE, True), (dm.OnError.LOG, False), (dm.OnError.IGNORE, False)],
)
def test_a_refused_symbol_fails_the_call_only_under_raise(
    setting: dm.OnError,
    fails: bool,
) -> None:
    policy = dataclasses.replace(DEFAULT_POLICY, refused_symbols=setting)
    assert fails_call(dm.EntitlementError(_REFUSAL, status=403), policy) is fails


def test_a_row_error_made_only_under_raise_always_fails_the_call() -> None:
    # A withheld or unrequested row's RowDataError is made only when its cause is RAISE.
    assert fails_call(RowDataError("withheld"), DEFAULT_POLICY)


@pytest.mark.parametrize(
    ("setting", "fails"),
    [(dm.OnError.RAISE, True), (dm.OnError.LOG, False), (dm.OnError.IGNORE, False)],
)
def test_a_symbol_massive_has_no_data_for_fails_the_call_only_under_raise(
    setting: dm.OnError, fails: bool
) -> None:
    policy = dataclasses.replace(DEFAULT_POLICY, unavailable_symbols=setting)
    assert fails_call(UnavailableSymbolError("no data"), policy) is fails


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_symbol_whose_rows_were_all_withheld_never_fails_the_call(setting: dm.OnError) -> None:
    # Its rows were withheld under LOG or IGNORE: a RAISE cause raised at the row instead.
    error = WithheldSymbolError("withheld")
    policy = dataclasses.replace(DEFAULT_POLICY, unconvertible_values=setting)
    assert not fails_call(error, policy)
    assert call_failure("AAPL", error, policy) is None


def test_a_failure_of_its_own_and_a_feed_refusal_never_fail_the_call_here() -> None:
    # A feed refusal fails the whole read before it reaches a symbol's outcome.
    strict = dataclasses.replace(DEFAULT_POLICY, refused_symbols=dm.OnError.RAISE)
    for error in (
        dm.MassiveError("HTTP 503: busy", status=503),
        FeedRefusalError(_REFUSAL, status=403),
    ):
        assert not fails_call(error, strict)
        assert call_failure("AAPL", error, strict) is None


def test_the_call_fails_with_a_refusal_that_names_the_setting_that_reads_the_others() -> None:
    strict = dataclasses.replace(DEFAULT_POLICY, refused_symbols=dm.OnError.RAISE)
    refusal = dm.EntitlementError(_REFUSAL, status=403)
    raised = call_failure("AAPL", refusal, strict)
    assert isinstance(raised, dm.EntitlementError)
    assert str(raised) == (
        f"{_REFUSAL} Remove AAPL, or, to read the others, pass "
        "on_error=dm.ErrorPolicy(refused_symbols=dm.OnError.LOG) to the call or to "
        "dm.MassiveConfig."
    )
    assert raised.status == 403
    assert call_failure("AAPL", refusal, DEFAULT_POLICY) is None
    unavailable = UnavailableSymbolError("no data")
    strict = dataclasses.replace(DEFAULT_POLICY, unavailable_symbols=dm.OnError.RAISE)
    assert call_failure("AAPL", unavailable, strict) is unavailable
