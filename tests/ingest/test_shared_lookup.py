"""Tests for the shared lookups: claims, failures, retirement, and the copies waiters raise.

The races between a maker and its waiters, through real reads and the rate governor, are
tested where the lookups are made, in `tests/ingest/test_rest.py`.
"""

import math
from concurrent.futures import Future
from typing import Any

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._errors import from_wire
from deephaven_massive._ingest import shared_lookup
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup


def _lookup() -> SharedLookup[str, object]:
    # No test here waits, so the governor is never asked.
    return SharedLookup("MassiveSession #1", _massive.RequestGovernor(["other"]))


def test_a_claim_is_made_once_and_its_value_is_known_after() -> None:
    lookup = _lookup()
    claim = lookup.claim_or_wait(
        "k", bucket="other", patience=_massive.Patience(math.inf), what="k", public=from_wire
    )
    assert isinstance(claim, Claim)
    assert lookup.known("k") is None  # being made
    value = object()
    lookup.complete(claim, value)
    assert lookup.known("k") is value


def test_failing_an_old_claim_keeps_a_newer_one() -> None:
    lookup = _lookup()
    old, created = lookup._claim("k")
    assert created
    lookup.fail(Claim("k", old), dm.MassiveError("boom"), "the read failed")
    new, created = lookup._claim("k")
    assert created
    lookup.fail(Claim("k", Future()), dm.MassiveError("boom"), "the read failed")  # not current
    assert lookup._claim("k") == (new, False)


@pytest.mark.parametrize(
    ("error", "stopped", "abandoned"),
    [
        (dm.MassiveError("refused", status=403), False, False),
        (dm.MassiveError("refused", status=403), True, True),
        (dm.MassiveError("rate limited", status=429), False, True),
        (KeyboardInterrupt(), False, True),
        (RuntimeError("bug"), False, False),
    ],
    ids=["refusal", "stopped", "gave-up", "interrupted", "defect"],
)
def test_a_failure_tells_waiters_to_copy_it_or_try_again(
    *, error: BaseException, stopped: bool, abandoned: bool
) -> None:
    lookup = _lookup()
    future, _ = lookup._claim("k")
    lookup.fail(Claim("k", future), error, "the read failed", stopped=stopped)
    held = future.exception()
    assert isinstance(held, shared_lookup._AbandonedError) is abandoned
    if isinstance(error, RuntimeError):
        assert isinstance(held, dm.MassiveError)
        assert held.source is dm.ErrorSource.LIBRARY
        assert str(held).startswith("the read failed: RuntimeError('bug')")
    assert lookup._claim("k")[1]  # forgotten


def test_waiters_get_a_copy_of_the_makers_error() -> None:
    future: Future[Any] = Future()
    error = dm.EntitlementError("no", status=403, request_id="r1")
    future.set_exception(error)
    with pytest.raises(dm.EntitlementError, match=r"^no$") as info:
        shared_lookup._outcome(future)
    assert info.value is not error
    assert (info.value.status, info.value.request_id) == (403, "r1")


def test_a_waiters_copy_keeps_whether_the_call_may_succeed_later() -> None:
    future: Future[Any] = Future()
    future.set_exception(dm.MassiveError("connection reset", is_retryable=True))
    with pytest.raises(dm.MassiveError) as info:
        shared_lookup._outcome(future)
    assert info.value.is_retryable


def test_a_value_is_retired_only_when_old_and_current(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [1000.0]
    monkeypatch.setattr(shared_lookup, "_monotonic", lambda: clock[0])
    lookup = _lookup()
    future, _ = lookup._claim("k")
    value = object()
    lookup.complete(Claim("k", future), value)
    clock[0] += 59
    assert lookup.retire("k", value, 60) is False  # young: kept
    assert lookup.known("k") is value
    assert lookup.retire("k", object(), 60) is True  # another value: read the current one
    assert lookup.known("k") is value
    clock[0] += 1
    assert lookup.retire("k", value, 60) is True  # old: forgotten
    assert lookup.known("k") is None
    assert lookup._claim("k")[1]
