"""Tests for one symbol's run of reads: its retry schedule, final failure, and span cause."""

from __future__ import annotations

import pytest

from deephaven_massive._errors import EntitlementError, ErrorSource, MassiveError
from deephaven_massive._tables import symbol_retry
from deephaven_massive._tables.symbol_read_rows import SymbolReadState
from deephaven_massive._tables.symbol_retry import (
    RETRY_DELAYS,
    RETRY_EVERY,
    Failed,
    Retry,
    SymbolRun,
    next_step,
    outage_failure,
)

LIMIT = 300.0
"""The default `max_outage`, in seconds."""


def busy() -> MassiveError:
    return MassiveError("Massive answered GET /x with HTTP 503", status=503)


def refused() -> EntitlementError:
    return EntitlementError("Massive answered GET /x with HTTP 403", status=403)


# --- next_step ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("attempts", "delay"),
    [(0, 2.0), (1, 2.0), (2, 8.0), (3, 30.0), (4, 60.0), (9, 60.0)],
)
def test_a_failure_that_may_pass_is_read_again_soon_then_every_minute(
    attempts: int, delay: float
) -> None:
    # 2, 8, and 30 s after the first, second, and third failure, then every minute; a failure
    # recorded before any read began (no attempt yet) gets the first delay.
    assert (RETRY_DELAYS, RETRY_EVERY) == ((2.0, 8.0, 30.0), 60.0)
    step = next_step(
        busy(), name="AAPL", failing_since=100.0, attempts=attempts, limit=LIMIT, now=110.0
    )
    assert step == Retry(
        due=110.0 + delay, delay=delay, attempts=max(attempts, 1), failing_for=10.0
    )


def test_a_failure_no_retry_can_fix_is_final_at_once() -> None:
    error = refused()
    step = next_step(error, name="AAPL", failing_since=100.0, attempts=1, limit=LIMIT, now=100.0)
    assert step == Failed(error, attempts=1)
    assert step.error is error  # the error as it is: nothing names a limit it never reached


@pytest.mark.parametrize(
    ("now", "final"),
    [(399.9, False), (400.0, True), (440.0, True)],
    ids=["just under", "at the limit", "past the limit"],
)
def test_a_failure_that_may_pass_is_final_once_its_run_has_lasted_max_outage(
    now: float, *, final: bool
) -> None:
    # The limit is a threshold checked as each failure lands: the failure that lands at or
    # past it is final, however far past, and its error names the limit.
    error = busy()
    step = next_step(error, name="AAPL", failing_since=100.0, attempts=5, limit=LIMIT, now=now)
    if not final:
        assert step == Retry(now + RETRY_EVERY, RETRY_EVERY, attempts=5, failing_for=now - 100.0)
        return
    assert step.attempts == 5
    assert isinstance(step, Failed)
    assert str(step.error) == (
        f"AAPL has failed for {now - 100.0:.0f} s, as long as max_outage=300 s allows. The "
        f"latest error: {error}"
    )
    assert step.error.__cause__ is error


def test_a_max_outage_of_zero_makes_the_first_failure_final() -> None:
    step = next_step(busy(), name="AAPL", failing_since=5.0, attempts=1, limit=0.0, now=5.0)
    assert isinstance(step, Failed)
    assert str(step.error).startswith("AAPL has failed for 0 s, as long as max_outage=0 s allows.")


def test_an_outage_failure_keeps_the_class_and_every_attribute_of_the_latest_error() -> None:
    latest = MassiveError("boom", status=502, request_id="r1", source=ErrorSource.MASSIVE)
    final = outage_failure("MSFT", latest, failing_for=340.4, limit=300.0)
    assert type(final) is MassiveError
    assert (final.status, final.request_id, final.source) == (502, "r1", ErrorSource.MASSIVE)
    assert (final.is_retryable, final.is_bug) == (True, False)
    assert str(final) == (
        "MSFT has failed for 340 s, as long as max_outage=300 s allows. The latest error: boom"
    )
    assert final.__cause__ is latest


# --- SymbolRun ---------------------------------------------------------------------------------


def test_a_run_counts_its_reads_and_keeps_its_failures_until_one_succeeds() -> None:
    run = SymbolRun()
    assert (run.state, run.attempts, run.failing_since, run.due) == (
        SymbolReadState.QUEUED,
        0,
        None,
        None,
    )
    run.started()
    assert (run.state, run.attempts) == (SymbolReadState.LOADING, 1)
    first = run.failed(busy(), name="AAPL", now=10.0, limit=LIMIT)
    assert first == Retry(due=12.0, delay=2.0, attempts=1, failing_for=0.0)
    assert (run.state, run.failing_since, run.due) == (SymbolReadState.RETRYING, 10.0, 12.0)
    run.started()
    second = run.failed(busy(), name="AAPL", now=12.5, limit=LIMIT)
    assert second == Retry(20.5, 8.0, attempts=2, failing_for=2.5)  # the run began at the first
    assert (run.state, run.attempts, run.due) == (SymbolReadState.RETRYING, 2, 20.5)
    run.started()
    run.succeeded()
    assert (run.state, run.attempts, run.failing_since, run.due) == (
        SymbolReadState.DONE,
        3,
        None,
        None,
    )
    # The next failure starts a new run of failures, timed from itself.
    run.started()
    third = run.failed(busy(), name="AAPL", now=500.0, limit=LIMIT)
    assert third == Retry(560.0, 60.0, attempts=4, failing_for=0.0)  # the fourth attempt's delay
    assert run.failing_since == 500.0


def test_a_run_that_fails_for_good_is_failed_and_never_due() -> None:
    run = SymbolRun(SymbolReadState.LOADING, attempts=1)
    error = refused()
    step = run.failed(error, name="AAPL", now=10.0, limit=LIMIT)
    assert step == Failed(error, attempts=1)
    assert (run.state, run.failing_since, run.due) == (SymbolReadState.FAILED, 10.0, None)


def test_a_step_no_case_handles_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(symbol_retry, "next_step", lambda *_, **__: "bogus")
    with pytest.raises(AssertionError, match="bogus"):
        SymbolRun().failed(busy(), name="AAPL", now=0.0, limit=LIMIT)
