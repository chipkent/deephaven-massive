"""Tests for the session's problem sink: the log's alarm, then the queue to `m.problems()`."""

import logging
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Redactor
from deephaven_massive._tables import problem_reporter
from deephaven_massive._tables.problem_events import (
    Problem,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    ProblemSpans,
    Span,
)
from deephaven_massive._tables.problem_reporter import ProblemReporter

if TYPE_CHECKING:
    from collections.abc import Sequence


KEY = "planted-key"
"""The session's API key, which no log line may show."""
REDACTOR = Redactor(KEY)


def event(
    *,
    cause: ProblemCause = ProblemCause.UNCONVERTIBLE_VALUES,
    stream_id: int | None = 7,
    policy: dm.OnError | None = dm.OnError.LOG,
    symbol: str | None = "AAPL",
    rows: int = 2,
) -> ProblemEvent:
    return ProblemEvent(
        cause=cause,
        action=ProblemAction.WITHHELD,
        dataset="stocks.trades",
        stream_id=stream_id,
        where="GET /v3/trades",
        symbol=symbol,
        field="Price",
        rows=rows,
        detail="Price holds 'x'",
        at=1,
        policy=policy,
    )


def spans(cause: ProblemCause, held: dict[str, Span]) -> ProblemSpans:
    return ProblemSpans(
        cause=cause, dataset="stocks.trades", stream_id=7, where="here", spans=held, at=1
    )


def warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == problem_reporter.__name__ and r.levelno == logging.WARNING
    ]


def test_the_first_problem_of_each_table_and_cause_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    queued: list[Problem] = []
    reporter = ProblemReporter(queued.extend, REDACTOR)
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        reporter([event(), event(symbol="MSFT")])
        reporter([event()])
        reporter([event(stream_id=8, symbol=None, rows=0)])
        reporter([event(cause=ProblemCause.UNREADABLE_KEYS)])
    assert warnings(caplog) == [
        (
            "stocks.trades table 7: unconvertible_values: 2 rows of AAPL, Price holds 'x' (at GET "
            "/v3/trades); m.problems() lists it and every later unconvertible_values problem of "
            "this table, which the log shows at DEBUG only"
        ),
        (
            "stocks.trades table 8: unconvertible_values: Price holds 'x' (at GET /v3/trades); "
            "m.problems() lists it and every later unconvertible_values problem of this table, "
            "which the log shows at DEBUG only"
        ),
        (
            "stocks.trades table 7: unreadable_keys: 2 rows of AAPL, Price holds 'x' (at GET "
            "/v3/trades); m.problems() lists it and every later unreadable_keys problem of this "
            "table, which the log shows at DEBUG only"
        ),
    ]
    debug = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(debug) == 5  # every problem's details
    assert len(queued) == 5


@pytest.mark.parametrize("policy", [dm.OnError.IGNORE, dm.OnError.RAISE])
def test_an_ignored_or_failing_cause_logs_at_debug_only(
    policy: dm.OnError, caplog: pytest.LogCaptureFixture
) -> None:
    queued: list[Problem] = []
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        ProblemReporter(queued.extend, REDACTOR)([event(policy=policy)])
    assert warnings(caplog) == []
    assert len(queued) == 1


def test_a_cause_its_source_warns_of_logs_at_debug_only(caplog: pytest.LogCaptureFixture) -> None:
    queued: list[Problem] = []
    outage = spans(ProblemCause.OUTAGE, {"outage": Span(detail="down", since=1)})
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        ProblemReporter(queued.extend, REDACTOR)([outage, event(cause=ProblemCause.STREAM_FAILED)])
    assert warnings(caplog) == []
    assert len(queued) == 2


def test_a_span_warns_by_its_first_span_and_an_empty_snapshot_only_queues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    queued: list[Sequence[Problem]] = []
    reporter = ProblemReporter(queued.append, REDACTOR)
    stale = spans(ProblemCause.STALE_SYMBOL, {"ESZ6": Span(symbol="ESZ6", detail="why", since=1)})
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        reporter([spans(ProblemCause.STALE_SYMBOL, {})])
        reporter([stale])
    assert warnings(caplog) == [
        (
            "stocks.trades table 7: stale_symbol: ESZ6: why (at here); m.problems() lists it "
            "and every later stale_symbol problem of this table, which the log shows at DEBUG "
            "only"
        )
    ]
    assert len(queued) == 2


def test_a_session_problem_names_no_table(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, problem_reporter.__name__):
        queued: list[Problem] = []
        ProblemReporter(queued.extend, REDACTOR)([event(stream_id=None)])
    assert warnings(caplog)[0].startswith("stocks.trades: unconvertible_values:")


def test_no_log_line_shows_a_secret(caplog: pytest.LogCaptureFixture) -> None:
    queued: list[Problem] = []
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        ProblemReporter(queued.extend, REDACTOR)([event(symbol=f"x{KEY}")])
    assert caplog.records
    assert all(KEY not in r.getMessage() for r in caplog.records)
    assert any("x***" in r.getMessage() for r in caplog.records)


def test_a_span_s_own_setting_decides_its_alarm(caplog: pytest.LogCaptureFixture) -> None:
    # A value canonicalize_symbols refused under IGNORE logs at DEBUG, whatever the table's
    # own setting.
    refused = spans(
        ProblemCause.NONCANONICAL_SYMBOLS,
        {"refused x": Span(symbol="x", detail="no", since=1, policy=dm.OnError.IGNORE)},
    )
    queued: list[Problem] = []
    with caplog.at_level(logging.DEBUG, problem_reporter.__name__):
        ProblemReporter(queued.extend, REDACTOR)([refused])
    assert warnings(caplog) == []
