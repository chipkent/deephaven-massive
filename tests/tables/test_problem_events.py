"""Tests for the problem events of the rows the library withholds, and their logging sink."""

import dataclasses
import logging

import pytest

from deephaven_massive._massive import RowFault, Transport
from deephaven_massive._tables import problem_events
from deephaven_massive._tables.problem_events import LoggedProblems, ProblemAction, ProblemEvent

EVENT = ProblemEvent(
    cause=RowFault.UNCONVERTIBLE_VALUES,
    action=ProblemAction.WITHHELD,
    dataset="stocks.trades",
    transport=Transport.REST,
    stream_id=None,
    where="GET /v3/trades/{stockTicker}",
    symbol="AAPL",
    field="Price",
    rows=2,
    reason="Price holds 'x', not a double",
    at=1,
)


def test_an_event_is_frozen_and_slotted() -> None:
    found = EVENT
    assert hash(found) == hash(dataclasses.replace(found))  # frozen, so hashable by value
    assert not hasattr(found, "__dict__")


def test_logged_problems_logs_one_debug_line_per_event(caplog: pytest.LogCaptureFixture) -> None:
    overflow = dataclasses.replace(
        EVENT,
        action=ProblemAction.OVERFLOW,
        transport=Transport.WS,
        stream_id=7,
        symbol=None,
        field=None,
        rows=9,
        where="wss://h/stocks channel T",
    )
    with caplog.at_level(logging.DEBUG, logger=problem_events.__name__):
        LoggedProblems()([EVENT, overflow])
        LoggedProblems()([])
    assert [r.levelno for r in caplog.records] == [logging.DEBUG] * 2
    assert [r.getMessage() for r in caplog.records] == [
        (
            "stocks.trades REST GET /v3/trades/{stockTicker}: withheld 2 rows of AAPL, "
            "unconvertible_values (stream None): Price holds 'x', not a double"
        ),
        (
            "stocks.trades WS wss://h/stocks channel T: overflow 9 rows of None, "
            "unconvertible_values (stream 7): Price holds 'x', not a double"
        ),
    ]
