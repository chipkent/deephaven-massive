"""Tests for `queries/_call.py`: a query's repr, the call that built it."""

import inspect
import logging
from datetime import UTC, date, datetime

import numpy as np
import pytest
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import to_j_instant, to_j_local_date, to_j_time_zone

import deephaven_massive as dm
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import FuturesVenue, Timespan
from deephaven_massive.queries._call import QueryCall, _is_default, _shown
from tests._fakes.queries import (
    NEWS_SPEC,
    make,
)


def test_query_repr_is_the_call_that_built_it(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    # Symbols come first and positionally, sorted, whatever the caller's order.
    assert repr(m.stocks.trades(["MSFT", "AAPL"])) == "m.stocks.trades(['AAPL', 'MSFT'])"
    assert repr(m.stocks.trades("AAPL")) == "m.stocks.trades('AAPL')"
    assert repr(m.stocks.trades(dm.ALL)) == "m.stocks.trades(dm.ALL)"
    watchlist = new_table([string_col("Symbol", ["A"])])
    assert repr(m.stocks.trades(watchlist)) == "m.stocks.trades(<symbol table>)"
    many = ["A", "B", "C", "D", "E", "F", "G"]
    assert repr(m.stocks.trades(many)) == ("m.stocks.trades(['A', 'B', 'C', 'D', 'E', … (2 more)])")
    # A required argument goes by position; only arguments that differ from their defaults
    # are shown, by their public names.
    assert repr(m.forex.bars("C:EURUSD", Timespan.MINUTE, multiplier=1)) == (
        "m.forex.bars('C:EURUSD', dm.Timespan.MINUTE)"
    )
    assert repr(m.forex.bars("C:EURUSD", timespan=Timespan.HOUR, multiplier=5)) == (
        "m.forex.bars('C:EURUSD', dm.Timespan.HOUR, multiplier=5)"
    )
    # Time bounds are written as the request holds them, parsed.
    assert repr(m.economy.inflation(start=date(2025, 1, 2), limit=10)) == (
        "m.economy.inflation(start='2025-01-02', limit=10)"
    )
    # Filters as the request parsed them; an empty mapping selects nothing and is left out.
    filtered = m.stocks.dividends(filters={"distribution_type": "special"})
    assert repr(filtered) == "m.stocks.dividends(filters={'distribution_type': 'special'})"
    assert repr(m.stocks.dividends(filters={})) == "m.stocks.dividends()"
    # A query built without its dataset method names only the dataset.
    assert repr(make(m, NEWS_SPEC)) == "m.economy.inflation()"


def test_query_repr_writes_values_as_a_user_types_them() -> None:
    assert _shown(Timespan.DAY) == "dm.Timespan.DAY"
    assert _shown(datetime(2025, 1, 2, 3, 4, tzinfo=UTC)) == "'2025-01-02T03:04:00+00:00'"
    # A set prints in one order every run: enum members in their enum's order, others by text.
    assert _shown(frozenset({Timespan.DAY, Timespan.MINUTE})) == (
        "{dm.Timespan.MINUTE, dm.Timespan.DAY}"
    )
    assert _shown(frozenset({"b", "a"})) == "{'a', 'b'}"
    assert _shown(("x", "y")) == "['x', 'y']"
    assert _shown(1.5) == "1.5"


def test_query_repr_writes_java_and_numpy_times_as_text(
    massive_session: dm.MassiveSession,
) -> None:
    # A day argument the request does not hold parsed is written as the caller's value.
    day = to_j_local_date("2025-01-02")
    assert day is not None
    assert repr(massive_session.stocks.daily_open_close("AAPL", day)) == (
        "m.stocks.daily_open_close('AAPL', '2025-01-02')"
    )
    assert _shown(np.datetime64("2025-01-02")) == "'2025-01-02'"
    assert _shown(to_j_instant("2025-01-02T03:04:00Z")) == "'2025-01-02T03:04:00+00:00'"
    # A Java object of another class, and a time no parser reads, keep their repr.
    other = to_j_time_zone("UTC")
    assert _shown(other) == repr(other)
    assert _shown(np.datetime64("NaT", "D")) == repr(np.datetime64("NaT", "D"))


def test_query_repr_falls_back_to_the_symbols_passed() -> None:
    # A request that does not store its symbols shows them as the caller passed them.
    assert QueryCall("m.x.y", (("symbols", "AAPL"),)).text(Request()) == "m.x.y('AAPL')"
    assert QueryCall("m.x.y", (("symbols", ("B", "A")),)).text(Request()) == "m.x.y(['B', 'A'])"


def test_an_array_argument_is_never_its_default() -> None:
    # A numpy array's `==` answers element by element; the repr must not ask its truth.
    assert not _is_default(np.array([1, 1]), 1)
    assert _is_default(1, 1)
    assert _is_default({FuturesVenue.CME}, frozenset({FuturesVenue.CME}))
    assert not _is_default(None, inspect.Parameter.empty)
    assert not _is_default(2, None)


def test_a_namespace_method_logs_the_input_it_refuses(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"),
        pytest.raises(TypeError),
    ):
        massive_session.stocks.trades(42)  # type: ignore[arg-type]  # a refused symbol type
    refusals = [r for r in caplog.records if "m.stocks.trades refused" in r.getMessage()]
    assert [r.levelno for r in refusals] == [logging.DEBUG]
    assert refusals[0].getMessage().startswith("m.stocks.trades refused: TypeError: ")
