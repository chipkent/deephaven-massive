"""Tests for `queries/_stream.py`: the stream verbs and `last_by()`."""

import dataclasses
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeAlias, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import (
    AssetClass,
    NotBars,
    Timespan,
)
from deephaven_massive.queries import (
    StreamHistoryQuery,
    SymbolStreamQuery,
)
from deephaven_massive.queries._stream import _check_streams, streaming_bars
from tests._fakes.queries import (
    BARS_SPEC,
    TRADES_SPEC,
    FakeStreams,
    W,
    make,
    strict,
    trades,
)

RowsWhen: TypeAlias = Callable[
    [Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]
]
"""The `rows_when` fixture: waits until a live table's rows satisfy a predicate."""


@pytest.mark.parametrize("capacity", [0, -1, 2**31])
def test_ring_bounds(massive_session: dm.MassiveSession, capacity: int) -> None:
    with pytest.raises(ValueError, match="capacity must be between 1 and 2147483647"):
        trades(massive_session).ring(capacity)


@pytest.mark.parametrize("capacity", [1.5, True, "10"])
def test_ring_types(massive_session: dm.MassiveSession, capacity: object) -> None:
    with pytest.raises(TypeError, match="capacity must be an int"):
        trades(massive_session).ring(cast("int", capacity))


def test_last_by_takes_its_keys_positionally(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    # As Deephaven's Table.last_by(by): q.last_by("Exchange") and q.last_by(by="Exchange").
    q = trades(massive_session)
    positional = q.last_by("Exchange")
    keyword = q.last_by(by=["Exchange"])
    assert positional.is_refreshing
    assert keyword.is_refreshing
    assert len(fake_streams.opened) == 1
    massive_session.release(positional)
    massive_session.release(keyword)


def test_duplicate_last_by_keys_open_no_stream(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    with pytest.raises(ValueError, match="more than once"):
        trades(massive_session).last_by(["Exchange", "Exchange"])
    assert fake_streams.opened == []  # rejected synchronously, before any stream opens


@pytest.mark.parametrize(
    ("by", "expected"),
    [
        ((), (ValueError, "at least one column")),
        ("Nope", (ValueError, r"has no columns \['Nope'\]")),
        (["Symbol", "Nope"], (ValueError, "Nope")),
        ([1], (TypeError, "only column names")),
        ({"Symbol"}, (TypeError, "by must be a str or a sequence of str")),
        # None is no mode: the default is "Symbol", written out.
        (None, (TypeError, "by must be a str or a sequence of str, not NoneType")),
        (["Symbol", "Exchange", "Symbol"], (ValueError, r"by names \['Symbol'\] more than once")),
    ],
)
def test_last_by_keys(
    massive_session: dm.MassiveSession, by: object, expected: tuple[type[Exception], str]
) -> None:
    error, message = expected
    with pytest.raises(error, match=message):
        trades(massive_session).last_by(by=cast("str", by))


def test_stream_verbs_build_results(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    q = trades(massive_session)
    results = [
        q.blink(),
        q.append_only(),
        q.ring(5),
        q.last_by(),
        q.last_by(by="Symbol"),
        q.last_by(by=("Symbol", "Exchange")),
        q.last_by(by=deque(["Symbol", "Exchange"])),  # any sequence, as the message says
    ]
    assert all(r.is_refreshing for r in results)
    schema = massive_session._core.catalog.dataset_schema(TRADES_SPEC)
    assert [c.name for c in results[0].columns] == list(schema.column_names)
    assert len(fake_streams.opened) == 1
    for result in results:
        massive_session.release(result)


def test_last_by_follows_a_reactive_table(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    current = trades(massive_session, watchlist).last_by()
    assert current.is_refreshing
    massive_session.release(current)


def test_a_static_symbol_table_is_a_fixed_symbol_list(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # Read once, like fetch() reads it: every verb opens the stream an equal symbol list opens.
    fixed = trades(massive_session, new_table([string_col("Symbol", ["MSFT", "AAPL", "AAPL"])]))
    results = [
        fixed.blink(),
        fixed.append_only(),
        fixed.ring(3),
        fixed.last_by(),
        fixed.last_by("Exchange"),
        trades(massive_session, ["AAPL", "MSFT"]).blink(),
    ]
    assert all(r.is_refreshing for r in results)
    assert len(fake_streams.opened) == 1
    assert massive_session._core.diagnostics.wait_until_shown(10.0)
    [row] = dh_to_pylist(massive_session.streams())
    assert (row["SymbolCount"], row["Symbols"]) == (2, "AAPL, MSFT")
    for result in results:
        massive_session.release(result)


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_without_symbols_opens_no_stream(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, verb: str
) -> None:
    for content in ([], [None]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        call = getattr(q, verb)
        with pytest.raises(
            ValueError, match=r"^m\.stocks\.trades\(\): the symbol table holds no symbols$"
        ):
            call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_row_no_request_can_carry_opens_no_stream_under_raise(
    massive_session: dm.MassiveSession,
    *,
    fake_streams: FakeStreams,
    verb: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Refused, as the same text in a list always is: never sent and never dropped.
    strict(massive_session, monkeypatch)
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "MSFT,NVDA"])]))
    call = getattr(q, verb)
    with pytest.raises(
        ValueError,
        match=r"^stocks\.trades: a symbol table holds 'MSFT,NVDA', which no request",
    ):
        call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_row_no_request_can_carry_is_skipped(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, verb: str
) -> None:
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "MSFT,NVDA"])]))
    call = getattr(q, verb)
    massive_session.release(call(3) if verb == "ring" else call())
    assert len(fake_streams.opened) == 1
    only_bad = trades(massive_session, new_table([string_col("Symbol", ["MSFT,NVDA"])]))
    call = getattr(only_bad, verb)
    with pytest.raises(ValueError, match=r"^m\.stocks\.trades\(\): no row of the symbol table can"):
        call(3) if verb == "ring" else call()
    assert len(fake_streams.opened) == 1


def test_bar_channels_follow_the_timespan(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    second = make(massive_session, BARS_SPEC, symbols=["C:EURUSD"], timespan=Timespan.SECOND)
    assert isinstance(second, StreamHistoryQuery)
    massive_session.release(second.blink())
    assert fake_streams.opened == [("forex.bars", "CAS")]


FOREX_SPEC = DatasetSpec(
    name="forex.quotes",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)


def test_last_by_matches_the_rows_of_a_reactive_table(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    rows_when: RowsWhen,
    *,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["C:EURUSD", "C:GBPUSD"])]))
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, SymbolStreamQuery)
    current = q.last_by()
    schema = massive_session._core.catalog.dataset_schema(FOREX_SPEC)
    # Received now: a row received before its symbol joined the watchlist is hidden.
    received = datetime.now(UTC)
    row = schema.to_arrow([{"Symbol": "C:EURUSD", "AskPrice": 1.08, "TimestampInsert": received}])
    fake_streams.publishers[1].add(dh_arrow.to_table(row))
    shown = rows_when(current, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]
    # Removing the symbol from the watchlist evicts its row.
    watchlist.delete(new_table([string_col("Symbol", ["C:EURUSD"])]))
    rows_when(current, lambda rows: not rows)
    massive_session.release(current)


def test_a_live_verb_refuses_a_symbol_table_row_no_request_can_carry_under_raise(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, monkeypatch: pytest.MonkeyPatch
) -> None:
    strict(massive_session, monkeypatch)
    watchlist = new_table([string_col("Symbol", ["C:EURUSD", "EUR/USD"])])
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, SymbolStreamQuery)
    for verb in (q.blink, q.append_only, q.last_by):
        with pytest.raises(
            ValueError, match=r"^forex\.\w+: a symbol table holds 'EUR/USD', which no request"
        ):
            verb()
    assert fake_streams.publishers == {}


def test_last_by_over_groups_of_symbols_filters_before_aggregating(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    rows_when: RowsWhen,
    *,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
    current = trades(massive_session, watchlist).last_by(by="Exchange")
    watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
    rows_when(watchlist, lambda rows: len(rows) == 1)
    # A late MSFT trade on exchange 4 must not hide AAPL's latest trade there.
    rows = massive_session._core.catalog.dataset_schema(TRADES_SPEC).to_arrow(
        [
            {"Symbol": "AAPL", "Exchange": 4, "Price": 200.0},
            {"Symbol": "MSFT", "Exchange": 4, "Price": 400.0},
        ]
    )
    fake_streams.publishers[1].add(dh_arrow.to_table(rows))
    shown = rows_when(current, lambda rows: len(rows) == 1)
    assert [(r["Symbol"], r["Exchange"]) for r in shown] == [("AAPL", 4)]
    massive_session.release(current)


def test_ring_accepts_its_lower_bound(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    result = trades(massive_session).ring(1)
    assert result.is_refreshing
    massive_session.release(result)


def test_streaming_bars_name_the_timespans_with_a_channel() -> None:
    assert streaming_bars(BARS_SPEC) == "1-second and 1-minute bars at multiplier=1"
    minute_only = dataclasses.replace(BARS_SPEC, channels={Timespan.MINUTE: "CA"})
    assert streaming_bars(minute_only) == "1-minute bars at multiplier=1"


@pytest.mark.parametrize(
    ("timespan", "multiplier"),
    [(Timespan.HOUR, 1), (Timespan.DAY, 1), (Timespan.MINUTE, 5), (Timespan.SECOND, 2)],
)
def test_bars_without_a_channel_refuse_to_stream(timespan: Timespan, multiplier: int) -> None:
    request = Request(symbols=("C:EURUSD",), timespan=timespan, multiplier=multiplier)
    call = "m.forex.bars('C:EURUSD', ...).blink()"
    expected = (
        f"{call}: only 1-second and 1-minute bars at multiplier=1 stream; read these bars with "
        "history(start, end)"
    )
    with pytest.raises(ValueError, match="only") as caught:
        _check_streams(BARS_SPEC, request, call=call)
    assert str(caught.value) == expected


@pytest.mark.parametrize("timespan", [Timespan.SECOND, Timespan.MINUTE])
def test_streaming_bars_and_other_streams_pass_the_check(timespan: Timespan) -> None:
    bars = Request(symbols=("C:EURUSD",), timespan=timespan, multiplier=1)
    _check_streams(BARS_SPEC, bars, call="m.forex.bars(...).blink()")
    # A stream that isn't bars has no timespan, and streams as asked.
    _check_streams(TRADES_SPEC, Request(symbols=("AAPL",)), call="m.stocks.trades(...).blink()")


def test_a_stream_method_of_bars_that_do_not_stream_names_the_call(
    massive_session: dm.MassiveSession,
) -> None:
    hourly = massive_session.forex.bars("C:EURUSD", dm.Timespan.HOUR)
    assert type(hourly) is StreamHistoryQuery
    for verb, call in [
        ("blink", hourly.blink),
        ("append_only", hourly.append_only),
        ("last_by", hourly.last_by),
    ]:
        with pytest.raises(
            ValueError,
            match=rf"^m\.forex\.bars\('C:EURUSD', dm\.Timespan\.HOUR\)\.{verb}\(\): only",
        ):
            call()
    with pytest.raises(ValueError, match=r"\.ring\(\): only 1-second and 1-minute bars"):
        hourly.ring(10)
