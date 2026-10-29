"""Tests for the Deephaven side of live streams: channel plans, sinks, the flusher, the fan-out.

No connection is involved: rows are put on a `Flusher`'s buffers as the connection's loop
thread would, and `flush()` is called explicitly. The non-gating benchmark at the end records
rows per second of the shared-batch fan-out against per-stream Python grouping.
"""

from __future__ import annotations

import logging
import statistics
import sys
import threading
import time
import types
import weakref
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import dtypes, input_table
from deephaven.arrow import to_arrow, to_table
from deephaven.stream import blink_to_append_only
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets import symbols
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, NotBars, UnknownSymbols
from deephaven_massive._errors import BoundedWarnings, MassiveError
from deephaven_massive._massive import DriftReport, Transport
from deephaven_massive._tables import stream
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.diagnostics import StreamUpdate
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.stream import (
    ChannelPlan,
    Flusher,
    RowFailure,
    StreamSink,
    open_stream_tables,
)
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._tables.core import SessionCore


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
W = Transport.WS
TIMEOUT = 15.0

TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(W, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)


def trade(symbol: str, price: float = 1.0, **fields: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Ticker": symbol,
        "Conditions": [12],
        "Exchange": 4,
        "Id": "1",
        "ParticipantTimestamp": 0,
        "Price": price,
        "SequenceNumber": 1,
        "SipTimestamp": 1_700_000_000_000,
        "Size": 100,
        "Tape": 3,
        "TrfId": None,
        "TrfTimestamp": None,
        "DecimalSize": "100",
    }
    row.update(fields)
    return row


ARRIVED = 1_700_000_000_123_456_789
"""When the test frames arrive, epoch nanoseconds: every converted row's `TimestampInsert`."""


def arrivals(rows: list[dict[str, object]]) -> list[int]:
    # One arrival per row, as the flusher passes them: every row of one frame.
    return [ARRIVED] * len(rows)


def to_arrow_column(table: Table, name: str) -> list[int]:
    # One Instant column of a table, as epoch nanoseconds.
    values: list[int] = to_arrow(table).column(name).cast(pa.int64()).to_pylist()
    return values


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


# --- ChannelPlan -------------------------------------------------------------------------------


def test_a_plan_reads_the_channel_for_the_dataset(massive_session: dm.MassiveSession) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL
    )
    assert plan.symbol_field == "Ticker"
    assert plan.required == frozenset({"Ticker", "SipTimestamp"})
    assert plan.wire_schema.field("SipTimestamp").type == pa.int64()
    assert [c[1] for c in plan.columns][:2] == ["Symbol", "Timestamp"]
    assert plan.columns[1][2:] == ("SipTimestamp", plan.columns[1][3])
    assert plan.times.timespan is None  # a channel that is not bars serves no timespan


def test_a_plan_needs_a_symbol_key(massive_session: dm.MassiveSession) -> None:
    keyless = DatasetSpec(
        name="test.keyless",
        summary="A test dataset.",
        asset_class=AssetClass.STOCKS,
        channels={NotBars.ANY: "T"},
    )
    with pytest.raises(ValueError, match="stocks/T declares no symbol key"):
        ChannelPlan.build(
            massive_session._core.catalog, keyless, "T", unknown_symbols=UnknownSymbols.FAIL
        )


FOREX_ROWS = [
    {
        "Ticker": "EUR/USD",
        "AskPrice": 1.1,
        "BidPrice": 1.0,
        "ParticipantTimestamp": 1_700_000_000_000,
        "Exchange": 1,
    },
    {
        "Ticker": "nonsense pair",
        "AskPrice": 1.1,
        "BidPrice": 1.0,
        "ParticipantTimestamp": 1_700_000_000_000,
        "Exchange": 1,
    },
]


def test_conversion_keeps_unreadable_symbols_in_raw_symbol_under_warn(
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        symbols, "_unknown_spellings", BoundedWarnings(logging.getLogger("t"), limit=5)
    )
    plan = ChannelPlan.build(
        massive_session._core.catalog, FOREX, "C", unknown_symbols=UnknownSymbols.WARN
    )
    cache: dict[str, str | None] = {}
    with caplog.at_level(logging.WARNING):
        converted = plan.convert(FOREX_ROWS, cache, arrived=arrivals(FOREX_ROWS))
        plan.convert(FOREX_ROWS, cache, arrived=arrivals(FOREX_ROWS))  # cached: no second warning
    assert converted.failures == []
    batch = converted.batch
    assert batch.column_names[:3] == ["Symbol", "Timestamp", "RawSymbol"]
    assert batch.column("Symbol").to_pylist() == ["C:EURUSD", None]
    assert batch.column("RawSymbol").to_pylist() == [None, "nonsense pair"]
    assert (converted.symbols, converted.null_symbols) == (frozenset({"C:EURUSD"}), True)
    assert batch.column("Timestamp").type == pa.timestamp("ns", tz="UTC")
    assert cache == {"EUR/USD": "C:EURUSD", "nonsense pair": None}
    assert caplog.text.count("Massive sent the symbol 'nonsense pair'") == 1


def test_conversion_fails_the_rows_of_unreadable_symbols_under_fail(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, FOREX, "C", unknown_symbols=UnknownSymbols.FAIL
    )
    converted = plan.convert(FOREX_ROWS, {}, arrived=arrivals(FOREX_ROWS))
    assert "RawSymbol" not in converted.batch.column_names
    assert converted.batch.column("Symbol").to_pylist() == ["C:EURUSD"]
    assert [f.symbol for f in converted.failures] == [None]
    message = converted.failures[0].message
    assert "test.forex: Massive sent the symbol 'nonsense pair'" in message
    assert "MASSIVE_UNKNOWN_SYMBOLS=warn" in message
    assert converted.null_symbols is False


def test_conversion_drops_and_reports_rows_with_bad_timestamps(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL
    )
    rows = [
        trade("AAPL"),
        trade("MSFT", SipTimestamp=5),
        trade("IBM", TrfTimestamp=2**62),
    ]
    converted = plan.convert(rows, {}, arrived=arrivals(rows))
    batch = converted.batch
    assert batch.column("Symbol").to_pylist() == ["AAPL"]
    assert (converted.symbols, converted.null_symbols) == (frozenset({"AAPL"}), False)
    assert converted.failures == [
        RowFailure(
            "MSFT",
            "timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100",
        ),
        RowFailure(
            "IBM",
            f"timestamp unit mismatch for test.trades.TrfTimestamp: {2**62} ms overflows "
            "64-bit nanoseconds",
        ),
    ]
    assert batch.column("ParticipantTimestamp").to_pylist() == [None]  # 0 is absent


def test_a_column_the_channel_lacks_is_null(massive_session: dm.MassiveSession) -> None:
    # A dataset in a kind has the kind's columns, some of which another member's source carries.
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL
    )
    wider = replace(
        plan,
        columns=(*plan.columns, (ColumnType.STRING, "Venue", None, None)),
        arrow_schema=plan.arrow_schema.append(pa.field("Venue", pa.string())),
    )
    batch = wider.convert([trade("AAPL")], {}, arrived=[ARRIVED]).batch
    assert batch.column("Venue").to_pylist() == [None]


# --- StreamSink --------------------------------------------------------------------------------


def make_sink(core: SessionCore, symbols: NormalizedSymbols) -> tuple[StreamSink, Table]:
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES),
        symbols,
        core=core,
        stream_id=1,
        host="socket.test",
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    return opened.sink, opened.handle.blink


def test_a_sink_selects_its_rows_without_work_per_row(massive_session: dm.MassiveSession) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL)
    converted = plan.convert([trade("AAPL"), trade("MSFT")], {}, arrived=[ARRIVED] * 2)
    batch = to_table(converted.batch)
    every, _ = make_sink(core, ALL)
    assert every._select(batch, converted) is batch
    assert every.receives(None)
    disjoint, _ = make_sink(core, ("IBM",))
    assert disjoint._select(batch, converted) is None  # no filter: it holds no batch symbol
    superset, _ = make_sink(core, ("AAPL", "IBM", "MSFT"))
    assert superset._select(batch, converted) is batch  # no filter: it holds them all
    some, _ = make_sink(core, ("AAPL",))
    selected = some._select(batch, converted)
    assert selected is not None
    assert selected.size == 1
    cached = some._filter
    assert some._select(batch, converted) is not None
    assert some._filter is cached  # the symbols did not change: the filter is reused
    assert some.receives("AAPL")
    assert not some.receives(None)
    some.set_symbols(frozenset({"AAPL", "MSFT"}))
    assert some._select(batch, converted) is batch
    some.set_symbols(frozenset())
    assert some._select(batch, converted) is None
    nameless = plan.convert([trade("AAPL"), trade("not a symbol")], {}, arrived=[ARRIVED] * 2)
    with_null = to_table(nameless.batch)
    some.set_symbols(frozenset({"AAPL"}))
    filtered = some._select(with_null, nameless)  # a null Symbol row needs the filter
    assert filtered is not None
    assert filtered.size == 1


def test_a_failed_sink_publishes_nothing_and_fails_once(
    massive_session: dm.MassiveSession,
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL)
    converted = plan.convert([trade("AAPL")], {}, arrived=[ARRIVED])
    batch = to_table(converted.batch)
    sink, _ = make_sink(core, ALL)
    assert sink.publish(batch, converted) == 1
    assert sink.fail(MassiveError("refused"))
    assert not sink.fail(MassiveError("again"))
    assert sink.publish(batch, converted) == 0
    assert sink.rows_published == 1


class _Pinned:
    # A `blink_publisher` table in its own scope, with what its shutdown callback saw and the
    # pin releases it deferred.
    def __init__(self, exec_ctx: ExecutionContext) -> None:
        self._update_graph = exec_ctx.update_graph.j_update_graph
        self.shut_down = threading.Event()
        self.on_update_graph: list[bool] = []
        self.deferred: list[Callable[[], None]] = []
        self.scope = new_scope()
        with self.scope.open():
            self.blink, self.publisher = stream.blink_publisher(
                "test.pinned",
                {"X": dtypes.int64},
                on_shutdown=self._on_shutdown,
                defer=self.deferred.append,
            )

    def _on_shutdown(self) -> None:
        self.on_update_graph.append(bool(self._update_graph.currentThreadProcessesUpdates()))
        self.shut_down.set()

    def failed(self) -> bool:
        return bool(self.blink.j_table.isFailed())

    def destroyed(self) -> bool:
        retained = bool(self.blink.j_table.tryRetainReference())
        if retained:
            self.blink.j_table.dropReference()
        return not retained


def test_a_queued_failure_keeps_its_table_alive_until_it_is_delivered(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Releasing the last scope of a table whose failure is queued must not destroy it. A
    # destroy on another thread races the failure's delivery inside Deephaven, which can then
    # deliver the failure twice: a fatal assertion that exits the process. While the shared lock
    # is held no update cycle runs, so the failure is still queued when the scope is released.
    pinned = _Pinned(dh_exec_ctx)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("refused"))
        pinned.scope.release()
        early = pinned.shut_down.is_set()
    assert not early  # pinned: the release did not destroy the table
    assert pinned.shut_down.wait(TIMEOUT)
    # The delivery shut the table down, inside the update cycle that failed it.
    assert pinned.on_update_graph == [True]
    assert pinned.failed()
    assert not pinned.destroyed()
    assert len(pinned.deferred) == 1
    pinned.deferred[0]()  # the session's reaper releases the pin, and the table dies
    assert pinned.destroyed()
    assert pinned.on_update_graph == [True]  # the adapter had already shut down


def test_a_destroyed_table_publishes_no_failure(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned = _Pinned(dh_exec_ctx)
    pinned.scope.release()
    assert pinned.on_update_graph == [False]  # the release destroyed it, on this thread
    published: list[Exception] = []
    monkeypatch.setattr(
        pinned.publisher, "_publisher", types.SimpleNamespace(publish_failure=published.append)
    )
    pinned.publisher.publish_failure(MassiveError("too late"))
    assert published == []
    assert pinned.deferred == []


def test_a_failure_that_raises_unpins_its_table(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure was not queued, so nothing would ever deliver it and release the pin.
    pinned = _Pinned(dh_exec_ctx)

    def interrupted(_error: Exception) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        pinned.publisher, "_publisher", types.SimpleNamespace(publish_failure=interrupted)
    )
    with pytest.raises(KeyboardInterrupt):
        pinned.publisher.publish_failure(MassiveError("refused"))
    pinned.scope.release()
    assert pinned.on_update_graph == [False]  # nothing pins it: the release destroyed it
    assert pinned.deferred == []
    assert not pinned.failed()


def test_open_stream_tables_releases_its_scope_on_failure(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The watch fails after the publisher exists: releasing the scope shuts the blink table down.
    shut_down = threading.Event()

    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no watch")

    monkeypatch.setattr(stream, "SymbolWatch", broken)
    core = massive_session._core
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        open_stream_tables(
            ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL),
            core.catalog.dataset_schema(TRADES),
            SymbolTable(watchlist),
            core=core,
            stream_id=1,
            host="socket.test",
            path="/stocks",
            on_shutdown=shut_down.set,
            on_symbols=lambda *_: None,
        )
    assert shut_down.wait(TIMEOUT)


def test_a_reactive_stream_starts_with_no_symbols(massive_session: dm.MassiveSession) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    sink, _ = make_sink(massive_session._core, SymbolTable(watchlist))
    assert not sink.receives("AAPL")


def test_a_reactive_streams_handle_holds_its_watch(massive_session: dm.MassiveSession) -> None:
    # Deephaven holds a table's listeners only weakly: the session's record of the stream holds
    # the watch through the handle, whatever else keeps it reachable, and stops it through it.
    core = massive_session._core
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    opened = open_stream_tables(
        ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL),
        core.catalog.dataset_schema(TRADES),
        SymbolTable(watchlist),
        core=core,
        stream_id=1,
        host="socket.test",
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    assert opened.watch is not None
    assert opened.handle.stop_watch == opened.watch.stop
    opened.handle.scope.release()


def test_a_failed_watchlist_fails_its_stream_on_the_reaper(
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The listener reports on an update-graph thread, so the failure is only queued there.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    failures = _record_failures(sink, monkeypatch)
    gate = threading.Event()

    def hold() -> None:  # holds the reaper until the call has returned
        assert gate.wait(TIMEOUT)

    core.defer(hold)
    # What the watch's callback does, called here from a Python thread, which line coverage
    # before 3.14 can see.
    stream._fail_later(
        weakref.ref(sink), "the symbol table failed, so the result can no longer follow it"
    )
    assert not sink.failed  # nothing failed on the reporting thread
    gate.set()
    drain()
    assert failures == ["the symbol table failed, so the result can no longer follow it"]


def _record_failures(sink: StreamSink, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    # Records the error each `publish_failure` of the sink's publisher fails its table with.
    failures: list[str] = []
    publisher = sink.publisher

    class _Recording:
        def publish_failure(self, error: Exception) -> None:
            failures.append(str(error))
            publisher.publish_failure(error)

    monkeypatch.setattr(sink, "publisher", _Recording())
    return failures


def test_a_failure_after_the_close_fails_the_table_with_the_close(
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The close marked the row first: the table names the close, as its row does, and the
    # failure is not logged, since the close logs its count.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    failures = _record_failures(sink, monkeypatch)
    closed = f"{core.label} was closed; this table no longer updates"
    monkeypatch.setattr(core, "fail_stream", lambda _stream_id, _error: closed)
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.stream"):
        assert sink.fail(MassiveError("a failure that raced the close"))
    assert failures == [closed]
    assert not caplog.records
    assert not sink.fail(MassiveError("again"))  # once only


# --- Time columns -------------------------------------------------------------------------------

MINUTE_NS = 60 * 10**9
# Friday 2025-03-07 16:30 CT and 17:30 CT: the second belongs to Monday 03-10's session.
BEFORE_ROLL_MS = 1_741_386_600_000
AFTER_ROLL_MS = 1_741_390_200_000


def futures_trade(symbol: str, at_ms: int) -> dict[str, object]:
    return {"Ticker": symbol, "Timestamp": at_ms, "SequenceNumber": 1, "Price": 1.0, "Size": 1}


def futures_bar(start_ms: int, end_ms: int) -> dict[str, object]:
    return {
        "Ticker": "ESM5",
        "WindowStart": start_ms,
        "EndTimestamp": end_ms,
        "Open": 1.0,
        "High": 1.0,
        "Low": 1.0,
        "Close": 1.0,
        "Volume": 1,
        "DollarVolume": 1.0,
        "Transactions": 1,
    }


class FakeSessionDates:
    """A schedule calendar that moves one symbol's rows to a fixed date and misses another's."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str | None], list[int | None], list[str | None]]] = []
        self.missing = {"NQM5"}

    def correct(
        self, symbols: pa.Array, instants: pa.Array, dates: pa.Array
    ) -> tuple[pa.Array, list[tuple[str, str]]]:
        names = symbols.to_pylist()
        self.calls.append((names, instants.to_pylist(), dates.to_pylist()))
        fixed = [
            "2025-03-11" if name == "ESM5" else day
            for name, day in zip(names, dates.to_pylist(), strict=True)
        ]
        misses = [(name, f"{name} missed") for name in sorted(self.missing) if name in names]
        return pa.array(fixed, pa.string()), misses


def test_a_futures_row_gets_the_rule_session_corrected_by_the_schedules(
    massive_session: dm.MassiveSession,
) -> None:
    spec = massive_session._core.catalog.by_name["futures.trades"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "T", unknown_symbols=UnknownSymbols.FAIL
    )
    rows = [
        futures_trade("GCJ5", BEFORE_ROLL_MS),
        futures_trade("GCJ5", AFTER_ROLL_MS),
        futures_trade("ESM5", BEFORE_ROLL_MS),
        futures_trade("NQM5", BEFORE_ROLL_MS),
    ]
    sessions = FakeSessionDates()
    converted = plan.convert(rows, {}, arrived=[ARRIVED] * 4, sessions=sessions)
    assert converted.batch.column("Date").to_pylist() == [
        "2025-03-07",
        "2025-03-10",  # after Friday's roll: Monday's session
        "2025-03-11",  # moved by the schedules
        "2025-03-07",
    ]
    assert sessions.calls[0][2] == ["2025-03-07", "2025-03-10", "2025-03-07", "2025-03-07"]
    assert converted.misses == (("NQM5", "NQM5 missed"),)
    # Without a calendar, the rule's dates stand.
    alone = plan.convert(rows, {}, arrived=[ARRIVED] * 4)
    assert alone.batch.column("Date").to_pylist()[2] == "2025-03-07"
    assert alone.misses == ()


def test_a_streamed_bar_ends_its_length_after_its_start(
    massive_session: dm.MassiveSession,
) -> None:
    spec = massive_session._core.catalog.by_name["stocks.bars"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "AM", unknown_symbols=UnknownSymbols.FAIL
    )
    start = 1_741_012_200_000  # 2025-03-03 14:30Z
    row = {
        "Ticker": "AAPL",
        "WindowStart": start,
        "EndTimestamp": start + 59_999,  # Massive's `e`: recomputed, not kept
        "Open": 1.0,
        "Close": 1.0,
        "High": 1.0,
        "Low": 1.0,
        "Volume": 1,
    }
    batch = plan.convert([row], {}, arrived=[ARRIVED]).batch
    assert batch.column("TimestampStart").cast(pa.int64()).to_pylist() == [start * 10**6]
    assert batch.column("TimestampEnd").cast(pa.int64()).to_pylist() == [start * 10**6 + MINUTE_NS]
    assert batch.column("Date").to_pylist() == ["2025-03-03"]
    assert "EndTimestamp" not in batch.schema.names


def test_a_futures_second_bar_ends_at_the_later_end(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    spec = massive_session._core.catalog.by_name["futures.bars"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "A", unknown_symbols=UnknownSymbols.FAIL
    )
    start = BEFORE_ROLL_MS
    rows = [futures_bar(start, start + 60_000), futures_bar(start, start + 1_000)]
    with caplog.at_level(logging.WARNING):
        batch = plan.convert(rows, {}, arrived=[ARRIVED] * 2).batch
    ends = batch.column("TimestampEnd").cast(pa.int64()).to_pylist()
    assert ends == [(start + 60_000) * 10**6, (start + 1_000) * 10**6]
    assert caplog.text.count("is not its start plus one second") == 1


def test_every_row_takes_the_arrival_of_its_frame(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    sink, blink = make_sink(core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", sink)
    before = time.time_ns()
    flusher.rows("T", [trade("AAPL")])
    buffer = flusher.routes.buffer("T")
    assert buffer is not None
    (stamped, rows) = buffer.get_nowait()  # the frame, with the instant it was handed over
    assert before <= stamped <= time.time_ns()
    assert [row["Ticker"] for row in rows] == ["AAPL"]
    # Two frames of one flush: each row takes its own frame's arrival.
    buffer.put((111, [trade("AAPL"), trade("MSFT")]))
    buffer.put((222, [trade("IBM")]))
    flusher.flush()
    wait_for(lambda: shown.size == 3)
    inserted = to_arrow_column(shown, "TimestampInsert")
    assert inserted == [111, 111, 222]


def test_a_session_miss_shows_in_the_streams_error_without_failing_it(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    spec = core.catalog.by_name["futures.trades"]
    plan = ChannelPlan.build(core.catalog, spec, "T", unknown_symbols=UnknownSymbols.FAIL)
    updates: list[dict[int, StreamUpdate]] = []
    monkeypatch.setattr(core, "update_streams", updates.append)
    calendar = FakeSessionDates()
    flusher = Flusher(core.exec_ctx, "test-flush", interval=3600.0, sessions=calendar)
    sinks = []
    for stream_id, wanted in ((1, ("NQM5",)), (2, ("ESM5",))):
        opened = open_stream_tables(
            plan,
            core.catalog.dataset_schema(spec),
            wanted,
            core=core,
            stream_id=stream_id,
            host="socket.test",
            path="/futures",
            on_shutdown=lambda: None,
            on_symbols=lambda *_: None,
        )
        flusher.routes.add("T", opened.sink)
        sinks.append(opened.sink)
    both = [futures_trade("NQM5", BEFORE_ROLL_MS), futures_trade("ESM5", BEFORE_ROLL_MS)]
    flusher.rows("T", both)
    flusher.flush()
    # The same miss again: the row already shows it.
    flusher.rows("T", both)
    flusher.flush()
    # Placed at last: the stream's next rows clear it.
    calendar.missing.clear()
    flusher.rows("T", both)
    flusher.flush()
    # A stream no longer routed is forgotten.
    calendar.missing.add("NQM5")
    flusher.rows("T", both)
    flusher.flush()
    flusher.routes.remove(1)
    flusher.flush()
    flusher.stop()
    assert not any(sink.failed for sink in sinks)
    assert updates[:3] == [
        {
            1: StreamUpdate(rows_published=1, error="NQM5 missed"),
            2: StreamUpdate(rows_published=1),
        },
        {1: StreamUpdate(rows_published=2), 2: StreamUpdate(rows_published=2)},
        {
            1: StreamUpdate(rows_published=3, clear_error=True),
            2: StreamUpdate(rows_published=3),
        },
    ]
    assert flusher._shown == {}


# --- Flusher -----------------------------------------------------------------------------------


@pytest.fixture
def flusher(massive_session: dm.MassiveSession) -> Iterator[Flusher]:
    f = Flusher(massive_session._core.exec_ctx, "test-flush", interval=3600.0)
    yield f
    f.stop()


def test_a_flush_publishes_each_channel_once_to_every_stream(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    core = massive_session._core
    aapl, aapl_blink = make_sink(core, ("AAPL",))
    every, every_blink = make_sink(core, ALL)
    idle, _ = make_sink(core, ("IBM",))  # receives none of these rows
    shown = [blink_to_append_only(aapl_blink), blink_to_append_only(every_blink)]
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", every)
    flusher.routes.add("T", idle)
    flusher.rows("T", [trade("AAPL"), trade("MSFT")])
    flusher.rows("Q", [trade("AAPL")])  # no stream ever read Q: dropped
    flusher.flush()
    rows_when(shown[0], lambda rows: len(rows) == 1)
    rows_when(shown[1], lambda rows: len(rows) == 2)
    assert (aapl.rows_published, every.rows_published, idle.rows_published) == (1, 2, 0)


def test_a_stream_routed_while_rows_drain_gets_them(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows_when: RowsWhen,
) -> None:
    # The routes are read after the rows are drained, so a stream routed meanwhile is served.
    core = massive_session._core
    first, _ = make_sink(core, ALL)
    late, late_blink = make_sink(core, ALL)
    shown = blink_to_append_only(late_blink)
    flusher.routes.add("T", first)
    flusher.rows("T", [trade("AAPL")])
    routes_type = type(flusher.routes)
    buffers = routes_type.buffers

    def routing_meanwhile(routes: object) -> tuple[object, ...]:
        drained = buffers(flusher.routes)
        flusher.routes.add("T", late)
        return drained

    monkeypatch.setattr(routes_type, "buffers", routing_meanwhile)
    flusher.flush()
    rows_when(shown, lambda rows: len(rows) == 1)


def test_a_failure_fails_each_stream_once(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    sink, _ = make_sink(core, ("AAPL",))
    flusher.routes.add("T", sink)
    flusher.fail([1], MassiveError("refused"))
    flusher.fail([1], MassiveError("again"))
    flusher.rows("T", [trade("AAPL")])
    flusher.flush()
    assert sink.failed
    assert sink.rows_published == 0  # a failed stream gets no rows


def test_a_batch_whose_every_row_failed_publishes_nothing(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",))
    msft, _ = make_sink(core, ("MSFT",))
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    flusher.rows("T", [trade("AAPL", SipTimestamp=5)])
    flusher.flush()
    assert aapl.failed
    assert not msft.failed


def test_a_failure_is_recorded_before_the_table_fails(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A verb that meets the failed table must find the failure recorded, to name it.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    order: list[str] = []
    monkeypatch.setattr(core, "fail_stream", lambda *_: order.append("recorded"))
    failing = types.SimpleNamespace(publish_failure=lambda _: order.append("failed"))
    monkeypatch.setattr(sink, "publisher", failing)
    sink.fail(MassiveError("refused"))
    assert order == ["recorded", "failed"]


def test_a_batch_that_cannot_be_converted_fails_only_its_readers(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    caplog: pytest.LogCaptureFixture,
    *,
    rows_when: RowsWhen,
) -> None:
    # One channel's bad batch costs neither another channel its rows nor the flush its updates.
    core = massive_session._core
    broken, _ = make_sink(core, ALL)
    healthy, healthy_blink = make_sink(core, ALL)
    shown = blink_to_append_only(healthy_blink)
    flusher.routes.add("T", broken)
    flusher.routes.add("Q", healthy)  # the same plan, routed as another channel's reader
    flusher.rows("T", [trade("AAPL", Price="not a number")])
    flusher.rows("Q", [trade("AAPL")])
    flusher.flush()
    assert broken.failed
    assert not healthy.failed
    rows_when(shown, lambda rows: len(rows) == 1)
    assert "test.trades: could not publish a T batch" in caplog.text


def test_drift_is_reported_for_the_streams_of_the_channel(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    with caplog.at_level(logging.WARNING):
        flusher.drift("T", DriftReport(extra=frozenset({"zz"})))
        flusher.flush()
    assert "test.trades: WS field 'zz' is not in the schema" in caplog.text


def test_routes_forget_a_stream_once(flusher: Flusher, massive_session: dm.MassiveSession) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    assert flusher.routes.remove(1) is sink
    assert flusher.routes.remove(1) is None
    assert flusher.routes.sinks() == ()


def test_a_sink_whose_session_is_gone_reports_nothing(
    *,
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    drain: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A sink outlives its stream's record in a container cleaned up later (the routes of a
    # connection a verdict moved the stream off): with its session freed, it still publishes
    # and fails its own table, and reports nothing.
    sink, _ = make_sink(massive_session._core, ALL)
    monkeypatch.setattr(sink, "_core", lambda: None)
    flusher.routes.add("T", sink)
    flusher.rows("T", [trade("AAPL")])
    flusher.drift("T", DriftReport(extra=frozenset({"zz"})))
    flusher.flush()
    assert sink.rows_published == 1
    sink.fail_later("never queued")
    drain()
    assert not sink.failed
    assert sink.fail(MassiveError("its table still fails"))


def test_one_flusher_reports_each_stream_to_its_own_session(
    massive_session: dm.MassiveSession, flusher: Flusher, stream_rows: Callable[[], Any]
) -> None:
    # A connection shared by two sessions: each stream's rows and drift reach its own session.
    other = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    try:
        mine, _ = make_sink(massive_session._core, ALL)
        theirs, _ = make_sink(other._core, ALL)
        flusher.routes.add("T", mine)
        flusher.routes.add("T", theirs)
        flusher.rows("T", [trade("AAPL")])
        flusher.flush()
        assert (mine.rows_published, theirs.rows_published) == (1, 1)
        assert (mine.core(), theirs.core()) == (massive_session._core, other._core)
    finally:
        other.close()


class _Fatal(BaseException):
    # Like KeyboardInterrupt: a flush that raises it ends the thread.
    pass


class _DyingFlusher(Flusher):
    __slots__ = ()

    def __init__(self, exec_ctx: ExecutionContext) -> None:
        super().__init__(exec_ctx, "dying-flush", interval=0.001)

    @override
    def flush(self) -> None:
        raise _Fatal


class _Stubborn:
    # A routed stream whose failure raises.
    stream_id = 2

    def fail(self, error: MassiveError) -> bool:
        raise RuntimeError(f"cannot fail: {error}")


def test_a_dying_flush_thread_fails_its_streams(
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)  # the death still escapes
    core = massive_session._core
    healthy, _ = make_sink(core, ALL)
    f = _DyingFlusher(core.exec_ctx)
    f.routes.add("T", cast("StreamSink", _Stubborn()))
    f.routes.add("T", healthy)
    with caplog.at_level(logging.ERROR):
        f.start()
        assert f.join(TIMEOUT)
    assert healthy.failed  # a stream that cannot be failed does not spare the others
    assert "dying-flush died; its streams fail" in caplog.text
    assert "dying-flush: could not fail a stream" in caplog.text
    assert [args.exc_type for args in escaped] == [_Fatal]


class _BrokenFlusher(Flusher):
    __slots__ = ("flushes",)

    def __init__(self, core: SessionCore) -> None:
        super().__init__(core.exec_ctx, "test-flush", interval=0.001)
        self.flushes = threading.Semaphore(0)

    @override
    def flush(self) -> None:
        self.flushes.release()
        raise RuntimeError("bad flush")


def test_the_flush_thread_logs_a_failing_flush_and_carries_on(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    f = _BrokenFlusher(massive_session._core)
    f.start()
    assert f.flushes.acquire(timeout=TIMEOUT)
    assert f.flushes.acquire(timeout=TIMEOUT)  # it kept going
    f.stop()
    assert f.join(TIMEOUT)
    assert "test-flush: a flush failed; continuing" in caplog.text


def test_an_unstarted_flusher_joins_at_once(massive_session: dm.MassiveSession) -> None:
    assert Flusher(massive_session._core.exec_ctx, "never", interval=1.0).join(0.0)


# --- Benchmark (records, never gates) ----------------------------------------------------------


def _median_seconds(work: Callable[[], None], *, repeats: int = 7, warmups: int = 3) -> float:
    # The median of warmed repetitions, so neither path pays the JIT's or a filter's first use.
    for _ in range(warmups):
        work()
    times = []
    for _ in range(repeats):
        started = time.perf_counter()
        work()
        times.append(time.perf_counter() - started)
    return statistics.median(times)


# The default run measures one point, 10 streams at 2,000 rows, which keeps the benchmark
# working. The other eight points cost several seconds and record rates no test asserts on, so
# they carry the `benchmark` marker, which the default run deselects; `-m benchmark` runs them.
_FAN_OUT_DEFAULT = (10, 2_000)
_FAN_OUT_POINTS = [
    pytest.param(
        streams,
        rows,
        marks=() if (streams, rows) == _FAN_OUT_DEFAULT else pytest.mark.benchmark,
        id=f"{streams}-{rows}",
    )
    for streams in (1, 10, 100)
    for rows in (200, 2_000, 20_000)
]


@pytest.mark.parametrize(("streams", "rows_per_flush"), _FAN_OUT_POINTS)
def test_benchmark_fan_out(
    massive_session: dm.MassiveSession,
    capsys: pytest.CaptureFixture[str],
    *,
    streams: int,
    rows_per_flush: int,
) -> None:
    # Records, never gates: the flush's shared batch against grouping the rows per stream in
    # one Python pass (one conversion and one table per stream). Each table is built as the
    # flush builds it, through `dataset_table`, so the cost of sharing dates is counted.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unknown_symbols=UnknownSymbols.FAIL)
    symbols = [f"S{i}" for i in range(streams)]
    rows = [trade(symbols[i % streams], float(i)) for i in range(rows_per_flush)]
    sinks = [make_sink(core, (symbol,))[0] for symbol in symbols]

    def shared() -> None:
        converted = plan.convert(rows, {}, arrived=arrivals(rows))
        table = dataset_table(converted.batch, plan.date_columns)
        for sink in sinks:
            sink.publish(table, converted)

    def grouped() -> None:
        buckets: dict[object, list[dict[str, object]]] = {}
        for row in rows:
            buckets.setdefault(row["Ticker"], []).append(row)
        for symbol, sink in zip(symbols, sinks, strict=True):
            own = buckets.get(symbol, [])
            batch = plan.convert(own, {}, arrived=arrivals(own)).batch
            sink.publisher.add(dataset_table(batch, plan.date_columns))

    def stored() -> None:
        dataset_table(plan.convert(rows, {}, arrived=arrivals(rows)).batch, plan.date_columns)

    def converted_only() -> None:
        to_table(plan.convert(rows, {}, arrived=arrivals(rows)).batch)

    shared_seconds = _median_seconds(shared)
    grouped_rate = rows_per_flush / _median_seconds(grouped)
    sharing = _median_seconds(stored) - _median_seconds(converted_only)
    with capsys.disabled():
        sys.stderr.write(
            f"\nfan-out of {rows_per_flush} rows to {streams} streams: "
            f"shared batch {rows_per_flush / shared_seconds:,.0f} rows/s "
            f"({shared_seconds * 1000:.2f} ms a flush, {sharing * 1000:.2f} ms of it sharing "
            f"dates), Python grouping {grouped_rate:,.0f} rows/s\n"
        )


def test_a_failed_watchlist_of_a_finished_stream_does_nothing() -> None:
    # The JVM may report a listener's error after the stream's sink is gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    stream._fail_later(cast("weakref.ref[StreamSink]", gone), "late")


def test_streamed_rows_of_one_day_share_one_stored_date(
    massive_session: dm.MassiveSession, string_objects: Callable[[Table, str], int]
) -> None:
    core = massive_session._core
    spec = core.catalog.by_name["stocks.trades"]
    plan = ChannelPlan.build(core.catalog, spec, "T", unknown_symbols=UnknownSymbols.FAIL)
    flusher = Flusher(core.exec_ctx, "test-flush", interval=3600.0)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(spec),
        ALL,
        core=core,
        stream_id=1,
        host="socket.test",
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    shown = blink_to_append_only(opened.handle.blink)
    flusher.routes.add("T", opened.sink)
    flusher.rows("T", [trade("AAPL"), trade("MSFT"), trade("IBM")])
    flusher.flush()
    flusher.stop()
    wait_for(lambda: shown.size == 3)
    assert string_objects(shown, "Date") == 1


def test_the_backlog_counts_buffered_rows_and_warns_when_it_grows(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=stream.__name__)
    monkeypatch.setattr(stream, "BACKLOG_WARNING_ROWS", 4)
    sink, _ = make_sink(massive_session._core, ("AAPL",))
    flusher.routes.add("T", sink)

    def warnings() -> int:
        return sum("rows are waiting to be published" in r.getMessage() for r in caplog.records)

    flusher.rows("Q", [trade("AAPL")])  # no stream ever read Q: dropped, never buffered
    flusher.rows("T", [trade("AAPL")] * 4)
    assert (flusher.backlog, warnings()) == (4, 0)  # at the level, not above it
    flusher.rows("T", [trade("AAPL")])
    # Each buffered frame holds its arrival time with its rows: the backlog counts the rows.
    flusher.rows("T", [trade("AAPL")] * 3)
    assert (flusher.backlog, warnings()) == (8, 1)  # logged once while it stays above
    flusher.flush()
    assert flusher.backlog == 0  # nothing was dropped: the flush took every row
    flusher.rows("T", [trade("AAPL")] * 5)
    assert warnings() == 2  # below half the level in between, so it is logged again


def test_the_backlog_warning_waits_until_the_backlog_falls_below_half(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream, "BACKLOG_WARNING_ROWS", 4)
    sink, _ = make_sink(massive_session._core, ("AAPL",))
    flusher.routes.add("T", sink)
    flusher.rows("T", [trade("AAPL")] * 5)
    assert flusher._backlog_warned
    flusher._drained(3)  # as a flush that took three of the five would
    assert flusher._backlog_warned  # two rows are not below half of four
    flusher._drained(1)
    assert not flusher._backlog_warned
