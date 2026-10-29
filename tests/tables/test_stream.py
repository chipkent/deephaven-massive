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
import weakref
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import dtypes, input_table
from deephaven.arrow import to_table
from deephaven.stream import blink_to_append_only

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import DriftReport, Transport
from deephaven_massive._tables import stream
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
    channels={None: "T"},
    keys={(W, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "C"},
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
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    assert plan.symbol_field == "Ticker"
    assert plan.required == frozenset({"Ticker", "SipTimestamp"})
    assert plan.wire_schema.field("SipTimestamp").type == pa.int64()
    assert [c[1] for c in plan.columns][:2] == ["Symbol", "Timestamp"]
    assert plan.columns[1][2:] == ("SipTimestamp", plan.columns[1][3])


def test_a_plan_needs_a_symbol_key(massive_session: dm.MassiveSession) -> None:
    keyless = DatasetSpec(
        name="test.keyless",
        summary="A test dataset.",
        asset_class=AssetClass.STOCKS,
        channels={None: "T"},
    )
    with pytest.raises(ValueError, match="stocks/T declares no symbol key"):
        ChannelPlan.build(massive_session._core.catalog, keyless, "T")


def test_conversion_maps_symbols_and_converts_every_column(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, FOREX, "C")
    cache: dict[str, str | None] = {}
    rows = [
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
    with caplog.at_level(logging.WARNING):
        converted = plan.convert(rows, cache)
        plan.convert(rows, cache)  # cached: no second warning
    assert converted.failures == []
    batch = converted.batch
    assert batch.column("Symbol").to_pylist() == ["C:EURUSD", None]
    assert (converted.symbols, converted.null_symbols) == (frozenset({"C:EURUSD"}), True)
    assert batch.column("Timestamp").type == pa.timestamp("ns", tz="UTC")
    assert cache == {"EUR/USD": "C:EURUSD", "nonsense pair": None}
    assert caplog.text.count("which is not a valid symbol") == 1


def test_conversion_drops_and_reports_rows_with_bad_timestamps(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    rows = [
        trade("AAPL"),
        trade("MSFT", SipTimestamp=5),
        trade("IBM", TrfTimestamp=2**62),
    ]
    converted = plan.convert(rows, {})
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
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    wider = replace(
        plan,
        columns=(*plan.columns, (ColumnType.STRING, "Venue", None, None)),
        arrow_schema=plan.arrow_schema.append(pa.field("Venue", pa.string())),
    )
    batch = wider.convert([trade("AAPL")], {}).batch
    assert batch.column("Venue").to_pylist() == [None]


# --- StreamSink --------------------------------------------------------------------------------


def make_sink(core: SessionCore, symbols: NormalizedSymbols) -> tuple[StreamSink, Table]:
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    converted = plan.convert([trade("AAPL"), trade("MSFT")], {})
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
    nameless = plan.convert([trade("AAPL"), trade("not a symbol")], {})
    with_null = to_table(nameless.batch)
    some.set_symbols(frozenset({"AAPL"}))
    filtered = some._select(with_null, nameless)  # a null Symbol row needs the filter
    assert filtered is not None
    assert filtered.size == 1


def test_a_failed_sink_publishes_nothing_and_fails_once(
    massive_session: dm.MassiveSession,
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    converted = plan.convert([trade("AAPL")], {})
    batch = to_table(converted.batch)
    sink, _ = make_sink(core, ALL)
    assert sink.publish(batch, converted) == 1
    assert sink.fail(MassiveError("refused"))
    assert not sink.fail(MassiveError("again"))
    assert sink.publish(batch, converted) == 0
    assert sink.rows_published == 1


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
            ChannelPlan.build(core.catalog, TRADES, "T"),
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


def test_a_failed_watchlist_fails_its_stream_on_the_reaper(
    massive_session: dm.MassiveSession, drain: Callable[[], None]
) -> None:
    # The listener reports on an update-graph thread, so the failure is only queued there.
    sink, _ = make_sink(massive_session._core, ALL)
    sink.fail_later(RuntimeError("the watchlist broke"))
    drain()
    assert sink.failed


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
    monkeypatch.setattr(sink.publisher, "publish_failure", lambda _: order.append("failed"))
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
        assert (mine.core, theirs.core) == (massive_session._core, other._core)
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


@pytest.mark.parametrize("rows_per_flush", [200, 2_000, 20_000])
@pytest.mark.parametrize("streams", [1, 10, 100])
def test_benchmark_fan_out(
    massive_session: dm.MassiveSession,
    capsys: pytest.CaptureFixture[str],
    *,
    streams: int,
    rows_per_flush: int,
) -> None:
    # Records, never gates: the flush's shared batch against grouping the rows per stream in
    # one Python pass (one conversion and one table per stream).
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    symbols = [f"S{i}" for i in range(streams)]
    rows = [trade(symbols[i % streams], float(i)) for i in range(rows_per_flush)]
    sinks = [make_sink(core, (symbol,))[0] for symbol in symbols]

    def shared() -> None:
        converted = plan.convert(rows, {})
        table = to_table(converted.batch)
        for sink in sinks:
            sink.publish(table, converted)

    def grouped() -> None:
        buckets: dict[object, list[dict[str, object]]] = {}
        for row in rows:
            buckets.setdefault(row["Ticker"], []).append(row)
        for symbol, sink in zip(symbols, sinks, strict=True):
            sink.publisher.add(to_table(plan.convert(buckets.get(symbol, []), {}).batch))

    shared_rate = rows_per_flush / _median_seconds(shared)
    grouped_rate = rows_per_flush / _median_seconds(grouped)
    with capsys.disabled():
        sys.stderr.write(
            f"\nfan-out of {rows_per_flush} rows to {streams} streams: "
            f"shared batch {shared_rate:,.0f} rows/s, "
            f"Python grouping {grouped_rate:,.0f} rows/s\n"
        )


def test_a_failed_watchlist_of_a_finished_stream_does_nothing() -> None:
    # The JVM may report a listener's error after the stream's sink is gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    stream._fail_later(cast("weakref.ref[StreamSink]", gone), RuntimeError("late"))
