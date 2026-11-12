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
from deephaven import dtypes
from deephaven.arrow import to_arrow, to_table
from deephaven.stream import blink_to_append_only
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets import wire_symbols
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import ALL, SymbolTable
from deephaven_massive._enums import AssetClass, NotBars, OnError, Recency
from deephaven_massive._errors import LIBRARY_BUG, MassiveError
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._massive import DriftReport, DriftSample, Transport, sources
from deephaven_massive._tables import stream
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.diagnostics import NotePart, StreamUpdate
from deephaven_massive._tables.execution_context import update_cycle
from deephaven_massive._tables.liveness import release_scope
from deephaven_massive._tables.stream import (
    ChannelPlan,
    Flusher,
    StreamSink,
    open_stream_tables,
)
from deephaven_massive._tables.watchlist import skipped_rows_text
from deephaven_massive.namespaces._options import ADMIN_MESSAGES

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._tables.session_core import SessionCore


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


def frame(rows: list[dict[str, object]], source_key: str = "stocks/T") -> list[tuple[object, ...]]:
    # One frame's rows as the decoder delivers them: each a tuple of the values of the channel's
    # source's fields, in their order, `None` where a row lacks one.
    columns = [field.column for field in sources(Transport.WS)[source_key].fields]
    return [tuple(row.get(column) for column in columns) for row in rows]


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
        massive_session._core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE
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
            massive_session._core.catalog, keyless, "T", unreadable_symbols=OnError.RAISE
        )


def test_a_plan_of_rows_without_symbols_reads_none(massive_session: dm.MassiveSession) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, ADMIN_MESSAGES, "AD", unreadable_symbols=OnError.LOG
    )
    assert plan.symbol_field is None
    assert plan.required == frozenset({"SipTimestamp"})
    assert [c[1] for c in plan.columns] == [
        "Date",
        "Timestamp",
        "MessageId",
        "Exchange",
        "Message",
        "SequenceNumber",
        "TimestampInsert",
    ]


def notice(**changes: object) -> dict[str, object]:
    # One admin message as the decoder's columns name its fields.
    row: dict[str, object] = {
        "MessageId": "1793911442930568079-4043826282",
        "Exchange": 302,
        "Message": "TEXT CFLEX SPY LST PM EUR 4 LEGS 1 @ 733.36",
        "SipTimestamp": 1_793_911_442_930_568_200,
        "SequenceNumber": 4_043_826_282,
    }
    return {**row, **changes}


def test_rows_without_symbols_convert_whole_and_drop_only_a_bad_time(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, ADMIN_MESSAGES, "AD", unreadable_symbols=OnError.LOG
    )
    rows = [notice(), notice(Exchange=323, SipTimestamp=5), notice(Exchange=None)]
    converted = plan.convert(frame(rows, "options/AD"), {}, arrived=arrivals(rows))
    batch = converted.batch
    # The row whose time fails is dropped and counted; a missing exchange is only null.
    assert batch.column("Exchange").to_pylist() == [302, None]
    assert batch.column("Message").to_pylist() == [notice()["Message"]] * 2  # the text, as sent
    assert batch.column("Date").to_pylist() == ["2026-11-05"] * 2
    assert "Symbol" not in batch.column_names
    assert (converted.symbols, converted.null_symbols, converted.dropped_symbols) == (
        frozenset(),
        False,
        {},
    )
    assert converted.drift.dropped_rows == 1


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


def quote(ticker: str, **fields: object) -> dict[str, object]:
    # One forex quote as Massive's WebSocket writes it, the pair as given.
    return {**FOREX_ROWS[0], "Ticker": ticker, **fields}


@pytest.mark.parametrize(
    ("mode", "level"), [(OnError.LOG, logging.ERROR), (OnError.IGNORE, logging.DEBUG)]
)
def test_conversion_keeps_unreadable_symbols_in_raw_symbol(
    massive_session: dm.MassiveSession,
    *,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    mode: OnError,
    level: int,
) -> None:
    fresh = BoundedLog(logging.getLogger("t"), limit=5, level=level)
    monkeypatch.setattr(
        wire_symbols, "_logged_spellings" if mode is OnError.LOG else "_ignored_spellings", fresh
    )
    plan = ChannelPlan.build(massive_session._core.catalog, FOREX, "C", unreadable_symbols=mode)
    cache: dict[str, str | None] = {}
    with caplog.at_level(logging.DEBUG, logger="t"):
        converted = plan.convert(frame(FOREX_ROWS, "forex/C"), cache, arrived=arrivals(FOREX_ROWS))
        plan.convert(
            frame(FOREX_ROWS, "forex/C"), cache, arrived=arrivals(FOREX_ROWS)
        )  # cached: no second warning
    assert (converted.unreadable, converted.drift) == ("", DriftReport())
    batch = converted.batch
    assert batch.column_names[:3] == ["Symbol", "Timestamp", "RawSymbol"]
    assert batch.column("Symbol").to_pylist() == ["C:EURUSD", None]
    assert batch.column("RawSymbol").to_pylist() == [None, "nonsense pair"]
    assert (converted.symbols, converted.null_symbols) == (frozenset({"C:EURUSD"}), True)
    assert batch.column("Timestamp").type == pa.timestamp("ns", tz="UTC")
    assert cache == {"EUR/USD": "C:EURUSD", "nonsense pair": None}
    records = [r for r in caplog.records if "Massive sent the symbol 'nonsense pair'" in r.message]
    assert [r.levelno for r in records] == [level]
    assert records[0].getMessage().endswith(LIBRARY_BUG)


def test_conversion_fails_the_rows_of_unreadable_symbols_under_raise(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, FOREX, "C", unreadable_symbols=OnError.RAISE
    )
    converted = plan.convert(frame(FOREX_ROWS, "forex/C"), {}, arrived=arrivals(FOREX_ROWS))
    assert "RawSymbol" not in converted.batch.column_names
    assert converted.batch.column("Symbol").to_pylist() == ["C:EURUSD"]
    message = converted.unreadable
    assert "test.forex: Massive sent the symbol 'nonsense pair'" in message
    assert "MassiveConfig(unreadable_symbols=dm.OnError.LOG)" in message
    assert message.endswith(LIBRARY_BUG)
    assert converted.null_symbols is False


def test_conversion_drops_rows_with_a_bad_time_and_nulls_other_bad_timestamps(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE
    )
    rows = [
        trade("AAPL"),
        trade("MSFT", SipTimestamp=5),
        trade("IBM", TrfTimestamp=2**62),
        trade("NVDA", SipTimestamp=7),
    ]
    converted = plan.convert(frame(rows), {}, arrived=arrivals(rows))
    batch = converted.batch
    # The rows whose time fails are dropped; IBM's odd TRF time only reads as null.
    assert batch.column("Symbol").to_pylist() == ["AAPL", "IBM"]
    assert batch.column("TrfTimestamp").to_pylist() == [None, None]
    assert (converted.symbols, converted.null_symbols) == (frozenset({"AAPL", "IBM"}), False)
    assert converted.unreadable == ""
    assert converted.drift == DriftReport(unconvertible=frozenset({"trft"}), dropped_rows=2)
    assert converted.drift.dropped_reason == (
        "timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100"
    )
    assert converted.drift.samples["trft"] == DriftSample(None, str(2**62))
    assert batch.column("ParticipantTimestamp").to_pylist() == [None, None]  # 0 is absent


def test_a_size_beyond_2_53_is_served_rounded_and_reported_as_drift(
    massive_session: dm.MassiveSession,
) -> None:
    # The trade's size is a `long` on the channel but a `double` in the dataset.
    catalog = massive_session._core.catalog
    trades = catalog.by_name["stocks.trades"]
    plan = ChannelPlan.build(catalog, trades, "T", unreadable_symbols=OnError.RAISE)
    rows = [trade("AAPL", Size=2**53 + 1), trade("MSFT", Size=2**53)]
    converted = plan.convert(frame(rows), {}, arrived=arrivals(rows))
    assert converted.batch.column("Size").to_pylist() == [float(2**53), float(2**53)]
    assert converted.drift == DriftReport(rounded=frozenset({"s"}))
    assert converted.drift.samples["s"] == DriftSample(None, str(2**53 + 1))


def test_rows_convert_as_the_same_rows_keyed_by_column(massive_session: dm.MassiveSession) -> None:
    # The rows a flush gathers, tuples of the source's fields, become the wire table Arrow
    # builds from the same rows keyed by column, however the rows were split into frames.
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE
    )
    rows = [
        trade("AAPL", 1.5),
        trade("MSFT", 2.0, Conditions=None, TrfId=7, TrfTimestamp=1_700_000_000_001),
        trade("IBM", 3.25, Conditions=[], Id="x", DecimalSize=None),
    ]
    reference = pa.Table.from_pylist(rows, schema=plan.wire_schema)
    batch = plan.convert(frame(rows), {}, arrived=arrivals(rows)).batch
    for name, column in [
        ("Price", "Price"),
        ("Size", "Size"),
        ("Conditions", "Conditions"),
        ("Exchange", "Exchange"),
        ("Id", "Id"),
        ("TrfId", "TrfId"),
    ]:
        assert batch.column(name).to_pylist() == reference.column(column).to_pylist(), name
    assert plan.convert([], {}, arrived=[]).batch.num_rows == 0


def test_a_column_the_channel_lacks_is_null(massive_session: dm.MassiveSession) -> None:
    # A dataset in a kind has the kind's columns, some of which another member's source carries.
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE
    )
    wider = replace(
        plan,
        columns=(*plan.columns, (ColumnType.STRING, "Venue", None, None)),
        arrow_schema=plan.arrow_schema.append(pa.field("Venue", pa.string())),
    )
    batch = wider.convert(frame([trade("AAPL")]), {}, arrived=[ARRIVED]).batch
    assert batch.column("Venue").to_pylist() == [None]


# --- StreamSink --------------------------------------------------------------------------------


def make_sink(
    core: SessionCore,
    symbols: NormalizedSymbols,
    plan: ChannelPlan | None = None,
    *,
    stream_id: int = 1,
    spec: DatasetSpec = TRADES,
) -> tuple[StreamSink, Table]:
    # A trades stream, or one of `spec`; streams given one `plan` share each flush's batch, as
    # one dataset's do.
    if plan is None:
        plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(spec, raw_symbol=False),
        symbols,
        core=core,
        stream_id=stream_id,
        host="socket.test",
        evidence="test host",
        recency=Recency.REALTIME,
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    opened.sink.publish_held()  # as the verb that opens a stream does once its result exists
    return opened.sink, opened.handle.blink


def test_a_new_sink_holds_its_rows_until_its_first_result_is_built(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    # Rows routed before the first result exists are held, then added in order before any
    # later row, so a result built after the stream subscribed still sees them.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES, raw_symbol=False),
        ("AAPL",),
        core=core,
        stream_id=1,
        host="socket.test",
        evidence="test host",
        recency=Recency.REALTIME,
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    sink = opened.sink
    assert sink.publish(shared_batch(plan, [trade("AAPL", 1.0)])) == 1  # counted, and held
    shown = blink_to_append_only(opened.handle.blink)  # the first result
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.stream"):
        sink.publish_held()
    assert "adding the 1 rows it received while it opened" in caplog.text
    sink.publish(shared_batch(plan, [trade("AAPL", 2.0)]))
    sink.publish_held()  # idempotent: nothing is held any more
    wait_for(lambda: shown.size == 2)
    assert [row[0] for row in shown.iter_tuple(cols="Price")] == [1.0, 2.0]
    release_scope(core.exec_ctx, opened.handle.scope)


def test_a_failed_sink_drops_the_rows_it_held(massive_session: dm.MassiveSession) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES, raw_symbol=False),
        ("AAPL",),
        core=core,
        stream_id=1,
        host="socket.test",
        evidence="test host",
        recency=Recency.REALTIME,
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    shown = blink_to_append_only(opened.handle.blink)
    opened.sink.publish(shared_batch(plan, [trade("AAPL")]))
    opened.sink.failed = True  # as `fail` leaves it
    opened.sink.publish_held()
    cycle = update_cycle(core.exec_ctx)
    wait_for(lambda: update_cycle(core.exec_ctx) >= cycle + 2)  # a cycle ran after the add
    assert shown.size == 0
    release_scope(core.exec_ctx, opened.handle.scope)


def shared_batch(
    plan: ChannelPlan,
    rows: list[dict[str, object]],
    *,
    splits: bool = True,
    source_key: str = "stocks/T",
) -> stream.SharedBatch:
    # A flush's batch of the rows, as the flusher shares it among its streams.
    converted = plan.convert(frame(rows, source_key), {}, arrived=arrivals(rows))
    return stream.SharedBatch(to_table(converted.batch), converted, splits=splits)


def test_a_sink_selects_its_rows_without_work_per_row(massive_session: dm.MassiveSession) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    batch = shared_batch(plan, [trade("AAPL"), trade("MSFT"), trade("IBM")])
    every, _ = make_sink(core, ALL)
    assert every._select(batch) is batch.table
    assert every.receives_all
    disjoint, _ = make_sink(core, ("SPY",))
    assert disjoint._select(batch) is None  # no filter: it holds no batch symbol
    superset, _ = make_sink(core, ("AAPL", "IBM", "MSFT", "SPY"))
    assert superset._select(batch) is batch.table  # no filter: it holds them all
    assert batch._partition is None  # no stream so far needed one symbol's rows
    some, _ = make_sink(core, ("AAPL", "MSFT", "SPY"))
    selected = some._select(batch)  # two of the batch's three symbols: a filter
    assert selected is not None
    assert selected.size == 2
    cached = some._filter
    assert some._select(batch) is not None
    assert some._filter is cached  # the symbols did not change: the filter is reused
    assert some.holds("AAPL")
    assert not some.receives_all
    some.set_symbols(frozenset({"AAPL", "MSFT", "IBM"}))
    assert some._select(batch) is batch.table
    some.set_symbols(frozenset())
    assert some._select(batch) is None
    # A session that keeps unreadable forex pairs (`LOG`) gives their rows a null Symbol: a
    # stream that holds every readable symbol of the batch still needs the filter to leave
    # them out.
    warned = ChannelPlan.build(core.catalog, FOREX, "C", unreadable_symbols=OnError.LOG)
    nameless = shared_batch(warned, FOREX_ROWS, source_key="forex/C")
    assert nameless.converted.null_symbols
    some.set_symbols(frozenset({"C:EURUSD"}))
    filtered = some._select(nameless)
    assert filtered is not None
    assert filtered.size == 1


def test_streams_of_one_batch_symbol_share_one_partition(
    massive_session: dm.MassiveSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # A stream that holds one of the batch's symbols reads that symbol's rows from a partition
    # of the batch built once per flush, not a filter of its own; arrival order is kept.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    rows = [trade("AAPL", 1), trade("MSFT", 2), trade("AAPL", 3), trade("not a symbol", 4)]
    batch = shared_batch(plan, rows)
    aapl, _ = make_sink(core, ("AAPL", "SPY"))  # one batch symbol among others it holds
    msft, _ = make_sink(core, ("MSFT",))
    aapl_rows = aapl._select(batch)
    partition = batch._partition
    assert partition is not None
    msft_rows = msft._select(batch)
    assert batch._partition is partition  # the second stream reuses the first one's partition
    assert aapl._filter is None
    assert msft._filter is None
    assert aapl_rows is not None
    assert msft_rows is not None
    assert [(r["Symbol"], r["Price"]) for r in dh_to_pylist(aapl_rows)] == [
        ("AAPL", 1.0),
        ("AAPL", 3.0),
    ]
    assert [(r["Symbol"], r["Price"]) for r in dh_to_pylist(msft_rows)] == [("MSFT", 2.0)]


def test_a_stream_of_one_batch_symbol_filters_when_the_batch_is_not_split(
    massive_session: dm.MassiveSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # Too few streams take one symbol each to pay for a partition: the stream filters.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    batch = shared_batch(plan, [trade("AAPL", 1), trade("MSFT", 2), trade("AAPL", 3)], splits=False)
    aapl, _ = make_sink(core, ("AAPL", "SPY"))
    rows = aapl._select(batch)
    assert batch._partition is None
    assert aapl._filter is not None
    assert rows is not None
    assert [r["Price"] for r in dh_to_pylist(rows)] == [1.0, 3.0]


def test_a_stream_takes_one_symbol_only_when_it_holds_one_of_several(
    massive_session: dm.MassiveSession,
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    two = shared_batch(plan, [trade("AAPL"), trade("MSFT")]).converted
    one = shared_batch(plan, [trade("AAPL")]).converted
    assert make_sink(core, ("AAPL", "SPY"))[0].takes_one_symbol(two)
    assert not make_sink(core, ALL)[0].takes_one_symbol(two)
    assert not make_sink(core, ("AAPL", "MSFT"))[0].takes_one_symbol(two)
    assert not make_sink(core, ("SPY",))[0].takes_one_symbol(two)
    assert not make_sink(core, ("AAPL",))[0].takes_one_symbol(one)  # the whole batch
    warned = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.LOG)
    nameless = shared_batch(warned, [trade("AAPL"), trade("not a symbol")]).converted
    assert make_sink(core, ("AAPL",))[0].takes_one_symbol(nameless)  # leaves the null rows


@pytest.mark.parametrize(
    ("streams", "rows", "splits"),
    [(3, 3, True), (2, 3, False), (3, 2, False)],
    ids=["enough", "too-few-streams", "too-few-rows"],
)
def test_a_flush_splits_its_batch_only_for_enough_streams_and_rows(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    streams: int,
    rows: int,
    splits: bool,
) -> None:
    # With the thresholds lowered to three streams and three rows: a batch is split only when
    # it is large enough and enough streams take one of its symbols each.
    monkeypatch.setattr(stream, "SPLIT_MIN_STREAMS", 3)
    monkeypatch.setattr(stream, "SPLIT_MIN_ROWS", 3)
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    symbols = [f"S{i}" for i in range(rows)]
    for symbol in symbols[:streams]:
        flusher.routes.add("T", make_sink(core, (symbol, "SPY"), plan)[0])
    flusher.routes.add("T", make_sink(core, ALL, plan)[0])
    made: list[stream.SharedBatch] = []
    publish = StreamSink.publish

    def recorded(sink: StreamSink, batch: stream.SharedBatch) -> int:
        made.append(batch)
        return publish(sink, batch)

    monkeypatch.setattr(StreamSink, "publish", recorded)
    flusher.rows("T", frame([trade(symbol) for symbol in symbols]), generation=1)
    flusher.flush()
    assert {batch.splits for batch in made} == {splits}


def test_a_failed_sink_publishes_nothing_and_fails_once(
    massive_session: dm.MassiveSession,
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    batch = shared_batch(plan, [trade("AAPL")])
    sink, _ = make_sink(core, ALL)
    assert sink.publish(batch) == 1
    assert sink.fail(MassiveError("refused"))
    assert not sink.fail(MassiveError("again"))
    assert sink.publish(batch) == 0
    assert sink.rows_published == 1


def test_one_failure_of_many_streams_is_logged_once_at_the_level_of_its_cause(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core
    first, _ = make_sink(core, ALL)
    second, _ = make_sink(core, ALL)
    refused = MassiveError("socket.test/stocks refused T.*")
    with caplog.at_level(logging.DEBUG, logger=stream.__name__):
        assert first.fail(refused)
        assert second.fail(refused)
    ours = [(r.levelno, r.getMessage()) for r in caplog.records if "failed:" in r.getMessage()]
    # An expected failure: one WARNING for its cause, the other stream's at DEBUG.
    assert ours == [
        (logging.WARNING, "test.trades stream 1 failed: socket.test/stocks refused T.*"),
        (logging.DEBUG, "test.trades stream 1 failed: socket.test/stocks refused T.*"),
    ]


def test_a_symbol_table_failure_is_the_user_s_defect_logged_at_error(
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    with caplog.at_level(logging.WARNING, logger=stream.__name__):
        sink.fail_later("the symbol table failed upstream: boom; fix that table")
        drain()
    [record] = caplog.records
    assert (record.levelno, record.getMessage()) == (
        logging.ERROR,
        "test.trades stream 1 failed: the symbol table failed upstream: boom; fix that table",
    )
    assert LIBRARY_BUG not in record.getMessage()  # the user's to fix, not a bug report


def test_a_sink_whose_failure_is_refused_publishes_nothing_more(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Deephaven refuses the failure: the row reads FAILED, so the stream stops publishing and
    # is not failed again.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    batch = shared_batch(plan, [trade("AAPL")])
    sink, _ = make_sink(core, ALL)

    def refused(_: Exception) -> None:
        raise RuntimeError("refused")

    monkeypatch.setattr(sink, "publisher", types.SimpleNamespace(publish_failure=refused))
    with pytest.raises(RuntimeError, match="refused"):
        sink.fail(MassiveError("entitlement"))
    assert sink.failed
    assert sink.publish(batch) == 0
    assert not sink.fail(MassiveError("again"))


def test_open_stream_tables_releases_its_scope_on_failure(
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_input_table: Callable[..., InputTable],
) -> None:
    # The watch fails after the publisher exists: releasing the scope shuts the blink table down.
    shut_down = threading.Event()

    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no watch")

    monkeypatch.setattr(stream, "SymbolWatch", broken)
    core = massive_session._core
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        open_stream_tables(
            ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE),
            core.catalog.dataset_schema(TRADES, raw_symbol=False),
            SymbolTable(watchlist),
            core=core,
            stream_id=1,
            host="socket.test",
            evidence="test host",
            recency=Recency.REALTIME,
            path="/stocks",
            on_shutdown=shut_down.set,
            on_symbols=lambda *_: None,
        )
    assert shut_down.wait(TIMEOUT)


def test_open_stream_tables_releases_its_scope_when_interrupted_at_the_end(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An interrupt after every table exists, as the handle is built, still releases the scope.
    shut_down = threading.Event()

    def interrupted(*_: object, **__: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(stream, "StreamHandle", interrupted)
    core = massive_session._core
    with pytest.raises(KeyboardInterrupt):
        open_stream_tables(
            ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE),
            core.catalog.dataset_schema(TRADES),
            ALL,
            core=core,
            stream_id=1,
            host="socket.test",
            evidence="test host",
            recency=Recency.REALTIME,
            path="/stocks",
            on_shutdown=shut_down.set,
            on_symbols=lambda *_: None,
        )
    assert shut_down.wait(TIMEOUT)


def test_a_reactive_stream_starts_with_no_symbols(
    massive_session: dm.MassiveSession, dh_input_table: Callable[..., InputTable]
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    sink, _ = make_sink(massive_session._core, SymbolTable(watchlist))
    assert not sink.holds("AAPL")
    assert not sink.receives_all


def test_a_reactive_streams_handle_holds_its_watch(
    massive_session: dm.MassiveSession, dh_input_table: Callable[..., InputTable]
) -> None:
    # Deephaven holds a table's listeners only weakly: the session's record of the stream holds
    # the watch through the handle, whatever else keeps it reachable, and stops it through it.
    core = massive_session._core
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    opened = open_stream_tables(
        ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE),
        core.catalog.dataset_schema(TRADES, raw_symbol=False),
        SymbolTable(watchlist),
        core=core,
        stream_id=1,
        host="socket.test",
        evidence="test host",
        recency=Recency.REALTIME,
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
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The listener reports on an update-graph thread, so the failure is only queued there.
    # The watch logs a failed symbol table only at DEBUG, so the stream's failure is what
    # reports it, at ERROR: the table is the user's to fix.
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
    with caplog.at_level(logging.DEBUG, logger=stream.__name__):
        gate.set()
        drain()
    assert failures == ["the symbol table failed, so the result can no longer follow it"]
    reported = (
        "test.trades stream 1 failed: the symbol table failed, so the result can no longer "
        "follow it"
    )
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [(logging.ERROR, reported)]


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
    """A schedule calendar that moves one symbol's rows to a fixed date and misses another's.

    A missed row gets a null date, as the real calendar gives it.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[list[str | None], list[int | None], list[str | None]]] = []
        self.missing = {"NQM5"}

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        names = symbols.to_pylist()
        self.calls.append((names, instants.to_pylist(), dates.to_pylist()))
        fixed = [
            "2025-03-11" if name == "ESM5" else None if name in self.missing else day
            for name, day in zip(names, dates.to_pylist(), strict=True)
        ]
        return pa.array(fixed, pa.string())


def test_a_futures_row_gets_the_rule_session_corrected_by_the_schedules(
    massive_session: dm.MassiveSession,
) -> None:
    spec = massive_session._core.catalog.by_name["futures.trades"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "T", unreadable_symbols=OnError.RAISE
    )
    rows = [
        futures_trade("GCJ5", BEFORE_ROLL_MS),
        futures_trade("GCJ5", AFTER_ROLL_MS),
        futures_trade("ESM5", BEFORE_ROLL_MS),
        futures_trade("NQM5", BEFORE_ROLL_MS),
    ]
    sessions = FakeSessionDates()
    converted = plan.convert(frame(rows, "futures/T"), {}, arrived=[ARRIVED] * 4, sessions=sessions)
    assert converted.batch.column("Date").to_pylist() == [
        "2025-03-07",
        "2025-03-10",  # after Friday's roll: Monday's session
        "2025-03-11",  # moved by the schedules
        None,  # missed by the schedules: the rule's date may be wrong
    ]
    assert sessions.calls[0][2] == ["2025-03-07", "2025-03-10", "2025-03-07", "2025-03-07"]
    assert converted.batch.column("Timestamp").null_count == 0  # every instant is kept
    # Without a calendar, the rule's dates stand.
    alone = plan.convert(frame(rows, "futures/T"), {}, arrived=[ARRIVED] * 4)
    assert alone.batch.column("Date").to_pylist()[2] == "2025-03-07"


def test_a_streamed_bar_ends_its_length_after_its_start(
    massive_session: dm.MassiveSession,
) -> None:
    spec = massive_session._core.catalog.by_name["stocks.bars"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "AM", unreadable_symbols=OnError.RAISE
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
    batch = plan.convert(frame([row], "stocks/AM"), {}, arrived=[ARRIVED]).batch
    assert batch.column("TimestampStart").cast(pa.int64()).to_pylist() == [start * 10**6]
    assert batch.column("TimestampEnd").cast(pa.int64()).to_pylist() == [start * 10**6 + MINUTE_NS]
    assert batch.column("Date").to_pylist() == ["2025-03-03"]
    assert "EndTimestamp" not in batch.schema.names


def test_a_futures_second_bar_ends_at_the_later_end(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    spec = massive_session._core.catalog.by_name["futures.bars"]
    plan = ChannelPlan.build(
        massive_session._core.catalog, spec, "A", unreadable_symbols=OnError.RAISE
    )
    start = BEFORE_ROLL_MS
    rows = [futures_bar(start, start + 60_000), futures_bar(start, start + 1_000)]
    with caplog.at_level(logging.INFO):
        batch = plan.convert(frame(rows, "futures/A"), {}, arrived=[ARRIVED] * 2).batch
    ends = batch.column("TimestampEnd").cast(pa.int64()).to_pylist()
    assert ends == [(start + 60_000) * 10**6, (start + 1_000) * 10**6]
    # The later end differs from Massive's own `e`, which the library never changes unseen.
    [notice] = [r for r in caplog.records if "is not its start plus one second" in r.getMessage()]
    assert notice.levelno == logging.WARNING


def test_every_row_takes_the_arrival_of_its_frame(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    sink, blink = make_sink(core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", sink)
    before = time.time_ns()
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    buffer = flusher.routes.buffer("T")
    assert buffer is not None
    (stamped, generation, rows) = buffer.get_nowait()  # the frame, its arrival and socket
    assert before <= stamped <= time.time_ns()
    assert rows == frame([trade("AAPL")])
    assert generation == 1
    # Two frames of one flush: each row takes its own frame's arrival.
    buffer.put((111, 1, frame([trade("AAPL"), trade("MSFT")])))
    buffer.put((222, 1, frame([trade("IBM")])))
    flusher.flush()
    wait_for(lambda: shown.size == 3)
    inserted = to_arrow_column(shown, "TimestampInsert")
    assert inserted == [111, 111, 222]


def test_a_session_miss_leaves_the_streams_error_alone_and_publishes_its_rows(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    spec = core.catalog.by_name["futures.trades"]
    plan = ChannelPlan.build(core.catalog, spec, "T", unreadable_symbols=OnError.RAISE)
    updates: list[dict[int, StreamUpdate]] = []
    monkeypatch.setattr(core, "update_streams", updates.append)
    calendar = FakeSessionDates()
    flusher = Flusher(core.exec_ctx, "test-flush", interval=3600.0, sessions=calendar)
    sinks = []
    for stream_id, wanted in ((1, ("NQM5",)), (2, ("ESM5",))):
        opened = open_stream_tables(
            plan,
            core.catalog.dataset_schema(spec, raw_symbol=False),
            wanted,
            core=core,
            stream_id=stream_id,
            host="socket.test",
            evidence="test host",
            recency=Recency.REALTIME,
            path="/futures",
            on_shutdown=lambda: None,
            on_symbols=lambda *_: None,
        )
        opened.sink.publish_held()
        flusher.routes.add("T", opened.sink)
        sinks.append(opened.sink)
    both = [futures_trade("NQM5", BEFORE_ROLL_MS), futures_trade("ESM5", BEFORE_ROLL_MS)]
    flusher.rows("T", frame(both, "futures/T"), generation=1)
    flusher.flush()
    calendar.missing.clear()
    flusher.rows("T", frame(both, "futures/T"), generation=1)
    flusher.flush()
    flusher.stop()
    # A miss only leaves its rows without a date: the stream is healthy, so its `Error` is
    # never written, and every row is published.
    assert not any(sink.failed for sink in sinks)
    assert updates[:2] == [
        {1: StreamUpdate(rows_published=1), 2: StreamUpdate(rows_published=1)},
        {1: StreamUpdate(rows_published=2), 2: StreamUpdate(rows_published=2)},
    ]


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
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), generation=1)
    flusher.rows("Q", frame([trade("AAPL")]), generation=1)  # no stream ever read Q: dropped
    flusher.flush()
    rows_when(shown[0], lambda rows: len(rows) == 1)
    rows_when(shown[1], lambda rows: len(rows) == 2)
    assert (aapl.rows_published, every.rows_published, idle.rows_published) == (1, 2, 0)


def test_each_stream_records_the_socket_its_newest_rows_came_from(
    massive_session: dm.MassiveSession,
) -> None:
    # One flush spans a reconnect: MSFT's only row came on the dropped socket, AAPL's newest
    # on the new one. Only a stream's first rows of a newer socket call the connection back.
    calls: list[None] = []
    flusher = Flusher(
        massive_session._core.exec_ctx,
        "test-flush",
        interval=3600.0,
        on_new_generation=lambda: calls.append(None),
    )
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",))
    msft, _ = make_sink(core, ("MSFT",))
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), generation=1)
    flusher.rows("T", frame([trade("AAPL")]), generation=2)
    flusher.flush()
    assert (aapl.rows_generation, msft.rows_generation) == (2, 1)
    assert (aapl.rows_published, msft.rows_published) == (2, 1)
    assert len(calls) == 1
    flusher.rows("T", frame([trade("AAPL")]), generation=2)
    flusher.flush()
    assert len(calls) == 1  # no stream got a newer socket's rows
    flusher.stop()


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
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
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
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    flusher.flush()
    assert sink.failed
    assert sink.rows_published == 0  # a failed stream gets no rows


def test_a_batch_whose_every_row_was_dropped_publishes_nothing_and_fails_nothing(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",))
    msft, _ = make_sink(core, ("MSFT",))
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)]), generation=1)
    flusher.flush()
    assert not aapl.failed
    assert not msft.failed
    assert aapl.rows_published == msft.rows_published == 0


def test_an_all_stream_survives_rows_whose_time_or_value_is_odd(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    caplog: pytest.LogCaptureFixture,
    *,
    rows_when: RowsWhen,
) -> None:
    core = massive_session._core
    every, blink = make_sink(core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", every)
    rows = [trade("AAPL"), trade("MSFT", SipTimestamp=5), trade("IBM", TrfTimestamp=2**62)]
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.diagnostics"):
        flusher.rows("T", frame(rows), generation=1)
        # The client dropped a row it could not route and counted it in the frame's drift.
        lost = DriftReport(dropped_rows=1, dropped_reason="a T row from h/stocks: no Ticker")
        flusher.drift("T", lost, where="wss://h/stocks channel T")
        flusher.flush()
        flusher.rows("T", frame([trade("NVDA")]), generation=1)
        flusher.flush()
    assert not every.failed
    assert every.rows_published == 3
    published = rows_when(shown, lambda found: len(found) == 3)
    assert [(r["Symbol"], r["TrfTimestamp"]) for r in published] == [
        ("AAPL", None),
        ("IBM", None),
        ("NVDA", None),
    ]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == [
        (
            "test.trades: WS field 'trft' held a value that does not fit its type; it reads as "
            f"null (schema drift) at test-flush channel T; first value {2**62}"
        ),
        (
            "test.trades: dropped 1 WS row(s) whose key cannot be read, the first at test-flush "
            "channel T: timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 "
            "to 2100; such rows are dropped and counted: the running count is logged at DEBUG "
            "and shown in the SchemaDrift of each live table that lost them"
        ),
    ]


def test_a_run_of_dropped_rows_fails_the_datasets_streams_on_the_channel(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    # One odd row fails nothing; a run of them with nothing published means the format changed.
    core = massive_session._core
    every, _ = make_sink(core, ALL)
    aapl, _ = make_sink(core, ("AAPL",))
    flusher.routes.add("T", every)
    flusher.routes.add("T", aapl)
    half = stream.DROPPED_RUN_LIMIT // 2
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)] * half), generation=1)
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL")]), generation=1)  # a published row starts the run again
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)] * half), generation=1)
    flusher.flush()
    assert (every.failed, aapl.failed) == (False, False)
    lost = DriftReport(dropped_rows=half, dropped_reason="a T row from h/stocks: no Ticker")
    with caplog.at_level(logging.ERROR):
        flusher.drift("T", lost, where="wss://h/stocks channel T")  # no rows, only drops
        flusher.flush()
    assert every.failed
    assert aapl.failed
    error = next(r.getMessage() for r in caplog.records if "were dropped" in r.getMessage())
    assert error.startswith(
        f"test.trades stream 1 failed: test.trades: the last {2 * half} rows of channel T were "
        "dropped because a key could not be read, and none was published, so Massive's format "
        "has most likely changed; the first: timestamp unit mismatch for test.trades.Timestamp"
    )
    assert flusher._runs == {}


def test_a_symbol_whose_every_row_is_dropped_fails_its_streams_while_others_publish(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    # AAPL publishes in every flush, so the dataset's run never grows; ILLQ's rows are all
    # dropped, and the streams that name ILLQ must not freeze without an error.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    every, _ = make_sink(core, ALL, plan)
    illq, _ = make_sink(core, ("ILLQ",), plan)
    aapl, _ = make_sink(core, ("AAPL",), plan)
    for sink in (every, illq, aapl):
        flusher.routes.add("T", sink)
    half = stream.DROPPED_RUN_LIMIT // 2
    flusher.rows("T", frame([trade("AAPL"), *[trade("ILLQ", SipTimestamp=5)] * half]), generation=1)
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    # The client dropped ILLQ rows too, counted by their canonical symbol.
    lost = DriftReport(dropped_rows=half - 1, dropped_reason="a T row for ILLQ: no Price")
    flusher.drift("T", lost, where="wss://h/stocks channel T", dropped={"ILLQ": half - 1})
    flusher.flush()
    assert (every.failed, illq.failed, aapl.failed) == (False, False, False)
    with caplog.at_level(logging.ERROR):
        flusher.rows("T", frame([trade("AAPL"), trade("ILLQ", SipTimestamp=5)]), generation=1)
        flusher.flush()
    assert (every.failed, illq.failed, aapl.failed) == (False, True, False)
    error = next(r.getMessage() for r in caplog.records if "rows of ILLQ" in r.getMessage())
    assert f"the last {2 * half} rows of ILLQ on channel T were dropped" in error
    assert "the first: timestamp unit mismatch for test.trades.Timestamp" in error


def test_an_all_stream_marks_a_starved_symbol_stale_until_it_publishes_again(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A dm.ALL stream is never failed for one symbol, but its last values for the symbol
    # freeze: its Error says so, without failing it, and clears once the symbol is back.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    every, _ = make_sink(core, ALL, plan, stream_id=7)
    flusher.routes.add("T", every)
    written: list[StreamUpdate] = []
    monkeypatch.setattr(core, "update_streams", lambda changes: written.extend(changes.values()))
    reason = "timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100"
    limit = stream.DROPPED_RUN_LIMIT
    for symbol in ("ILLQ", "AAA", "BBB", "CCC"):
        flusher.rows(
            "T", frame([trade("AAPL"), *[trade(symbol, SipTimestamp=5)] * limit]), generation=1
        )
        flusher.flush()
    assert not every.failed

    def stale(update: StreamUpdate) -> str | None:
        # The stale-symbol part an update sets; None when it sets or clears no such part.
        return (update.notes or {}).get(NotePart.STALE_SYMBOLS)

    errors = [stale(u) for u in written if stale(u) is not None]
    assert errors == [
        f"rows for ILLQ are being dropped ({reason}); its last values may be stale",
        f"rows for ILLQ, AAA are being dropped ({reason}); their last values may be stale",
        f"rows for ILLQ, AAA, BBB are being dropped ({reason}); their last values may be stale",
        (
            f"rows for ILLQ, AAA, BBB and 1 more are being dropped ({reason}); their last values "
            "may be stale"
        ),
    ]
    written.clear()
    flusher.rows("T", frame([trade(symbol) for symbol in ("ILLQ", "AAA", "BBB")]), generation=1)
    flusher.flush()
    assert [stale(u) for u in written] == [
        f"rows for CCC are being dropped ({reason}); its last values may be stale"
    ]
    written.clear()
    flusher.rows("T", frame([trade("CCC")]), generation=1)
    flusher.flush()
    assert [u.notes for u in written] == [{NotePart.STALE_SYMBOLS: None}]
    written.clear()
    flusher.rows("T", frame([trade("CCC")]), generation=1)
    flusher.flush()
    assert [u.notes for u in written] == [None]  # nothing left to clear
    flusher.routes.remove(7)
    flusher.flush()
    assert (flusher._stale, flusher._noted) == ({}, {})


def test_a_stale_symbol_warns_once_when_it_starves_and_logs_its_return_at_info(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    for stream_id in (7, 8):
        flusher.routes.add("T", make_sink(core, ALL, plan, stream_id=stream_id)[0])
    reason = "timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100"
    limit = stream.DROPPED_RUN_LIMIT
    with caplog.at_level(logging.INFO, logger=stream.__name__):
        # Two runs of ILLQ: the second finds it stale already, so it warns once.
        for _ in range(2):
            flusher.rows(
                "T", frame([trade("AAPL"), *[trade("ILLQ", SipTimestamp=5)] * limit]), generation=1
            )
            flusher.flush()
        flusher.rows("T", frame([trade("ILLQ")]), generation=1)
        flusher.flush()
    mine = [r for r in caplog.records if r.name == stream.__name__]
    assert [(r.levelno, r.getMessage()) for r in mine] == [
        (
            logging.WARNING,
            (
                f"test.trades: every recent row of 1 [ILLQ] on channel T was dropped because a key "
                f"could not be read (the first: {reason}), so the dm.ALL streams 2 [7, 8] may be "
                "stale for them; they keep running and name the symbols in their Error until a "
                "row is published"
            ),
        ),
        (
            logging.INFO,
            (
                "test.trades: rows of 1 [ILLQ] on channel T are published again; the dm.ALL "
                "streams no longer name them as stale"
            ),
        ),
    ]


def test_a_stale_symbol_is_not_called_published_when_its_stream_failed_instead(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, FOREX, "C", unreadable_symbols=OnError.RAISE)
    every, _ = make_sink(core, ALL, plan, spec=FOREX)
    flusher.routes.add("C", every)
    limit = stream.DROPPED_RUN_LIMIT
    rows = [quote("EUR/USD"), *[quote("GBP/USD", ParticipantTimestamp=5)] * limit]
    flusher.rows("C", frame(rows, "forex/C"), generation=1)
    flusher.flush()
    with caplog.at_level(logging.INFO, logger=stream.__name__):
        # A pair the library cannot read fails the stream before GBP/USD's row could publish.
        rows = [quote("GBP/USD"), quote("nonsense pair")]
        flusher.rows("C", frame(rows, "forex/C"), generation=1)
        flusher.flush()
    assert every.failed
    assert "published again" not in caplog.text


def test_a_dataset_whose_every_row_is_dropped_fails_its_streams_without_a_stale_warning(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    every, _ = make_sink(core, ALL, plan)
    flusher.routes.add("T", every)
    with caplog.at_level(logging.WARNING, logger=stream.__name__):
        # The symbol's run and the dataset's reach the limit in one flush: the stream fails,
        # so no WARNING says it keeps running.
        flusher.rows(
            "T", frame([trade("ILLQ", SipTimestamp=5)] * stream.DROPPED_RUN_LIMIT), generation=1
        )
        flusher.flush()
    assert every.failed
    assert "may be stale" not in caplog.text


def test_a_flush_merges_its_frames_drift_and_reports_it_outside_the_flush_lock(
    *,
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    flusher.routes.add("T", sink)
    reports: list[tuple[DriftReport, str, bool]] = []

    def report(_: object, drift: DriftReport, *, where: str) -> None:
        reports.append((drift, where, flusher._flush_lock.locked()))

    monkeypatch.setattr(Flusher, "_report_drift", staticmethod(report))
    where = "wss://h/stocks channel T"
    for reason in ("first", "second", "third"):
        flusher.drift("T", DriftReport(dropped_rows=2, dropped_reason=reason), where=where)
    flusher.flush()
    # Three frames' drops are one report, with every row and the first reason, made once
    # the flush lock is free.
    assert reports == [(DriftReport(dropped_rows=6, dropped_reason="first"), where, False)]
    flusher.flush()
    assert len(reports) == 1  # each flush reports its own drift only


def test_a_published_row_of_a_symbol_ends_its_run(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    illq, _ = make_sink(core, ("ILLQ",))
    flusher.routes.add("T", illq)
    flusher.rows("T", frame([trade("ILLQ", SipTimestamp=5)] * 3), generation=1)
    flusher.flush()
    assert flusher._symbol_runs[illq.plan]["ILLQ"][0] == 3
    flusher.rows("T", frame([trade("ILLQ"), trade("ILLQ", SipTimestamp=5)]), generation=1)
    flusher.flush()
    assert flusher._symbol_runs[illq.plan] == {}
    flusher.routes.remove(1)
    flusher.flush()
    assert flusher._symbol_runs == {}


def test_one_flush_of_dropped_rows_at_the_limit_fails_the_streams_by_the_run_rule(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    every, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", every)
    with caplog.at_level(logging.ERROR):
        flusher.rows(
            "T", frame([trade("AAPL", SipTimestamp=5)] * stream.DROPPED_RUN_LIMIT), generation=1
        )
        flusher.flush()
    assert every.failed
    assert "could not publish" not in caplog.text
    assert f"the last {stream.DROPPED_RUN_LIMIT} rows of channel T were dropped" in caplog.text


def test_a_flush_forgets_the_run_of_a_dataset_no_longer_routed(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)]), generation=1)
    flusher.flush()
    assert flusher._runs[sink.plan][0] == 1
    flusher.routes.remove(1)
    flusher.flush()
    assert flusher._runs == {}


def test_a_row_failure_that_is_refused_spares_the_other_streams(
    *,
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Deephaven refuses one stream's failure for an unreadable symbol, which fails every stream
    # of the dataset on the channel: the other stream is failed all the same.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, FOREX, "C", unreadable_symbols=OnError.RAISE)

    def sink(stream_id: int, symbol: str) -> StreamSink:
        # The two streams share the dataset's plan, as the streams of one connection do.
        opened = open_stream_tables(
            plan,
            core.catalog.dataset_schema(FOREX, raw_symbol=False),
            (symbol,),
            core=core,
            stream_id=stream_id,
            host="socket.test",
            evidence="test host",
            recency=Recency.REALTIME,
            path="/forex",
            on_shutdown=lambda: None,
            on_symbols=lambda *_: None,
        )
        opened.sink.publish_held()
        return opened.sink

    aapl, msft = sink(1, "C:EURUSD"), sink(2, "C:GBPUSD")

    def refused(_: Exception) -> None:
        raise RuntimeError("refused")

    monkeypatch.setattr(aapl, "publisher", types.SimpleNamespace(publish_failure=refused))
    flusher.routes.add("C", aapl)
    flusher.routes.add("C", msft)
    flusher.rows("C", frame([quote("!!"), quote("GBP/USD")], "forex/C"), generation=1)
    flusher.flush()
    assert aapl.failed
    assert msft.failed
    assert msft.rows_published == 0
    assert "test-flush: could not fail stream 1" in caplog.text


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
    flusher.rows("T", frame([trade("AAPL", Price="not a number")]), generation=1)
    flusher.rows("Q", frame([trade("AAPL")]), generation=1)
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
        flusher.drift("T", DriftReport(extra=frozenset({"zz"})), where="wss://h/stocks channel T")
        flusher.flush()
    assert "test.trades: WS field 'zz' is not in the schema" in caplog.text


def test_routes_forget_a_stream_once(flusher: Flusher, massive_session: dm.MassiveSession) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    assert flusher.routes.remove(1) is sink
    assert flusher.routes.remove(1) is None
    assert flusher.routes.sinks() == ()


def test_a_flush_forgets_the_symbols_of_streams_no_longer_routed(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), generation=1)
    flusher.flush()
    assert flusher._symbols == {sink.plan: {"AAPL": "AAPL", "MSFT": "MSFT"}}
    flusher.routes.remove(1)
    flusher.flush()
    assert flusher._symbols == {}


def test_a_full_symbol_cache_starts_over_and_the_rows_still_publish(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caplog: pytest.LogCaptureFixture,
    rows_when: RowsWhen,
) -> None:
    # A `dm.ALL` stream meets new contracts every day: the cache never outgrows its limit.
    monkeypatch.setattr(stream, "SYMBOL_CACHE_LIMIT", 2)
    sink, blink = make_sink(massive_session._core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), generation=1)
    flusher.flush()
    assert flusher._symbols[sink.plan] == {"AAPL": "AAPL", "MSFT": "MSFT"}
    with caplog.at_level(logging.DEBUG, logger=stream.__name__):
        flusher.rows("T", frame([trade("IBM")]), generation=1)
        flusher.flush()
    assert flusher._symbols[sink.plan] == {}
    assert (
        "test.trades: the T symbol cache holds 3 spellings, past its limit of 2; starts over"
        in caplog.text
    )
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    flusher.flush()
    assert flusher._symbols[sink.plan] == {"AAPL": "AAPL"}
    rows = rows_when(shown, lambda rows: len(rows) == 4)
    assert [row["Symbol"] for row in rows] == ["AAPL", "MSFT", "IBM", "AAPL"]


def test_an_unreadable_spelling_is_logged_once_across_a_symbol_cache_reset(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        wire_symbols,
        "_logged_spellings",
        BoundedLog(logging.getLogger("t"), limit=5, level=logging.ERROR),
    )
    monkeypatch.setattr(stream, "SYMBOL_CACHE_LIMIT", 1)
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, FOREX, "C", unreadable_symbols=OnError.LOG)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(FOREX, raw_symbol=True),
        ALL,
        core=core,
        stream_id=1,
        host="socket.test",
        path="/forex",
        evidence="test host",
        recency=Recency.REALTIME,
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    opened.sink.publish_held()
    flusher.routes.add("C", opened.sink)
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            flusher.rows("C", frame(FOREX_ROWS, "forex/C"), generation=1)
            flusher.flush()
            assert flusher._symbols[plan] == {}  # two spellings pass the limit of one
    assert not opened.sink.failed
    assert opened.sink.rows_published == 4
    assert caplog.text.count("Massive sent the symbol 'nonsense pair'") == 1


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
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    flusher.drift(
        "T", DriftReport(extra=frozenset({"zz"})), where="wss://socket.test/stocks channel T"
    )
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
        flusher.rows("T", frame([trade("AAPL")]), generation=1)
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
    plan = None  # read by the flush that forgets the plans no longer routed

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
    # A defect of the library: one ERROR with how to report it, not CRITICAL, and not one per
    # stream it fails.
    died = [r for r in caplog.records if r.getMessage().startswith("the flush thread")]
    assert [(r.levelno, r.getMessage()) for r in died] == [
        (
            logging.ERROR,
            "the flush thread dying-flush died (_Fatal()); this table no longer updates. "
            + LIBRARY_BUG,
        )
    ]
    assert died[0].exc_info is not None
    assert "dying-flush: could not fail stream 2" in caplog.text
    assert [args.exc_type for args in escaped] == [_Fatal]


class _Recording:
    # A routed stream that records the errors it is failed with.
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.plan = None  # read by the flush that forgets the plans no longer routed
        self.errors: list[MassiveError] = []

    def fail(self, error: MassiveError) -> bool:
        self.errors.append(error)
        return True


def test_a_stream_that_cannot_be_failed_costs_the_flush_nothing(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    caplog: pytest.LogCaptureFixture,
    *,
    rows_when: RowsWhen,
) -> None:
    # A queued failure that raises leaves the other failures, every stream's rows, and the
    # row counts of the flush in place.
    core = massive_session._core
    healthy, healthy_blink = make_sink(core, ALL)
    shown = blink_to_append_only(healthy_blink)
    recording = _Recording(3)
    flusher.routes.add("T", healthy)
    flusher.routes.add("Q", cast("StreamSink", _Stubborn()))
    flusher.routes.add("Q", cast("StreamSink", recording))
    flusher.fail([2], MassiveError("refused"))
    flusher.fail([3], MassiveError("also refused"))
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    flusher.flush()
    assert [str(error) for error in recording.errors] == ["also refused"]
    rows_when(shown, lambda rows: len(rows) == 1)
    assert healthy.rows_published == 1
    assert "test-flush: could not fail stream 2 (refused)" in caplog.text


def test_a_drift_report_that_raises_costs_the_flush_nothing(
    *,
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The streams' row counts still reach their session after a drift report fails.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    flusher.routes.add("T", sink)
    reported: list[dict[int, StreamUpdate]] = []
    monkeypatch.setattr(core, "update_streams", reported.append)

    def broken(*_: object, **__: object) -> None:
        raise RuntimeError("no drift")

    monkeypatch.setattr(Flusher, "_report_drift", staticmethod(broken))
    flusher.drift("T", DriftReport(extra=frozenset({"zz"})), where="wss://h/stocks channel T")
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    flusher.flush()
    assert [update[1].rows_published for update in reported] == [1]
    assert "test-flush: could not report the drift of wss://h/stocks channel T" in (caplog.text)


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
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    symbols = [f"S{i}" for i in range(streams)]
    rows = [trade(symbols[i % streams], float(i)) for i in range(rows_per_flush)]
    sinks = [make_sink(core, (symbol,))[0] for symbol in symbols]

    def shared() -> None:
        converted = plan.convert(frame(rows), {}, arrived=arrivals(rows))
        splits = rows_per_flush >= stream.SPLIT_MIN_ROWS and streams >= stream.SPLIT_MIN_STREAMS
        batch = stream.SharedBatch(
            dataset_table(converted.batch, plan.date_columns), converted, splits=splits
        )
        for sink in sinks:
            sink.publish(batch)

    def grouped() -> None:
        buckets: dict[object, list[dict[str, object]]] = {}
        for row in rows:
            buckets.setdefault(row["Ticker"], []).append(row)
        for symbol, sink in zip(symbols, sinks, strict=True):
            own = buckets.get(symbol, [])
            batch = plan.convert(frame(own), {}, arrived=arrivals(own)).batch
            sink.publisher.add(dataset_table(batch, plan.date_columns))

    def stored() -> None:
        dataset_table(
            plan.convert(frame(rows), {}, arrived=arrivals(rows)).batch, plan.date_columns
        )

    def converted_only() -> None:
        to_table(plan.convert(frame(rows), {}, arrived=arrivals(rows)).batch)

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


# The sweep behind `SPLIT_MIN_STREAMS`: each point times 15 repetitions, so the whole sweep
# carries the `benchmark` marker and runs only with -m benchmark.
_SPLIT_POINTS = [
    pytest.param(rows, distinct, streams, id=f"{rows}-{distinct}-{streams}")
    for rows, distinct in ((2_000, 2_000), (5_000, 2_500), (20_000, 1_000), (50_000, 25_000))
    for streams in (1, 4, 8, 16, 32)
]


@pytest.mark.benchmark
@pytest.mark.parametrize(("rows_per_flush", "distinct", "streams"), _SPLIT_POINTS)
def test_benchmark_symbol_split(
    massive_session: dm.MassiveSession,
    capsys: pytest.CaptureFixture[str],
    *,
    rows_per_flush: int,
    distinct: int,
    streams: int,
) -> None:
    # Records, never gates: what streams that each take one symbol of a batch cost when they
    # share a partition of the batch, against a filter each. Each repetition builds a fresh
    # table, as each flush does: Deephaven remembers a partition of a table it has already
    # split, so reusing one table would time only the lookups.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE)
    rows = [trade(f"S{i % distinct}", float(i)) for i in range(rows_per_flush)]
    converted = plan.convert(frame(rows), {}, arrived=arrivals(rows))
    sinks = [make_sink(core, (f"S{i}",))[0] for i in range(streams)]

    def median_ms(*, splits: bool) -> float:
        times = []
        for repetition in range(15):
            batch = stream.SharedBatch(to_table(converted.batch), converted, splits=splits)
            started = time.perf_counter()
            for sink in sinks:
                sink.publish(batch)
            if repetition >= 3:  # the first ones warm the JIT and the filters up
                times.append(time.perf_counter() - started)
        return statistics.median(times) * 1000

    split_ms = median_ms(splits=True)
    filter_ms = median_ms(splits=False)
    with capsys.disabled():
        sys.stderr.write(
            f"\nsymbol split of {rows_per_flush} rows, {distinct} symbols, {streams} streams: "
            f"partition {split_ms:.2f} ms, filters {filter_ms:.2f} ms\n"
        )


def test_a_failed_watchlist_of_a_finished_stream_does_nothing() -> None:
    # The JVM may report a listener's error after the stream's sink is gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    stream._fail_later(cast("weakref.ref[StreamSink]", gone), "late")


def test_skipped_watchlist_rows_reach_the_streams_error(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # What the watch's `on_skipped` does, called here from a Python thread, which line coverage
    # before 3.14 can see: the skipped values become the row's Error, and none clears it.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    reported: list[dict[int, StreamUpdate]] = []
    monkeypatch.setattr(core, "update_streams", lambda changes: reported.append(dict(changes)))
    stream._show_skipped(weakref.ref(sink), ("msft",))
    stream._show_skipped(weakref.ref(sink), ())
    assert reported == [
        {sink.stream_id: StreamUpdate(skipped=skipped_rows_text(["msft"]))},
        {sink.stream_id: StreamUpdate(clear_skipped=True)},
    ]


def test_skipped_watchlist_rows_of_a_finished_stream_do_nothing() -> None:
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    stream._show_skipped(cast("weakref.ref[StreamSink]", gone), ("msft",))


def test_streamed_rows_of_one_day_share_one_stored_date(
    massive_session: dm.MassiveSession, string_objects: Callable[[Table, str], int]
) -> None:
    core = massive_session._core
    spec = core.catalog.by_name["stocks.trades"]
    plan = ChannelPlan.build(core.catalog, spec, "T", unreadable_symbols=OnError.RAISE)
    flusher = Flusher(core.exec_ctx, "test-flush", interval=3600.0)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(spec, raw_symbol=False),
        ALL,
        core=core,
        stream_id=1,
        host="socket.test",
        evidence="test host",
        recency=Recency.REALTIME,
        path="/stocks",
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    shown = blink_to_append_only(opened.handle.blink)
    opened.sink.publish_held()
    flusher.routes.add("T", opened.sink)
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT"), trade("IBM")]), generation=1)
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

    flusher.rows(
        "Q", frame([trade("AAPL")]), generation=1
    )  # no stream ever read Q: dropped, never buffered
    flusher.rows("T", frame([trade("AAPL")] * 4), generation=1)
    assert (flusher.backlog, warnings()) == (4, 0)  # at the level, not above it
    flusher.rows("T", frame([trade("AAPL")]), generation=1)
    # Each buffered frame holds its arrival time with its rows: the backlog counts the rows.
    flusher.rows("T", frame([trade("AAPL")] * 3), generation=1)
    assert (flusher.backlog, warnings()) == (8, 1)  # logged once while it stays above
    flusher.flush()
    assert flusher.backlog == 0  # nothing was dropped: the flush took every row
    flusher.rows("T", frame([trade("AAPL")] * 5), generation=1)
    assert warnings() == 2  # below half the level in between, so it is logged again


def test_the_backlog_warning_waits_until_the_backlog_falls_below_half(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream, "BACKLOG_WARNING_ROWS", 4)
    sink, _ = make_sink(massive_session._core, ("AAPL",))
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL")] * 5), generation=1)
    assert flusher._backlog_warned
    flusher._drained(3)  # as a flush that took three of the five would
    assert flusher._backlog_warned  # two rows are not below half of four
    flusher._drained(1)
    assert not flusher._backlog_warned


@pytest.mark.benchmark
@pytest.mark.parametrize("rows_per_frame", [1, 10, 100])
def test_benchmark_flush_conversion(
    massive_session: dm.MassiveSession, capsys: pytest.CaptureFixture[str], rows_per_frame: int
) -> None:
    # Records, never gates: the flush thread's cost of turning 10,000 rows, decoded from frames
    # of `rows_per_frame` rows each, into one batch of the dataset's schema.
    plan = ChannelPlan.build(
        massive_session._core.catalog, TRADES, "T", unreadable_symbols=OnError.RAISE
    )
    frames = [
        frame([trade(f"S{(f + i) % 50}", float(i)) for i in range(rows_per_frame)])
        for f in range(10_000 // rows_per_frame)
    ]
    rows = [trade("S0")] * sum(map(len, frames))

    def convert() -> None:
        # As a flush gathers the frames' rows.
        plan.convert([row for rows in frames for row in rows], {}, arrived=arrivals(rows))

    seconds = _median_seconds(convert)
    with capsys.disabled():
        sys.stderr.write(
            f"\nflush of {len(rows)} rows from frames of {rows_per_frame}: "
            f"{seconds * 1000:.2f} ms, {seconds * 1e6 / len(rows):.2f} us a row\n"
        )


def test_a_streams_blink_table_is_created_under_the_update_graphs_shared_lock(
    massive_session: dm.MassiveSession, blink_creations: list[bool]
) -> None:
    before = len(blink_creations)
    sink, _blink = make_sink(massive_session._core, ("AAPL",))
    assert blink_creations[before:] == [True]
    sink.fail(MassiveError("done"))
