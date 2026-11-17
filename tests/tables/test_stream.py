"""Tests for the Deephaven side of live streams: channel plans, sinks, the flusher, the fan-out.

No connection is involved: rows are put on a `Flusher`'s buffers as the connection's loop
thread would, and `flush()` is called explicitly. The non-gating benchmark at the end records
rows per second of the shared-batch fan-out against per-stream Python grouping.
"""

from __future__ import annotations

import functools
import logging
import re
import statistics
import sys
import threading
import time
import types
import weakref
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from deephaven import dtypes
from deephaven.arrow import to_arrow, to_table
from deephaven.stream import blink_to_append_only
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import ALL, SymbolTable
from deephaven_massive._enums import AssetClass, NotBars, OnError, Recency
from deephaven_massive._error_policy import DEFAULT_POLICY, ResolvedPolicy, resolve_policy
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    ErrorSource,
    MassiveError,
    SessionClosedError,
)
from deephaven_massive._massive import (
    DriftReport,
    RowFault,
    RowMark,
    Transport,
    sources,
)
from deephaven_massive._tables import held_rows, problem_reporter, stream
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import update_cycle
from deephaven_massive._tables.futures_dates import LookupState, Readiness
from deephaven_massive._tables.held_rows import DELAYED_AFTER, Delayed, HeldRows, Settled
from deephaven_massive._tables.liveness import release_scope
from deephaven_massive._tables.problem_events import (
    Problem,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    ProblemSpans,
)
from deephaven_massive._tables.row_memory import frame_bytes
from deephaven_massive._tables.stream import (
    ChannelPlan,
    Flusher,
    StreamSink,
    open_stream_tables,
)
from deephaven_massive._tables.watchlist import SkippedValues, SymbolRowRule
from deephaven_massive.namespaces._options import ADMIN_MESSAGES

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import RowMarks
    from deephaven_massive._tables.diagnostics import StreamUpdate
    from deephaven_massive._tables.publisher import Publisher
    from deephaven_massive._tables.session_core import SessionCore


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
W = Transport.WS
TIMEOUT = 15.0
FLUSH_URL = "wss://socket.test/stocks"
"""The connection URL of the tests' flushers, which starts their messages."""

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


UNREADABLE_FAILS = resolve_policy(
    dm.ErrorPolicy(unreadable_symbols=OnError.RAISE), dm.ErrorPolicy()
)
"""A stream's policy under which a symbol spelling the library cannot read fails it."""


# --- ChannelPlan -------------------------------------------------------------------------------


def test_a_plan_reads_the_channel_for_the_dataset(massive_session: dm.MassiveSession) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
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
        ChannelPlan.build(massive_session._core.catalog, keyless, "T")


def test_a_plan_of_rows_without_symbols_reads_none(massive_session: dm.MassiveSession) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, ADMIN_MESSAGES, "AD")
    assert plan.symbol_field is None
    assert plan.required == frozenset({"SipTimestamp"})
    assert [c[1] for c in plan.columns] == [
        "Date",
        "Timestamp",
        "MessageId",
        "Exchange",
        "Message",
        "SequenceNumber",
        "Retransmission",
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


def test_rows_without_symbols_convert_whole_and_withhold_only_a_bad_time(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, ADMIN_MESSAGES, "AD")
    rows = [notice(), notice(Exchange=323, SipTimestamp=5), notice(Exchange=None)]
    converted = plan.convert(frame(rows, "options/AD"), (), {}, arrived=arrivals(rows))
    batch = converted.batch
    # The row whose time fails is withheld; a missing exchange is Massive's null.
    assert batch.column("Exchange").to_pylist() == [302, None]
    assert batch.column("Message").to_pylist() == [notice()["Message"]] * 2  # the text, as sent
    assert batch.column("Date").to_pylist() == ["2026-11-05"] * 2
    assert "Symbol" not in batch.column_names
    assert converted.symbols == frozenset()
    withheld = converted.withheld
    assert withheld.counts() == {RowFault.UNREADABLE_KEYS: 1}
    assert [(g.symbol, g.sent, g.field) for g in withheld.groups] == [(None, None, "Timestamp")]
    assert withheld.by_symbol() == {}


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


def test_conversion_withholds_unreadable_symbols_and_reports_their_spellings(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    # The plan serves streams of any policy: it withholds the row and leaves logging and
    # failing to each stream.
    plan = ChannelPlan.build(massive_session._core.catalog, FOREX, "C")
    cache: dict[str, str | None] = {}
    with caplog.at_level(logging.DEBUG):
        converted = plan.convert(
            frame(FOREX_ROWS, "forex/C"), (), cache, arrived=arrivals(FOREX_ROWS)
        )
    [group] = converted.withheld.groups
    assert (group.fault, group.symbol, group.sent, group.rows) == (
        RowFault.UNREADABLE_SYMBOLS,
        None,
        "nonsense pair",
        1,
    )
    batch = converted.batch
    assert batch.column("Symbol").to_pylist() == ["C:EURUSD"]
    assert "RawSymbol" not in batch.column_names
    assert converted.symbols == frozenset({"C:EURUSD"})
    assert batch.column("Timestamp").type == pa.timestamp("ns", tz="UTC")
    assert cache == {"EUR/USD": "C:EURUSD", "nonsense pair": None}
    assert "nonsense pair" not in caplog.text


def test_conversion_withholds_rows_with_a_bad_time_or_another_bad_timestamp(
    massive_session: dm.MassiveSession,
) -> None:
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    rows = [
        trade("AAPL"),
        trade("MSFT", SipTimestamp=5),
        trade("IBM", TrfTimestamp=2**62),
        trade("NVDA", SipTimestamp=7),
    ]
    converted = plan.convert(frame(rows), (), {}, arrived=arrivals(rows))
    batch = converted.batch
    # The rows whose time fails are withheld, and so is IBM's, whose TRF time fails.
    assert batch.column("Symbol").to_pylist() == ["AAPL"]
    assert batch.column("TrfTimestamp").to_pylist() == [None]
    assert converted.symbols == frozenset({"AAPL"})
    withheld = converted.withheld
    assert withheld.counts() == {RowFault.UNREADABLE_KEYS: 2, RowFault.UNCONVERTIBLE_VALUES: 1}
    assert withheld.by_symbol() == {"MSFT": 1, "IBM": 1, "NVDA": 1}
    assert withheld.first_reason == (
        "timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100"
    )
    assert batch.column("ParticipantTimestamp").to_pylist() == [None]  # 0 is absent


def test_a_size_beyond_2_53_is_withheld(
    massive_session: dm.MassiveSession,
) -> None:
    # The trade's size is a `long` on the channel but a `double` in the dataset.
    catalog = massive_session._core.catalog
    trades = catalog.by_name["stocks.trades"]
    plan = ChannelPlan.build(catalog, trades, "T")
    rows = [trade("AAPL", Size=2**53 + 1), trade("MSFT", Size=2**53)]
    converted = plan.convert(frame(rows), (), {}, arrived=arrivals(rows))
    assert converted.batch.column("Size").to_pylist() == [float(2**53)]
    assert converted.batch.column("Symbol").to_pylist() == ["MSFT"]
    assert [(g.fault, g.symbol, g.field) for g in converted.withheld.groups] == [
        (RowFault.UNCONVERTIBLE_VALUES, "AAPL", "Size")
    ]


def test_rows_convert_as_the_same_rows_keyed_by_column(massive_session: dm.MassiveSession) -> None:
    # The rows a flush gathers, tuples of the source's fields, become the wire table Arrow
    # builds from the same rows keyed by column, however the rows were split into frames.
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    rows = [
        trade("AAPL", 1.5),
        trade("MSFT", 2.0, Conditions=None, TrfId=7, TrfTimestamp=1_700_000_000_001),
        trade("IBM", 3.25, Conditions=[], Id="x", DecimalSize=None),
    ]
    reference = pa.Table.from_pylist(rows, schema=plan.wire_schema)
    batch = plan.convert(frame(rows), (), {}, arrived=arrivals(rows)).batch
    for name, column in [
        ("Price", "Price"),
        ("Size", "Size"),
        ("Conditions", "Conditions"),
        ("Exchange", "Exchange"),
        ("Id", "Id"),
        ("TrfId", "TrfId"),
    ]:
        assert batch.column(name).to_pylist() == reference.column(column).to_pylist(), name
    assert plan.convert([], (), {}, arrived=[]).batch.num_rows == 0


def test_a_column_the_channel_lacks_is_null(massive_session: dm.MassiveSession) -> None:
    # A dataset in a kind has the kind's columns, some of which another member's source carries.
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    wider = replace(
        plan,
        columns=(*plan.columns, (ColumnType.STRING, "Venue", None, None)),
        arrow_schema=plan.arrow_schema.append(pa.field("Venue", pa.string())),
    )
    batch = wider.convert(frame([trade("AAPL")]), (), {}, arrived=[ARRIVED]).batch
    assert batch.column("Venue").to_pylist() == [None]


# --- StreamSink --------------------------------------------------------------------------------


def unread_keys(
    *rows: int, reason: str = "a T row: no Ticker", sent: str | None = None
) -> RowMarks:
    # The client's marks of a frame's rows whose key it could not read.
    found = RowMark(
        rows=rows,
        fields=("Ticker",) * len(rows),
        reasons=(reason,) * len(rows),
        symbols=(sent,) * len(rows),
    )
    return {RowFault.UNREADABLE_KEYS: found}


def capture_problems(core: SessionCore, monkeypatch: pytest.MonkeyPatch) -> list[ProblemEvent]:
    # The problem events the session receives, in order.
    events: list[ProblemEvent] = []
    monkeypatch.setattr(core, "problems", events.extend)
    return events


def make_sink(
    core: SessionCore,
    symbols: NormalizedSymbols,
    plan: ChannelPlan | None = None,
    *,
    stream_id: int = 1,
    policy: ResolvedPolicy = UNREADABLE_FAILS,
) -> tuple[StreamSink, Table]:
    # A trades stream; streams given one `plan` share each flush's batch, as one dataset's do.
    if plan is None:
        plan = ChannelPlan.build(core.catalog, TRADES, "T")
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES),
        symbols,
        core=core,
        policy=policy,
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES),
        ("AAPL",),
        core=core,
        policy=UNREADABLE_FAILS,
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(TRADES),
        ("AAPL",),
        core=core,
        policy=UNREADABLE_FAILS,
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
    plan: ChannelPlan, rows: list[dict[str, object]], *, splits: bool = True
) -> stream.SharedBatch:
    # A flush's batch of the rows, as the flusher shares it among its streams.
    converted = plan.convert(frame(rows), (), {}, arrived=arrivals(rows))
    return stream.SharedBatch(to_table(converted.batch), converted, splits=splits)


def test_a_sink_selects_its_rows_without_work_per_row(massive_session: dm.MassiveSession) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
    # A row whose symbol the library cannot read is withheld, so a stream that holds every
    # readable symbol of the batch takes it whole.
    nameless = shared_batch(plan, [trade("AAPL"), trade("MSFT"), trade("not a symbol")])
    assert nameless.converted.withheld.total == 1
    some.set_symbols(frozenset({"AAPL", "MSFT"}))
    assert some._select(nameless) is nameless.table


def test_streams_of_one_batch_symbol_share_one_partition(
    massive_session: dm.MassiveSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # A stream that holds one of the batch's symbols reads that symbol's rows from a partition
    # of the batch built once per flush, not a filter of its own; arrival order is kept.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    two = shared_batch(plan, [trade("AAPL"), trade("MSFT")]).converted
    one = shared_batch(plan, [trade("AAPL")]).converted
    assert make_sink(core, ("AAPL", "SPY"))[0].takes_one_symbol(two)
    assert not make_sink(core, ALL)[0].takes_one_symbol(two)
    assert not make_sink(core, ("AAPL", "MSFT"))[0].takes_one_symbol(two)
    assert not make_sink(core, ("SPY",))[0].takes_one_symbol(two)
    assert not make_sink(core, ("AAPL",))[0].takes_one_symbol(one)  # the whole batch
    # A withheld row leaves its batch one symbol: the stream takes the whole batch.
    nameless = shared_batch(plan, [trade("AAPL"), trade("not a symbol")]).converted
    assert not make_sink(core, ("AAPL",))[0].takes_one_symbol(nameless)


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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
    flusher.rows("T", frame([trade(symbol) for symbol in symbols]), {}, generation=1)
    flusher.flush()
    assert {batch.splits for batch in made} == {splits}


def test_a_failed_sink_publishes_nothing_and_fails_once(
    massive_session: dm.MassiveSession,
) -> None:
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    batch = shared_batch(plan, [trade("AAPL")])
    sink, _ = make_sink(core, ALL)
    assert sink.publish(batch) == 1
    assert sink.fail(MassiveError("refused"))
    assert not sink.fail(MassiveError("again"))
    assert sink.publish(batch) == 0
    assert sink.rows_published == 1


def test_one_failure_of_many_streams_is_logged_once_at_the_level_its_bug_flag_gives(
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
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
            ChannelPlan.build(core.catalog, TRADES, "T"),
            core.catalog.dataset_schema(TRADES),
            SymbolTable(watchlist),
            core=core,
            policy=UNREADABLE_FAILS,
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
            ChannelPlan.build(core.catalog, TRADES, "T"),
            core.catalog.dataset_schema(TRADES),
            ALL,
            core=core,
            policy=UNREADABLE_FAILS,
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
        ChannelPlan.build(core.catalog, TRADES, "T"),
        core.catalog.dataset_schema(TRADES),
        SymbolTable(watchlist),
        core=core,
        policy=UNREADABLE_FAILS,
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
    assert [str(e) for e in failures] == [
        "the symbol table failed, so the result can no longer follow it"
    ]
    assert isinstance(failures[0], MassiveError)
    assert failures[0].source is ErrorSource.USER  # the table is the user's to fix
    reported = (
        "test.trades stream 1 failed: the symbol table failed, so the result can no longer "
        "follow it"
    )
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [(logging.ERROR, reported)]


def _record_failures(sink: StreamSink, monkeypatch: pytest.MonkeyPatch) -> list[Exception]:
    # Records the error each `publish_failure` of the sink's publisher fails its table with.
    failures: list[Exception] = []
    publisher = sink.publisher

    class _Recording:
        def publish_failure(self, error: Exception) -> None:
            failures.append(error)
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
    closed = f"{core.label} was closed; this table no longer updates. " + NEW_SESSION
    monkeypatch.setattr(core, "fail_stream", lambda _stream_id, _error: closed)
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.stream"):
        assert sink.fail(MassiveError("a failure that raced the close"))
    assert [str(e) for e in failures] == [closed]
    assert isinstance(failures[0], SessionClosedError)
    assert not caplog.records
    assert not sink.fail(MassiveError("again"))  # once only


# --- Time columns -------------------------------------------------------------------------------

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
    plan = ChannelPlan.build(massive_session._core.catalog, spec, "T")
    rows = [
        futures_trade("GCJ5", BEFORE_ROLL_MS),
        futures_trade("GCJ5", AFTER_ROLL_MS),
        futures_trade("ESM5", BEFORE_ROLL_MS),
        futures_trade("NQM5", BEFORE_ROLL_MS),
    ]
    sessions = FakeSessionDates()
    converted = plan.convert(
        frame(rows, "futures/T"), (), {}, arrived=[ARRIVED] * 4, sessions=sessions
    )
    assert converted.batch.column("Date").to_pylist() == [
        "2025-03-07",
        "2025-03-10",  # after Friday's roll: Monday's session
        "2025-03-11",  # moved by the schedules
        None,  # missed by the schedules: the rule's date may be wrong
    ]
    assert sessions.calls[0][2] == ["2025-03-07", "2025-03-10", "2025-03-07", "2025-03-07"]
    assert converted.batch.column("Timestamp").null_count == 0  # every instant is kept
    # Without a calendar, the rule's dates stand.
    alone = plan.convert(frame(rows, "futures/T"), (), {}, arrived=[ARRIVED] * 4)
    assert alone.batch.column("Date").to_pylist()[2] == "2025-03-07"


def test_a_streamed_bar_ends_at_massive_s_end(
    massive_session: dm.MassiveSession,
) -> None:
    spec = massive_session._core.catalog.by_name["stocks.bars"]
    plan = ChannelPlan.build(massive_session._core.catalog, spec, "AM")
    start = 1_741_012_200_000  # 2025-03-03 14:30Z
    row = {
        "Ticker": "AAPL",
        "WindowStart": start,
        "EndTimestamp": start + 59_999,  # Massive's `e`, kept as sent
        "Open": 1.0,
        "Close": 1.0,
        "High": 1.0,
        "Low": 1.0,
        "Volume": 1,
    }
    batch = plan.convert(frame([row], "stocks/AM"), (), {}, arrived=[ARRIVED]).batch
    assert batch.column("TimestampStart").cast(pa.int64()).to_pylist() == [start * 10**6]
    assert batch.column("TimestampEnd").cast(pa.int64()).to_pylist() == [(start + 59_999) * 10**6]
    assert batch.column("Date").to_pylist() == ["2025-03-03"]
    assert "EndTimestamp" not in batch.schema.names


def test_a_futures_second_bar_ends_at_massive_s_end(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    spec = massive_session._core.catalog.by_name["futures.bars"]
    plan = ChannelPlan.build(massive_session._core.catalog, spec, "A")
    start = BEFORE_ROLL_MS
    rows = [futures_bar(start, start + 60_000), futures_bar(start, start + 1_000)]
    with caplog.at_level(logging.INFO):
        batch = plan.convert(frame(rows, "futures/A"), (), {}, arrived=[ARRIVED] * 2).batch
    ends = batch.column("TimestampEnd").cast(pa.int64()).to_pylist()
    assert ends == [(start + 60_000) * 10**6, (start + 1_000) * 10**6]
    # Massive's `e` is kept, and an end that is not one second on is logged.
    [notice] = [r for r in caplog.records if "is not its start plus its length" in r.getMessage()]
    assert notice.levelno == logging.WARNING


def test_every_row_takes_the_arrival_of_its_frame(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    core = massive_session._core
    sink, blink = make_sink(core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", sink)
    before = time.time_ns()
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    buffer = flusher.routes.buffer("T")
    assert buffer is not None
    (stamped, generation, rows, marks, nbytes) = buffer.get_nowait()  # arrival, socket, rows
    assert before <= stamped <= time.time_ns()
    assert rows == frame([trade("AAPL")])
    assert generation == 1
    assert marks == {}
    assert nbytes == frame_bytes(rows)
    # Two frames of one flush: each row takes its own frame's arrival.
    buffer.put((111, 1, frame([trade("AAPL"), trade("MSFT")]), {}, 0))
    buffer.put((222, 1, frame([trade("IBM")]), {}, 0))
    flusher.flush()
    wait_for(lambda: shown.size == 3)
    inserted = to_arrow_column(shown, "TimestampInsert")
    assert inserted == [111, 111, 222]


class FakeFuturesCalendar(FakeSessionDates):
    """A futures calendar whose tickers' states the test sets; every ticker waits at first."""

    def __init__(self) -> None:
        super().__init__()
        self.missing = set()
        self.version = 0
        self.found: dict[str, LookupState] = {}
        self.dead = False

    @override
    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        names = symbols.to_pylist()
        ready = [self.found.get(n, WAITING).readiness is Readiness.READY for n in names]
        return pa.array(
            [d if r else None for d, r in zip(dates.to_pylist(), ready, strict=True)], pa.string()
        )

    def alive(self) -> bool:
        return not self.dead

    def states(self, symbols: Iterable[str]) -> dict[str, LookupState]:
        return {s: self.found.get(s, WAITING) for s in symbols}

    def unplaced(self, symbol: str, instant: int) -> str:
        return f"no session holds {symbol}"

    def set(self, ticker: str, state: LookupState) -> None:
        self.found[ticker] = state
        self.version += 1


WAITING = LookupState(Readiness.WAITING, "its product is being looked up")


class FuturesFlush:
    """A futures flusher with a fake calendar and two streams: NQM5's (1) and ESM5's (2).

    `first` replaces stream 1's symbols, such as `ALL` for a stream of every ticker.
    """

    def __init__(
        self,
        core: SessionCore,
        monkeypatch: pytest.MonkeyPatch,
        *,
        policy: ResolvedPolicy = DEFAULT_POLICY,
        first: NormalizedSymbols = ("NQM5",),
    ) -> None:
        spec = core.catalog.by_name["futures.trades"]
        self.plan = ChannelPlan.build(core.catalog, spec, "T")
        self.updates: list[dict[int, StreamUpdate]] = []
        self.events: list[Problem] = []
        monkeypatch.setattr(core, "update_streams", self.updates.append)
        monkeypatch.setattr(core, "problems", self.events.extend)
        self.calendar = FakeFuturesCalendar()
        self.now = 1_000.0
        self.held: HeldRows[ChannelPlan] = HeldRows(
            self.calendar, max_outage=300.0, clock=lambda: self.now
        )
        self.flusher = Flusher(
            core.exec_ctx,
            "test-flush",
            url=FLUSH_URL,
            interval=3600.0,
            sessions=self.calendar,
            held=self.held,
        )
        self.sinks: list[StreamSink] = []
        streams: tuple[tuple[int, NormalizedSymbols], ...] = ((1, first), (2, ("ESM5",)))
        for stream_id, wanted in streams:
            opened = open_stream_tables(
                self.plan,
                core.catalog.dataset_schema(spec),
                wanted,
                core=core,
                policy=policy,
                stream_id=stream_id,
                host="socket.test",
                evidence="test host",
                recency=Recency.REALTIME,
                path="/futures",
                on_shutdown=lambda: None,
                on_symbols=lambda *_: None,
            )
            opened.sink.publish_held()
            self.flusher.routes.add("T", opened.sink)
            self.sinks.append(opened.sink)

    def send(self, *tickers: str, generation: int = 1) -> None:
        rows = [futures_trade(t, BEFORE_ROLL_MS) for t in tickers]
        self.flusher.rows("T", frame(rows, "futures/T"), {}, generation=generation)
        self.flusher.flush()

    def published(self) -> dict[int, int]:
        return {sink.stream_id: sink.rows_published for sink in self.sinks}


@pytest.fixture
def futures_flush(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> Iterator[FuturesFlush]:
    found = FuturesFlush(massive_session._core, monkeypatch)
    yield found
    found.flusher.stop()


def test_futures_rows_wait_for_their_date_then_publish_in_order(
    futures_flush: FuturesFlush,
) -> None:
    flush = futures_flush
    flush.calendar.set("ESM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.send("NQM5", "ESM5")
    # NQM5's row waits; ESM5's, dated, publishes; neither stream is failed or told anything.
    assert flush.published() == {1: 0, 2: 1}
    assert flush.held.rows_of(flush.plan) == 1
    flush.send("NQM5")
    assert flush.held.rows_of(flush.plan) == 2
    flush.calendar.set("NQM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.flusher.flush()  # no new rows: the held ones publish on their own
    assert flush.published() == {1: 2, 2: 1}
    assert flush.held.rows_of(flush.plan) == 0
    assert not any(sink.failed for sink in flush.sinks)
    assert flush.events == []


def test_held_rows_of_two_sockets_publish_each_with_its_own(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5", generation=1)
    flush.send("NQM5", generation=2)  # after a reconnect
    flush.calendar.set("NQM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.flusher.flush()
    assert flush.published()[1] == 2
    assert flush.sinks[0].rows_generation == 2


def test_a_ticker_a_watchlist_removes_drops_its_held_rows(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5")
    flush.sinks[0].set_symbols(frozenset({"GCM5"}))  # its watchlist removed NQM5
    flush.send("ESM5")
    assert flush.held.rows_of(flush.plan) == 1  # only ESM5's, which stream 2 receives


def test_held_rows_never_count_as_dropped(futures_flush: FuturesFlush) -> None:
    # Rows that only wait are read whole: a long wait never looks like a format change.
    flush = futures_flush
    for _ in range(stream.DROPPED_RUN_LIMIT + 1):
        flush.send("NQM5")
    assert not any(sink.failed for sink in flush.sinks)
    assert flush.held.rows_of(flush.plan) == stream.DROPPED_RUN_LIMIT + 1


def test_a_stale_futures_ticker_s_span_ends_only_when_its_held_row_publishes(
    futures_flush: FuturesFlush,
) -> None:
    # NQM5's every row is withheld until it is stale; its next row is read whole but waits for
    # its date, so nothing of NQM5 reaches the stream yet, and the span must hold.
    flush = futures_flush
    limit = stream.DROPPED_RUN_LIMIT
    lost = frame([futures_trade("NQM5", BEFORE_ROLL_MS)] * limit, "futures/T")
    marks = unread_keys(*range(limit), reason="a T row for NQM5: no Ticker", sent="NQM5")
    flush.flusher.rows("T", lost, marks, generation=1)
    flush.flusher.flush()
    assert [list(found) for found in stale_spans(flush.events, 1)] == [["NQM5"]]
    flush.events.clear()
    flush.send("NQM5")
    assert flush.published()[1] == 0
    assert stale_spans(flush.events, 1) == []  # the row waits: the span holds
    flush.calendar.set("NQM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.flusher.flush()
    assert flush.published()[1] == 1
    assert stale_spans(flush.events, 1) == [{}]  # published: the span ends


def test_a_refused_lookup_fails_only_the_streams_that_receive_the_ticker(
    futures_flush: FuturesFlush,
) -> None:
    flush = futures_flush
    refusal = dm.EntitlementError("HTTP 403", status=403)
    flush.calendar.set("NQM5", LookupState(Readiness.REFUSED, "refused", error=refusal))
    flush.calendar.set("ESM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.send("NQM5", "ESM5")
    assert [sink.failed for sink in flush.sinks] == [True, False]
    assert flush.published()[2] == 1


@pytest.mark.parametrize(
    ("readiness", "cause"),
    [
        (Readiness.UNLISTED, ProblemCause.UNLISTED_CONTRACTS),
        (Readiness.UNSCHEDULED, ProblemCause.UNSCHEDULED_PRODUCTS),
    ],
)
def test_discarded_rows_are_withheld_and_listed_with_the_streams_setting(
    *,
    futures_flush: FuturesFlush,
    readiness: Readiness,
    cause: ProblemCause,
) -> None:
    # Each flush's discards are events of the stream that would have received them, with its
    # setting; the session's reporter logs the first of each cause.
    flush = futures_flush
    flush.calendar.set("NQM5", LookupState(readiness, "gone"))
    flush.send("NQM5")
    flush.send("NQM5")
    assert flush.published() == {1: 0, 2: 0}
    assert [
        (e.cause, e.action, e.symbol, e.rows, e.stream_id, e.policy)
        for e in flush.events
        if isinstance(e, ProblemEvent)
    ] == [(cause, ProblemAction.WITHHELD, "NQM5", 1, 1, OnError.LOG)] * 2
    assert not flush.sinks[0].failed


@pytest.mark.parametrize(
    ("readiness", "setting"),
    [
        (Readiness.UNLISTED, "unlisted_contracts"),
        (Readiness.UNSCHEDULED, "unscheduled_products"),
    ],
)
def test_discarded_rows_fail_a_stream_whose_setting_raises(
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    readiness: Readiness,
    setting: str,
) -> None:
    policy = resolve_policy(dm.ErrorPolicy(**{setting: OnError.RAISE}), dm.ErrorPolicy())
    flush = FuturesFlush(massive_session._core, monkeypatch, policy=policy)
    flush.calendar.set("NQM5", LookupState(readiness, "gone"))
    flush.send("NQM5")
    flush.flusher.stop()
    assert [sink.failed for sink in flush.sinks] == [True, False]


def undated(flush: FuturesFlush) -> list[dict[str, str]]:
    # Each `undated_rows` snapshot the streams reported: its waiting tickers, with why.
    return [
        {name: span.detail for name, span in p.spans.items()}
        for p in flush.events
        if isinstance(p, ProblemSpans) and p.cause is ProblemCause.UNDATED_ROWS
    ]


def test_a_long_wait_is_an_undated_rows_span_until_the_rows_publish(
    futures_flush: FuturesFlush,
) -> None:
    flush = futures_flush
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    flush.now += 1
    flush.flusher.flush()  # still waiting: no change, no new snapshot
    [waiting] = undated(flush)
    assert list(waiting) == ["NQM5"]
    flush.calendar.set("NQM5", LookupState(Readiness.READY, loaded_at=0.0))
    flush.flusher.flush()
    assert undated(flush)[1:] == [{}]  # ended once its rows published
    assert flush.published()[1] == 1


def test_a_later_wait_opens_a_new_span(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    for _ in range(2):
        flush.calendar.set("NQM5", WAITING)
        flush.send("NQM5")
        flush.now += DELAYED_AFTER
        flush.flusher.flush()
        flush.calendar.set("NQM5", LookupState(Readiness.READY, loaded_at=0.0))
        flush.flusher.flush()
    assert [list(found) for found in undated(flush)] == [["NQM5"], [], ["NQM5"], []]
    assert flush.published()[1] == 2


def test_a_discarded_ticker_ends_its_wait(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    flush.calendar.set("NQM5", LookupState(Readiness.UNSCHEDULED, "gone"))
    flush.flusher.flush()
    assert [list(found) for found in undated(flush)] == [["NQM5"], []]


def undated_snapshots(flush: FuturesFlush, stream_id: int) -> list[ProblemSpans]:
    # Each `undated_rows` snapshot one stream reported, whole.
    return [
        p
        for p in flush.events
        if isinstance(p, ProblemSpans)
        and p.cause is ProblemCause.UNDATED_ROWS
        and p.stream_id == stream_id
    ]


def test_a_ticker_dropped_from_the_watchlist_ends_its_wait(futures_flush: FuturesFlush) -> None:
    # The stream no longer receives the ticker, so its rows can never publish to it: the span
    # ends at the next flush, though another stream keeps the ticker's rows waiting.
    flush = futures_flush
    nqm5, esm5 = flush.sinks
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    esm5.set_symbols(frozenset({"ESM5", "NQM5"}))  # keeps NQM5's rows waiting
    nqm5.set_symbols(frozenset({"GCM5"}))
    flush.flusher.flush()
    assert [list(found.spans) for found in undated_snapshots(flush, 1)] == [["NQM5"], []]


def test_a_ticker_no_stream_receives_any_more_ends_its_wait(
    futures_flush: FuturesFlush,
) -> None:
    # Its held rows are dropped, so nothing settles them again: the flush still ends the span.
    flush = futures_flush
    nqm5, _ = flush.sinks
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    nqm5.set_symbols(frozenset({"GCM5"}))
    flush.flusher.flush()
    assert flush.held.rows_of(flush.plan) == 0
    assert [list(found.spans) for found in undated_snapshots(flush, 1)] == [["NQM5"], []]


def test_a_stream_of_every_ticker_keeps_its_wait_across_flushes(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A `dm.ALL` stream names no ticker, yet receives every one: a flush never ends its wait as
    # if the ticker had left its watchlist.
    flush = FuturesFlush(massive_session._core, monkeypatch, first=ALL)
    try:
        flush.send("NQM5")
        flush.now += DELAYED_AFTER
        flush.flusher.flush()
        flush.flusher.flush()
        assert [list(found.spans) for found in undated_snapshots(flush, 1)] == [["NQM5"]]
    finally:
        flush.flusher.stop()


def test_a_stream_that_receives_a_waiting_ticker_later_shows_its_wait(
    futures_flush: FuturesFlush,
) -> None:
    # Settling reports NQM5's wait once, to stream 1; stream 2's watchlist adds NQM5 later and
    # shows the same wait from its next flush.
    flush = futures_flush
    _, esm5 = flush.sinks
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    assert undated_snapshots(flush, 2) == []
    esm5.set_symbols(frozenset({"ESM5", "NQM5"}))
    flush.flusher.flush()
    assert [list(found.spans) for found in undated_snapshots(flush, 2)] == [["NQM5"]]
    assert [list(found.spans) for found in undated_snapshots(flush, 1)] == [["NQM5"]]


def test_a_waiting_ticker_s_rows_grow_on_its_span(futures_flush: FuturesFlush) -> None:
    # More rows of a ticker whose wait was reported join the hold: its span counts them and
    # keeps its start.
    flush = futures_flush
    flush.send("NQM5")
    flush.now += DELAYED_AFTER
    flush.flusher.flush()
    flush.send("NQM5", "NQM5")
    first, later = undated_snapshots(flush, 1)
    assert (first.spans["NQM5"].rows, later.spans["NQM5"].rows) == (1, 3)
    assert later.spans["NQM5"].since == first.spans["NQM5"].since


def test_a_wait_whose_older_rows_were_discarded_holds_on_while_newer_rows_wait(
    futures_flush: FuturesFlush,
) -> None:
    # Settling discards one of NQM5's rows, but the hold still has a reported wait of it: the
    # dataset's open wait keeps its reason and counts the rows left.
    flush = futures_flush
    reported = Settled(None, delayed={"NQM5": Delayed(3, "its product is being looked up")})
    assert flush.flusher._open_waits(flush.plan, reported, {"NQM5": 3}) == {
        "NQM5": (3, "its product is being looked up")
    }
    discarded = Settled(None, discarded={Readiness.UNSCHEDULED: {"NQM5": (1, "no session")}})
    assert flush.flusher._open_waits(flush.plan, discarded, {"NQM5": 2}) == {
        "NQM5": (2, "its product is being looked up")
    }
    assert flush.flusher._open_waits(flush.plan, Settled(None), {}) == {}


def test_a_later_report_of_a_waiting_ticker_updates_its_rows_and_keeps_its_start(
    futures_flush: FuturesFlush,
) -> None:
    # Driven through the stream's span bookkeeping directly: settling reports a wait once, so
    # the rows of an open span change only when the stream's waits are handed in again.
    flush = futures_flush
    sink = flush.sinks[0]

    def report(rows: int) -> ProblemSpans | None:
        waits = {"NQM5": (rows, "its product is being looked up")}
        return flush.flusher._undated_spans(
            flush.plan, waits, sink, where="test", receives=sink.holds
        )

    first = report(1)
    assert first is not None
    assert report(1) is None  # nothing changed
    later = report(3)
    assert later is not None
    assert (first.spans["NQM5"].rows, later.spans["NQM5"].rows) == (1, 3)
    assert later.spans["NQM5"].since == first.spans["NQM5"].since


def test_past_the_byte_limit_the_streams_of_the_dataset_fail(
    futures_flush: FuturesFlush, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(held_rows, "HELD_BYTES_LIMIT", 1)
    futures_flush.send("NQM5")
    assert all(sink.failed for sink in futures_flush.sinks)


def test_held_rows_past_the_byte_limit_fail_their_streams_between_frames(
    futures_flush: FuturesFlush, monkeypatch: pytest.MonkeyPatch
) -> None:
    futures_flush.send("NQM5")
    monkeypatch.setattr(held_rows, "HELD_BYTES_LIMIT", 1)
    futures_flush.flusher.flush()  # no new rows: the held ones are settled alone
    assert all(sink.failed for sink in futures_flush.sinks)


def test_a_dead_calendar_fails_the_streams_whose_rows_wait(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5")
    flush.calendar.dead = True
    flush.flusher.flush()
    assert all(sink.failed for sink in flush.sinks)
    assert flush.held.holding() == ()


def test_held_rows_of_a_dataset_no_stream_reads_are_dropped(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5")
    for sink in flush.sinks:
        flush.flusher.routes.remove(sink.stream_id)
    flush.flusher.flush()
    assert flush.held.holding() == ()


def test_a_held_dataset_whose_streams_failed_drops_its_rows(futures_flush: FuturesFlush) -> None:
    flush = futures_flush
    flush.send("NQM5")
    for sink in flush.sinks:
        sink.fail(dm.MassiveError("gone"))
    flush.flusher.flush()
    assert flush.held.holding() == ()


def test_a_failure_settling_held_rows_fails_their_streams_as_a_defect(
    futures_flush: FuturesFlush, monkeypatch: pytest.MonkeyPatch
) -> None:
    flush = futures_flush
    flush.send("NQM5")

    def broken(symbols: Iterable[str]) -> dict[str, LookupState]:
        raise RuntimeError("calendar broke")

    monkeypatch.setattr(flush.calendar, "states", broken)
    flush.calendar.version += 1
    flush.flusher.flush()
    assert all(sink.failed for sink in flush.sinks)
    assert flush.held.holding() == ()


# --- Flusher -----------------------------------------------------------------------------------


@pytest.fixture
def flusher(massive_session: dm.MassiveSession) -> Iterator[Flusher]:
    f = Flusher(massive_session._core.exec_ctx, "test-flush", url=FLUSH_URL, interval=3600.0)
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
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), {}, generation=1)
    flusher.rows("Q", frame([trade("AAPL")]), {}, generation=1)  # no stream ever read Q: dropped
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
        url=FLUSH_URL,
        interval=3600.0,
        on_new_generation=lambda: calls.append(None),
    )
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",))
    msft, _ = make_sink(core, ("MSFT",))
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), {}, generation=1)
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=2)
    flusher.flush()
    assert (aapl.rows_generation, msft.rows_generation) == (2, 1)
    assert (aapl.rows_published, msft.rows_published) == (2, 1)
    assert len(calls) == 1
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=2)
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
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
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
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
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
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)]), {}, generation=1)
    flusher.flush()
    assert not aapl.failed
    assert not msft.failed
    assert aapl.rows_published == msft.rows_published == 0


def test_an_all_stream_survives_rows_whose_time_or_value_is_odd(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
    rows_when: RowsWhen,
) -> None:
    core = massive_session._core
    every, blink = make_sink(core, ALL)
    shown = blink_to_append_only(blink)
    flusher.routes.add("T", every)
    rows = [
        trade("AAPL"),
        trade("MSFT", SipTimestamp=5),
        trade("IBM", TrfTimestamp=2**62),
        trade("ZZZ", Ticker=None),
    ]
    events = capture_problems(core, monkeypatch)
    # The client marked a row it could not route; the plan marks the other two odd rows.
    flusher.rows(
        "T",
        frame(rows),
        unread_keys(3, reason="a T row from h/stocks: no Ticker"),
        generation=1,
    )
    flusher.flush()
    flusher.rows("T", frame([trade("NVDA")]), {}, generation=1)
    flusher.flush()
    assert not every.failed
    assert every.rows_published == 2
    published = rows_when(shown, lambda found: len(found) == 2)
    assert [r["Symbol"] for r in published] == ["AAPL", "NVDA"]
    assert [(e.cause, e.symbol, e.field, e.rows, e.stream_id) for e in events] == [
        (ProblemCause.UNREADABLE_KEYS, "MSFT", "Timestamp", 1, every.stream_id),
        (ProblemCause.UNCONVERTIBLE_VALUES, "IBM", "TrfTimestamp", 1, every.stream_id),
        (ProblemCause.UNREADABLE_KEYS, None, "Ticker", 1, every.stream_id),
    ]


def test_a_run_of_dropped_rows_fails_the_datasets_streams_on_the_channel(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    # One odd row fails nothing; a run of them with nothing published means the format changed,
    # which fails the dm.ALL stream: every row of the channel is its. The AAPL stream's own rows
    # are half the run, so it goes on.
    core = massive_session._core
    every, _ = make_sink(core, ALL)
    aapl, _ = make_sink(core, ("AAPL",))
    flusher.routes.add("T", every)
    flusher.routes.add("T", aapl)
    half = stream.DROPPED_RUN_LIMIT // 2
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)] * half), {}, generation=1)
    flusher.flush()
    flusher.rows(
        "T", frame([trade("AAPL")]), {}, generation=1
    )  # a published row starts the run again
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)] * half), {}, generation=1)
    flusher.flush()
    assert (every.failed, aapl.failed) == (False, False)
    # The client's marked rows count too.
    lost = frame([trade("AAPL", Ticker=None)] * half)
    with caplog.at_level(logging.WARNING):
        flusher.rows(
            "T",
            lost,
            unread_keys(*range(half), reason="a T row from h/stocks: no Ticker"),
            generation=1,
        )
        flusher.flush()
    assert every.failed
    assert not aapl.failed
    [record] = [r for r in caplog.records if "could not read the last" in r.getMessage()]
    assert record.levelno == logging.WARNING  # Massive's data, no bug of the library
    assert record.getMessage() == (
        f"test.trades stream 1 failed: test.trades: the library could not read the last "
        f"{2 * half} rows Massive sent on channel T whole, so it withheld them and published "
        "none. Timestamp unit mismatch for test.trades.Timestamp: 5 ms is outside 1990 to 2100. "
        "m.problems() counts the withheld rows by cause and symbol."
    )
    assert flusher._runs == {}


def stale_spans(problems: list[Problem], stream_id: int) -> list[dict[str, str]]:
    # Each `stale_symbol_data` snapshot of one stream, as its symbols and their details.
    return [
        {name: span.detail for name, span in p.spans.items()}
        for p in problems
        if isinstance(p, ProblemSpans)
        and p.cause is ProblemCause.STALE_SYMBOL_DATA
        and p.stream_id == stream_id
    ]


def test_a_symbol_whose_every_row_is_withheld_is_stale_in_its_streams_and_fails_none(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # AAPL publishes in every flush, so the dataset's run never grows; ILLQ's rows are all
    # withheld, and the streams that hold ILLQ must not freeze without a word: each marks it
    # stale, and none fails, so AAPL's rows keep flowing to the stream that names both.
    core = massive_session._core
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    every, _ = make_sink(core, ALL, plan, stream_id=7)
    illq, _ = make_sink(core, ("ILLQ", "AAPL"), plan, stream_id=8)
    aapl, _ = make_sink(core, ("AAPL",), plan, stream_id=9)
    for sink in (every, illq, aapl):
        flusher.routes.add("T", sink)
    half = stream.DROPPED_RUN_LIMIT // 2
    flusher.rows(
        "T", frame([trade("AAPL"), *[trade("ILLQ", SipTimestamp=5)] * half]), {}, generation=1
    )
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    # The client marked ILLQ rows too, counted by their canonical symbol.
    lost = frame([trade("ILLQ", Price=None)] * (half - 1))
    marks = unread_keys(*range(half - 1), reason="a T row for ILLQ: no Price", sent="ILLQ")
    flusher.rows("T", lost, marks, generation=1)
    flusher.flush()
    assert stale_spans(problems, 8) == []
    flusher.rows("T", frame([trade("AAPL"), trade("ILLQ", SipTimestamp=5)]), {}, generation=1)
    flusher.flush()
    assert (every.failed, illq.failed, aapl.failed) == (False, False, False)
    why = (
        f"The library withheld the last {2 * half} rows of this symbol. Timestamp unit mismatch "
        "for test.trades.Timestamp: 5 ms is outside 1990 to 2100."
    )
    assert stale_spans(problems, 7) == [{"ILLQ": why}]
    assert stale_spans(problems, 8) == [{"ILLQ": why}]
    assert stale_spans(problems, 9) == []  # a stream that never held ILLQ
    problems.clear()
    flusher.rows("T", frame([trade("ILLQ")]), {}, generation=1)
    flusher.flush()
    assert (stale_spans(problems, 7), stale_spans(problems, 8)) == ([{}], [{}])  # published


def test_an_all_stream_marks_each_starved_symbol_stale_until_it_publishes_again(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    every, _ = make_sink(core, ALL, plan, stream_id=7)
    flusher.routes.add("T", every)
    limit = stream.DROPPED_RUN_LIMIT
    for symbol in ("ILLQ", "AAA"):
        flusher.rows(
            "T", frame([trade("AAPL"), *[trade(symbol, SipTimestamp=5)] * limit]), {}, generation=1
        )
        flusher.flush()
    assert not every.failed
    assert [list(found) for found in stale_spans(problems, 7)] == [["ILLQ"], ["ILLQ", "AAA"]]
    first = next(p for p in problems if isinstance(p, ProblemSpans))
    problems.clear()
    flusher.rows("T", frame([trade("ILLQ")]), {}, generation=1)
    flusher.flush()
    [left] = [p for p in problems if isinstance(p, ProblemSpans)]
    assert list(left.spans) == ["AAA"]
    assert left.spans["AAA"].since > first.spans["ILLQ"].since
    problems.clear()
    flusher.rows("T", frame([trade("ILLQ")]), {}, generation=1)
    flusher.flush()
    assert stale_spans(problems, 7) == []  # nothing changed
    flusher.routes.remove(7)
    flusher.flush()
    assert flusher._stale == {}


def test_a_stale_symbol_the_stream_no_longer_holds_ends_its_span(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A watchlist row deleted while its symbol is stale: no row of it can reach the stream
    # again, so its span ends at the next flush, rows or none, instead of holding until the
    # stream closes.
    core = massive_session._core
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    named, _ = make_sink(core, ("ILLQ", "AAPL"), plan, stream_id=8)
    flusher.routes.add("T", named)
    limit = stream.DROPPED_RUN_LIMIT
    flusher.rows(
        "T", frame([trade("AAPL"), *[trade("ILLQ", SipTimestamp=5)] * limit]), {}, generation=1
    )
    flusher.flush()
    assert [list(found) for found in stale_spans(problems, 8)] == [["ILLQ"]]
    problems.clear()
    named.set_symbols(frozenset({"AAPL"}))
    flusher.flush()  # no rows: the watchlist may have emptied
    assert stale_spans(problems, 8) == [{}]
    problems.clear()
    flusher.flush()
    assert stale_spans(problems, 8) == []  # ended once


def test_a_stale_symbol_whose_run_reaches_the_limit_again_updates_its_detail_in_place(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The run starts over at the limit; a second full run with another first reason restates
    # why, and the span keeps the start it had.
    core = massive_session._core
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    every, _ = make_sink(core, ALL, plan, stream_id=7)
    flusher.routes.add("T", every)
    limit = stream.DROPPED_RUN_LIMIT
    flusher.rows(
        "T", frame([trade("AAPL"), *[trade("ILLQ", SipTimestamp=5)] * limit]), {}, generation=1
    )
    flusher.flush()
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    lost = frame([trade("ILLQ", Price=None)] * limit)
    marks = unread_keys(*range(limit), reason="a T row for ILLQ: no Price", sent="ILLQ")
    flusher.rows("T", lost, marks, generation=1)
    flusher.flush()
    assert not every.failed
    first, later = (
        p.spans["ILLQ"]
        for p in problems
        if isinstance(p, ProblemSpans) and p.cause is ProblemCause.STALE_SYMBOL_DATA
    )
    assert first.detail.startswith(
        f"The library withheld the last {limit} rows of this symbol. Timestamp"
    )
    assert later.detail == (
        f"The library withheld the last {limit} rows of this symbol. A T row for ILLQ: no Price."
    )
    assert later.since == first.since


def test_a_dataset_whose_every_row_is_withheld_fails_its_streams_and_marks_nothing_stale(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    every, _ = make_sink(core, ALL, plan)
    flusher.routes.add("T", every)
    # The symbol's run and the dataset's reach the limit in one flush: the stream fails.
    flusher.rows(
        "T", frame([trade("ILLQ", SipTimestamp=5)] * stream.DROPPED_RUN_LIMIT), {}, generation=1
    )
    flusher.flush()
    assert every.failed
    assert stale_spans(problems, every.stream_id) == []


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
    flusher.rows("T", frame([trade("ILLQ", SipTimestamp=5)] * 3), {}, generation=1)
    flusher.flush()
    assert flusher._symbol_runs[illq.plan]["ILLQ"][0] == 3
    flusher.rows("T", frame([trade("ILLQ"), trade("ILLQ", SipTimestamp=5)]), {}, generation=1)
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
    with caplog.at_level(logging.WARNING):
        flusher.rows(
            "T", frame([trade("AAPL", SipTimestamp=5)] * stream.DROPPED_RUN_LIMIT), {}, generation=1
        )
        flusher.flush()
    assert every.failed
    assert "could not publish" not in caplog.text
    assert (
        f"the library could not read the last {stream.DROPPED_RUN_LIMIT} rows Massive sent "
        "on channel T whole"
    ) in caplog.text


def test_a_flush_forgets_the_run_of_a_dataset_no_longer_routed(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    sink, _ = make_sink(massive_session._core, ALL)
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5)]), {}, generation=1)
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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")

    def sink(stream_id: int, symbol: str) -> StreamSink:
        # The two streams share the dataset's plan, as the streams of one connection do.
        opened = open_stream_tables(
            plan,
            core.catalog.dataset_schema(TRADES),
            (symbol,),
            core=core,
            policy=UNREADABLE_FAILS,
            stream_id=stream_id,
            host="socket.test",
            evidence="test host",
            recency=Recency.REALTIME,
            path="/stocks",
            on_shutdown=lambda: None,
            on_symbols=lambda *_: None,
        )
        opened.sink.publish_held()
        return opened.sink

    aapl, msft = sink(1, "AAPL"), sink(2, "MSFT")

    def refused(_: Exception) -> None:
        raise RuntimeError("refused")

    monkeypatch.setattr(aapl, "publisher", types.SimpleNamespace(publish_failure=refused))
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    flusher.rows("T", frame([trade("!!"), trade("MSFT")]), {}, generation=1)
    flusher.flush()
    assert aapl.failed
    assert msft.failed
    assert msft.rows_published == 0
    assert (
        f"{FLUSH_URL}: the library could not stop stream 1 with this error: test.trades: "
        "Massive sent 1 row the library cannot read whole"
    ) in caplog.text


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
    flusher.rows("T", frame([trade("AAPL", Price="not a number")]), {}, generation=1)
    flusher.rows("Q", frame([trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert broken.failed
    assert not healthy.failed
    rows_when(shown, lambda rows: len(rows) == 1)
    assert (
        "test.trades: the library could not publish the rows Massive sent on channel T: "
    ) in caplog.text


def test_drift_is_a_problem_of_each_stream_of_the_channel(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    events = capture_problems(core, monkeypatch)
    sink, _ = make_sink(core, ALL)
    gone, _ = make_sink(core, ALL, stream_id=2)
    flusher.routes.add("T", sink)
    flusher.routes.add("T", gone)
    # Stream 2's session was collected.
    monkeypatch.setattr(StreamSink, "core", lambda self: None if self.stream_id == 2 else core)
    flusher.drift("T", DriftReport(extra=frozenset({"zz"})), where="wss://h/stocks channel T")
    flusher.flush()
    assert [(e.cause, e.field, e.stream_id) for e in events] == [
        (ProblemCause.SCHEMA_DRIFT, "zz", sink.stream_id)
    ]


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
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), {}, generation=1)
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
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT")]), {}, generation=1)
    flusher.flush()
    assert flusher._symbols[sink.plan] == {"AAPL": "AAPL", "MSFT": "MSFT"}
    with caplog.at_level(logging.DEBUG, logger=stream.__name__):
        flusher.rows("T", frame([trade("IBM")]), {}, generation=1)
        flusher.flush()
    assert flusher._symbols[sink.plan] == {}
    assert (
        "test.trades: the T symbol cache holds 3 spellings, past its limit of 2; starts over"
        in caplog.text
    )
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert flusher._symbols[sink.plan] == {"AAPL": "AAPL"}
    rows = rows_when(shown, lambda rows: len(rows) == 4)
    assert [row["Symbol"] for row in rows] == ["AAPL", "MSFT", "IBM", "AAPL"]


def test_an_unreadable_spelling_warns_once_across_a_symbol_cache_reset(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(stream, "SYMBOL_CACHE_LIMIT", 1)
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, FOREX, "C")
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(FOREX),
        ALL,
        core=core,
        policy=DEFAULT_POLICY,
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
            flusher.rows("C", frame(FOREX_ROWS, "forex/C"), {}, generation=1)
            flusher.flush()
            assert flusher._symbols[plan] == {}  # two spellings pass the limit of one
    assert not opened.sink.failed
    assert opened.sink.rows_published == 2  # the unreadable row of each flush is withheld
    spelled = [
        r
        for r in caplog.records
        if r.name == problem_reporter.__name__ and "nonsense pair" in r.getMessage()
    ]
    assert len(spelled) == 1  # the stream's first unreadable_symbols problem


def _forex_sink(
    core: SessionCore, plan: ChannelPlan, *, stream_id: int, setting: OnError
) -> StreamSink:
    # A `dm.ALL` forex stream on `plan` whose call set `unreadable_symbols` to `setting`.
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(FOREX),
        ALL,
        core=core,
        policy=resolve_policy(dm.ErrorPolicy(unreadable_symbols=setting), dm.ErrorPolicy()),
        stream_id=stream_id,
        host="socket.test",
        path="/forex",
        evidence="test host",
        recency=Recency.REALTIME,
        on_shutdown=lambda: None,
        on_symbols=lambda *_: None,
    )
    opened.sink.publish_held()
    return opened.sink


@pytest.mark.parametrize(
    "settings",
    [(OnError.RAISE, OnError.LOG, OnError.IGNORE), (OnError.IGNORE, OnError.IGNORE)],
)
def test_each_stream_of_a_plan_applies_its_own_unreadable_setting(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    *,
    settings: tuple[OnError, ...],
) -> None:
    # One plan converts the batch once for every stream: a stream under RAISE fails, the others
    # publish the readable row only, and each gets the withheld row's event with its setting.
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, FOREX, "C")
    sinks = [
        _forex_sink(core, plan, stream_id=n, setting=setting)
        for n, setting in enumerate(settings, 1)
    ]
    for sink in sinks:
        flusher.routes.add("C", sink)
    events = capture_problems(core, monkeypatch)
    flusher.rows("C", frame(FOREX_ROWS, "forex/C"), {}, generation=1)
    flusher.flush()
    for sink, setting in zip(sinks, settings, strict=True):
        assert sink.failed is (setting is OnError.RAISE)
        assert sink.rows_published == (0 if sink.failed else 1)
    # Every stream's session records the row, a failed stream's too, with its own setting.
    assert [(e.stream_id, e.cause, e.symbol, e.rows, e.policy) for e in events] == [
        (sink.stream_id, ProblemCause.UNREADABLE_SYMBOLS, "nonsense pair", 1, setting)
        for sink, setting in zip(sinks, settings, strict=True)
    ]


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
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
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
        flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
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
        super().__init__(exec_ctx, "dying-flush", url=FLUSH_URL, interval=0.001)

    @override
    def flush(self) -> None:
        raise _Fatal


class _Unfailable:
    # A stream's publisher whose failure raises; its stream is routed no rows.
    def add(self, table: Table) -> None:
        raise AssertionError(table)

    def publish_failure(self, error: Exception) -> None:
        raise RuntimeError(f"cannot fail: {error}")


class _FailureLog:
    # A stream's publisher that records the errors its table is failed with; its stream is
    # routed no rows.
    def __init__(self) -> None:
        self.errors: list[Exception] = []

    def add(self, table: Table) -> None:
        raise AssertionError(table)

    def publish_failure(self, error: Exception) -> None:
        self.errors.append(error)


def _sink_of(core: SessionCore, stream_id: int, publisher: Publisher) -> StreamSink:
    # A trades stream that publishes to `publisher`, outside the session's registry.
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    return StreamSink(
        stream_id,
        core=core,
        plan=plan,
        publisher=publisher,
        symbols=None,
        policy=UNREADABLE_FAILS,
    )


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
    f.routes.add("T", _sink_of(core, 2, _Unfailable()))
    f.routes.add("T", healthy)
    with caplog.at_level(logging.ERROR):
        f.start()
        assert f.join(TIMEOUT)
    assert healthy.failed  # a stream that cannot be failed does not spare the others
    # A defect of the library: one ERROR with how to report it, not CRITICAL, and not one per
    # stream it fails.
    died = [
        r for r in caplog.records if r.getMessage().startswith(f"{FLUSH_URL}: the library thread")
    ]
    assert [(r.levelno, r.getMessage()) for r in died] == [
        (
            logging.ERROR,
            f"{FLUSH_URL}: the library thread that publishes this connection's rows died: "
            "_Fatal(). This table no longer updates. " + LIBRARY_BUG,
        )
    ]
    assert died[0].exc_info is not None
    assert f"{FLUSH_URL}: the library could not stop stream 2 with this error: " in caplog.text
    assert [args.exc_type for args in escaped] == [_Fatal]


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
    recording = _FailureLog()
    flusher.routes.add("T", healthy)
    flusher.routes.add("Q", _sink_of(core, 2, _Unfailable()))
    flusher.routes.add("Q", _sink_of(core, 3, recording))
    flusher.fail([2], MassiveError("refused"))
    flusher.fail([3], MassiveError("also refused"))
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert [str(error) for error in recording.errors] == ["also refused"]
    rows_when(shown, lambda rows: len(rows) == 1)
    assert healthy.rows_published == 1
    assert (
        f"{FLUSH_URL}: the library could not stop stream 2 with this error: refused " + LIBRARY_BUG
    ) in caplog.text


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
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert [update[1].rows_published for update in reported] == [1]
    assert (
        "wss://h/stocks channel T: the library could not report the fields Massive sent "
        f"beyond its documentation. {LIBRARY_BUG}"
    ) in caplog.text


class _BrokenFlusher(Flusher):
    __slots__ = ("flushes",)

    def __init__(self, core: SessionCore) -> None:
        super().__init__(core.exec_ctx, "test-flush", url=FLUSH_URL, interval=0.001)
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
    assert (
        f"{FLUSH_URL}: publishing one batch of rows failed, and the next batch goes on. "
        + LIBRARY_BUG
    ) in caplog.text


def test_an_unstarted_flusher_joins_at_once(massive_session: dm.MassiveSession) -> None:
    assert Flusher(massive_session._core.exec_ctx, "never", url=FLUSH_URL, interval=1.0).join(0.0)


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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    symbols = [f"S{i}" for i in range(streams)]
    rows = [trade(symbols[i % streams], float(i)) for i in range(rows_per_flush)]
    sinks = [make_sink(core, (symbol,))[0] for symbol in symbols]

    def shared() -> None:
        converted = plan.convert(frame(rows), (), {}, arrived=arrivals(rows))
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
            batch = plan.convert(frame(own), (), {}, arrived=arrivals(own)).batch
            sink.publisher.add(dataset_table(batch, plan.date_columns))

    def stored() -> None:
        dataset_table(
            plan.convert(frame(rows), (), {}, arrived=arrivals(rows)).batch, plan.date_columns
        )

    def converted_only() -> None:
        to_table(plan.convert(frame(rows), (), {}, arrived=arrivals(rows)).batch)

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
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    rows = [trade(f"S{i % distinct}", float(i)) for i in range(rows_per_flush)]
    converted = plan.convert(frame(rows), (), {}, arrived=arrivals(rows))
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
    def gone() -> None:
        return None  # the sink was collected

    stream._fail_later(gone, "late")


def test_skipped_watchlist_rows_are_spans_of_the_streams_problems(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # What the watch's `on_skipped` does, called here from a Python thread, which line coverage
    # before 3.14 can see: each skipped value is a span, which ends once none skips it.
    core = massive_session._core
    sink, _ = make_sink(core, ALL)
    problems: list[Problem] = []
    monkeypatch.setattr(core, "problems", problems.extend)
    rule = SymbolRowRule(
        "test.trades", AssetClass.STOCKS, as_written=False, noncanonical=dm.OnError.LOG
    )
    report = functools.partial(stream._report_problems, weakref.ref(sink))
    skipped = SkippedValues(rule, stream_id=sink.stream_id, report=report)
    skipped.update(("msft",))
    skipped.update(("msft",))  # unchanged: nothing reported
    skipped.update(())
    spans = [p for p in problems if isinstance(p, ProblemSpans)]
    assert [(p.cause, p.stream_id, list(p.spans)) for p in spans] == [
        (ProblemCause.NONCANONICAL_SYMBOLS, sink.stream_id, ["skipped None 'msft'"]),
        (ProblemCause.NONCANONICAL_SYMBOLS, sink.stream_id, []),
    ]
    assert spans[0].spans["skipped None 'msft'"].detail == rule.refusal("msft")
    assert spans[0].policy is dm.OnError.LOG


def test_problems_of_a_finished_stream_go_nowhere() -> None:
    def gone() -> None:
        return None  # the sink was collected

    stream._report_problems(gone, [])


def test_streamed_rows_of_one_day_share_one_stored_date(
    massive_session: dm.MassiveSession, string_objects: Callable[[Table, str], int]
) -> None:
    core = massive_session._core
    spec = core.catalog.by_name["stocks.trades"]
    plan = ChannelPlan.build(core.catalog, spec, "T")
    flusher = Flusher(core.exec_ctx, "test-flush", url=FLUSH_URL, interval=3600.0)
    opened = open_stream_tables(
        plan,
        core.catalog.dataset_schema(spec),
        ALL,
        core=core,
        policy=UNREADABLE_FAILS,
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
    flusher.rows("T", frame([trade("AAPL"), trade("MSFT"), trade("IBM")]), {}, generation=1)
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
        return sum("rows wait to be published" in r.getMessage() for r in caplog.records)

    flusher.rows(
        "Q", frame([trade("AAPL")]), {}, generation=1
    )  # no stream ever read Q: dropped, never buffered
    flusher.rows("T", frame([trade("AAPL")] * 4), {}, generation=1)
    assert (flusher.backlog, warnings()) == (4, 0)  # at the level, not above it
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    # Each buffered frame holds its arrival time with its rows: the backlog counts the rows.
    flusher.rows("T", frame([trade("AAPL")] * 3), {}, generation=1)
    assert (flusher.backlog, warnings()) == (8, 1)  # logged once while it stays above
    assert [r.getMessage() for r in caplog.records] == [
        (
            f"{FLUSH_URL}: 5 rows wait to be published, more than 4. Deephaven's update graph is "
            "falling behind the stream, as it does while a console command runs, so memory grows "
            "until it catches up. Past 1024 MB, the live tables of this connection stop."
        )
    ]
    flusher.flush()
    assert flusher.backlog == 0  # nothing was dropped: the flush took every row
    flusher.rows("T", frame([trade("AAPL")] * 5), {}, generation=1)
    assert warnings() == 2  # below half the level in between, so it is logged again


def test_the_backlog_warning_waits_until_the_backlog_falls_below_half(
    massive_session: dm.MassiveSession, flusher: Flusher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream, "BACKLOG_WARNING_ROWS", 4)
    sink, _ = make_sink(massive_session._core, ("AAPL",))
    flusher.routes.add("T", sink)
    flusher.rows("T", frame([trade("AAPL")] * 5), {}, generation=1)
    assert flusher._backlog_warned
    flusher._drained(3, 0)  # as a flush that took three of the five would
    assert flusher._backlog_warned  # two rows are not below half of four
    flusher._drained(1, 0)
    assert not flusher._backlog_warned


def test_rows_past_the_memory_limit_fail_every_stream_routed_while_they_were_dropped(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    rows_when: RowsWhen,
) -> None:
    # Two frames fit; the third would pass the limit, so it and every frame after it are
    # dropped. The next flush fails each stream routed then, the one routed during the drops
    # too, and frees the buffered rows; a stream routed after that flush gets its rows.
    caplog.set_level(logging.WARNING, logger=stream.__name__)
    two = frame([trade("AAPL")] * 2)
    monkeypatch.setattr(stream, "HELD_BYTES_LIMIT", 2 * frame_bytes(two))
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    first, _ = make_sink(core, ("AAPL",), plan, stream_id=1)
    flusher.routes.add("T", first)
    flusher.rows("T", two, {}, generation=1)
    flusher.rows("T", frame([trade("AAPL")] * 2), {}, generation=1)
    assert (flusher.backlog, flusher._backlog_bytes) == (4, 2 * frame_bytes(two))
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)  # past the limit: dropped
    during, _ = make_sink(core, ("AAPL",), plan, stream_id=2)
    flusher.routes.add("T", during)
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)  # dropped: still over
    assert flusher.backlog == 4
    dropping = [r.getMessage() for r in caplog.records if "drops the rows" in r.getMessage()]
    assert dropping == [  # logged once, at the frame that passed the limit
        (
            f"{FLUSH_URL}: 4 rows, about 0 MB, wait to be published, and 1 more on channel T would "
            "pass the 0 MB the library keeps for one connection. Deephaven's update graph fell "
            "behind the stream, or stopped, as it does while a console command runs. The library "
            "drops the rows from now on and stops every live table of this connection."
        )
    ]
    flusher.flush()
    assert first.failed
    assert during.failed
    assert (first.rows_published, during.rows_published) == (0, 0)
    assert (flusher.backlog, flusher._backlog_bytes) == (0, 0)
    failures = [r.getMessage() for r in caplog.records if "stream 1 failed" in r.getMessage()]
    (failure,) = failures
    # The two dropped frames held a row each.
    assert re.fullmatch(
        re.escape(
            "test.trades stream 1 failed: test.trades: 4 rows, about 0 MB, waited to be "
            "published, and 2 more would pass the 0 MB the library keeps for one connection. "
            "Deephaven's update graph fell behind the stream, or stopped, as it does while a "
            "console command runs. The rows from "
        )
        + r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ"
        + re.escape(
            " on are missing, so this table stopped. Call the method again for a new live table."
        ),
        failure,
    )
    after, shown = make_sink(core, ("AAPL",), plan, stream_id=3)
    kept = blink_to_append_only(shown)  # before the flush, so no cycle's rows pass it by
    flusher.routes.add("T", after)
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert not after.failed
    rows_when(kept, lambda rows: len(rows) == 1)


def test_the_missing_rows_date_from_the_oldest_frame_the_flush_discards(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The frame buffered before the drops is discarded too, so the rows are missing from its
    # arrival, not from the first dropped frame's.
    caplog.set_level(logging.WARNING, logger=stream.__name__)
    one = frame([trade("AAPL")])
    monkeypatch.setattr(stream, "HELD_BYTES_LIMIT", frame_bytes(one))
    sink, _ = make_sink(massive_session._core, ("AAPL",), stream_id=1)
    flusher.routes.add("T", sink)
    flusher.rows("T", one, {}, generation=1)
    buffer = flusher.routes.buffer("T")
    assert buffer is not None
    (_, generation, rows, marks, nbytes) = buffer.get_nowait()
    buffer.put((1_000_000_000, generation, rows, marks, nbytes))  # arrived at 1970-01-01T00:00:01Z
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)  # past the limit
    flusher.flush()
    assert sink.failed
    (failure,) = [r.getMessage() for r in caplog.records if "stream 1 failed" in r.getMessage()]
    assert "The rows from 1970-01-01T00:00:01Z on are missing" in failure


def test_a_stream_routed_as_a_flush_takes_the_overflow_misses_no_row(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
    rows_when: RowsWhen,
) -> None:
    # The flush lets frames be buffered again before it reads the routes to fail: a stream
    # routed just after that read, whose frame comes at once, keeps running with its row.
    one = frame([trade("AAPL")])
    monkeypatch.setattr(stream, "HELD_BYTES_LIMIT", frame_bytes(one))
    core = massive_session._core
    plan = ChannelPlan.build(core.catalog, TRADES, "T")
    early, _ = make_sink(core, ("AAPL",), plan, stream_id=1)
    late, shown = make_sink(core, ("AAPL",), plan, stream_id=2)
    kept = blink_to_append_only(shown)  # before the flush, so no cycle's rows pass it by
    flusher.routes.add("T", early)
    flusher.rows("T", one, {}, generation=1)
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)  # past the limit
    routes_type = type(flusher.routes)
    sinks = routes_type.sinks

    def routing_after(routes: object) -> tuple[StreamSink, ...]:
        found = sinks(flusher.routes)
        monkeypatch.setattr(routes_type, "sinks", sinks)
        flusher.routes.add("T", late)
        flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)
        return found

    monkeypatch.setattr(routes_type, "sinks", routing_after)
    flusher.flush()
    assert early.failed
    assert not late.failed
    # The frame that came after the routing; the frames buffered before were discarded.
    rows_when(kept, lambda rows: len(rows) == 1)


@pytest.mark.benchmark
@pytest.mark.parametrize("rows_per_frame", [1, 10, 100])
def test_benchmark_flush_conversion(
    massive_session: dm.MassiveSession, capsys: pytest.CaptureFixture[str], rows_per_frame: int
) -> None:
    # Records, never gates: the flush thread's cost of turning 10,000 rows, decoded from frames
    # of `rows_per_frame` rows each, into one batch of the dataset's schema.
    plan = ChannelPlan.build(massive_session._core.catalog, TRADES, "T")
    frames = [
        frame([trade(f"S{(f + i) % 50}", float(i)) for i in range(rows_per_frame)])
        for f in range(10_000 // rows_per_frame)
    ]
    rows = [trade("S0")] * sum(map(len, frames))

    def convert() -> None:
        # As a flush gathers the frames' rows.
        plan.convert([row for rows in frames for row in rows], (), {}, arrived=arrivals(rows))

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


def test_a_flush_hands_events_to_live_sessions_only_and_survives_one_that_raises(
    *,
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    core = massive_session._core
    gone, _ = make_sink(core, ("AAPL",), stream_id=1)
    failing, _ = make_sink(core, ("AAPL",), stream_id=2)
    live, _ = make_sink(core, ("AAPL",), stream_id=3)
    alive = StreamSink.core
    # Stream 1's session was collected; stream 2's sink of problems raises.
    monkeypatch.setattr(
        StreamSink, "core", lambda self: None if self.stream_id == 1 else alive(self)
    )
    seen: list[Problem] = []

    def problems(found: Sequence[Problem]) -> None:
        if found[0].stream_id == 2:
            raise RuntimeError("broken sink")
        seen.extend(found)

    monkeypatch.setattr(core, "problems", problems)

    def event(stream_id: int) -> ProblemEvent:
        return ProblemEvent(
            cause=ProblemCause.UNCONVERTIBLE_VALUES,
            action=ProblemAction.WITHHELD,
            dataset="test.trades",
            stream_id=stream_id,
            where="flush channel T",
            symbol="AAPL",
            field="Price",
            rows=1,
            detail="Price holds 'x'",
            at=0,
        )

    with caplog.at_level(logging.ERROR, stream.__name__):
        flusher._report_events([(sink, (event(sink.stream_id),)) for sink in (gone, failing, live)])
    assert [e.stream_id for e in seen] == [3]
    assert [r.getMessage().split(". ")[0] for r in caplog.records] == [
        f"{FLUSH_URL}: the library could not report the problems of stream 2"
    ]


def test_a_named_stream_never_fails_for_another_symbol_s_withheld_rows(
    massive_session: dm.MassiveSession, flusher: Flusher
) -> None:
    # AAPL and MSFT share channel T. A run of MSFT rows the library cannot read, with no AAPL
    # row at all (after hours, say), marks MSFT stale in its stream, which fails no stream; the
    # AAPL stream received none of those rows.
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",), stream_id=1)
    msft, _ = make_sink(core, ("MSFT",), stream_id=2)
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    odd = [trade("MSFT", SipTimestamp=5)] * stream.DROPPED_RUN_LIMIT
    flusher.rows("T", frame(odd), {}, generation=1)
    flusher.flush()
    assert (aapl.failed, msft.failed) == (False, False)
    assert list(flusher._stale) == [2]


def test_a_run_of_rows_with_no_readable_symbol_fails_the_named_streams(
    massive_session: dm.MassiveSession, flusher: Flusher, caplog: pytest.LogCaptureFixture
) -> None:
    # Such a row could have been any stream's, so a run of them with nothing published fails
    # the named streams too: otherwise they would wait, empty, with no error.
    core = massive_session._core
    aapl, _ = make_sink(core, ("AAPL",), stream_id=1, policy=resolve_policy(None, dm.ErrorPolicy()))
    flusher.routes.add("T", aapl)
    half = stream.DROPPED_RUN_LIMIT // 2
    nameless = frame([trade("AAPL", Ticker=None)] * half)
    flusher.rows("T", nameless, unread_keys(*range(half)), generation=1)
    flusher.flush()
    states = [aapl.failed]
    flusher.rows("T", frame([trade("AAPL")]), {}, generation=1)  # a published row resets it
    flusher.flush()
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            flusher.rows("T", nameless, unread_keys(*range(half)), generation=1)
            flusher.flush()
    states.append(aapl.failed)
    assert states == [False, True]
    [record] = [r for r in caplog.records if "could not read the symbol" in r.getMessage()]
    assert record.levelno == logging.WARNING  # Massive's data, no bug of the library
    assert record.getMessage() == (
        "test.trades stream 1 failed: test.trades: the library could not read the symbol of "
        "the last 100 rows Massive sent on channel T, so it withheld them and published none. "
        "A T row: no Ticker. m.problems() counts the withheld rows by cause and symbol."
    )
    assert flusher._unrouted == {}


def test_each_stream_records_only_its_own_withheld_rows_under_any_policy(
    massive_session: dm.MassiveSession,
    flusher: Flusher,
    *,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An AAPL row is withheld: the AAPL stream gets its event, the MSFT stream does not.
    core = massive_session._core
    quiet = resolve_policy(dm.ErrorPolicy(unreadable_keys=OnError.IGNORE), dm.ErrorPolicy())
    aapl, _ = make_sink(core, ("AAPL",), stream_id=1, policy=quiet)
    msft, _ = make_sink(core, ("MSFT",), stream_id=2, policy=quiet)
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    events = capture_problems(core, monkeypatch)
    rows = [trade("AAPL", SipTimestamp=5), trade("AAPL"), trade("MSFT")]
    flusher.rows("T", frame(rows), {}, generation=1)
    flusher.flush()
    assert [(e.stream_id, e.rows, e.policy) for e in events] == [(1, 1, OnError.IGNORE)]


def test_a_stream_reads_its_symbols_once_for_a_batch_s_withheld_rows(
    massive_session: dm.MassiveSession, flusher: Flusher, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The stream drops AAPL from its symbols while the batch is judged: its count and its
    # events come from one reading, so a counted row always has its event.
    core = massive_session._core
    events = capture_problems(core, monkeypatch)
    aapl, _ = make_sink(core, ("AAPL",), stream_id=1)
    flusher.routes.add("T", aapl)
    answers = iter([True])
    monkeypatch.setattr(stream.StreamSink, "holds", lambda _sink, _symbol: next(answers, False))
    flusher.rows("T", frame([trade("AAPL", SipTimestamp=5), trade("AAPL")]), {}, generation=1)
    flusher.flush()
    assert [(event.symbol, event.rows) for event in events] == [("AAPL", 1)]


def test_rows_with_no_readable_symbol_beside_another_symbol_s_rows_never_fail_a_quiet_stream(
    massive_session: dm.MassiveSession, flusher: Flusher, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    # AAPL and MSFT share channel T, and MSFT keeps publishing. Rows with no readable symbol
    # could be MSFT's, so they never fail the quiet AAPL stream; each one is still a problem
    # of AAPL's, since it could have been AAPL's.
    core = massive_session._core
    events = capture_problems(core, monkeypatch)
    aapl, _ = make_sink(core, ("AAPL",), stream_id=1)
    msft, _ = make_sink(core, ("MSFT",), stream_id=2)
    flusher.routes.add("T", aapl)
    flusher.routes.add("T", msft)
    nameless = [trade("AAPL", Ticker=None)] * 10
    for _ in range(stream.DROPPED_RUN_LIMIT // 10 + 1):
        flusher.rows("T", frame([*nameless, trade("MSFT")]), unread_keys(*range(10)), generation=1)
        flusher.flush()
    assert (aapl.failed, msft.failed) == (False, False)
    counted = sum(e.rows for e in events if e.stream_id == 1)
    assert counted == 10 * (stream.DROPPED_RUN_LIMIT // 10 + 1)
