"""Tests for the diagnostics tables and their writer thread."""

import contextlib
import dataclasses
import functools
import logging
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pyarrow as pa
import pytest
from deephaven import new_table
from deephaven.column import string_col
from deephaven.dtypes import DType
from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table
from deephaven.table_factory import InputTable
from deephaven.table_listener import TableUpdate, listen
from deephaven.update_graph import exclusive_lock
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets.schema import Schema
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._massive import DriftReport, DriftSample, Transport
from deephaven_massive._tables import diagnostics
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.diagnostics import (
    CONNECTION_KEYS,
    CONNECTIONS,
    FLAT_FILES_AND_REST,
    GAPS_SHOWN,
    QUERY_SHOWN,
    RATE_LIMIT_KEYS,
    RATE_LIMITS,
    SCHEMA_DRIFT_SHOWN,
    STREAM_KEYS,
    STREAMS,
    WRITE_INTERVAL,
    ConnectionRow,
    ConnectionState,
    Diagnostics,
    DriftRegistry,
    LimitSource,
    NotePart,
    RateLimitRow,
    RateLimitState,
    StreamKind,
    StreamRow,
    StreamState,
    StreamUpdate,
    as_row,
    describe_symbols,
    gaps_text,
    shown_query,
    thread_died,
)
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.symbol_read_rows import (
    SYMBOL_READ_KEYS,
    SYMBOL_READS,
    SymbolReadRow,
    SymbolReadState,
)

Rows = Callable[[Table], list[dict[str, Any]]]
TIMEOUT = 30.0

STREAM = StreamRow(
    stream_id=7,
    kind=StreamKind.STREAM,
    dataset="stocks.trades",
    query="<StreamHistoryLatestQuery stocks.trades symbols=('AAPL', 'MSFT')>",
    symbol_count=2,
    symbols="AAPL, MSFT",
    held_results=1,
    transport=Transport.WS,
    created_at=1_700_000_000_000_000_000,
)


def test_column_sets() -> None:
    assert CONNECTIONS.column_names == (
        "Transport",
        "AssetClass",
        "Family",
        "Host",
        "Path",
        "Recency",
        "Evidence",
        "State",
        "Topics",
        "AckedTopics",
        "PendingTopics",
        "NoAckTopics",
        "BufferedRows",
        "Reconnects",
        "LastMessageAt",
        "NextReconnectAt",
        "UpdatedAt",
        "LastError",
        "SchemaDrift",
    )
    assert STREAMS.column_names == (
        "StreamId",
        "Kind",
        "Dataset",
        "Query",
        "SymbolCount",
        "Symbols",
        "HeldResults",
        "Deferred",
        "Transport",
        "Host",
        "Path",
        "Recency",
        "RowsPublished",
        "Loading",
        "RequestsPerCycle",
        "State",
        "Error",
        "Gaps",
        "CreatedAt",
        "SchemaDrift",
    )
    assert [s.value for s in StreamState] == [
        "RUNNING",
        "LOADING",
        "DONE",
        "FAILED",
        "RELEASED",
        "CLOSED",
    ]
    assert [k.value for k in StreamKind] == ["stream", "poll", "read"]
    assert [s.value for s in ConnectionState] == [
        "RESOLVING",
        "CONNECTING",
        "CONNECTED",
        "WAITING_SLOT",
        "RECONNECTING",
        "FAILED",
        "CLOSED",
    ]
    assert RATE_LIMITS.column_names == (
        "Bucket",
        "State",
        "RequestsPerMinute",
        "LimitSource",
        "InFlight",
        "Waiting",
        "RecentRequests",
        "Attempts",
        "Refusals",
        "PausedUntil",
        "LastRefusal",
        "UpdatedAt",
    )
    assert [s.value for s in RateLimitState] == ["READY", "PACED", "PAUSED"]
    assert [s.value for s in LimitSource] == ["DECLARED", "LEARNED"]


def test_stream_updates_set_only_the_given_fields() -> None:
    row = dataclasses.replace(STREAM, host="socket.massive.com", rows_published=3)
    assert StreamUpdate().apply(row) == row
    updated = StreamUpdate(rows_published=10, deferred=2, symbol_count=5).apply(row)
    assert (updated.rows_published, updated.deferred, updated.symbol_count) == (10, 2, 5)
    assert updated.host == "socket.massive.com"
    slow = StreamUpdate(notes={NotePart.QUIET: "slow"}).apply(row)
    assert as_row(slow)["Error"] == "slow"


def test_warnings_of_several_sources_coexist_and_clear_alone() -> None:
    # The connection's outage and options-cap notes and the flush thread's stale symbols are
    # three writers of one Error: each sets or clears only its own part, shown in NotePart
    # order, so no source ever replaces or clears another's warning.
    row = dataclasses.replace(STREAM, deferred=2)
    stale = StreamUpdate(notes={NotePart.STALE_SYMBOLS: "rows for ESH5 are being dropped"})
    cap = StreamUpdate(notes={NotePart.CAP: "over capacity: 1002/1000; 2 deferred"})
    down = StreamUpdate(notes={NotePart.OUTAGE: "connection down since T; no data"})
    three = down.apply(cap.apply(stale.apply(row)))
    assert as_row(three)["Error"] == (
        "connection down since T; no data; over capacity: 1002/1000; 2 deferred; rows for ESH5 "
        "are being dropped"
    )
    back = StreamUpdate(notes={NotePart.OUTAGE: None}).apply(three)  # the feed is back
    assert as_row(back)["Error"] == (
        "over capacity: 1002/1000; 2 deferred; rows for ESH5 are being dropped"
    )
    freed = StreamUpdate(deferred=0, notes={NotePart.CAP: None}).apply(back)  # the cap frees up
    assert as_row(freed)["Error"] == "rows for ESH5 are being dropped"
    cleared = StreamUpdate(notes={NotePart.STALE_SYMBOLS: None, NotePart.QUIET: None})
    assert as_row(cleared.apply(freed))["Error"] is None  # clearing an absent part is harmless
    again = StreamUpdate(notes={NotePart.STALE_SYMBOLS: "rows for NQH5 are being dropped"})
    assert str(as_row(again.apply(three))["Error"]).endswith("rows for NQH5 are being dropped")


def test_skipped_symbols_and_the_transport_warning_neither_hide_nor_clear_each_other() -> None:
    cap = StreamUpdate(notes={NotePart.CAP: "over capacity: 1002/1000; 2 deferred"})
    row = cap.apply(dataclasses.replace(STREAM, deferred=2))
    both = StreamUpdate(skipped="skipping 1 value").apply(row)
    assert as_row(both)["Error"] == "over capacity: 1002/1000; 2 deferred; skipping 1 value"
    alone = StreamUpdate(deferred=0, notes={NotePart.CAP: None}).apply(both)  # the cap frees up
    assert as_row(alone)["Error"] == "skipping 1 value"
    assert StreamUpdate(rows_published=1).apply(alone).skipped == "skipping 1 value"
    back = StreamUpdate(notes={NotePart.OUTAGE: "waiting for a slot"}).apply(alone)
    fixed = StreamUpdate(clear_skipped=True, skipped="ignored").apply(back)  # the row is fixed
    assert as_row(fixed)["Error"] == "waiting for a slot"
    assert as_row(StreamUpdate(notes={NotePart.OUTAGE: None}).apply(fixed))["Error"] is None
    stopped = dataclasses.replace(both, state=StreamState.FAILED, error="the cause")
    assert as_row(stopped)["Error"] == "the cause"  # a stop shows only its cause


@pytest.mark.parametrize("state", [StreamState.FAILED, StreamState.CLOSED])
def test_stream_updates_keep_the_cause_of_a_stop(state: StreamState) -> None:
    # A flush or a poll cycle that races a failure or the close never replaces its cause.
    row = dataclasses.replace(STREAM, state=state, error="the cause")
    for text in ("a warning", None):
        updated = StreamUpdate(notes={NotePart.POLL_CYCLE: text}).apply(row)
        assert (updated.state, updated.error, as_row(updated)["Error"]) == (
            state,
            "the cause",
            "the cause",
        )
    assert StreamUpdate(rows_published=7).apply(row).rows_published == 7  # counts still move


def test_rows_map_fields_to_columns() -> None:
    assert list(as_row(STREAM)) == list(STREAMS.column_names)
    connection = ConnectionRow(
        transport=Transport.REST, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
    )
    assert list(as_row(connection)) == list(CONNECTIONS.column_names)


def test_describe_symbols(dh_exec_ctx: ExecutionContext) -> None:
    assert describe_symbols(("AAPL", "MSFT")) == (2, "AAPL, MSFT")
    many = tuple(f"S{i:02}" for i in range(25))
    count, text = describe_symbols(many)
    assert count == 25
    assert text.endswith("S19 (+5 more)")
    assert describe_symbols(dm.ALL) == (None, "ALL")
    table = SymbolTable(new_table([string_col("Symbol", ["A"])]))
    assert describe_symbols(table) == (None, "reactive table")


def _diagnostics(exec_ctx: ExecutionContext) -> Diagnostics:
    # The session passes its reaper's `release_later`; releasing at once is the same here, where
    # no table is ever failed.
    return Diagnostics(
        exec_ctx,
        "test diagnostics",
        label="TestSession",
        release_later=functools.partial(release_scope, exec_ctx),
        on_death=print,
    )


@pytest.mark.parametrize(
    ("schema", "keys"),
    [
        (CONNECTIONS, CONNECTION_KEYS),
        (STREAMS, STREAM_KEYS),
        (RATE_LIMITS, RATE_LIMIT_KEYS),
        (SYMBOL_READS, SYMBOL_READ_KEYS),
    ],
    ids=["connections", "streams", "rate_limits", "symbol_reads"],
)
def test_each_table_has_its_schemas_columns_keys_first(
    dh_exec_ctx: ExecutionContext, schema: Schema, keys: tuple[str, ...]
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    table = getattr(diagnostics, schema.name)
    names = [column.name for column in schema.columns]
    assert names[: len(keys)] == list(keys)  # `last_by` puts the keys first
    assert [column.name for column in table.columns] == names
    assert [column.data_type for column in table.columns] == list(col_defs(schema).values())
    diagnostics.release()


def test_the_tables_are_not_input_tables(dh_exec_ctx: ExecutionContext) -> None:
    # Nobody can edit them in the web UI: no table carries the input-table attribute.
    diagnostics = _diagnostics(dh_exec_ctx)
    for table in (
        diagnostics.connections,
        diagnostics.streams,
        diagnostics.rate_limits,
        diagnostics.symbol_reads,
    ):
        assert not isinstance(table, InputTable)
        assert not table.j_table.hasAttribute("InputTable")
    diagnostics.release()


def test_rows_replace_rows_by_key_in_order(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    diagnostics.write(STREAM)
    diagnostics.write(dataclasses.replace(STREAM, held_results=0, state=StreamState.RELEASED))
    diagnostics.write(dataclasses.replace(STREAM, stream_id=8))
    diagnostics.write(
        ConnectionRow(
            transport=Transport.REST,
            asset_class=AssetClass.STOCKS,
            family=ChannelFamily.TICK,
            host="delayed.massive.com",
            recency=Recency.DELAYED,
            evidence="last_trade.timeframe=DELAYED",
        )
    )
    diagnostics.write(
        ConnectionRow(
            transport=Transport.WS,
            asset_class=AssetClass.STOCKS,
            family=ChannelFamily.TICK,
            state=ConnectionState.CONNECTED,
        )
    )
    assert diagnostics.wait_until_shown(TIMEOUT)
    rows = sorted(dh_to_pylist(diagnostics.streams), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["HeldResults"], r["State"], r["Transport"]) for r in rows] == [
        (7, 0, "RELEASED", "WebSocket"),
        (8, 1, "RUNNING", "WebSocket"),
    ]
    connections = dh_to_pylist(diagnostics.connections)
    assert {(r["Transport"], r["AssetClass"], r["State"], r["Recency"]) for r in connections} == {
        ("REST", "stocks", "RESOLVING", "DELAYED"),
        ("WebSocket", "stocks", "CONNECTED", None),
    }
    diagnostics.release()


def test_two_adds_of_one_key_in_one_cycle_show_the_latest(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    # Two batches can land in one update cycle: `last_by` keeps the later row, as a keyed
    # input table did. No cycle runs under the lock, so both adds wait for the same one.
    diagnostics = _diagnostics(dh_exec_ctx)
    streams = diagnostics._streams
    with exclusive_lock(dh_exec_ctx.update_graph):
        streams.add([dataclasses.replace(STREAM, held_results=1)])
        streams.add([dataclasses.replace(STREAM, held_results=2)])
    assert wait_for_update_cycle(dh_exec_ctx, TIMEOUT)
    assert [r["HeldResults"] for r in dh_to_pylist(diagnostics.streams)] == [2]
    diagnostics.release()


def test_new_keys_append_in_first_seen_order_and_an_update_is_a_modify(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    for stream_id in (7, 3, 9):
        diagnostics.write(dataclasses.replace(STREAM, stream_id=stream_id))
    assert diagnostics.wait_until_shown(TIMEOUT)
    updates: list[tuple[int, int, int]] = []

    def on_update(update: TableUpdate, is_replay: bool) -> None:
        j_update = update.j_table_update
        updates.append(
            (j_update.added().size(), j_update.modified().size(), j_update.removed().size())
        )

    handle = listen(diagnostics.streams, on_update)
    try:
        diagnostics.write(dataclasses.replace(STREAM, stream_id=5))
        assert diagnostics.wait_until_shown(TIMEOUT)
        diagnostics.write(dataclasses.replace(STREAM, stream_id=3, held_results=4))
        assert diagnostics.wait_until_shown(TIMEOUT)
    finally:
        handle.stop()
    rows = dh_to_pylist(diagnostics.streams)
    assert [(r["StreamId"], r["HeldResults"]) for r in rows] == [(7, 1), (3, 4), (9, 1), (5, 1)]
    assert updates == [(1, 0, 0), (0, 1, 0)]  # the new key is added, the known one modified
    diagnostics.release()


def test_failed_writes_are_logged_and_the_writer_continues(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(
            dataclasses.replace(
                STREAM,
                stream_id=8,
                dataset=5,  # type: ignore[arg-type]  # the point: a row that cannot be written
            )
        )
        diagnostics.write(STREAM)
        assert diagnostics.wait_until_shown(TIMEOUT)
    assert "failed to write a streams row" in caplog.text
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    diagnostics.release()


class _BrokenPublisher:
    # A diagnostics table's publisher whose add fails.
    def add(self, rows: Table) -> None:
        raise RuntimeError("the streams table is broken")


def test_a_failing_table_does_not_cost_the_other_its_rows(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    broken: BlinkPublisher = _BrokenPublisher()  # type: ignore[assignment]  # a fake whose add fails
    diagnostics._streams.publisher = broken
    connection = ConnectionRow(
        transport=Transport.WS, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
    )
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(STREAM)
        diagnostics.write(connection)
        assert diagnostics.wait_until_shown(TIMEOUT)
    assert "failed to update the streams table" in caplog.text
    assert len(dh_to_pylist(diagnostics.connections)) == 1  # added despite the failure
    diagnostics.release()


class _CountingPublisher:
    # Wraps a publisher, records when each `add` began and ended, and holds the first one until
    # released, so rows queued meanwhile reach the writer as one batch.
    def __init__(self, publisher: BlinkPublisher, *, hold: bool = True) -> None:
        self.publisher = publisher
        self.adds: list[float] = []
        self.ends: list[float] = []
        self.entered = threading.Event()
        self.gate = threading.Event()
        if not hold:
            self.gate.set()

    def add(self, rows: Table) -> None:
        self.adds.append(time.monotonic())
        self.entered.set()
        assert self.gate.wait(TIMEOUT)
        self.publisher.add(rows)
        self.ends.append(time.monotonic())


def _count_adds(diagnostics: Diagnostics, *, hold: bool = True) -> _CountingPublisher:
    counting = _CountingPublisher(diagnostics._streams.publisher, hold=hold)
    publisher: BlinkPublisher = counting  # type: ignore[assignment]  # a fake that counts adds
    diagnostics._streams.publisher = publisher
    return counting


def test_a_batch_is_converted_once(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    to_arrow = Schema.to_arrow

    def counting(self: Schema, rows: Sequence[Mapping[str, object]]) -> pa.Table:
        calls.append(len(rows))
        return to_arrow(self, rows)

    diagnostics = _diagnostics(dh_exec_ctx)
    monkeypatch.setattr(Schema, "to_arrow", counting)
    counting_adds = _count_adds(diagnostics)
    diagnostics.write(dataclasses.replace(STREAM, stream_id=100))
    assert counting_adds.entered.wait(TIMEOUT)  # the writer now holds its next batch back
    for n in range(3):
        diagnostics.write(dataclasses.replace(STREAM, stream_id=n))
    counting_adds.gate.set()
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert calls == [1, 3]  # one conversion per batch, never one per row
    diagnostics.release()


def test_a_batch_of_rows_that_do_not_fit_adds_nothing(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    counting = _count_adds(diagnostics, hold=False)
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(
            dataclasses.replace(
                STREAM,
                stream_id=8,
                dataset=5,  # type: ignore[arg-type]  # the point: a row that cannot be written
            )
        )
        assert diagnostics.wait_until_shown(TIMEOUT)
    assert "failed to write a streams row" in caplog.text
    assert counting.adds == []
    assert dh_to_pylist(diagnostics.streams) == []
    diagnostics.release()


def test_queued_rows_are_coalesced_into_few_adds_at_least_an_interval_apart(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    counting = _count_adds(diagnostics)
    diagnostics.write(dataclasses.replace(STREAM, stream_id=0, held_results=-1))
    assert counting.entered.wait(TIMEOUT)  # the burst queues while the first add is held
    for n in range(1_000):
        diagnostics.write(dataclasses.replace(STREAM, stream_id=n % 10, held_results=n))
    counting.gate.set()
    assert diagnostics.wait_until_shown(TIMEOUT)
    # The writer waited out the interval after the held add before it took the burst.
    assert len(counting.adds) == 2
    gaps = [start - end for end, start in zip(counting.ends, counting.adds[1:], strict=False)]
    assert all(gap >= WRITE_INTERVAL for gap in gaps), gaps
    rows = sorted(dh_to_pylist(diagnostics.streams), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["HeldResults"]) for r in rows] == [(n, 990 + n) for n in range(10)]
    diagnostics.release()


def test_release_adds_queued_rows_then_releases(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    keeper = new_scope()
    keeper.manage(diagnostics.streams)
    diagnostics.write(STREAM)
    diagnostics.release()
    diagnostics.release()  # idempotent
    assert not diagnostics.thread.is_alive()
    assert diagnostics.wait_until_shown(TIMEOUT)  # the kept table takes the rows in
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    diagnostics.write(dataclasses.replace(STREAM, stream_id=9))  # dropped: the writer has stopped
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    release_scope(dh_exec_ctx, keeper)


def test_a_release_that_does_not_wait_lets_the_writer_finish_later(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    # A caller holding the update graph's lock, as a console command does, cannot wait for the
    # writer, which releases the scope under that lock: the release returns at once, and the
    # writer adds the queued rows, releases the tables, and ends once the lock is let go.
    diagnostics = _diagnostics(dh_exec_ctx)
    keeper = new_scope()
    keeper.manage(diagnostics.streams)
    with exclusive_lock(dh_exec_ctx.update_graph):
        diagnostics.write(STREAM)
        diagnostics.release(wait=False)
        diagnostics.write(dataclasses.replace(STREAM, stream_id=9))  # dropped: released
    assert diagnostics.wait_until_shown(TIMEOUT)  # the writer ends, then a cycle shows its rows
    assert not diagnostics.thread.is_alive()
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    release_scope(dh_exec_ctx, keeper)


def test_wait_until_shown_gives_up_after_its_timeout(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    counting = _count_adds(diagnostics)
    diagnostics.write(STREAM)
    assert counting.entered.wait(TIMEOUT)  # the writer is stuck in its add
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.diagnostics"):
        assert not diagnostics.wait_until_shown(0.0)
        diagnostics.release(wait=False)
        assert not diagnostics.wait_until_shown(0.0)  # the writer has not ended either
    assert caplog.text.count("did not add the queued rows within 0.0 s") == 2
    counting.gate.set()
    diagnostics.thread.join(TIMEOUT)
    assert not diagnostics.thread.is_alive()


def test_a_failed_table_creation_releases_the_scope(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    scopes: list[LivenessScope] = []
    real_scope = new_scope
    real_publisher = blink_publisher
    created: list[Table] = []

    def tracking_scope() -> LivenessScope:
        scope = real_scope()
        scopes.append(scope)
        return scope

    def second_fails(
        name: str,
        columns: dict[str, DType],
        *,
        on_shutdown: Callable[[], None],
        release_later: Callable[[LivenessScope], None],
    ) -> tuple[Table, BlinkPublisher]:
        if created:
            raise RuntimeError("no second table")
        blink, publisher = real_publisher(
            name, columns, on_shutdown=on_shutdown, release_later=release_later
        )
        created.append(blink)
        return blink, publisher

    monkeypatch.setattr(diagnostics, "new_scope", tracking_scope)
    monkeypatch.setattr(diagnostics, "blink_publisher", second_fails)
    with pytest.raises(RuntimeError, match="no second table"):
        _diagnostics(dh_exec_ctx)
    assert scopes[0].j_scope is None  # released, with the first table in it


def test_concurrent_writes_racing_a_release_leave_one_consistent_row_per_key(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    # Many threads write while the release runs (free-threaded too): nothing deadlocks, every
    # key shows one row, and each shown row is one that was written.
    diagnostics = _diagnostics(dh_exec_ctx)
    keeper = new_scope()
    keeper.manage(diagnostics.streams)
    start = threading.Barrier(9)

    def writer(stream_id: int) -> None:
        start.wait(TIMEOUT)
        for n in range(200):
            diagnostics.write(dataclasses.replace(STREAM, stream_id=stream_id, held_results=n))

    threads = [threading.Thread(target=writer, args=(stream_id,)) for stream_id in range(8)]
    for thread in threads:
        thread.start()
    diagnostics.write(STREAM)  # queued before the release, so it is always shown
    start.wait(TIMEOUT)
    diagnostics.release()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    assert diagnostics.wait_until_shown(TIMEOUT)
    rows = dh_to_pylist(diagnostics.streams)
    ids = [r["StreamId"] for r in rows]
    assert len(ids) == len(set(ids))  # one row per key
    assert 7 in ids
    assert all(0 <= r["HeldResults"] < 200 for r in rows)
    release_scope(dh_exec_ctx, keeper)


def test_describe_symbols_at_the_truncation_boundary() -> None:
    twenty = tuple(f"S{i:02}" for i in range(20))
    assert describe_symbols(twenty) == (20, ", ".join(twenty))  # exactly 20: no suffix
    count, text = describe_symbols((*twenty, "S20"))
    assert (count, text) == (21, ", ".join(twenty) + " (+1 more)")


def test_fresh_rows_start_at_zero() -> None:
    connection = as_row(
        ConnectionRow(
            transport=Transport.WS, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
        )
    )
    counts = (
        "Topics",
        "AckedTopics",
        "PendingTopics",
        "NoAckTopics",
        "BufferedRows",
        "Reconnects",
    )
    assert {name: connection[name] for name in counts} == dict.fromkeys(counts, 0)
    assert connection["State"] == "RESOLVING"
    stream = as_row(STREAM)
    assert (stream["Deferred"], stream["RowsPublished"], stream["State"], stream["Error"]) == (
        0,
        0,
        "RUNNING",
        None,
    )


def test_writes_after_release_are_not_queued(dh_exec_ctx: ExecutionContext) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    diagnostics.release()
    diagnostics.write(STREAM)
    assert diagnostics._writer._queue.empty()  # nothing would ever drain it


def test_connection_rows_replace_by_their_key(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    tick = ConnectionRow(
        transport=Transport.WS, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
    )
    diagnostics.write(dataclasses.replace(tick, state=ConnectionState.CONNECTING))
    diagnostics.write(dataclasses.replace(tick, state=ConnectionState.CONNECTED, reconnects=1))
    diagnostics.write(
        dataclasses.replace(tick, family=ChannelFamily.FMV, state=ConnectionState.FAILED)
    )
    assert diagnostics.wait_until_shown(TIMEOUT)
    rows = sorted(dh_to_pylist(diagnostics.connections), key=lambda r: r["Family"])
    assert [(r["Family"], r["State"], r["Reconnects"]) for r in rows] == [
        ("FMV", "FAILED", 0),
        ("TICK", "CONNECTED", 1),
    ]
    diagnostics.release()


def test_gaps_accumulate_and_outlast_the_note_that_announced_them() -> None:
    # 2026-11-02T14:00:00Z, in epoch nanoseconds, and one minute.
    start, minute = 1_793_628_000 * 10**9, 60 * 10**9
    row = STREAM
    assert as_row(row)["Gaps"] is None
    down = StreamUpdate(notes={NotePart.OUTAGE: "connection down since 2026-11-02T14:00:00Z"})
    row = down.apply(row)
    row = StreamUpdate(notes={NotePart.OUTAGE: None}, gap=(start, start + minute)).apply(row)
    assert as_row(row)["Error"] is None
    assert as_row(row)["Gaps"] == "2026-11-02T14:00:00Z to 2026-11-02T14:01:00Z"
    # A stopped stream still shows its gaps: they are history, not a warning.
    failed = dataclasses.replace(row, state=StreamState.FAILED, error="boom")
    assert as_row(failed)["Gaps"] == "2026-11-02T14:00:00Z to 2026-11-02T14:01:00Z"


def test_gaps_text_shows_the_latest_and_counts_the_earlier_ones() -> None:
    minute = 60 * 10**9
    start = 1_793_628_000 * 10**9
    gaps = [(start + 2 * n * minute, start + (2 * n + 1) * minute) for n in range(GAPS_SHOWN + 2)]
    text = gaps_text(gaps)
    assert text is not None
    assert text.startswith("2 earlier gaps; 2026-11-02T14:04:00Z to 2026-11-02T14:05:00Z; ")
    assert text.count(" to ") == GAPS_SHOWN
    one_more = gaps_text(gaps[1:])
    assert one_more is not None
    assert one_more.startswith("1 earlier gap; 2026-11-02T14:04:00Z")
    assert gaps_text([]) is None


def test_stream_updates_keep_schema_drift() -> None:
    row = dataclasses.replace(STREAM, schema_drift="WS:x+")
    assert StreamUpdate(rows_published=1).apply(row).schema_drift == "WS:x+"
    # Clearing the error never clears the drift: drift is not a failure.
    assert StreamUpdate(notes={NotePart.CAP: None}).apply(row).schema_drift == "WS:x+"


def test_schema_drift_reaches_both_tables(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    diagnostics.write(dataclasses.replace(STREAM, schema_drift="REST:halted+", error="boom"))
    diagnostics.write(
        ConnectionRow(
            transport=Transport.WS,
            asset_class=AssetClass.STOCKS,
            family=ChannelFamily.TICK,
            last_error="reset",
            schema_drift="T:trfi-",
        )
    )
    assert diagnostics.wait_until_shown(TIMEOUT)
    (stream,) = dh_to_pylist(diagnostics.streams)
    assert (stream["SchemaDrift"], stream["Error"]) == ("REST:halted+", "boom")
    (connection,) = dh_to_pylist(diagnostics.connections)
    assert (connection["SchemaDrift"], connection["LastError"]) == ("T:trfi-", "reset")
    diagnostics.release()


# The drift registry.

DRIFT_LOGGER = "deephaven_massive._tables.diagnostics"


# Where the drift was seen, as a transport names it in the WARNING.
WHERE = "GET /v3/trades/{stockTicker}"


def extra(dataset: str, transport: str, field: str) -> str:
    return (
        f"{dataset}: {transport} field {field!r} is not in the schema; it is dropped "
        f"(schema drift) at {WHERE}"
    )


def status(path: str, value: str) -> str:
    return f"{path}: undocumented response status {value!r}; the rows are kept (schema drift)"


def drift_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == DRIFT_LOGGER and r.levelno == logging.WARNING
    ]


def test_drift_warns_once_per_dataset_transport_and_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    drift = DriftReport(extra=frozenset({"halted"}), missing=frozenset({"trf_id"}))
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        for _ in range(3):
            registry.report("stocks.trades", Transport.FLAT_FILES, drift, where=WHERE)
        registry.report("stocks.trades", Transport.FLAT_FILES, DriftReport(), where=WHERE)
        # Another transport, or another dataset, is another warning.
        registry.report(
            "stocks.trades", Transport.REST, DriftReport(extra=frozenset({"halted"})), where=WHERE
        )
        registry.report(
            "stocks.quotes", Transport.FLAT_FILES, DriftReport(extra=drift.extra), where=WHERE
        )
    assert drift_warnings(caplog) == [
        extra("stocks.trades", "FLAT_FILES", "halted"),
        (
            "stocks.trades: FLAT_FILES field 'trf_id' is missing; it reads as null "
            f"(schema drift) at {WHERE}"
        ),
        extra("stocks.trades", "REST", "halted"),
        extra("stocks.quotes", "FLAT_FILES", "halted"),
    ]


def test_drift_warnings_are_sorted_within_a_report(caplog: pytest.LogCaptureFixture) -> None:
    registry = DriftRegistry()
    drift = DriftReport(extra=frozenset({"b", "a"}), missing=frozenset({"d", "c"}))
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("stocks.trades", Transport.WS, drift, where=WHERE)
    assert [message.split("'")[1] for message in drift_warnings(caplog)] == ["a", "b", "c", "d"]


def test_drift_ignores_statuses_in_report_and_warns_per_path_and_status(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report(
            "stocks.trades", Transport.REST, DriftReport(statuses=frozenset({"X"})), where=WHERE
        )
        assert drift_warnings(caplog) == []
        registry.report_statuses("/v3/trades/{stockTicker}", frozenset({"PARTIAL", "MAYBE"}))
        registry.report_statuses("/v3/trades/{stockTicker}", frozenset({"PARTIAL"}))
        registry.report_statuses("/v3/quotes/{stockTicker}", frozenset({"PARTIAL"}))
    assert drift_warnings(caplog) == [
        status("/v3/trades/{stockTicker}", "MAYBE"),
        status("/v3/trades/{stockTicker}", "PARTIAL"),
        status("/v3/quotes/{stockTicker}", "PARTIAL"),
    ]


def test_drift_is_recorded_per_owner_and_only_changes_are_returned() -> None:
    registry = DriftRegistry()
    assert registry.record(7, "REST", DriftReport()) is None
    assert registry.record(7, "REST", DriftReport(extra=frozenset({"x"}))) == "REST:x+"
    assert registry.record(7, "REST", DriftReport(extra=frozenset({"x"}))) is None
    both = DriftReport(extra=frozenset({"x"}), missing=frozenset({"a"}))
    assert registry.record(7, "REST", both) == "REST:a-, REST:x+"
    # A connection's key is another owner; its prefix is the channel.
    key = ("WS", "stocks", "TICK")
    assert registry.record(key, "T", DriftReport(missing=frozenset({"trfi"}))) == "T:trfi-"
    assert registry.record(key, "Q", DriftReport(missing=frozenset({"trfi"}))) == "Q:trfi-, T:trfi-"
    # A forgotten owner starts again; forgetting an unknown owner is harmless.
    registry.forget(7)
    registry.forget(99)
    assert registry.record(7, "REST", DriftReport(extra=frozenset({"y"}))) == "REST:y+"


def test_a_value_that_does_not_fit_warns_once_and_shows_with_a_question_mark(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    drift = DriftReport(
        unconvertible=frozenset({"x"}), samples={"x": DriftSample("C:EURUSD", '"4.8"')}
    )
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("forex.trades", Transport.WS, drift, where=WHERE)
        registry.report("forex.trades", Transport.WS, drift, where=WHERE)
    assert drift_warnings(caplog) == [
        (
            "forex.trades: WS field 'x' held a value that does not fit its type; it reads as null "
            f'(schema drift) at {WHERE}; symbol C:EURUSD; first value "4.8"'
        )
    ]
    assert registry.record(3, "WebSocket", drift) == "WebSocket:x?"


def test_dropped_rows_warn_once_per_dataset_then_count_at_debug(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = DriftRegistry()
    first = DriftReport(dropped_rows=2, dropped_reason="row 0: no Ticker")
    later = DriftReport(dropped_rows=3, dropped_reason="row 4: no Ticker")
    with caplog.at_level(logging.DEBUG, logger=DRIFT_LOGGER):
        registry.report("stocks.trades", Transport.REST, first, where=WHERE)
        # Within the interval, a drop is counted, not logged.
        registry.report("stocks.trades", Transport.WS, later, where="wss://h/stocks channel T")
        registry.report("stocks.trades", Transport.REST, DriftReport(), where=WHERE)
        registry.report("stocks.quotes", Transport.REST, first, where=WHERE)
        # Once the interval has passed, the next drop logs the running count.
        monkeypatch.setattr(diagnostics, "DROPPED_COUNT_INTERVAL", 0.0)
        registry.report("stocks.trades", Transport.WS, later, where="wss://h/stocks channel T")
    assert [(r.levelno, r.getMessage()) for r in caplog.records[:2]] == [
        (
            logging.WARNING,
            (
                f"stocks.trades: dropped 2 REST row(s) whose key cannot be read, the first at "
                f"{WHERE}: row 0: no Ticker; such rows are dropped and counted: the running "
                "count is logged at DEBUG and shown in the SchemaDrift of each live table that "
                "lost them"
            ),
        ),
        (
            logging.WARNING,
            (
                f"stocks.quotes: dropped 2 REST row(s) whose key cannot be read, the first at "
                f"{WHERE}: row 0: no Ticker; such rows are dropped and counted: the running "
                "count is logged at DEBUG and shown in the SchemaDrift of each live table that "
                "lost them"
            ),
        ),
    ]
    (counted,) = caplog.records[2:]
    assert counted.levelno == logging.DEBUG
    # Every drop since the WARNING: 3 counted silently, then 3 more.
    assert re.fullmatch(
        r"stocks\.trades: dropped 6 more row\(s\) whose key cannot be read in the last \d+ s "
        r"\(8 so far\); the latest WS at wss://h/stocks channel T: row 4: no Ticker",
        counted.getMessage(),
    )


def test_dropped_rows_are_counted_in_the_owners_schema_drift() -> None:
    # After its one WARNING, a dropped row stays visible: each owner's row counts its drops,
    # per prefix, every count before every field. A count counts reads: a report of a row
    # read again adds to it again.
    registry = DriftRegistry()
    first = DriftReport(dropped_rows=2, dropped_reason="row 0: no Ticker")
    assert registry.record(1, "REST", first) == "REST:2 rows dropped"
    assert registry.record(1, "REST", DriftReport()) is None
    halted = DriftReport(extra=frozenset({"halted"}), dropped_rows=1)
    assert registry.record(1, "REST", halted) == "REST:3 rows dropped, REST:halted+"
    assert registry.record(1, "WebSocket", DriftReport(dropped_rows=1)) == (
        "REST:3 rows dropped, WebSocket:1 row dropped, REST:halted+"
    )
    assert registry.record(2, "T", DriftReport(dropped_rows=1)) == "T:1 row dropped"
    registry.forget(1)
    assert registry.record(1, "REST", DriftReport(dropped_rows=1)) == "REST:1 row dropped"


def test_a_cut_schema_drift_keeps_every_prefix_s_dropped_count() -> None:
    # One prefix's fields fill the text past its limit; a later prefix's count, which sorts
    # after them by name, still shows.
    registry = DriftRegistry()
    fields = frozenset(f"field_{index:02d}" for index in range(30))
    registry.record(1, "REST", DriftReport(extra=fields))
    text = registry.record(1, "WebSocket", DriftReport(dropped_rows=4))
    assert text is not None
    assert len(text) == SCHEMA_DRIFT_SHOWN
    assert text.endswith("…")
    assert text.startswith("WebSocket:4 rows dropped, REST:field_00+, ")


def test_a_rounded_whole_number_warns_once_and_shows_with_a_tilde(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    big = str(2**53 + 1)
    drift = DriftReport(rounded=frozenset({"s"}), samples={"s": DriftSample(None, big)})
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("stocks.last_quote", Transport.REST, drift, where=WHERE)
        registry.report("stocks.last_quote", Transport.REST, drift, where=WHERE)
    assert drift_warnings(caplog) == [
        (
            "stocks.last_quote: REST field 's' held a whole number beyond 2**53, which its "
            "double column holds only rounded; it reads rounded (schema drift) at "
            f"{WHERE}; first value {big}"
        )
    ]
    assert registry.record(3, "REST", drift) == "REST:s~"


def test_schema_drift_text_is_cut_at_its_limit() -> None:
    registry = DriftRegistry()
    fields = frozenset(f"field_{n:03}" for n in range(30))
    text = registry.record(1, "WS", DriftReport(extra=fields))
    assert text is not None
    assert len(text) == SCHEMA_DRIFT_SHOWN
    assert text.startswith("WS:field_000+, WS:field_001+, ")
    assert text.endswith("…")
    # Exactly at the limit, nothing is cut.
    exact = DriftReport(extra=frozenset({"f" * (SCHEMA_DRIFT_SHOWN - len("WS:+"))}))
    assert registry.record(2, "WS", exact) == f"WS:{'f' * (SCHEMA_DRIFT_SHOWN - 4)}+"


def test_drift_warns_once_across_racing_threads(caplog: pytest.LogCaptureFixture) -> None:
    # Many reader threads report the same drift at once: exactly one warning per field.
    registry = DriftRegistry()
    drift = DriftReport(extra=frozenset(f"x{n}" for n in range(20)))
    start = threading.Barrier(16)

    def report() -> None:
        start.wait(TIMEOUT)
        for _ in range(50):
            registry.report("stocks.trades", Transport.FLAT_FILES, drift, where=WHERE)
            registry.record(1, "REST", drift)

    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        threads = [threading.Thread(target=report) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
    assert sorted(drift_warnings(caplog)) == sorted(
        extra("stocks.trades", "FLAT_FILES", f"x{n}") for n in range(20)
    )
    assert registry.record(1, "REST", drift) is None


class _MeetingSet(set[tuple[str, Transport, str]]):
    # A membership test that reads, then waits for a second thread to read too. Without the
    # registry's lock both threads meet here having both found the field new, and both warn.
    # With the lock the second thread cannot arrive while the first holds it, so the wait
    # times out and the second thread then finds the field already warned.
    def __init__(self, meeting: threading.Barrier) -> None:
        super().__init__()
        self._meeting = meeting

    @override
    def __contains__(self, item: object) -> bool:
        found = super().__contains__(item)  # read before meeting: both read the same state
        with contextlib.suppress(threading.BrokenBarrierError):
            self._meeting.wait(MEETING_TIMEOUT)
        return found


class _MeetingOwners:
    # The same meeting point for `record`'s lookup of an owner's entries: the part of a dict
    # that `record` uses.
    def __init__(self, meeting: threading.Barrier) -> None:
        self._meeting = meeting
        self._entries: dict[object, frozenset[str]] = {}

    def get(self, key: object, default: frozenset[str]) -> frozenset[str]:
        found = self._entries.get(key, default)
        with contextlib.suppress(threading.BrokenBarrierError):
            self._meeting.wait(MEETING_TIMEOUT)
        return found

    def __setitem__(self, key: object, value: frozenset[str]) -> None:
        self._entries[key] = value


MEETING_TIMEOUT = 1.0
"""How long the first thread waits for the second at the meeting point; with the lock in
place it always waits this long, then goes on alone."""


def test_the_lock_makes_the_check_and_the_update_one_step(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Deterministic on every build: the meeting point forces the interleaving that a missing
    # lock would allow, so the test fails with the GIL too if the lock is removed.
    registry = DriftRegistry()
    monkeypatch.setattr(registry, "_warned", _MeetingSet(threading.Barrier(2)))
    monkeypatch.setattr(registry, "_owners", _MeetingOwners(threading.Barrier(2)))
    drift = DriftReport(extra=frozenset({"halted"}))
    texts: list[str | None] = []

    def report() -> None:
        registry.report("stocks.trades", Transport.FLAT_FILES, drift, where=WHERE)
        texts.append(registry.record(1, "REST", drift))

    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        threads = [threading.Thread(target=report) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
    assert drift_warnings(caplog) == [extra("stocks.trades", "FLAT_FILES", "halted")]
    assert sorted(texts, key=str) == [None, "REST:halted+"]


def test_a_drift_warning_names_where_the_symbol_and_the_first_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    where = "wss://delayed.massive.com/stocks channel T"
    drift = DriftReport(
        extra=frozenset({"vwq", "new"}),
        missing=frozenset({"x", "y"}),
        samples={
            "vwq": DriftSample("AAPL", "182.33"),
            "new": DriftSample(None, '"a"'),  # no symbol in the sample: the caller's is used
            "x": DriftSample("MSFT", None),
        },
    )
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("stocks.trades", Transport.WS, drift, where=where, symbol="SPY")
        # Without a sample or a symbol, the WARNING names where alone.
        registry.report(
            "stocks.quotes", Transport.WS, DriftReport(extra=frozenset({"q"})), where=where
        )
    dropped, null = "is not in the schema; it is dropped", "is missing; it reads as null"
    at = f"(schema drift) at {where}"
    assert drift_warnings(caplog) == [
        f"stocks.trades: WS field 'new' {dropped} {at}; symbol SPY; first value \"a\"",
        f"stocks.trades: WS field 'vwq' {dropped} {at}; symbol AAPL; first value 182.33",
        f"stocks.trades: WS field 'x' {null} {at}; symbol MSFT",
        f"stocks.trades: WS field 'y' {null} {at}; symbol SPY",
        f"stocks.quotes: WS field 'q' {dropped} {at}",
    ]


# --- Rate limits ---------------------------------------------------------------------------------


class _RatePull:
    """A rate-limit pull that counts its calls; `rows` is what the next call returns."""

    def __init__(self) -> None:
        self.pulls = 0
        self.rows: list[RateLimitRow] = []
        self.change: float | None = None
        # Answers given before `rows` and `change`, in order, so a test can script pulls the
        # writer makes on its own, before the test could change anything.
        self.script: list[tuple[list[RateLimitRow], float | None] | Exception] = []
        self.pulled = threading.Semaphore(0)  # released once per pull

    def __call__(self) -> tuple[list[RateLimitRow], float | None]:
        self.pulls += 1
        self.pulled.release()
        if self.script:
            answer = self.script.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return list(self.rows), self.change


def _rate_row(bucket: str, state: RateLimitState) -> RateLimitRow:
    return RateLimitRow(bucket=bucket, state=state, updated_at=1)


def test_rate_limit_rows_are_pulled_from_their_source_when_told(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    diagnostics.rate_limits_changed()  # no pull yet: nothing is queued
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert source.pulls == 0
    source.rows = [
        _rate_row("stocks", RateLimitState.READY),
        _rate_row("other", RateLimitState.READY),
    ]
    diagnostics.watch_rate_limits(source)  # pulls once
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert source.pulls == 1
    source.rows = [
        dataclasses.replace(
            _rate_row("stocks", RateLimitState.PAUSED),
            requests_per_minute=5,
            limit_source=LimitSource.LEARNED,
            paused_until=2 * 10**18,
            last_refusal="HTTP 429",
        )
    ]
    diagnostics.rate_limits_changed()
    assert diagnostics.wait_until_shown(TIMEOUT)
    rows = {r["Bucket"]: r for r in dh_to_pylist(diagnostics.rate_limits)}
    assert set(rows) == {"stocks", "other"}
    stocks = rows["stocks"]
    assert (stocks["State"], stocks["RequestsPerMinute"], stocks["LimitSource"]) == (
        "PAUSED",
        5,
        "LEARNED",
    )
    assert stocks["LastRefusal"] == "HTTP 429"
    assert rows["other"]["State"] == "READY"
    with pytest.raises(RuntimeError, match="already have a pull"):
        diagnostics.watch_rate_limits(source)
    diagnostics.release()
    diagnostics.rate_limits_changed()  # after release: ignored
    assert source.pulls == 2


def test_many_changes_queue_one_pull(dh_exec_ctx: ExecutionContext) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    started, go = threading.Event(), threading.Event()

    def slow() -> tuple[list[RateLimitRow], float | None]:
        started.set()
        go.wait(TIMEOUT)
        return source()

    diagnostics.watch_rate_limits(slow)
    assert started.wait(TIMEOUT)  # the writer is pulling; the marker is taken
    for _ in range(50):
        diagnostics.rate_limits_changed()  # one marker queued, the rest dropped
    assert diagnostics._writer._queue.qsize() == 1  # the queue stays bounded
    go.set()
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert source.pulls == 2
    diagnostics.release()


def test_rate_limit_rows_are_pulled_again_when_time_alone_changes_them(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    # The pause has already ended (change due now): the writer must not wait for an item.
    source.script = [([_rate_row("stocks", RateLimitState.PAUSED)], 0.0)]
    source.rows = [_rate_row("stocks", RateLimitState.READY)]
    diagnostics.watch_rate_limits(source)
    assert source.pulled.acquire(timeout=TIMEOUT)
    # Nothing is queued: only the writer's timed wait can pull the rows again.
    assert source.pulled.acquire(timeout=TIMEOUT)
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert [r["State"] for r in dh_to_pylist(diagnostics.rate_limits)] == ["READY"]
    assert source.pulls == 2  # and with no change due, it waits for an item again
    diagnostics.release()


def test_a_release_right_after_a_change_leaves_no_thread_behind(
    dh_exec_ctx: ExecutionContext,
) -> None:
    before = set(threading.enumerate())
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.change = 30.0  # a change due later
    diagnostics.watch_rate_limits(source)
    diagnostics.rate_limits_changed()
    diagnostics.release()
    assert set(threading.enumerate()) <= before


def test_after_release_nothing_more_is_queued(dh_exec_ctx: ExecutionContext) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    diagnostics.watch_rate_limits(source)
    assert source.pulled.acquire(timeout=TIMEOUT)
    diagnostics.release()  # as a collected session's reaper does
    diagnostics.rate_limits_changed()
    diagnostics.write(STREAM)
    diagnostics.thread.join(TIMEOUT)
    assert not diagnostics.thread.is_alive()
    assert source.pulls == 1
    assert diagnostics.wait_until_shown(TIMEOUT)  # nothing is queued for a writer that is gone


def test_a_busy_queue_does_not_hold_back_a_due_rate_limit_pull(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A queue that never goes quiet never lets the timed wait expire. Stand that in
    # deterministically: the wait never times out, so only the batch itself can pull.
    monkeypatch.setattr(Diagnostics, "_rate_wait", lambda _self: None)
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.script = [([_rate_row("stocks", RateLimitState.PAUSED)], 0.0)]  # due at once
    try:
        diagnostics.watch_rate_limits(source)
        assert source.pulled.acquire(timeout=TIMEOUT)  # the first pull
        diagnostics.write(STREAM)  # a batch handled after the due time
        assert source.pulled.acquire(timeout=TIMEOUT)  # pulls the rows too
    finally:
        diagnostics.release()
    assert source.pulls == 2


def test_a_timed_pull_whose_source_fails_is_logged_and_not_retried_at_once(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.script = [([], 0.0), RuntimeError("source broke")]  # due at once, then it fails
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.watch_rate_limits(source)
        assert source.pulled.acquire(timeout=TIMEOUT)
        assert source.pulled.acquire(timeout=TIMEOUT)  # the timed pull, which fails
        # Its due time is spent: the writer waits for the next notice, not in a tight loop.
        assert not source.pulled.acquire(timeout=0.2)
    diagnostics.release()
    assert source.pulls == 2
    assert "failed to update the rate_limits table" in caplog.text


def test_a_long_query_is_cut_like_the_symbol_list() -> None:
    short = "<Query reference.news start=2025-01-02>"
    assert shown_query(short) == short
    exact = "q" * QUERY_SHOWN
    assert shown_query(exact) == exact
    long = "<StreamHistoryQuery stocks.trades symbols=(" + "'AAPL', " * 100 + ")>"
    assert shown_query(long) == long[: QUERY_SHOWN - 1] + "…"
    assert len(shown_query(long)) == QUERY_SHOWN


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("sip_timestamp", "SipTimestamp"),
        ("id", "Id"),
        ("filer_cik", "FilerCik"),
        ("alreadyCamel", "AlreadyCamel"),
    ],
)
def test_a_field_name_becomes_a_pascal_case_column_name(name: str, expected: str) -> None:
    assert diagnostics._pascal_case(name) == expected


# --- A followed read's row ----------------------------------------------------------------------


def test_a_followed_read_row_shows_its_loading_count() -> None:
    assert as_row(dataclasses.replace(STREAM, loading=2))["Loading"] == 2
    assert as_row(STREAM)["Loading"] is None


def test_a_followed_read_update_sets_its_loading_and_transport() -> None:
    row = dataclasses.replace(STREAM, transport=Transport.REST)
    updated = StreamUpdate(loading=3, transport=FLAT_FILES_AND_REST).apply(row)
    assert (updated.loading, updated.transport) == (3, FLAT_FILES_AND_REST)
    assert StreamUpdate().apply(updated) == updated


# --- m.symbol_reads() -------------------------------------------------------------------------

_SYMBOL_READ = SymbolReadRow(
    stream_id=7,
    symbol="AAPL",
    dataset="stocks.bars",
    state=SymbolReadState.QUEUED,
    added_at=1_000,
    updated_at=2_000,
)


def _symbol_read(symbol: str = "AAPL") -> SymbolReadRow:
    return dataclasses.replace(_SYMBOL_READ, symbol=symbol)


def _reads(to_list: Rows, diagnostics: Diagnostics) -> list[tuple[int, str, str]]:
    return sorted(
        (r["StreamId"], r["Symbol"], r["State"]) for r in to_list(diagnostics.symbol_reads)
    )


def test_symbol_reads_show_the_latest_row_of_each_key(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    assert diagnostics.symbol_reads.is_refreshing
    diagnostics.write(_symbol_read())
    diagnostics.write(_symbol_read("MSFT"))
    diagnostics.write(dataclasses.replace(_symbol_read(), stream_id=8))
    assert diagnostics.wait_until_shown(TIMEOUT)
    diagnostics.write(
        dataclasses.replace(_symbol_read(), state=SymbolReadState.LOADING, attempts=1)
    )
    assert diagnostics.wait_until_shown(TIMEOUT)
    expected = [(7, "AAPL", "LOADING"), (7, "MSFT", "QUEUED"), (8, "AAPL", "QUEUED")]
    assert _reads(dh_to_pylist, diagnostics) == expected
    diagnostics.release()


def test_the_tables_are_created_under_the_update_graphs_shared_lock(
    dh_exec_ctx: ExecutionContext, blink_creations: list[bool]
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    assert blink_creations == [True, True, True, True]
    diagnostics.release()


def test_a_thread_that_died_of_an_exception_is_a_library_bug() -> None:
    died = thread_died("MassiveSession #1", "reaper", RuntimeError("boom"))
    assert str(died) == (
        "MassiveSession #1: its reaper thread died (RuntimeError('boom')); this table no longer "
        f"updates. {LIBRARY_BUG}"
    )
    assert (died.source, died.is_bug) == (dm.ErrorSource.LIBRARY, True)
    # An interrupt ends the thread too, but is no bug of the library.
    interrupted = thread_died("MassiveSession #1", "reaper", KeyboardInterrupt())
    assert (interrupted.source, interrupted.is_bug) == (dm.ErrorSource.USER, False)
