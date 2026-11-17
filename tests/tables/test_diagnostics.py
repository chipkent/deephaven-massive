"""Tests for the diagnostics tables and their writer thread."""

import dataclasses
import functools
import logging
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

import deephaven_massive as dm
from deephaven_massive._datasets.schema import Schema
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._errors import LIBRARY_BUG, BrokenGuaranteeError
from deephaven_massive._massive import Redactor, Transport
from deephaven_massive._tables import diagnostics
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.diagnostics import (
    CONNECTION_KEYS,
    CONNECTIONS,
    FLAT_FILES_AND_REST,
    QUERY_SHOWN,
    RATE_LIMIT_KEYS,
    RATE_LIMITS,
    STREAM_KEYS,
    STREAMS,
    WRITE_INTERVAL,
    ConnectionRow,
    ConnectionState,
    Diagnostics,
    DriftRegistry,
    LimitSource,
    RateLimitRow,
    RateLimitState,
    StreamKind,
    StreamRow,
    StreamState,
    StreamUpdate,
    as_row,
    describe_symbols,
    shown_query,
    thread_died,
)
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.problem_events import (
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    SpanTracker,
)
from deephaven_massive._tables.problem_ledger import PROBLEM_KEYS, PROBLEMS
from deephaven_massive._tables.publisher import BlinkPublisher, Publisher, blink_publisher
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
        "CreatedAt",
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
        "WAITING",
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
    assert as_row(updated)["Error"] is None


@pytest.mark.parametrize("state", [StreamState.FAILED, StreamState.CLOSED])
def test_stream_updates_keep_the_cause_of_a_stop(state: StreamState) -> None:
    # A flush or a poll cycle that races a failure or the close never replaces its cause.
    row = dataclasses.replace(STREAM, state=state, error="the cause")
    updated = StreamUpdate(rows_published=7).apply(row)
    assert (updated.state, updated.error, as_row(updated)["Error"]) == (
        state,
        "the cause",
        "the cause",
    )
    assert updated.rows_published == 7  # counts still move


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


def test_describe_symbols_raises_a_library_defect_for_a_symbols_form_with_no_rule() -> None:
    message = "summary has no rule for a value of type object"
    with pytest.raises(BrokenGuaranteeError, match=message):
        describe_symbols(object())  # type: ignore[arg-type]  # the point: refused at run time


def _diagnostics(exec_ctx: ExecutionContext) -> Diagnostics:
    # The session passes its reaper's `release_later`; releasing at once is the same here, where
    # no table is ever failed.
    return Diagnostics(
        exec_ctx,
        "test diagnostics",
        label="TestSession",
        redactor=Redactor("planted-key"),
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
        (PROBLEMS, PROBLEM_KEYS),
    ],
    ids=["connections", "streams", "rate_limits", "symbol_reads", "problems"],
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
        diagnostics.problems,
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
    def add(self, table: Table) -> None:
        raise RuntimeError("the streams table is broken")

    def publish_failure(self, error: Exception) -> None:
        raise AssertionError(error)  # the writer lives: nothing fails the table


def test_a_failing_table_does_not_cost_the_other_its_rows(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    diagnostics._streams.publisher = _BrokenPublisher()
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
    def __init__(self, publisher: Publisher, *, hold: bool = True) -> None:
        self.publisher = publisher
        self.adds: list[float] = []
        self.ends: list[float] = []
        self.entered = threading.Event()
        self.gate = threading.Event()
        if not hold:
            self.gate.set()

    def add(self, table: Table) -> None:
        self.adds.append(time.monotonic())
        self.entered.set()
        assert self.gate.wait(TIMEOUT)
        self.publisher.add(table)
        self.ends.append(time.monotonic())

    def publish_failure(self, error: Exception) -> None:
        self.publisher.publish_failure(error)


def _count_adds(diagnostics: Diagnostics, *, hold: bool = True) -> _CountingPublisher:
    counting = _CountingPublisher(diagnostics._streams.publisher, hold=hold)
    diagnostics._streams.publisher = counting
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


DRIFT_LOGGER = "deephaven_massive._tables.diagnostics"


def status(path: str, value: str) -> str:
    return f"{path}: undocumented response status {value!r}; the rows are kept (schema drift)"


def drift_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == DRIFT_LOGGER and r.levelno == logging.WARNING
    ]


def test_drift_warns_once_per_path_and_undocumented_status(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report_statuses("/v3/trades/{stockTicker}", frozenset({"PARTIAL", "MAYBE"}))
        registry.report_statuses("/v3/trades/{stockTicker}", frozenset({"PARTIAL"}))
        registry.report_statuses("/v3/quotes/{stockTicker}", frozenset({"PARTIAL"}))
    assert drift_warnings(caplog) == [
        status("/v3/trades/{stockTicker}", "MAYBE"),
        status("/v3/trades/{stockTicker}", "PARTIAL"),
        status("/v3/quotes/{stockTicker}", "PARTIAL"),
    ]


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
    assert blink_creations == [True, True, True, True, True]
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


def test_an_item_the_writer_has_no_rule_for_is_a_defect(dh_exec_ctx: ExecutionContext) -> None:
    # Only rows, events, and the rate-limit marker are queued; anything else raises, never pulls.
    diagnostics = _diagnostics(dh_exec_ctx)
    with pytest.raises(BrokenGuaranteeError, match=r"^the diagnostics queue holds 'text'"):
        diagnostics._apply(["text"])
    diagnostics.release()


def _problem(*, at: int = 1_000, stream_id: int = 7, symbol: str | None = "AAPL") -> ProblemEvent:
    return ProblemEvent(
        cause=ProblemCause.UNCONVERTIBLE_VALUES,
        action=ProblemAction.WITHHELD,
        dataset="stocks.trades",
        stream_id=stream_id,
        where="GET /v3/trades",
        symbol=symbol,
        field="Price",
        rows=1,
        detail="Price holds 'x' for planted-key",
        at=at,
        policy=dm.OnError.LOG,
    )


def test_problems_add_up_and_take_their_table_s_query(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    diagnostics.report_problems([_problem(), _problem(at=2_000)])
    diagnostics.report_problems([_problem(stream_id=9, symbol=None)])
    diagnostics.name_read(9, "m.stocks.trades('AAPL').fetch(wait=True)")
    assert diagnostics.wait_until_shown(TIMEOUT)
    diagnostics.write(STREAM)  # stream 7's row names its query after its problems
    assert diagnostics.wait_until_shown(TIMEOUT)
    rows = sorted(dh_to_pylist(diagnostics.problems), key=lambda r: r["StreamId"])
    columns = ("StreamId", "Symbol", "Rows", "Occurrences", "Query", "Detail", "Source")
    assert [tuple(r[c] for c in columns) for r in rows] == [
        (7, "AAPL", 2, 2, STREAM.query, "Price holds 'x' for ***", "library"),
        (
            9,
            None,
            1,
            1,
            "m.stocks.trades('AAPL').fetch(wait=True)",
            "Price holds 'x' for ***",
            "library",
        ),
    ]
    diagnostics.release()
    diagnostics.report_problems([_problem(symbol="MSFT")])  # after release: not queued
    diagnostics.name_read(10, "ignored")


def test_a_final_stream_row_ends_its_open_spans(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    tracker = SpanTracker(ProblemCause.OUTAGE, dataset="stocks.trades", stream_id=7, where="w")
    spans = tracker.update({"outage": (None, None, "down", 0)}, at=5, since=1)
    assert spans is not None
    diagnostics.report_problems([spans])
    diagnostics.write(STREAM)
    assert diagnostics.wait_until_shown(TIMEOUT)
    [row] = dh_to_pylist(diagnostics.problems)
    assert (row["Action"], row["EndedAt"]) == ("GAP", None)
    diagnostics.write(dataclasses.replace(STREAM, state=StreamState.FAILED, error="gone"))
    assert diagnostics.wait_until_shown(TIMEOUT)
    [ended] = dh_to_pylist(diagnostics.problems)
    assert ended["EndedAt"] is not None
    diagnostics.release()


def test_a_table_that_left_its_registry_ends_its_open_spans(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    tracker = SpanTracker(
        ProblemCause.STALE_SYMBOL_DATA, dataset="stocks.trades", stream_id=7, where="w"
    )
    spans = tracker.update({"X": ("X", None, "stale", 0)}, at=5, since=1)
    assert spans is not None
    diagnostics.report_problems([spans])
    diagnostics.stream_gone(7)  # released, and its blink table shut down
    assert diagnostics.wait_until_shown(TIMEOUT)
    [row] = dh_to_pylist(diagnostics.problems)
    assert row["EndedAt"] is not None
    diagnostics.release()
    diagnostics.stream_gone(8)  # after release: not queued


def test_a_paused_bucket_is_a_rate_limited_span_until_its_pause_ends(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.rows = [
        dataclasses.replace(
            _rate_row("stocks", RateLimitState.PAUSED),
            paused_until=1_793_628_000 * 10**9,
            last_refusal="HTTP 429",
        ),
        _rate_row("other", RateLimitState.READY),
    ]
    diagnostics.watch_rate_limits(source)
    assert diagnostics.wait_until_shown(TIMEOUT)
    [row] = dh_to_pylist(diagnostics.problems)
    assert (row["Cause"], row["Action"], row["StreamId"], row["Dataset"], row["EndedAt"]) == (
        "rate_limited",
        "DELAYED",
        None,
        None,
        None,
    )
    assert row["Detail"] == "bucket stocks paused until 2026-11-02T14:00:00Z: HTTP 429"
    assert row["Field"] == "stocks"
    source.rows = [_rate_row("stocks", RateLimitState.READY)]
    diagnostics.rate_limits_changed()
    assert diagnostics.wait_until_shown(TIMEOUT)
    [ended] = dh_to_pylist(diagnostics.problems)
    assert ended["EndedAt"] is not None
    diagnostics.release()


def test_a_pause_that_holds_at_release_ends_with_the_session(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    # A session closed during a pause leaves no pause open in a view kept from before.
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.rows = [_rate_row("stocks", RateLimitState.PAUSED)]
    diagnostics.watch_rate_limits(source)
    assert diagnostics.wait_until_shown(TIMEOUT)
    keeper = new_scope()
    keeper.manage(diagnostics.problems)
    [row] = dh_to_pylist(diagnostics.problems)
    assert row["EndedAt"] is None
    diagnostics.release()
    assert diagnostics.wait_until_shown(TIMEOUT)  # the kept table takes the last rows in
    [ended] = dh_to_pylist(diagnostics.problems)
    assert ended["EndedAt"] is not None
    release_scope(dh_exec_ctx, keeper)


def test_a_failed_last_pause_row_costs_the_release_nothing(
    *,
    dh_exec_ctx: ExecutionContext,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.rows = [_rate_row("stocks", RateLimitState.PAUSED)]
    diagnostics.watch_rate_limits(source)
    assert diagnostics.wait_until_shown(TIMEOUT)

    def broken(*_: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(type(diagnostics._ledger), "apply", broken)
    with caplog.at_level(logging.ERROR, "deephaven_massive._tables.diagnostics"):
        diagnostics.release()
    # The failure is logged as the library's, and the writer still ends cleanly: no table fails.
    [record] = caplog.records
    assert record.getMessage().startswith(
        "diagnostics: failed to end the open rate-limit pauses in the problems table."
    )
    assert diagnostics._scope_released


def test_buckets_paused_in_one_pull_are_rows_of_their_own(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    # Both pauses start at the same pull; each keeps its own row, and one ending leaves the
    # other open.
    diagnostics = _diagnostics(dh_exec_ctx)
    source = _RatePull()
    source.rows = [
        _rate_row("stocks", RateLimitState.PAUSED),
        _rate_row("options", RateLimitState.PAUSED),
    ]
    diagnostics.watch_rate_limits(source)
    assert diagnostics.wait_until_shown(TIMEOUT)
    assert sorted(r["Field"] for r in dh_to_pylist(diagnostics.problems)) == ["options", "stocks"]
    source.rows = [
        _rate_row("stocks", RateLimitState.READY),
        _rate_row("options", RateLimitState.PAUSED),
    ]
    diagnostics.rate_limits_changed()
    assert diagnostics.wait_until_shown(TIMEOUT)
    ended = {r["Field"]: r["EndedAt"] is not None for r in dh_to_pylist(diagnostics.problems)}
    assert ended == {"stocks": True, "options": False}
    diagnostics.release()
