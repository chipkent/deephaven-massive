"""Tests for the diagnostics tables and their writer thread."""

import contextlib
import dataclasses
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

import pyarrow as pa
import pytest
from deephaven import input_table, new_table
from deephaven.column import string_col
from deephaven.dtypes import DType
from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets.schema import Schema
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, Transport
from deephaven_massive._massive import DriftReport
from deephaven_massive._tables.diagnostics import (
    CONNECTIONS,
    SCHEMA_DRIFT_SHOWN,
    STREAMS,
    ConnectionRow,
    ConnectionState,
    Diagnostics,
    DriftRegistry,
    StreamRow,
    StreamState,
    StreamUpdate,
    as_row,
    describe_symbols,
)
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._types import override

Rows = Callable[[Table], list[dict[str, Any]]]
TIMEOUT = 30.0

STREAM = StreamRow(
    stream_id=7,
    dataset="stocks.trades",
    symbol_count=2,
    symbols="AAPL, MSFT",
    held_results=1,
    transport=Transport.WS,
    created_at=1_700_000_000_000_000_000,
)


def _stream(**changes: object) -> StreamRow:
    return dataclasses.replace(STREAM, **cast("dict[str, Any]", changes))


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
        "Reconnects",
        "LastMessageAt",
        "NextReconnectAt",
        "UpdatedAt",
        "LastError",
        "SchemaDrift",
    )
    assert STREAMS.column_names == (
        "StreamId",
        "Dataset",
        "SymbolCount",
        "Symbols",
        "HeldResults",
        "Deferred",
        "Transport",
        "Host",
        "Path",
        "RowsPublished",
        "RequestsPerCycle",
        "State",
        "Error",
        "CreatedAt",
        "SchemaDrift",
    )
    assert [s.value for s in StreamState] == ["RUNNING", "FAILED", "RELEASED", "CLOSED"]
    assert [s.value for s in ConnectionState] == [
        "RESOLVING",
        "CONNECTING",
        "CONNECTED",
        "WAITING_SLOT",
        "RECONNECTING",
        "FAILED",
        "CLOSED",
    ]


def test_stream_updates_set_only_the_given_fields() -> None:
    row = _stream(host="socket.massive.com", rows_published=3, error="old")
    assert StreamUpdate().apply(row) == row
    updated = StreamUpdate(rows_published=10, deferred=2, symbol_count=5, path="/stocks").apply(row)
    assert (updated.rows_published, updated.deferred, updated.symbol_count) == (10, 2, 5)
    assert (updated.host, updated.path, updated.error) == ("socket.massive.com", "/stocks", "old")
    assert StreamUpdate(host="delayed.massive.com", error="slow").apply(row).error == "slow"


def test_stream_updates_can_clear_a_warning() -> None:
    # The options contract cap no longer defers anything: the warning goes back to null.
    row = _stream(deferred=234, error="over capacity: 1234/1000; 234 deferred")
    cleared = StreamUpdate(deferred=0, clear_error=True).apply(row)
    assert (cleared.deferred, cleared.error, cleared.state) == (0, None, StreamState.RUNNING)
    assert StreamUpdate(error="new", clear_error=True).apply(row).error is None


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


def test_rows_replace_rows_by_key_in_order(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics.write(STREAM)
    diagnostics.write(_stream(held_results=0, state=StreamState.RELEASED))
    diagnostics.write(_stream(stream_id=8))
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
    assert diagnostics.barrier().wait(TIMEOUT)
    rows = sorted(dh_to_pylist(diagnostics.streams), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["HeldResults"], r["State"], r["Transport"]) for r in rows] == [
        (7, 0, "RELEASED", "WS"),
        (8, 1, "RUNNING", "WS"),
    ]
    connections = dh_to_pylist(diagnostics.connections)
    assert {(r["Transport"], r["AssetClass"], r["State"], r["Recency"]) for r in connections} == {
        ("REST", "stocks", "RESOLVING", "DELAYED"),
        ("WS", "stocks", "CONNECTED", None),
    }
    diagnostics.release()


def test_failed_writes_are_logged_and_the_writer_continues(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(_stream(stream_id=8, dataset=cast("str", 5)))
        diagnostics.write(STREAM)
        assert diagnostics.barrier().wait(TIMEOUT)
    assert "failed to write a streams row" in caplog.text
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    diagnostics.release()


class _BrokenTable:
    # A diagnostics table whose add fails.
    def add(self, rows: Table) -> None:
        raise RuntimeError("the streams table is broken")


def test_a_failing_table_does_not_cost_the_other_its_rows(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics._streams = cast("InputTable", _BrokenTable())
    connection = ConnectionRow(
        transport=Transport.WS, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
    )
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(STREAM)
        diagnostics.write(connection)
        assert diagnostics.barrier().wait(TIMEOUT)
    assert "failed to update the streams table" in caplog.text
    assert len(dh_to_pylist(diagnostics.connections)) == 1  # applied despite the failure
    diagnostics.release()


def test_a_batch_is_converted_once(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    to_arrow = Schema.to_arrow

    def counting(self: Schema, rows: Sequence[Mapping[str, object]]) -> pa.Table:
        calls.append(len(rows))
        return to_arrow(self, rows)

    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    monkeypatch.setattr(Schema, "to_arrow", counting)
    diagnostics.write(STREAM)  # the writer may take this one alone; then the rest as one batch
    assert diagnostics.barrier().wait(TIMEOUT)
    calls.clear()
    counting_table = CountingTable(diagnostics.streams)
    diagnostics._streams = cast("InputTable", counting_table)
    diagnostics.write(_stream(stream_id=100))
    assert counting_table.entered.wait(TIMEOUT)  # the writer now holds its next batch back
    for n in range(3):
        diagnostics.write(_stream(stream_id=n))
    counting_table.gate.set()
    assert diagnostics.barrier().wait(TIMEOUT)
    assert calls == [1, 3]  # one conversion per batch, never one per row
    diagnostics.release()


def test_a_batch_of_rows_that_do_not_fit_adds_nothing(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.write(_stream(stream_id=8, dataset=cast("str", 5)))
        assert diagnostics.barrier().wait(TIMEOUT)
    assert "failed to write a streams row" in caplog.text
    assert dh_to_pylist(diagnostics.streams) == []
    diagnostics.release()


def test_release_applies_queued_rows_then_releases(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")

    keeper = new_scope()
    keeper.manage(diagnostics.streams)
    diagnostics.write(STREAM)
    diagnostics.release()
    diagnostics.release()  # idempotent
    assert diagnostics.barrier().is_set()  # nothing left to wait for
    assert not diagnostics.thread.is_alive()
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    diagnostics.write(_stream(stream_id=9))  # dropped: the writer has stopped
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    keeper.release()


def test_a_failed_table_creation_releases_the_scope(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    scopes: list[LivenessScope] = []
    real_scope = new_scope
    real_table = input_table
    created: list[InputTable] = []

    def tracking_scope() -> LivenessScope:
        scope = real_scope()
        scopes.append(scope)
        return scope

    def second_fails(col_defs: dict[str, DType], *, key_cols: list[str]) -> InputTable:
        if created:
            raise RuntimeError("no second table")
        created.append(real_table(col_defs, key_cols=key_cols))
        return created[0]

    monkeypatch.setattr("deephaven_massive._tables.diagnostics.new_scope", tracking_scope)
    monkeypatch.setattr("deephaven_massive._tables.diagnostics.input_table", second_fails)
    with pytest.raises(RuntimeError, match="no second table"):
        Diagnostics(dh_exec_ctx, "test diagnostics")
    assert scopes[0].j_scope is None  # released, with the first table in it


def test_stop_does_not_wait(dh_exec_ctx: ExecutionContext) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics.stop()
    diagnostics.thread.join(TIMEOUT)
    assert not diagnostics.thread.is_alive()


class CountingTable:
    # Wraps an input table, counts `add` calls, and holds the first one until released, so
    # rows queued meanwhile reach the writer as one batch.
    def __init__(self, table: InputTable) -> None:
        self.table = table
        self.adds = 0
        self.entered = threading.Event()
        self.gate = threading.Event()

    def add(self, rows: Table) -> None:
        self.adds += 1
        self.entered.set()
        assert self.gate.wait(TIMEOUT)
        self.table.add(rows)


def test_queued_rows_are_coalesced_into_few_adds(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    counting = CountingTable(diagnostics.streams)
    diagnostics._streams = cast("InputTable", counting)
    diagnostics.write(STREAM)
    assert counting.entered.wait(TIMEOUT)
    for n in range(1_000):
        diagnostics.write(_stream(stream_id=n % 10, held_results=n))
    counting.gate.set()
    assert diagnostics.barrier().wait(TIMEOUT)
    assert counting.adds == 2  # the held first row, then one add for the other 1,000
    rows = sorted(dh_to_pylist(counting.table), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["HeldResults"]) for r in rows] == [(n, 990 + n) for n in range(10)]
    diagnostics.release()


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
    counts = ("Topics", "AckedTopics", "PendingTopics", "NoAckTopics", "Reconnects")
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
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics.release()
    diagnostics.write(STREAM)
    assert diagnostics._writer._queue.empty()  # nothing would ever drain it


def test_connection_rows_replace_by_their_key(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    tick = ConnectionRow(
        transport=Transport.WS, asset_class=AssetClass.STOCKS, family=ChannelFamily.TICK
    )
    diagnostics.write(dataclasses.replace(tick, state=ConnectionState.CONNECTING))
    diagnostics.write(dataclasses.replace(tick, state=ConnectionState.CONNECTED, reconnects=1))
    diagnostics.write(
        dataclasses.replace(tick, family=ChannelFamily.FMV, state=ConnectionState.FAILED)
    )
    assert diagnostics.barrier().wait(TIMEOUT)
    rows = sorted(dh_to_pylist(diagnostics.connections), key=lambda r: r["Family"])
    assert [(r["Family"], r["State"], r["Reconnects"]) for r in rows] == [
        ("FMV", "FAILED", 0),
        ("TICK", "CONNECTED", 1),
    ]
    diagnostics.release()


def test_stream_updates_keep_schema_drift() -> None:
    row = _stream(schema_drift="WS:x+")
    assert StreamUpdate(rows_published=1).apply(row).schema_drift == "WS:x+"
    # Clearing the error never clears the drift: drift is not a failure.
    assert StreamUpdate(clear_error=True).apply(row).schema_drift == "WS:x+"


def test_schema_drift_reaches_both_tables(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows
) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics.write(_stream(schema_drift="REST:halted+", error="boom"))
    diagnostics.write(
        ConnectionRow(
            transport=Transport.WS,
            asset_class=AssetClass.STOCKS,
            family=ChannelFamily.TICK,
            last_error="reset",
            schema_drift="T:trfi-",
        )
    )
    assert diagnostics.barrier().wait(TIMEOUT)
    (stream,) = dh_to_pylist(diagnostics.streams)
    assert (stream["SchemaDrift"], stream["Error"]) == ("REST:halted+", "boom")
    (connection,) = dh_to_pylist(diagnostics.connections)
    assert (connection["SchemaDrift"], connection["LastError"]) == ("T:trfi-", "reset")
    diagnostics.release()


# The drift registry.

DRIFT_LOGGER = "deephaven_massive._tables.diagnostics"


def extra(dataset: str, transport: str, field: str) -> str:
    return (
        f"{dataset}: {transport} field {field!r} is not in the schema; it is dropped (schema drift)"
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
            registry.report("stocks.trades", Transport.FLAT_FILES, drift)
        registry.report("stocks.trades", Transport.FLAT_FILES, DriftReport())
        # Another transport, or another dataset, is another warning.
        registry.report("stocks.trades", Transport.REST, DriftReport(extra=frozenset({"halted"})))
        registry.report("stocks.quotes", Transport.FLAT_FILES, DriftReport(extra=drift.extra))
    assert drift_warnings(caplog) == [
        extra("stocks.trades", "FLAT_FILES", "halted"),
        "stocks.trades: FLAT_FILES field 'trf_id' is missing; it reads as null (schema drift)",
        extra("stocks.trades", "REST", "halted"),
        extra("stocks.quotes", "FLAT_FILES", "halted"),
    ]


def test_drift_warnings_are_sorted_within_a_report(caplog: pytest.LogCaptureFixture) -> None:
    registry = DriftRegistry()
    drift = DriftReport(extra=frozenset({"b", "a"}), missing=frozenset({"d", "c"}))
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("stocks.trades", Transport.WS, drift)
    assert [message.split("'")[1] for message in drift_warnings(caplog)] == ["a", "b", "c", "d"]


def test_drift_ignores_statuses_in_report_and_warns_per_path_and_status(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = DriftRegistry()
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        registry.report("stocks.trades", Transport.REST, DriftReport(statuses=frozenset({"X"})))
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
            registry.report("stocks.trades", Transport.FLAT_FILES, drift)
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
        registry.report("stocks.trades", Transport.FLAT_FILES, drift)
        texts.append(registry.record(1, "REST", drift))

    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        threads = [threading.Thread(target=report) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
    assert drift_warnings(caplog) == [extra("stocks.trades", "FLAT_FILES", "halted")]
    assert sorted(texts, key=str) == [None, "REST:halted+"]
