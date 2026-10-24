"""Tests for the diagnostics tables and their writer thread (DESIGN § 17)."""

import dataclasses
import logging
import threading
from collections.abc import Callable
from typing import Any, cast

import pytest
from deephaven import input_table, new_table
from deephaven.column import string_col
from deephaven.dtypes import DType
from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._diagnostics import (
    CONNECTIONS,
    STREAMS,
    ConnectionRow,
    ConnectionState,
    Diagnostics,
    StreamRow,
    StreamState,
    as_row,
    describe_symbols,
)
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, Transport
from deephaven_massive._liveness import new_scope
from deephaven_massive._symbols import SymbolTable

Rows = Callable[[Table], list[dict[str, Any]]]
TIMEOUT = 10.0

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
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._diagnostics"):
        diagnostics.write(_stream(stream_id=8, dataset=cast("str", 5)))
        diagnostics.write(STREAM)
        assert diagnostics.barrier().wait(TIMEOUT)
    assert "failed to write a streams row" in caplog.text
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
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

    monkeypatch.setattr("deephaven_massive._diagnostics.new_scope", tracking_scope)
    monkeypatch.setattr("deephaven_massive._diagnostics.input_table", second_fails)
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
