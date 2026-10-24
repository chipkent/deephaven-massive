"""Tests for the diagnostics tables and their writer thread (DESIGN § 17)."""

import dataclasses
import logging
from collections.abc import Callable
from typing import Any, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col
from deephaven.execution_context import ExecutionContext
from deephaven.table import Table

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
        diagnostics.write(_stream(dataset=cast("str", 5)))
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
    assert not diagnostics._thread.is_alive()
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    diagnostics.write(_stream(stream_id=9))  # dropped: the writer has stopped
    assert [r["StreamId"] for r in dh_to_pylist(diagnostics.streams)] == [7]
    keeper.release()


def test_stop_does_not_wait(dh_exec_ctx: ExecutionContext) -> None:
    diagnostics = Diagnostics(dh_exec_ctx, "test diagnostics")
    diagnostics.stop()
    diagnostics._thread.join(TIMEOUT)
    assert not diagnostics._thread.is_alive()
