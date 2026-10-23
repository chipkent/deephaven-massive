"""Tests for the diagnostics tables (DESIGN § 17)."""

from collections.abc import Callable
from typing import Any

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
    describe_symbols,
)
from deephaven_massive._symbols import SymbolTable

Rows = Callable[[Table], list[dict[str, Any]]]


def _stream(**changes: object) -> StreamRow:
    fields: dict[str, Any] = {
        "stream_id": 7,
        "dataset": "stocks.trades",
        "symbol_count": 2,
        "symbols": "AAPL, MSFT",
        "held_results": 1,
        "transport": "WS",
        "created_at": 1_700_000_000_000_000_000,
    }
    fields.update(changes)
    return StreamRow(**fields)


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


def test_rows_cover_their_schemas() -> None:
    assert set(_stream().as_row()) == set(STREAMS.column_names)
    connection = ConnectionRow(transport="REST", asset_class="stocks", family="TICK")
    assert set(connection.as_row()) == set(CONNECTIONS.column_names)


def test_describe_symbols(dh_exec_ctx: ExecutionContext) -> None:
    assert describe_symbols(("AAPL", "MSFT")) == (2, "AAPL, MSFT")
    many = tuple(f"S{i:02}" for i in range(25))
    count, text = describe_symbols(many)
    assert count == 25
    assert text.endswith("S19 (+5 more)")
    assert describe_symbols(dm.ALL) == (None, "ALL")
    table = SymbolTable(new_table([string_col("Symbol", ["A"])]))
    assert describe_symbols(table) == (None, "reactive table")


def test_writes_replace_rows_by_key(dh_exec_ctx: ExecutionContext, dh_to_pylist: Rows) -> None:
    diagnostics = Diagnostics()
    diagnostics.write_stream(_stream())
    diagnostics.write_stream(_stream(held_results=0, state=StreamState.RELEASED))
    diagnostics.write_stream(_stream(stream_id=8))
    rows = sorted(dh_to_pylist(diagnostics.streams), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["HeldResults"], r["State"]) for r in rows] == [
        (7, 0, "RELEASED"),
        (8, 1, "RUNNING"),
    ]
    assert rows[0]["CreatedAt"] is not None
    diagnostics.write_connection(
        ConnectionRow(
            transport="REST",
            asset_class="stocks",
            family="TICK",
            host="delayed.massive.com",
            recency="DELAYED",
            evidence="last_trade.timeframe=DELAYED",
        )
    )
    diagnostics.write_connection(
        ConnectionRow(
            transport="WS", asset_class="stocks", family="TICK", state=ConnectionState.CONNECTED
        )
    )
    connections = dh_to_pylist(diagnostics.connections)
    assert {(r["Transport"], r["State"]) for r in connections} == {
        ("REST", "RESOLVING"),
        ("WS", "CONNECTED"),
    }
    diagnostics.release()


def test_release_is_idempotent_and_drops_later_writes(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostics = Diagnostics()
    diagnostics.release()
    diagnostics.release()
    with caplog.at_level("DEBUG", logger="deephaven_massive._diagnostics"):
        diagnostics.write_stream(_stream())
    assert "dropping streams row" in caplog.text
