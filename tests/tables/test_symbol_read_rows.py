"""Tests for the rows of `m.symbol_reads()`: its columns, its states, and a row's values."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from deephaven.table import Table

from deephaven_massive._massive import Transport
from deephaven_massive._tables.symbol_read_rows import (
    SYMBOL_READ_KEYS,
    SYMBOL_READS,
    SymbolReadRow,
    SymbolReadState,
    symbol_read_values,
)

Rows = Callable[[Table], list[dict[str, Any]]]
TIMEOUT = 30.0

ROW = SymbolReadRow(
    stream_id=7,
    symbol="AAPL",
    dataset="stocks.bars",
    state=SymbolReadState.QUEUED,
    added_at=1_000,
    updated_at=2_000,
)


def test_the_columns_and_the_states() -> None:
    assert SYMBOL_READS.column_names == (
        "StreamId",
        "Symbol",
        "Dataset",
        "State",
        "Transport",
        "AddedAt",
        "ReadAt",
        "Rows",
        "Attempts",
        "NextAttemptAt",
        "Error",
        "UpdatedAt",
    )
    assert SYMBOL_READ_KEYS == ("StreamId", "Symbol")
    assert [s.value for s in SymbolReadState] == [
        "QUEUED",
        "LOADING",
        "RETRYING",
        "DONE",
        "FAILED",
        "REMOVED",
        "STOPPED",
        "CLOSED",
    ]


def test_a_row_maps_its_fields_to_the_columns() -> None:
    values = symbol_read_values(
        dataclasses.replace(ROW, state=SymbolReadState.DONE, transport=Transport.FLAT_FILES)
    )
    assert list(values) == list(SYMBOL_READS.column_names)
    assert (values["State"], values["Transport"]) == ("DONE", "flat files")
    assert symbol_read_values(dataclasses.replace(ROW, transport="flat files and REST"))[
        "Transport"
    ] == ("flat files and REST")
