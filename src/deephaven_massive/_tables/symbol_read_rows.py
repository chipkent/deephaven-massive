"""The rows of `m.symbol_reads()`: one per symbol of each followed `history()` or `fetch()`.

A `history()` or `fetch()` given a refreshing symbol table follows it
(`_tables/followed_read.py`), and the session's follow coordinator
(`_tables/follow_coordinator.py`) decides each symbol's state. This module defines what shows
those states to the user:

- `SymbolReadState` and `SymbolReadRow`: a symbol's state and the immutable row that shows it.
- `SYMBOL_READS` and `SYMBOL_READ_KEYS`: the table's columns and its key, `StreamId` and
  `Symbol`.
- `symbol_read_values`: a row as its columns, for the diagnostics writer.

The coordinator hands each row to the session core (`SessionCore.write_symbol_reads`), which
keeps the latest row of every key, so `m.close()` can write the unfinished ones `CLOSED`, and
queues it for the diagnostics writer (`_tables/diagnostics.py`), which publishes it to the
table's blink publisher; the table users see is that blink table's `last_by` on the key. No
input table holds the rows. Everything here is an immutable value or a pure function.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
from typing import Final

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._massive import Transport

__all__ = [
    "SYMBOL_READS",
    "SYMBOL_READ_KEYS",
    "SymbolReadRow",
    "SymbolReadState",
    "symbol_read_values",
]


class SymbolReadState(StrEnum):
    """The `State` of a row of `m.symbol_reads()`: where one symbol of a followed read stands.

    A symbol starts `QUEUED` when its row enters the symbol table, becomes `LOADING` when a
    read of it begins, and `DONE` once its rows are in the result. A read that fails in a way
    that may pass makes it `RETRYING` until its next attempt; one that fails for good, or the
    last retry, makes it `FAILED`. `REMOVED`, `STOPPED`, and `CLOSED` end its following: its
    row left the symbol table, the result was released and nothing uses it any more, or the
    session closed.
    """

    QUEUED = "QUEUED"
    LOADING = "LOADING"
    RETRYING = "RETRYING"
    DONE = "DONE"
    FAILED = "FAILED"
    REMOVED = "REMOVED"
    STOPPED = "STOPPED"
    CLOSED = "CLOSED"


_S, _I, _L, _T = ColumnType.STRING, ColumnType.INT, ColumnType.LONG, ColumnType.INSTANT

SYMBOL_READS: Final = Schema(
    "symbol_reads",
    tuple(
        Column(name, column_type)
        for name, column_type in (
            ("StreamId", _L),
            ("Symbol", _S),
            ("Dataset", _S),
            ("State", _S),
            ("Transport", _S),
            ("AddedAt", _T),
            ("ReadAt", _T),
            ("Rows", _L),
            ("Attempts", _I),
            ("NextAttemptAt", _T),
            ("Error", _S),
            ("UpdatedAt", _T),
        )
    ),
)
"""The columns of `m.symbol_reads()`."""

SYMBOL_READ_KEYS: Final = ("StreamId", "Symbol")
"""The key columns of `m.symbol_reads()`."""


@dataclass(frozen=True, kw_only=True, slots=True)
class SymbolReadRow:
    """One row of `m.symbol_reads()`. Timestamps are epoch nanoseconds; None is null.

    Made by the session's follow coordinator, the only owner of a followed read's per-symbol
    state, and written through `SessionCore.write_symbol_reads`; immutable.

    Attributes:
        stream_id: The followed read's `StreamId` in `m.streams()` (a key).
        symbol: The canonical symbol (a key).
        dataset: The dataset's name, such as `"stocks.bars"`.
        state: Where the symbol stands (`SymbolReadState`).
        transport: The source of its latest read: `Transport.REST`, `Transport.FLAT_FILES`, or
            text such as `"flat files and REST"`; None before its first read began.
        added_at: When its row last entered the symbol table.
        read_at: When the download whose rows the result holds began, so the rows are
            Massive's answer as of then; None while it has none.
        rows: Its rows in the result.
        attempts: Reads started since it was last added.
        next_attempt_at: When its next read begins; set only while `RETRYING`.
        error: The text of its latest failure; once a read succeeds, why the library skipped
            it (`SymbolRows.skipped`), or None.
        updated_at: When the row was made.
    """

    stream_id: int
    symbol: str
    dataset: str
    state: SymbolReadState
    transport: Transport | str | None = None
    added_at: int
    read_at: int | None = None
    rows: int = 0
    attempts: int = 0
    next_attempt_at: int | None = None
    error: str | None = None
    updated_at: int


def symbol_read_values(row: SymbolReadRow) -> dict[str, object]:
    """Return a row keyed by its column names, as `SYMBOL_READS` takes it.

    Args:
        row: The row.

    Returns:
        Each field under its column's name, a `Transport` written as its label (`flat files`).
    """
    values: dict[str, object] = {}
    for field, column in zip(fields(row), SYMBOL_READS.column_names, strict=True):
        value = getattr(row, field.name)
        values[column] = TRANSPORT_LABELS[value] if isinstance(value, Transport) else value
    return values
