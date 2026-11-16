"""Live check of a `history()` that follows a live symbol table, against Massive.

A `history()` given a refreshing symbol table reads each symbol added to it over the call's
fixed range, drops each one removed, and on a re-add either reuses the earlier download
(`dm.Reread.NEVER`) or downloads it again (`ALWAYS`, the default). `m.symbol_reads()` shows
each symbol's read. The test follows a watchlist of daily bars from REST over a past week,
adds Apple and a ticker with no data, removes and re-adds Apple under each member, and
records what `m.symbol_reads()` shows at each step. Unadjusted bars keep it independent of
splits. It sends about four requests. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.update_graph import shared_lock

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table
    from deephaven.table_factory import InputTable

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Reads: TypeAlias = "dict[str, dict[str, Any]]"

_START, _END = "2025-06-02", "2025-06-06"
"""A past week of five trading days."""

_NO_DATA = "ZZZZQ"
"""A stock ticker in Massive's grammar that names no listed stock, so it has no bars."""

_WAIT = 90.0
"""Seconds a step may take: a request of a subscription Massive limits may wait for a later
minute."""


def _wait_for(read: Callable[[], Reads], done: Callable[[Reads], bool]) -> Reads:
    """Read `m.symbol_reads()` rows until `done` holds.

    Returns:
        The rows that satisfied it, by symbol.
    """
    pause = threading.Event()
    deadline = time.monotonic() + _WAIT
    while True:
        rows = read()
        if done(rows):
            return rows
        assert time.monotonic() < deadline, f"m.symbol_reads() never got there: {rows}"
        pause.wait(0.25)


def _change(table: InputTable, *, add: str | None = None, delete: str | None = None) -> None:
    """Add a symbol to the watchlist, or delete one from it."""
    editable = table
    if add is not None:
        editable.add(new_table([string_col("Symbol", [add])]))
    if delete is not None:
        editable.delete(new_table([string_col("Symbol", [delete])]))


class _Diagnostics:
    """The session's `m.streams()` and `m.symbol_reads()` views, read for one result at a time."""

    def __init__(self, m: dm.MassiveSession, table_rows: TableRows) -> None:
        self.streams = m.streams()
        self.symbol_reads = m.symbol_reads()
        self.table_rows = table_rows

    def newest_read(self, after: int) -> int:
        """Wait for a followed read newer than `after`, and return its `StreamId`.

        Returns:
            The `StreamId`: ids rise in the order the results are made.
        """
        pause = threading.Event()
        deadline = time.monotonic() + _WAIT
        while True:
            ids = [r["StreamId"] for r in self.table_rows(self.streams) if r["Kind"] == "read"]
            newest = int(max(ids, default=0))
            if newest > after:
                return newest
            assert time.monotonic() < deadline, "the followed read has no m.streams() row"
            pause.wait(0.25)

    def reads(self, stream_id: int) -> Reads:
        """Return the `m.symbol_reads()` rows of one followed read.

        Returns:
            The rows, by symbol.
        """
        return {
            str(row["Symbol"]): row
            for row in self.table_rows(self.symbol_reads)
            if row["StreamId"] == stream_id
        }

    def stream(self, stream_id: int) -> dict[str, Any]:
        """Return the `m.streams()` row of one followed read.

        Returns:
            The row.
        """
        (row,) = [r for r in self.table_rows(self.streams) if r["StreamId"] == stream_id]
        return row


def _aapl_rows(table_rows: TableRows, result: Table) -> int:
    """Count AAPL's rows in a result.

    Returns:
        The count.
    """
    return sum(1 for row in table_rows(result) if row["Symbol"] == "AAPL")


@pytest.mark.requests(4)
@pytest.mark.timeout(400)
def test_a_followed_history_reads_reuses_and_rereads_a_symbol(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether a followed `history()` reads, drops, reuses, and reads again as documented.

    Follows a watchlist with `m.stocks.bars(wl, dm.Timespan.DAY, adjusted=False)
    .history(<a past week>, source=REST, reread=NEVER)`, adds AAPL and a ticker with no data
    (two requests), removes AAPL, and adds it back (no request: NEVER reuses the download).
    Then a second result over the same watchlist with the default `ALWAYS` reads AAPL (one
    request), and a removal and re-add downloads it again (one request).

    Passes when the result's `m.symbol_reads()` rows carry its `StreamId` in `m.streams()`,
    the dataset, and `REST`; AAPL is `DONE` with `Rows` equal to its rows in the result and
    the ticker with no data `DONE` with 0 rows; AAPL is `REMOVED` and its rows gone after the
    removal; under `NEVER` the re-added AAPL is `DONE` with the same `ReadAt`, `Rows`, and
    `Attempts`, and a `ReadAt` before its new `AddedAt`; and under `ALWAYS` the re-added AAPL is
    `LOADING` or `DONE` with a later `ReadAt`. `m.rate_limits()` is recorded only: every
    session of the key shares it.
    """
    m = live_session
    with shared_lock(get_exec_ctx().update_graph):  # created before any cycle can refresh it
        wl = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    query = m.stocks.bars(wl, dm.Timespan.DAY, adjusted=False)
    never = query.history(
        _START, _END, source=dm.HistorySource.REST, reread=dm.Reread.NEVER, wait=True
    )
    seen = _Diagnostics(m, table_rows)
    held = [never, seen.streams, seen.symbol_reads]
    try:
        never_id = seen.newest_read(0)
        _change(wl, add="AAPL")
        _change(wl, add=_NO_DATA)
        first = _wait_for(
            lambda: seen.reads(never_id),
            lambda rows: {rows.get(s, {}).get("State") for s in ("AAPL", _NO_DATA)} == {"DONE"},
        )
        evidence(f"NEVER, first reads: {first}")
        assert seen.stream(never_id)["Kind"] == "read"
        assert {row["Dataset"] for row in first.values()} == {"stocks.bars"}
        assert {row["Transport"] for row in first.values()} == {"REST"}
        assert first["AAPL"]["Rows"] == _aapl_rows(table_rows, never) > 0
        assert first[_NO_DATA]["Rows"] == 0

        _change(wl, delete="AAPL")
        removed = _wait_for(
            lambda: seen.reads(never_id), lambda rows: rows["AAPL"]["State"] == "REMOVED"
        )
        evidence(f"NEVER, after the removal: {removed['AAPL']}")
        assert _aapl_rows(table_rows, never) == 0

        _change(wl, add="AAPL")
        back = _wait_for(
            lambda: seen.reads(never_id),
            lambda rows: (
                rows["AAPL"]["State"] == "DONE"
                and rows["AAPL"]["AddedAt"] > first["AAPL"]["AddedAt"]
            ),
        )["AAPL"]
        evidence(f"NEVER, after the re-add: {back}")
        for column in ("ReadAt", "Rows", "Attempts"):
            assert back[column] == first["AAPL"][column], column
        assert back["ReadAt"] < back["AddedAt"]

        always = query.history(_START, _END, source=dm.HistorySource.REST, wait=True)
        held.append(always)
        always_id = seen.newest_read(never_id)
        before = _wait_for(
            lambda: seen.reads(always_id),
            lambda rows: rows.get("AAPL", {}).get("State") == "DONE",
        )["AAPL"]
        _change(wl, delete="AAPL")
        _change(wl, add="AAPL")
        again = _wait_for(
            lambda: seen.reads(always_id),
            lambda rows: (
                rows["AAPL"]["AddedAt"] > before["AddedAt"]
                and (
                    rows["AAPL"]["State"] == "LOADING"
                    or (
                        rows["AAPL"]["State"] == "DONE"
                        and rows["AAPL"]["ReadAt"] > before["ReadAt"]
                    )
                )
            ),
        )["AAPL"]
        evidence(f"ALWAYS, before: {before}; after the re-add: {again}")
        limits = m.rate_limits()
        held.append(limits)
        evidence(f"rate_limits(): {table_rows(limits)}")
    finally:
        for table in held:
            m.release(table)
