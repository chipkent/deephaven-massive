"""Tests for the canonical view of a reactive symbol table and the watch that follows it."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table, time_table
from deephaven.column import int_col, string_col

from deephaven_massive._enums import AssetClass
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.watchlist import SymbolWatch, _SymbolCounter, canonical_view

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest
    from deephaven.table import Table


def test_canonical_view(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["btc-usd", "X:ETHUSD", "??"])])
    view = canonical_view(AssetClass.CRYPTO, table)
    assert dh_arrow.to_arrow(view).column("Symbol").to_pylist() == ["X:BTCUSD", "X:ETHUSD", None]


def test_canonical_view_without_a_class_reads_each_prefix(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["aapl", "C:EURUSD", "I:SPX", "Z:BAD"])])
    view = canonical_view(None, table)
    symbols = dh_arrow.to_arrow(view).column("Symbol").to_pylist()
    assert symbols == ["AAPL", "C:EURUSD", "I:SPX", None]


Change = tuple[frozenset[str], frozenset[str]]


def watching(table: Table) -> tuple[SymbolWatch, list[Change]]:
    seen: list[Change] = []
    lock = threading.Lock()

    def record(added: frozenset[str], removed: frozenset[str]) -> None:
        with lock:
            seen.append((added, removed))

    return SymbolWatch(AssetClass.FOREX, table, record), seen


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def test_a_watch_reports_changes_of_the_symbol_set(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watchlist.add(
        new_table([int_col("Key", [1, 2, 3]), string_col("Symbol", ["EUR/USD", "eurusd", "??"])])
    )
    watch, seen = watching(watchlist)
    watch.start()  # the replay: two spellings of one pair count once; "??" is ignored
    assert seen == [(frozenset({"C:EURUSD"}), frozenset())]
    watchlist.delete(new_table([int_col("Key", [1])]))  # one of its two rows: still held
    watchlist.add(new_table([int_col("Key", [3]), string_col("Symbol", ["GBP/USD"])]))  # modify
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"C:GBPUSD"}), frozenset())
    watchlist.delete(new_table([int_col("Key", [2])]))
    wait_for(lambda: len(seen) == 3)
    assert seen[2] == (frozenset(), frozenset({"C:EURUSD"}))


def test_an_empty_table_reports_nothing(dh_exec_ctx: object) -> None:
    watch, seen = watching(input_table({"Symbol": dtypes.string}))
    watch.start()
    assert seen == []


def test_a_failing_watch_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    _SymbolCounter(lambda *_: None).on_error(RuntimeError("listener broke"))
    assert "a symbol table's watch failed and stops following it: listener broke" in caplog.text


def test_a_watch_lives_as_long_as_its_keeper(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    keeper_scope = new_scope()
    with keeper_scope.open():
        keeper = time_table("PT1s")
    watch_scope = new_scope()
    with watch_scope.open():
        watch, seen = watching(watchlist)
        watch.keep_alive_with(keeper)
    watch.start()
    watch_scope.release()  # the keeper still holds the view and the listener
    watchlist.add(new_table([string_col("Symbol", ["EUR/USD"])]))
    wait_for(lambda: len(seen) == 1)
    keeper_scope.release()
