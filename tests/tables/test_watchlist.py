"""Tests for the check of a symbol table's rows, its watch, and `canonicalize_symbols`."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, cast

import pytest
from deephaven import dtypes, input_table, new_table, time_table
from deephaven.column import int_col, string_col
from deephaven.dherror import DHError

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.watchlist import (
    SymbolWatch,
    _row_converter,
    _SymbolCounter,
    invalid_row_message,
    read_symbols,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

Change = tuple[frozenset[str], frozenset[str]]


def values(table: Table, column: str = "Symbol") -> list[object]:
    return [row[0] for row in table.iter_tuple(cols=column)]


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


# --- read_symbols and the message ------------------------------------------------------------


def test_read_symbols_returns_the_rows_sorted_without_nulls(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:GBPUSD", None, "C:EURUSD", "C:GBPUSD"])])
    assert read_symbols(AssetClass.FOREX, table) == ("C:EURUSD", "C:GBPUSD")
    assert read_symbols(AssetClass.FOREX, new_table([string_col("Symbol", [None])])) == ()


def test_read_symbols_refuses_a_row_that_is_not_canonical(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:EURUSD", "EUR/USD"])])
    with pytest.raises(
        ValueError,
        match=r"^a symbol table holds 'EUR/USD' \('EUR/USD' is not Massive's ticker; use "
        r"'C:EURUSD' .*\)\. Fix the row, or pass the table through dm\.canonicalize_symbols"
        r"\(table, dm\.AssetClass\.FOREX, on_error=dm\.OnError\.NULL\), which converts what it "
        r"can and nulls the rest$",
    ):
        read_symbols(AssetClass.FOREX, table)


def test_the_message_of_a_dataset_that_spans_classes_names_no_class() -> None:
    message = invalid_row_message(None, "aapl")
    assert "did you mean 'AAPL'?" in message
    assert "dm.canonicalize_symbols(table, dm.AssetClass.<class>, " in message


# --- SymbolWatch ---------------------------------------------------------------------------------


def watching(table: Table) -> tuple[SymbolWatch, list[Change], list[str]]:
    seen: list[Change] = []
    refused: list[str] = []
    lock = threading.Lock()

    def record(added: frozenset[str], removed: frozenset[str]) -> None:
        with lock:
            seen.append((added, removed))

    def refuse(message: str) -> None:
        with lock:
            refused.append(message)

    return SymbolWatch(AssetClass.FOREX, table, record, on_invalid=refuse), seen, refused


def test_a_watch_reports_changes_of_the_symbol_set(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watchlist.add(
        new_table([int_col("Key", [1, 2, 3]), string_col("Symbol", ["C:EURUSD", "C:EURUSD", None])])
    )
    watch, seen, refused = watching(watchlist)
    watch.start()  # the replay: two rows of one pair count once; the null row is ignored
    assert seen == [(frozenset({"C:EURUSD"}), frozenset())]
    watchlist.delete(new_table([int_col("Key", [1])]))  # one of its two rows: still held
    watchlist.add(new_table([int_col("Key", [3]), string_col("Symbol", ["C:GBPUSD"])]))  # modify
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"C:GBPUSD"}), frozenset())
    watchlist.delete(new_table([int_col("Key", [2])]))
    wait_for(lambda: len(seen) == 3)
    assert seen[2] == (frozenset(), frozenset({"C:EURUSD"}))
    assert refused == []


def test_a_watch_refuses_a_late_row_that_is_not_canonical(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watch, seen, refused = watching(watchlist)
    watch.start()
    watchlist.add(
        new_table([int_col("Key", [1, 2]), string_col("Symbol", ["EUR/USD", "C:GBPUSD"])])
    )
    wait_for(lambda: len(seen) == 1 and len(refused) == 1)
    assert seen == [(frozenset({"C:GBPUSD"}), frozenset())]  # the bad row is never subscribed
    assert refused == [invalid_row_message(AssetClass.FOREX, "EUR/USD")]
    watchlist.delete(new_table([int_col("Key", [1])]))  # removing it changes nothing
    watchlist.add(new_table([int_col("Key", [3]), string_col("Symbol", ["C:USDJPY"])]))
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"C:USDJPY"}), frozenset())
    assert len(refused) == 1


def test_an_empty_table_reports_nothing(dh_exec_ctx: object) -> None:
    watch, seen, _ = watching(input_table({"Symbol": dtypes.string}))
    watch.start()
    assert seen == []


def test_a_failing_watch_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    counter = _SymbolCounter(None, lambda *_: None, lambda _: None)
    counter.on_error(RuntimeError("listener broke"))
    assert "a symbol table's watch failed and stops following it: listener broke" in caplog.text


def test_a_watch_lives_as_long_as_its_keeper(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    keeper_scope = new_scope()
    with keeper_scope.open():
        keeper = time_table("PT1s")
    watch_scope = new_scope()
    with watch_scope.open():
        watch, seen, _ = watching(watchlist)
        watch.keep_alive_with(keeper)
    watch.start()
    watch_scope.release()  # the keeper still holds the view and the listener
    watchlist.add(new_table([string_col("Symbol", ["C:EURUSD"])]))
    wait_for(lambda: len(seen) == 1)
    keeper_scope.release()


class _Update:
    # The parts of a `TableUpdate` the counter reads, for calls from a Python thread: Deephaven
    # delivers real updates on its own threads, which line coverage before 3.14 cannot see.
    def __init__(self, **columns: list[str]) -> None:
        self._columns = columns

    def added(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("added", [])}

    def modified(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("modified", [])}

    def modified_prev(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("modified_prev", [])}

    def removed(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("removed", [])}


def test_the_counter_reports_set_changes_of_updates() -> None:
    seen: list[Change] = []
    refused: list[str] = []
    counter = _SymbolCounter(None, lambda a, r: seen.append((a, r)), refused.append)
    updates = [
        _Update(added=["A", "A", "B", "b"]),
        _Update(removed=["A", "b"]),  # one of A's two rows remains; a bad row leaves quietly
        _Update(modified=["C"], modified_prev=["A"]),  # A's last row now holds C
        _Update(modified=["x:btc"], modified_prev=["C"]),  # C's row now holds a bad symbol
    ]
    for update in updates:
        counter.on_update(cast("TableUpdate", update), is_replay=False)
    assert seen == [
        (frozenset({"A", "B"}), frozenset()),
        (frozenset({"C"}), frozenset({"A"})),
        (frozenset(), frozenset({"C"})),
    ]
    assert refused == [invalid_row_message(None, "b"), invalid_row_message(None, "x:btc")]


# --- canonicalize_symbols ------------------------------------------------------------------------


def test_canonicalize_a_static_table(dh_exec_ctx: object) -> None:
    table = new_table(
        [
            string_col("Pair", ["EUR/USD", None, "gbp-usd", "C:USDJPY"]),
            int_col("Size", [1, 2, 3, 4]),
        ]
    )
    result = dm.canonicalize_symbols(table, AssetClass.FOREX, column="Pair")
    assert values(result, "Pair") == ["C:EURUSD", None, "C:GBPUSD", "C:USDJPY"]
    assert values(result, "Size") == [1, 2, 3, 4]
    assert not result.is_refreshing


def test_canonicalize_a_static_table_with_a_convention(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["BRK-B", "BAC-PL", "AAPL"])])
    result = dm.canonicalize_symbols(table, AssetClass.STOCKS, convention=SymbolConvention.YAHOO)
    assert values(result) == ["BRK.B", "BACpL", "AAPL"]


def test_a_bad_value_of_a_static_table_raises(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["BRK-B", "b1", "b2", "b3", "b4", "b5", "b6"])])
    with pytest.raises(
        ValueError, match=r"^7 value\(s\) of Symbol are not stocks symbols: "
    ) as raised:
        dm.canonicalize_symbols(table, AssetClass.STOCKS)
    message = str(raised.value)
    assert "'BRK-B' is not a Massive stock ticker" in message
    assert "'b4'" in message  # the first five in sorted order
    assert "'b5'" not in message
    assert message.endswith("Pass on_error=dm.OnError.NULL to null them instead")


def test_a_bad_value_of_a_static_table_is_nulled_and_logged(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    bad = [f"b{i:02}" for i in range(25)]
    table = new_table([string_col("Symbol", ["EUR/USD", *bad, "b00"])])
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.watchlist"):
        result = dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=OnError.NULL)
    assert values(result) == ["C:EURUSD", *[None] * 26]
    messages = [r.getMessage() for r in caplog.records]
    assert len(messages) == 21  # twenty values, then one record that says the rest are not
    assert messages[0] == (
        "canonicalize_symbols: cannot read 'b00' as a forex pair of two three-letter codes; "
        "write it as BASE/QUOTE, such as 'EUR/USD'; its rows get a null Symbol"
    )
    assert messages[-1] == "20 values were reported; further ones are not logged"


def test_canonicalize_a_refreshing_table(dh_exec_ctx: object) -> None:
    source = input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO)
    assert result.is_refreshing
    source.add(new_table([string_col("Symbol", ["BTC-USD", None])]))
    wait_for(lambda: result.size == 2)
    assert values(result) == ["X:BTCUSD", None]
    source.add(new_table([string_col("Symbol", ["nope"])]))  # a later bad row fails the table
    wait_for(lambda: bool(result.j_table.isFailed()))
    scope.release()


def test_a_bad_initial_row_of_a_refreshing_table_raises(dh_exec_ctx: object) -> None:
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["nope"])]))
    scope = new_scope()
    with scope.open(), pytest.raises(DHError, match="cannot read 'nope' as a crypto pair"):
        dm.canonicalize_symbols(source, AssetClass.CRYPTO)
    scope.release()


def test_a_bad_row_of_a_refreshing_table_is_nulled(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    source = input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.NULL)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.watchlist"):
        source.add(new_table([string_col("Symbol", ["BTC-USD", "nope", None, "nope"])]))
        wait_for(lambda: result.size == 4)
    assert values(result) == ["X:BTCUSD", None, None, None]
    assert caplog.text.count("cannot read 'nope'") == 1
    assert not result.j_table.isFailed()
    scope.release()


def test_canonicalize_symbols_rejects_bad_arguments(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:EURUSD"]), int_col("Size", [1])])
    with pytest.raises(TypeError, match=r"^table must be a Deephaven Table, not list$"):
        dm.canonicalize_symbols(cast("Table", []), AssetClass.FOREX)
    with pytest.raises(TypeError, match=r"^asset_class must be dm\.AssetClass"):
        dm.canonicalize_symbols(table, cast("AssetClass", "forex"))
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonicalize_symbols(
            table, AssetClass.STOCKS, convention=cast("SymbolConvention", "yahoo")
        )
    with pytest.raises(
        TypeError, match=r"^on_error must be dm\.OnError, e\.g\. dm\.OnError\.NULL$"
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=cast("OnError", "null"))
    with pytest.raises(ValueError, match=r"^a convention applies to stocks only, not forex$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, convention=SymbolConvention.YAHOO)
    with pytest.raises(TypeError, match=r"^column must be a str, not int$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column=cast("str", 1))
    with pytest.raises(ValueError, match=r"^column 'Size' must be a String column of the table$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Size")
    with pytest.raises(ValueError, match=r"^column 'Pair' must be a String column of the table$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Pair")


def test_the_row_converter_raises_or_nulls() -> None:
    # Called from a Python thread: Deephaven runs a formula on its own threads, which line
    # coverage before 3.14 cannot see.
    refused: list[tuple[str, str]] = []

    def convert(text: str) -> str | None:
        return dm.canonical_symbol(text, AssetClass.FOREX)

    def refuse(value: str, reason: str) -> None:
        refused.append((value, reason))

    strict = _row_converter(convert, refuse, OnError.RAISE)
    assert strict(None) is None
    assert strict("EUR/USD") == "C:EURUSD"
    with pytest.raises(ValueError, match="cannot read 'nope'"):
        strict("nope")
    lenient = _row_converter(convert, refuse, OnError.NULL)
    assert lenient("nope") is None
    assert lenient("GBP-USD") == "C:GBPUSD"
    assert [value for value, _ in refused] == ["nope"]
