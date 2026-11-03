"""Tests for `canonicalize_symbols`, the table form of the symbol converters."""

from __future__ import annotations

import importlib
import logging
import threading
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, new_table
from deephaven.column import int_col, string_col

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.symbols import _row_converter

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table
    from deephaven.table_factory import InputTable


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
        dm.canonicalize_symbols(table, AssetClass.STOCKS, on_error=OnError.RAISE)
    message = str(raised.value)
    assert "'BRK-B' is not a Massive stock ticker" in message
    assert "'b4'" in message  # the first five in sorted order
    assert "'b5'" not in message
    assert message.endswith("Pass on_error=dm.OnError.LOG to null them instead")


def test_a_bad_value_of_a_static_table_is_nulled_and_logged(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    bad = [f"b{i:02}" for i in range(25)]
    # Out of order in the table: the values are logged sorted, before the formula runs.
    table = new_table([string_col("Symbol", ["EUR/USD", *reversed(bad), "b00"])])
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        result = dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=OnError.LOG)
    assert values(result) == ["C:EURUSD", *[None] * 26]
    messages = [r.getMessage() for r in caplog.records]
    assert len(messages) == 21  # twenty values, then one record that says the rest are not
    assert messages[0] == (
        "canonicalize_symbols: cannot read 'b00' as a forex pair of two three-letter codes; "
        "write it as BASE/QUOTE, such as 'EUR/USD'; its rows get a null Symbol"
    )
    assert messages[-1] == "20 values were reported; further ones are not logged"


def test_canonicalize_a_refreshing_table(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
    assert result.is_refreshing
    source.add(new_table([string_col("Symbol", ["BTC-USD", None])]))
    wait_for(lambda: result.size == 2)
    assert values(result) == ["X:BTCUSD", None]
    source.add(new_table([string_col("Symbol", ["no pe!"])]))  # a later bad row fails the table
    wait_for(lambda: bool(result.j_table.isFailed()))
    scope.release()


def test_a_bad_initial_row_of_a_refreshing_table_raises(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["no pe!"])]))
    scope = new_scope()
    with scope.open(), pytest.raises(ValueError, match=r"^1 value\(s\) of Symbol are not crypto"):
        dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
    scope.release()


def test_a_bad_row_of_a_refreshing_table_is_nulled(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.LOG)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        source.add(new_table([string_col("Symbol", ["BTC-USD", "no pe!", None, "no pe!"])]))
        wait_for(lambda: result.size == 4)
    assert values(result) == ["X:BTCUSD", None, None, None]
    assert caplog.text.count("cannot read 'no pe!'") == 1
    failed = bool(result.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
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
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, convention=cast("SymbolConvention", None))
    with pytest.raises(TypeError, match=r"^on_error must be dm\.OnError, e\.g\. dm\.OnError\.LOG$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=cast("OnError", "log"))
    with pytest.raises(
        ValueError, match=r"^the YAHOO convention applies to stocks only, not forex$"
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, convention=SymbolConvention.YAHOO)
    with pytest.raises(TypeError, match=r"^column must be a str, not int$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column=cast("str", 1))
    with pytest.raises(ValueError, match=r"^column 'Size' must be a String column of the table$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Size")
    with pytest.raises(
        ValueError,
        match=r"^table has no column 'Pair' \(columns: Symbol, Size\); pass column='Symbol', ",
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Pair")
    tickers = new_table([string_col("Ticker", ["C:EURUSD"])])
    with pytest.raises(
        ValueError, match=r"^table has no column 'Symbol' \(columns: Ticker\); pass column='Ticker'"
    ):
        dm.canonicalize_symbols(tickers, AssetClass.FOREX)
    sizes = new_table([int_col("Size", [1])])
    with pytest.raises(
        ValueError, match=r"^table has no column 'Symbol' \(columns: Size\); it has no String"
    ):
        dm.canonicalize_symbols(sizes, AssetClass.FOREX)


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
    with pytest.raises(ValueError, match="cannot read 'no pe!'"):
        strict("no pe!")
    lenient = _row_converter(convert, refuse, OnError.LOG)
    assert lenient("no pe!") is None
    assert lenient("GBP-USD") == "C:GBPUSD"
    assert [value for value, _ in refused] == ["no pe!"]


def _listeners(table: Table) -> int:
    # The number of child listeners a Deephaven table holds: a private engine field, read by
    # commons-lang3 (shipped with Deephaven) through jpy, which ships no type stubs. The read
    # runs in Java because JDK 17's setAccessible fails when called over JNI with no Java
    # caller frame.
    jpy = cast("Any", importlib.import_module("jpy"))
    fields = jpy.get_type("org.apache.commons.lang3.reflect.FieldUtils")
    force_access = True  # Java takes it positionally: readField(target, name, forceAccess)
    references = fields.readField(table.j_table, "childListenerReferences", force_access)
    manager = jpy.get_type("io.deephaven.util.datastructures.SimpleReferenceManager")
    held = jpy.cast(references, manager)
    if held is not None:
        return int(held.size())
    # One child is held directly as its reference rather than in a manager.
    return 0 if references is None else 1


def test_canonicalizing_a_refreshing_table_leaves_no_hidden_table(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    # The pre-check reads the rows without a derived table, so the source's only child is the
    # result's own formula, and none is left once the scope is released.
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["EUR/USD", "EUR/USD"])]))
    assert _listeners(source) == 0
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.FOREX)
    assert _listeners(source) == 1
    assert values(result) == ["C:EURUSD", "C:EURUSD"]
    scope.release()
    assert _listeners(source) == 0
