"""Tests for `canonicalize_symbols`, the table form of the symbol converters."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

import jpy
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


YAHOO = SymbolConvention.YAHOO


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


def test_text_a_converter_does_not_recognize_stays_as_written(dh_exec_ctx: object) -> None:
    # Massive answers it: the library judges no ticker's shape.
    table = new_table([string_col("Symbol", ["EUR/USD", "not a pair"])])
    result = dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=OnError.RAISE)
    assert values(result) == ["C:EURUSD", "not a pair"]


def test_a_bad_value_of_a_static_table_raises(dh_exec_ctx: object) -> None:
    # A Yahoo dot names a listing outside the US, and Massive's dot a share class.
    table = new_table([string_col("Symbol", ["BRK-B", *(f"V{i}.L" for i in range(1, 7))])])
    with pytest.raises(
        ValueError, match=r"^6 value\(s\) of Symbol do not convert to stocks symbols: "
    ) as raised:
        dm.canonicalize_symbols(table, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.RAISE)
    message = str(raised.value)
    assert "'V1.L' names another instrument" in message
    assert "'V5.L'" in message  # the first five in sorted order
    assert "'V6.L'" not in message
    assert message.endswith("Pass on_error=dm.OnError.LOG to null them instead")


def test_a_bad_value_of_a_static_table_is_nulled_and_logged(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    bad = [f"B{i:02}.L" for i in range(25)]
    # Out of order in the table: the values are logged sorted, before the formula runs.
    table = new_table([string_col("Symbol", ["BRK-B", *reversed(bad), "B00.L"])])
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        result = dm.canonicalize_symbols(
            table, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.LOG
        )
    assert values(result) == ["BRK.B", *[None] * 26]
    messages = [r.getMessage() for r in caplog.records]
    assert len(messages) == 21  # twenty values, then one record that says the rest are not
    assert messages[0].startswith("canonicalize_symbols: 'B00.L' names another instrument")
    assert messages[0].endswith("; its rows get a null Symbol")
    assert messages[-1] == "20 values were reported; further ones are not logged"


def test_a_bad_value_under_ignore_is_nulled_and_logged_only_at_debug(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    # A converter refuses only a spelling Massive reads as another instrument.
    table = new_table([string_col("Symbol", ["BRK-B", "IGN.L"])])
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.symbols"):
        result = dm.canonicalize_symbols(
            table, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.IGNORE
        )
    assert values(result) == ["BRK.B", None]
    [(level, message)] = [(r.levelno, r.getMessage()) for r in caplog.records]
    assert level == logging.DEBUG
    assert message.startswith("canonicalize_symbols: 'IGN.L' names another instrument")
    assert message.endswith("; its rows get a null Symbol")


def test_canonicalize_a_refreshing_table(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(
            source, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.RAISE
        )
    assert result.is_refreshing
    source.add(new_table([string_col("Symbol", ["BRK-B", None])]))
    wait_for(lambda: result.size == 2)
    assert values(result) == ["BRK.B", None]
    source.add(new_table([string_col("Symbol", ["VOD.L"])]))  # a later bad row fails the table
    wait_for(lambda: bool(result.j_table.isFailed()))
    scope.release()


def test_a_bad_initial_row_of_a_refreshing_table_raises(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["VOD.L"])]))
    scope = new_scope()
    with (
        scope.open(),
        pytest.raises(ValueError, match=r"^1 value\(s\) of Symbol do not convert to stocks"),
    ):
        dm.canonicalize_symbols(source, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.RAISE)
    scope.release()


def test_a_bad_row_of_a_refreshing_table_is_nulled(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(
            source, AssetClass.STOCKS, convention=YAHOO, on_error=OnError.LOG
        )
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        source.add(new_table([string_col("Symbol", ["BRK-B", "VOD.L", None, "VOD.L"])]))
        wait_for(lambda: result.size == 4)
    assert values(result) == ["BRK.B", None, None, None]
    assert caplog.text.count("'VOD.L' names another instrument") == 1
    failed = bool(result.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
    scope.release()


def test_canonicalize_symbols_rejects_bad_arguments(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:EURUSD"]), int_col("Size", [1])])
    with pytest.raises(TypeError, match=r"^table must be a Deephaven Table, not list$"):
        dm.canonicalize_symbols([], AssetClass.FOREX)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"^asset_class must be dm\.AssetClass"):
        dm.canonicalize_symbols(table, "forex")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonicalize_symbols(
            table,
            AssetClass.STOCKS,
            convention="yahoo",  # type: ignore[arg-type]  # the point: refused at run time
        )
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonicalize_symbols(
            table,
            AssetClass.FOREX,
            convention=None,  # type: ignore[arg-type]  # the point: refused at run time
        )
    with pytest.raises(TypeError, match=r"^on_error must be dm\.OnError, e\.g\. dm\.OnError\.LOG$"):
        dm.canonicalize_symbols(
            table,
            AssetClass.FOREX,
            on_error="log",  # type: ignore[arg-type]  # the point: refused at run time
        )
    with pytest.raises(
        ValueError, match=r"^the YAHOO convention applies to stocks only, not forex$"
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, convention=SymbolConvention.YAHOO)
    with pytest.raises(TypeError, match=r"^column must be a str, not int$"):
        dm.canonicalize_symbols(
            table,
            AssetClass.FOREX,
            column=1,  # type: ignore[arg-type]  # the point: refused at run time
        )
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
        return dm.canonical_symbol(text, AssetClass.STOCKS, convention=YAHOO)

    def refuse(value: str, reason: str) -> None:
        refused.append((value, reason))

    strict = _row_converter(convert, refuse, OnError.RAISE)
    assert strict(None) is None
    assert strict("BRK-B") == "BRK.B"
    with pytest.raises(ValueError, match=r"'VOD\.L' names another instrument"):
        strict("VOD.L")
    lenient = _row_converter(convert, refuse, OnError.LOG)
    assert lenient("VOD.L") is None
    assert lenient("BAC-PL") == "BACpL"
    assert [value for value, _ in refused] == ["VOD.L"]


def _listeners(table: Table) -> int:
    # The number of child listeners a Deephaven table holds: a private engine field, read by
    # commons-lang3 (shipped with Deephaven) through jpy, which ships no type stubs. The read
    # runs in Java because JDK 17's setAccessible fails when called over JNI with no Java
    # caller frame.
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
