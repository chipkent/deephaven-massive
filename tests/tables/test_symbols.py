"""Tests for `canonicalize_symbols`, the table form of the symbol converters."""

from __future__ import annotations

import logging
import re
import threading
from typing import TYPE_CHECKING

import jpy
import pytest
from deephaven import dtypes, new_table
from deephaven.column import int_col, string_col

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import strict_converter
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.symbols import _row_converter

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
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


def test_canonicalize_a_static_table(dh_exec_ctx: ExecutionContext) -> None:
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


def test_canonicalize_a_static_table_with_a_convention(dh_exec_ctx: ExecutionContext) -> None:
    table = new_table([string_col("Symbol", ["BRK-B", "BAC-PL", "AAPL"])])
    result = dm.canonicalize_symbols(table, AssetClass.STOCKS, convention=SymbolConvention.YAHOO)
    assert values(result) == ["BRK.B", "BACpL", "AAPL"]


def test_a_convention_names_case_whitespace_and_emptiness_per_row(
    dh_exec_ctx: ExecutionContext,
) -> None:
    table = new_table([string_col("Symbol", ["brk-b", "BRK-B ", ""])])
    with pytest.raises(
        ValueError, match=r"^canonicalize_symbols cannot convert 3 values"
    ) as raised:
        dm.canonicalize_symbols(
            table, AssetClass.STOCKS, convention=SymbolConvention.YAHOO, on_error=OnError.RAISE
        )
    assert str(raised.value) == (
        "canonicalize_symbols cannot convert 3 values of the Symbol column to stock tickers. '' "
        "is empty, "
        "not a ticker. 'BRK-B ' has surrounding whitespace. Pass 'BRK-B'. 'brk-b' is not a "
        "YAHOO ticker. The YAHOO convention writes tickers in upper case. If you mean 'BRK-B', "
        "pass 'BRK-B'. Correct those values, or pass on_error=dm.OnError.LOG to give their rows "
        "a null symbol."
    )


def test_a_bad_value_of_a_static_table_raises(dh_exec_ctx: ExecutionContext) -> None:
    table = new_table([string_col("Symbol", ["BRK-B", "b1", "b2", "b3", "b4", "b5", "b6"])])
    with pytest.raises(
        ValueError,
        match=r"^canonicalize_symbols cannot convert 7 values of the Symbol column to stock "
        r"tickers\. The "
        r"first 5: 'BRK-B' is not a Massive stock ticker\. ",
    ) as raised:
        dm.canonicalize_symbols(table, AssetClass.STOCKS, on_error=OnError.RAISE)
    message = str(raised.value)
    assert "'b4'" in message  # the first five in sorted order
    assert "'b5'" not in message
    assert message.endswith(
        "Correct those values, or pass on_error=dm.OnError.LOG to give their rows a null symbol."
    )
    # The advice works: under LOG the bad rows get a null symbol and the table is made.
    logged = dm.canonicalize_symbols(table, AssetClass.STOCKS, on_error=OnError.LOG)
    assert values(logged) == [None] * 7


def test_a_bad_value_of_a_static_table_is_nulled_and_logged(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
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
        "canonicalize_symbols: 'b00' becomes a null Symbol. 'b00' is not a forex pair of two "
        "three-letter currency codes. Write it as BASE/QUOTE, such as 'EUR/USD'."
    )
    assert messages[-1] == "20 values were reported. Further ones are not logged."


def test_a_bad_value_under_ignore_is_nulled_and_logged_only_at_debug(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    table = new_table([string_col("Symbol", ["EUR/USD", "b00"])])
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.symbols"):
        result = dm.canonicalize_symbols(table, AssetClass.FOREX, on_error=OnError.IGNORE)
    assert values(result) == ["C:EURUSD", None]
    message = (
        "canonicalize_symbols: 'b00' becomes a null Symbol. 'b00' is not a forex pair of two "
        "three-letter currency codes. Write it as BASE/QUOTE, such as 'EUR/USD'."
    )
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [(logging.DEBUG, message)]


def test_canonicalize_a_refreshing_table(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
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
    release_scope(dh_exec_ctx, scope)


def test_a_bad_initial_row_of_a_refreshing_table_raises(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["no pe!"])]))
    scope = new_scope()
    with (
        scope.open(),
        pytest.raises(
            ValueError,
            match=r"^canonicalize_symbols cannot convert 1 value of the Symbol column to crypto "
            r"tickers\. "
            r"'no pe!' is not a crypto pair\. ",
        ),
    ):
        dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
    release_scope(dh_exec_ctx, scope)


def test_a_bad_row_of_a_refreshing_table_is_nulled(
    dh_exec_ctx: ExecutionContext,
    caplog: pytest.LogCaptureFixture,
    dh_input_table: Callable[..., InputTable],
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.LOG)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        source.add(new_table([string_col("Symbol", ["BTC-USD", "no pe!", None, "no pe!"])]))
        wait_for(lambda: result.size == 4)
    assert values(result) == ["X:BTCUSD", None, None, None]
    assert caplog.text.count("'no pe!' becomes a null Symbol") == 1
    failed = bool(result.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
    release_scope(dh_exec_ctx, scope)


def test_a_failing_input_fails_the_converted_table_and_nothing_else(
    dh_exec_ctx: ExecutionContext,
    *,
    caplog: pytest.LogCaptureFixture,
    dh_input_table: Callable[..., InputTable],
) -> None:
    # The watch of the refused values ignores its input's failure: the converted table fails
    # with its input on its own, and the watch's listener, run by the cycle that fails the
    # input, raises nothing.
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        strict = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
        result = dm.canonicalize_symbols(strict, AssetClass.CRYPTO, on_error=OnError.LOG)
    with caplog.at_level(logging.ERROR):
        source.add(new_table([string_col("Symbol", ["no pe!"])]))  # fails `strict`
        wait_for(lambda: bool(result.j_table.isFailed()))
        assert wait_for_update_cycle(dh_exec_ctx, 10.0)
    assert not [r for r in caplog.records if r.name.startswith("deephaven_massive")]
    release_scope(dh_exec_ctx, scope)


def test_canonicalize_symbols_rejects_bad_arguments(dh_exec_ctx: ExecutionContext) -> None:
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
        ValueError,
        match=r"^convention=dm\.SymbolConvention\.YAHOO is for stock tickers, not forex\. ",
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, convention=SymbolConvention.YAHOO)
    with pytest.raises(TypeError, match=r"^column must be a str, not int$"):
        dm.canonicalize_symbols(
            table,
            AssetClass.FOREX,
            column=1,  # type: ignore[arg-type]  # the point: refused at run time
        )
    with pytest.raises(
        ValueError,
        match=r"^column='Size' names a column of int, not of String\. Pass the name of a String "
        r"column of symbols\.$",
    ):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Size")
    pair = (
        "table has no column 'Pair'. Its columns are Symbol and Size. Pass column='Symbol', or "
        "the name of another String column of symbols."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(pair)}$"):
        dm.canonicalize_symbols(table, AssetClass.FOREX, column="Pair")
    assert values(dm.canonicalize_symbols(table, AssetClass.FOREX, column="Symbol")) == ["C:EURUSD"]
    tickers = new_table([string_col("Ticker", ["C:EURUSD"])])
    with pytest.raises(
        ValueError,
        match=r"^table has no column 'Symbol'\. Its columns are Ticker\. Pass column='Ticker'",
    ):
        dm.canonicalize_symbols(tickers, AssetClass.FOREX)
    sizes = new_table([int_col("Size", [1])])
    with pytest.raises(
        ValueError,
        match=r"^table has no column 'Symbol'\. Its columns are Size\. Pass a table with a "
        r"String column of symbols\.$",
    ):
        dm.canonicalize_symbols(sizes, AssetClass.FOREX)


def test_the_row_converter_raises_or_nulls(caplog: pytest.LogCaptureFixture) -> None:
    # Called from a Python thread: Deephaven runs a formula on its own threads, which line
    # coverage before 3.14 cannot see.
    convert = strict_converter(AssetClass.FOREX, SymbolConvention.MASSIVE)
    log = BoundedLog(logging.getLogger("deephaven_massive._tables.symbols"), limit=5, level=30)
    strict = _row_converter(
        convert, effect=effect_of(OnError.RAISE), log=log, consequence="becomes a null S"
    )
    assert strict(None) is None
    assert strict("EUR/USD") == "C:EURUSD"
    with pytest.raises(ValueError, match=r"^'no pe!' is not a forex pair of two"):
        strict("no pe!")
    lenient = _row_converter(
        convert, effect=effect_of(OnError.LOG), log=log, consequence="becomes a null S"
    )
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.symbols"):
        assert lenient("no pe!") is None
        assert lenient("GBP-USD") == "C:GBPUSD"
    assert caplog.messages == [
        (
            "canonicalize_symbols: 'no pe!' becomes a null S. 'no pe!' is not a forex pair of "
            "two three-letter currency codes. Write it as BASE/QUOTE, such as 'EUR/USD'."
        )
    ]


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
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    # The pre-check reads the rows without a derived table, so the source's children are the
    # result's own formula and the watch of the values it refuses, which the result keeps; none
    # is left once the scope is released.
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["EUR/USD", "EUR/USD"])]))
    assert _listeners(source) == 0
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.FOREX)
    assert _listeners(source) == 2
    assert values(result) == ["C:EURUSD", "C:EURUSD"]
    release_scope(dh_exec_ctx, scope)
    assert _listeners(source) == 0
