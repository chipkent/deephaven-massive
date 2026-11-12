"""Tests for the `Request` record and the other plain records of a request."""

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.request import Request, ValueTable
from deephaven_massive._datasets.symbols import SymbolTable


def test_symbol_keys(dh_exec_ctx: object) -> None:
    assert Request(symbols=("A",)).symbol_key() == ("A",)
    assert Request(symbols=dm.ALL).symbol_key() is dm.ALL
    table = new_table([string_col("Symbol", ["A"])])
    assert Request(symbols=SymbolTable(table)).symbol_key() == SymbolTable(table)
    with pytest.raises(ValueError, match="needs symbols"):
        Request().symbol_key()


def test_requests_are_hashable_values() -> None:
    assert Request(symbols=("A",)) == Request(symbols=("A",))
    assert hash(Request(symbols=("A",))) == hash(Request(symbols=("A",)))


def test_a_value_table_compares_by_its_wrapper_and_sends_each_value_alone(
    dh_exec_ctx: object,
) -> None:
    codes = new_table([string_col("ProductCode", ["ES", "NQ"])])
    values = ValueTable("product_code", "product_code", "ProductCode", table=codes)
    assert values == ValueTable("product_code", "product_code", "ProductCode", table=codes)
    other = new_table([string_col("ProductCode", ["ES", "NQ"])])
    assert values != ValueTable("product_code", "product_code", "ProductCode", table=other)
    request = Request(params=(("date", "2026-11-02"),), limit=5, value_table=values)
    # One value is the request the method builds for it alone, its parameter in name order.
    assert request.for_value("ES") == Request(
        params=(("date", "2026-11-02"), ("product_code", "ES")), limit=5
    )
