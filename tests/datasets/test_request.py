"""Tests for the `Request` record and the other plain records of a request."""

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.request import Request
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
