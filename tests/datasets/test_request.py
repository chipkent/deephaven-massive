"""Tests for the `Request` record and the other plain records of a request."""

from datetime import date

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.request import HostDecision, Request
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import Recency, Timespan


def test_symbol_keys(dh_exec_ctx: object) -> None:
    assert Request(symbols=("A",)).symbol_key() == ("A",)
    assert Request(symbols=dm.ALL).symbol_key() is dm.ALL
    table = new_table([string_col("Symbol", ["A"])])
    assert Request(symbols=SymbolTable(table)).symbol_key() == SymbolTable(table)
    with pytest.raises(ValueError, match="needs symbols"):
        Request().symbol_key()


def test_describe_shows_only_the_fields_that_differ_from_their_defaults() -> None:
    assert Request().describe() == ""
    request = Request(
        symbols=("C:EURUSD",),
        timespan=Timespan.MINUTE,
        multiplier=5,
        adjusted=False,
        start=date(2025, 1, 2),
        params=(("sort", "timestamp"),),
        filters=(("limit", 3),),
    )
    assert request.describe() == (
        "symbols=('C:EURUSD',) timespan=Timespan.MINUTE multiplier=5 adjusted=False "
        "start=2025-01-02 params={'sort': 'timestamp'} filters={'limit': 3}"
    )


def test_describe_renders_all_and_a_reactive_table(dh_exec_ctx: object) -> None:
    assert Request(symbols=dm.ALL).describe() == "symbols=ALL"
    table = new_table([string_col("Symbol", ["A"])])
    assert Request(symbols=SymbolTable(table)).describe() == "symbols=<symbol table>"


def test_requests_and_host_decisions_are_hashable_values() -> None:
    assert Request(symbols=("A",)) == Request(symbols=("A",))
    assert hash(Request(symbols=("A",))) == hash(Request(symbols=("A",)))
    decision = HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")
    assert decision == HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")
    assert {decision: 1}[HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")] == 1
