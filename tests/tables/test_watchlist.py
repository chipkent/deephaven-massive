"""Tests for the canonical view of a reactive symbol table."""

from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col

from deephaven_massive._enums import AssetClass
from deephaven_massive._tables.watchlist import canonical_view


def test_canonical_view(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["btc-usd", "X:ETHUSD", "??"])])
    view = canonical_view(AssetClass.CRYPTO, table)
    assert dh_arrow.to_arrow(view).column("Symbol").to_pylist() == ["X:BTCUSD", "X:ETHUSD", None]


def test_canonical_view_without_a_class_reads_each_prefix(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["aapl", "C:EURUSD", "I:SPX", "Z:BAD"])])
    view = canonical_view(None, table)
    symbols = dh_arrow.to_arrow(view).column("Symbol").to_pylist()
    assert symbols == ["AAPL", "C:EURUSD", "I:SPX", None]
