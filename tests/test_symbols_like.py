"""Tests for `_symbols_like.py`: `dm.SymbolsLike`, every form a dataset's `symbols` takes."""

import typing
from collections.abc import Sequence

from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import AllSymbols


def test_symbols_like_names_every_form_symbols_takes() -> None:
    assert set(typing.get_args(dm.SymbolsLike)) == {str, Sequence[str], Table, AllSymbols}


def test_all_is_an_instance_of_the_symbols_like_member() -> None:
    assert isinstance(dm.ALL, AllSymbols)
