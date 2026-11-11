"""Tests for `deephaven_massive.typehints`, the public package of query and argument types."""

import json
import subprocess
import sys
import typing
from collections.abc import Sequence

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import typehints
from deephaven_massive._datasets import dates, filters, symbols
from deephaven_massive._session import CATALOG, MassiveSession
from deephaven_massive.typehints import _queries

_QUERY_CLASSES = (
    "FetchQuery",
    "HistoryQuery",
    "Query",
    "StreamHistoryQuery",
    "StreamQuery",
    "SymbolStreamQuery",
)


def test_the_package_exports_the_query_classes_and_argument_types() -> None:
    assert sorted(typehints.__all__) == sorted(
        [*_QUERY_CLASSES, "AllSymbols", "DateLike", "FilterValue", "Symbols"]
    )
    for name in _QUERY_CLASSES:
        assert getattr(typehints, name) is getattr(_queries, name)
    assert typehints.AllSymbols is symbols.AllSymbols
    assert typehints.DateLike is dates.DateLike
    assert typehints.FilterValue is filters.FilterValue


def test_symbols_alias() -> None:
    assert set(typing.get_args(typehints.Symbols)) == {
        str,
        Sequence[str],
        Table,
        typehints.AllSymbols,
    }


@pytest.mark.parametrize("name", [*_QUERY_CLASSES, "AllSymbols"])
def test_each_class_docstring_gives_its_import(name: str) -> None:
    text = getattr(typehints, name).__doc__ or ""
    assert f"from deephaven_massive.typehints import {name}\n" in text


def test_query_is_the_base_of_every_query_class() -> None:
    for name in _QUERY_CLASSES:
        assert issubclass(getattr(typehints, name), typehints.Query)


def test_all_is_an_instance_of_the_exported_class() -> None:
    assert isinstance(dm.ALL, typehints.AllSymbols)


def test_every_dataset_returns_an_exported_query_class() -> None:
    exported = {getattr(typehints, name) for name in typehints.__all__}
    for spec in CATALOG.by_name.values():
        assert _queries.query_class(spec) in exported, spec.name


def test_a_dataset_method_returns_one_of_its_classes(massive_session: MassiveSession) -> None:
    query = massive_session.stocks.trades("AAPL")
    assert isinstance(query, typehints.StreamHistoryQuery)
    assert isinstance(query, typehints.SymbolStreamQuery)
    assert isinstance(query, typehints.StreamQuery)
    assert isinstance(query, typehints.HistoryQuery)
    assert isinstance(query, typehints.Query)


def test_a_stream_without_symbols_is_a_plain_stream_query(massive_session: MassiveSession) -> None:
    query = massive_session.options.admin_messages()
    assert type(query) is typehints.StreamQuery
    assert not isinstance(query, typehints.SymbolStreamQuery)


def test_importing_the_package_root_does_not_import_it() -> None:
    # A fresh interpreter without a JVM: importing the package does not load the types.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import json, sys\n"
                "import deephaven_massive\n"
                "print(json.dumps('deephaven_massive.typehints' in sys.modules))"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) is False
