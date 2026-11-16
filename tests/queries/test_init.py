"""Tests for `deephaven_massive.queries`, the public package of the query classes."""

import re

import pytest

from deephaven_massive import queries
from deephaven_massive._session import CATALOG, MassiveSession
from deephaven_massive.queries import _fetch, _history, _query, _stream
from deephaven_massive.queries._choice import query_class

_DEFINED = {
    "FetchQuery": _fetch,
    "HistoryQuery": _history,
    "Query": _query,
    "StreamHistoryQuery": _history,
    "NoSymbolStreamQuery": _stream,
    "StreamQuery": _stream,
}


def test_the_package_exports_the_query_classes_from_their_modules() -> None:
    assert sorted(queries.__all__) == sorted(_DEFINED)
    for name, module in _DEFINED.items():
        assert getattr(queries, name) is getattr(module, name)


@pytest.mark.parametrize("name", sorted(_DEFINED))
def test_each_class_docstring_gives_its_import(name: str) -> None:
    text = getattr(queries, name).__doc__ or ""
    assert re.search(rf"from deephaven_massive\.queries import {name}\b", text)


def test_query_is_the_base_of_every_query_class() -> None:
    for name in _DEFINED:
        assert issubclass(getattr(queries, name), queries.Query)


def test_every_dataset_returns_an_exported_query_class() -> None:
    exported = {getattr(queries, name) for name in queries.__all__}
    for spec in CATALOG.by_name.values():
        assert query_class(spec) in exported, spec.name


def test_a_dataset_method_returns_one_of_its_classes(massive_session: MassiveSession) -> None:
    query = massive_session.stocks.trades("AAPL")
    assert isinstance(query, queries.StreamHistoryQuery)
    assert isinstance(query, queries.StreamQuery)
    assert not isinstance(query, queries.NoSymbolStreamQuery)
    assert isinstance(query, queries.HistoryQuery)
    assert isinstance(query, queries.Query)


def test_a_stream_without_symbols_is_a_no_symbol_stream_query(
    massive_session: MassiveSession,
) -> None:
    query = massive_session.options.admin_messages()
    assert type(query) is queries.NoSymbolStreamQuery
    assert not isinstance(query, queries.StreamQuery)
