"""Tests for `deephaven_massive.queries`, the public module of query types."""

import json
import subprocess
import sys

import deephaven_massive as dm
from deephaven_massive import _query, _types, queries
from deephaven_massive._session import CATALOG, MassiveSession


def test_the_module_exports_the_query_classes_and_argument_types() -> None:
    assert sorted(queries.__all__) == [
        "AllSymbols",
        "DateLike",
        "FilterValue",
        "FuturesBarTimespan",
        "HistoryQuery",
        "Query",
        "StreamBarTimespan",
        "StreamHistoryQuery",
        "StreamQuery",
        "SymbolStreamQuery",
        "Symbols",
    ]
    assert queries.Query is _query.Query
    assert queries.StreamQuery is _query.StreamQuery
    assert queries.SymbolStreamQuery is _query.SymbolStreamQuery
    assert queries.HistoryQuery is _query.HistoryQuery
    assert queries.StreamHistoryQuery is _query.StreamHistoryQuery
    assert queries.Symbols is _types.Symbols
    assert queries.DateLike is _types.DateLike
    assert queries.FilterValue is _types.FilterValue
    assert queries.AllSymbols is _types.AllSymbols
    assert queries.StreamBarTimespan is _types.StreamBarTimespan
    assert queries.FuturesBarTimespan is _types.FuturesBarTimespan


def test_all_is_an_instance_of_the_exported_class() -> None:
    assert isinstance(dm.ALL, queries.AllSymbols)


def test_every_dataset_returns_an_exported_query_class() -> None:
    exported = {getattr(queries, name) for name in queries.__all__}
    for spec in CATALOG.by_name.values():
        assert _query.query_class(spec) in exported, spec.name


def test_a_dataset_method_returns_one_of_its_classes(massive_session: MassiveSession) -> None:
    query = massive_session.stocks.trades("AAPL")
    assert isinstance(query, queries.StreamHistoryQuery)
    assert isinstance(query, queries.SymbolStreamQuery)
    assert isinstance(query, queries.StreamQuery)
    assert isinstance(query, queries.HistoryQuery)


def test_a_stream_without_symbols_is_a_plain_stream_query(massive_session: MassiveSession) -> None:
    query = massive_session.options.admin_messages()
    assert type(query) is queries.StreamQuery
    assert not isinstance(query, queries.SymbolStreamQuery)


def test_importing_the_package_does_not_import_it() -> None:
    # A fresh interpreter without a JVM: importing the package does not load the query types.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import json, sys\n"
                "import deephaven_massive\n"
                "print(json.dumps('deephaven_massive.queries' in sys.modules))"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) is False
