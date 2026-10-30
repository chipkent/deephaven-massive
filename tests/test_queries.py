"""Tests for `deephaven_massive.queries`, the public module of query types."""

import json
import subprocess
import sys

from deephaven_massive import _query, _types, queries
from deephaven_massive._session import MassiveSession


def test_the_module_exports_the_query_classes_and_argument_types() -> None:
    assert sorted(queries.__all__) == [
        "DateLike",
        "FilterValue",
        "HistoryQuery",
        "Query",
        "StreamHistoryQuery",
        "StreamQuery",
        "Symbols",
    ]
    assert queries.Query is _query.Query
    assert queries.StreamQuery is _query.StreamQuery
    assert queries.HistoryQuery is _query.HistoryQuery
    assert queries.StreamHistoryQuery is _query.StreamHistoryQuery
    assert queries.Symbols is _types.Symbols
    assert queries.DateLike is _types.DateLike
    assert queries.FilterValue is _types.FilterValue


def test_a_dataset_method_returns_one_of_its_classes(massive_session: MassiveSession) -> None:
    query = massive_session.stocks.trades("AAPL")
    assert isinstance(query, queries.StreamHistoryQuery)
    assert isinstance(query, queries.StreamQuery)
    assert isinstance(query, queries.HistoryQuery)


def test_importing_the_package_does_not_import_it() -> None:
    # A fresh interpreter without a JVM: the package and every public name it resolves lazily
    # never load the query types.
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
