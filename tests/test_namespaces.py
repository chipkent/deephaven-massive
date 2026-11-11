"""Tests for `deephaven_massive.namespaces`, the public module of namespace classes."""

import json
import subprocess
import sys
import typing

import pytest

from deephaven_massive import namespaces
from deephaven_massive._namespaces import partners, stocks
from deephaven_massive._namespaces.query_building import Namespace
from deephaven_massive._session import MassiveSession

# Each namespace's path from the session, and its class.
_PATHS = {
    "stocks": namespaces.Stocks,
    "stocks.financials": namespaces.StockFinancials,
    "stocks.filings": namespaces.StockFilings,
    "options": namespaces.Options,
    "futures": namespaces.Futures,
    "indices": namespaces.Indices,
    "forex": namespaces.Forex,
    "crypto": namespaces.Crypto,
    "reference": namespaces.Reference,
    "indicators": namespaces.Indicators,
    "economy": namespaces.Economy,
    "partners": namespaces.Partners,
    "partners.benzinga": namespaces.Benzinga,
    "partners.etf_global": namespaces.EtfGlobal,
    "partners.tmx": namespaces.Tmx,
    "partners.fable": namespaces.Fable,
}


def test_the_module_exports_every_namespace_class() -> None:
    assert sorted(namespaces.__all__) == sorted(cls.__name__ for cls in _PATHS.values())
    assert namespaces.Stocks is stocks.Stocks
    assert namespaces.Benzinga is partners.Benzinga


@pytest.mark.parametrize("path", sorted(_PATHS))
def test_each_property_returns_and_is_annotated_with_its_public_class(
    massive_session: MassiveSession, path: str
) -> None:
    *parents, name = path.split(".")
    owner: object = massive_session
    for parent in parents:
        owner = getattr(owner, parent)
    accessor = getattr(type(owner), name)
    assert isinstance(accessor, property)
    assert accessor.fget is not None
    assert typing.get_type_hints(accessor.fget)["return"] is _PATHS[path]
    assert type(getattr(owner, name)) is _PATHS[path]


def test_every_namespace_the_session_reaches_is_listed(massive_session: MassiveSession) -> None:
    reached: dict[str, type] = {}

    def collect(owner: object, prefix: str) -> None:
        for name, member in vars(type(owner)).items():
            if isinstance(member, property):
                value = getattr(owner, name)
                if isinstance(value, Namespace):
                    reached[prefix + name] = type(value)
                    collect(value, f"{prefix}{name}.")

    collect(massive_session, "")
    assert reached == _PATHS


def test_importing_the_package_does_not_import_it() -> None:
    # A fresh interpreter without a JVM: importing the package does not load the namespaces.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import json, sys\n"
                "import deephaven_massive\n"
                "print(json.dumps('deephaven_massive.namespaces' in sys.modules))"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) is False
