"""Tests for `deephaven_massive.namespaces`, the public package of the namespace classes."""

import re
import typing

import pytest

from deephaven_massive import namespaces
from deephaven_massive._session import MassiveSession
from deephaven_massive.namespaces import _partners, _stocks
from deephaven_massive.namespaces._namespace import Namespace

# Each namespace's path from the session, and its class.
_PATHS = {
    "stocks": namespaces.StocksNamespace,
    "stocks.financials": namespaces.StocksFinancialsNamespace,
    "stocks.filings": namespaces.StocksFilingsNamespace,
    "options": namespaces.OptionsNamespace,
    "futures": namespaces.FuturesNamespace,
    "indices": namespaces.IndicesNamespace,
    "forex": namespaces.ForexNamespace,
    "crypto": namespaces.CryptoNamespace,
    "reference": namespaces.ReferenceNamespace,
    "indicators": namespaces.IndicatorsNamespace,
    "economy": namespaces.EconomyNamespace,
    "partners": namespaces.PartnersNamespace,
    "partners.benzinga": namespaces.PartnersBenzingaNamespace,
    "partners.etf_global": namespaces.PartnersEtfGlobalNamespace,
    "partners.tmx": namespaces.PartnersTmxNamespace,
    "partners.fable": namespaces.PartnersFableNamespace,
}


def test_the_package_exports_every_namespace_class() -> None:
    assert sorted(namespaces.__all__) == sorted(cls.__name__ for cls in _PATHS.values())
    assert namespaces.StocksNamespace is _stocks.StocksNamespace
    assert namespaces.PartnersBenzingaNamespace is _partners.PartnersBenzingaNamespace


@pytest.mark.parametrize("path", sorted(_PATHS))
def test_each_class_is_named_for_its_path(path: str) -> None:
    words = (word.capitalize() for part in path.split(".") for word in part.split("_"))
    assert _PATHS[path].__name__ == "".join(words) + "Namespace"


@pytest.mark.parametrize("path", sorted(_PATHS))
def test_each_class_docstring_gives_its_import(path: str) -> None:
    name = _PATHS[path].__name__
    text = _PATHS[path].__doc__ or ""
    assert re.search(rf"from deephaven_massive\.namespaces import {name}\b", text)


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
