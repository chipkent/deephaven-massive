"""Tests for `Namespace`, the base class of the namespace classes."""

import inspect
import typing

import pytest

import deephaven_massive as dm
from deephaven_massive.namespaces import StocksNamespace
from deephaven_massive.namespaces._namespace import Namespace


def test_namespaces_hold_the_core(massive_session: dm.MassiveSession) -> None:
    namespace = Namespace(massive_session)
    assert namespace._core is massive_session._core
    assert not hasattr(namespace, "_query")


def test_namespaces_repr_as_their_path(massive_session: dm.MassiveSession) -> None:
    label = repr(massive_session).removeprefix("<").split(" open")[0]
    m = massive_session
    namespaces = {
        "stocks": m.stocks,
        "options": m.options,
        "futures": m.futures,
        "indices": m.indices,
        "forex": m.forex,
        "crypto": m.crypto,
        "reference": m.reference,
        "indicators": m.indicators,
        "economy": m.economy,
        "partners": m.partners,
        "partners.benzinga": m.partners.benzinga,
        "partners.etf_global": m.partners.etf_global,
        "partners.tmx": m.partners.tmx,
        "partners.fable": m.partners.fable,
    }
    for path, namespace in namespaces.items():
        assert repr(namespace) == f"<{label} {path}>"


def test_a_dataset_method_keeps_its_class_name(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    bars = m.stocks.bars
    with pytest.raises(
        TypeError, match=r"^StocksNamespace\.bars\(\) takes 3 positional arguments but 5 were"
    ):
        bars("AAPL", dm.Timespan.MINUTE, 1, "x")  # type: ignore[call-arg, arg-type]  # the point
    assert m.options.chain.__qualname__ == "OptionsNamespace.chain"
    assert typing.get_overloads(inspect.unwrap(StocksNamespace.bars)) == []
