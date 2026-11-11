"""Tests for `Namespace`, the base class of the namespace classes, and `shows_call`."""

import inspect
from typing import TYPE_CHECKING, cast

import numpy as np
import pytest

import deephaven_massive as dm
from deephaven_massive._enums import FuturesVenue
from deephaven_massive.namespaces._namespace import Namespace, _is_default

if TYPE_CHECKING:
    from collections.abc import Callable


def test_namespaces_hold_the_core(massive_session: dm.MassiveSession) -> None:
    namespace = Namespace(massive_session._core)
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


def test_a_wrong_call_names_the_method_by_its_path(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    bars = cast("Callable[..., object]", m.stocks.bars)  # a call no overload accepts
    with pytest.raises(
        TypeError, match=r"^stocks\.bars\(\) takes 3 positional arguments but 5 were"
    ):
        bars("AAPL", dm.Timespan.MINUTE, 1, "adjusted")
    news = cast("Callable[..., object]", m.partners.benzinga.news)
    with pytest.raises(TypeError, match=r"^partners\.benzinga\.news\(\) got an unexpected"):
        news(tickerz="AAPL")
    assert m.options.chain.__qualname__ == "options.chain"
    assert m.stocks.financials.balance_sheets.__qualname__ == "stocks.financials.balance_sheets"


def test_an_array_argument_is_never_its_default() -> None:
    # A numpy array's `==` answers element by element; the repr must not ask its truth.
    assert not _is_default(np.array([1, 1]), 1)
    assert _is_default(1, 1)
    assert _is_default({FuturesVenue.CME}, frozenset({FuturesVenue.CME}))
    assert not _is_default(None, inspect.Parameter.empty)
    assert not _is_default(2, None)
