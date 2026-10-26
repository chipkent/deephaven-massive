"""Tests for the `m.forex` namespace."""

import deephaven_massive as dm
from deephaven_massive._domains import forex
from deephaven_massive._domains.base import Namespace


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.forex
    assert type(namespace) is forex._Forex
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert forex.DATASETS == ()
