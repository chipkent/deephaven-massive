"""Tests for the `m.futures` namespace."""

import deephaven_massive as dm
from deephaven_massive._namespaces import futures
from deephaven_massive._namespaces.base import Namespace


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.futures
    assert type(namespace) is futures._Futures
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert futures.DATASETS == ()
