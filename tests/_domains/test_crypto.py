"""Tests for the `m.crypto` namespace."""

import deephaven_massive as dm
from deephaven_massive._domains import crypto
from deephaven_massive._domains.base import Namespace


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.crypto
    assert type(namespace) is crypto._Crypto
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert crypto.DATASETS == ()
