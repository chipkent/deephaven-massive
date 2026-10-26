"""Tests for the `m.partners` namespace and its four vendor namespaces."""

import deephaven_massive as dm
from deephaven_massive._namespaces import partners
from deephaven_massive._namespaces.base import Namespace


def test_vendors_are_nested(massive_session: dm.MassiveSession) -> None:
    vendors = massive_session.partners
    assert type(vendors) is partners._Partners
    assert type(vendors.benzinga) is partners._Benzinga
    assert type(vendors.etf_global) is partners._EtfGlobal
    assert type(vendors.tmx) is partners._Tmx
    assert type(vendors.fable) is partners._Fable
    for vendor in (vendors.benzinga, vendors.etf_global, vendors.tmx, vendors.fable):
        assert isinstance(vendor, Namespace)
        assert vendor._core is massive_session._core


def test_datasets_are_declared_by_the_partner_package() -> None:
    assert partners.DATASETS == ()
