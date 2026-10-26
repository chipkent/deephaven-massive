"""Tests for the public surface of `deephaven_massive`."""

import importlib.metadata
import types
from enum import StrEnum

import deephaven_massive as dm

EXPECTED_EXPORTS = [
    "MassiveSession",
    "MassiveConfig",
    "MassiveError",
    "AuthenticationError",
    "EntitlementError",
    "ALL",
    "AssetClass",
    "Timespan",
    "PollMode",
    "HistorySource",
    "FuturesVenue",
    "Direction",
    "ContractType",
    "SeriesType",
    "Timeframe",
    "Order",
    "Market",
    "Locale",
    "ConditionDataType",
    "Sip",
    "__version__",
]


def test_version_is_the_installed_distribution_version() -> None:
    assert dm.__version__ == importlib.metadata.version("deephaven-massive")


def test_all_is_exactly_the_public_api() -> None:
    assert len(EXPECTED_EXPORTS) == 21
    assert sorted(dm.__all__) == sorted(EXPECTED_EXPORTS)


def test_every_export_is_defined() -> None:
    for name in dm.__all__:
        assert hasattr(dm, name), name


def test_public_names_come_from_private_modules() -> None:
    public = [getattr(dm, name) for name in dm.__all__ if name not in {"ALL", "__version__"}]
    for obj in public:
        assert obj.__module__.startswith("deephaven_massive._"), obj


def test_fourteen_enums_are_exported() -> None:
    enums = [
        n
        for n in dm.__all__
        if isinstance(getattr(dm, n), type) and issubclass(getattr(dm, n), StrEnum)
    ]
    assert len(enums) == 14


def test_exception_hierarchy() -> None:
    assert issubclass(dm.AuthenticationError, dm.MassiveError)
    assert issubclass(dm.EntitlementError, dm.MassiveError)
    assert issubclass(dm.MassiveError, Exception)


def test_no_public_names_outside_all() -> None:
    public = {
        name
        for name, value in vars(dm).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert public == set(dm.__all__) - {"__version__"}
