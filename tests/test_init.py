"""Tests for the public surface of `deephaven_massive`."""

import importlib.metadata

import deephaven_massive as dm


def test_version_is_the_installed_distribution_version() -> None:
    assert dm.__version__ == importlib.metadata.version("deephaven-massive")
