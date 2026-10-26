"""Tests for the `_namespaces` package itself, which is deliberately empty."""

from pathlib import Path

from deephaven_massive import _namespaces


def test_package_init_is_empty() -> None:
    assert Path(_namespaces.__file__).read_text() == ""
