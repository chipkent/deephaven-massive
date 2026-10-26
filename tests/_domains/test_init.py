"""Tests for the `_domains` package itself, which is deliberately empty."""

from pathlib import Path

from deephaven_massive import _domains


def test_package_init_is_empty() -> None:
    assert Path(_domains.__file__).read_text() == ""
