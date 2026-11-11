"""Tests for `_override`, the `override` decorator on every supported Python."""

from deephaven_massive._override import override


def test_override_returns_the_method_unchanged() -> None:
    def method() -> int:
        return 1

    assert override(method) is method
