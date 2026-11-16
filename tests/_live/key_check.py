"""The key a live test may use: never under CI, and only when one is set.

Every live test reaches Massive with the key in `MASSIVE_API_KEY`, and never under CI (`CI` set
to anything but blank): a key added to CI's secrets later still never reaches Massive from CI.
`live_key` makes that check, skipping the test, and the autouse key check of
`tests/conftest.py` calls it first, before any fixture of a live test reaches Massive: every
live fixture is function-scoped, so none is set up before that check.

It imports neither the library nor Deephaven, so a pytest run can load it with no JVM.
"""

from __future__ import annotations

import os

import pytest


def live_key() -> str:
    """Return the API key a live test may use, or skip the test when it may reach no server.

    Under CI (`CI` set to anything but blank) live tests never run, so a key added to CI's
    secrets later still never reaches Massive from CI, and without `MASSIVE_API_KEY` they cannot.

    Returns:
        The key, stripped of surrounding whitespace.
    """
    if os.environ.get("CI", "").strip():
        pytest.skip("live tests never run in CI")
    return required_env("MASSIVE_API_KEY")


def required_env(name: str) -> str:
    """Return an environment variable's value, or skip the test when it is unset or blank.

    Args:
        name: The variable, such as `"MASSIVE_API_KEY"`.

    Returns:
        The value, stripped of surrounding whitespace.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.skip(f"{name} is not set")
    return value
