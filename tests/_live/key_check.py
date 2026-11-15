"""The key a live test may use, and the run's plans, learned only once that key may be used.

Every live test reaches Massive with the key in `MASSIVE_API_KEY`, and never under CI (`CI` set
to anything but blank): a key added to CI's secrets later still never reaches Massive from CI.
`live_key` makes that check, skipping the test, and every fixture that sets up a live test calls
it before anything reaches Massive.

The run's plans are the session fixture `live_plans`, which the first live test that needs them
sets up before any of its function-scoped fixtures, the autouse key check of `tests/conftest.py`
included. So the fixture makes the same check first itself, and only then asks the run's source
of plans (`PLANS_SOURCE`), which `tests/conftest.py` sets: the declared plans, the cached ones,
or a probe of Massive.

The module is also the pytest plugin that holds the fixture: `tests/conftest.py` registers it.
It imports neither the library nor Deephaven, so a pytest run can load it with `-p` and no JVM.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Final

import pytest

from tests._live.plans import LivePlans

PLUGIN_NAME: Final = "live-key-check"
"""The name `tests/conftest.py` registers this module under as a pytest plugin."""

PLANS_SOURCE: Final = pytest.StashKey[Callable[[pytest.Config, str], LivePlans]]()
"""The config stash key of the function that learns the run's plans for a key, given the pytest
configuration and the key. `tests/conftest.py` sets it; `run_plans` calls it only past the key
check."""


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


def run_plans(pytestconfig: pytest.Config) -> LivePlans:
    """Return the plans of the run's key, after the check that the key may be used.

    This is the session fixture `live_plans`. Only the live tests request it, so a unit run
    never learns the plans. Being session-scoped, it is set up before any function-scoped
    fixture of the first live test that needs it, the autouse key check included, so it makes
    the same check (`live_key`) first itself: under CI, or without `MASSIVE_API_KEY`, the test
    skips before the run's source of plans (`PLANS_SOURCE`) is asked, and so before anything
    reaches Massive.

    Args:
        pytestconfig: The pytest configuration, whose stash holds the source of plans.

    Returns:
        The plans the source gives for the key.
    """
    return pytestconfig.stash[PLANS_SOURCE](pytestconfig, live_key())


live_plans = pytest.fixture(scope="session", name="live_plans")(run_plans)
"""The run's plans, as a session fixture (`run_plans`)."""
