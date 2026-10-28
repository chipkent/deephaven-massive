"""Live checks of Massive's REST rate limit: the refusal's text and which requests it counts.

Massive limits a subscription on its free Basic tier to 5 REST requests per minute and answers
HTTP 429 beyond that. These tests run only with `MASSIVE_LIVE_BASIC_SUBSCRIPTION` naming a
subscription the key's account holds on Basic (`stocks`, `options`, `indices`, `currencies`,
or `futures`), since a paid tier never refuses. Each test first lets a minute pass, so the
requests of earlier tests no longer count, then sends a handful of requests, each through a
fresh wire client whose clock refuses to wait: the first 429 ends that request at once
instead of being retried, and its text is read from the client's log.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

Evidence: TypeAlias = "Callable[[str], None]"
Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"

_LIMIT = 5
"""Requests per minute Massive allows a Basic subscription."""

_MINUTE = 61.0
"""Seconds a test lets pass first, so earlier requests have left the limit's window."""

_BY_SUBSCRIPTION: dict[str, Request] = {
    "stocks": ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": "AAPL"}, {}),
    "options": ("/v3/reference/options/contracts", {}, {"underlying_ticker": "AAPL", "limit": 1}),
    "indices": ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": "I:SPX"}, {}),
    "currencies": ("/v2/aggs/ticker/{forexTicker}/prev", {"forexTicker": "C:EURUSD"}, {}),
    "futures": ("/futures/v1/products", {}, {"limit": 1}),
}
"""A cheap request of each subscription's own data."""

_UNASSIGNED: tuple[Request, ...] = (
    ("/v1/marketstatus/now", {}, {}),
    ("/v3/reference/tickers/types", {}, {}),
    ("/fed/v1/treasury-yields", {}, {"limit": 1}),
)
"""Requests of data that belongs to no subscription: market status, reference, economy."""


class _WaitRefusedError(Exception):
    """Raised by `_NoWaitClock` where the client would wait (a retry or a 429's pause)."""


class _NoWaitClock:
    """The real time, but a clock that never waits: every wait raises `_WaitRefusedError`."""

    def monotonic(self) -> float:
        """Return `time.monotonic()`."""
        return time.monotonic()

    def time(self) -> float:
        """Return `time.time()`."""
        return time.time()

    def wait(self, event: threading.Event, seconds: float) -> bool:
        """Refuse to wait.

        Raises:
            _WaitRefusedError: Always.
        """
        raise _WaitRefusedError(f"the client would wait {seconds:.1f} s")


@pytest.fixture
def send(live_config: MassiveConfig, caplog: pytest.LogCaptureFixture) -> Callable[[Request], str]:
    """Return a function that sends one request without retrying, and describes the answer.

    Returns:
        A function mapping a request to `"200"`, to `"429 <log line>"` (the refusal's message
        as the client logged it), or to the text of another failure.
    """
    caplog.set_level(logging.INFO, logger="deephaven_massive._massive.rest.client")

    def run(request: Request) -> str:
        path, path_params, query = request
        client = RestClient(
            live_config.api_key,
            base_url=live_config.rest_base_url,
            max_concurrency=1,
            clock=_NoWaitClock(),
        )
        caplog.clear()
        try:
            client.get(source(Transport.REST, path), path_params, query)
        except _WaitRefusedError:
            logged = [r.getMessage() for r in caplog.records if "HTTP" in r.getMessage()]
            return f"refused, then {logged}"
        except WireError as error:
            return f"{type(error).__name__}: {error}"
        finally:
            client.close()
        return "200"

    return run


def _pause() -> None:
    """Let the rate limit's window of earlier requests pass."""
    threading.Event().wait(_MINUTE)


def _subscription_request(live_env: Callable[[str], str]) -> tuple[str, Request]:
    """Return the Basic subscription the run names and a request of its data.

    Returns:
        The subscription's name and the request.
    """
    name = live_env("MASSIVE_LIVE_BASIC_SUBSCRIPTION").lower()
    if name not in _BY_SUBSCRIPTION:
        pytest.fail(f"MASSIVE_LIVE_BASIC_SUBSCRIPTION must be one of {sorted(_BY_SUBSCRIPTION)}")
    return name, _BY_SUBSCRIPTION[name]


@pytest.mark.timeout(110)
def test_text_of_the_rate_limit_refusal(
    send: Callable[[Request], str], *, live_env: Callable[[str], str], evidence: Evidence
) -> None:
    name, request = _subscription_request(live_env)
    _pause()
    outcomes = [send(request) for _ in range(_LIMIT + 2)]
    for number, outcome in enumerate(outcomes, start=1):
        evidence(f"{name} request {number} of {request[0]}: {outcome}")
    assert any(outcome != "200" for outcome in outcomes), "no request was refused"


@pytest.mark.timeout(110)
def test_requests_without_a_subscription_after_the_limit_is_reached(
    send: Callable[[Request], str], *, live_env: Callable[[str], str], evidence: Evidence
) -> None:
    name, request = _subscription_request(live_env)
    _pause()
    for number in range(1, _LIMIT + 2):
        evidence(f"{name} request {number}: {send(request)}")
    for unassigned in _UNASSIGNED:
        evidence(f"then {unassigned[0]}: {send(unassigned)}")


@pytest.mark.timeout(110)
def test_requests_without_a_subscription_alone(
    send: Callable[[Request], str], *, live_env: Callable[[str], str], evidence: Evidence
) -> None:
    _subscription_request(live_env)
    _pause()
    for number in range(1, 2 * _LIMIT + 1):
        unassigned = _UNASSIGNED[number % len(_UNASSIGNED)]
        evidence(f"request {number} of {unassigned[0]}: {send(unassigned)}")
