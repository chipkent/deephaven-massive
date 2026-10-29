"""Live checks of Massive's REST rate limit: the refusal's text and which requests it counts.

Massive limits each class on its free Basic plan to 5 REST requests per minute and answers
HTTP 429 beyond that. These tests deliberately draw a few refusals from a class the run
declares Basic (`MASSIVE_LIVE_PLANS`; Indices is preferred, since a run needs it least), so
they need at least one class left on Basic, and the run puts them first, while that class's
minute is unspent. They run with no other client of the key.

Each test first lets a minute pass, so earlier requests no longer count, then sends single raw
requests (one attempt each, no retry) and records each answer's status, its rate-limit
headers, and its message. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING

import pytest
import urllib3

from deephaven_massive._massive import Transport, source
from deephaven_massive._massive.net.http import ssl_context
from deephaven_massive._massive.rest.query import request_target

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"

_LIMIT = 5
"""Requests per minute Massive allows a Basic subscription."""

_MINUTE = 61.0
"""Seconds a test lets pass first, so earlier requests have left the limit's window."""

_BY_CLASS: dict[str, Request] = {
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

_PROBE_EVERY = 5.0
"""Seconds between the probes that look for the end of a refusal."""

_PROBE_FOR = 150.0
"""Seconds after the first request when the probes stop, refused or not."""

_RATE_HEADERS = ("retry-after", "ratelimit", "rate-limit")
"""Substrings of the response headers recorded with each answer."""


@pytest.fixture
def send(live_config: MassiveConfig) -> Iterator[Callable[[Request], tuple[int, str]]]:
    """Yield a function that sends one request once, without retrying, and describes the answer.

    Yields:
        A function mapping a request to its HTTP status and a description: the rate-limit
        headers and, for a refusal, Massive's message (or the start of the body).
    """
    pool = urllib3.PoolManager(ssl_context=ssl_context(), retries=False)
    headers = {"Authorization": f"Bearer {live_config.api_key}"}

    def run(request: Request) -> tuple[int, str]:
        path, path_params, query = request
        target = request_target(source(Transport.REST, path), path_params, query)
        response = pool.request("GET", live_config.rest_base_url + target, headers=headers)
        limits = {
            name: value
            for name, value in response.headers.items()
            if any(part in name.lower() for part in _RATE_HEADERS)
        }
        if response.status == 200:
            return 200, f"headers {limits}"
        try:
            body = json.loads(response.data)
            message = body.get("message") or body.get("error") if isinstance(body, dict) else body
        except ValueError:
            message = response.data[:200].decode("utf-8", "replace")
        return response.status, f"headers {limits}; message {message!r}"

    yield run
    pool.clear()


def _pause() -> None:
    """Let the rate limit's window of earlier requests pass."""
    threading.Event().wait(_MINUTE)


@pytest.mark.needs("some:basic")
@pytest.mark.settles("65")
@pytest.mark.timeout(100)
def test_text_of_the_rate_limit_refusal(
    send: Callable[[Request], tuple[int, str]],
    *,
    basic_class: str,
    evidence: Evidence,
) -> None:
    """The exact status, headers, and message of a Basic subscription's rate-limit refusal.

    Sends seven requests of the named subscription's data within a minute and records each
    answer. Reading it: the 429's message is the text that marks a Basic refusal, so the rate
    governor's match must contain it; its headers show whether Massive says when to retry.
    Fails when no request is refused: the class is then not on Basic, as declared.
    """
    name, request = basic_class, _BY_CLASS[basic_class]
    _pause()
    statuses = []
    for number in range(1, _LIMIT + 3):
        status, described = send(request)
        statuses.append(status)
        evidence(f"{name} request {number} of {request[0]}: HTTP {status}; {described}")
    assert 429 in statuses, f"no request was refused: is {name} on Basic, as declared?"


@pytest.mark.needs("some:basic")
@pytest.mark.settles("66")
@pytest.mark.timeout(100)
def test_requests_without_a_subscription_after_the_limit_is_reached(
    send: Callable[[Request], tuple[int, str]],
    *,
    basic_class: str,
    evidence: Evidence,
) -> None:
    """Whether requests of data outside every subscription count against a Basic limit.

    Spends the named Basic subscription's minute (until its first 429), then sends one request
    each of market status, ticker types, and treasury yields. Reading it: all three answered
    means data outside the subscriptions is not charged to the spent subscription; refusals
    mean it is, and the rate governor must charge it there. Fails when the limit is never
    reached: the class is then not on Basic, as declared, and the later answers say nothing.
    """
    name, request = basic_class, _BY_CLASS[basic_class]
    _pause()
    refused = False
    for number in range(1, _LIMIT + 3):
        status, described = send(request)
        evidence(f"{name} request {number}: HTTP {status}; {described}")
        if status == 429:
            refused = True
            break
    if not refused:
        pytest.fail(f"the {name} limit was not reached, although {name} is declared Basic")
    for unassigned in _UNASSIGNED:
        status, described = send(unassigned)
        evidence(f"then {unassigned[0]}: HTTP {status}; {described}")


@pytest.mark.needs("some:basic")
@pytest.mark.settles("66")
@pytest.mark.timeout(100)
def test_requests_without_a_subscription_alone(
    send: Callable[[Request], tuple[int, str]],
    *,
    basic_class: str,
    evidence: Evidence,
) -> None:
    """Whether data outside every subscription has a per-minute limit of its own.

    Sends ten requests of market status, ticker types, and treasury yields within a minute,
    with no subscription's data in between. Reading it: all answered means such data is not
    rate-limited; a refusal after five means it has a Basic limit of its own, which the rate
    governor must pace as a bucket of its own.
    """
    _pause()
    for number in range(1, 2 * _LIMIT + 1):
        unassigned = _UNASSIGNED[number % len(_UNASSIGNED)]
        status, described = send(unassigned)
        evidence(f"request {number} of {unassigned[0]}: HTTP {status}; {described}")


@pytest.mark.needs("some:basic")
@pytest.mark.settles("67")
@pytest.mark.timeout(260)
def test_when_a_spent_minute_frees_its_requests(
    send: Callable[[Request], tuple[int, str]],
    *,
    basic_class: str,
    evidence: Evidence,
) -> None:
    """Whether Massive's minute slides from each request, and whether refusals count against it.

    Sends the named Basic subscription's requests one after another until the first 429, then
    one request every 5 s until one is answered (for at most 150 s from the first request),
    recording each answer's time since the first request. Reading it: the first answer about
    60 s after the first request means a window that slides from each request; one at a
    minute boundary means a fixed window; one later than 60 s after the last answered request
    means the refused probes counted against the limit. Fails when no request is refused: the
    class is then not on Basic, as declared.
    """
    name, request = basic_class, _BY_CLASS[basic_class]
    _pause()
    start = time.monotonic()
    refused = False
    for number in range(1, _LIMIT + 3):
        status, described = send(request)
        evidence(f"{name} request {number} at {time.monotonic() - start:.1f} s: HTTP {status}")
        if status == 429:
            evidence(f"the first refusal: {described}")
            refused = True
            break
    if not refused:
        pytest.fail(f"the {name} limit was not reached, although {name} is declared Basic")
    while time.monotonic() - start < _PROBE_FOR:
        threading.Event().wait(_PROBE_EVERY)
        status, described = send(request)
        evidence(f"probe at {time.monotonic() - start:.1f} s: HTTP {status}; {described}")
        if status != 429:
            break
    else:
        evidence(f"still refused {_PROBE_FOR:.0f} s after the first request")
