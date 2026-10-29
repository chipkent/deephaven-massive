"""Live checks of Massive's REST rate limit: the refusal's text and which requests it counts.

Massive limits each class on its free Basic plan to 5 REST requests per minute and answers
HTTP 429 beyond that. The test here deliberately draws refusals from a class the key holds on
Basic (Indices is preferred, since a run needs it least), so it needs at least one class left
on Basic, and the run puts it first. It runs with no other client of the key.

The test first lets a minute pass, so earlier requests no longer count, then sends single raw
requests (one attempt each, no retry) and records each answer's status, its rate-limit
headers, and its message. Which requests count against which limit is recorded, never
asserted: the library's own rules for data outside every subscription may change while the
evidence stays the same. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING

import pytest
import urllib3

from deephaven_massive._massive import Transport, source
from deephaven_massive._massive.net.http import TlsConsumer, ssl_context
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
    pool = urllib3.PoolManager(ssl_context=ssl_context(TlsConsumer.URLLIB3), retries=False)
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


@pytest.mark.needs("some:basic")
@pytest.mark.settles("65", "66", "67")
@pytest.mark.timeout(330)
def test_a_spent_basic_minute_its_refusal_and_when_it_frees(
    send: Callable[[Request], tuple[int, str]],
    *,
    basic_class: str,
    by_class: Mapping[str, Request],
    evidence: Evidence,
) -> None:
    """The text of a Basic refusal, what it counts, and when a spent minute frees its requests.

    One sequence after a single pause of 61 s, so earlier requests have left Massive's window:

    1. Sends the Basic class's own requests one after another until the first HTTP 429, and
       records each answer's status, rate-limit headers, and time since the first request,
       and the refusal's full text: the message the rate governor's match must contain, and
       whether its headers say when to retry.
    2. Sends ten requests of data outside every subscription (market status, ticker types,
       treasury yields), and records each answer: answered ones say that data is not charged
       to the spent class, and a refusal after five that it has a Basic limit of its own.
    3. Sends the class's request every 5 s until one is answered, for at most 150 s from the
       first request, and records when: about 60 s after the first request means a window
       that slides from each request, one at a minute boundary a fixed window, and one later
       than 60 s after the last answered request means refused requests count against it.

    The evidence answers each question; the test asserts only that the class was refused
    within its first seven requests, which a class on Basic always is. A failure there means
    the class is not on Basic, and the rest of the evidence would say nothing.
    """
    name, request = basic_class, by_class[basic_class]
    threading.Event().wait(_MINUTE)
    start = time.monotonic()
    refused = False
    for number in range(1, _LIMIT + 3):
        status, described = send(request)
        evidence(f"{name} request {number} at {time.monotonic() - start:.1f} s: HTTP {status}")
        if status == 429:
            evidence(f"the first refusal: {described}")
            refused = True
            break
        evidence(f"    {described}")
    assert refused, f"no {name} request was refused: is {name} on Basic?"
    for number in range(1, 2 * _LIMIT + 1):
        unassigned = _UNASSIGNED[number % len(_UNASSIGNED)]
        status, described = send(unassigned)
        evidence(
            f"unassigned request {number} of {unassigned[0]} at {time.monotonic() - start:.1f} s:"
            f" HTTP {status}; {described}"
        )
    while time.monotonic() - start < _PROBE_FOR:
        threading.Event().wait(_PROBE_EVERY)
        status, described = send(request)
        evidence(f"probe at {time.monotonic() - start:.1f} s: HTTP {status}; {described}")
        if status != 429:
            break
    else:
        evidence(f"still refused {_PROBE_FOR:.0f} s after the first request")


@pytest.fixture
def by_class(index_symbol: str) -> dict[str, Request]:
    """Return a cheap request of each subscription's own data.

    Args:
        index_symbol: `I:SPX`, or `I:NDX` when Indices is on Basic, which excludes SPX: a
            refused index would answer 403, never the 429 the test waits for.

    Returns:
        The request, by class.
    """
    return {
        "stocks": ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": "AAPL"}, {}),
        "options": (
            "/v3/reference/options/contracts",
            {},
            {"underlying_ticker": "AAPL", "limit": 1},
        ),
        "indices": ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": index_symbol}, {}),
        "currencies": ("/v2/aggs/ticker/{forexTicker}/prev", {"forexTicker": "C:EURUSD"}, {}),
        "futures": ("/futures/v1/products", {}, {"limit": 1}),
    }
