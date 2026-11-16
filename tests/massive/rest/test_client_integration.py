"""Live checks of Massive's REST rate limit: the refusal's text and which requests it counts.

Massive may limit a subscription to 5 REST requests per minute, and answers HTTP 429 beyond
that. The tests here deliberately draw refusals from the subscription the owner's settings file
says Massive limits (`limited_subscription`: indices preferred, since a run needs it least),
and skip when the file lists none. They are marked `integration_rate_limits`, so
`-m integration` leaves them out and `-m integration_rate_limits` runs them, with no other
client of the key.

Each test first lets a minute pass, so earlier requests no longer count, then sends single raw
requests (one attempt each, no retry, outside the run's rate governor) and records each
answer's status, its rate-limit headers, and its message. Which requests count against which
limit is recorded, never asserted: the library's own rules for data outside every
subscription may change while the evidence stays the same. One more test, outside the rate
limits, records what a later page's `next_url` answers at once and after a wait. How to run
these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import pytest
import urllib3

import deephaven_massive as dm
from deephaven_massive._massive import Transport, source
from deephaven_massive._massive.net.http import new_ssl_context
from deephaven_massive._massive.rest.query import request_target
from tests._live import classes, summary

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"

_LIMIT = 5
"""Requests per minute Massive allows a subscription it limits."""

_MINUTE = 61.0
"""Seconds a test lets pass first, so earlier requests have left the limit's window."""

_OPTION = "O:SPY271217C00600000"
"""The options request's contract, a long-dated SPY call: options data, charged to the options
subscription, unlike the options contracts list, which is reference data of none. Were it
unlisted, the empty answer would still count against the class's minute."""

_UNASSIGNED: tuple[Request, ...] = (
    ("/v1/marketstatus/now", {}, {}),
    ("/v3/reference/tickers/types", {}, {}),
    ("/fed/v1/treasury-yields", {}, {"limit": 1}),
    ("/v3/reference/tickers", {}, {"limit": 1}),
    ("/benzinga/v1/firms", {}, {"limit": 1}),
    ("/v3/snapshot", {}, {"ticker.any_of": ["AAPL", "C:EURUSD"]}),
)
"""Requests of data that belongs to no subscription: market status, reference (ticker types,
and a ticker lookup across markets), economy, a partner's data, and a unified snapshot of two
classes (a stock and a forex pair)."""

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
    pool = urllib3.PoolManager(ssl_context=new_ssl_context(), retries=False)
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


_ANSWERS = (200, 404, 429)
"""The statuses a limited subscription's request counts by: answered (the options request's
expired contract answers 404, which counts against the minute the same) or refused for the
minute."""


def _require_answer(status: int, described: str, what: str) -> None:
    """Skip on Massive's refusal of the key, and fail on any status but an answer or a 429.

    A 403 or a 500 is no evidence that Massive did not limit the subscription.

    Args:
        status: The response's HTTP status.
        described: Its rate-limit headers and message.
        what: The request, for the reason.
    """
    if status == 403:
        pytest.skip(f"{summary.REFUSED}HTTP 403 for {what}: {described}")
    assert status in _ANSWERS, f"{what}: HTTP {status}; {described}"


@pytest.mark.integration_rate_limits
@pytest.mark.settles("65", "67")
@pytest.mark.verifies("66")
@pytest.mark.timeout(330)
def test_a_spent_minute_its_refusal_and_when_it_frees(
    send: Callable[[Request], tuple[int, str]],
    *,
    limited_subscription: dm.Subscription,
    by_class: Mapping[str, Request],
    evidence: Evidence,
) -> None:
    """The text of a rate-limit refusal, what it counts, and when a spent minute frees its requests.

    One sequence after a single pause of 61 s, so earlier requests have left Massive's window:

    1. Sends the limited subscription's own requests one after another until the first HTTP
       429, and records each answer's status, rate-limit headers, and time since the first
       request, and the refusal's full text: the message the rate governor's match must
       contain, and whether its headers say when to retry.
    2. Sends ten requests of data outside every subscription, in turn: market status, ticker
       types, treasury yields, a ticker lookup across markets, a partner's page (Benzinga
       firms), and a unified snapshot of a stock and a forex pair. Records each answer:
       answered ones say that data is not charged to the spent subscription, and a refusal
       after five that it has a limit of its own. A 403 (Massive refusing the key a partner's
       data or a snapshot) says only that; whether Massive counted it shows in the answers
       after it.
    3. Sends the class's request every 5 s until one is answered, for at most 150 s from the
       first request, and records when: about 60 s after the first request means a window
       that slides from each request, one at a minute boundary a fixed window, and one later
       than 60 s after the last answered request means refused requests count against it.

    The evidence answers each question. A 403 for the subscription's request skips the test,
    quoting Massive, and another status that is neither an answer nor a 429 fails it. The test
    skips when Massive answers the first seven requests of the subscription: the owner's
    settings say Massive limits it and Massive's answer disagrees, and the rest of the evidence
    would say nothing. It asserts that none of the first five requests of data outside every
    subscription was refused with HTTP 429: the library charges that data to a bucket of its
    own, never to the spent subscription.
    """
    name = limited_subscription.value
    request = by_class[name]
    threading.Event().wait(_MINUTE)
    start = time.monotonic()
    refused = False
    for number in range(1, _LIMIT + 3):
        status, described = send(request)
        evidence(f"{name} request {number} at {time.monotonic() - start:.1f} s: HTTP {status}")
        _require_answer(status, described, f"{name} request {number}")
        if status == 429:
            evidence(f"the first refusal: {described}")
            refused = True
            break
        evidence(f"    {described}")
    if not refused:
        pytest.skip(
            f"Massive answered {_LIMIT + 2} requests of {name} in a minute though "
            "rest_requests_per_minute lists it"
        )
    unassigned_statuses: list[int] = []
    for number in range(1, 2 * _LIMIT + 1):
        unassigned = _UNASSIGNED[number % len(_UNASSIGNED)]
        status, described = send(unassigned)
        unassigned_statuses.append(status)
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
    assert 429 not in unassigned_statuses[:_LIMIT], (
        f"data outside every subscription was refused with the {name} minute spent: "
        f"{unassigned_statuses}"
    )


@pytest.mark.integration_rate_limits
@pytest.mark.verifies("66")
@pytest.mark.requests(8)
@pytest.mark.timeout(150)
def test_whether_a_ticker_lookup_for_one_market_is_charged_to_its_class(
    send: Callable[[Request], tuple[int, str]],
    *,
    live_config: MassiveConfig,
    by_class: Mapping[str, Request],
    evidence: Evidence,
) -> None:
    """Whether Massive charges a ticker lookup filtered by a class's market to that class.

    The library charges every reference read to the `other` bucket, a lookup filtered by
    market (`/v3/reference/tickers?market=fx`) and the session's crypto ticker list included,
    as Massive's knowledge base says reference endpoints are counted on their own. One
    sequence after a single pause of 61 s, so earlier requests have left Massive's window,
    on the first subscription the owner's settings file says Massive limits
    (`rest_requests_per_minute`) whose tickers a market lists (indices, else currencies, else
    stocks: `classes.TICKER_MARKETS`); it skips when the file lists none of them:

    1. Sends the class's own request five times, which spends its minute; Massive answers each.
    2. Looks up one ticker in each of the class's markets (`indices`; `fx` and `crypto`;
       `stocks`), then in a market of another class, the control, which a spent class never
       refuses.
    3. Sends the class's request once more, which Massive refuses when it limits the class.

    Records each answer's status, rate-limit headers, and time. The test asserts that the
    first five requests were answered (otherwise another client of the key used the class),
    and skips when Massive answers the last, since the lookups' answers then say nothing:
    the owner's settings say Massive limits the class and Massive's answer disagrees. A 403 for
    the class's request skips the test, quoting Massive, and another status that is neither an
    answer nor a 429 fails it. Passes
    when, besides, no lookup of the class's own market was refused with HTTP 429: Massive
    counts it elsewhere, as the library assumes. A refused lookup means Massive charges it to
    the class, so the library must charge market-filtered lookups, and the crypto ticker list,
    to the class rather than to `other`.
    """
    names = list(classes.TICKER_MARKETS)
    listed = [n for n in names if dm.Subscription(n) in live_config.rest_requests_per_minute]
    if not listed:
        pytest.skip(
            "list indices, currencies, or stocks in rest_requests_per_minute of your settings "
            "file if Massive limits it; this test spends its minute"
        )
    name, request = listed[0], by_class[listed[0]]
    markets = classes.TICKER_MARKETS[name]
    other_market = classes.TICKER_MARKETS[names[(names.index(name) + 1) % len(names)]][0]
    threading.Event().wait(_MINUTE)
    start = time.monotonic()
    spent = []
    charged: list[str] = []
    for number in range(1, _LIMIT + 1):
        status, described = send(request)
        spent.append(status)
        evidence(f"{name} request {number} at {time.monotonic() - start:.1f} s: HTTP {status}")
        evidence(f"    {described}")
        _require_answer(status, described, f"{name} request {number}")
    for market in (*markets, other_market):
        status, described = send(("/v3/reference/tickers", {}, {"market": market, "limit": 1}))
        whose = "another class's, the control" if market == other_market else name
        if status == 429 and market != other_market:
            charged.append(market)
        evidence(
            f"ticker lookup market={market} ({whose}) at {time.monotonic() - start:.1f} s: "
            f"HTTP {status}; {described}"
        )
    status, described = send(request)
    evidence(
        f"{name} request after the lookups at {time.monotonic() - start:.1f} s: HTTP {status}; "
        f"{described}"
    )
    _require_answer(status, described, f"{name} request after the lookups")
    assert 429 not in spent, f"{name} refused one of its first five requests: another client?"
    if status != 429:
        pytest.skip(
            f"Massive answered 6 requests of {name} in a minute though "
            "rest_requests_per_minute lists it"
        )
    assert not charged, f"Massive charged the lookups of markets {charged} to {name}"


@pytest.fixture
def by_class() -> dict[str, Request]:
    """Return a cheap request of each subscription's own data.

    The index request reads `I:NDX`, an index Massive serves widely, since a refused index
    would answer 403, never the 429 the tests wait for. These requests are sent only to a
    subscription the owner's settings file says Massive limits.

    Returns:
        The request, by class.
    """
    return {
        "stocks": ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": "AAPL"}, {}),
        "options": ("/v2/aggs/ticker/{optionsTicker}/prev", {"optionsTicker": _OPTION}, {}),
        "indices": ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": "I:NDX"}, {}),
        "currencies": ("/v2/aggs/ticker/{forexTicker}/prev", {"forexTicker": "C:EURUSD"}, {}),
        "futures": ("/futures/v1/products", {}, {"limit": 1}),
    }


_CURSOR_WAIT = 90.0
"""Seconds the cursor test waits before it asks for the same later page a second time."""


@pytest.mark.settles("117")
@pytest.mark.requests(3)
@pytest.mark.timeout(200)
def test_why_a_later_page_answers_404(live_config: MassiveConfig, evidence: Evidence) -> None:
    """What a later page's `next_url` answers now, and again after a wait: its cursor's lifetime.

    A paged REST read that meets HTTP 404 on a later page raises `CutShortError`, which the
    library calls retryable: a new call starts over with a new cursor. That assumes the 404
    means the cursor expired or its page moved, which nothing has verified. Reads the first
    page of the ticker list (`/v3/reference/tickers`, `limit=5`), then asks for its
    `next_url` at once and again after `_CURSOR_WAIT` seconds, each a single raw request with
    no retry, and records each status and message. Settles nothing by itself: a 404 on the
    later request supports the lifetime explanation; two 200s show the cursor outlives the
    wait, and the 404's cause stays open.
    """
    pool = urllib3.PoolManager(ssl_context=new_ssl_context(), retries=False)
    headers = {"Authorization": f"Bearer {live_config.api_key}"}
    origin = urlsplit(live_config.rest_base_url)[:2]

    def get(url: str) -> tuple[int, dict[str, object]]:
        if urlsplit(url)[:2] != origin:  # the key goes only where the client would send it
            pytest.fail(f"refusing to send the API key to {url!r}: not the REST base URL's origin")
        response = pool.request("GET", url, headers=headers)
        try:
            body = json.loads(response.data)
        except ValueError:
            body = {"body": response.data[:200].decode("utf-8", "replace")}
        return response.status, body if isinstance(body, dict) else {"body": body}

    try:
        status, first = get(f"{live_config.rest_base_url}/v3/reference/tickers?limit=5")
        rows = first.get("results")
        evidence(f"first page: HTTP {status}, {len(rows) if isinstance(rows, list) else 0} rows")
        later = first.get("next_url")
        assert status == 200
        assert isinstance(later, str), "the first page has no next_url to follow"
        status, body = get(later)
        evidence(f"its next_url at once: HTTP {status}, message {body.get('message')!r}")
        time.sleep(_CURSOR_WAIT)
        status, body = get(later)
        evidence(
            f"the same next_url after {_CURSOR_WAIT:.0f} s: HTTP {status}, "
            f"message {body.get('message')!r}"
        )
    finally:
        pool.clear()
