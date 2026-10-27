"""Tests for the REST client, against the fake REST server over real sockets.

Time is virtual: every client gets a `FakeClock`, so retry waits are recorded, never slept.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
)
from deephaven_massive._massive.rest import client as client_module
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath, WireUnit
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator

    from deephaven_massive._massive.rest.response import Page

_KEY = "secret-key-123"
_TLS = Path(__file__).resolve().parents[2] / "data" / "massive" / "rest"
_TRADES = RestPath(
    key="/v3/trades/{stockTicker}",
    fields=(),
    params=(
        Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),
        Param(name="limit", kind=ParamKind.INTEGER),
        Param(name="timestamp.gte", kind=ParamKind.TIMESTAMP, unit=WireUnit.NS),
    ),
)
_AAPL = {"stockTicker": "AAPL"}


def _client(
    server: FakeRestServer, clock: FakeClock | None = None, *, max_concurrency: int = 4
) -> RestClient:
    return RestClient(
        _KEY, base_url=server.url, max_concurrency=max_concurrency, clock=clock or FakeClock()
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(fake_rest: FakeRestServer, clock: FakeClock) -> Iterator[RestClient]:
    rest = _client(fake_rest, clock)
    yield rest
    rest.close()


class _Jitter:
    """A stand-in for the client's jitter source that always draws `value`."""

    def __init__(self, value: float) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


@pytest.fixture(autouse=True)
def _half_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Draw jitter 0.5, so each transient wait is exactly 10 % above its base."""
    monkeypatch.setattr(client_module, "_JITTER", _Jitter(0.5))


@pytest.fixture
def no_proxy_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    return monkeypatch


def _ok(*, status: str = "OK", request_id: str = "req-1", next_url: str | None = None) -> Reply:
    return Reply(envelope([{"i": 1}], status=status, request_id=request_id, next_url=next_url))


# Construction and transport.


def test_the_pool_never_retries_and_has_the_timeouts_and_size(client: RestClient) -> None:
    options = client._pool.connection_pool_kw
    assert options["retries"].total is False
    assert (options["timeout"].connect_timeout, options["timeout"].read_timeout) == (10.0, 30.0)
    assert options["maxsize"] == 4
    assert options["ssl_context"] is not None


@pytest.mark.parametrize(
    "base_url",
    ["https://api.massive.com/v3", "https://api.massive.com?x=1", "https://u@api.massive.com"],
)
def test_the_base_url_names_only_an_origin(base_url: str) -> None:
    with pytest.raises(ValueError, match="must name only a scheme, host, and port"):
        RestClient(_KEY, base_url=base_url, max_concurrency=1)


def test_the_base_url_is_http_or_https() -> None:
    with pytest.raises(ValueError, match="is not an absolute http or https URL"):
        RestClient(_KEY, base_url="ftp://api.massive.com", max_concurrency=1)


def test_max_concurrency_is_at_least_one() -> None:
    with pytest.raises(ValueError, match="max_concurrency must be at least 1, not 0"):
        RestClient(_KEY, base_url="https://api.massive.com", max_concurrency=0)


def test_a_trailing_slash_on_the_base_url_is_accepted(fake_rest: FakeRestServer) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok())
    rest = RestClient(_KEY, base_url=fake_rest.url + "/", max_concurrency=1, clock=FakeClock())
    assert rest.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert fake_rest.requests[0].target == "/v3/trades/AAPL"


def test_every_request_sends_the_bearer_key_agent_and_gzip_and_no_key_in_the_url(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok())
    client.get(_TRADES, _AAPL, {"limit": 5})
    (received,) = fake_rest.requests
    assert received.target == "/v3/trades/AAPL?limit=5"
    assert received.headers["authorization"] == f"Bearer {_KEY}"
    assert received.headers["user-agent"] == USER_AGENT
    assert received.headers["accept-encoding"] == "gzip"
    assert _KEY not in received.target


def test_a_gzip_body_is_decoded(client: RestClient, fake_rest: FakeRestServer) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([{"i": 2}]), gzip=True))
    assert client.get(_TRADES, _AAPL, {}).rows == [{"i": 2}]


def test_get_returns_the_page_with_its_envelope(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(status="DELAYED", request_id="r9"))
    page = client.get(_TRADES, _AAPL, {})
    assert (page.status, page.request_id, page.rows) == ("DELAYED", "r9", [{"i": 1}])


def test_parameters_that_do_not_fit_raise_before_any_request(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    with pytest.raises(ValueError, match="has no query parameter 'apiKey'"):
        client.get(_TRADES, _AAPL, {"apiKey": _KEY})
    with pytest.raises(TypeError, match="'limit' takes integer values"):
        client.pages(_TRADES, _AAPL, {"limit": "5"})
    assert fake_rest.requests == []


# Proxies and certificates.


def test_https_proxy_routes_requests_through_the_proxy(
    fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", fake_rest.url)
    fake_rest.reply("/v3/trades/AAPL", _ok())
    rest = RestClient(
        _KEY, base_url="http://api.massive.test", max_concurrency=1, clock=FakeClock()
    )
    assert rest.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    (received,) = fake_rest.requests
    assert (received.proxied, received.host_url) == (True, "http://api.massive.test")


def test_no_proxy_bypasses_the_proxy(
    fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    no_proxy_env.setenv("NO_PROXY", "127.0.0.1")
    fake_rest.reply("/v3/trades/AAPL", _ok())
    assert _client(fake_rest).get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert fake_rest.requests[0].proxied is False


@pytest.fixture
def tls_server() -> Iterator[FakeRestServer]:
    server = FakeRestServer(tls=(_TLS / "tls_cert.pem", _TLS / "tls_key.pem"))
    server.start()
    yield server
    server.close()


def test_a_certificate_failure_is_never_retried_and_names_ssl_cert_file(
    tls_server: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    clock = FakeClock()
    rest = _client(tls_server, clock)
    with pytest.raises(
        WireError, match=r"TLS certificate verification failed for 127\.0\.0\.1"
    ) as caught:
        rest.get(_TRADES, _AAPL, {})
    assert "SSL_CERT_FILE" in caught.value.message
    assert (caught.value.status, caught.value.path) == (None, "/v3/trades/AAPL")
    assert clock.sleeps == []
    assert tls_server.requests == []


def test_ssl_cert_file_supplies_the_trusted_certificates(
    tls_server: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", str(_TLS / "tls_cert.pem"))
    tls_server.reply("/v3/trades/AAPL", _ok())
    assert _client(tls_server).get(_TRADES, _AAPL, {}).rows == [{"i": 1}]


# Retries.


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_transient_status_waits_half_one_and_two_seconds_plus_jitter(
    client: RestClient, fake_rest: FakeRestServer, *, clock: FakeClock, status: int
) -> None:
    failure = Reply(b"down", status=status)
    fake_rest.reply("/v3/trades/AAPL", failure, failure, failure, _ok())
    assert client.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert clock.sleeps == pytest.approx([0.55, 1.1, 2.2])
    assert len(fake_rest.requests) == 4


def test_a_persistent_transient_status_fails_after_four_attempts(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"request_id": "r", "message": "busy"}, status=503))
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {})
    assert str(caught.value) == (
        "HTTP 503 from GET /v3/trades/AAPL on 127.0.0.1 (request_id r): busy"
    )
    assert caught.value.status == 503
    assert len(fake_rest.requests) == 4
    assert len(clock.sleeps) == 3


def test_a_connection_reset_is_retried(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(reset=True), _ok())
    assert client.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert clock.sleeps == pytest.approx([0.55])


def test_a_persistent_connection_failure_names_the_host_and_cause(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(reset=True))
    with pytest.raises(WireError, match=r"^GET /v3/trades/AAPL on 127\.0\.0\.1 failed: ") as caught:
        client.get(_TRADES, _AAPL, {})
    assert (caught.value.status, caught.value.host) == (None, "127.0.0.1")
    assert len(fake_rest.requests) == 4
    assert len(clock.sleeps) == 3


def test_a_refused_connection_is_retried_then_fails(no_proxy_env: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    server = FakeRestServer()
    server.start()
    url = server.url
    server.close()
    rest = RestClient(_KEY, base_url=url, max_concurrency=1, clock=clock)
    with pytest.raises(WireError, match="failed: "):
        rest.get(_TRADES, _AAPL, {})
    assert len(clock.sleeps) == 3


@pytest.mark.parametrize(
    ("status", "kind"),
    [(401, WireAuthError), (403, WireEntitlementError), (404, WireError), (400, WireError)],
)
def test_other_statuses_fail_at_once(
    client: RestClient,
    fake_rest: FakeRestServer,
    *,
    clock: FakeClock,
    status: int,
    kind: type[WireError],
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"message": "no"}, status=status))
    with pytest.raises(kind) as caught:
        client.get(_TRADES, _AAPL, {})
    assert type(caught.value) is kind
    assert caught.value.status == status
    assert len(fake_rest.requests) == 1
    assert clock.sleeps == []


def test_a_redirect_is_never_followed(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    moved = Reply(b"", status=302, headers={"Location": "https://evil.example/steal"})
    fake_rest.reply("/v3/trades/AAPL", moved)
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {})
    assert caught.value.status == 302
    assert len(fake_rest.requests) == 1


def test_a_429_waits_its_retry_after(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    limited = Reply(b"", status=429, headers={"Retry-After": "7"})
    fake_rest.reply("/v3/trades/AAPL", limited, _ok())
    assert client.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert clock.sleeps == [7.0]
    assert clock.monotonic() == 7.0


def test_a_429_with_an_http_date_waits_until_then(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    # The fake clock's wall time starts at 1800000000 (2027-01-15 08:00:00 UTC).
    limited = Reply(b"", status=429, headers={"Retry-After": "Fri, 15 Jan 2027 08:00:12 GMT"})
    fake_rest.reply("/v3/trades/AAPL", limited, _ok())
    client.get(_TRADES, _AAPL, {})
    assert clock.sleeps == [12.0]


def test_a_429_without_retry_after_waits_15_30_then_60(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    limited = Reply({"message": "slow down"}, status=429)
    fake_rest.reply("/v3/trades/AAPL", limited, limited, limited, _ok())
    assert client.get(_TRADES, _AAPL, {}).rows == [{"i": 1}]
    assert clock.sleeps == [15.0, 30.0, 60.0]


def test_a_persistent_429_is_a_wire_error_with_status_429(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"message": "slow down"}, status=429))
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {})
    assert type(caught.value) is WireError
    assert caught.value.status == 429
    assert caught.value.message.endswith(": slow down")
    assert len(fake_rest.requests) == 4


@pytest.mark.parametrize(("retry_after", "held"), [(None, 60.0), ("9", 9.0)])
def test_a_429_on_the_last_attempt_still_holds_back_the_next_request(
    client: RestClient,
    fake_rest: FakeRestServer,
    *,
    clock: FakeClock,
    retry_after: str | None,
    held: float,
) -> None:
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers=headers))
    fake_rest.reply("/v3/trades/MSFT", _ok())
    with pytest.raises(WireError):
        client.get(_TRADES, _AAPL, {})
    before = clock.sleeps
    client.get(_TRADES, {"stockTicker": "MSFT"}, {})
    assert clock.sleeps == [*before, held]


def test_a_retry_after_over_60_seconds_fails_at_once_naming_the_wait(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply(
        "/v3/trades/AAPL",
        Reply({"request_id": "r", "message": "quota"}, status=429, headers={"Retry-After": "61"}),
    )
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {})
    assert str(caught.value) == (
        "HTTP 429 from GET /v3/trades/AAPL on 127.0.0.1 (request_id r): quota "
        "(Massive asked to wait 61 s, more than the 60 s the client waits)"
    )
    assert (caught.value.status, caught.value.request_id) == (429, "r")
    assert (caught.value.host, caught.value.path) == ("127.0.0.1", "/v3/trades/AAPL")
    assert len(fake_rest.requests) == 1
    assert clock.sleeps == []


def test_a_retry_after_of_exactly_60_seconds_is_honoured(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "60"}), _ok())
    client.get(_TRADES, _AAPL, {})
    assert clock.sleeps == [60.0]


def test_a_429_holds_back_every_later_request_of_the_client(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "5"}), _ok())
    fake_rest.reply("/v3/trades/MSFT", _ok())
    client.get(_TRADES, _AAPL, {})
    # The not-before instant has passed, so the next request goes at once.
    client.get(_TRADES, {"stockTicker": "MSFT"}, {})
    assert clock.sleeps == [5.0]


def test_the_not_before_instant_holds_back_16_threads_together(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    rest = _client(fake_rest, clock, max_concurrency=17)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "30"}), _ok())
    results: list[Page] = []
    errors: list[WireError] = []
    lock = threading.Lock()

    def call() -> None:
        try:
            page = rest.get(_TRADES, _AAPL, {})
        except WireError as error:
            with lock:
                errors.append(error)
            return
        with lock:
            results.append(page)

    first = threading.Thread(target=call)
    first.start()
    assert clock.wait_for_sleepers(1) == [30.0]
    others = [threading.Thread(target=call) for _ in range(16)]
    for thread in others:
        thread.start()
    # Every thread waits for the same instant, and none has sent anything.
    assert clock.wait_for_sleepers(17) == [30.0] * 17
    assert len(fake_rest.requests) == 1
    clock.advance(30.0)
    for thread in [first, *others]:
        thread.join(10)
    assert errors == []
    assert len(results) == 17
    assert len(fake_rest.requests) == 18
    rest.close()


# Paging.


def _paged(server: FakeRestServer, ticker: str, pages: int, *, size: int = 2) -> list[list[int]]:
    """Script `pages` pages of `size` rows for one ticker, linked by `next_url`.

    Returns:
        The row numbers of each page.
    """
    numbers = [[page * size + i for i in range(size)] for page in range(pages)]
    for page, rows in enumerate(numbers):
        target = f"/v3/trades/{ticker}" + (f"?cursor={page}" if page else "")
        next_url = (
            f"{server.url}/v3/trades/{ticker}?cursor={page + 1}&apiKey={_KEY}"
            if page + 1 < pages
            else None
        )
        server.reply(target, Reply(envelope([{"n": n} for n in rows], next_url=next_url)))
    return numbers


def _numbers(pages: Iterator[Page]) -> list[list[object]]:
    return [[row["n"] for row in page.rows] for page in pages]


def test_pages_follow_next_url_without_the_api_key(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    expected = _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {})) == expected
    targets = [r.target for r in fake_rest.requests]
    assert targets == ["/v3/trades/AAPL", "/v3/trades/AAPL?cursor=1", "/v3/trades/AAPL?cursor=2"]
    assert all(r.headers["authorization"] == f"Bearer {_KEY}" for r in fake_rest.requests)


def test_pages_are_requested_only_as_the_iterator_reaches_them(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    pages = client.pages(_TRADES, _AAPL, {})
    assert fake_rest.requests == []
    next(pages)
    assert len(fake_rest.requests) == 1


def test_the_limit_cuts_the_last_page_and_stops_paging(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, limit=3)) == [[0, 1], [2]]
    assert len(fake_rest.requests) == 2


def test_a_limit_on_a_page_boundary_requests_no_further_page(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, limit=4)) == [[0, 1], [2, 3]]
    assert len(fake_rest.requests) == 2


def test_a_limit_above_the_rows_yields_every_page(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 2)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, limit=100)) == [[0, 1], [2, 3]]


@pytest.mark.parametrize("limit", [0, -1])
def test_a_limit_below_one_is_refused_at_once(client: RestClient, limit: int) -> None:
    with pytest.raises(ValueError, match=f"limit must be at least 1, not {limit}"):
        client.pages(_TRADES, _AAPL, {}, limit=limit)


@pytest.mark.parametrize("next_url", [None, "", 5])
def test_paging_stops_without_a_usable_next_url(
    client: RestClient, fake_rest: FakeRestServer, next_url: object
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"status": "OK", "results": [], "next_url": next_url}))
    assert len(list(client.pages(_TRADES, _AAPL, {}))) == 1


def test_a_body_without_an_envelope_ends_paging(fake_rest: FakeRestServer) -> None:
    upcoming = RestPath(key="/v1/marketstatus/upcoming", fields=(), result_key=None)
    fake_rest.reply("/v1/marketstatus/upcoming", Reply([{"status": "closed"}]))
    pages = list(_client(fake_rest).pages(upcoming, {}, {}))
    assert [page.rows for page in pages] == [[{"status": "closed"}]]


def test_a_next_url_on_another_host_is_refused_and_never_requested(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url="https://evil.example/v3/trades/AAPL?cursor=1"))
    pages = client.pages(_TRADES, _AAPL, {})
    assert len(next(pages).rows) == 1
    with pytest.raises(
        WireError, match=r"refusing to follow next_url 'https://evil\.example"
    ) as caught:
        next(pages)
    assert caught.value.host == "127.0.0.1"
    assert len(fake_rest.requests) == 1


def test_a_refused_next_url_never_puts_the_key_in_an_error_or_a_log(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    leaked = f"https://api.polygon.io/v3/trades/AAPL?cursor=1&apiKey={_KEY}"
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=leaked))
    with pytest.raises(WireError) as caught:
        list(client.pages(_TRADES, _AAPL, {}))
    assert "api.polygon.io/v3/trades/AAPL?cursor=1" in caught.value.message
    assert _KEY not in caught.value.message
    assert _KEY not in str(caught.value.__cause__)
    assert not any(_KEY in r.getMessage() for r in caplog.records)


def test_a_reset_while_paging_retries_that_page_not_the_first(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    expected = _paged(fake_rest, "AAPL", 3)
    good = Reply(
        envelope([{"n": 2}, {"n": 3}], next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=2")
    )
    fake_rest.reply("/v3/trades/AAPL?cursor=1", Reply(reset=True), good)
    fake_rest._script["/v3/trades/AAPL?cursor=1"].popleft()  # drop the plain scripted page
    assert _numbers(client.pages(_TRADES, _AAPL, {})) == expected
    targets = [r.target for r in fake_rest.requests]
    assert targets == [
        "/v3/trades/AAPL",
        "/v3/trades/AAPL?cursor=1",
        "/v3/trades/AAPL?cursor=1",
        "/v3/trades/AAPL?cursor=2",
    ]
    assert clock.sleeps == pytest.approx([0.55])


def test_a_bad_body_on_a_later_page_is_an_error(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1"))
    fake_rest.reply("/v3/trades/AAPL?cursor=1", Reply(b"<html>proxy error</html>"))
    pages = client.pages(_TRADES, _AAPL, {})
    next(pages)
    with pytest.raises(WireError, match="the body is not JSON"):
        next(pages)


def test_16_threads_page_through_one_client_with_a_429_and_resets(
    fake_rest: FakeRestServer,
) -> None:
    clock = FakeClock()
    rest = _client(fake_rest, clock, max_concurrency=16)
    expected = {}
    for i in range(16):
        ticker = f"T{i}"
        expected[ticker] = _paged(fake_rest, ticker, 3)
        cursor = f"/v3/trades/{ticker}?cursor=1"
        fake_rest._script[cursor].appendleft(Reply(reset=True))
    first = fake_rest._script["/v3/trades/T0"]
    first.appendleft(Reply(b"", status=429, headers={"Retry-After": "2"}))
    arrivals: list[float] = []
    fake_rest.on_request(
        lambda received: (
            arrivals.append(clock.monotonic()) if received.target == "/v3/trades/T0" else None
        )
    )
    got: dict[str, list[list[object]]] = {}
    errors: list[WireError] = []
    lock = threading.Lock()
    start = threading.Barrier(16)

    def page_through(ticker: str) -> None:
        start.wait()
        try:
            numbers = _numbers(rest.pages(_TRADES, {"stockTicker": ticker}, {}))
        except WireError as error:
            with lock:
                errors.append(error)
            return
        with lock:
            got[ticker] = numbers

    threads = [threading.Thread(target=page_through, args=(t,)) for t in expected]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert errors == []
    assert got == expected
    # The retry after the 429 arrived no sooner than its Retry-After.
    assert len(arrivals) == 2
    assert round(arrivals[1] - arrivals[0], 9) >= 2.0
    assert len(fake_rest.requests) == 16 * 4 + 1
    rest.close()


# Closing.


def test_a_closed_client_refuses_requests(fake_rest: FakeRestServer) -> None:
    rest = _client(fake_rest)
    rest.close()
    rest.close()
    with pytest.raises(WireError, match="the REST client is closed"):
        rest.get(_TRADES, _AAPL, {})
    assert fake_rest.requests == []


def test_a_client_closed_during_a_retry_wait_stops(fake_rest: FakeRestServer) -> None:
    clock = FakeClock()
    rest = _client(fake_rest, clock)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503))
    fake_rest.on_request(lambda _: rest.close())
    with pytest.raises(WireError, match="the REST client is closed"):
        rest.get(_TRADES, _AAPL, {})
    assert len(fake_rest.requests) == 1


# Logging.


def test_requests_retries_and_failures_are_logged_without_the_key(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503), _ok(request_id="r1"))
    fake_rest.reply("/v3/trades/MSFT", Reply(b"x", status=502))
    client.get(_TRADES, _AAPL, {"limit": 1})
    with pytest.raises(WireError):
        client.get(_TRADES, {"stockTicker": "MSFT"}, {})
    ours = [r for r in caplog.records if r.name == client_module.__name__]
    messages = [(r.levelno, r.getMessage()) for r in ours]
    assert (
        logging.DEBUG,
        "GET /v3/trades/AAPL?limit=1 -> 503 (request_id None) in 0.000 s",
    ) in messages
    assert (
        logging.INFO,
        (
            "HTTP 503 from GET /v3/trades/AAPL?limit=1 on 127.0.0.1: an empty body; "
            "retry 1 of 3 in 0.55 s"
        ),
    ) in messages
    assert (
        logging.DEBUG,
        "GET /v3/trades/AAPL?limit=1 -> 200 (request_id r1): 1 rows in 0.000 s",
    ) in messages
    assert (
        logging.ERROR,
        "HTTP 502 from GET /v3/trades/MSFT on 127.0.0.1: x; giving up after 4 attempts",
    ) in messages
    every = [r.getMessage() + repr(r.args) for r in caplog.records]
    assert every
    assert not any(_KEY in text or "Bearer" in text for text in every)


def test_a_paged_next_url_key_never_reaches_a_log(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _paged(fake_rest, "AAPL", 2)
    list(client.pages(_TRADES, _AAPL, {}))
    assert caplog.records
    assert not any(_KEY in r.getMessage() for r in caplog.records)
