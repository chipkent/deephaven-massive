"""Tests for the REST client, against the fake REST server over real sockets.

Time is virtual: every client gets a `FakeClock`, so retry waits are recorded, never slept.
"""

from __future__ import annotations

import logging
import math
import ssl
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
import urllib3

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
)
from deephaven_massive._massive.rest import client as client_module
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.rest.governor import (
    UNLIMITED,
    Admission,
    Patience,
    Permit,
    RequestGovernor,
)
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath, WireUnit
from deephaven_massive._types import override
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterator

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


def _governor(clock: FakeClock | None = None) -> RequestGovernor:
    return RequestGovernor(("stocks", "options"), clock=clock or FakeClock())


def _client(
    server: FakeRestServer,
    clock: FakeClock | None = None,
    *,
    max_concurrency: int = 4,
    governor: RequestGovernor | None = None,
) -> RestClient:
    return RestClient(
        _KEY,
        base_url=server.url,
        max_concurrency=max_concurrency,
        governor=governor or _governor(clock),
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
    retries = options["retries"]
    assert retries is client_module._NO_RETRIES
    assert (retries.total, retries.redirect) == (False, False)
    assert (options["timeout"].connect_timeout, options["timeout"].read_timeout) == (10.0, 30.0)
    assert options["maxsize"] == 4
    assert options["ssl_context"] is not None


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.massive.com/v3",
        "https://api.massive.com?x=1",
        "https://u@api.massive.com",
        "https://:pw@api.massive.com",
        "https://api.massive.com#f",
    ],
)
def test_the_base_url_names_only_an_origin(base_url: str) -> None:
    with pytest.raises(ValueError, match="must name only a scheme, host, and port"):
        RestClient(_KEY, base_url=base_url, max_concurrency=1, governor=_governor())


def test_the_base_url_is_http_or_https() -> None:
    with pytest.raises(ValueError, match="is not an absolute http or https URL"):
        RestClient(_KEY, base_url="ftp://api.massive.com", max_concurrency=1, governor=_governor())


@pytest.mark.parametrize("base_url", ["http://api.massive.com", "http://10.0.0.1:8080"])
def test_plain_http_is_refused_except_for_loopback(base_url: str) -> None:
    with pytest.raises(ValueError, match="would send the API key in clear text; use https"):
        RestClient(_KEY, base_url=base_url, max_concurrency=1, governor=_governor())


@pytest.mark.parametrize(
    "base_url", ["http://localhost:8080", "http://127.0.0.2", "http://[::1]:8080"]
)
def test_plain_http_is_allowed_for_loopback(base_url: str) -> None:
    RestClient(_KEY, base_url=base_url, max_concurrency=1, governor=_governor()).close()


@pytest.mark.parametrize(
    "api_key",
    ["", "s3cr3t\n", " s3cr3t", "s3cr3t ", "s3c r3t", "s3c\tr3t", "s3cr\u00e9t", "s3cr3t\r\nX: y"],
)
def test_an_api_key_that_cannot_be_a_header_is_refused_without_being_shown(
    api_key: str,
) -> None:
    with pytest.raises(ValueError, match="api_key must be non-empty visible ASCII") as caught:
        RestClient(
            api_key, base_url="https://api.massive.com", max_concurrency=1, governor=_governor()
        )
    assert "s3c" not in str(caught.value)


def test_max_concurrency_is_at_least_one() -> None:
    with pytest.raises(ValueError, match="max_concurrency must be at least 1, not 0"):
        RestClient(
            _KEY, base_url="https://api.massive.com", max_concurrency=0, governor=_governor()
        )


def test_a_trailing_slash_on_the_base_url_is_accepted(fake_rest: FakeRestServer) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok())
    rest = RestClient(_KEY, base_url=fake_rest.url + "/", max_concurrency=1, governor=_governor())
    assert rest.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert fake_rest.requests[0].target == "/v3/trades/AAPL"


def test_every_request_sends_the_bearer_key_agent_and_gzip_and_no_key_in_the_url(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok())
    client.get(_TRADES, _AAPL, {"limit": 5}, bucket="stocks")
    (received,) = fake_rest.requests
    assert received.target == "/v3/trades/AAPL?limit=5"
    assert received.headers["authorization"] == f"Bearer {_KEY}"
    assert received.headers["user-agent"] == USER_AGENT
    assert received.headers["accept-encoding"] == "gzip"
    assert _KEY not in received.target


def test_a_gzip_body_is_decoded(client: RestClient, fake_rest: FakeRestServer) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([{"i": 2}]), gzip=True))
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 2}]


def test_get_returns_the_page_with_its_envelope(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(status="DELAYED", request_id="r9"))
    page = client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert (page.status, page.request_id, page.rows) == ("DELAYED", "r9", [{"i": 1}])


def test_a_numeric_request_id_never_fails_a_call(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    # Massive's documentation samples send `"request_id": 1`.
    fake_rest.reply(
        "/v3/trades/AAPL",
        Reply({"status": "OK", "request_id": 1, "results": [{"i": 1}]}),
        Reply({"request_id": 7, "message": "bad ticker"}, status=400),
    )
    page = client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert (page.status, page.request_id, page.rows) == ("OK", "1", [{"i": 1}])
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert caught.value.request_id == "7"
    assert "(request_id 7): bad ticker" in str(caught.value)


def test_parameters_that_do_not_fit_raise_before_any_request(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    with pytest.raises(ValueError, match="has no query parameter 'apiKey'"):
        client.get(_TRADES, _AAPL, {"apiKey": _KEY}, bucket="stocks")
    with pytest.raises(TypeError, match="'limit' takes integer values"):
        client.pages(_TRADES, _AAPL, {"limit": "5"}, bucket="stocks")
    assert fake_rest.requests == []


# Proxies and certificates.


@pytest.fixture
def tunnelled(tls_server: FakeRestServer, no_proxy_env: pytest.MonkeyPatch) -> FakeRestServer:
    """Trust the TLS fake and script one page on it, so a client can reach it through a proxy.

    Returns:
        The TLS fake.
    """
    no_proxy_env.setenv("SSL_CERT_FILE", str(_TLS / "tls_cert.pem"))
    no_proxy_env.delenv("SSL_CERT_DIR", raising=False)
    tls_server.reply("/v3/trades/AAPL", _ok())
    return tls_server


def test_https_goes_through_the_proxy_in_a_connect_tunnel(
    tunnelled: FakeRestServer, fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", fake_rest.url)
    rest = _client(tunnelled)
    assert rest.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    (connect,) = fake_rest.requests
    assert (connect.method, connect.target) == ("CONNECT", tunnelled.url.removeprefix("https://"))
    (received,) = tunnelled.requests
    assert received.headers["authorization"] == f"Bearer {_KEY}"
    rest.close()


def test_proxy_credentials_go_to_the_proxy_and_the_bearer_key_never_does(
    tunnelled: FakeRestServer, fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    host = fake_rest.url.removeprefix("http://")
    no_proxy_env.setenv("HTTPS_PROXY", f"http://alice:s%40cret@{host}")
    rest = _client(tunnelled)
    rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    (connect,) = fake_rest.requests
    # base64 of "alice:s@cret"
    assert connect.headers["proxy-authorization"] == "Basic YWxpY2U6c0BjcmV0"
    assert "authorization" not in connect.headers
    assert _KEY not in repr(connect.headers)
    (received,) = tunnelled.requests
    assert "proxy-authorization" not in received.headers
    assert "s%40cret" not in str(rest._pool.proxy)
    rest.close()


def test_a_proxy_that_refuses_the_credentials_is_never_retried(
    tunnelled: FakeRestServer, fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    # Repeating credentials the proxy refused could lock the account on a corporate proxy.
    host = fake_rest.url.removeprefix("http://")
    no_proxy_env.setenv("HTTPS_PROXY", f"http://alice:wr0ng@{host}")
    fake_rest.refuse_tunnels(407)
    clock = FakeClock()
    governor = _governor(clock)
    rest = _client(tunnelled, governor=governor)
    with pytest.raises(WireError, match="the proxy refused its credentials") as caught:
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    rest.close()
    assert governor.snapshot()[0].recent == 0  # the tunnel never opened: no slot held
    assert caught.value.message == (
        "GET /v3/trades/AAPL: the proxy refused its credentials (HTTP 407 for a tunnel to "
        "127.0.0.1); check the user and password in HTTPS_PROXY"
    )
    assert (caught.value.status, caught.value.path) == (407, "/v3/trades/AAPL")
    assert [r.method for r in fake_rest.requests] == ["CONNECT"]
    assert (tunnelled.requests, clock.waits) == ([], [])


def test_a_proxy_reached_over_https_is_verified_like_massive(
    no_proxy_env: pytest.MonkeyPatch,
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", "https://proxy.corp:3129")
    rest = RestClient(
        _KEY, base_url="https://api.massive.test", max_concurrency=1, governor=_governor()
    )
    assert type(rest._pool).__name__ == "ProxyManager"
    proxy_context = rest._pool.connection_pool_kw["_proxy_config"].ssl_context
    assert proxy_context is not None
    assert proxy_context.verify_mode is ssl.CERT_REQUIRED
    assert proxy_context.check_hostname
    rest.close()


def test_proxy_credentials_never_reach_an_error_or_a_log(
    no_proxy_env: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    no_proxy_env.setenv("HTTPS_PROXY", "http://alice:topsecret@127.0.0.1:9")
    rest = RestClient(
        _KEY, base_url="https://api.massive.test", max_concurrency=1, governor=_governor()
    )
    with pytest.raises(WireError, match="failed: ") as caught:
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert "topsecret" not in caught.value.message
    assert caplog.records
    assert not any("topsecret" in r.getMessage() for r in caplog.records)


def test_no_proxy_bypasses_the_proxy(
    fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    no_proxy_env.setenv("NO_PROXY", "127.0.0.1")
    fake_rest.reply("/v3/trades/AAPL", _ok())
    assert _client(fake_rest).get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
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
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert "SSL_CERT_FILE" in caught.value.message
    assert (caught.value.status, caught.value.path) == (None, "/v3/trades/AAPL")
    assert clock.waits == []
    assert tls_server.requests == []


def test_ssl_cert_file_supplies_the_trusted_certificates(
    tls_server: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", str(_TLS / "tls_cert.pem"))
    tls_server.reply("/v3/trades/AAPL", _ok())
    assert _client(tls_server).get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]


# Retries.


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_transient_status_waits_half_one_and_two_seconds_plus_jitter(
    client: RestClient, fake_rest: FakeRestServer, *, clock: FakeClock, status: int
) -> None:
    failure = Reply(b"down", status=status)
    fake_rest.reply("/v3/trades/AAPL", failure, failure, failure, _ok())
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert clock.waits == pytest.approx([0.55, 1.1, 2.2])
    assert len(fake_rest.requests) == 4


def test_a_persistent_transient_status_fails_after_four_attempts(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"request_id": "r", "message": "busy"}, status=503))
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert str(caught.value) == (
        "HTTP 503 from GET /v3/trades/AAPL on 127.0.0.1 (request_id r): busy"
    )
    assert caught.value.status == 503
    assert len(fake_rest.requests) == 4
    assert len(clock.waits) == 3


def test_a_connection_reset_is_retried(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(reset=True), _ok())
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert clock.waits == pytest.approx([0.55])


def test_a_persistent_connection_failure_names_the_host_and_cause(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(reset=True))
    with pytest.raises(WireError, match=r"^GET /v3/trades/AAPL on 127\.0\.0\.1 failed: ") as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert (caught.value.status, caught.value.host) == (None, "127.0.0.1")
    assert len(fake_rest.requests) == 4
    assert len(clock.waits) == 3


def test_a_refused_connection_is_retried_then_fails(no_proxy_env: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    server = FakeRestServer()
    server.start()
    url = server.url
    server.close()
    governor = _governor(clock)
    rest = RestClient(_KEY, base_url=url, max_concurrency=1, governor=governor)
    with pytest.raises(WireError, match="failed: "):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert len(clock.waits) == 3
    # No connection was made, so no attempt reached Massive: none holds a slot.
    state = governor.snapshot()[0]
    assert (state.attempts, state.in_flight, state.recent) == (4, 0, 0)


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
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert type(caught.value) is kind
    assert caught.value.status == status
    assert len(fake_rest.requests) == 1
    assert clock.waits == []


def test_a_redirect_is_never_followed(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    moved = Reply(b"", status=302, headers={"Location": "https://evil.example/steal"})
    fake_rest.reply("/v3/trades/AAPL", moved)
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert caught.value.status == 302
    assert len(fake_rest.requests) == 1


def test_a_429_waits_its_retry_after(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    limited = Reply(b"", status=429, headers={"Retry-After": "7"})
    fake_rest.reply("/v3/trades/AAPL", limited, _ok())
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert clock.waits == [7.0]
    assert clock.monotonic() == 7.0


def test_a_429_with_an_http_date_waits_until_then_by_the_responses_own_clock(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    # The response's Date says 08:00:00, so 08:00:12 is 12 s away, whatever this clock says.
    limited = Reply(
        b"",
        status=429,
        headers={
            "Date": "Fri, 15 Jan 2027 08:00:00 GMT",
            "Retry-After": "Fri, 15 Jan 2027 08:00:12 GMT",
        },
    )
    fake_rest.reply("/v3/trades/AAPL", limited, _ok())
    clock.advance(3600.0)  # an hour ahead of the server
    client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [12.0]


def test_with_an_unusable_date_an_http_date_is_measured_on_the_local_clock(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    # The fake clock's wall time starts at 1800000000 (2027-01-15 08:00:00 UTC). (The fake
    # server always sends a Date; the client's `_http_time` test covers none at all.)
    headers = {"Date": "not a date", "Retry-After": "Fri, 15 Jan 2027 08:00:12 -0000"}
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers=headers), _ok())
    client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [12.0]


def test_http_time_reads_a_date_header() -> None:
    assert client_module._http_time(None) is None
    assert client_module._http_time("soon") is None
    assert client_module._http_time("Fri, 15 Jan 2027 08:00:00 GMT") == 1_800_000_000.0
    assert client_module._http_time("Fri, 15 Jan 2027 08:00:00 -0000") == 1_800_000_000.0


def test_a_429_without_retry_after_pauses_15_30_then_60_and_never_counts_as_a_retry(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    limited = Reply({"message": "slow down"}, status=429)
    flaky = Reply(b"", status=503)
    # More 429s than the three retries, and transient failures between them.
    fake_rest.reply("/v3/trades/AAPL", limited, limited, flaky, limited, limited, flaky, _ok())
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    # A 503's answer ends the run of refusals, so the next run starts at 15 again.
    assert clock.waits == [15.0, 30.0, pytest.approx(0.55), 15.0, 30.0, pytest.approx(1.1)]
    assert len(fake_rest.requests) == 7


def test_a_429_pauses_only_its_bucket_and_every_client_of_the_governor(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    first = _client(fake_rest, governor=governor)
    second = _client(fake_rest, governor=governor)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "5"}), _ok())
    fake_rest.reply("/v3/trades/MSFT", _ok())
    first.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [5.0]
    second.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="options")  # another bucket
    second.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="stocks")  # the pause has passed
    assert clock.waits == [5.0]
    first.close()
    second.close()


def test_a_pause_past_the_calls_patience_fails_it_with_status_429_naming_the_request(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    refusal = Reply(
        {"request_id": "r", "message": "quota"}, status=429, headers={"Retry-After": "601"}
    )
    fake_rest.reply("/v3/trades/AAPL", refusal)
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks", patience=Patience(600.0))
    assert str(caught.value).startswith(
        "Massive is refusing stocks requests with HTTP 429 (latest refusal: HTTP 429 from GET "
        "/v3/trades/AAPL on 127.0.0.1 (request_id r): quota). Its next request, a probe, goes at "
        "2027-01-15T08:10:01+00:00 ("
    )
    assert "waits out at most 600 s of pauses and has waited 0 s" in str(caught.value)
    assert caught.value.status == 429
    assert (caught.value.host, caught.value.path) == ("127.0.0.1", "/v3/trades/AAPL")
    # Another call with the same patience fails at once, without being sent.
    fake_rest.reply("/v3/trades/MSFT", _ok())
    with pytest.raises(WireError, match="Its next request, a probe, goes at") as again:
        client.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="stocks", patience=Patience(600.0))
    assert (again.value.status, again.value.path) == (429, "/v3/trades/MSFT")
    assert len(fake_rest.requests) == 1
    assert clock.waits == []


def test_a_refusal_naming_the_per_minute_maximum_paces_the_bucket(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    heard: list[tuple[str, str, bool]] = []

    def hook(bucket: str, error: WireError, *, basic: bool) -> None:
        heard.append((bucket, error.message, basic))

    rest = RestClient(
        _KEY, base_url=fake_rest.url, max_concurrency=1, governor=_governor(clock), on_refused=hook
    )
    basic = Reply({"message": "You've exceeded the maximum requests per minute."}, status=429)
    fake_rest.reply("/v3/trades/AAPL", basic, _ok())
    fake_rest.reply("/v3/trades/MSFT", Reply({"message": "busy"}, status=429), _ok())
    rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    rest.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="options")
    assert heard == [
        (
            "stocks",
            (
                "HTTP 429 from GET /v3/trades/AAPL on 127.0.0.1: You've exceeded the maximum "
                "requests per minute."
            ),
            True,
        ),
        ("options", "HTTP 429 from GET /v3/trades/MSFT on 127.0.0.1: busy", False),
    ]
    # A paced bucket pauses a whole window; an unpaced one 15 s.
    assert clock.waits == [61.0, 15.0]
    assert rest.governor.snapshot()[0].learned
    rest.close()


def test_an_unknown_bucket_is_refused_before_anything_is_sent(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        client.get(_TRADES, _AAPL, {}, bucket="crypto")
    assert fake_rest.requests == []


def test_the_client_waits_for_its_bucket_s_slot(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    governor.declare({"stocks"})
    rest = _client(fake_rest, governor=governor)
    fake_rest.reply("/v3/trades/AAPL", _ok())
    for _ in range(6):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [61.0]
    assert governor.snapshot()[0].attempts == 6
    rest.close()


def test_16_threads_wait_out_one_pause_together(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    rest = _client(fake_rest, clock, max_concurrency=17)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "30"}), _ok())
    results: list[Page] = []
    errors: list[WireError] = []
    lock = threading.Lock()

    def call() -> None:
        try:
            page = rest.get(_TRADES, _AAPL, {}, bucket="stocks")
        except WireError as error:
            with lock:
                errors.append(error)
            return
        with lock:
            results.append(page)

    first = threading.Thread(target=call, daemon=True)
    first.start()
    assert clock.wait_for_sleepers(1) == [30.0]
    others = [threading.Thread(target=call, daemon=True) for _ in range(16)]
    for thread in others:
        thread.start()
    # The head waits for the pause and the rest for their turn; none has sent anything. Once
    # the pause ends the head probes alone, and the rest go once its answer comes.
    assert clock.wait_for_sleepers(17) == [30.0] + [math.inf] * 16
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
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks")) == expected
    targets = [r.target for r in fake_rest.requests]
    assert targets == ["/v3/trades/AAPL", "/v3/trades/AAPL?cursor=1", "/v3/trades/AAPL?cursor=2"]
    assert all(r.headers["authorization"] == f"Bearer {_KEY}" for r in fake_rest.requests)


def test_pages_are_requested_only_as_the_iterator_reaches_them(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    pages = client.pages(_TRADES, _AAPL, {}, bucket="stocks")
    assert fake_rest.requests == []
    next(pages)
    assert len(fake_rest.requests) == 1


def test_the_limit_cuts_the_last_page_and_stops_paging(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks", limit=3)) == [[0, 1], [2]]
    assert len(fake_rest.requests) == 2


def test_a_limit_on_a_page_boundary_requests_no_further_page(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks", limit=4)) == [[0, 1], [2, 3]]
    assert len(fake_rest.requests) == 2


def test_a_limit_above_the_rows_yields_every_page(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    _paged(fake_rest, "AAPL", 2)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks", limit=100)) == [
        [0, 1],
        [2, 3],
    ]


@pytest.mark.parametrize("limit", [0, -1])
def test_a_limit_below_one_is_refused_at_once(client: RestClient, limit: int) -> None:
    with pytest.raises(ValueError, match=f"limit must be at least 1, not {limit}"):
        client.pages(_TRADES, _AAPL, {}, bucket="stocks", limit=limit)


@pytest.mark.parametrize("next_url", [None, "", 5])
def test_paging_stops_without_a_usable_next_url(
    client: RestClient, fake_rest: FakeRestServer, next_url: object
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply({"status": "OK", "results": [], "next_url": next_url}))
    assert len(list(client.pages(_TRADES, _AAPL, {}, bucket="stocks"))) == 1


def test_a_body_without_an_envelope_ends_paging(fake_rest: FakeRestServer) -> None:
    upcoming = RestPath(key="/v1/marketstatus/upcoming", fields=(), result_key=None)
    fake_rest.reply("/v1/marketstatus/upcoming", Reply([{"status": "closed"}]))
    pages = list(_client(fake_rest).pages(upcoming, {}, {}, bucket="stocks"))
    assert [page.rows for page in pages] == [[{"status": "closed"}]]


def test_a_next_url_on_another_host_is_refused_and_never_requested(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url="https://evil.example/v3/trades/AAPL?cursor=1"))
    pages = client.pages(_TRADES, _AAPL, {}, bucket="stocks")
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
        list(client.pages(_TRADES, _AAPL, {}, bucket="stocks"))
    assert "api.polygon.io/v3/trades/AAPL?cursor=1" in caught.value.message
    assert _KEY not in caught.value.message
    assert _KEY not in str(caught.value.__cause__)
    assert not any(_KEY in r.getMessage() for r in caplog.records)


def test_a_reset_while_paging_retries_that_page_not_the_first(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    expected = _paged(fake_rest, "AAPL", 3)
    fake_rest.reply_first("/v3/trades/AAPL?cursor=1", Reply(reset=True))
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks")) == expected
    targets = [r.target for r in fake_rest.requests]
    assert targets == [
        "/v3/trades/AAPL",
        "/v3/trades/AAPL?cursor=1",
        "/v3/trades/AAPL?cursor=1",
        "/v3/trades/AAPL?cursor=2",
    ]
    assert clock.waits == pytest.approx([0.55])


def test_a_next_url_that_repeats_the_page_is_refused(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1"))
    fake_rest.reply(
        "/v3/trades/AAPL?cursor=1",
        _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1&apiKey=k"),
    )
    pages = client.pages(_TRADES, _AAPL, {}, bucket="stocks")
    next(pages)
    next(pages)
    with pytest.raises(
        WireError,
        match=r"^/v3/trades/\{stockTicker\}: next_url /v3/trades/AAPL\?cursor=1 repeats a page",
    ) as caught:
        next(pages)
    assert caught.value.path == "/v3/trades/AAPL?cursor=1"
    assert "apiKey" not in caught.value.message
    assert len(fake_rest.requests) == 2


def test_a_next_url_cycle_through_earlier_pages_is_refused(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1"))
    fake_rest.reply(
        "/v3/trades/AAPL?cursor=1", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=2")
    )
    fake_rest.reply(
        "/v3/trades/AAPL?cursor=2", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1")
    )
    pages = client.pages(_TRADES, _AAPL, {}, bucket="stocks")
    for _ in range(3):
        next(pages)
    with pytest.raises(
        WireError, match=r"repeats a page this pagination already fetched"
    ) as caught:
        next(pages)
    assert caught.value.path == "/v3/trades/AAPL?cursor=1"
    assert [r.target for r in fake_rest.requests] == [
        "/v3/trades/AAPL",
        "/v3/trades/AAPL?cursor=1",
        "/v3/trades/AAPL?cursor=2",
    ]


def test_a_next_url_back_to_the_first_page_is_refused(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1"))
    fake_rest.reply("/v3/trades/AAPL?cursor=1", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL"))
    with pytest.raises(WireError, match=r"next_url /v3/trades/AAPL repeats") as caught:
        list(client.pages(_TRADES, _AAPL, {}, bucket="stocks"))
    assert caught.value.path == "/v3/trades/AAPL"
    assert len(fake_rest.requests) == 2


def test_separate_paginations_may_fetch_the_same_targets(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    expected = _paged(fake_rest, "AAPL", 3)
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks")) == expected
    assert _numbers(client.pages(_TRADES, _AAPL, {}, bucket="stocks")) == expected


def test_a_bad_body_on_a_later_page_is_an_error(
    client: RestClient, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/trades/AAPL", _ok(next_url=f"{fake_rest.url}/v3/trades/AAPL?cursor=1"))
    fake_rest.reply("/v3/trades/AAPL?cursor=1", Reply(b"<html>proxy error</html>"))
    pages = client.pages(_TRADES, _AAPL, {}, bucket="stocks")
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
        fake_rest.reply_first(cursor, Reply(reset=True))
    fake_rest.reply_first("/v3/trades/T0", Reply(b"", status=429, headers={"Retry-After": "2"}))
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
            numbers = _numbers(rest.pages(_TRADES, {"stockTicker": ticker}, {}, bucket="stocks"))
        except WireError as error:
            with lock:
                errors.append(error)
            return
        with lock:
            got[ticker] = numbers

    threads = [threading.Thread(target=page_through, args=(t,), daemon=True) for t in expected]
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
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert fake_rest.requests == []


def test_a_client_closed_during_a_retry_wait_stops(fake_rest: FakeRestServer) -> None:
    clock = FakeClock()
    rest = _client(fake_rest, clock)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503))
    fake_rest.on_request(lambda _: rest.close())
    with pytest.raises(WireError, match="the REST client is closed"):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert len(fake_rest.requests) == 1


def test_close_ends_a_pause_at_once(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    rest = _client(fake_rest, clock)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "60"}))
    errors: list[WireError] = []

    def call() -> None:
        try:
            rest.get(_TRADES, _AAPL, {}, bucket="stocks")
        except WireError as error:
            errors.append(error)

    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    assert clock.wait_for_sleepers(1) == [60.0]
    rest.close()
    thread.join(10)
    assert not thread.is_alive()
    assert [e.message for e in errors] == ["GET /v3/trades/AAPL: the REST client is closed"]
    assert clock.monotonic() == 0.0
    assert len(fake_rest.requests) == 1


def test_close_ends_a_transient_retry_wait_at_once(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    rest = _client(fake_rest, clock)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503))
    errors: list[WireError] = []

    def call() -> None:
        try:
            rest.get(_TRADES, _AAPL, {}, bucket="stocks")
        except WireError as error:
            errors.append(error)

    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    assert clock.wait_for_sleepers(1) == [pytest.approx(0.55)]
    rest.close()
    thread.join(10)
    assert not thread.is_alive()
    assert [e.message for e in errors] == ["GET /v3/trades/AAPL: the REST client is closed"]
    assert clock.monotonic() == 0.0


class _Call:
    """A `get` on its own thread, recording its page or its error."""

    def __init__(self, rest: RestClient, cancel: Cancel | None = None) -> None:
        self.page: Page | None = None
        self.error: WireError | None = None
        self._rest = rest
        self._cancel = cancel
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            self.page = self._rest.get(_TRADES, _AAPL, {}, bucket="stocks", cancel=self._cancel)
        except WireError as error:
            self.error = error

    def join(self) -> None:
        self.thread.join(10)
        assert not self.thread.is_alive()


def _full(clock: FakeClock, fake_rest: FakeRestServer) -> RequestGovernor:
    """Return a governor whose Basic stocks bucket has spent its five slots."""
    governor = _governor(clock)
    governor.declare({"stocks"})
    fake_rest.reply("/v3/trades/AAPL", _ok())
    rest = _client(fake_rest, governor=governor)
    for _ in range(5):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    rest.close()
    return governor


def test_the_callers_cancel_ends_a_slot_wait(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    rest = _client(fake_rest, governor=_full(clock, fake_rest))
    cancel = Cancel()
    call = _Call(rest, cancel)
    assert clock.wait_for_sleepers(1) == [61.0]
    cancel.set()
    call.join()
    assert call.error is not None
    assert call.error.message == "GET /v3/trades/AAPL: cancelled"
    assert (call.error.status, call.error.path) == (None, "/v3/trades/AAPL")
    assert len(fake_rest.requests) == 5
    assert rest.governor.snapshot()[0].waiting == 0
    rest.close()


def test_closing_one_client_ends_its_waits_and_nobody_elses(fake_rest: FakeRestServer) -> None:
    clock = FakeClock(manual=True)
    governor = _full(clock, fake_rest)
    closing, staying = _client(fake_rest, governor=governor), _client(fake_rest, governor=governor)
    first = _Call(closing)
    clock.wait_for_sleepers(1)
    second = _Call(staying)
    clock.wait_for_sleepers(2)
    closing.close()
    first.join()
    assert first.error is not None
    assert first.error.message == "GET /v3/trades/AAPL: the REST client is closed"
    assert clock.wait_for_sleepers(1) == [61.0]  # the other client's request is the head now
    clock.advance(61.0)
    second.join()
    assert second.page is not None
    assert len(fake_rest.requests) == 6
    staying.close()


def test_a_cancel_just_as_the_request_is_admitted_sends_nothing(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    cancel = Cancel()
    # A short pause makes the request queue, so its admission sends a change notice.
    refused = governor.admission("stocks", lane=object()).acquire(Cancel())
    governor.refused(refused, retry_after=1.0, message="slow down")

    def admitted() -> None:  # the change notice of the admission sets the token
        if governor.snapshot()[0].attempts == 2:
            cancel.set()

    governor.subscribe(admitted)
    with pytest.raises(WireError, match=r"^GET /v3/trades/AAPL: cancelled$"):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks", cancel=cancel)
    assert fake_rest.requests == []
    state = governor.snapshot()[0]
    # The unsent attempt holds no slot; only the refused one does.
    assert (state.attempts, state.in_flight, state.recent) == (2, 0, 1)
    rest.close()


def test_a_request_interrupted_on_the_wire_hands_its_slot_back(
    fake_rest: FakeRestServer, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)

    def interrupted(*_: object, **__: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(rest._pool, "request", interrupted)
    with pytest.raises(KeyboardInterrupt):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert governor.snapshot()[0].in_flight == 0
    rest.close()


# Logging.


def test_requests_retries_and_failures_are_logged_without_the_key(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503), _ok(request_id="r1"))
    fake_rest.reply("/v3/trades/MSFT", Reply(b"x", status=502))
    client.get(_TRADES, _AAPL, {"limit": 1}, bucket="stocks")
    with pytest.raises(WireError):
        client.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="stocks")
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
    list(client.pages(_TRADES, _AAPL, {}, bucket="stocks"))
    assert caplog.records
    assert not any(_KEY in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "headers",
    [
        {"Retry-After": "0"},
        {"Date": "Fri, 15 Jan 2027 08:00:00 GMT", "Retry-After": "Fri, 15 Jan 2027 08:00:00 GMT"},
    ],
)
def test_a_retry_after_of_now_pauses_on_the_schedule_and_gives_up_in_time(
    *, client: RestClient, fake_rest: FakeRestServer, clock: FakeClock, headers: dict[str, str]
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers=headers))
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks", patience=Patience(600.0))
    # Never a resend at once: 15, 30, then 60 s, until a pause would end past the patience.
    assert caught.value.status == 429
    assert clock.waits[:3] == [15.0, 30.0, 60.0]
    assert len(fake_rest.requests) == 12


def test_a_hook_that_raises_still_pauses_the_bucket_and_reports_the_attempt(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)

    def interrupted(bucket: str, error: WireError, *, basic: bool) -> None:
        raise KeyboardInterrupt

    rest = RestClient(
        _KEY, base_url=fake_rest.url, max_concurrency=1, governor=governor, on_refused=interrupted
    )
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": "9"}))
    with pytest.raises(KeyboardInterrupt):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    state = governor.snapshot()[0]
    assert (state.in_flight, state.refusals) == (0, 1)
    assert state.paused_until == pytest.approx(clock.time() + 9.0)
    rest.close()


class _RecordingGovernor(RequestGovernor):
    """A governor that records the lane of every request it is asked to admit."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(("stocks", "options"), clock=clock)
        self.lanes: list[object] = []

    @override
    def admission(
        self, bucket: str, *, lane: Hashable, patience: Patience = UNLIMITED
    ) -> Admission:
        self.lanes.append(lane)
        return super().admission(bucket, lane=lane, patience=patience)


def test_a_call_without_a_token_is_a_lane_of_its_own_and_a_token_shares_one(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _RecordingGovernor(clock)
    rest = _client(fake_rest, governor=governor)
    fake_rest.reply("/v3/trades/AAPL", _ok())
    shared = Cancel()
    rest.get(_TRADES, _AAPL, {}, bucket="stocks", cancel=shared)
    rest.get(_TRADES, _AAPL, {}, bucket="stocks", cancel=shared)
    rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    first, second, third, fourth = governor.lanes
    assert first is shared
    assert second is shared
    assert third is not None
    assert third is not shared
    assert third is not fourth
    rest.close()


def _caused_by(cause: Exception) -> urllib3.exceptions.HTTPError:
    """Return an urllib3 error raised from `cause`, so only the chain names the failure."""
    error = urllib3.exceptions.HTTPError("request failed")
    error.__cause__ = cause
    return error


@pytest.mark.parametrize(
    ("error", "held"),
    [
        (
            urllib3.exceptions.ProxyError(
                "Unable to connect to proxy",
                urllib3.exceptions.NewConnectionError(cast("Any", None), "connection refused"),
            ),
            0,
        ),
        (
            urllib3.exceptions.ProxyError(
                "Unable to connect to proxy", urllib3.exceptions.SSLError("handshake failed")
            ),
            0,
        ),
        (urllib3.exceptions.NewConnectionError(cast("Any", None), "connection refused"), 0),
        (_caused_by(urllib3.exceptions.NewConnectionError(cast("Any", None), "refused")), 0),
        # Without a proxy a TLS error may come after the request left: each of its four
        # attempts (it is retried as a transient failure) keeps its slot.
        (urllib3.exceptions.SSLError(ssl.SSLError("handshake failed")), 4),
    ],
)
def test_an_attempt_that_never_connected_frees_its_slot_even_behind_a_proxy(
    *,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    error: Exception,
    held: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    governor = _governor(clock)
    governor.declare({"stocks"})
    rest = _client(fake_rest, governor=governor)

    def unreachable(*_: object, **__: object) -> None:
        raise error

    monkeypatch.setattr(rest._pool, "request", unreachable)
    with pytest.raises(WireError):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    state = governor.snapshot()[0]
    assert (state.in_flight, state.recent) == (0, held)
    rest.close()


def test_an_attempt_that_failed_on_the_wire_keeps_its_slot(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", reset=True))
    with pytest.raises(WireError):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert governor.snapshot()[0].recent == 4  # each attempt may have reached Massive
    rest.close()


def test_an_interrupt_just_after_the_grant_leaves_no_permit_held(
    fake_rest: FakeRestServer, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ctrl-C landing between the grant's return and anything after it: the claim's exit
    # still reports the permit, so a pause's probe can never be lost this way.
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    real = Admission.acquire

    def interrupted(self: Admission, cancel: Cancel) -> Permit:
        real(self, cancel)
        raise KeyboardInterrupt

    monkeypatch.setattr(Admission, "acquire", interrupted)
    with pytest.raises(KeyboardInterrupt):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert fake_rest.requests == []
    state = governor.snapshot()[0]
    assert (state.attempts, state.in_flight) == (1, 0)
    rest.close()
