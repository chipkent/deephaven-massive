"""Tests for the REST client, against the fake REST server over real sockets.

Time is virtual: every client gets a `FakeClock`, so retry waits are recorded, never slept.
"""

from __future__ import annotations

import logging
import math
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import urllib3
from typing_extensions import override

from deephaven_massive._massive import WireFailure
from deephaven_massive._massive.net import USER_AGENT, http
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
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    import socket
    from collections.abc import Hashable, Iterator
    from concurrent.futures import Future

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
_CONNECTION = urllib3.connection.HTTPConnection("api.massive.com")
"""The connection a refused connection names; never opened."""


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


def test_a_pool_is_built_only_when_a_request_finds_none_idle(
    fake_rest: FakeRestServer, no_proxy_env: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A pool's TLS context takes about 15 ms and 0.75 MB to build, so the client builds none
    # up front: a request builds one only when no pool is idle, and the next one reuses it.
    monkeypatch = no_proxy_env
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR", "SSLKEYLOGFILE"):
        monkeypatch.delenv(name, raising=False)
    built: list[urllib3.PoolManager] = []
    new_pool = client_module._new_pool

    def counted(proxy: str | None) -> urllib3.PoolManager:
        built.append(new_pool(proxy))
        return built[-1]

    monkeypatch.setattr(client_module, "_new_pool", counted)
    fake_rest.reply_default(_ok())
    with caplog.at_level(logging.DEBUG):
        rest = _client(fake_rest)
        assert (rest._pools, built) == ([], [])
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert rest._pools == built
    assert len(built) == 1
    assert rest._idle == built  # given back, to be taken again
    # The trust is logged once, by the client, not once per TLS context built.
    trusting = [r.getMessage() for r in caplog.records if "trusting" in r.getMessage()]
    assert trusting == [
        (
            f"REST client for {fake_rest.url}: up to 4 connection pools of one connection, each "
            "built on first need with its own TLS context trusting certifi's bundle "
            "(SSL_CERT_FILE='', SSL_CERT_DIR='', SSLKEYLOGFILE=''), through no proxy"
        )
    ]
    rest.close()


def test_a_pool_that_cannot_be_built_hands_back_its_slot_and_permit(
    fake_rest: FakeRestServer, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor, max_concurrency=1)

    def interrupted(_proxy: str | None) -> urllib3.PoolManager:
        raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(client_module, "_new_pool", interrupted)
        with pytest.raises(KeyboardInterrupt):
            rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    state = governor.snapshot()[0]
    assert (state.in_flight, state.recent, rest._pools) == (0, 0, [])
    # The one slot is free again: the next request takes it and builds the pool.
    fake_rest.reply_default(_ok())
    assert rest.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert len(rest._pools) == 1
    rest.close()


def test_each_connection_has_a_pool_that_never_retries_with_the_timeouts(
    client: RestClient,
) -> None:
    pool = client._take_pool()
    assert client._pools == [pool]
    for pool in client._pools:
        options = pool.connection_pool_kw
        retries = options["retries"]
        assert retries is client_module._NO_RETRIES
        assert (retries.total, retries.redirect) == (False, False)
        timeout = options["timeout"]
        assert (timeout.connect_timeout, timeout.read_timeout) == (10.0, 30.0)
        assert options["maxsize"] == 1
        assert options["ssl_context"] is not None


@pytest.mark.parametrize(
    ("base_url", "reason"),
    [
        ("https://api.massive.com/v3", "must name only a scheme, host, and port"),
        ("http://api.massive.com", "would send the API key in clear text"),
        ("ftp://api.massive.com", "is not an absolute http or https URL"),
    ],
)
def test_the_client_refuses_a_base_url_the_shared_rule_refuses(base_url: str, reason: str) -> None:
    with pytest.raises(ValueError, match=f"^base_url '.*' {reason}"):
        RestClient(_KEY, base_url=base_url, max_concurrency=1, governor=_governor())


def test_plain_http_is_allowed_for_loopback() -> None:
    RestClient(_KEY, base_url="http://[::1]:8080", max_concurrency=1, governor=_governor()).close()


@pytest.mark.parametrize(
    "api_key",
    ["", "s3cr3t\n", " s3cr3t", "s3cr3t ", "s3c r3t", "s3c\tr3t", "s3cr\u00e9t", "s3cr3t\r\nX: y"],
)
def test_an_api_key_that_cannot_be_a_header_is_refused_without_being_shown(
    api_key: str,
) -> None:
    with pytest.raises(ValueError, match=r"^api_key must be non-empty visible ASCII") as caught:
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


def test_a_gzip_bomb_is_refused_at_the_cap_and_never_retried(
    *,
    client: RestClient,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", 1024 * 1024)
    monkeypatch.setattr(client_module, "_READ_CHUNK", 64 * 1024)
    bomb = Reply(b"\0" * (64 * 1024 * 1024), gzip=True)  # 64 MiB in about 64 KiB of gzip
    assert len(bomb.payload()) < 128 * 1024
    fake_rest.reply("/v3/trades/AAPL", bomb, _ok())
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    error = caught.value
    assert error.status == 200
    assert (error.host, error.path) == ("127.0.0.1", "/v3/trades/AAPL")
    assert "more than 1048576 bytes" in error.message
    assert _KEY not in str(error)
    assert clock.waits == []  # never retried
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    # The next request is answered at its first attempt: the bomb's unread rest never
    # stands in for its response.
    assert len(fake_rest.requests) == 2
    assert clock.waits == []


def test_an_abandoned_body_never_stands_in_for_the_next_reply(
    *,
    client: RestClient,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", 1024 * 1024)
    monkeypatch.setattr(client_module, "_READ_CHUNK", 4 * 1024)
    tail = threading.Event()
    bomb = Reply(b"\0" * (64 * 1024 * 1024), gzip=True, hold=tail, hold_after=32 * 1024)
    fake_rest.reply("/v3/trades/AAPL", bomb, _ok())
    try:
        with pytest.raises(WireError, match="more than 1048576 bytes"):
            client.get(_TRADES, _AAPL, {}, bucket="stocks")
        # The bomb's tail has not arrived, so its connection looks idle to the pool; reusing
        # it would read the tail as the next reply.
        assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
        assert clock.waits == []
    finally:
        tail.set()


def test_an_oversized_429_still_pauses_its_bucket(
    fake_rest: FakeRestServer, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", 1024)
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    fake_rest.reply("/v3/trades/AAPL", Reply(b"x" * 4096, status=429, headers={"Retry-After": "5"}))
    with pytest.raises(WireError, match="more than 1024 bytes") as caught:
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert caught.value.status == 429
    state = governor.snapshot()[0]
    assert (state.refusals, state.in_flight) == (1, 0)
    assert state.paused_until is not None
    rest.close()


def test_an_oversized_reply_ends_the_run_of_refusals_as_any_answer_does(
    *,
    client: RestClient,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", 1024)
    limited = Reply({"message": "slow down"}, status=429)
    fake_rest.reply("/v3/trades/AAPL", limited, Reply(b"x" * 4096), limited, _ok())
    with pytest.raises(WireError, match="more than 1024 bytes"):
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    # Massive answered between the two refusals, so the second pause starts the schedule over.
    assert clock.waits == [15.0, 15.0]


def test_a_body_of_exactly_the_cap_is_read(
    client: RestClient, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    reply = Reply(envelope([{"i": 3}]), gzip=True)
    size = len(Reply(envelope([{"i": 3}])).payload())
    monkeypatch.setattr(client_module, "_READ_CHUNK", 7)
    fake_rest.reply("/v3/trades/AAPL", reply)
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", size)
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 3}]
    monkeypatch.setattr(client_module, "MAX_BODY_BYTES", size - 1)
    with pytest.raises(WireError, match=f"more than {size - 1} bytes"):
        client.get(_TRADES, _AAPL, {}, bucket="stocks")


def test_a_body_that_does_not_decompress_is_never_retried_and_is_logged(
    *,
    client: RestClient,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    corrupt = Reply(b"not gzip at all", headers={"Content-Encoding": "gzip"})
    fake_rest.reply("/v3/trades/AAPL", corrupt, _ok())
    with pytest.raises(WireError, match="does not decompress") as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert caught.value.status == 200
    assert caught.value.kind is WireFailure.UNREADABLE  # the body arrived whole but cannot be read
    assert "not gzip" not in caught.value.message  # the body itself is never quoted
    # The call that raises is what the user sees, so the client records the reply at DEBUG.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert f"{caught.value.message} (HTTP 200); not retried" in caplog.messages
    assert not any("not gzip" in r.getMessage() for r in caplog.records)
    assert clock.waits == []
    assert len(fake_rest.requests) == 1


def test_a_body_that_breaks_off_is_retried(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    cut = Reply(envelope([{"i": 9}] * 100), cut_after=50)
    fake_rest.reply("/v3/trades/AAPL", cut, _ok())
    assert client.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert clock.waits == pytest.approx([0.55])
    assert len(fake_rest.requests) == 2


def test_a_429_whose_body_breaks_off_is_still_a_refusal(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    cut = Reply({"message": "slow down " * 50}, status=429, cut_after=20)
    fake_rest.reply("/v3/trades/AAPL", cut, _ok())
    assert rest.get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]
    assert clock.waits == [15.0]  # the refusal's pause, not a transient retry's
    assert governor.snapshot()[0].refusals == 1
    rest.close()


def test_a_429_whose_body_does_not_decompress_is_a_refusal_that_fails(
    fake_rest: FakeRestServer, clock: FakeClock
) -> None:
    governor = _governor(clock)
    rest = _client(fake_rest, governor=governor)
    corrupt = Reply(b"not gzip", status=429, headers={"Content-Encoding": "gzip"})
    fake_rest.reply("/v3/trades/AAPL", corrupt)
    with pytest.raises(WireError, match="does not decompress") as caught:
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert caught.value.status == 429
    state = governor.snapshot()[0]
    assert (state.refusals, state.in_flight) == (1, 0)
    assert len(fake_rest.requests) == 1
    rest.close()


@pytest.mark.parametrize("escaped", [False, True], ids=["plain", "escaped"])
def test_a_streamed_body_is_cleaned_of_the_key_before_any_error_or_log(
    *,
    client: RestClient,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    escaped: bool,
) -> None:
    caplog.set_level(logging.DEBUG)
    # Small reads, so the key straddles the pieces the body arrives in.
    monkeypatch.setattr(client_module, "_READ_CHUNK", 5)
    spelled = "\\u0073" + _KEY[1:] if escaped else _KEY  # `s` as a JSON escape
    body = f'{{"status":"ERROR","request_id":"r1","message":"bad key {spelled}"}}'.encode()
    fake_rest.reply("/v3/trades/AAPL", Reply(body, status=400, gzip=True))
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert "bad key ***" in caught.value.message
    every = [caught.value.message, *(r.getMessage() for r in caplog.records)]
    assert not any(_KEY in text or _KEY[1:] in text for text in every)


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
    assert all("s%40cret" not in str(pool.proxy) for pool in rest._pools)
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
    assert caught.value.kind is WireFailure.LOCAL  # the proxy setting on this machine is the fix
    assert [r.method for r in fake_rest.requests] == ["CONNECT"]
    assert (tunnelled.requests, clock.waits) == ([], [])


def test_a_proxy_reached_over_https_is_verified_like_massive(
    no_proxy_env: pytest.MonkeyPatch,
) -> None:
    no_proxy_env.setenv("HTTPS_PROXY", "https://proxy.corp:3129")
    rest = RestClient(
        _KEY, base_url="https://api.massive.test", max_concurrency=1, governor=_governor()
    )
    pool = rest._take_pool()
    assert type(pool).__name__ == "ProxyManager"
    proxy_context = pool.connection_pool_kw["_proxy_config"].ssl_context
    assert proxy_context is not None
    assert proxy_context.verify_mode is ssl.CERT_REQUIRED
    assert proxy_context.check_hostname
    # One context serves both legs, so the proxy and Massive are verified against one trust
    # store; the one thread holding the slot opens them one after the other.
    assert proxy_context is pool.connection_pool_kw["ssl_context"]
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
    assert caught.value.kind is WireFailure.LOCAL  # SSL_CERT_FILE, on this machine, is the fix
    assert clock.waits == []
    assert tls_server.requests == []


def test_ssl_cert_file_supplies_the_trusted_certificates(
    tls_server: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", str(_TLS / "tls_cert.pem"))
    tls_server.reply("/v3/trades/AAPL", _ok())
    assert _client(tls_server).get(_TRADES, _AAPL, {}, bucket="stocks").rows == [{"i": 1}]


def test_an_unreadable_ssl_cert_dir_warns_once_when_the_client_is_built(
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # OpenSSL skips a certificate directory it cannot list without a word, so the client says
    # so when it is built: once, however many pools it has, not once per pool's TLS context.
    missing = tmp_path / "missing"
    monkeypatch.setenv("SSL_CERT_DIR", str(missing))
    monkeypatch.setattr(http, "_WARNED", set())
    examined: list[str] = []
    examine = http._directories_identity

    def counted(cert_dir: str, *, configured: bool) -> tuple[object, ...]:
        examined.append(cert_dir)
        return examine(cert_dir, configured=configured)

    monkeypatch.setattr(http, "_directories_identity", counted)
    with caplog.at_level(logging.WARNING, logger=http.__name__):
        _client(fake_rest, max_concurrency=3).close()
    assert examined == [str(missing)]
    assert [record.levelno for record in caplog.records] == [logging.WARNING]
    assert f"cannot list the certificate directory {str(missing)!r}" in caplog.text


def test_no_two_requests_in_flight_open_connections_on_one_ssl_context(
    tls_server: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # urllib3 writes to a connection's SSLContext as it opens it, while OpenSSL may be copying
    # that state for another thread's handshake with the GIL released, which corrupts the
    # handshake or crashes. So each connection slot has a pool of its own, with one connection
    # and its own context, and the requests in flight at once never share a context.
    monkeypatch.setenv("SSL_CERT_FILE", str(_TLS / "tls_cert.pem"))
    # Every reply closes its connection, so every request opens a connection and handshakes.
    tls_server.reply_default(Reply(_ok().body, headers={"Connection": "close"}))
    rest = _client(tls_server, max_concurrency=3)
    handshakes: list[int] = []  # the context of each connection the client opened
    handshakes_lock = threading.Lock()  # the client's threads append to it at once
    wrap_socket = ssl.SSLContext.wrap_socket

    def recorded(
        self: ssl.SSLContext,
        sock: socket.socket,
        *,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | None = None,
    ) -> ssl.SSLSocket:
        # urllib3 and the fake server's accepting socket pass only these, by keyword.
        if not server_side:  # the fake's own handshakes are not the client's
            with handshakes_lock:
                handshakes.append(id(self))
        return wrap_socket(
            self,
            sock,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
            suppress_ragged_eofs=suppress_ragged_eofs,
            server_hostname=server_hostname,
        )

    monkeypatch.setattr(ssl.SSLContext, "wrap_socket", recorded)
    ready = threading.Condition()
    hold = threading.Event()
    in_flight = 0

    def held(_received: object) -> None:
        nonlocal in_flight
        with ready:
            in_flight += 1
            ready.notify_all()
        hold.wait(10)

    tls_server.on_request(held)
    tickers = [{"stockTicker": f"T{i}"} for i in range(6)]
    with ThreadPoolExecutor(3) as pool:
        calls = [pool.submit(rest.get, _TRADES, ticker, {}, bucket="stocks") for ticker in tickers]
        with ready:
            assert ready.wait_for(lambda: in_flight == 3, 10)
        assert rest._idle == []  # every slot's pool is held
        assert len(rest._pools) == 3  # one built per slot, and no more
        with handshakes_lock:
            held_at_once = set(handshakes)
        hold.set()
        assert [call.result(10).rows for call in calls] == [[{"i": 1}]] * 6
    rest.close()
    # The three requests held at once each opened their connection on a context of its own,
    assert len(held_at_once) == 3
    # every connection was opened on one of the pools' three contexts, one per pool,
    contexts = {id(pool.connection_pool_kw["ssl_context"]) for pool in rest._pools}
    assert len(contexts) == 3
    assert (len(handshakes), set(handshakes)) == (6, contexts)
    # and every pool was given back, to be taken again.
    assert sorted(map(id, rest._idle)) == sorted(map(id, rest._pools))


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
    assert caught.value.kind is WireFailure.NO_ANSWER  # no answer, so a later request may get one
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


def test_a_429_logs_once_at_info_and_the_retry_logs_its_wait(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    limited = Reply({"message": "slow down"}, status=429, headers={"Retry-After": "7"})
    fake_rest.reply("/v3/trades/AAPL", limited, _ok())
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    infos = [r.getMessage() for r in caplog.records if r.levelno >= logging.INFO]
    assert infos == [
        (
            "stocks: paused 7.0 s (Retry-After 7 s) after HTTP 429; the refusal: HTTP 429 from GET "
            "/v3/trades/AAPL on 127.0.0.1: slow down"
        )
    ]
    url = f"{fake_rest.url}/v3/trades/AAPL"
    gets = [m for m in caplog.messages if m.startswith(f"GET {url} -> ")]
    assert gets[0].startswith(f"GET {url} -> 429 (request_id None) in 0.000 s, waited 0.0 s ")
    assert gets[1] == (
        f"GET {url} -> 200 (request_id req-1): 1 rows in 0.000 s, waited 7.0 s for a permit"
    )


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
    # server always sends a Date; `net.retry.http_date_seconds` reads none at all as None.)
    headers = {"Date": "not a date", "Retry-After": "Fri, 15 Jan 2027 08:00:12 -0000"}
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=429, headers=headers), _ok())
    client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [12.0]


@pytest.mark.parametrize("header", ["86400", "9" * 400, "Fri, 31 Dec 9999 23:59:59 GMT"])
def test_a_retry_after_beyond_ten_minutes_pauses_ten_minutes(
    client: RestClient, fake_rest: FakeRestServer, clock: FakeClock, *, header: str
) -> None:
    fake_rest.reply(
        "/v3/trades/AAPL", Reply(b"", status=429, headers={"Retry-After": header}), _ok()
    )
    client.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert clock.waits == [600.0]


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
        {"request_id": "r", "message": "quota"}, status=429, headers={"Retry-After": "301"}
    )
    fake_rest.reply("/v3/trades/AAPL", refusal)
    with pytest.raises(WireError) as caught:
        client.get(_TRADES, _AAPL, {}, bucket="stocks", patience=Patience(300.0))
    assert str(caught.value).startswith(
        "Massive is refusing stocks requests with HTTP 429 (latest refusal: HTTP 429 from GET "
        "/v3/trades/AAPL on 127.0.0.1 (request_id r): quota). Its next request, a probe, goes at "
        "2027-01-15T08:05:01+00:00 ("
    )
    assert "waits out at most 300 s of pauses and has spent 0 s of its patience" in str(
        caught.value
    )
    assert caught.value.status == 429
    assert (caught.value.host, caught.value.path) == ("127.0.0.1", "/v3/trades/AAPL")
    # Another call with the same patience fails at once, without being sent.
    fake_rest.reply("/v3/trades/MSFT", _ok())
    with pytest.raises(WireError, match="Its next request, a probe, goes at") as again:
        client.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="stocks", patience=Patience(300.0))
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
    with pytest.raises(WireError, match="the REST client is closed") as closed:
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert closed.value.kind is WireFailure.STOPPED  # no answer was asked for
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
    assert call.error.kind is WireFailure.STOPPED
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

    monkeypatch.setattr(urllib3.PoolManager, "request", interrupted)
    with pytest.raises(KeyboardInterrupt):
        rest.get(_TRADES, _AAPL, {}, bucket="stocks")
    assert governor.snapshot()[0].in_flight == 0
    assert sorted(map(id, rest._idle)) == sorted(map(id, rest._pools))  # its pool is back
    rest.close()


# Logging.


def test_a_200_whose_page_cannot_be_read_is_logged_with_its_body(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(b"<html>not json</html>"))
    with (
        caplog.at_level(logging.DEBUG, logger=client_module.__name__),
        pytest.raises(WireError) as info,
    ):
        client.get(_TRADES, _AAPL, {}, bucket="stocks")
    [line] = [m for m in caplog.messages if " -> 200 " in m]
    assert line.startswith(
        f"GET {fake_rest.url}/v3/trades/AAPL -> 200 in 0.000 s, waited 0.0 s for a permit, but "
    )
    assert line.endswith(f"cannot be read ({info.value}); body: <html>not json</html>")


def test_requests_retries_and_failures_are_logged_without_the_key(
    client: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    body = '{"status":"ERROR","error":"' + "e" * 2500 + '"}'
    fake_rest.reply("/v3/trades/AAPL", Reply(b"", status=503), _ok(request_id="r1"))
    fake_rest.reply("/v3/trades/MSFT", Reply(body.encode(), status=502))
    client.get(_TRADES, _AAPL, {"limit": 1}, bucket="stocks")
    with pytest.raises(WireError):
        client.get(_TRADES, {"stockTicker": "MSFT"}, {}, bucket="stocks")
    ours = [r for r in caplog.records if r.name == client_module.__name__]
    messages = [(r.levelno, r.getMessage()) for r in ours]
    url = fake_rest.url
    # Each attempt names the whole URL; a failed one quotes its body, up to 2,000 characters.
    assert (
        logging.DEBUG,
        (
            f"GET {url}/v3/trades/AAPL?limit=1 -> 503 (request_id None) in 0.000 s, waited 0.0 s "
            "for a permit; body: "
        ),
    ) in messages
    assert (
        logging.DEBUG,
        (
            f"GET {url}/v3/trades/MSFT -> 502 (request_id None) in 0.000 s, waited 0.0 s for a "
            f"permit; body: {body[:2000]}"
        ),
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
        (
            f"GET {url}/v3/trades/AAPL?limit=1 -> 200 (request_id r1): 1 rows in 0.000 s, waited "
            "0.0 s for a permit"
        ),
    ) in messages
    # The error quotes only the envelope's message, cut to 200 characters.
    assert (
        logging.DEBUG,
        f"HTTP 502 from GET /v3/trades/MSFT on 127.0.0.1: {'e' * 200}; giving up after 4 attempts",
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
                urllib3.exceptions.NewConnectionError(_CONNECTION, "connection refused"),
            ),
            0,
        ),
        (
            urllib3.exceptions.ProxyError(
                "Unable to connect to proxy", urllib3.exceptions.SSLError("handshake failed")
            ),
            0,
        ),
        (urllib3.exceptions.NewConnectionError(_CONNECTION, "connection refused"), 0),
        (_caused_by(urllib3.exceptions.NewConnectionError(_CONNECTION, "refused")), 0),
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

    monkeypatch.setattr(urllib3.PoolManager, "request", unreachable)
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


def test_overlapping_fan_outs_share_the_pool_without_discarding_connections(
    fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Two fan-outs of two requests each, on a client of two connections: two requests reach
    # Massive at once while the others wait for a connection, so urllib3 never opens an extra
    # connection only to discard it ("Connection pool is full").
    rest = _client(fake_rest, max_concurrency=2)
    fake_rest.reply_default(_ok())
    ready = threading.Condition()
    hold = threading.Event()
    in_flight = most = 0

    def held(_received: object) -> None:
        nonlocal in_flight, most
        with ready:
            in_flight += 1
            most = max(most, in_flight)
            ready.notify_all()
        hold.wait(10)
        with ready:
            in_flight -= 1

    fake_rest.on_request(held)
    waiting = threading.Event()
    wait = rest._slots._ready.wait

    def noted(timeout: float | None = None) -> bool:
        waiting.set()
        return wait(timeout)

    rest._slots._ready.wait = noted  # type: ignore[method-assign]  # the point: see the wait
    caplog.set_level(logging.WARNING, logger="urllib3")

    def fan_out(pool: ThreadPoolExecutor, first: int) -> list[Future[Page]]:
        tickers = [{"stockTicker": f"T{i}"} for i in (first, first + 1)]
        return [pool.submit(rest.get, _TRADES, ticker, {}, bucket="stocks") for ticker in tickers]

    with ThreadPoolExecutor(2) as one, ThreadPoolExecutor(2) as other:
        calls = fan_out(one, 0) + fan_out(other, 2)
        with ready:
            assert ready.wait_for(lambda: in_flight == 2, 10)
        assert waiting.wait(10)  # a third request waits for a connection
        hold.set()
        pages = [call.result(10) for call in calls]
    rest.close()
    assert [page.rows for page in pages] == [[{"i": 1}]] * 4
    assert most == 2
    assert [r.getMessage() for r in caplog.records if "pool is full" in r.getMessage()] == []
