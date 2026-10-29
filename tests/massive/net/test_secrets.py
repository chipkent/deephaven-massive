"""Tests for `net.secrets`, and the planted-secret test of every wire client.

The planted-secret test gives the REST, WebSocket, and flat-file clients distinctive secrets,
turns on DEBUG for every logger (the library's, urllib3's, websockets', botocore's), drives
each client through the failures most likely to quote a secret back (a 401, a 429, an error
body and a non-JSON body that echo the key, an `auth_failed` reply and a status that echo it,
and an S3 403), and then searches every captured record, formatted with its
traceback, and every error's text, `repr`, and traceback for the secrets. Neither may appear.
The S3 access key id is not a secret: botocore's DEBUG log of the signed request names it.
"""

from __future__ import annotations

import json
import logging
import traceback
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._massive.net.secrets import REDACTED, redact, redacted
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath
from deephaven_massive._massive.ws.client import WsClient, WsState
from deephaven_massive._massive.ws.protocol import Status, StatusKind
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws import FakeMassiveServer

PLANTED_KEY = "PLANTED-api-key-7f3c9e51"
PLANTED_S3 = "PLANTED-s3-secret-a4d2b806"
PLANTED = (PLANTED_KEY, PLANTED_S3)

_TRADES = RestPath(
    key="/v3/trades/{stockTicker}",
    fields=(),
    params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
)
_FILE = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"


# --- redact and redacted ----------------------------------------------------------------------


def test_redact_replaces_every_occurrence_of_each_secret() -> None:
    text = f"{PLANTED_KEY} and {PLANTED_S3}, then {PLANTED_KEY} again"
    expected = f"{REDACTED} and {REDACTED}, then {REDACTED} again"
    assert redact(text, PLANTED_KEY, PLANTED_S3) == expected


def test_redact_skips_an_absent_or_empty_secret() -> None:
    # Replacing "" would put the marker between every character.
    assert redact("plain text", None, "") == "plain text"


def test_redact_returns_text_without_a_secret_unchanged() -> None:
    assert redact("HTTP 404 from GET /v3/x", PLANTED_KEY) == "HTTP 404 from GET /v3/x"


def test_redacted_returns_a_clean_error_itself() -> None:
    error = WireError("HTTP 500 from GET /v3/x", status=500)
    assert redacted(error, PLANTED_KEY) is error


@pytest.mark.parametrize("kind", [WireError, WireAuthError, WireEntitlementError])
def test_redacted_copies_an_error_that_quotes_a_secret(kind: type[WireError]) -> None:
    error = kind(
        f"HTTP 401 from GET /v3/x: unknown key {PLANTED_KEY}",
        status=401,
        host="api.massive.com",
        path="/v3/x",
        request_id="r1",
    )
    clean = redacted(error, PLANTED_KEY)
    assert type(clean) is kind
    assert clean.message == f"HTTP 401 from GET /v3/x: unknown key {REDACTED}"
    assert (clean.status, clean.host, clean.path, clean.request_id) == (
        401,
        "api.massive.com",
        "/v3/x",
        "r1",
    )


# --- the planted-secret test --------------------------------------------------------------------


def _texts(error: BaseException) -> str:
    """Return everything a person could see of an error: its text, `repr`, and traceback."""
    return "\n".join((str(error), repr(error), *traceback.format_exception(error)))


def _assert_no_secret(caplog: pytest.LogCaptureFixture, *errors: BaseException) -> None:
    """Assert that no captured record, formatted with its traceback, nor any error, holds one."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    seen = [handler.format(record) for record in caplog.records]
    seen.extend(_texts(error) for error in errors)
    assert seen  # the clients did log, so the search below means something
    for text in seen:
        for secret in PLANTED:
            assert secret not in text, text


@pytest.fixture
def every_logger_at_debug(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    # The root's level reaches every logger without one of its own: urllib3's, websockets',
    # botocore's, and the library's.
    caplog.set_level(logging.DEBUG)

    return caplog


@pytest.fixture
def rest(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    client = RestClient(PLANTED_KEY, base_url=fake_rest.url, max_concurrency=2, clock=FakeClock())
    yield client
    client.close()


@pytest.mark.parametrize(
    "reply",
    [
        Reply(envelope(status="ERROR", message=f"Unknown API Key: {PLANTED_KEY}"), status=401),
        Reply(envelope(status="ERROR", error=f"bad request for key {PLANTED_KEY}"), status=400),
        Reply(f"<html>proxy refused Bearer {PLANTED_KEY}</html>".encode(), status=502),
        Reply(f"not json, but the key: {PLANTED_KEY}".encode()),
        Reply(envelope(status="NOT_AUTHORIZED", message=f"{PLANTED_KEY} lacks this plan")),
        Reply(f"slow down, {PLANTED_KEY}".encode(), status=429),
    ],
    ids=["401", "400 envelope", "502 page", "200 not json", "NOT_AUTHORIZED", "429"],
)
def test_the_rest_client_never_shows_the_api_key(
    *,
    rest: RestClient,
    fake_rest: FakeRestServer,
    every_logger_at_debug: pytest.LogCaptureFixture,
    reply: Reply,
) -> None:
    fake_rest.reply("/v3/trades/AAPL", reply)
    with pytest.raises(WireError) as info:
        rest.get(_TRADES, {"stockTicker": "AAPL"}, {})
    # The server's words are kept, with the key replaced, so the failure still explains itself.
    assert REDACTED in info.value.message
    _assert_no_secret(every_logger_at_debug, info.value)


def test_the_websocket_client_never_shows_the_api_key(
    fake_ws_server: FakeMassiveServer, every_logger_at_debug: pytest.LogCaptureFixture
) -> None:
    fake_ws_server.auth_replies.append(("auth_failed", f"no such key: {PLANTED_KEY}"))
    states: list[tuple[WsState, object]] = []
    client = WsClient(
        fake_ws_server.url(),
        api_key=PLANTED_KEY,
        sources=(),
        on_open=lambda _generation: None,
        on_status=lambda _generation, _status: None,
        on_data=lambda *_: None,
        on_state=lambda state, detail: states.append((state, detail)),
        reconnect_backoff=1.0,
        max_reconnect_backoff=1.0,
        clock=FakeClock(),
    )
    client.start()
    assert client.join(10.0)  # a rejected key is fatal, so the client ends by itself
    [(state, failure)] = [(s, d) for s, d in states if s is WsState.FAILED]
    assert isinstance(failure, WireAuthError)
    assert failure.message.endswith(f"no such key: {REDACTED}")
    _assert_no_secret(every_logger_at_debug, failure)
    # The frame log that would show the authentication frame stays off.
    assert not [r for r in every_logger_at_debug.records if r.name.endswith(".ws.client.wire")]
    assert state is WsState.FAILED


def test_a_status_echoing_the_api_key_reaches_the_library_cleaned(
    fake_ws_server: FakeMassiveServer, every_logger_at_debug: pytest.LogCaptureFixture
) -> None:
    received: list[Status] = []
    opened: list[int] = []
    client = WsClient(
        fake_ws_server.url(),
        api_key=PLANTED_KEY,
        sources=(),
        on_open=opened.append,
        on_status=lambda _generation, status: received.append(status),
        on_data=lambda *_: None,
        on_state=lambda *_: None,
        reconnect_backoff=1.0,
        max_reconnect_backoff=1.0,
        clock=FakeClock(),
    )
    client.start()
    try:
        fake_ws_server.wait_for(lambda server: len(server.connections) == 1)
        frame = [{"ev": "status", "status": "error", "message": f"refused for {PLANTED_KEY}"}]
        fake_ws_server.wait_for(lambda server: server.connections[0].auth is not None)
        fake_ws_server.send(json.dumps(frame))
        fake_ws_server.wait_for(lambda _server: bool(received))
    finally:
        client.close()
        assert client.join(10.0)
    assert received == [Status(kind=StatusKind.REFUSED, message=f"refused for {REDACTED}")]
    _assert_no_secret(every_logger_at_debug)


def _flat_files() -> FlatFileClient:
    return FlatFileClient(
        access_key_id="AKIDPLANTED",
        secret_access_key=PLANTED_S3,
        endpoint_url="https://files.massive.com",
        bucket="flatfiles",
        max_pool=2,
        clock=FakeClock(),
    )


def test_the_flat_file_client_never_shows_the_s3_secret(
    fake_s3: FakeS3, every_logger_at_debug: pytest.LogCaptureFixture
) -> None:
    fake_s3.refuse("us_", status=403, code="SignatureDoesNotMatch", message="Rejected")
    client = _flat_files()
    try:
        with pytest.raises(WireAuthError) as info:
            client.open(_FILE)
    finally:
        client.close()
    _assert_no_secret(every_logger_at_debug, info.value)


@pytest.mark.parametrize(("status", "code"), [(403, "AccessDenied"), (503, "SlowDown")])
def test_an_s3_message_echoing_the_secret_is_cleaned(
    *, status: int, code: str, fake_s3: FakeS3, every_logger_at_debug: pytest.LogCaptureFixture
) -> None:
    # S3 never quotes the secret, but its words are the server's, so they are cleaned anyway,
    # in the error and in its whole traceback, after the retries of a transient status too.
    fake_s3.refuse("us_", status=status, code=code, message=f"denied {PLANTED_S3}")
    client = _flat_files()
    try:
        with pytest.raises(WireError) as info:
            client.open(_FILE)
    finally:
        client.close()
    assert info.value.message.endswith(f"({code}: denied {REDACTED})")
    assert info.value.__cause__ is None  # botocore's error quotes the secret: not chained
    _assert_no_secret(every_logger_at_debug, info.value)


def test_an_s3_error_without_the_secret_keeps_its_cause(fake_s3: FakeS3) -> None:
    fake_s3.refuse("us_", status=403, code="AccessDenied", message="Access Denied")
    client = _flat_files()
    try:
        with pytest.raises(WireEntitlementError) as info:
            client.open(_FILE)
    finally:
        client.close()
    assert info.value.__cause__ is not None
