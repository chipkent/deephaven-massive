"""Tests for `net.capture`: the optional sink that receives every reply the clients read.

The REST and WebSocket clients run against the fake servers with a sink installed. Each test
checks what the sink receives: every REST body, refusals and unreadable pages included, each
WebSocket frame after authentication and never the authentication exchange, every reply
cleaned of a key the server echoes, and nothing at all once the sink is removed. A sink that
raises never reaches the client and is logged once.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive import CapturedReply, set_reply_capture
from deephaven_massive._massive.net import capture
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.net.redaction import REDACTED
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.rest.governor import RequestGovernor
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath
from deephaven_massive._massive.ws.client import WsClient
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests._fakes.rest import FakeRestServer
    from tests._fakes.ws import FakeMassiveServer

PLANTED_KEY = "PLANTED-api-key-5b19c2e4"

_TRADES = RestPath(
    key="/v3/trades/{stockTicker}",
    fields=(),
    params=(
        Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),
        Param(name="limit", kind=ParamKind.INTEGER),
        Param(name="sort", kind=ParamKind.STRING),
    ),
)


@pytest.fixture
def replies() -> Iterator[list[CapturedReply]]:
    """Install a sink that keeps every reply, and remove it after the test.

    Yields:
        The replies the sink received, in order.
    """
    received: list[CapturedReply] = []
    set_reply_capture(received.append)
    yield received
    set_reply_capture(None)


@pytest.fixture
def rest(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor(("stocks",), clock=FakeClock())
    client = RestClient(PLANTED_KEY, base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield client
    client.close()


@pytest.fixture
def fresh_warning() -> Iterator[None]:
    """Let the next sink failure be the process's first, and restore the flag afterwards.

    Yields:
        Nothing.
    """
    saved = capture._hook.warned
    capture._hook.warned = False
    yield
    capture._hook.warned = saved


# --- REST -----------------------------------------------------------------------------------


def test_a_page_reaches_the_sink_with_its_request_and_its_body(
    rest: RestClient, fake_rest: FakeRestServer, replies: list[CapturedReply]
) -> None:
    body = envelope([{"price": 1.5}], request_id="abc")
    fake_rest.reply("/v3/trades/AAPL", Reply(body))
    before = time.time_ns()
    rest.get(_TRADES, {"stockTicker": "AAPL"}, {"limit": 5, "sort": "a b"}, bucket="stocks")
    [reply] = replies
    assert reply.transport == "rest"
    assert reply.host == "127.0.0.1"
    assert reply.path == "/v3/trades/AAPL"
    assert reply.query == (("limit", "5"), ("sort", "a b"))
    assert (reply.status, reply.request_id) == (200, "abc")
    assert before <= reply.received_at <= time.time_ns()
    assert reply.text == Reply(body).payload().decode()


def test_a_refusal_reaches_the_sink_before_the_client_raises(
    rest: RestClient, fake_rest: FakeRestServer, replies: list[CapturedReply]
) -> None:
    body = envelope(status="NOT_FOUND", request_id="r404", message="no such ticker")
    fake_rest.reply("/v3/trades/ZZZZZQ", Reply(body, status=404))
    with pytest.raises(WireError):
        rest.get(_TRADES, {"stockTicker": "ZZZZZQ"}, {}, bucket="stocks")
    [reply] = replies
    assert (reply.status, reply.request_id, reply.query) == (404, "r404", ())
    assert "no such ticker" in reply.text


def test_a_page_that_cannot_be_read_reaches_the_sink_without_a_request_id(
    rest: RestClient, fake_rest: FakeRestServer, replies: list[CapturedReply]
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(b"<html>not json \xff</html>"))
    with pytest.raises(WireError):
        rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")
    [reply] = replies
    assert (reply.status, reply.request_id) == (200, None)
    assert reply.text == "<html>not json �</html>"  # an invalid byte becomes U+FFFD


def test_a_key_the_server_echoes_reaches_the_sink_cleaned(
    rest: RestClient, fake_rest: FakeRestServer, replies: list[CapturedReply]
) -> None:
    echoes = [
        Reply(envelope(status="ERROR", message=f"Unknown API Key: {PLANTED_KEY}"), status=401),
        # The key with its first character spelled as a JSON escape, which decodes to it.
        Reply(f'{{"status":"OK","results":[{{"note":"key \\u0050{PLANTED_KEY[1:]}"}}]}}'.encode()),
    ]
    fake_rest.reply("/v3/trades/AAPL", *echoes)
    with pytest.raises(WireError):
        rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")
    rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")
    assert len(replies) == 2
    for reply in replies:
        assert REDACTED in reply.text
        assert PLANTED_KEY[1:] not in repr(reply)  # neither the key nor its escaped spelling


def test_nothing_reaches_a_sink_once_it_is_removed(
    rest: RestClient, fake_rest: FakeRestServer, replies: list[CapturedReply]
) -> None:
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([])))
    set_reply_capture(None)
    rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")
    assert replies == []


@pytest.mark.usefixtures("fresh_warning")
def test_a_sink_that_raises_never_fails_the_client_and_is_logged_once(
    rest: RestClient, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(_reply: CapturedReply) -> None:
        raise RuntimeError("the recorder broke")

    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([{"price": 1.5}])))
    set_reply_capture(broken)
    try:
        with caplog.at_level(logging.WARNING, logger=capture.__name__):
            pages = [rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")]
            pages.append(rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks"))
    finally:
        set_reply_capture(None)
    assert [page.rows for page in pages] == [[{"price": 1.5}]] * 2
    [record] = [r for r in caplog.records if r.name == capture.__name__]
    assert record.levelno == logging.WARNING
    assert record.exc_info is not None
    assert str(record.exc_info[1]) == "the recorder broke"


# --- WebSocket ------------------------------------------------------------------------------


def test_frames_after_authentication_reach_the_sink_and_the_handshake_never_does(
    fake_ws_server: FakeMassiveServer, replies: list[CapturedReply]
) -> None:
    client = WsClient(
        fake_ws_server.url("/stocks"),
        api_key=PLANTED_KEY,
        sources=(),
        on_open=lambda _generation: None,
        on_status=lambda _generation, _status: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
        reconnect_backoff=1.0,
        max_reconnect_backoff=1.0,
        clock=FakeClock(),
    )
    client.start()
    try:
        fake_ws_server.wait_for(lambda server: len(server.connections) == 1)
        fake_ws_server.wait_for(lambda server: server.connections[0].auth is not None)
        frame = f'[{{"ev":"status","status":"success","message":"echo {PLANTED_KEY}"}}]'
        fake_ws_server.send(frame)
        deadline = time.monotonic() + 10.0
        while not replies and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        client.close()
        assert client.join(10.0)
    [reply] = replies  # the greeting and the reply to the key are not captured
    assert (reply.transport, reply.host, reply.path) == ("ws", "127.0.0.1", "/stocks")
    assert (reply.query, reply.status, reply.request_id) == ((), None, None)
    assert reply.text == f'[{{"ev":"status","status":"success","message":"echo {REDACTED}"}}]'


def test_a_binary_frame_reaches_the_sink_as_text(replies: list[CapturedReply]) -> None:
    capture.capture_frame(host="socket.massive.com", path="/crypto", frame=b'[{"ev":"XT"}]\xfe')
    [reply] = replies
    assert reply.text == '[{"ev":"XT"}]�'
