"""Tests for `net.redaction`, and the planted-secret test of every wire client.

The planted-secret test gives the REST, WebSocket, and flat-file clients distinctive secrets,
turns on DEBUG for every logger (the library's, urllib3's, websockets', botocore's), drives
each client through the failures most likely to quote a secret back (a 401, a 429, an error
body, a non-JSON body, and a row that echo the key, an `auth_failed` reply, a status, and a data
row that echo it, and S3 errors), each also with the key spelled by JSON escapes, and then
searches every captured record, formatted with its traceback, and every error's text, `repr`,
and traceback for the secrets. Neither may appear. The S3 access key id is an identifier, not a
secret, so the test does not look for it.
"""

from __future__ import annotations

import json
import logging
import re
import time
import traceback
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._massive.net.redaction import REDACTED, Redactor, redact
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.rest.governor import RequestGovernor
from deephaven_massive._massive.spec.types import (
    Field,
    Param,
    ParamKind,
    RestPath,
    Source,
    Transport,
    WireType,
)
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
# The key with its first character spelled as a JSON escape: no byte search finds it, but it
# decodes to the key itself.
ESCAPED_KEY = "\\u0050" + PLANTED_KEY[1:]

_TRADES = RestPath(
    key="/v3/trades/{stockTicker}",
    fields=(),
    params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
)
_FILE = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"


# --- redact and Redactor --------------------------------------------------------------------


def test_redact_replaces_every_occurrence_of_each_secret() -> None:
    text = f"{PLANTED_KEY} and {PLANTED_S3}, then {PLANTED_KEY} again"
    expected = f"{REDACTED} and {REDACTED}, then {REDACTED} again"
    assert redact(text, PLANTED_KEY, PLANTED_S3) == expected


def test_redact_skips_an_absent_or_empty_secret() -> None:
    # Replacing "" would put the marker between every character.
    assert redact("plain text", None, "") == "plain text"


def test_redact_returns_text_without_a_secret_unchanged() -> None:
    text = "HTTP 404 from GET /v3/x"
    assert redact(text, PLANTED_KEY) is text


def test_the_redactor_replaces_each_secret_and_leaves_clean_data_uncopied() -> None:
    data = f'{{"message":"{PLANTED_KEY}","other":"{PLANTED_S3}"}}'.encode()
    cleaned = Redactor(PLANTED_KEY, PLANTED_S3, None, "").received(data)
    assert cleaned == b'{"message":"***","other":"***"}'
    clean = b'{"status":"OK"}'
    assert Redactor(PLANTED_KEY).received(clean) is clean
    assert Redactor().received(clean) is clean  # no secret to look for
    text = '[{"ev":"status","message":"fine"}]'
    assert Redactor(PLANTED_KEY).received(text) is text


@pytest.mark.parametrize("kind", [bytes, str])
def test_the_redactor_finds_a_secret_spelled_by_json_escapes(kind: type) -> None:
    raw = f'{{"error":"bad key {ESCAPED_KEY}","k\\u0065y":["{ESCAPED_KEY}",1]}}'
    data = raw.encode() if kind is bytes else raw
    cleaned = Redactor(PLANTED_KEY).received(data)
    assert type(cleaned) is kind
    assert json.loads(cleaned) == {"error": "bad key ***", "key": ["***", 1]}


def test_the_redactor_passes_on_escapes_that_spell_no_secret_character() -> None:
    # The escape spells `é`, a character of no secret: the body is not searched further.
    data = b'{"text":"caf\\u00e9","n":1}'
    assert Redactor(PLANTED_KEY).received(data) is data


@pytest.mark.parametrize(
    ("secret", "spelled"),
    [
        ("a/b-key", "a\\/b-key"),  # the short escape of `/`
        ("key-\U0001f600", "key-\\ud83d\\ude00"),  # a surrogate pair, beyond the BMP
        ("Key-1", "\\u004Bey-1"),  # upper-case hex digits
    ],
)
def test_the_redactor_finds_each_escape_a_secret_character_has(secret: str, spelled: str) -> None:
    data = f'{{"echo":"{spelled}"}}'.encode()
    assert json.loads(Redactor(secret).received(data)) == {"echo": REDACTED}


def test_the_redactor_leaves_escapes_without_a_secret_alone() -> None:
    data = b'{"text":"a \\"quoted\\" word","n":1}'
    assert Redactor(PLANTED_KEY).received(data) is data


def test_an_escape_that_spells_no_secret_is_left_alone_in_text_that_is_not_json() -> None:
    data = b"<html>a \\u0050 page</html>"
    assert Redactor(PLANTED_KEY).received(data) is data


@pytest.mark.parametrize(
    "data",
    [
        f'{{"status":"ERROR","error":"bad key {ESCAPED_KEY}'.encode(),  # cut short
        f"<html>proxy refused Bearer {ESCAPED_KEY}</html>".encode(),
        f"<p>{PLANTED_KEY[:3]}\\u004e{PLANTED_KEY[4:]}</p>".encode(),  # `N` as an escape
    ],
    ids=["truncated envelope", "html page", "escape mid-key, lower-case hex"],
)
def test_the_redactor_finds_an_escaped_secret_in_text_that_is_not_json(data: bytes) -> None:
    cleaned = Redactor(PLANTED_KEY).received(data)
    assert REDACTED.encode() in cleaned
    assert _unescaped(cleaned.decode()).count(PLANTED_KEY) == 0


def test_a_secret_with_a_quote_is_found_in_its_json_escaped_form() -> None:
    odd_key = 'key"with\\odd'
    encoded = json.dumps({"echo": odd_key}).encode()
    assert json.loads(Redactor(odd_key).received(encoded)) == {"echo": REDACTED}


def test_a_key_beyond_the_bmp_is_found_as_itself_in_text_and_bytes() -> None:
    emoji_key = "key-\U0001f600"
    redactor = Redactor(emoji_key)
    assert redactor.received(f"a {emoji_key} b") == f"a {REDACTED} b"
    assert redactor.received(f"a {emoji_key} b".encode()) == f"a {REDACTED} b".encode()


@pytest.mark.parametrize("kind", [bytes, str])
def test_a_key_of_backslashes_is_searched_in_linear_time(kind: type) -> None:
    # Every spelling of a backslash starts with one, so a pattern that could backtrack into a
    # character's spelling took close to a minute for this key against 2 KB of backslashes.
    slashes_key = "\\" * 20 + "X"
    raw = ("\\" * 44 + "Y") * 50
    data = raw.encode() if kind is bytes else raw
    started = time.perf_counter()
    assert Redactor(slashes_key).received(data) is data
    assert time.perf_counter() - started < 1.0


def test_a_key_of_backslashes_is_found_as_itself_and_as_json() -> None:
    slashes_key = "a\\\\b"
    redactor = Redactor(slashes_key)
    assert redactor.received(f"<p>{slashes_key}</p>".encode()) == b"<p>***</p>"  # not JSON
    echoed = json.dumps({"echo": slashes_key}).encode()  # each backslash written as two
    assert json.loads(redactor.received(echoed)) == {"echo": REDACTED}
    spelled = b'{"echo":"a\\u005c\\\\b"}'  # one backslash as a u escape, one as a short one
    assert json.loads(redactor.received(spelled)) == {"echo": REDACTED}


# --- the planted-secret test --------------------------------------------------------------------


def _texts(error: BaseException) -> str:
    """Return everything a person could see of an error: its text, `repr`, and traceback."""
    return "\n".join((str(error), repr(error), *traceback.format_exception(error)))


def _unescaped(text: str) -> str:
    """Return text with every JSON `u` escape decoded, so an escaped secret reads as itself."""
    return re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), text)


def _assert_no_secret(caplog: pytest.LogCaptureFixture, *errors: BaseException) -> None:
    """Assert that no captured record, formatted with its traceback, nor any error, holds one.

    Each text is searched as it is and with its JSON escapes decoded, so a secret spelled by
    escapes counts as shown.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    seen = [handler.format(record) for record in caplog.records]
    seen.extend(_texts(error) for error in errors)
    assert seen  # the clients did log, so the search below means something
    for text in seen:
        for secret in PLANTED:
            assert secret not in text, text
            assert secret not in _unescaped(text), text


@pytest.fixture
def every_logger_at_debug(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    # The root's level reaches every logger without one of its own: urllib3's, websockets',
    # botocore's, and the library's.
    caplog.set_level(logging.DEBUG)
    return caplog


def _escaped_envelope(status: str, key: str, words: str) -> bytes:
    # An envelope whose message spells the key with a JSON escape, as raw bytes.
    return f'{{"status":"{status}","request_id":"x","{key}":"{words}{ESCAPED_KEY}"}}'.encode()


@pytest.fixture
def rest(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor(("stocks",), clock=FakeClock())
    client = RestClient(PLANTED_KEY, base_url=fake_rest.url, max_concurrency=2, governor=governor)
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
        Reply(_escaped_envelope("ERROR", "message", "Unknown API Key: "), status=401),
        Reply(_escaped_envelope("ERROR", "error", "bad request for key "), status=400),
        Reply(_escaped_envelope("NOT_AUTHORIZED", "message", "no plan for ")),
        Reply(_escaped_envelope("ERROR", "error", "slow down, "), status=429),
        Reply(_escaped_envelope("ERROR", "error", "bad request for key ")[:-1], status=400),
        Reply(f"<html>proxy refused Bearer {ESCAPED_KEY}</html>".encode(), status=502),
    ],
    ids=[
        "401",
        "400 envelope",
        "502 page",
        "200 not json",
        "NOT_AUTHORIZED",
        "429",
        "401 escaped",
        "400 escaped",
        "NOT_AUTHORIZED escaped",
        "429 escaped",
        "400 escaped, truncated",
        "502 page escaped",
    ],
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
        # A 429 fails once its pauses outlast the call's patience, quoting the refusal.
        rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks", give_up_at=600.0)
    # The server's words are kept, with the key replaced, so the failure still explains itself.
    assert REDACTED in info.value.message
    _assert_no_secret(every_logger_at_debug, info.value)


@pytest.mark.parametrize("spelled", [PLANTED_KEY, ESCAPED_KEY], ids=["plain", "escaped"])
def test_a_page_whose_rows_echo_the_api_key_is_cleaned_before_it_is_decoded(
    *,
    rest: RestClient,
    fake_rest: FakeRestServer,
    every_logger_at_debug: pytest.LogCaptureFixture,
    spelled: str,
) -> None:
    body = f'{{"status":"OK","request_id":"x","results":[{{"note":"key {spelled}"}}]}}'
    fake_rest.reply("/v3/trades/AAPL", Reply(body.encode()))
    page = rest.get(_TRADES, {"stockTicker": "AAPL"}, {}, bucket="stocks")
    assert page.rows == [{"note": f"key {REDACTED}"}]
    _assert_no_secret(every_logger_at_debug)


@pytest.mark.parametrize("spelled", [PLANTED_KEY, ESCAPED_KEY], ids=["plain", "escaped"])
def test_the_websocket_client_never_shows_the_api_key(
    *,
    fake_ws_server: FakeMassiveServer,
    every_logger_at_debug: pytest.LogCaptureFixture,
    spelled: str,
) -> None:
    reply = f'[{{"ev":"status","status":"auth_failed","message":"no such key: {spelled}"}}]'
    fake_ws_server.auth_replies.append(reply.encode())
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


@pytest.mark.parametrize("spelled", [PLANTED_KEY, ESCAPED_KEY], ids=["plain", "escaped"])
def test_a_frame_echoing_the_api_key_reaches_the_library_cleaned(
    *,
    fake_ws_server: FakeMassiveServer,
    every_logger_at_debug: pytest.LogCaptureFixture,
    spelled: str,
) -> None:
    received: list[Status] = []
    rows: list[dict[str, object]] = []
    trades = Source(
        key="stocks/T",
        transport=Transport.WS,
        fields=(Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),),
    )
    client = WsClient(
        fake_ws_server.url(),
        api_key=PLANTED_KEY,
        sources=(trades,),
        on_open=lambda _generation: None,
        on_status=lambda _generation, status: received.append(status),
        on_data=lambda _generation, _channel, data, _rejects, _drift: rows.extend(data),
        on_state=lambda *_: None,
        reconnect_backoff=1.0,
        max_reconnect_backoff=1.0,
        clock=FakeClock(),
    )
    client.start()
    try:
        fake_ws_server.wait_for(lambda server: len(server.connections) == 1)
        frame = (
            f'[{{"ev":"status","status":"error","message":"refused for {spelled}"}},'
            f'{{"ev":"T","sym":"{spelled}"}}]'
        )
        fake_ws_server.wait_for(lambda server: server.connections[0].auth is not None)
        fake_ws_server.send(frame)
        fake_ws_server.wait_for(lambda _server: bool(received) and bool(rows))
    finally:
        client.close()
        assert client.join(10.0)
    assert received == [Status(kind=StatusKind.REFUSED, message=f"refused for {REDACTED}")]
    assert rows == [{"Ticker": REDACTED}]
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


def test_the_redactor_passes_on_a_body_whose_escapes_spell_no_whole_secret() -> None:
    # The escape spells `P`, a character of the key, so the body is searched; nothing changes.
    data = b'{"text":"\\u0050ython"}'
    assert Redactor(PLANTED_KEY).received(data) is data
