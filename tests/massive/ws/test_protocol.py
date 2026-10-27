from __future__ import annotations

import json
import logging
import re
import ssl
from datetime import UTC, date, datetime
from email.utils import format_datetime
from typing import TYPE_CHECKING

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import InvalidMessage, InvalidStatus, InvalidURI
from websockets.http11 import Response

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import (
    DriftReport,
    Field,
    Source,
    Transport,
    WireType,
    WireUnit,
)
from deephaven_massive._massive.ws import protocol
from deephaven_massive._massive.ws.protocol import (
    MAX_TOPICS_PER_FRAME,
    Data,
    FrameDecoder,
    Status,
    StatusKind,
    auth_frame,
    classify_failure,
    handshake_status,
    subscription_frames,
)

if TYPE_CHECKING:
    from collections.abc import Callable

HOST, PATH = "socket.massive.com", "/stocks"

TRADES = Source(
    key="stocks/T",
    transport=Transport.WS,
    fields=(
        Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
        Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE),
        Field(wire_key="s", column="Size", wire_type=WireType.LONG),
        Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.MS),
        Field(wire_key="c", column="Conditions", wire_type=WireType.INT_ARRAY),
    ),
)
# One field of every other type, for the conversion tests.
EVERYTHING = Source(
    key="stocks/E",
    transport=Transport.WS,
    fields=(
        Field(wire_key="b", column="Flag", wire_type=WireType.BOOL),
        Field(wire_key="d", column="Day", wire_type=WireType.DATE),
        Field(wire_key="i", column="When", wire_type=WireType.ISO_INSTANT),
        Field(wire_key="j", column="Extra", wire_type=WireType.JSON_TEXT),
    ),
)
FOREX = Source(
    key="forex/CA",
    transport=Transport.WS,
    fields=(
        Field(wire_key="pair", column="Pair", wire_type=WireType.STRING),
        Field(wire_key="c", column="Close", wire_type=WireType.DOUBLE),
    ),
)


@pytest.fixture
def decoder() -> FrameDecoder:
    return FrameDecoder([TRADES, EVERYTHING], host=HOST, path=PATH)


def _frame(*items: object) -> str:
    return json.dumps(list(items))


def _decode_one(decoder: FrameDecoder, item: dict[str, object]) -> Data:
    [data] = decoder.decode(_frame(item))
    assert isinstance(data, Data)
    return data


# --- control frames -----------------------------------------------------------------------------


def test_auth_frame_is_compact_text() -> None:
    assert auth_frame('k"1') == '{"action":"auth","params":"k\\"1"}'


def test_subscription_frames_chunk_at_1000_topics_in_order() -> None:
    topics = [f"T.S{i}" for i in range(2 * MAX_TOPICS_PER_FRAME + 1)]
    frames = subscription_frames("subscribe", topics)
    assert all(isinstance(f, str) for f in frames)
    decoded = [json.loads(f) for f in frames]
    assert [d["action"] for d in decoded] == ["subscribe"] * 3
    assert [d["params"].count(",") + 1 for d in decoded] == [1000, 1000, 1]
    assert ",".join(d["params"] for d in decoded) == ",".join(topics)


def test_no_topics_make_no_frames() -> None:
    assert subscription_frames("unsubscribe", []) == []


def test_unsubscribe_frames_name_their_action() -> None:
    assert subscription_frames("unsubscribe", ["A.*"]) == [
        '{"action":"unsubscribe","params":"A.*"}'
    ]


# --- the handshake ------------------------------------------------------------------------------


def test_handshake_status_reads_the_first_status(load_fixture: Callable[[str, str], bytes]) -> None:
    assert handshake_status(load_fixture("massive/ws", "connected.json")) == (
        "connected",
        "Connected Successfully",
    )


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        ('{"ev":"status","status":"auth_success"}', ("auth_success", "")),
        ('[{"ev":"T"},{"ev":"status","status":"x","message":5}]', ("x", "")),
    ],
)
def test_handshake_status_tolerates_a_bare_object_and_odd_messages(
    frame: str, expected: tuple[str, str]
) -> None:
    assert handshake_status(frame) == expected


@pytest.mark.parametrize(
    "frame", ["[]", '[{"ev":"status"}]', '[{"ev":"status","status":1}]', "[1]"]
)
def test_handshake_status_refuses_a_frame_without_a_status(frame: str) -> None:
    with pytest.raises(ValueError, match="expected a status object"):
        handshake_status(frame)


def test_handshake_status_refuses_a_frame_that_is_not_json() -> None:
    # orjson raises its own subclass of JSONDecodeError, with its own message.
    with pytest.raises(json.JSONDecodeError):
        handshake_status("not json")


# --- statuses after authentication -------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [
        ("success", "subscribed to: T.AAPL", StatusKind.ACK, ("T.AAPL",)),
        ("success", "Subscribed to: T.AAPL, Q.AAPL ,", StatusKind.ACK, ("T.AAPL", "Q.AAPL")),
        ("success", "subscribed to", StatusKind.ACK, ()),
        ("success", "unsubscribed to: T.AAPL", StatusKind.OTHER, ()),
        ("error", "not authorized to access T.AAPL.", StatusKind.REFUSED, ("T.AAPL",)),
        ("error", "not authorized", StatusKind.REFUSED, ()),
        ("max_connections", "too many", StatusKind.MAX_CONNECTIONS, ()),
        ("auth_success", "authenticated", StatusKind.OTHER, ()),
    ],
)
def test_status_objects_are_classified(
    decoder: FrameDecoder, case: tuple[str, str, StatusKind, tuple[str, ...]]
) -> None:
    status, message, kind, topics = case
    [decoded] = decoder.decode(_frame({"ev": "status", "status": status, "message": message}))
    assert decoded == Status(kind=kind, topics=topics, message=message)


def test_a_refusal_names_only_topics_of_known_channels_once(decoder: FrameDecoder) -> None:
    message = "U.S. real-time data for 'T.AAPL' and T.* (T.AAPL) is not in X.Y; T."
    [status] = decoder.decode(_frame({"ev": "status", "status": "error", "message": message}))
    assert isinstance(status, Status)
    assert status.topics == ("T.AAPL", "T.*")


def test_a_status_without_a_message_has_empty_text(decoder: FrameDecoder) -> None:
    assert decoder.decode(_frame({"ev": "status", "status": "error"})) == [
        Status(kind=StatusKind.REFUSED)
    ]


@pytest.mark.parametrize(
    ("kind", "message", "verdict"),
    [
        (StatusKind.REFUSED, "Your plan lacks REAL-TIME access", True),
        (StatusKind.REFUSED, "not authorized", False),
        (StatusKind.OTHER, "real-time", False),
    ],
)
def test_the_realtime_verdict(kind: StatusKind, message: str, verdict: bool) -> None:
    assert Status(kind=kind, message=message).realtime_verdict is verdict


# --- data ---------------------------------------------------------------------------------------


def test_rows_use_long_form_columns_and_absent_fields_are_none(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", "p": 100, "c": [1, 2]})
    assert data.channel == "T"
    assert data.rows == [
        {"Ticker": "AAPL", "Price": 100.0, "Size": None, "SipTimestamp": None, "Conditions": [1, 2]}
    ]
    assert isinstance(data.rows[0]["Price"], float)
    assert data.rejects == []


def test_a_null_value_is_none(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", "p": None})
    assert data.rows[0]["Price"] is None
    assert data.drift.missing == {"s", "t", "c"}


def test_drift_collects_extra_and_missing_keys_over_the_channel(decoder: FrameDecoder) -> None:
    [data] = decoder.decode(
        _frame(
            {"ev": "T", "sym": "A", "p": 1, "s": 1, "t": 1, "c": [], "new": 1},
            {"ev": "T", "sym": "B", "p": 1, "s": 1, "t": 1, "z": 2},
        )
    )
    assert isinstance(data, Data)
    assert data.drift == DriftReport(extra=frozenset({"new", "z"}), missing=frozenset({"c"}))
    assert len(data.rows) == 2


def test_symbols_pass_through_as_the_wire_spells_them() -> None:
    decoder = FrameDecoder([FOREX], host=HOST, path="/forex")
    decoder.set_required("CA", frozenset({"Pair"}), symbol="Pair")
    data = _decode_one(decoder, {"ev": "CA", "pair": "EUR/USD", "c": "x"})
    assert data.rows == []
    assert data.rejects[0].symbol == "EUR/USD"


def test_each_channel_is_one_data_event_at_its_first_object(decoder: FrameDecoder) -> None:
    events = decoder.decode(
        _frame(
            {"ev": "E", "b": True},
            {"ev": "status", "status": "success", "message": "subscribed to: T.A"},
            {"ev": "T", "sym": "A"},
            {"ev": "E", "b": False},
        )
    )
    assert [type(e).__name__ for e in events] == ["Data", "Status", "Data"]
    first, _, second = events
    assert isinstance(first, Data)
    assert isinstance(second, Data)
    assert (first.channel, [r["Flag"] for r in first.rows]) == ("E", [True, False])
    assert second.channel == "T"


def test_a_single_object_frame_is_decoded(decoder: FrameDecoder) -> None:
    [data] = decoder.decode(json.dumps({"ev": "E", "b": True}).encode())
    assert isinstance(data, Data)
    assert data.rows == [{"Flag": True, "Day": None, "When": None, "Extra": None}]


# --- conversions --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [
        ("b", False, False),
        ("d", "2026-01-02", date(2026, 1, 2)),
        ("i", "2026-01-02T14:30:00Z", 1_767_364_200_000_000_000),
        ("i", "2026-01-02T09:30:00-05:00", 1_767_364_200_000_000_000),
        ("i", "2026-01-02T14:30:00.123456789Z", 1_767_364_200_123_456_789),
        ("i", "2026-01-02T14:30:00.5+00:00", 1_767_364_200_500_000_000),
        ("i", "1969-12-31T23:59:59.25Z", -750_000_000),
        ("j", {"a": [1, "é"]}, '{"a":[1,"é"]}'),
        ("j", "text", '"text"'),
    ],
)
def test_values_convert_to_their_python_form(
    decoder: FrameDecoder, case: tuple[str, object, object]
) -> None:
    key, value, expected = case
    data = _decode_one(decoder, {"ev": "E", key: value})
    [row] = data.rows
    converted = row[{"b": "Flag", "d": "Day", "i": "When", "j": "Extra"}[key]]
    assert converted == expected
    assert type(converted) is type(expected)


@pytest.mark.parametrize(
    "case",
    [
        ("s", 2**63 - 1, 2**63 - 1),
        ("s", -(2**63), -(2**63)),
        ("t", 1536036818784, 1536036818784),
        ("c", [2**31 - 1, -(2**31)], [2**31 - 1, -(2**31)]),
    ],
)
def test_integers_at_their_limits_are_accepted(
    decoder: FrameDecoder, case: tuple[str, object, object]
) -> None:
    key, value, expected = case
    [row] = _decode_one(decoder, {"ev": "T", "sym": "A", key: value}).rows
    assert row[{"s": "Size", "t": "SipTimestamp", "c": "Conditions"}[key]] == expected


@pytest.mark.parametrize(
    "case",
    [
        ("p", "1.5", "a number"),
        ("p", True, "a number"),
        ("s", 1.5, "an int64"),
        ("s", True, "an int64"),
        ("s", 2**63, "an int64 in [-9223372036854775808, 9223372036854775807]"),
        ("t", "1536036818784", "an int64"),
        ("c", 5, "a list of int32"),
        ("c", [1, 2**31], "a list of int32 in [-2147483648, 2147483647]"),
        ("c", [True], "a list of int32"),
        ("sym", 5, "text"),
    ],
)
def test_an_unconvertible_value_rejects_its_row(
    decoder: FrameDecoder, case: tuple[str, object, str]
) -> None:
    key, value, expects = case
    item = {"ev": "T", "sym": "AAPL", key: value}
    data = _decode_one(decoder, item)
    assert data.rows == []
    [reject] = data.rejects
    assert reject.channel == "T"
    assert reject.symbol is None  # no symbol column was declared
    assert isinstance(reject.error, WireError)
    assert (reject.error.host, reject.error.path, reject.error.status) == (HOST, PATH, None)
    assert f"field {key!r}" in reject.error.message
    assert f"expects {expects}, got " in reject.error.message


@pytest.mark.parametrize(
    "case",
    [
        ("b", 1, "true or false"),
        ("b", "true", "true or false"),
        ("d", "2026-1-2", "a date as YYYY-MM-DD"),
        ("d", "2026-13-01", "a date as YYYY-MM-DD"),
        ("d", "2026W011x", "a date as YYYY-MM-DD"),
        ("d", 20260102, "a date as YYYY-MM-DD"),
        ("i", "2026-01-02", "an ISO-8601 date-time with an offset"),
        ("i", "2026-01-02T14:30:00", "an ISO-8601 date-time with an offset"),
        ("i", "2026-01-02T14:30:00.1234567891Z", "an ISO-8601 date-time with an offset"),
        ("i", "2026-01-02T25:00:00Z", "an ISO-8601 date-time with an offset"),
        ("i", "2026-01-02T14:30:00+0000", "an ISO-8601 date-time with an offset"),
        ("i", 1, "an ISO-8601 date-time with an offset"),
    ],
)
def test_text_types_refuse_what_they_cannot_parse(
    decoder: FrameDecoder, case: tuple[str, object, str]
) -> None:
    key, value, expects = case
    data = _decode_one(decoder, {"ev": "E", key: value})
    [reject] = data.rejects
    assert f"expects {expects}, got {value!r}" in reject.error.message


@pytest.mark.parametrize(
    "case",
    [
        (WireType.LONG, -(2**63) - 1, "an int64 in [-9223372036854775808, 9223372036854775807]"),
        (WireType.DOUBLE, 10**400, "a number within the range of a double"),
    ],
)
def test_converters_refuse_integers_beyond_their_range(case: tuple[WireType, int, str]) -> None:
    # Only the standard-library decoder produces these integers (orjson refuses or rounds them),
    # so the converters are called directly.
    wire_type, value, expects = case
    with pytest.raises(protocol._UnconvertibleError, match=re.escape(expects)):
        protocol._CONVERTERS[wire_type](value)


def test_a_reject_quotes_at_most_50_characters_of_the_value(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": "x" * 200})
    message = data.rejects[0].error.message
    assert message.endswith("got " + repr("x" * 200)[:50])


# --- required keys and symbols ------------------------------------------------------------------


def test_a_missing_required_key_rejects_while_a_missing_optional_field_is_null(
    decoder: FrameDecoder,
) -> None:
    decoder.set_required("T", frozenset({"Ticker", "SipTimestamp"}), symbol="Ticker")
    [data] = decoder.decode(
        _frame(
            {"ev": "T", "sym": "AAPL", "t": 1},
            {"ev": "T", "sym": "MSFT", "p": 2.0},
            {"ev": "T", "t": 3, "sym": None},
        )
    )
    assert isinstance(data, Data)
    assert data.rows == [
        {"Ticker": "AAPL", "Price": None, "Size": None, "SipTimestamp": 1, "Conditions": None}
    ]
    assert [(r.channel, r.symbol) for r in data.rejects] == [("T", "MSFT"), ("T", None)]
    assert data.rejects[0].error.message == (
        "rejected a T row for MSFT from socket.massive.com/stocks: "
        "missing required key 't' (SipTimestamp)"
    )
    assert data.rejects[1].error.message == (
        "rejected a T row from socket.massive.com/stocks: missing required key 'sym' (Ticker)"
    )


def test_a_reject_carries_the_raw_symbol_only_when_it_is_text(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset(), symbol="Ticker")
    [data] = decoder.decode(_frame({"ev": "T", "sym": "BRK.A", "p": "x"}, {"ev": "T", "sym": 7}))
    assert isinstance(data, Data)
    assert [r.symbol for r in data.rejects] == ["BRK.A", None]


def test_set_required_replaces_the_earlier_choice(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset({"Size"}), symbol="Ticker")
    decoder.set_required("T", frozenset())
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": "bad"})
    assert data.rejects[0].symbol is None


@pytest.mark.parametrize(
    "case",
    [
        ("Q", frozenset(), None, "no source for channel 'Q'"),
        ("T", frozenset({"Bid", "Ticker"}), None, r"source 'stocks/T' has no columns \['Bid'\]"),
        ("T", frozenset(), "Symbol", r"has no columns \['Symbol'\]"),
    ],
)
def test_set_required_checks_its_names(
    decoder: FrameDecoder, case: tuple[str, frozenset[str], str | None, str]
) -> None:
    channel, keys, symbol, message = case
    with pytest.raises(ValueError, match=message):
        decoder.set_required(channel, keys, symbol=symbol)


# --- frames the decoder drops --------------------------------------------------------------------


def test_a_frame_that_is_not_json_is_dropped_and_logged(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    assert decoder.decode("{oops") == []
    assert "not JSON" in caplog.text
    assert caplog.records[0].levelno == logging.ERROR


def test_an_element_without_ev_is_dropped_and_its_siblings_kept(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    events = decoder.decode(_frame(5, {"sym": "A"}, {"ev": 3}, {"ev": "E", "b": True}))
    assert len(events) == 1
    assert caplog.text.count("no text 'ev'") == 3


def test_an_unknown_channel_is_dropped_and_warned_once(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    for _ in range(2):
        assert decoder.decode(_frame({"ev": "XQ", "pair": "BTC-USD"})) == []
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert "'XQ'" in caplog.text


@pytest.mark.parametrize(
    ("sources", "message"),
    [
        ([TRADES, TRADES], "two sources for channel 'T'"),
        ([Source(key="stocks", transport=Transport.WS, fields=())], "not a WebSocket source"),
        ([Source(key="stocks/T", transport=Transport.REST, fields=())], "not a WebSocket source"),
    ],
)
def test_the_sources_are_checked(sources: list[Source], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        FrameDecoder(sources, host=HOST, path=PATH)


# --- failure classification ---------------------------------------------------------------------


def _status_error(status: int, headers: dict[str, str] | None = None) -> InvalidStatus:
    return InvalidStatus(Response(status, "x", Headers(headers or {}), b""))


def _cert_error() -> ssl.SSLCertVerificationError:
    error = ssl.SSLCertVerificationError(1, "certificate verify failed")
    error.verify_message = "unable to get local issuer certificate"
    return error


def _eof() -> InvalidMessage:
    error = InvalidMessage("did not receive a valid HTTP response")
    error.__cause__ = EOFError("connection closed")
    return error


NOW = 1_000_000.0
IN_90_SECONDS = format_datetime(datetime.fromtimestamp(NOW + 90, UTC), usegmt=True)


def _classify(error: Exception) -> WireError | float:
    return classify_failure(error, host=HOST, path=PATH, now=NOW)


@pytest.mark.parametrize(
    "error",
    [
        ConnectionRefusedError(61, "refused"),
        TimeoutError(),
        _eof(),
        _status_error(500),
        _status_error(502),
        _status_error(503),
        _status_error(504),
    ],
    ids=["refused", "timeout", "eof", "500", "502", "503", "504"],
)
def test_transient_failures_are_retried_on_the_backoff_alone(error: Exception) -> None:
    assert _classify(error) == 0.0


@pytest.mark.parametrize(
    ("headers", "wait"),
    [
        ({"Retry-After": "40"}, 40.0),
        ({"Retry-After": " 7 "}, 7.0),
        ({"Retry-After": IN_90_SECONDS}, 90.0),
        ({"Retry-After": "Thu, 01 Jan 1970 00:00:00 -0000"}, 0.0),
        ({"Retry-After": "soon"}, 0.0),
        ({"Retry-After": "²"}, 0.0),
        ({}, 0.0),
    ],
    ids=["seconds", "padded", "http-date", "past-naive-date", "garbage", "unicode-digit", "absent"],
)
def test_a_429_is_retried_after_its_retry_after(headers: dict[str, str], wait: float) -> None:
    # websockets itself treats a 429 as fatal; the client retries it.
    assert _classify(_status_error(429, headers)) == pytest.approx(wait)


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_other_http_statuses_are_fatal(status: int) -> None:
    error = _classify(_status_error(status))
    assert isinstance(error, WireError)
    assert (error.status, error.host, error.path) == (status, HOST, PATH)
    assert error.message == f"WebSocket connection to {HOST}{PATH} failed: HTTP {status}"


def test_an_invalid_uri_is_fatal_without_a_status() -> None:
    error = _classify(InvalidURI("x", "bad"))
    assert isinstance(error, WireError)
    assert error.status is None
    assert error.message == (
        f"WebSocket connection to {HOST}{PATH} failed: InvalidURI: x isn't a valid URI: bad"
    )


def test_a_certificate_failure_is_fatal_and_names_ssl_cert_file() -> None:
    # websockets would retry it, as an OSError; the client never does.
    error = _classify(_cert_error())
    assert isinstance(error, WireError)
    assert error.status is None
    assert (error.host, error.path) == (HOST, PATH)
    assert error.message == (
        f"TLS certificate verification failed for {HOST}: unable to get local issuer "
        "certificate; set SSL_CERT_FILE to a bundle that trusts the server's certificate"
    )


def test_an_unreadable_retry_after_is_warned_about(caplog: pytest.LogCaptureFixture) -> None:
    assert _classify(_status_error(429, {"Retry-After": "soon"})) == 0.0
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == "ignoring an unreadable Retry-After header: 'soon'"


def test_an_object_of_exactly_the_declared_shape_has_no_drift(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": 1, "s": 1, "t": 1, "c": []})
    assert data.drift == DriftReport()
