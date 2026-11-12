from __future__ import annotations

import itertools
import json
import logging
import ssl
import statistics
import sys
import time
from datetime import UTC, date, datetime
from email.utils import format_datetime
from typing import TYPE_CHECKING

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import (
    InvalidMessage,
    InvalidProxy,
    InvalidProxyMessage,
    InvalidProxyStatus,
    InvalidStatus,
    InvalidURI,
)
from websockets.http11 import Response

from deephaven_massive._massive import WireFailure
from deephaven_massive._massive.net.errors import LIBRARY_BUG_REPORT, WireError
from deephaven_massive._massive.spec import sources
from deephaven_massive._massive.spec.types import (
    Conversion,
    DriftReport,
    DriftSample,
    Field,
    Source,
    Transport,
    WireType,
    WireUnit,
)
from deephaven_massive._massive.ws import protocol
from deephaven_massive._massive.ws import protocol as protocol_module
from deephaven_massive._massive.ws.protocol import (
    DROPPED_ERROR_LIMIT,
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
        # The vendor marks `t` required, so its absence is drift; the others are optional.
        Field(
            wire_key="t",
            column="SipTimestamp",
            wire_type=WireType.EPOCH,
            unit=WireUnit.MS,
            required=True,
        ),
        Field(wire_key="c", column="Conditions", wire_type=WireType.LONG_ARRAY),
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
# Fields that take a named conversion, as the generated schema records one for a field whose
# documented values its declared type does not describe.
CONVERTED = Source(
    key="forex/C",
    transport=Transport.WS,
    fields=(
        Field(
            wire_key="x",
            column="Exchange",
            wire_type=WireType.LONG,
            conversion=Conversion.QUOTED_INTEGER,
        ),
        Field(
            wire_key="i",
            column="TradeId",
            wire_type=WireType.STRING,
            conversion=Conversion.TEXT_OR_INTEGER_ID,
        ),
    ),
)
# A price Massive sends as text, as the futures stream does, beside a key the library ignores.
TEXT_PRICES = Source(
    key="stocks/F",
    transport=Transport.WS,
    fields=(
        Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
        Field(
            wire_key="p",
            column="Price",
            wire_type=WireType.DOUBLE,
            conversion=Conversion.DECIMAL_TEXT_NUMBER,
        ),
    ),
    ignored_keys=frozenset({"lp"}),
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


_SOURCES = {s.key.partition("/")[2]: s for s in (TRADES, EVERYTHING, CONVERTED, FOREX, TEXT_PRICES)}


def _dicts(data: Data) -> list[dict[str, object]]:
    # The rows as dictionaries by long-form column, so a test reads them by name.
    columns = [field.column for field in _SOURCES[data.channel].fields]
    return [dict(zip(columns, row, strict=True)) for row in data.rows]


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


# --- data ---------------------------------------------------------------------------------------


def test_rows_use_long_form_columns_and_absent_fields_are_none(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", "p": 100, "c": [1, 2]})
    assert data.channel == "T"
    assert _dicts(data) == [
        {"Ticker": "AAPL", "Price": 100.0, "Size": None, "SipTimestamp": None, "Conditions": [1, 2]}
    ]
    assert isinstance(_dicts(data)[0]["Price"], float)
    assert (data.drift.unconvertible, data.drift.dropped_rows) == (frozenset(), 0)


def test_a_null_value_is_none(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", "p": None})
    assert _dicts(data)[0]["Price"] is None


def test_drift_reports_extra_keys_and_only_vendor_required_fields_as_missing(
    decoder: FrameDecoder,
) -> None:
    [data] = decoder.decode(
        _frame(
            {"ev": "T", "sym": "A", "p": 1, "s": 1, "t": 1, "c": [], "new": 1},
            {"ev": "T", "sym": "B", "s": 1, "z": 2},
            {"ev": "T", "sym": "C", "t": None},
        )
    )
    assert isinstance(data, Data)
    # The optional `p` and `c` are absent without drift; the vendor-required `t`, absent once
    # and null once, is missing. The extra keys are dropped from the rows.
    assert data.drift == DriftReport(extra=frozenset({"new", "z"}), missing=frozenset({"t"}))
    columns = {"Ticker", "Price", "Size", "SipTimestamp", "Conditions"}
    assert [set(r) for r in _dicts(data)] == [columns] * 3
    assert [r["SipTimestamp"] for r in _dicts(data)] == [1, None, None]


def test_drift_samples_name_the_first_symbol_and_value_of_each_key(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset(), symbol="Ticker")
    [data] = decoder.decode(
        _frame(
            {"ev": "T", "sym": "A", "t": 1, "new": {"deep": [1, "é"]}},
            {"ev": "T", "sym": "B", "new": 2, "z": "x" * 300},
            {"ev": "T", "sym": 7, "t": None},
        )
    )
    assert isinstance(data, Data)
    assert (
        dict(data.drift.samples)
        == {
            "new": DriftSample("A", '{"deep": [1, "é"]}'),  # the first row's value, not B's
            "z": DriftSample("B", json.dumps("x" * 300)[:200]),
            "t": DriftSample("B", None),  # first missing in B's row; a missing key has no value
            "sym": DriftSample(None, "7"),  # a symbol that is not text reads as null
        }
    )


def test_drift_samples_have_no_symbol_without_a_declared_symbol(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "t": 1, "new": True})
    assert dict(data.drift.samples) == {"new": DriftSample(None, "true")}


def test_drift_samples_are_read_only_and_outside_equality_and_repr() -> None:
    first = DriftReport(extra=frozenset({"x"}), samples={"x": DriftSample("A", "1")})
    second = DriftReport(extra=frozenset({"x"}), samples={"x": DriftSample("B", "2")})
    assert first == second
    assert hash(first) == hash(second)
    assert "samples" not in repr(first)
    with pytest.raises(TypeError):
        first.samples["y"] = DriftSample(None, None)  # type: ignore[index]  # the point: read-only


def test_a_row_with_an_odd_value_is_logged_whole_at_debug(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    decoder.set_required("T", frozenset({"Ticker"}), symbol="Ticker")
    row = {"ev": "T", "sym": "AAPL", "p": "182.33a", "s": 100, "t": 1761660187112}
    with caplog.at_level(logging.DEBUG, logger=protocol.__name__):
        data = _decode_one(decoder, row)
    assert _dicts(data)[0]["Price"] is None
    assert caplog.messages == [
        (
            "read as null in a T row for AAPL from socket.massive.com/stocks: Price (wire key "
            "'p') expects double, got \"182.33a\"; the row: "
            '{"ev":"T","sym":"AAPL","p":"182.33a","s":100,"t":1761660187112}'
        ),
    ]


def test_a_dropped_row_is_logged_whole_at_debug(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    decoder.set_required("T", frozenset({"SipTimestamp"}), symbol="Ticker")
    row = {"ev": "T", "sym": "AAPL", "t": 4.0}
    with caplog.at_level(logging.DEBUG, logger=protocol.__name__):
        data = _decode_one(decoder, row)
    assert _dicts(data) == []
    [reject] = data.rejects
    assert (reject.channel, reject.symbol) == ("T", "AAPL")
    assert reject.error.message == (
        "dropped a T row for AAPL from socket.massive.com/stocks: SipTimestamp (wire key 't') "
        "expects epoch, got 4.0"
    )
    assert reject.raw == '{"ev":"T","sym":"AAPL","t":4.0}'
    assert caplog.messages == [f"{reject.error.message}; the row: {reject.raw}"]


def test_an_odd_row_is_not_serialized_with_debug_off(
    decoder: FrameDecoder, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(_row: object) -> str:
        raise AssertionError("the row was serialized with DEBUG off")

    monkeypatch.setattr(protocol, "row_excerpt", refuse)
    # A dropped row's `Reject` always carries its text; a kept row's odd value needs none.
    data = _decode_one(decoder, {"ev": "T", "p": "x", "t": 1})
    assert (len(_dicts(data)), data.drift.unconvertible) == (1, frozenset({"p"}))


def test_a_vendor_required_field_held_as_null_is_missing(decoder: FrameDecoder) -> None:
    # Present in every object, but only ever as null.
    [data] = decoder.decode(_frame({"ev": "T", "sym": "A", "t": None}, {"ev": "T", "t": None}))
    assert isinstance(data, Data)
    assert data.drift == DriftReport(missing=frozenset({"t"}))


def test_a_dropped_row_still_counts_in_the_drift(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset({"Ticker"}))
    [data] = decoder.decode(
        _frame(
            {"ev": "T", "sym": "A", "p": 1, "s": 1, "t": 1, "c": []},
            {"ev": "T", "p": "bad", "new": 1},
            {"ev": "T", "sym": "B", "p": "bad", "s": 1, "c": [], "z": 2},
        )
    )
    assert isinstance(data, Data)
    assert [r["Ticker"] for r in _dicts(data)] == ["A", "B"]
    assert len(data.rejects) == 1
    # The dropped row's extra key counts; its odd value does not, since no row reads it.
    assert data.drift == DriftReport(
        extra=frozenset({"new", "z"}), missing=frozenset({"t"}), unconvertible=frozenset({"p"})
    )


def test_symbols_pass_through_as_the_wire_spells_them() -> None:
    decoder = FrameDecoder([FOREX], host=HOST, path="/forex")
    decoder.set_required("CA", frozenset({"Pair"}), symbol="Pair")
    data = _decode_one(decoder, {"ev": "CA", "pair": "EUR/USD", "c": "x"})
    assert _dicts(data)[0]["Pair"] == "EUR/USD"
    assert data.drift.samples["c"].symbol == "EUR/USD"


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
    assert (first.channel, [r["Flag"] for r in _dicts(first)]) == ("E", [True, False])
    assert second.channel == "T"


def test_a_single_object_frame_is_decoded(decoder: FrameDecoder) -> None:
    [data] = decoder.decode(json.dumps({"ev": "E", "b": True}).encode())
    assert isinstance(data, Data)
    assert _dicts(data) == [{"Flag": True, "Day": None, "When": None, "Extra": None}]


# --- conversions --------------------------------------------------------------------------------
# `spec.convert` owns the rules of every type and every named conversion and is tested there;
# these tests show that the decoder applies it to every field, a field's named conversion
# included, and reads a value it refuses as null, or drops its row when the value is a key.


def test_every_field_goes_through_the_shared_converter(decoder: FrameDecoder) -> None:
    item = {
        "ev": "E",
        "b": False,
        "d": "2026-01-02",
        "i": "2026-01-02T14:30:00.123456789Z",
        "j": {"a": [1, "é"]},
    }
    [row] = _dicts(_decode_one(decoder, item))
    assert row == {
        "Flag": False,
        "Day": date(2026, 1, 2),
        "When": 1_767_364_200_123_456_789,
        "Extra": '{"a":[1,"é"]}',
    }


@pytest.mark.parametrize(
    "case",
    [
        ("p", "Price", "1.5", "double", '"1.5"'),
        ("s", "Size", True, "long", "true"),
        ("t", "SipTimestamp", "1536036818784", "epoch", '"1536036818784"'),
        ("c", "Conditions", [1, 2**63], "long-array", "[1, 9223372036854775808]"),
        ("sym", "Ticker", 5, "string", "5"),
    ],
)
def test_an_unconvertible_value_reads_as_null_and_is_drift(
    decoder: FrameDecoder, case: tuple[str, str, object, str, str]
) -> None:
    key, column, value, _, quoted = case
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", key: value})
    [row] = _dicts(data)
    assert row[column] is None
    assert (data.drift.unconvertible, data.drift.dropped_rows) == (frozenset({key}), 0)
    assert data.drift.samples[key] == DriftSample(None, quoted)  # no symbol column declared


@pytest.mark.parametrize(
    "case",
    [
        ("p", "Price", "1.5", "double", '"1.5"'),
        ("t", "SipTimestamp", "1536036818784", "epoch", '"1536036818784"'),
        ("sym", "Ticker", 5, "string", "5"),
    ],
)
def test_an_unconvertible_required_key_drops_its_row(
    decoder: FrameDecoder, case: tuple[str, str, object, str, str]
) -> None:
    key, column, value, expects, quoted = case
    decoder.set_required("T", frozenset({column}))
    data = _decode_one(decoder, {"ev": "T", "sym": "AAPL", key: value})
    assert _dicts(data) == []
    assert data.drift.unconvertible == frozenset()
    [reject] = data.rejects
    assert reject.channel == "T"
    assert reject.symbol is None  # no symbol column was declared
    assert (reject.error.host, reject.error.path, reject.error.status) == (HOST, PATH, None)
    # The clause after the prefix is the one `spec.convert` shares with the REST decoder,
    # quoting the value's JSON text.
    assert reject.error.message == (
        f"dropped a T row from socket.massive.com/stocks: "
        f"{column} (wire key {key!r}) expects {expects}, got {quoted}"
    )


def test_a_named_conversion_applies_to_its_field() -> None:
    decoder = FrameDecoder([CONVERTED], host=HOST, path="/forex")
    [data] = decoder.decode(_frame({"ev": "C", "x": "48", "i": 7}, {"ev": "C", "x": 49, "i": "ab"}))
    assert isinstance(data, Data)
    assert _dicts(data) == [
        {"Exchange": 48, "TradeId": "7"},
        {"Exchange": 49, "TradeId": "ab"},
    ]
    assert data.drift == DriftReport()


@pytest.mark.parametrize(
    "case",
    [
        # Only the named conversion reads the field: neither decimal text nor a float passes.
        ("x", "Exchange", "4.8", "long (by the conversion quoted-integer)", '"4.8"'),
        ("x", "Exchange", 48.5, "long (by the conversion quoted-integer)", "48.5"),
        ("i", "TradeId", 1.5, "string (by the conversion text-or-integer-id)", "1.5"),
    ],
)
def test_a_value_its_named_conversion_cannot_read_is_odd(
    case: tuple[str, str, object, str, str], caplog: pytest.LogCaptureFixture
) -> None:
    key, column, value, expects, quoted = case
    decoder = FrameDecoder([CONVERTED], host=HOST, path="/forex")
    with caplog.at_level(logging.DEBUG, logger=protocol.__name__):
        data = _decode_one(decoder, {"ev": "C", key: value})
    assert _dicts(data)[0][column] is None
    assert data.drift.unconvertible == {key}
    assert caplog.messages[0].startswith(
        f"read as null in a C row from socket.massive.com/forex: "
        f"{column} (wire key {key!r}) expects {expects}, got {quoted}; the row: "
    )


def test_an_odd_field_keeps_the_sample_of_its_first_row(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset(), symbol="Ticker")
    [data] = decoder.decode(
        _frame({"ev": "T", "sym": "A", "p": "x"}, {"ev": "T", "sym": "B", "p": "y"})
    )
    assert isinstance(data, Data)
    assert [r["Price"] for r in _dicts(data)] == [None, None]
    assert data.drift.samples["p"] == DriftSample("A", '"x"')


def test_an_odd_value_sample_quotes_at_most_200_characters(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": "x" * 300})
    assert data.drift.samples["p"].value == json.dumps("x" * 300)[:200]


# --- required keys and symbols ------------------------------------------------------------------


def test_a_missing_required_key_drops_while_a_missing_optional_field_is_null(
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
    assert _dicts(data) == [
        {"Ticker": "AAPL", "Price": None, "Size": None, "SipTimestamp": 1, "Conditions": None}
    ]
    assert [(r.channel, r.symbol) for r in data.rejects] == [("T", "MSFT"), ("T", None)]
    assert data.rejects[0].error.message == (
        "dropped a T row for MSFT from socket.massive.com/stocks: "
        "no SipTimestamp (wire key 't'), which the dataset needs"
    )
    assert data.rejects[1].error.message == (
        "dropped a T row from socket.massive.com/stocks: "
        "no Ticker (wire key 'sym'), which the dataset needs"
    )


def test_a_dropped_row_without_a_text_symbol_names_none(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset({"Ticker"}), symbol="Ticker")
    data = _decode_one(decoder, {"ev": "T", "t": 3, "sym": 7})
    [reject] = data.rejects
    assert reject.symbol is None
    assert reject.error.message == (
        "dropped a T row from socket.massive.com/stocks: Ticker (wire key 'sym') expects "
        "string, got 7"
    )


def test_a_drift_sample_carries_the_raw_symbol_only_when_it_is_text(
    decoder: FrameDecoder,
) -> None:
    decoder.set_required("T", frozenset(), symbol="Ticker")
    [data] = decoder.decode(
        _frame({"ev": "T", "sym": 7, "p": "x"}, {"ev": "T", "sym": "B", "s": 1.5})
    )
    assert isinstance(data, Data)
    assert data.drift.samples["p"].symbol is None
    assert data.drift.samples["s"].symbol == "B"


def test_set_required_replaces_the_earlier_choice(decoder: FrameDecoder) -> None:
    decoder.set_required("T", frozenset({"Size"}), symbol="Ticker")
    decoder.set_required("T", frozenset())
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": "bad"})
    assert len(_dicts(data)) == 1
    assert data.drift.samples["p"].symbol is None


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
    assert caplog.records[0].getMessage().endswith(LIBRARY_BUG_REPORT)


def test_dropped_frames_log_a_bounded_number_of_errors(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    for _ in range(DROPPED_ERROR_LIMIT + 5):
        assert decoder.decode("{oops") == []
    ours = [r for r in caplog.records if r.name == protocol_module.__name__]
    # The limit's ERRORs, one ERROR saying the rest are quieter, then DEBUG only.
    assert [r.levelno for r in ours] == (
        [logging.ERROR] * (DROPPED_ERROR_LIMIT + 1) + [logging.DEBUG] * 5
    )
    assert ours[DROPPED_ERROR_LIMIT].getMessage() == (
        f"socket.massive.com/stocks: {DROPPED_ERROR_LIMIT} dropped frames or elements were "
        "logged; further ones are logged at DEBUG"
    )


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
        (
            [
                Source(
                    key="stocks/T",
                    transport=Transport.WS,
                    fields=(Field(wire_key="day.c", column="DayC", wire_type=WireType.DOUBLE),),
                )
            ],
            r"source 'stocks/T' has dotted wire keys \['day\.c'\]; a WebSocket object is flat",
        ),
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
    assert error.kind is WireFailure.REFUSED  # the server answered the handshake


@pytest.mark.parametrize(
    "error",
    [
        InvalidProxyStatus(Response(407, "x", Headers({}), b"")),
        InvalidProxy("http://proxy", "bad"),
    ],
    ids=["refused-credentials", "bad-setting"],
)
def test_a_proxy_failure_is_this_machines_to_fix(error: Exception) -> None:
    classified = _classify(error)
    assert isinstance(classified, WireError)
    assert classified.kind is WireFailure.LOCAL


@pytest.mark.parametrize(
    "error",
    [
        InvalidProxyStatus(Response(502, "x", Headers({}), b"")),
        InvalidProxyMessage("the proxy's reply is not HTTP"),
    ],
    ids=["proxy-502", "garbled-reply"],
)
def test_a_proxy_that_could_not_reach_the_server_is_retried(error: Exception) -> None:
    # No answer from the server, as the REST and flat-file clients count it.
    assert _classify(error) == 0.0


def test_a_proxy_that_refuses_the_credentials_names_its_status() -> None:
    error = _classify(InvalidProxyStatus(Response(407, "x", Headers({}), b"")))
    assert isinstance(error, WireError)
    assert error.status == 407


def test_a_broken_handshake_is_the_servers_answer() -> None:
    error = _classify(InvalidMessage("did not receive a valid HTTP response"))
    assert isinstance(error, WireError)
    assert (error.status, error.kind) == (None, WireFailure.REFUSED)


def test_an_invalid_uri_is_fatal_without_a_status() -> None:
    error = _classify(InvalidURI("x", "bad"))
    assert isinstance(error, WireError)
    assert error.status is None
    assert error.message == (
        f"WebSocket connection to {HOST}{PATH} failed: InvalidURI: x isn't a valid URI: bad"
    )
    assert error.kind is WireFailure.CLIENT_DEFECT  # the client built the URI


def test_a_certificate_failure_is_fatal_and_names_ssl_cert_file() -> None:
    # websockets would retry it, as an OSError; the client never does.
    error = _classify(_cert_error())
    assert isinstance(error, WireError)
    assert error.status is None
    assert (error.host, error.path) == (HOST, PATH)
    assert error.message.startswith(f"TLS certificate verification failed for {HOST}: ")
    assert error.kind is WireFailure.LOCAL  # SSL_CERT_FILE, on this machine, is the fix
    assert error.message.endswith(
        "; set SSL_CERT_FILE to a bundle that trusts the server's certificate"
    )


def test_a_wrapped_certificate_failure_is_fatal_too() -> None:
    wrapper = OSError("connection failed")
    wrapper.__cause__ = _cert_error()
    error = _classify(wrapper)
    assert isinstance(error, WireError)
    assert "SSL_CERT_FILE" in error.message


def test_an_unreadable_retry_after_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    assert _classify(_status_error(429, {"Retry-After": "soon"})) == 0.0
    [record] = caplog.records
    assert record.levelno == logging.DEBUG
    assert record.getMessage() == (
        "ignoring a Retry-After that is neither seconds nor an HTTP date: 'soon'"
    )


def test_an_object_of_exactly_the_declared_shape_has_no_drift(decoder: FrameDecoder) -> None:
    data = _decode_one(decoder, {"ev": "T", "sym": "A", "p": 1, "s": 1, "t": 1, "c": []})
    assert data.drift == DriftReport()


# --- reading by column -------------------------------------------------------------------------

# Objects of the three test channels, odd ones included: absent and null keys, extra keys,
# values of the wrong type or out of range, and values only a named conversion reads.
_OBJECTS: tuple[dict[str, object], ...] = (
    {"ev": "T", "sym": "AAPL", "p": 1.5, "s": 100, "t": 1, "c": [12, 37]},
    {"ev": "T", "sym": "MSFT", "p": 2, "s": 5, "t": 2},
    {"ev": "T", "sym": "IBM", "p": None, "s": None, "t": None, "c": None},
    {"ev": "T", "sym": "NVDA", "p": "1.5", "s": 1, "t": 3},
    {"ev": "T", "sym": "AMD", "p": 1.0, "s": 2**63, "t": 4, "c": [2**63]},
    {"ev": "T", "sym": "TSLA", "p": 1.0, "s": True, "t": 5.0, "c": [True]},
    {"ev": "T", "p": 3.5, "s": 1, "t": 6, "x": 4},
    {"ev": "T", "sym": 7, "p": 2**60 + 1, "s": 1, "t": 7, "zz": {"a": 1}},
    {"ev": "E", "b": True, "d": "2025-03-03", "i": "2025-03-03T14:30:00.5Z", "j": [1, "a"]},
    {"ev": "E", "b": 1, "d": "2025-02-30", "i": "2025-03-03T14:30:00", "j": None},
    {"ev": "E", "d": None, "i": "2025-03-03T14:30:00+01:00", "j": {"k": None}},
    {"ev": "C", "x": "48", "i": 7},
    {"ev": "C", "x": 49, "i": "ab"},
    {"ev": "C", "x": "4.8", "i": 1.5},
    {"ev": "C", "x": None, "i": None, "y": 1},
    {"ev": "F", "sym": "ESZ6", "p": "7676.5", "lp": 0},
    {"ev": "F", "sym": "ESZ6", "p": 7679},
    {"ev": "F", "sym": "ESZ6", "p": "1e5"},
    {"ev": "F", "sym": "ESZ6", "p": "1\n2"},  # a newline, which the column reading joins on
    {"ev": "F", "sym": "ESZ6", "p": None, "zz": 1},
)


def _events(decoder: FrameDecoder, frame: str) -> list[object]:
    # Every event of a frame, compared whole: each value with its exact type, each reject by
    # its fields, and each drift with its samples, which its equality leaves out.
    events: list[object] = []
    for event in decoder.decode(frame):
        if isinstance(event, Status):
            events.append(event)
            continue
        rows = [[(type(v), v) for v in row] for row in event.rows]
        rejects = [(r.channel, r.symbol, r.error.message, r.raw) for r in event.rejects]
        events.append((event.channel, rows, rejects, event.drift, dict(event.drift.samples)))
    return events


def _decoder() -> FrameDecoder:
    # Only the ticker is required, so a missing `t`, which the vendor marks required, is drift
    # that the column reading reports itself.
    decoder = FrameDecoder([TRADES, EVERYTHING, CONVERTED, TEXT_PRICES], host=HOST, path=PATH)
    decoder.set_required("T", frozenset({"Ticker"}), symbol="Ticker")
    return decoder


def test_frames_read_by_column_decode_exactly_as_one_by_one(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The same frames, read by column where the reading can vouch for them and one by one
    # otherwise, and read one by one throughout: identical rows, rejects, drift, and samples.
    # Every ordered pair of objects, repeated into frames of five; every run of one to twelve
    # objects of the list taken in turn; and each object six times alone.
    frames = [_frame(a, b, b, a, a) for a, b in itertools.product(_OBJECTS, repeat=2)]
    ring = itertools.cycle(_OBJECTS)
    frames += [_frame(*itertools.islice(ring, size)) for size in range(1, 13) for _ in range(15)]
    frames += [_frame(*[item] * 6) for item in _OBJECTS]
    caplog.set_level(logging.DEBUG, logger=protocol.__name__)
    by_column = [_events(_decoder(), frame) for frame in frames]
    read_by_column = sum("rows one by one" in m for m in caplog.messages)
    monkeypatch.setattr(protocol, "COLUMN_MIN_OBJECTS", 10**9)
    one_by_one = [_events(_decoder(), frame) for frame in frames]
    assert by_column == one_by_one
    assert read_by_column  # some frames needed the reference reading


def test_a_frame_of_enough_clean_objects_is_read_by_column(
    monkeypatch: pytest.MonkeyPatch, decoder: FrameDecoder
) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("an object was read on its own")

    monkeypatch.setattr(FrameDecoder, "_row", refuse)
    item = {"ev": "T", "sym": "AAPL", "p": 1, "s": 1, "t": 1, "c": [1]}
    [data] = decoder.decode(_frame(*[item] * protocol.COLUMN_MIN_OBJECTS))
    assert isinstance(data, Data)
    assert data.rows == [("AAPL", 1.0, 1, 1, [1])] * protocol.COLUMN_MIN_OBJECTS
    assert type(data.rows[0][1]) is float  # as the converter makes it


def test_a_frame_of_text_prices_is_read_by_column_and_its_ignored_key_is_no_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The futures trade as Massive sent it on 2026-11-02: the price as text.
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("an object was read on its own")

    monkeypatch.setattr(FrameDecoder, "_row", refuse)
    decoder = FrameDecoder([TEXT_PRICES], host=HOST, path=PATH)
    items = [{"ev": "F", "sym": "ESZ6", "p": "7676.5", "lp": 0}, {"ev": "F", "sym": "ESZ6"}]
    [data] = decoder.decode(_frame(*items * protocol.COLUMN_MIN_OBJECTS))
    assert isinstance(data, Data)
    assert data.rows == [("ESZ6", 7676.5), ("ESZ6", None)] * protocol.COLUMN_MIN_OBJECTS
    assert data.drift == DriftReport()


def test_a_price_text_that_is_no_plain_decimal_is_odd() -> None:
    decoder = FrameDecoder([TEXT_PRICES], host=HOST, path=PATH)
    data = _decode_one(decoder, {"ev": "F", "sym": "ESZ6", "p": "NaN"})
    assert _dicts(data) == [{"Ticker": "ESZ6", "Price": None}]
    assert data.drift.unconvertible == {"p"}


def test_a_frame_of_few_objects_is_read_one_by_one(
    monkeypatch: pytest.MonkeyPatch, decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(*_args: object, **_kwargs: object) -> bool:
        raise AssertionError("few objects were read by column")

    monkeypatch.setattr(FrameDecoder, "_by_column", refuse)
    item = {"ev": "T", "sym": "AAPL", "p": 1, "s": 1, "t": 1}
    few = protocol.COLUMN_MIN_OBJECTS - 1
    with caplog.at_level(logging.DEBUG, logger=protocol.__name__):
        [data] = decoder.decode(_frame(*[item] * few))
    assert isinstance(data, Data)
    assert data.rows == [("AAPL", 1.0, 1, 1, None)] * few
    assert caplog.messages == []  # reading few objects one by one is no fallback


def test_a_source_without_fields_has_an_empty_row_per_object() -> None:
    bare = Source(key="stocks/Z", transport=Transport.WS, fields=())
    for count in (1, protocol.COLUMN_MIN_OBJECTS):
        [data] = FrameDecoder([bare], host=HOST, path=PATH).decode(_frame(*[{"ev": "Z"}] * count))
        assert isinstance(data, Data)
        assert data.rows == [()] * count


def test_a_frame_the_column_reading_cannot_vouch_for_says_why_at_debug(
    decoder: FrameDecoder, caplog: pytest.LogCaptureFixture
) -> None:
    decoder.set_required("T", frozenset({"SipTimestamp"}), symbol="Ticker")
    clean = {"ev": "T", "sym": "AAPL", "p": 1.5, "s": 1, "t": 1}
    cases = [
        ({**clean, "p": "x"}, "the column Price holds a value that needs its own check"),
        ({**clean, "t": None}, "the required column SipTimestamp is absent or null in some object"),
    ]
    for odd, reason in cases:
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=protocol.__name__):
            decoder.decode(_frame(*[clean] * 3, odd))
        assert caplog.messages[0] == (
            f"socket.massive.com/stocks: reading 4 T rows one by one: {reason}"
        )


# --- benchmark ----------------------------------------------------------------------------------


def _trade_frame(rows: int) -> str:
    # A frame of real-shaped stock trades: every field of the generated `stocks/T` source.
    return json.dumps(
        [
            {
                "ev": "T",
                "sym": f"S{i % 50}",
                "i": str(i),
                "x": 4,
                "p": 100.25 + i,
                "s": 100,
                "c": [12, 37],
                "t": 1_700_000_000_000 + i,
                "q": i,
                "z": 3,
                "pt": 1_700_000_000_000 + i,
            }
            for i in range(rows)
        ]
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("rows", [1, 10, 100, 1_000])
def test_benchmark_frame_decoding(capsys: pytest.CaptureFixture[str], rows: int) -> None:
    # Records, never gates: the loop thread's cost of decoding one frame of trades, per row.
    trades = sources(Transport.WS)["stocks/T"]
    decoder = FrameDecoder([trades], host=HOST, path=PATH)
    decoder.set_required("T", frozenset({"Ticker", "SipTimestamp"}), symbol="Ticker")
    frame = _trade_frame(rows)
    repeats = max(20, 20_000 // rows)
    times = []
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(repeats):
            decoder.decode(frame)
        times.append((time.perf_counter() - started) / repeats)
    seconds = statistics.median(times)
    with capsys.disabled():
        sys.stderr.write(
            f"\nframe of {rows} trades: {seconds * 1e6:.1f} us to decode, "
            f"{seconds * 1e6 / rows:.2f} us a row\n"
        )
