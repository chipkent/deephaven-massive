from __future__ import annotations

import json
import logging
from datetime import date
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from deephaven_massive._massive.rest import batch as batch_module
from deephaven_massive._massive.rest.batch import to_batch
from deephaven_massive._massive.spec.types import (
    Conversion,
    DriftReport,
    DriftSample,
    Field,
    Param,
    ParamKind,
    RestPath,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Callable


def _field(
    wire_key: str, wire_type: WireType, *, column: str | None = None, required: bool = False
) -> Field:
    unit = WireUnit.NS if wire_type is WireType.EPOCH else None
    name = column or "".join(part.title() for part in wire_key.replace(".", "_").split("_"))
    return Field(wire_key=wire_key, column=name, wire_type=wire_type, unit=unit, required=required)


def _path(*fields: Field) -> RestPath:
    return RestPath(key="/v3/x", fields=fields)


_TRADES = RestPath(
    key="/v3/trades/{stockTicker}",
    fields=(
        _field("conditions", WireType.LONG_ARRAY),
        _field("exchange", WireType.LONG),
        _field("id", WireType.STRING),
        _field("participant_timestamp", WireType.EPOCH),
        _field("price", WireType.DOUBLE, required=True),
        _field("sequence_number", WireType.LONG),
        _field("sip_timestamp", WireType.EPOCH, required=True),
        _field("size", WireType.DOUBLE),
        _field("tape", WireType.LONG),
        _field("trf_id", WireType.LONG),
    ),
    params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
)


def _odd(wire_type: WireType, value: object) -> tuple[pa.RecordBatch, DriftReport]:
    """Read a page whose second row holds a value that does not fit its field.

    Returns:
        The page's batch and drift.
    """
    path = _path(_field("k", WireType.LONG), _field("v", wire_type))
    return to_batch([{"k": 1}, {"k": 2, "v": value}], path, required={"K"})


# The batch.


def test_a_trades_page_becomes_one_batch_in_field_order(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    rows = json.loads(load_fixture("massive/rest", "trades.json"))["results"]
    batch, drift = to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert batch.schema == pa.schema(
        [
            ("Conditions", pa.list_(pa.int64())),
            ("Exchange", pa.int64()),
            ("Id", pa.string()),
            ("ParticipantTimestamp", pa.int64()),
            ("Price", pa.float64()),
            ("SequenceNumber", pa.int64()),
            ("SipTimestamp", pa.int64()),
            ("Size", pa.float64()),
            ("Tape", pa.int64()),
            ("TrfId", pa.int64()),
        ]
    )
    assert batch.to_pylist() == [
        {
            "Conditions": [12, 41],
            "Exchange": 11,
            "Id": "1",
            "ParticipantTimestamp": 1517562000015577000,
            "Price": 171.55,
            "SequenceNumber": 1063,
            "SipTimestamp": 1517562000016036600,
            "Size": 100.0,
            "Tape": 3,
            "TrfId": None,
        }
    ]
    assert drift == DriftReport()


def test_no_rows_give_an_empty_batch_of_the_full_schema() -> None:
    batch, drift = to_batch([], _TRADES, required={"SipTimestamp"})
    assert batch.num_rows == 0
    assert batch.schema.names[0] == "Conditions"
    assert batch.num_columns == len(_TRADES.fields)
    assert drift == DriftReport()


def test_the_rows_are_not_modified() -> None:
    rows = [{"day": {"c": 1.0}, "zz": 1}]
    to_batch(rows, _path(_field("day.c", WireType.DOUBLE)), required=())
    assert rows == [{"day": {"c": 1.0}, "zz": 1}]


# Nested objects.


def test_dotted_wire_keys_reach_into_nested_objects() -> None:
    path = _path(
        _field("ticker", WireType.STRING),
        _field("day.c", WireType.DOUBLE),
        _field("lastQuote.p.x", WireType.LONG, column="Deep"),
    )
    rows: list[dict[str, object]] = [
        {"ticker": "A", "day": {"c": 1.5}, "lastQuote": {"p": {"x": 7}}},
        {"ticker": "B", "day": None},
        {"ticker": "C", "day": {}, "lastQuote": {}},
    ]
    batch, drift = to_batch(rows, path, required=())
    assert batch.to_pydict() == {
        "Ticker": ["A", "B", "C"],
        "DayC": [1.5, None, None],
        "Deep": [7, None, None],
    }
    assert drift == DriftReport()


def test_a_nested_object_that_is_not_an_object_reads_as_absent_and_is_drift() -> None:
    batch, drift = to_batch([{"day": 5}], _path(_field("day.c", WireType.DOUBLE)), required=())
    assert batch.column(0).to_pylist() == [None]
    assert drift == DriftReport(unconvertible=frozenset({"day"}))
    assert drift.samples["day"] == DriftSample(None, "5")


def test_a_row_whose_nested_object_is_not_one_is_logged_whole_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows: list[dict[str, object]] = [{"day": {"c": 1.0}}, {"day": 5, "note": "x"}]
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        batch, _ = to_batch(rows, _path(_field("day.c", WireType.DOUBLE)), required=())
    assert batch.column(0).to_pylist() == [1.0, None]
    assert caplog.messages == [
        "/v3/x: reading 2 rows one by one: the wire key 'day' holds a value that is not an object",
        ('/v3/x: row 1: wire key \'day\' expects an object, got 5; the row: {"day":5,"note":"x"}'),
    ]


def test_a_required_key_under_an_object_that_is_not_one_drops_the_row() -> None:
    rows: list[dict[str, object]] = [{"day": {"c": 1.0}}, {"day": 5}]
    path = _path(_field("day.c", WireType.DOUBLE))
    batch, drift = to_batch(rows, path, required={"DayC"})
    assert batch.column(0).to_pylist() == [1.0]
    assert drift.dropped_rows == 1
    assert drift.dropped_reason == (
        "/v3/x: row 1: no DayC (wire key 'day.c'), which the dataset needs"
    )


def test_a_dropped_row_is_not_serialized_with_debug_off(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(_row: object) -> str:
        raise AssertionError("the row was serialized with DEBUG off")

    monkeypatch.setattr(batch_module, "row_excerpt", refuse)
    with caplog.at_level(logging.INFO, logger=batch_module.__name__):
        batch, drift = to_batch([{"price": 1.0}], _TRADES, required={"SipTimestamp"})
    assert (batch.num_rows, drift.dropped_rows) == (0, 1)


def test_a_json_text_field_keeps_its_object_whole() -> None:
    path = _path(_field("insights", WireType.JSON_TEXT))
    batch, drift = to_batch([{"insights": [{"a": 1, "b": "é"}]}], path, required=())
    assert batch.column(0).to_pylist() == ['[{"a":1,"b":"é"}]']
    assert drift == DriftReport()


# Drift.


def test_extra_keys_are_dropped_and_reported_by_their_dotted_key() -> None:
    path = _path(_field("day.c", WireType.DOUBLE))
    rows: list[dict[str, object]] = [
        {"day": {"c": 1.0, "zz": 2}, "new": {"a": 1, "b": 2}},
        {"other": None, "new": 5},
    ]
    batch, drift = to_batch(rows, path, required=())
    assert batch.schema.names == ["DayC"]
    assert drift == DriftReport(extra=frozenset({"day.zz", "new", "other"}))
    # Each extra key's first value, quoted; a REST page leaves the symbol to the caller.
    assert dict(drift.samples) == {
        "day.zz": DriftSample(None, "2"),
        "new": DriftSample(None, '{"a": 1, "b": 2}'),
        "other": DriftSample(None, "null"),
    }


def test_a_missing_vendor_required_field_is_reported() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": 1, "price": 1.0},
        {"sip_timestamp": 2, "price": None},
    ]
    batch, drift = to_batch(rows, _TRADES, required=())
    assert batch.column(4).to_pylist() == [1.0, None]
    assert drift == DriftReport(missing=frozenset({"price"}))


def test_a_missing_optional_field_is_null_and_silent() -> None:
    batch, drift = to_batch([{"price": 1.0, "sip_timestamp": 1}], _TRADES, required=())
    assert batch.column(1).to_pylist() == [None]
    assert drift == DriftReport()


@pytest.mark.parametrize("row", [{"price": 1.0}, {"price": 1.0, "sip_timestamp": None}])
def test_a_row_missing_a_required_key_is_dropped_and_counted(row: dict[str, object]) -> None:
    rows: list[dict[str, object]] = [{"price": 1.0, "sip_timestamp": 1}, row, row]
    batch, drift = to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert batch.column("SipTimestamp").to_pylist() == [1]
    assert drift == DriftReport(dropped_rows=2)
    # The first dropped row's reason, naming the path, the row, and the key.
    assert drift.dropped_reason == (
        "/v3/trades/{stockTicker}: row 1: no SipTimestamp (wire key 'sip_timestamp'), "
        "which the dataset needs"
    )


def test_a_required_key_that_does_not_fit_drops_its_row() -> None:
    rows: list[dict[str, object]] = [{"sip_timestamp": 4.0}, {"sip_timestamp": 7}]
    batch, drift = to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert batch.column("SipTimestamp").to_pylist() == [7]
    assert drift.dropped_rows == 1
    assert drift.unconvertible == frozenset()  # a dropped row's value is no drift of a field
    assert drift.dropped_reason == (
        "/v3/trades/{stockTicker}: row 0: SipTimestamp (wire key 'sip_timestamp') expects "
        "epoch, got 4.0"
    )


def test_a_page_whose_every_row_is_dropped_is_returned_empty() -> None:
    # Whether that is an error is the library's decision, which knows the whole page.
    batch, drift = to_batch([{"price": 1.0}], _TRADES, required={"SipTimestamp"})
    assert batch.num_rows == 0
    assert drift.dropped_rows == 1


def test_a_required_column_the_path_lacks_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match=r"has no columns \['Symbol'\]"):
        to_batch([], _TRADES, required={"Symbol", "Price"})


# Types: `spec.convert` checks each value; these tests pin the Arrow column it lands in.


@pytest.mark.parametrize(
    ("wire_type", "value", "arrow_type", "stored"),
    [
        (WireType.LONG, 2**63 - 1, pa.int64(), 2**63 - 1),
        (WireType.EPOCH, 1517562000016, pa.int64(), 1517562000016),
        (WireType.DOUBLE, 3, pa.float64(), 3.0),
        (WireType.BOOL, False, pa.bool_(), False),
        (WireType.STRING, "x", pa.string(), "x"),
        (WireType.DATE, "2026-02-28", pa.date32(), date(2026, 2, 28)),
        (WireType.LONG_ARRAY, [1, -(2**40)], pa.list_(pa.int64()), [1, -(2**40)]),
        (WireType.STRING_ARRAY, ["a", "b"], pa.list_(pa.string()), ["a", "b"]),
        (WireType.JSON_TEXT, {"k": [1]}, pa.string(), '{"k":[1]}'),
    ],
)
def test_each_wire_type_lands_in_its_arrow_type(
    wire_type: WireType, value: object, *, arrow_type: pa.DataType, stored: object
) -> None:
    batch, _ = to_batch([{"v": value}], _path(_field("v", wire_type)), required=())
    assert batch.schema.field(0).type == arrow_type
    assert batch.column(0).to_pylist() == [stored]


def test_an_iso_instant_lands_as_exact_utc_nanoseconds() -> None:
    path = _path(_field("v", WireType.ISO_INSTANT))
    rows: list[dict[str, object]] = [{"v": "2026-01-02T03:04:05.123456789+01:00"}, {"v": None}]
    batch, _ = to_batch(rows, path, required=())
    assert batch.schema.field(0).type == pa.timestamp("ns", tz="UTC")
    assert batch.column(0).cast(pa.int64()).to_pylist() == [1767319445_123456789, None]


@pytest.mark.parametrize(
    ("wire_type", "value"),
    [
        (WireType.LONG, 1.0),
        (WireType.LONG, True),
        (WireType.DOUBLE, True),
        (WireType.BOOL, 1),
        (WireType.ISO_INSTANT, "2026-01-02T03:04:05"),
        (WireType.ISO_INSTANT, "9999-12-31T00:00:00Z"),
        (WireType.LONG_ARRAY, [2**63]),
        (WireType.LONG_ARRAY, [1.0]),
        (WireType.STRING_ARRAY, ["a", 1]),
        (WireType.STRING_ARRAY, "ab"),
    ],
)
def test_a_value_that_does_not_fit_reads_as_null_and_is_drift(
    wire_type: WireType, value: object
) -> None:
    batch, drift = _odd(wire_type, value)
    assert batch.to_pydict() == {"K": [1, 2], "V": [None, None]}
    assert drift == DriftReport(unconvertible=frozenset({"v"}))
    assert drift.samples["v"] == DriftSample(None, json.dumps(value))


def test_an_odd_value_is_quoted_to_200_characters_and_its_row_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The clause after the row is the one `spec.convert` shares with the WebSocket decoder.
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        _, drift = _odd(WireType.LONG, "x" * 300)
    assert drift.samples["v"] == DriftSample(None, f'"{"x" * 199}')
    assert caplog.messages[1].startswith(
        f"/v3/x: row 1: V (wire key 'v') expects long, got \"{'x' * 199}; the row: "
    )


@pytest.mark.parametrize(
    ("row", "reason", "problem"),
    [
        (
            {"price": "1.5x", "sip_timestamp": 1},
            "the column Price holds a value that needs its own check",
            "Price (wire key 'price') expects double, got \"1.5x\"",
        ),
        (
            {"price": 1.0},
            "the required column SipTimestamp is absent or null in some row",
            "no SipTimestamp (wire key 'sip_timestamp'), which the dataset needs",
        ),
    ],
)
def test_a_row_that_cannot_be_read_is_logged_whole_at_debug(
    row: dict[str, object], *, reason: str, problem: str, caplog: pytest.LogCaptureFixture
) -> None:
    # The page is first read by column, which gives up and says why; the row-by-row reading
    # then finds the odd row and logs it whole.
    rows: list[dict[str, object]] = [{"price": 1.0, "sip_timestamp": 1}, row]
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        to_batch(rows, _TRADES, required={"SipTimestamp"})
    whole = json.dumps(row, separators=(",", ":"))
    assert caplog.messages == [
        f"/v3/trades/{{stockTicker}}: reading 2 rows one by one: {reason}",
        f"/v3/trades/{{stockTicker}}: row 1: {problem}; the row: {whole}",
    ]


def test_a_declared_conversion_is_applied_and_named_on_failure() -> None:
    # A timestamp the vendor's spec leaves as plain text, decoded as the instant its samples
    # show; a value that is not one fails, naming the conversion.
    when = Field(
        wire_key="serverTime",
        column="ServerTime",
        wire_type=WireType.ISO_INSTANT,
        conversion=Conversion.ISO_TEXT_INSTANT,
    )
    batch, _ = to_batch([{"serverTime": "1970-01-01T00:00:01Z"}], _path(when), required=())
    assert batch.column(0).cast(pa.int64()).to_pylist() == [1_000_000_000]
    _, drift = to_batch([{"serverTime": "soon"}], _path(when), required={"ServerTime"})
    assert "iso-instant (by the conversion iso-text-instant)" in drift.dropped_reason


# Reading by column and row by row: one result.

_NESTED = _path(
    _field("ticker", WireType.STRING, required=True),
    _field("day.c", WireType.DOUBLE),
    _field("day.v", WireType.LONG, required=True),
    _field("last.quote.p", WireType.DOUBLE),
    _field("last.when", WireType.DATE),
    _field("codes", WireType.LONG_ARRAY),
    _field("names", WireType.STRING_ARRAY),
    _field("open", WireType.BOOL),
    _field("raw", WireType.JSON_TEXT),
    _field("at", WireType.ISO_INSTANT),
)

_PAGES: list[list[dict[str, object]]] = [
    [],
    [
        {
            "ticker": "A",
            "day": {"c": 1.5, "v": 7},
            "last": {"quote": {"p": 2}, "when": "2026-01-02"},
            "codes": [1, None, -(2**63)],
            "names": ["x", None],
            "open": True,
            "raw": {"k": [1]},
            "at": "2026-01-02T03:04:05Z",
        },
        # A null and an absent object leave their fields null; null list items are kept.
        {"ticker": "B", "day": None, "codes": [], "names": None, "open": False, "raw": None},
        {"ticker": None, "last": {"quote": None}, "codes": None},
    ],
    [
        # Undeclared keys at the top, inside a declared object, and a whole undeclared object;
        # the first row that holds each gives its sample.
        {"ticker": "A", "note": 1, "day": {"c": 1.0, "x": "first"}},
        {"ticker": "B", "note": 2, "day": {"x": "second"}, "extra": {"deep": True}},
        {"ticker": "C", "last": {"quote": {"p": 3.0, "s": 1}, "flag": None}},
    ],
]


@pytest.mark.parametrize("rows", _PAGES, ids=["empty", "nested", "drift"])
def test_reading_by_column_matches_reading_row_by_row(
    rows: list[dict[str, object]], caplog: pytest.LogCaptureFixture
) -> None:
    # Every page here is read by column; the row-by-row reading, the reference, agrees on the
    # batch and on the drift.
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        batch, drift = to_batch(rows, _NESTED, required={"Ticker"} if not rows else ())
    assert caplog.messages == []  # read by column: no page fell back
    reading = batch_module._by_row(
        rows, _NESTED, objects=batch_module._prefixes(_NESTED.fields), required=()
    )
    assert batch.columns == reading.arrays
    assert (reading.unconvertible, reading.dropped) == ({}, 0)
    assert drift == DriftReport(
        extra=frozenset(reading.extra),
        missing=frozenset(reading.missing),
        samples={key: DriftSample(None, json.dumps(v)) for key, v in reading.extra.items()},
    )


def test_the_drift_of_both_readings_names_dotted_keys_and_first_samples() -> None:
    _, drift = to_batch(_PAGES[2], _NESTED, required=())
    assert drift.extra == {"note", "day.x", "extra", "last.quote.s", "last.flag"}
    assert drift.missing == {"day.v"}
    assert drift.samples["day.x"] == DriftSample(None, '"first"')
    assert drift.samples["extra"] == DriftSample(None, '{"deep": true}')


def test_an_integer_a_double_cannot_hold_exactly_is_read_row_by_row(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrow refuses an integer it would round; the converter rounds it, as a JSON number is.
    big = 2**60 + 1
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        path = _path(_field("v", WireType.DOUBLE))
        batch, _ = to_batch([{"v": big}, {"v": 2}], path, required=())
    assert batch.column(0).to_pylist() == [float(big), 2.0]
    assert caplog.messages == [
        "/v3/x: reading 2 rows one by one: the column V holds a value that needs its own check"
    ]
