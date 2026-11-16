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
    RowFault,
    RowMark,
    RowMarker,
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
        The page's batch and drift; the row is marked for its value.
    """
    path = _path(_field("k", WireType.LONG), _field("v", wire_type))
    batch, drift, marks = to_batch([{"k": 1}, {"k": 2, "v": value}], path, required={"K"})
    [(fault, mark)] = marks.items()
    assert (fault, mark.rows, mark.fields) == (RowFault.UNCONVERTIBLE_VALUES, (1,), ("V",))
    assert mark.reasons[0].startswith("/v3/x: row 1: V (wire key 'v') expects")
    return batch, drift


# The batch.


def test_a_trades_page_becomes_one_batch_in_field_order(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    rows = json.loads(load_fixture("massive/rest", "trades.json"))["results"]
    batch, drift, _marks = to_batch(rows, _TRADES, required={"SipTimestamp"})
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
    batch, drift, _marks = to_batch([], _TRADES, required={"SipTimestamp"})
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
    batch, drift, _marks = to_batch(rows, path, required=())
    assert batch.to_pydict() == {
        "Ticker": ["A", "B", "C"],
        "DayC": [1.5, None, None],
        "Deep": [7, None, None],
    }
    assert drift == DriftReport()


def test_a_nested_object_that_is_not_an_object_reads_as_absent_and_is_drift() -> None:
    batch, drift, _marks = to_batch(
        [{"day": 5}], _path(_field("day.c", WireType.DOUBLE)), required=()
    )
    assert batch.column(0).to_pylist() == [None]
    assert drift == DriftReport(unconvertible=frozenset({"day"}))
    assert drift.samples["day"] == DriftSample(None, "5")


def test_a_row_whose_nested_object_is_not_one_is_logged_whole_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows: list[dict[str, object]] = [{"day": {"c": 1.0}}, {"day": 5, "note": "x"}]
    with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
        batch, _, _ = to_batch(rows, _path(_field("day.c", WireType.DOUBLE)), required=())
    assert batch.column(0).to_pylist() == [1.0, None]
    assert caplog.messages == [
        "/v3/x: reading 2 rows one by one: the wire key 'day' holds a value that is not an object",
        ('/v3/x: row 1: wire key \'day\' expects an object, got 5; the row: {"day":5,"note":"x"}'),
    ]


def test_a_required_key_under_an_object_that_is_not_one_marks_the_row() -> None:
    rows: list[dict[str, object]] = [{"day": {"c": 1.0}}, {"day": 5}]
    path = _path(_field("day.c", WireType.DOUBLE))
    batch, drift, marks = to_batch(rows, path, required={"DayC"})
    assert batch.column(0).to_pylist() == [1.0, None]
    assert marks == {
        RowFault.UNREADABLE_KEYS: RowMark(
            rows=(1,),
            fields=("DayC",),
            reasons=("/v3/x: row 1: no DayC (wire key 'day.c'), which the dataset needs",),
            symbols=(None,),
        )
    }
    assert drift == DriftReport()  # the row's fault is its key: its object is no drift


def test_a_dropped_row_is_not_serialized_with_debug_off(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(_row: object) -> str:
        raise AssertionError("the row was serialized with DEBUG off")

    monkeypatch.setattr(batch_module, "row_excerpt", refuse)
    with caplog.at_level(logging.INFO, logger=batch_module.__name__):
        batch, _drift, marks = to_batch([{"price": 1.0}], _TRADES, required={"SipTimestamp"})
    assert (batch.num_rows, marks[RowFault.UNREADABLE_KEYS].rows) == (1, (0,))


def test_a_json_text_field_keeps_its_object_whole() -> None:
    path = _path(_field("insights", WireType.JSON_TEXT))
    batch, drift, _marks = to_batch([{"insights": [{"a": 1, "b": "é"}]}], path, required=())
    assert batch.column(0).to_pylist() == ['[{"a":1,"b":"é"}]']
    assert drift == DriftReport()


# Drift.


def test_extra_keys_are_dropped_and_reported_by_their_dotted_key() -> None:
    path = _path(_field("day.c", WireType.DOUBLE))
    rows: list[dict[str, object]] = [
        {"day": {"c": 1.0, "zz": 2}, "new": {"a": 1, "b": 2}},
        {"other": None, "new": 5},
    ]
    batch, drift, _marks = to_batch(rows, path, required=())
    assert batch.schema.names == ["DayC"]
    assert drift == DriftReport(extra=frozenset({"day.zz", "new", "other"}))
    # Each extra key's first value, quoted; a REST page leaves the symbol to the caller.
    assert dict(drift.samples) == {
        "day.zz": DriftSample(None, "2"),
        "new": DriftSample(None, '{"a": 1, "b": 2}'),
        "other": DriftSample(None, "null"),
    }


def test_an_ignored_key_is_dropped_without_drift(caplog: pytest.LogCaptureFixture) -> None:
    # The financial statements as Massive sent them on 2026-11-02, with two keys the library
    # ignores beside one it does not know.
    path = RestPath(
        key="/stocks/financials/v1/balance-sheets",
        fields=(_field("cik", WireType.STRING),),
        ignored_keys=frozenset({"max_ticker", "min_ticker"}),
    )
    rows: list[dict[str, object]] = [
        {"cik": "0000320193", "max_ticker": "AAPL", "min_ticker": "AAPL", "zz": 1},
        {"cik": "0000320193"},
    ]
    for page in (rows, rows[1:]):
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=batch_module.__name__):
            batch, drift, _marks = to_batch(page, path, required=())
        assert batch.column("Cik").to_pylist() == ["0000320193"] * len(page)
        assert drift.extra == ({"zz"} if len(page) == 2 else set())
        assert set(drift.samples) == drift.extra
        dropped = "/stocks/financials/v1/balance-sheets: dropped the keys it ignores"
        assert (dropped in caplog.text) == (len(page) == 2)


def test_a_missing_vendor_required_field_is_reported() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": 1, "price": 1.0},
        {"sip_timestamp": 2, "price": None},
    ]
    batch, drift, _marks = to_batch(rows, _TRADES, required=())
    assert batch.column(4).to_pylist() == [1.0, None]
    assert drift == DriftReport(missing=frozenset({"price"}))


def test_a_missing_optional_field_is_null_and_silent() -> None:
    batch, drift, _marks = to_batch([{"price": 1.0, "sip_timestamp": 1}], _TRADES, required=())
    assert batch.column(1).to_pylist() == [None]
    assert drift == DriftReport()


@pytest.mark.parametrize("row", [{"price": 1.0}, {"price": 1.0, "sip_timestamp": None}])
def test_a_row_missing_a_required_key_is_kept_and_marked(row: dict[str, object]) -> None:
    rows: list[dict[str, object]] = [{"price": 1.0, "sip_timestamp": 1}, row, row]
    batch, drift, marks = to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert batch.column("SipTimestamp").to_pylist() == [1, None, None]
    assert drift == DriftReport()
    # Each row's reason names the path, the row, and the key.
    reason = "no SipTimestamp (wire key 'sip_timestamp'), which the dataset needs"
    assert marks == {
        RowFault.UNREADABLE_KEYS: RowMark(
            rows=(1, 2),
            fields=("SipTimestamp", "SipTimestamp"),
            reasons=(
                f"/v3/trades/{{stockTicker}}: row 1: {reason}",
                f"/v3/trades/{{stockTicker}}: row 2: {reason}",
            ),
            symbols=(None, None),
        )
    }


def test_a_required_key_that_does_not_fit_marks_its_row_whatever_else_is_odd() -> None:
    # The first row's price does not fit either, but the row has one fault, its key, and its
    # price is no drift of the field.
    rows: list[dict[str, object]] = [
        {"sip_timestamp": 4.0, "price": "x", "ticker": "AAPL"},
        {"sip_timestamp": 7},
    ]
    path = RestPath(
        key=_TRADES.key,
        fields=(*_TRADES.fields, _field("ticker", WireType.STRING)),
        params=_TRADES.params,
    )
    batch, drift, marks = to_batch(rows, path, required={"SipTimestamp"}, symbol="Ticker")
    assert batch.column("SipTimestamp").to_pylist() == [None, 7]
    assert drift.unconvertible == frozenset()
    assert marks == {
        RowFault.UNREADABLE_KEYS: RowMark(
            rows=(0,),
            fields=("SipTimestamp",),
            reasons=(
                (
                    "/v3/trades/{stockTicker}: row 0: SipTimestamp (wire key 'sip_timestamp') "
                    "expects epoch, got 4.0"
                ),
            ),
            symbols=("AAPL",),
        )
    }


def test_a_symbol_column_the_path_lacks_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match=r"has no columns \['Nope'\]"):
        to_batch([], _TRADES, required=(), symbol="Nope")


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
    batch, _, _ = to_batch([{"v": value}], _path(_field("v", wire_type)), required=())
    assert batch.schema.field(0).type == arrow_type
    assert batch.column(0).to_pylist() == [stored]


def test_an_iso_instant_lands_as_exact_utc_nanoseconds() -> None:
    path = _path(_field("v", WireType.ISO_INSTANT))
    rows: list[dict[str, object]] = [{"v": "2026-01-02T03:04:05.123456789+01:00"}, {"v": None}]
    batch, _, _ = to_batch(rows, path, required=())
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
    batch, _, _ = to_batch([{"serverTime": "1970-01-01T00:00:01Z"}], _path(when), required=())
    assert batch.column(0).cast(pa.int64()).to_pylist() == [1_000_000_000]
    _, _, marks = to_batch([{"serverTime": "soon"}], _path(when), required={"ServerTime"})
    [reason] = marks[RowFault.UNREADABLE_KEYS].reasons
    assert "iso-instant (by the conversion iso-text-instant)" in reason


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
        batch, drift, marks = to_batch(rows, _NESTED, required={"Ticker"} if not rows else ())
    assert caplog.messages == []  # read by column: no page fell back
    marker = RowMarker()
    reading = batch_module._by_row(
        rows,
        _NESTED,
        objects=batch_module._prefixes(_NESTED.fields),
        required=(),
        symbol=None,
        marker=marker,
    )
    assert batch.columns == reading.arrays
    assert (reading.unconvertible, marks, marker.marks()) == ({}, {}, {})
    assert drift == DriftReport(
        extra=frozenset(reading.extra),
        missing=frozenset(reading.missing),
        samples={key: DriftSample(None, json.dumps(v)) for key, v in reading.extra.items()},
    )


def test_the_drift_of_both_readings_names_dotted_keys_and_first_samples() -> None:
    _, drift, _ = to_batch(_PAGES[2], _NESTED, required=())
    assert drift.extra == {"note", "day.x", "extra", "last.quote.s", "last.flag"}
    assert drift.missing == {"day.v"}
    assert drift.samples["day.x"] == DriftSample(None, '"first"')
    assert drift.samples["extra"] == DriftSample(None, '{"deep": true}')


def test_an_integer_a_double_cannot_hold_exactly_marks_its_row() -> None:
    # A double would round it, so it is no value Massive sent: its row is marked, and a large
    # integer a double holds exactly still reads.
    path = _path(_field("v", WireType.DOUBLE))
    rows: list[dict[str, object]] = [{"v": 2**60 + 1}, {"v": 2**60}, {"v": 2}]
    batch, drift, marks = to_batch(rows, path, required=())
    assert batch.column(0).to_pylist() == [None, 2.0**60, 2.0]
    assert drift.unconvertible == frozenset({"v"})
    [(fault, mark)] = marks.items()
    assert (fault, mark.rows) == (RowFault.UNCONVERTIBLE_VALUES, (0,))
