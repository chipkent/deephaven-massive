from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.rest.batch import to_batch
from deephaven_massive._massive.spec.types import (
    DriftReport,
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
        _field("conditions", WireType.INT_ARRAY),
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


def _bad(wire_type: WireType, value: object) -> WireError:
    with pytest.raises(WireError) as caught:
        to_batch([{"v": value}], _path(_field("v", wire_type)), required=())
    return caught.value


# The batch.


def test_a_trades_page_becomes_one_batch_in_field_order(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    rows = json.loads(load_fixture("massive/rest", "trades.json"))["results"]
    batch, drift = to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert batch.schema == pa.schema(
        [
            ("Conditions", pa.list_(pa.int32())),
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


def test_a_nested_object_that_is_not_an_object_is_unconvertible() -> None:
    with pytest.raises(WireError, match=r"/v3/x: day must be an object, got 5"):
        to_batch([{"day": 5}], _path(_field("day.c", WireType.DOUBLE)), required=())


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
        {"other": None},
    ]
    batch, drift = to_batch(rows, path, required=())
    assert batch.schema.names == ["DayC"]
    assert drift == DriftReport(extra=frozenset({"day.zz", "new", "other"}))


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
def test_a_missing_required_key_is_an_error_naming_it(row: dict[str, object]) -> None:
    rows: list[dict[str, object]] = [{"price": 1.0, "sip_timestamp": 1}, row]
    with pytest.raises(WireError) as caught:
        to_batch(rows, _TRADES, required={"SipTimestamp"})
    assert str(caught.value) == (
        "/v3/trades/{stockTicker}: row 1 has no SipTimestamp (wire key 'sip_timestamp'), "
        "which the dataset needs"
    )
    assert (caught.value.status, caught.value.path) == (None, "/v3/trades/{stockTicker}")


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
        (WireType.INT_ARRAY, [1, -2], pa.list_(pa.int32()), [1, -2]),
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
        (WireType.BOOL, 1),
        (WireType.ISO_INSTANT, "2026-01-02T03:04:05"),
        (WireType.ISO_INSTANT, "9999-12-31T00:00:00Z"),
        (WireType.INT_ARRAY, [2**31]),
    ],
)
def test_a_value_that_does_not_fit_is_a_wire_error(wire_type: WireType, value: object) -> None:
    assert f"v must be {wire_type}" in str(_bad(wire_type, value))


def test_an_unconvertible_value_names_the_field_type_and_50_characters() -> None:
    error = _bad(WireType.LONG, "x" * 100)
    assert str(error) == f'/v3/x: v must be long, got "{"x" * 49}'
    assert (error.status, error.path, error.host) == (None, "/v3/x", None)
    assert error.__cause__ is None
