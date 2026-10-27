from __future__ import annotations

from datetime import date

import pytest

from deephaven_massive._massive.spec import convert as convert_module
from deephaven_massive._massive.spec.convert import (
    DECLARED_CONVERSIONS,
    UnconvertibleError,
    convert,
    convert_field,
)
from deephaven_massive._massive.spec.types import Field, WireType


def _refused(wire_type: WireType, value: object) -> None:
    with pytest.raises(UnconvertibleError):
        convert(wire_type, value)


def test_every_wire_type_has_a_converter() -> None:
    assert set(convert_module._CONVERTERS) == set(WireType)


def test_unconvertible_is_a_value_error() -> None:
    assert issubclass(UnconvertibleError, ValueError)


@pytest.mark.parametrize("wire_type", [WireType.LONG, WireType.EPOCH])
def test_a_long_takes_the_signed_64_bit_range(wire_type: WireType) -> None:
    assert convert(wire_type, 2**63 - 1) == 2**63 - 1
    assert convert(wire_type, -(2**63)) == -(2**63)
    for value in (2**63, -(2**63) - 1, 1.0, True, "5", None):
        _refused(wire_type, value)


def test_a_double_takes_any_number_but_not_a_bool() -> None:
    assert convert(WireType.DOUBLE, 3) == 3.0
    assert type(convert(WireType.DOUBLE, 3)) is float
    assert convert(WireType.DOUBLE, 2.5) == 2.5
    for value in (False, "2.5", 10**400, [1.0]):
        _refused(WireType.DOUBLE, value)


def test_a_bool_takes_only_true_or_false() -> None:
    assert convert(WireType.BOOL, value=True) is True
    assert convert(WireType.BOOL, value=False) is False
    for value in (1, 0, "true"):
        _refused(WireType.BOOL, value)


def test_a_string_takes_only_text() -> None:
    assert convert(WireType.STRING, "x") == "x"
    for value in (1, {"a": 1}):
        _refused(WireType.STRING, value)


def test_a_date_takes_iso_calendar_dates() -> None:
    assert convert(WireType.DATE, "2026-02-28") == date(2026, 2, 28)
    fullwidth = "".join(chr(0xFF10 + int(digit)) for digit in "2026") + "-02-28"
    for value in ("2026-02-30", "20260228", "2026-02-28T00:00:00Z", fullwidth, 20260228):
        _refused(WireType.DATE, value)


@pytest.mark.parametrize(
    ("text", "nanos"),
    [
        ("1970-01-01T00:00:00Z", 0),
        ("2026-01-02T03:04:05Z", 1767323045_000000000),
        ("2026-01-02T03:04:05.123456789Z", 1767323045_123456789),
        ("2026-01-02T03:04:05.000000001Z", 1767323045_000000001),
        ("2026-01-02T03:04:05.5+00:00", 1767323045_500000000),
        ("2026-01-02T03:04:05-05:00", 1767341045_000000000),
        ("2026-01-02T03:04:05+05:30", 1767303245_000000000),
        ("2026-01-02t03:04:05z", 1767323045_000000000),
        ("1969-12-31T23:59:59.999999999Z", -1),
        ("2262-04-11T23:47:16.854775807Z", 2**63 - 1),
        ("1677-09-21T00:12:43.145224192Z", -(2**63)),
    ],
)
def test_an_iso_instant_is_exact_integer_nanoseconds(text: str, nanos: int) -> None:
    assert convert(WireType.ISO_INSTANT, text) == nanos


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-02T03:04:05",
        "2026-01-02T03:04:05.123",
        "2026-01-02 03:04:05Z",
        "2026-01-02T03:04Z",
        "2026-01-02T03:04:05.Z",
        "2026-01-02T03:04:05.1234567890Z",
        "2026-01-02T03:04:05+0100",
        "2026-02-30T03:04:05Z",
        "2026-01-02T24:00:00Z",
        "2026-01-02T23:60:00Z",
        "2026-01-02T23:59:60Z",
        "2026-01-02T03:04:05+24:00",
        "2026-01-02T03:04:05+01:60",
        "2262-04-11T23:47:16.854775808Z",
        "1677-09-21T00:12:43.145224191Z",
        "9999-12-31T00:00:00Z",
        "0001-01-01T00:00:00Z",
        1767323045,
    ],
)
def test_an_iso_instant_needs_a_real_time_with_seconds_and_an_offset(value: object) -> None:
    _refused(WireType.ISO_INSTANT, value)


def test_an_int_array_takes_lists_of_32_bit_integers() -> None:
    assert convert(WireType.INT_ARRAY, [2**31 - 1, -(2**31)]) == [2**31 - 1, -(2**31)]
    assert convert(WireType.INT_ARRAY, []) == []
    for value in ([2**31], [-(2**31) - 1], [1, True], [1.0], 5, "1,2"):
        _refused(WireType.INT_ARRAY, value)


def test_json_text_is_compact_and_keeps_order_and_unicode() -> None:
    assert convert(WireType.JSON_TEXT, "a") == '"a"'
    assert convert(WireType.JSON_TEXT, {"k": [1, 2.5, True], "a": "é"}) == (
        '{"k":[1,2.5,true],"a":"é"}'
    )


# Declared conversions.


def _declared(declared: WireType, wire_type: WireType) -> Field:
    unit = None
    return Field(wire_key="x", column="X", wire_type=wire_type, unit=unit, declared_type=declared)


def test_a_field_without_a_declared_type_converts_by_its_wire_type() -> None:
    plain = Field(wire_key="x", column="X", wire_type=WireType.DATE)
    assert convert_field(plain, "2025-02-22") == date(2025, 2, 22)
    with pytest.raises(UnconvertibleError):
        convert_field(plain, 7)


_CONVERSIONS = [
    (WireType.STRING, WireType.ISO_INSTANT, "1970-01-01T00:00:01Z", 1_000_000_000),
    (WireType.ISO_INSTANT, WireType.DATE, "2025-02-22", date(2025, 2, 22)),
    (WireType.STRING, WireType.LONG, "48", 48),
    (WireType.LONG, WireType.STRING, 7, "7"),
    (WireType.LONG, WireType.INT_ARRAY, 12, [12]),
]


@pytest.mark.parametrize("case", _CONVERSIONS, ids=lambda case: f"{case[0]}-{case[1]}")
def test_each_declared_conversion_converts(
    case: tuple[WireType, WireType, object, object],
) -> None:
    declared, wire_type, value, expected = case
    assert convert_field(_declared(declared, wire_type), value) == expected


@pytest.mark.parametrize(
    ("declared", "wire_type", "value"),
    [
        (WireType.STRING, WireType.ISO_INSTANT, "2025-02-22"),  # a date is not an instant
        (WireType.ISO_INSTANT, WireType.DATE, "2025-02-22T00:00:00Z"),  # nor an instant a date
        (WireType.STRING, WireType.LONG, "4.8"),
        (WireType.STRING, WireType.LONG, "-1"),
        (WireType.STRING, WireType.LONG, " 48"),
        (WireType.STRING, WireType.LONG, 48),  # the declared form is text
        (WireType.STRING, WireType.LONG, str(2**63)),
        (WireType.LONG, WireType.STRING, "7"),
        (WireType.LONG, WireType.STRING, True),
        (WireType.LONG, WireType.INT_ARRAY, 2**31),
        (WireType.LONG, WireType.INT_ARRAY, [12]),
    ],
)
def test_a_value_the_declared_conversion_cannot_read_fails(
    declared: WireType, wire_type: WireType, value: object
) -> None:
    with pytest.raises(UnconvertibleError):
        convert_field(_declared(declared, wire_type), value)


def test_a_declared_conversion_that_does_not_exist_is_refused() -> None:
    odd = _declared(WireType.BOOL, WireType.STRING)
    with pytest.raises(ValueError, match="no declared conversion from bool to string"):
        convert_field(odd, value=True)


def test_the_declared_pairs_are_the_table() -> None:
    assert {
        (WireType.STRING, WireType.ISO_INSTANT),
        (WireType.ISO_INSTANT, WireType.DATE),
        (WireType.STRING, WireType.LONG),
        (WireType.LONG, WireType.STRING),
        (WireType.LONG, WireType.INT_ARRAY),
    } == DECLARED_CONVERSIONS
