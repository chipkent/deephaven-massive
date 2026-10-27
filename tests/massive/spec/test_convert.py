from __future__ import annotations

from datetime import date

import pytest

from deephaven_massive._massive.spec import convert as convert_module
from deephaven_massive._massive.spec.convert import (
    UnconvertibleError,
    convert,
    convert_field,
)
from deephaven_massive._massive.spec.types import Conversion, Field, WireType, WireUnit


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


# Named conversions.


def _with(conversion: Conversion) -> Field:
    wire_type = conversion.wire_type
    unit = WireUnit.NS if wire_type is WireType.EPOCH else None
    return Field(wire_key="x", column="X", wire_type=wire_type, unit=unit, conversion=conversion)


def test_a_field_without_a_conversion_converts_by_its_wire_type() -> None:
    plain = Field(wire_key="x", column="X", wire_type=WireType.DATE)
    assert convert_field(plain, "2025-02-22") == date(2025, 2, 22)
    with pytest.raises(UnconvertibleError):
        convert_field(plain, 7)


_CONVERSIONS = [
    (Conversion.ISO_TEXT_INSTANT, "1970-01-01T00:00:01Z", 1_000_000_000),
    (Conversion.ISO_DATE_TEXT, "2025-02-22", date(2025, 2, 22)),
    (Conversion.QUOTED_INTEGER, "48", 48),
    (Conversion.INTEGER_ID_AS_TEXT, 7, "7"),
    (Conversion.INTEGER_OR_TEXT_ID, "2208", "2208"),  # the documented form
    (Conversion.INTEGER_OR_TEXT_ID, 2208, "2208"),  # the declared form
    (Conversion.TEXT_OR_INTEGER_ID, "517478762", "517478762"),
    (Conversion.TEXT_OR_INTEGER_ID, 413131, "413131"),  # an identifier sent as an integer
    (Conversion.NUMBER_LABEL, "1.0", "1.0"),
    (Conversion.NUMBER_LABEL, 1.0, "1.0"),
    (Conversion.NUMBER_LABEL, 2, "2.0"),  # one spelling for a whole-number version
    (Conversion.ONE_CODE, 12, [12]),
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00.000Z", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00+00:00", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "1969-12-31T00:00:00z", date(1969, 12, 31)),
    (Conversion.DATE_AS_EPOCH, 1_753_851_600_000_000_000, 1_753_851_600_000_000_000),
]


@pytest.mark.parametrize("case", _CONVERSIONS, ids=lambda case: f"{case[0]}-{case[1]!r}")
def test_each_conversion_converts(case: tuple[Conversion, object, object]) -> None:
    conversion, value, expected = case
    assert convert_field(_with(conversion), value) == expected


@pytest.mark.parametrize(
    ("conversion", "value"),
    [
        (Conversion.ISO_TEXT_INSTANT, "2025-02-22"),  # a date is not an instant
        (Conversion.ISO_DATE_TEXT, "2025-02-22T00:00:00Z"),  # nor an instant a date
        (Conversion.QUOTED_INTEGER, "4.8"),
        (Conversion.QUOTED_INTEGER, "-1"),
        (Conversion.QUOTED_INTEGER, " 48"),
        (Conversion.QUOTED_INTEGER, 48),  # the declared form is text
        (Conversion.QUOTED_INTEGER, str(2**63)),
        (Conversion.INTEGER_ID_AS_TEXT, "7"),  # integer only: text fails loudly
        (Conversion.INTEGER_ID_AS_TEXT, True),
        (Conversion.INTEGER_OR_TEXT_ID, 7.5),
        (Conversion.INTEGER_OR_TEXT_ID, True),
        (Conversion.TEXT_OR_INTEGER_ID, False),
        (Conversion.TEXT_OR_INTEGER_ID, 2**63),
        (Conversion.NUMBER_LABEL, True),
        (Conversion.NUMBER_LABEL, None),
        (Conversion.NUMBER_LABEL, 10**400),  # too large for a double
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T05:00:00Z"),  # a time that would be lost
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00.001Z"),
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00"),  # no offset
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00-05:00"),  # midnight elsewhere
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T01:00:00+01:00"),  # the same instant
        (Conversion.MIDNIGHT_UTC_DATE, "2023-03-09T19:00:00-05:00"),  # written on the 9th
        (Conversion.MIDNIGHT_UTC_DATE, "2023-02-30T00:00:00Z"),  # no such date
        (Conversion.MIDNIGHT_UTC_DATE, 1_678_406_400),
        (Conversion.DATE_AS_EPOCH, "2025-07-30"),
        (Conversion.DATE_AS_EPOCH, 2**63),
        (Conversion.ONE_CODE, 2**31),
        (Conversion.ONE_CODE, [12]),
    ],
)
def test_a_value_the_conversion_cannot_read_fails(conversion: Conversion, value: object) -> None:
    with pytest.raises(UnconvertibleError):
        convert_field(_with(conversion), value)


# A value the plain converter of each conversion's declared type refuses, and the conversion
# accepts, or (for a conversion from untyped text) the value the conversion retypes.
_NOT_A_NO_OP = {
    Conversion.ISO_TEXT_INSTANT: "1970-01-01T00:00:01Z",
    Conversion.ISO_DATE_TEXT: "2025-02-22",
    Conversion.QUOTED_INTEGER: "48",
    Conversion.INTEGER_ID_AS_TEXT: 7,
    Conversion.INTEGER_OR_TEXT_ID: "2208",
    Conversion.TEXT_OR_INTEGER_ID: 413131,
    Conversion.NUMBER_LABEL: "1.0",
    Conversion.ONE_CODE: 12,
    Conversion.MIDNIGHT_UTC_DATE: "2023-03-10T00:00:00Z",
    Conversion.DATE_AS_EPOCH: 1,
}


@pytest.mark.parametrize("conversion", list(Conversion))
def test_no_conversion_is_a_no_op(conversion: Conversion) -> None:
    # Each conversion changes what is accepted or what comes out, so no field ever declares
    # a conversion that leaves its values as the plain converter would.
    value = _NOT_A_NO_OP[conversion]
    converted = convert_field(_with(conversion), value)
    try:
        plain = convert(conversion.declared, value)
    except UnconvertibleError:
        return
    assert (plain, conversion.declared) != (converted, conversion.wire_type)
