from __future__ import annotations

import itertools
from datetime import date
from types import NoneType

import pytest

from deephaven_massive._massive.spec import convert as convert_module
from deephaven_massive._massive.spec.convert import (
    UnconvertibleError,
    column_converter,
    convert,
    convert_field,
    excerpt,
    field_converter,
    missing_key_message,
    row_excerpt,
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


def test_a_long_array_takes_lists_of_64_bit_integers() -> None:
    assert convert(WireType.LONG_ARRAY, [2**63 - 1, -(2**63), 2**31]) == [
        2**63 - 1,
        -(2**63),
        2**31,
    ]
    assert convert(WireType.LONG_ARRAY, []) == []
    assert convert(WireType.LONG_ARRAY, [1, None]) == [1, None]  # a null element stays null
    for value in ([2**63], [-(2**63) - 1], [1, True], [1.0], 5, "1,2"):
        _refused(WireType.LONG_ARRAY, value)


def test_a_string_array_takes_lists_of_text() -> None:
    assert convert(WireType.STRING_ARRAY, ["AAPL", "BRK.B", ""]) == ["AAPL", "BRK.B", ""]
    assert convert(WireType.STRING_ARRAY, []) == []
    assert convert(WireType.STRING_ARRAY, ["a", None]) == ["a", None]  # a null element stays
    for value in (["a", 1], [["a"]], "a", {"a": 1}, 5):
        _refused(WireType.STRING_ARRAY, value)


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
    (Conversion.QUOTED_INTEGER, 48, 48),  # the declared form, which the stream may send
    (Conversion.TEXT_OR_INTEGER_ID, "2208", "2208"),
    (Conversion.TEXT_OR_INTEGER_ID, 2208, "2208"),
    (Conversion.TEXT_OR_INTEGER_ID, "517478762", "517478762"),
    (Conversion.TEXT_OR_INTEGER_ID, 413131, "413131"),  # an identifier sent as an integer
    (Conversion.NUMBER_LABEL, "1.0", "1.0"),
    (Conversion.NUMBER_LABEL, 1.0, "1.0"),
    (Conversion.NUMBER_LABEL, 2, "2.0"),  # one spelling for a whole-number version
    (Conversion.NUMBER_LABEL, 1e20, "1e+20"),
    (Conversion.CODE_LIST_TEXT, 12, [12]),
    (Conversion.CODE_LIST_TEXT, "12", [12]),
    (Conversion.CODE_LIST_TEXT, "12,37", [12, 37]),  # a flat file's list
    (Conversion.CODE_LIST_TEXT, f"{2**31},1", [2**31, 1]),  # any 64-bit code
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00.000Z", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "2023-03-10T00:00:00+00:00", date(2023, 3, 10)),
    (Conversion.MIDNIGHT_UTC_DATE, "1969-12-31T00:00:00z", date(1969, 12, 31)),
    (Conversion.CHICAGO_MIDNIGHT_DATE, 1_753_851_600_000_000_000, date(2025, 7, 30)),  # CDT
    (Conversion.CHICAGO_MIDNIGHT_DATE, 1_765_778_400_000_000_000, date(2025, 12, 15)),  # CST
    (Conversion.CHICAGO_MIDNIGHT_DATE, "2026-12-18", date(2026, 12, 18)),
    (Conversion.INTEGER_AS_NUMBER, 951.6112, 951.6112),  # a crypto volume
    (Conversion.INTEGER_AS_NUMBER, 91, 91.0),
    (Conversion.DECIMAL_TEXT_NUMBER, "7676.5", 7676.5),  # a futures price sent as text
    (Conversion.DECIMAL_TEXT_NUMBER, "7679", 7679.0),
    (Conversion.DECIMAL_TEXT_NUMBER, "-0.25", -0.25),
    (Conversion.DECIMAL_TEXT_NUMBER, 7676.5, 7676.5),  # the declared form
    (Conversion.DECIMAL_TEXT_NUMBER, 91, 91.0),
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
        (Conversion.QUOTED_INTEGER, 48.5),
        (Conversion.QUOTED_INTEGER, True),
        (Conversion.INTEGER_AS_NUMBER, "1.5"),
        (Conversion.INTEGER_AS_NUMBER, True),
        # Text Python's `float` reads but that is no plain decimal.
        (Conversion.DECIMAL_TEXT_NUMBER, "1e5"),
        (Conversion.DECIMAL_TEXT_NUMBER, "NaN"),
        (Conversion.DECIMAL_TEXT_NUMBER, "inf"),
        (Conversion.DECIMAL_TEXT_NUMBER, " 1.5"),
        (Conversion.DECIMAL_TEXT_NUMBER, "+1.5"),
        (Conversion.DECIMAL_TEXT_NUMBER, "1_000"),
        (Conversion.DECIMAL_TEXT_NUMBER, ".5"),
        (Conversion.DECIMAL_TEXT_NUMBER, "5."),
        (Conversion.DECIMAL_TEXT_NUMBER, "\u0667"),  # an Arabic-Indic digit seven
        (Conversion.DECIMAL_TEXT_NUMBER, ""),
        (Conversion.DECIMAL_TEXT_NUMBER, "9" * 400),  # beyond a finite double
        (Conversion.DECIMAL_TEXT_NUMBER, 10**400),
        (Conversion.DECIMAL_TEXT_NUMBER, True),
        (Conversion.DECIMAL_TEXT_NUMBER, [1.5]),
        (Conversion.QUOTED_INTEGER, str(2**63)),
        (Conversion.TEXT_OR_INTEGER_ID, 7.5),
        (Conversion.TEXT_OR_INTEGER_ID, True),
        (Conversion.NUMBER_LABEL, float("nan")),
        (Conversion.NUMBER_LABEL, float("inf")),
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
        (Conversion.CHICAGO_MIDNIGHT_DATE, 1_753_833_600_000_000_000),  # midnight UTC
        (Conversion.CHICAGO_MIDNIGHT_DATE, 1_753_851_600_000_000_001),  # not a whole second
        (Conversion.CHICAGO_MIDNIGHT_DATE, 1_753_851_601_000_000_000),  # a second past
        (Conversion.CHICAGO_MIDNIGHT_DATE, 2**63),
        (Conversion.CHICAGO_MIDNIGHT_DATE, "2025-07-30T00:00:00Z"),
        (Conversion.CHICAGO_MIDNIGHT_DATE, 1.5),
        (Conversion.CODE_LIST_TEXT, 2**63),
        (Conversion.CODE_LIST_TEXT, [12]),
        (Conversion.CODE_LIST_TEXT, "12, 37"),
        (Conversion.CODE_LIST_TEXT, "12,"),
        (Conversion.CODE_LIST_TEXT, "12;37"),
        (Conversion.CODE_LIST_TEXT, f"1,{2**63}"),
    ],
)
def test_a_value_the_conversion_cannot_read_fails(conversion: Conversion, value: object) -> None:
    with pytest.raises(UnconvertibleError):
        convert_field(_with(conversion), value)


# For each conversion and each type a specification may declare for it: a value the conversion
# accepts. For a widening conversion the plain converter of the declared type refuses it; for
# one that only retypes values its declared type accepts, the plain converter accepts it too.
_SAMPLES = {
    (Conversion.ISO_TEXT_INSTANT, WireType.STRING): "1970-01-01T00:00:01Z",
    (Conversion.ISO_DATE_TEXT, WireType.ISO_INSTANT): "2025-02-22",
    (Conversion.QUOTED_INTEGER, WireType.LONG): "48",
    (Conversion.TEXT_OR_INTEGER_ID, WireType.STRING): 413131,
    (Conversion.TEXT_OR_INTEGER_ID, WireType.LONG): "2208",
    (Conversion.NUMBER_LABEL, WireType.DOUBLE): "1.0",
    (Conversion.CODE_LIST_TEXT, WireType.LONG): "12,37",
    (Conversion.MIDNIGHT_UTC_DATE, WireType.DATE): "2023-03-10T00:00:00Z",
    (Conversion.CHICAGO_MIDNIGHT_DATE, WireType.DATE): 1_753_851_600_000_000_000,
    (Conversion.INTEGER_AS_NUMBER, WireType.LONG): 951.6112,
    (Conversion.DECIMAL_TEXT_NUMBER, WireType.DOUBLE): "7676.5",
    (Conversion.DECIMAL_TEXT_NUMBER, WireType.LONG): "7676.5",
}


@pytest.mark.parametrize(
    ("conversion", "declared"),
    [(c, d) for c in Conversion for d in sorted(c.declarations)],
    ids=str,
)
def test_each_conversion_widens_or_retypes_exactly_as_it_says(
    conversion: Conversion, declared: WireType
) -> None:
    # `widens` is what the converters do, not a table to keep in step: a widening conversion
    # accepts a form the declared type refuses, and any other conversion changes the type of
    # values the declared type accepts. So no conversion is a no-op.
    value = _SAMPLES[conversion, declared]
    convert_field(_with(conversion), value)
    try:
        convert(declared, value)
    except UnconvertibleError:
        assert conversion.widens
    else:
        assert not conversion.widens
        assert conversion.wire_type is not declared


# Messages: every decoder shows these after its own context.


def test_a_plain_field_refusal_names_the_field_type_and_value() -> None:
    field = Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)
    with pytest.raises(UnconvertibleError) as caught:
        convert_field(field, "1.5")
    assert str(caught.value) == "Price (wire key 'p') expects double, got \"1.5\""


def test_a_named_conversion_refusal_names_the_conversion() -> None:
    with pytest.raises(UnconvertibleError) as caught:
        convert_field(_with(Conversion.QUOTED_INTEGER), [4])
    assert str(caught.value) == (
        "X (wire key 'x') expects long (by the conversion quoted-integer), got [4]"
    )


def test_convert_alone_refuses_without_a_message() -> None:
    with pytest.raises(UnconvertibleError) as caught:
        convert(WireType.LONG, "1")
    assert str(caught.value) == ""


@pytest.mark.parametrize(
    ("value", "quoted"),
    [
        (True, "true"),
        (None, "null"),
        ("é", '"é"'),
        ({"a": [1]}, '{"a": [1]}'),
        ("x" * 100, '"' + "x" * 100 + '"'),
        ("x" * 300, '"' + "x" * 199),
    ],
)
def test_an_excerpt_is_the_json_text_cut_to_200_characters(value: object, quoted: str) -> None:
    assert excerpt(value) == quoted


def test_a_row_excerpt_is_the_compact_json_text_cut_to_2000_characters() -> None:
    assert row_excerpt({"ev": "T", "sym": "AAPL", "p": "é"}) == '{"ev":"T","sym":"AAPL","p":"é"}'
    long = row_excerpt({"v": "x" * 3000})
    assert long == '{"v":"' + "x" * 1994


def test_the_missing_key_clause_names_the_column_and_wire_key() -> None:
    field = Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.MS)
    assert missing_key_message(field) == "no SipTimestamp (wire key 't'), which the dataset needs"


# --- column reading -----------------------------------------------------------------------------

# Values of every kind a decoder meets, odd ones included: each column below draws from them.
_POOL: tuple[object, ...] = (
    None,
    0,
    7,
    -3,
    2**53 + 1,  # a double rounds it
    2**63 - 1,
    2**63,  # beyond a long
    -(2**63) - 1,
    10**400,  # beyond a double
    1.5,
    -0.0,
    True,
    False,
    "",
    "AAPL",
    "48",
    "4.8",
    "-7676.25",
    "1e5",
    "9" * 400,  # a decimal beyond a finite double
    "12,37",
    "2025-03-03",
    "2025-02-30",
    "2025-03-03T14:30:00Z",
    "2025-03-03T00:00:00+00:00",
    [],
    [12, 37],
    [12, None],
    [2**63],
    [True],
    ["a", None],
    ["a", 1],
    {"deep": [1, 2]},
)


def _field(wire_type: WireType, conversion: Conversion | None = None) -> Field:
    unit = WireUnit.MS if wire_type is WireType.EPOCH else None
    return Field(wire_key="v", column="V", wire_type=wire_type, unit=unit, conversion=conversion)


_FIELDS = [
    *(_field(wire_type) for wire_type in WireType),
    *(_field(conversion.wire_type, conversion) for conversion in Conversion),
]


def _typed(values: list[object] | None) -> object:
    # Values with their exact types, so `1` and `1.0`, or `True` and `1`, never compare equal.
    if values is None:
        return None
    return [(type(v), [(type(e), e) for e in v] if isinstance(v, list) else v) for v in values]


@pytest.mark.parametrize("field", _FIELDS, ids=lambda f: str(f.conversion or f.wire_type))
def test_a_column_reads_exactly_as_its_values_one_by_one(field: Field) -> None:
    # Every column of 3 values from the pool: when the field's converter takes every value,
    # the column holds exactly what it gives for each; when it refuses one, the column reader
    # refuses the column, so the decoder reads the rows one by one.
    read = column_converter(field)
    one = field_converter(field)
    for values in itertools.product(_POOL, repeat=3):
        try:
            expected: list[object] | None = [None if v is None else one(v) for v in values]
        except UnconvertibleError:
            expected = None
        assert _typed(read(list(values))) == _typed(expected), values


def test_a_column_of_plain_values_is_kept_without_a_copy() -> None:
    values: list[object] = [1.5, None, 2.5]
    price = Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)
    assert column_converter(price)(values) is values
    # An integer becomes a float, as the converter makes it: a new list.
    mixed: list[object] = [1.5, 2]
    assert _typed(column_converter(price)(mixed)) == [(float, 1.5), (float, 2.0)]


def test_a_column_left_to_arrow_keeps_its_numbers_unchecked() -> None:
    # A caller that builds an Arrow array of the field's type gets integers back as sent: Arrow
    # makes floats of a double column's integers, and refuses those out of range itself.
    price = Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)
    mixed: list[object] = [1.5, 2, None]
    assert column_converter(price, arrow_checks_numbers=True)(mixed) is mixed
    size = Field(wire_key="s", column="Size", wire_type=WireType.LONG)
    too_large: list[object] = [1, 2**63]
    assert column_converter(size, arrow_checks_numbers=True)(too_large) is too_large
    assert column_converter(size)(too_large) is None
    codes = Field(wire_key="c", column="Codes", wire_type=WireType.LONG_ARRAY)
    lists: list[object] = [[1, 2**63], None]
    assert column_converter(codes, arrow_checks_numbers=True)(lists) is lists
    assert column_converter(codes)(lists) is None


def test_a_column_of_decimal_texts_reads_in_one_pass() -> None:
    # The futures stream's prices, as Massive sent them on 2026-11-02, with a null between.
    price = _field(WireType.DOUBLE, Conversion.DECIMAL_TEXT_NUMBER)
    read = column_converter(price)
    assert _typed(read(["7676.5", None, "7679"])) == [
        (float, 7676.5),
        (NoneType, None),
        (float, 7679.0),
    ]
    assert _typed(read(["7676.25", "7676.5"])) == [(float, 7676.25), (float, 7676.5)]
    # One text that is no plain decimal, or too large, sends the column to the rows.
    assert read(["7676.5", "1e5"]) is None
    assert read(["7676.5", "9" * 400]) is None
    # Numbers read as a `double` column reads them, kept when already floats.
    floats: list[object] = [7676.5, None]
    assert read(floats) is floats
    assert _typed(read([7679, 1.5])) == [(float, 7679.0), (float, 1.5)]
    # Text and numbers in one column are read value by value, to the same result.
    assert _typed(read(["7676.5", 7679])) == [(float, 7676.5), (float, 7679.0)]


def test_a_decimal_text_column_left_to_arrow_keeps_its_numbers() -> None:
    price = _field(WireType.DOUBLE, Conversion.DECIMAL_TEXT_NUMBER)
    mixed: list[object] = [7679, 1.5, None]
    assert column_converter(price, arrow_checks_numbers=True)(mixed) is mixed
    # Text still becomes floats here: Arrow would not read it as a double.
    assert column_converter(price, arrow_checks_numbers=True)(["7676.5"]) == [7676.5]
