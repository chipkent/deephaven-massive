"""Check one decoded JSON value against its field's `WireType` and convert it, exactly.

Every decoder that reads JSON (the REST client's `to_batch`, the WebSocket client's frame
decoder) converts each field value through the same converter, so one value means the same
thing on every transport. `convert_field` converts one value; a decoder of many values looks up
each field's converter once (`field_converter`) and calls it per value, or reads a whole
column at once (`column_converter`). The result is a plain Python scalar, independent of Arrow;
each decoder builds its own columns from it.

| `WireType` | Accepts | Returns |
|---|---|---|
| `long`, `epoch` | an integer in the signed 64-bit range (never a `bool` or a float) | `int` |
| `double` | any number (never a `bool`) | `float` |
| `bool` | `true` or `false` (never a number) | `bool` |
| `string` | text | `str` |
| `date` | text `YYYY-MM-DD` naming a real date | `datetime.date` |
| `iso-instant` | ISO-8601 date-time text, seconds, offset `Z` or `±HH:MM` | `int` ns, int64 |
| `long-array` | a list of signed 64-bit integers and nulls | `list` of `int` and `None` |
| `string-array` | a list of text values or nulls | `list` of `str` and `None` |
| `json-text` | any JSON value | `str`, compact JSON |

A value that does not fit raises `UnconvertibleError`; nothing is coerced. An epoch stays a raw
count in its field's unit: the library converts it to nanoseconds and checks its bounds.

**Named conversions.** Where the vendor's documented samples show values a field's declared
type does not describe, the refresh records a `Conversion` on the field (`Field.conversion`),
and `convert_field` applies exactly that conversion instead of the plain converter of the
field's `wire_type`. Each conversion has one meaning, whatever field takes it; the plain
converters above are never loosened:

| `Conversion` | Accepts | Returns |
|---|---|---|
| `iso-text-instant` | ISO-8601 instant text the specification leaves untyped | `int` ns |
| `iso-date-text` | `YYYY-MM-DD` text declared a date-time | `datetime.date` |
| `quoted-integer` | an integer, or decimal digits in text such as `"48"` | `int` |
| `text-or-integer-id` | text, or an integer in the signed 64-bit range | `str` |
| `number-label` | text as sent, or a finite number as float text (`1` gives `"1.0"`) | `str` |
| `code-list-text` | an integer, or comma-separated integers in text (`"12,37"`) | `list[int]` |
| `midnight-utc-date` | `YYYY-MM-DD`, or ISO-8601 midnight at offset `Z` or `+00:00` | `date` |
| `chicago-midnight-date` | `YYYY-MM-DD`, or epoch nanoseconds of a midnight in Chicago | `date` |
| `integer-as-number` | any number, as `double` accepts it | `float` |

`text-or-integer-id` serves a field the specification declares either an integer or text
(`Conversion.declarations`). A value the conversion cannot read (`"4.8"` for `quoted-integer`,
`"12;37"` for `code-list-text`, a non-zero clock or another offset for `midnight-utc-date`, an
instant other than a Chicago midnight for `chicago-midnight-date`)
raises `UnconvertibleError` like any other bad value.

An `iso-instant` is computed as exact integer nanoseconds since the Unix epoch (UTC) from its
date, time, up to nine fractional digits, and offset, with integer arithmetic only, so no
digit is lost to a float or to `datetime`'s microsecond precision. Text without an offset is
refused, not assumed to be UTC: a field that Massive really sends offset-less would need a
named conversion, so the assumption is never silent.

**Messages.** The decoders share the text of their two row failures, so a failure reads the
same on every transport and each decoder adds only its own context (the REST path and row, or
the WebSocket channel, symbol, and host):

- `convert_field` raises `UnconvertibleError` whose message names the field, the expected type
  and any named conversion, and quotes the value: `Price (wire key 'p') expects double, got
  "1.5"`. A decoder that called `field_converter`'s function builds the same text with
  `unconvertible_message`.
- `missing_key_message` gives the clause for a row that lacks a key the dataset needs:
  `no SipTimestamp (wire key 't'), which the dataset needs`.
- `excerpt` quotes a value as every message does: its JSON text, cut to 200 characters, and
  `row_excerpt` quotes a whole offending row the same way, cut to 2,000, for a DEBUG line.

**Columns.** `column_converter` reads one field's values from many objects at once, as REST
pages and WebSocket frames are read: a column of plain values that already have the type the
converter would return (an `int` in range for a `long`, a `float` for a `double`) is returned as
it is, its types checked for the whole list in a few calls with no Python call per value, and
any other column goes through the field's converter value by value. It never judges a column it
cannot vouch for: when any value would be refused, it returns `None`, and the decoder reads the
rows one by one with `convert_field`, which judges each value on its own. Either way the values
are the same: a column it returns holds exactly what the converter gives for each value,
unless a caller that builds Arrow arrays leaves the numbers for Arrow to check and convert.

The functions are pure and safe from any thread. This module imports only the standard
library and `spec.types`.
"""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, date, datetime, time, timedelta
from itertools import chain
from types import MappingProxyType, NoneType
from typing import TYPE_CHECKING, Final, TypeVar, cast
from zoneinfo import ZoneInfo

from deephaven_massive._massive.spec.types import Conversion, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive.spec.types import Field

__all__ = [
    "UnconvertibleError",
    "column_converter",
    "convert",
    "convert_field",
    "excerpt",
    "field_converter",
    "missing_key_message",
    "row_excerpt",
    "unconvertible_message",
]

_T = TypeVar("_T")

_INT64 = (-(2**63), 2**63 - 1)
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}", re.ASCII)
_DECIMAL_INTEGER = re.compile(r"\d+", re.ASCII)
# Codes written as decimal integers separated by single commas, such as `12,37`.
_CODE_LIST = re.compile(r"\d+(?:,\d+)*", re.ASCII)
# Hours 00-23, minutes and seconds 00-59 (no leap second), in the time and in the offset.
_INSTANT = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<h>[01]\d|2[0-3]):(?P<m>[0-5]\d):(?P<s>[0-5]\d)"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?:Z|(?P<sign>[+-])(?P<oh>[01]\d|2[0-3]):(?P<om>[0-5]\d))",
    re.ASCII | re.IGNORECASE,
)
_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()
# How many characters of a value's JSON text a message quotes: enough for any value Massive
# sends, so the quote shows the whole problem.
_EXCERPT = 200
# How many characters of a whole row's JSON text a DEBUG line quotes.
_ROW_EXCERPT = 2000
_SECONDS_PER_DAY = 86_400
_NANOS_PER_SECOND = 1_000_000_000


class UnconvertibleError(ValueError):
    """A value does not fit its field's `WireType`, or its field's named conversion.

    `convert_field` raises it with the shared message (the field, the expected type and any
    named conversion, and the value's excerpt); the decoder that called it puts that message
    after its own context, in the log line of a value it reads as null or in the reason of a
    row it drops. `convert` raises it with no message, since it knows no field.
    """


def _long(value: object) -> int:
    """Return a `long` or `epoch` value: an integer in the signed 64-bit range.

    A plain `int`, what a JSON decoder gives for every such value, skips the type checks.

    Args:
        value: A decoded JSON value.

    Returns:
        `value`.

    Raises:
        UnconvertibleError: If `value` is not an `int` (a `bool` is not), or is out of range.
    """
    if type(value) is not int and (isinstance(value, bool) or not isinstance(value, int)):
        raise UnconvertibleError
    if not _INT64[0] <= value <= _INT64[1]:
        raise UnconvertibleError
    return value


def _double(value: object) -> float:
    """Return a `double` value.

    Args:
        value: A decoded JSON value.

    Returns:
        `value` as a `float`.

    Raises:
        UnconvertibleError: If `value` is not a number, is a `bool`, or is an integer too large
            for a double.
    """
    if type(value) is float:  # what a JSON decoder gives for nearly every such value
        return value
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise UnconvertibleError
    try:
        return float(value)
    except OverflowError:
        raise UnconvertibleError from None


def _of_type(kind: type[_T]) -> Callable[[object], _T]:
    """Return a checker that accepts only values of the decoded JSON type `kind`.

    Args:
        kind: `bool` or `str`.

    Returns:
        A function returning its argument, or raising `UnconvertibleError` for any other type.
    """

    def check(value: object) -> _T:
        if not isinstance(value, kind):
            raise UnconvertibleError
        return value

    return check


def _date(value: object) -> date:
    """Return a `date` value.

    Args:
        value: A decoded JSON value.

    Returns:
        The calendar date.

    Raises:
        UnconvertibleError: If `value` is not text of the form `YYYY-MM-DD` naming a real date.
    """
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise UnconvertibleError
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise UnconvertibleError from None


def _instant(value: object) -> int:
    """Return an `iso-instant` value as exact integer nanoseconds since the Unix epoch.

    Args:
        value: A decoded JSON value.

    Returns:
        The instant in UTC, exact to the nanosecond, within the signed 64-bit range (from
        1677-09-21 to 2262-04-11), which an Arrow `timestamp[ns]` can hold.

    Raises:
        UnconvertibleError: If `value` is not ISO-8601 date-time text with seconds and an
            offset (`Z` or `±HH:MM`), or names no real date, time of day, or offset. Raised by
            `_long` for an instant outside the signed 64-bit nanosecond range.
    """
    match = _INSTANT.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise UnconvertibleError
    day = _date(match["date"]).toordinal() - _EPOCH_ORDINAL
    clock = (int(match["h"]) * 60 + int(match["m"])) * 60 + int(match["s"])
    offset = (int(match["oh"] or 0) * 60 + int(match["om"] or 0)) * 60
    if match["sign"] == "-":
        offset = -offset
    total = day * _SECONDS_PER_DAY + clock - offset
    return _long(total * _NANOS_PER_SECOND + int((match["fraction"] or "").ljust(9, "0")))


def _long_array(value: object) -> list[int | None]:
    """Return a `long-array` value.

    A null element is kept: a Deephaven `long[]` holds it as its null value, and nothing is
    coerced to make room for it.

    Args:
        value: A decoded JSON value.

    Returns:
        `value`, a list of integers in the signed 64-bit range and nulls.

    Raises:
        UnconvertibleError: If `value` is not a list, or an element is neither null nor such an
            integer.
    """
    if not isinstance(value, list):
        raise UnconvertibleError
    return [None if element is None else _long(element) for element in value]


def _string_array(value: object) -> list[str | None]:
    """Return a `string-array` value.

    A null element is kept, as a null `String` of the Deephaven `String[]`.

    Args:
        value: A decoded JSON value.

    Returns:
        `value`, a list of text values and nulls.

    Raises:
        UnconvertibleError: If `value` is not a list, or an element is neither null nor text.
    """
    if not isinstance(value, list) or not all(e is None or isinstance(e, str) for e in value):
        raise UnconvertibleError
    return value


def _json_text(value: object) -> str:
    """Return a `json-text` value as compact JSON text.

    Args:
        value: Any decoded JSON value.

    Returns:
        The value encoded with no insignificant whitespace, keys in their received order.
    """
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


# The converter of each wire type.
_CONVERTERS: dict[WireType, Callable[[object], object]] = {
    WireType.LONG: _long,
    WireType.EPOCH: _long,
    WireType.DOUBLE: _double,
    WireType.BOOL: _of_type(bool),
    WireType.STRING: _of_type(str),
    WireType.DATE: _date,
    WireType.ISO_INSTANT: _instant,
    WireType.LONG_ARRAY: _long_array,
    WireType.STRING_ARRAY: _string_array,
    WireType.JSON_TEXT: _json_text,
}


def convert(wire_type: WireType, value: object) -> object:
    """Check a decoded JSON value against a wire type and return its converted form.

    Call it once per non-null value of a type with no field at hand (the refresh checks
    vendor samples this way); a decoder calls `convert_field`. A null is absent, never
    converted. Each type's accepted values and result are in the module's table. A value that
    does not fit raises `UnconvertibleError` with no message.

    Args:
        wire_type: The field's type.
        value: The decoded JSON value, not `None`.

    Returns:
        The value as the module's table gives for `wire_type`.
    """
    return _CONVERTERS[wire_type](value)


def _quoted_integer(value: object) -> int:
    """Return a `long` sent as an integer or as a quoted decimal integer.

    Args:
        value: A decoded JSON value.

    Returns:
        The integer, or the integer the text spells.

    Raises:
        UnconvertibleError: If `value` is neither an integer nor text of decimal digits only,
            or is out of the signed 64-bit range.
    """
    if isinstance(value, str):
        if not _DECIMAL_INTEGER.fullmatch(value):
            raise UnconvertibleError
        value = int(value)
    return _long(value)


def _identifier(value: object) -> str:
    """Return an identifier the vendor sends as text or as an integer, as text.

    Any other value (a `bool`, a float, an integer outside the signed 64-bit range) raises
    `UnconvertibleError`.

    Args:
        value: A decoded JSON value.

    Returns:
        Text unchanged, or an integer's decimal text.
    """
    return value if isinstance(value, str) else str(_long(value))


def _number_label(value: object) -> str:
    """Return a label the specification types as a number (a version such as `"1.0"`) as text.

    Text is kept as sent. A finite number is written as Python writes a float, so `1` and
    `1.0` both give `"1.0"`, the form the vendor's samples use, and `1e20` gives `"1e+20"`.

    Args:
        value: A decoded JSON value.

    Returns:
        The label's text.

    Raises:
        UnconvertibleError: If `value` is neither text nor a finite number that fits a
            double (a `bool` is not a number).
    """
    if isinstance(value, str):
        return value
    number = _double(value)
    if not math.isfinite(number):
        raise UnconvertibleError
    return repr(number)


def _midnight_date(value: object) -> date:
    """Return a date the vendor sends as `YYYY-MM-DD` or as ISO-8601 midnight in UTC.

    Only the written date is used, and only when nothing else is written: the clock must be
    `00:00:00` (any fraction all zeros) and the offset `Z` or `+00:00`. Any other clock or
    offset would carry information a date drops, so it is refused rather than shifted.

    Args:
        value: A decoded JSON value.

    Returns:
        The calendar date as written.

    Raises:
        UnconvertibleError: If `value` is neither form, or names no real date.
    """
    if isinstance(value, str) and _DATE.fullmatch(value):
        return _date(value)
    match = _INSTANT.fullmatch(value) if isinstance(value, str) else None
    if (
        match is None
        or (match["h"], match["m"], match["s"]) != ("00", "00", "00")
        or (match["fraction"] or "0").strip("0")
        or (
            match["sign"] is not None
            and (match["sign"], match["oh"], match["om"]) != ("+", "00", "00")
        )
    ):
        raise UnconvertibleError
    return _date(match["date"])


_CHICAGO = ZoneInfo("America/Chicago")
"""The zone of the CME's calendar, at whose midnight the futures snapshot sends a settlement day."""
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _chicago_midnight_date(value: object) -> date:
    """Return a date sent as `YYYY-MM-DD` or as the epoch nanoseconds of its Chicago midnight.

    The futures snapshot sends a contract's settlement day as an instant, `1753851600000000000`
    (2025-07-30 05:00 UTC, midnight of 2025-07-30 in Chicago). Only an instant at exactly
    midnight in Chicago names a day without loss; any other instant would carry a time a
    date drops, so it is refused rather than shifted.

    Args:
        value: A decoded JSON value.

    Returns:
        The Chicago calendar date of that midnight, or the date as written.

    Raises:
        UnconvertibleError: If `value` is neither form, is not a whole second, or is not
            midnight in Chicago.
    """
    if isinstance(value, str):
        return _date(value)
    seconds, rest = divmod(_long(value), _NANOS_PER_SECOND)
    if rest:
        raise UnconvertibleError
    # Every signed 64-bit count of nanoseconds lies in the years 1677 to 2262, which a
    # `datetime` holds, so the arithmetic cannot overflow.
    local = (_UNIX_EPOCH + timedelta(seconds=seconds)).astimezone(_CHICAGO)
    if local.time() != time(0):
        raise UnconvertibleError
    return local.date()


def _code_list(value: object) -> list[int]:
    """Return codes sent as one integer or as comma-separated integers in text.

    Args:
        value: A decoded JSON value, or a flat file's cell text.

    Returns:
        The codes, each in the signed 64-bit range.

    Raises:
        UnconvertibleError: If `value` is neither an integer nor text of decimal integers
            separated by single commas.
    """
    if isinstance(value, str):
        if not _CODE_LIST.fullmatch(value):
            raise UnconvertibleError
        return [_long(int(code)) for code in value.split(",")]
    return [_long(value)]


# The converter of each named conversion.
_NAMED: Mapping[Conversion, Callable[[object], object]] = MappingProxyType(
    {
        Conversion.ISO_TEXT_INSTANT: _instant,
        Conversion.ISO_DATE_TEXT: _date,
        Conversion.QUOTED_INTEGER: _quoted_integer,
        Conversion.TEXT_OR_INTEGER_ID: _identifier,
        Conversion.NUMBER_LABEL: _number_label,
        Conversion.CODE_LIST_TEXT: _code_list,
        Conversion.MIDNIGHT_UTC_DATE: _midnight_date,
        Conversion.CHICAGO_MIDNIGHT_DATE: _chicago_midnight_date,
        Conversion.INTEGER_AS_NUMBER: _double,
    }
)


def field_converter(field: Field) -> Callable[[object], object]:
    """Return the function that checks and converts one field's values.

    A decoder looks it up once per field and calls it once per non-null value, which saves
    `convert_field`'s lookup of the converter on every value of a large page or frame. It is
    the plain converter of `field.wire_type`, or the field's named conversion; a value that does
    not fit raises `UnconvertibleError` with no message, and the decoder then builds the shared
    message with `unconvertible_message`.

    Example:
        ::

            from deephaven_massive._massive.spec.convert import field_converter
            from deephaven_massive._massive.spec.types import Field, WireType

            price = Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)
            to_price = field_converter(price)
            assert to_price(1) == 1.0

    Args:
        field: The field.

    Returns:
        A function from a decoded JSON value (not `None`) to the value in the form `convert`
        gives for `field.wire_type`.
    """
    if field.conversion is None:
        return _CONVERTERS[field.wire_type]
    return _NAMED[field.conversion]


_AS_SENT: Final[Mapping[WireType, frozenset[type]]] = MappingProxyType(
    {
        WireType.LONG: frozenset({int, NoneType}),
        WireType.EPOCH: frozenset({int, NoneType}),
        WireType.DOUBLE: frozenset({float, NoneType}),
        WireType.BOOL: frozenset({bool, NoneType}),
        WireType.STRING: frozenset({str, NoneType}),
        WireType.LONG_ARRAY: frozenset({list, NoneType}),
        WireType.STRING_ARRAY: frozenset({list, NoneType}),
    }
)
"""The JSON value types a whole column of each plain wire type may hold (null included) to be
kept as it is: the values its converter returns unchanged. The check is on exact types, so a
`bool` is never an `int` here, and a `double` column holding an integer goes through the
converter, which makes it a float. The integers of a `long` column, and of a `long-array`
column's lists, must also lie in the signed 64-bit range."""

_ITEMS_AS_SENT: Final[Mapping[WireType, frozenset[type]]] = MappingProxyType(
    {
        WireType.LONG_ARRAY: frozenset({int, NoneType}),
        WireType.STRING_ARRAY: frozenset({str, NoneType}),
    }
)
"""The JSON value types the items of a list column may hold to be kept as they are."""


def _in_int64(values: list[object], *, has_null: bool) -> bool:
    """Return whether every integer of a list of integers and nulls is in the signed 64-bit range.

    Args:
        values: Integers, and `None`s when `has_null`.
        has_null: Whether `values` holds a `None`, which is then left out.

    Returns:
        True when every integer is in range, or there is none.
    """
    numbers = cast("list[int]", [v for v in values if v is not None] if has_null else values)
    return not numbers or (_INT64[0] <= min(numbers) and max(numbers) <= _INT64[1])


_NUMBERS: Final = frozenset({int, float, NoneType})
"""The JSON value types of a `double` column that `float` converts as its converter does."""


def _as_floats(values: list[object], *, has_null: bool) -> list[object] | None:
    """Return a column of integers, floats, and nulls as floats, as the `double` converter gives.

    A `double` column often mixes integers in (a price of exactly 100 is sent as `100`), so this
    converts the whole list with the builtin `float`, not the converter's Python call per value.
    `float` rounds a large integer exactly as the converter does.

    Args:
        values: Integers and floats, and `None`s when `has_null`.
        has_null: Whether `values` holds a `None`, which then stays `None`.

    Returns:
        A new list of floats (and `None`s); or None when an integer is too large for a double,
        which the converter refuses.
    """
    numbers = cast("list[int | float | None]", values)
    try:
        if has_null:
            return [None if v is None else float(v) for v in numbers]
        return list(map(float, cast("list[int | float]", numbers)))
    except OverflowError:
        return None


def column_converter(
    field: Field, *, arrow_checks_numbers: bool = False
) -> Callable[[list[object]], list[object] | None]:
    """Return the function that checks and converts one field's values from many objects at once.

    A decoder that reads a page or a frame column by column looks it up once per field and
    calls it once per column. A column of a plain wire type (no named conversion) whose values
    the converter would return unchanged (`_AS_SENT`) is returned as it is, at the cost of a
    few calls over the whole list; a `double` column that mixes integers in is converted with
    the builtin `float` in one pass; any other column calls the field's converter
    (`field_converter`) once per value.

    A caller that turns the column straight into an Arrow array of the field's type can leave
    the numbers to Arrow (`arrow_checks_numbers`), which checks them in one native pass: Arrow
    refuses an integer outside the signed 64-bit range in a `long` column, and one a double
    cannot hold exactly in a `double` column, and on that refusal the caller reads the column
    again without `arrow_checks_numbers`.

    Example:
        ::

            from deephaven_massive._massive.spec.convert import column_converter
            from deephaven_massive._massive.spec.types import Field, WireType

            price = Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)
            read_prices = column_converter(price)
            assert read_prices([1.5, None, 2]) == [1.5, None, 2.0]
            assert read_prices([1.5, "2"]) is None  # read the rows one by one instead

    Args:
        field: The field.
        arrow_checks_numbers: Whether the caller builds an Arrow array of the field's type from
            the column at once and reads the column again without it when Arrow refuses it
            (`OverflowError` or `pyarrow.ArrowInvalid`). The integers of a `long`, `epoch`, or
            `long-array` column are then not range-checked here, and a `double` column that
            mixes integers in is kept as it is, for Arrow to make floats of. Default False:
            the column returned holds exactly what the converter gives.

    Returns:
        A function from the field's value in each object (`None` where the object lacks it or
        holds null) to the values in the form `convert_field` gives for each (`None` stays
        `None`): the list it was given, or a new one. It returns `None` when the converter
        refuses some value: the decoder must then read the rows one by one, which judges each
        value on its own.
    """
    convert = field_converter(field)
    as_sent = None if field.conversion is not None else _AS_SENT.get(field.wire_type)
    items_as_sent = _ITEMS_AS_SENT.get(field.wire_type)
    long_values = not arrow_checks_numbers and field.wire_type in {WireType.LONG, WireType.EPOCH}
    long_items = not arrow_checks_numbers and field.wire_type is WireType.LONG_ARRAY
    double_values = field.wire_type is WireType.DOUBLE

    def converted(values: list[object]) -> list[object] | None:
        try:
            return [None if v is None else convert(v) for v in values]
        except UnconvertibleError:
            return None

    if as_sent is None:
        return converted

    def kept(values: list[object]) -> list[object] | None:
        types = set(map(type, values))
        if not types <= as_sent:
            if double_values and types <= _NUMBERS:
                if arrow_checks_numbers:
                    return values
                return _as_floats(values, has_null=NoneType in types)
            return converted(values)
        has_null = NoneType in types
        if items_as_sent is not None:
            lists = cast("list[list[object]]", filter(None, values))
            items = list(chain.from_iterable(lists))
            item_types = set(map(type, items))
            if not item_types <= items_as_sent or (
                long_items and not _in_int64(items, has_null=NoneType in item_types)
            ):
                return None
        elif long_values and not _in_int64(values, has_null=has_null):
            return None
        return values

    return kept


def unconvertible_message(field: Field, value: object) -> str:
    """Return the clause for a value that does not fit its field.

    Every decoder that reads such a value as null, or drops a row for it, puts this clause
    after its own context, so the problem reads the same on every transport.

    Args:
        field: The field.
        value: The decoded JSON value that `field_converter(field)` refused.

    Returns:
        The clause, naming the field, the expected type and any named conversion, and quoting
        the value, such as `Exchange (wire key 'x') expects long (by the conversion
        quoted-integer), got "4.8"`.
    """
    expected = str(field.wire_type)
    if field.conversion is not None:
        expected += f" (by the conversion {field.conversion})"
    return f"{field.column} (wire key {field.wire_key!r}) expects {expected}, got {excerpt(value)}"


def convert_field(field: Field, value: object) -> object:
    """Check a decoded JSON value against its field and return its converted form.

    For a field with no `conversion` this is `convert(field.wire_type, value)`. For a field
    with one, the value arrives in the vendor's documented form and that named conversion
    (see the module's table) turns it into the form `convert` gives for `field.wire_type`.
    Call it once per non-null value; a decoder of many values takes `field_converter` once
    instead.

    Args:
        field: The field the value belongs to.
        value: The decoded JSON value, not `None`.

    Returns:
        The value in the form `convert` gives for `field.wire_type`.

    Raises:
        UnconvertibleError: If the value does not fit, with the clause of
            `unconvertible_message`, which every decoder shows after its own context.
    """
    try:
        return field_converter(field)(value)
    except UnconvertibleError:
        raise UnconvertibleError(unconvertible_message(field, value)) from None


def excerpt(value: object) -> str:
    """Quote a decoded JSON value for a message.

    Args:
        value: Any decoded JSON value.

    Returns:
        The value's JSON text (so `true`, `"1.5"`, `null`), non-ASCII kept, cut to its first
        200 characters.
    """
    return json.dumps(value, ensure_ascii=False)[:_EXCERPT]


def row_excerpt(row: object) -> str:
    """Quote a whole decoded row for a DEBUG line, so the line shows every field it held.

    Args:
        row: A decoded JSON object, as a decoder received it.

    Returns:
        Its compact JSON text, non-ASCII kept, cut to its first 2,000 characters.
    """
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))[:_ROW_EXCERPT]


def missing_key_message(field: Field) -> str:
    """Return the clause for a row that lacks a key the dataset needs, or holds it as null.

    Every decoder that drops a row for it puts this clause after its own context, so the
    reason reads the same on every transport.

    Args:
        field: The field the dataset needs.

    Returns:
        Such as `no SipTimestamp (wire key 't'), which the dataset needs`.
    """
    return f"no {field.column} (wire key {field.wire_key!r}), which the dataset needs"
