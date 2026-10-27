"""Check one decoded JSON value against its field's `WireType` and convert it, exactly.

Every decoder that reads JSON (the REST client's `to_batch`, the WebSocket client's frame
decoder) converts each field value through `convert`, so one value means the same thing on
every transport. The result is a plain Python scalar, independent of Arrow; each decoder
builds its own columns from it.

| `WireType` | Accepts | Returns |
|---|---|---|
| `long`, `epoch` | an integer in the signed 64-bit range (never a `bool` or a float) | `int` |
| `double` | any number (never a `bool`) | `float` |
| `bool` | `true` or `false` (never a number) | `bool` |
| `string` | text | `str` |
| `date` | text `YYYY-MM-DD` naming a real date | `datetime.date` |
| `iso-instant` | ISO-8601 date-time text, seconds, offset `Z` or `±HH:MM` | `int` ns, int64 |
| `int-array` | a list of integers, each in the signed 32-bit range | `list[int]` |
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
| `quoted-integer` | decimal digits in text, such as `"48"` | `int` |
| `integer-id-as-text` | an integer in the signed 64-bit range; text is refused | `str` |
| `text-or-integer-id` | text, or an integer in the signed 64-bit range | `str` |
| `number-label` | text as sent, or a finite number as float text (`1` gives `"1.0"`) | `str` |
| `one-code` | one integer in the signed 32-bit range | `list[int]` |
| `midnight-utc-date` | `YYYY-MM-DD`, or ISO-8601 midnight at offset `Z` or `+00:00` | `date` |
| `date-as-epoch` | an integer in the signed 64-bit range (a count in the field's unit) | `int` |

`text-or-integer-id` serves a field the specification declares either an integer or text
(`Conversion.declarations`). A value the conversion cannot read (`"4.8"` for `quoted-integer`,
`"7"` for `integer-id-as-text`, a non-zero clock or another offset for `midnight-utc-date`)
raises `UnconvertibleError` like any other bad value.

An `iso-instant` is computed as exact integer nanoseconds since the Unix epoch (UTC) from its
date, time, up to nine fractional digits, and offset, with integer arithmetic only, so no
digit is lost to a float or to `datetime`'s microsecond precision. Text without an offset is
refused, not assumed to be UTC: a field that Massive really sends offset-less would need a
named conversion, so the assumption is never silent.

The functions are pure and safe from any thread. This module imports only the standard
library and `spec.types`.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.spec.types import Conversion, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive.spec.types import Field

__all__ = ["UnconvertibleError", "convert", "convert_field"]

_T = TypeVar("_T")

_INT64 = (-(2**63), 2**63 - 1)
_INT32 = (-(2**31), 2**31 - 1)
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}", re.ASCII)
_DECIMAL_INTEGER = re.compile(r"\d+", re.ASCII)
# Hours 00-23, minutes and seconds 00-59 (no leap second), in the time and in the offset.
_INSTANT = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<h>[01]\d|2[0-3]):(?P<m>[0-5]\d):(?P<s>[0-5]\d)"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?:Z|(?P<sign>[+-])(?P<oh>[01]\d|2[0-3]):(?P<om>[0-5]\d))",
    re.ASCII | re.IGNORECASE,
)
_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()
_SECONDS_PER_DAY = 86_400
_NANOS_PER_SECOND = 1_000_000_000


class UnconvertibleError(ValueError):
    """A value does not fit its field's `WireType`.

    Raised by `convert` with no message; the decoder that called it names the source, the
    field, the expected type, and the value in its own error (a `WireError` or a `Reject`).
    """


def _integer(value: object, bounds: tuple[int, int]) -> int:
    """Return `value` as an integer within `bounds`.

    Args:
        value: A decoded JSON value.
        bounds: The inclusive range.

    Returns:
        `value`.

    Raises:
        UnconvertibleError: If `value` is not an `int` (a `bool` is not), or is out of range.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise UnconvertibleError
    if not bounds[0] <= value <= bounds[1]:
        raise UnconvertibleError
    return value


def _long(value: object) -> int:
    """Return a `long` or `epoch` value: an integer in the signed 64-bit range.

    Args:
        value: A decoded JSON value.

    Returns:
        `value`.
    """
    return _integer(value, _INT64)


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


def _int_array(value: object) -> list[int]:
    """Return an `int-array` value.

    Args:
        value: A decoded JSON value.

    Returns:
        `value`, a list of integers in the signed 32-bit range.

    Raises:
        UnconvertibleError: If `value` is not a list, or an element is not such an integer.
    """
    if not isinstance(value, list):
        raise UnconvertibleError
    return [_integer(element, _INT32) for element in value]


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
    WireType.INT_ARRAY: _int_array,
    WireType.JSON_TEXT: _json_text,
}


def convert(wire_type: WireType, value: object) -> object:
    """Check a decoded JSON value against a wire type and return its converted form.

    Call it once per non-null field value; a null is absent, never converted. Each type's
    accepted values and result are in the module's table. A value that does not fit raises
    `UnconvertibleError`, which the caller turns into its own error naming the field.

    Args:
        wire_type: The field's type.
        value: The decoded JSON value, not `None`.

    Returns:
        The value as the module's table gives for `wire_type`.
    """
    return _CONVERTERS[wire_type](value)


def _quoted_integer(value: object) -> int:
    """Return a `long` sent as a quoted decimal integer.

    Args:
        value: A decoded JSON value.

    Returns:
        The integer the text spells.

    Raises:
        UnconvertibleError: If `value` is not text of decimal digits only, or is out of the
            signed 64-bit range.
    """
    if not isinstance(value, str) or not _DECIMAL_INTEGER.fullmatch(value):
        raise UnconvertibleError
    return _long(int(value))


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


def _integer_id(value: object) -> str:
    """Return an integer identifier as text; text is refused.

    Args:
        value: A decoded JSON value.

    Returns:
        The integer's decimal text.
    """
    return str(_long(value))


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


def _one_code(value: object) -> list[int]:
    """Return a single integer as a one-element `int-array`.

    Args:
        value: A decoded JSON value.

    Returns:
        A list holding the integer, which must be in the signed 32-bit range.
    """
    return [_integer(value, _INT32)]


# The converter of each named conversion.
_NAMED: Mapping[Conversion, Callable[[object], object]] = MappingProxyType(
    {
        Conversion.ISO_TEXT_INSTANT: _instant,
        Conversion.ISO_DATE_TEXT: _date,
        Conversion.QUOTED_INTEGER: _quoted_integer,
        Conversion.INTEGER_ID_AS_TEXT: _integer_id,
        Conversion.TEXT_OR_INTEGER_ID: _identifier,
        Conversion.NUMBER_LABEL: _number_label,
        Conversion.ONE_CODE: _one_code,
        Conversion.MIDNIGHT_UTC_DATE: _midnight_date,
        Conversion.DATE_AS_EPOCH: _long,
    }
)


def convert_field(field: Field, value: object) -> object:
    """Check a decoded JSON value against its field and return its converted form.

    For a field with no `conversion` this is `convert(field.wire_type, value)`. For a field
    with one, the value arrives in the vendor's documented form and that named conversion
    (see the module's table) turns it into the form `convert` gives for `field.wire_type`.
    Call it once per non-null value; a value that does not fit raises `UnconvertibleError`.

    Args:
        field: The field the value belongs to.
        value: The decoded JSON value, not `None`.

    Returns:
        The value in the form `convert` gives for `field.wire_type`.
    """
    if field.conversion is None:
        return convert(field.wire_type, value)
    return _NAMED[field.conversion](value)
