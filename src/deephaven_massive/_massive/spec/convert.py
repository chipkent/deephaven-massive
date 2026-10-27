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

**Declared conversions.** Where the vendor's specification gives a field the wrong type, the
refresh records the declared type on the field (`Field.declared_type`) and the real one as its
`wire_type`. `convert_field` then converts deliberately from the one to the other, and only for
the pairs in `DECLARED_CONVERSIONS`:

| Declared | Wire type | Accepts |
|---|---|---|
| `string` | `iso-instant` | untyped text that is an ISO instant; returns `int` ns |
| `iso-instant` | `date` | text declared a date-time that is a date; returns `datetime.date` |
| `string` | `long` | a quoted decimal integer such as `"48"` (digits only); returns `int` |
| `long` | `string` | an integer sent where text is meant (an identifier); returns `str` |
| `long` | `int-array` | one integer where a list is meant (a condition code); returns `list[int]` |

A value the declared conversion cannot read (`"4.8"` for a `long`) raises
`UnconvertibleError` like any other bad value; no other conversion exists.

An `iso-instant` is computed as exact integer nanoseconds since the Unix epoch (UTC) from its
date, time, up to nine fractional digits, and offset, with integer arithmetic only, so no
digit is lost to a float or to `datetime`'s microsecond precision. Text without an offset is
refused, not assumed to be UTC: a field that Massive really sends offset-less would need a
declared conversion in the spec, so the assumption is never silent.

The function is pure and safe from any thread. This module imports only the standard library
and `spec.types`.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.spec.types import WireType

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._massive.spec.types import Field

__all__ = ["DECLARED_CONVERSIONS", "UnconvertibleError", "convert", "convert_field"]

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


def _integer_text(value: object) -> str:
    """Return an integer identifier as text.

    Args:
        value: A decoded JSON value.

    Returns:
        The integer's decimal text.
    """
    return str(_long(value))


def _one_code(value: object) -> list[int]:
    """Return a single integer as a one-element `int-array`.

    Args:
        value: A decoded JSON value.

    Returns:
        A list holding the integer, which must be in the signed 32-bit range.
    """
    return [_integer(value, _INT32)]


# Each declared conversion: (declared type, wire type) to its converter.
_DECLARED: dict[tuple[WireType, WireType], Callable[[object], object]] = {
    (WireType.STRING, WireType.ISO_INSTANT): _instant,
    (WireType.ISO_INSTANT, WireType.DATE): _date,
    (WireType.STRING, WireType.LONG): _quoted_integer,
    (WireType.LONG, WireType.STRING): _integer_text,
    (WireType.LONG, WireType.INT_ARRAY): _one_code,
}

DECLARED_CONVERSIONS: frozenset[tuple[WireType, WireType]] = frozenset(_DECLARED)
"""The `(declared type, wire type)` pairs a field's declared conversion may name."""


def convert_field(field: Field, value: object) -> object:
    """Check a decoded JSON value against its field and return its converted form.

    For a field the vendor types correctly this is `convert(field.wire_type, value)`. For a
    field with a `declared_type`, the value arrives in the vendor's actual form and is
    converted deliberately to `field.wire_type` by the pair's declared conversion (see the
    module's table). Call it once per non-null value.

    Args:
        field: The field the value belongs to.
        value: The decoded JSON value, not `None`.

    Returns:
        The value in the form `convert` gives for `field.wire_type`.

    Raises:
        ValueError: If the field names a declared conversion that does not exist; the
            refresh never generates one, so this means the generated file was edited.
    """
    if field.declared_type is None:
        return convert(field.wire_type, value)
    converter = _DECLARED.get((field.declared_type, field.wire_type))
    if converter is None:
        raise ValueError(
            f"field {field.wire_key!r}: no declared conversion from {field.declared_type} "
            f"to {field.wire_type}"
        )
    return converter(value)
