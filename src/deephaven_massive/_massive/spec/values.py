"""The wire text of a REST parameter value, by the `ParamKind` the spec gives its parameter.

Every value is encoded by its parameter's kind, never by guessing from the Python type, so a
value of the wrong type is refused here instead of reaching Massive as surprising text:

| Kind | Accepts | Sent as |
|---|---|---|
| `STRING` | `str` (an enum member by its value) | the text |
| `INTEGER` | `int` | decimal digits |
| `NUMBER` | `int` or finite `float` | digits; a `float` positionally (`0.0000001`) |
| `BOOLEAN` | `bool` | `true` or `false` |
| `ENUM` | `str` or enum member, one of `Param.values` | the value |
| `DATE` | `date`, or aware `datetime` | `YYYY-MM-DD`, or ISO-8601 date-time in UTC (`Z`) |
| `TIMESTAMP` | `date`, or aware `datetime` | `YYYY-MM-DD`, or an integer in `Param.unit` |
| `LIST` | non-empty sequence of `str` (not a `str` itself) | joined with commas |

A `bool` is never accepted as an `INTEGER` or `NUMBER`, and a naive `datetime` is refused,
since its instant would depend on the machine's time zone. An instant becomes a count of the
parameter's unit (nanoseconds for most, milliseconds for the indicators) by exact integer
arithmetic on the `timedelta` from the epoch, never through a float. An instant that is not a
whole number of that unit (a microsecond instant for a millisecond parameter) is refused rather
than rounded, because rounding a range bound silently moves it. An instant for a `DATE`
parameter is written in UTC with a `Z`, the form of Massive's own examples, whatever zone the
`datetime` carries.

The REST client encodes every request with `encode_value` (through `rest.query`), and the
library's filter check calls the same function when a query is built, so a filter it accepts
always encodes. The module is part of `spec`: it imports only the standard library and
`spec.types`, so importing it loads no client and no network module. The functions are pure
and never modify their arguments, so they are safe from any thread.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from deephaven_massive._massive.spec.types import ParamKind, WireUnit

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._massive.spec.types import Param

__all__ = ["encode_value"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROS_PER_SECOND = 1_000_000
_SECONDS_PER_DAY = 86_400
# How many nanoseconds make one of each unit.
_NANOS_PER = {WireUnit.NS: 1, WireUnit.US: 1000, WireUnit.MS: 1_000_000, WireUnit.S: 1_000_000_000}


def _instant_count(label: str, value: datetime, unit: WireUnit) -> int:
    """Return an aware instant as an exact integer count of `unit` since the Unix epoch.

    Args:
        label: How messages name the value, such as `"parameter 'timestamp.gte'"`.
        value: An aware `datetime`. A subclass that carries nanoseconds below the microsecond
            in a `nanosecond` attribute (`pandas.Timestamp`) keeps them.
        unit: The unit to count in.

    Returns:
        The count, computed in integer nanoseconds from the `timedelta`'s days, seconds, and
        microseconds, plus the value's `nanosecond`, then divided exactly by the unit.

    Raises:
        ValueError: If the instant is not a whole number of `unit`.
    """
    delta = value - _EPOCH
    micros = (delta.days * _SECONDS_PER_DAY + delta.seconds) * _MICROS_PER_SECOND
    nanos = (micros + delta.microseconds) * 1000 + getattr(value, "nanosecond", 0)
    count, rest = divmod(nanos, _NANOS_PER[unit])
    if rest:
        raise ValueError(
            f"{label} takes whole {unit} since the epoch; {value.isoformat()} is not one"
        )
    return count


def _date_or_instant(param: Param, value: object, label: str) -> str:
    """Encode a `DATE` or `TIMESTAMP` value.

    Args:
        param: The parameter.
        value: A `date`, or an aware `datetime`.
        label: How messages name the value.

    Returns:
        The date as `YYYY-MM-DD`; an instant as an integer count of `Param.unit` for
        `TIMESTAMP`, and for `DATE` as ISO-8601 date-time text in UTC ending in `Z`
        (`2025-06-02T04:00:00Z`), with microseconds when it has any.

    Raises:
        TypeError: If `value` is neither a `date` nor a `datetime`.
        ValueError: If `value` is a naive `datetime`, or an instant that is not a whole number
            of a timestamp parameter's unit.
    """
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ValueError(f"{label}: a naive datetime has no instant; give it a time zone")
        if param.unit is not None:
            return str(_instant_count(label, value, param.unit))
        return value.astimezone(UTC).isoformat().removesuffix("+00:00") + "Z"
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"{label} takes a date or an aware datetime, not {value!r}")


def _number(param: Param, value: object, label: str) -> str:
    """Encode an `INTEGER` or `NUMBER` value.

    Args:
        param: The parameter.
        value: An `int`, or for `NUMBER` also a finite `float`; never a `bool`.
        label: How messages name the value.

    Returns:
        The number's text: an `int` as its digits, a `float` as its shortest exact decimal
        written out in positional notation, without an exponent or trailing zeros (`1e20` is
        `100000000000000000000`, `1e-7` is `0.0000001`, `2.0` is `2`, `-0.0` is `0`), since a
        query parameter is not documented to take scientific notation. A subclass (an
        `IntEnum` member, numpy's `float64`) is sent by its numeric value, never by its own
        `repr`.

    Raises:
        TypeError: If `value` is not a number the kind accepts.
        ValueError: If `value` is a `float` that is not finite.
    """
    accepted = (int,) if param.kind is ParamKind.INTEGER else (int, float)
    if isinstance(value, bool) or not isinstance(value, accepted):
        raise TypeError(f"{label} takes {param.kind} values, not {value!r}")
    if isinstance(value, int):
        return str(int(value))
    if not math.isfinite(value):
        raise ValueError(f"{label} takes a finite number, not {value!r}")
    text = format(Decimal(repr(float(value))), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _text(param: Param, value: object, label: str) -> str:
    """Encode a `STRING` or `ENUM` value.

    Args:
        param: The parameter.
        value: A `str` or an enum member whose value is a `str`.
        label: How messages name the value.

    Returns:
        The text, or the enum member's value.

    Raises:
        TypeError: If `value` is neither.
        ValueError: If the parameter is an `ENUM` and the text is not one of its values.
    """
    text = value.value if isinstance(value, Enum) else value
    if not isinstance(text, str):
        raise TypeError(f"{label} takes text, not {value!r}")
    if param.kind is ParamKind.ENUM and text not in param.values:
        raise ValueError(f"{label} takes one of {list(param.values)}, not {text!r}")
    return text


def _list(_param: Param, value: object, label: str) -> str:
    """Encode a `LIST` value.

    Args:
        _param: The parameter; every list is encoded alike.
        value: A non-empty sequence of `str`, which is not itself a `str`.
        label: How messages name the value.

    Returns:
        The items joined with commas.

    Raises:
        TypeError: If `value` is not a sequence of `str`.
        ValueError: If `value` is empty, which would ask Massive for no filter at all.
    """
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise TypeError(f"{label} takes a sequence of text, not {value!r}")
    items = list(value)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"{label} takes a sequence of text, not {value!r}")
    if not items:
        raise ValueError(f"{label} takes at least one value")
    return ",".join(items)


def _boolean(_param: Param, value: object, label: str) -> str:
    """Encode a `BOOLEAN` value.

    Args:
        _param: The parameter; every boolean is encoded alike.
        value: A `bool`.
        label: How messages name the value.

    Returns:
        `"true"` or `"false"`.

    Raises:
        TypeError: If `value` is not a `bool`.
    """
    if not isinstance(value, bool):
        raise TypeError(f"{label} takes a bool, not {value!r}")
    return "true" if value else "false"


# The encoder of each kind's values.
_ENCODERS: dict[ParamKind, Callable[[Param, object, str], str]] = {
    ParamKind.STRING: _text,
    ParamKind.ENUM: _text,
    ParamKind.INTEGER: _number,
    ParamKind.NUMBER: _number,
    ParamKind.BOOLEAN: _boolean,
    ParamKind.DATE: _date_or_instant,
    ParamKind.TIMESTAMP: _date_or_instant,
    ParamKind.LIST: _list,
}


def encode_value(param: Param, value: object, *, label: str | None = None) -> str:
    """Return the wire text of one parameter value, by the parameter's kind.

    A value of a type the kind does not accept raises `TypeError`. A value of the right type
    that cannot be sent raises `ValueError`: a naive `datetime`, an instant finer than the
    parameter's unit, an enum value the parameter does not list, a non-finite number, or an
    empty list.

    Args:
        param: The parameter, from the path's `RestPath.params`.
        value: The value; see the module's table for what each kind accepts.
        label: How an error message names the value, such as `"stocks.dividends filter
            'frequency'"`, so a caller that knows the value by another name reports it once;
            `"parameter '<wire name>'"` by default.

    Returns:
        The text to send, not yet percent-encoded.

    Example:
        ::

            from datetime import date

            from deephaven_massive._massive import Transport, encode_value, source

            news = source(Transport.REST, "/v2/reference/news")
            (published,) = [p for p in news.params if p.name == "published_utc.gte"]
            encode_value(published, date(2025, 6, 2))  # "2025-06-02"
    """
    return _ENCODERS[param.kind](param, value, label or f"parameter {param.name!r}")
