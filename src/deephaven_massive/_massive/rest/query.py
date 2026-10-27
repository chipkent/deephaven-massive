"""Turn a REST path and its parameter values into the request target the client sends.

The REST client calls `request_target` once per first page; later pages follow the server's
`next_url`, which `next_target` checks and cleans. Every value is encoded by the `ParamKind`
the spec gives its parameter, never by guessing from the Python type, so a value of the wrong
type is refused here instead of reaching Massive as surprising text:

| Kind | Accepts | Sent as |
|---|---|---|
| `STRING` | `str` (an enum member by its value) | the text |
| `INTEGER` | `int` | decimal digits |
| `NUMBER` | `int` or finite `float` | the shortest exact decimal (`repr`) |
| `BOOLEAN` | `bool` | `true` or `false` |
| `ENUM` | `str` or enum member, one of `Param.values` | the value |
| `DATE` | `date`, or aware `datetime` | `YYYY-MM-DD`, or ISO-8601 date-time |
| `TIMESTAMP` | `date`, or aware `datetime` | `YYYY-MM-DD`, or an integer in `Param.unit` |
| `LIST` | non-empty sequence of `str` (not a `str` itself) | joined with commas |

A `bool` is never accepted as an `INTEGER` or `NUMBER`, and a naive `datetime` is refused,
since its instant would depend on the machine's time zone. An instant becomes a count of the
parameter's unit (nanoseconds for most, milliseconds for the indicators) by exact integer
arithmetic on the `timedelta` from the epoch, never through a float. An instant that is not a
whole number of that unit (a microsecond instant for a millisecond parameter) is refused rather
than rounded, because rounding a range bound silently moves it.

Names arrive in wire form (`"timestamp.gte"`); the library's filter spelling is translated
before the client. The functions are pure and never modify their arguments, so they are safe
from any thread. This module imports only the standard library and `spec`.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from enum import Enum
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote, urlencode, urlsplit

from deephaven_massive._massive.spec.types import ParamKind, WireUnit

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive.spec.types import Param, RestPath

__all__ = ["Origin", "encode_value", "next_target", "origin_of", "request_target"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROS_PER_SECOND = 1_000_000
_SECONDS_PER_DAY = 86_400
# A unit at most a microsecond long: how many of it make one microsecond (exact multiply).
_PER_MICRO = {WireUnit.NS: 1000, WireUnit.US: 1}
# A unit longer than a microsecond: how many microseconds make one of it (checked divide).
_MICROS_PER = {WireUnit.MS: 1000, WireUnit.S: _MICROS_PER_SECOND}
_DEFAULT_PORTS = {"http": 80, "https": 443}

Origin = tuple[str, str, int]
"""A URL's scheme, lower-case host name, and port (the scheme's default when not written)."""


def origin_of(url: str) -> Origin:
    """Return the origin of an absolute URL, for comparing where two URLs point.

    Args:
        url: An absolute `http` or `https` URL, such as `"https://api.massive.com"`.

    Returns:
        `(scheme, host, port)`, with the scheme and host lower-cased and the scheme's default
        port filled in.

    Raises:
        ValueError: If `url` is not an absolute `http` or `https` URL with a host, or its port
            is not a number.
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        raise ValueError(f"{url!r} is not an absolute http or https URL")
    return scheme, parts.hostname, parts.port or _DEFAULT_PORTS[scheme]


def _instant_count(param: Param, value: datetime, unit: WireUnit) -> int:
    """Return an aware instant as an exact integer count of `unit` since the Unix epoch.

    Args:
        param: The parameter, for the error message.
        value: An aware `datetime`.
        unit: The unit to count in.

    Returns:
        The count, computed from the `timedelta`'s days, seconds, and microseconds.

    Raises:
        ValueError: If the instant is not a whole number of `unit`.
    """
    delta = value - _EPOCH
    micros = (delta.days * _SECONDS_PER_DAY + delta.seconds) * _MICROS_PER_SECOND
    micros += delta.microseconds
    if unit in _PER_MICRO:
        return micros * _PER_MICRO[unit]
    count, rest = divmod(micros, _MICROS_PER[unit])
    if rest:
        raise ValueError(
            f"parameter {param.name!r} takes whole {unit} since the epoch; "
            f"{value.isoformat()} is not one"
        )
    return count


def _date_or_instant(param: Param, value: object) -> str:
    """Encode a `DATE` or `TIMESTAMP` value.

    Args:
        param: The parameter.
        value: A `date`, or an aware `datetime`.

    Returns:
        The date as `YYYY-MM-DD`; an instant as an integer count of `Param.unit` for
        `TIMESTAMP` and as ISO-8601 date-time text for `DATE`. An instant that is not a whole
        number of the unit raises `ValueError`.

    Raises:
        TypeError: If `value` is neither a `date` nor a `datetime`.
        ValueError: If `value` is a naive `datetime`.
    """
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ValueError(
                f"parameter {param.name!r}: a naive datetime has no instant; give it a time zone"
            )
        if param.unit is not None:
            return str(_instant_count(param, value, param.unit))
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"parameter {param.name!r} takes a date or an aware datetime, not {value!r}")


def _number(param: Param, value: object) -> str:
    """Encode an `INTEGER` or `NUMBER` value.

    Args:
        param: The parameter.
        value: An `int`, or for `NUMBER` also a finite `float`; never a `bool`.

    Returns:
        The number's shortest exact decimal text.

    Raises:
        TypeError: If `value` is not a number the kind accepts.
        ValueError: If `value` is a `float` that is not finite.
    """
    accepted = (int,) if param.kind is ParamKind.INTEGER else (int, float)
    if isinstance(value, bool) or not isinstance(value, accepted):
        raise TypeError(f"parameter {param.name!r} takes {param.kind} values, not {value!r}")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"parameter {param.name!r} takes a finite number, not {value!r}")
    return repr(value)


def _text(param: Param, value: object) -> str:
    """Encode a `STRING` or `ENUM` value.

    Args:
        param: The parameter.
        value: A `str` or an enum member whose value is a `str`.

    Returns:
        The text, or the enum member's value.

    Raises:
        TypeError: If `value` is neither.
        ValueError: If the parameter is an `ENUM` and the text is not one of its values.
    """
    text = value.value if isinstance(value, Enum) else value
    if not isinstance(text, str):
        raise TypeError(f"parameter {param.name!r} takes text, not {value!r}")
    if param.kind is ParamKind.ENUM and text not in param.values:
        raise ValueError(
            f"parameter {param.name!r} takes one of {list(param.values)}, not {text!r}"
        )
    return text


def _list(param: Param, value: object) -> str:
    """Encode a `LIST` value.

    Args:
        param: The parameter.
        value: A non-empty sequence of `str`, which is not itself a `str`.

    Returns:
        The items joined with commas.

    Raises:
        TypeError: If `value` is not a sequence of `str`.
        ValueError: If `value` is empty, which would ask Massive for no filter at all.
    """
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise TypeError(f"parameter {param.name!r} takes a sequence of text, not {value!r}")
    items = list(value)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"parameter {param.name!r} takes a sequence of text, not {value!r}")
    if not items:
        raise ValueError(f"parameter {param.name!r} takes at least one value")
    return ",".join(items)


def _boolean(param: Param, value: object) -> str:
    """Encode a `BOOLEAN` value.

    Args:
        param: The parameter.
        value: A `bool`.

    Returns:
        `"true"` or `"false"`.

    Raises:
        TypeError: If `value` is not a `bool`.
    """
    if not isinstance(value, bool):
        raise TypeError(f"parameter {param.name!r} takes a bool, not {value!r}")
    return "true" if value else "false"


# The encoder of each kind's values.
_ENCODERS: dict[ParamKind, Callable[[Param, object], str]] = {
    ParamKind.STRING: _text,
    ParamKind.ENUM: _text,
    ParamKind.INTEGER: _number,
    ParamKind.NUMBER: _number,
    ParamKind.BOOLEAN: _boolean,
    ParamKind.DATE: _date_or_instant,
    ParamKind.TIMESTAMP: _date_or_instant,
    ParamKind.LIST: _list,
}


def encode_value(param: Param, value: object) -> str:
    """Return the wire text of one parameter value, by the parameter's kind.

    A value of a type the kind does not accept raises `TypeError`. A value of the right type
    that cannot be sent raises `ValueError`: a naive `datetime`, an instant finer than the
    parameter's unit, an enum value the parameter does not list, a non-finite number, or an
    empty list.

    Args:
        param: The parameter, from the path's `RestPath.params`.
        value: The value; see the module's table for what each kind accepts.

    Returns:
        The text to send, not yet percent-encoded.
    """
    return _ENCODERS[param.kind](param, value)


def request_target(
    rest_path: RestPath, path_params: Mapping[str, object], query_params: Mapping[str, object]
) -> str:
    """Return the path and query of the first request for a REST path.

    Args:
        rest_path: The path, whose `params` say each parameter's kind and place.
        path_params: A value for every placeholder of the template, by wire name. Each is
            encoded by its kind and then percent-encoded whole, so a `/` in a value stays
            inside its segment.
        query_params: Query values by wire name, in the order to send them. A `None` value is
            left out.

    Returns:
        The request target, such as `"/v3/trades/AAPL?timestamp.gte=1704067200000000000"`,
        with no `?` when no query value is sent.

    Raises:
        ValueError: If a path parameter is missing, `None`, or unknown, or a query name is not
            a query parameter of the path. Each value is encoded by `encode_value`, which
            raises `TypeError` or `ValueError` for a value its kind cannot send.
    """
    params = {param.name: param for param in rest_path.params}
    path_names = {name for name, param in params.items() if param.in_path}
    given = dict(path_params)
    if set(given) != path_names or any(value is None for value in given.values()):
        raise ValueError(
            f"{rest_path.template} takes path parameters {sorted(path_names)}, "
            f"given {sorted(name for name, value in given.items() if value is not None)}"
        )
    segments = {
        name: quote(encode_value(params[name], value), safe="") for name, value in given.items()
    }
    path = rest_path.template.format_map(segments)
    query = []
    for name, value in dict(query_params).items():
        param = params.get(name)
        if param is None or param.in_path:
            raise ValueError(f"{rest_path.template} has no query parameter {name!r}")
        if value is not None:
            query.append((name, encode_value(param, value)))
    return f"{path}?{urlencode(query, quote_via=quote)}" if query else path


def next_target(next_url: str, origin: Origin) -> str:
    """Return the request target of a `next_url`, after checking where it points.

    Massive's `next_url` is an absolute URL that carries the cursor and the original query.
    The client requests it only on the REST host it was built for: the bearer key must never
    go to another host. Any `apiKey` query parameter is removed, so the key is never put in a
    URL (and so never in a log); the rest of the query is kept byte for byte. The error for a
    refused `next_url` quotes it without `apiKey` and without user information, so no
    credential reaches an exception or a log either.

    Args:
        next_url: The `next_url` of the previous page.
        origin: The origin of the client's base URL, from `origin_of`.

    Returns:
        The target to request on `origin`: the path, and the query without `apiKey`.

    Raises:
        ValueError: If `next_url` is not an absolute `http` or `https` URL, or its scheme,
            host, or port differs from `origin`'s; the message names both, redacted.
    """
    parts = urlsplit(next_url)
    kept = [
        item
        for item in parts.query.split("&")
        if item and unquote(item.partition("=")[0]) != "apiKey"
    ]
    path = parts.path or "/"
    target = f"{path}?{'&'.join(kept)}" if kept else path
    try:
        points_to: Origin | None = origin_of(next_url)
    except ValueError:
        points_to = None
    if points_to != origin:
        shown = f"{parts.scheme}://{parts.netloc.rpartition('@')[2]}{target}"
        raise ValueError(f"next_url {shown!r} leaves {origin[0]}://{origin[1]}:{origin[2]}")
    return target
