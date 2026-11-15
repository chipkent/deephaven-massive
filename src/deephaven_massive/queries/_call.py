"""How a query shows the call that built it: `QueryCall`, the query's `repr`.

A user who prints a query, or reads a log line or an `m.streams()` row about one, needs to see
the call they typed, such as `m.stocks.bars(['AAPL', 'MSFT'], dm.Timespan.MINUTE,
multiplier=5)`, not the parts of a request. `QueryCall` keeps that call. `shows_call` wraps
each dataset method so it records the call on the query it returns, and `Query.__repr__` writes
it from the query's normalized request, so two calls that normalize alike print alike. The
functions here write each argument's value the way a user types it.
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Final, ParamSpec, TypeVar

import numpy as np
from deephaven.table import Table

from deephaven_massive._datasets.dates import parse_date_like
from deephaven_massive._datasets.symbols import ALL
from deephaven_massive.queries._query import shown_as

if TYPE_CHECKING:
    from deephaven_massive._datasets.request import Request
    from deephaven_massive.queries._query import Query

__all__ = ["SYMBOLS_SHOWN", "QueryCall", "shows_call"]

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_Q = TypeVar("_Q", bound="Query")

_FROM_REQUEST: Final = frozenset(
    {"symbols", "timespan", "multiplier", "adjusted", "start", "end", "limit"}
)
"""The arguments a query's repr writes as its request holds them, normalized: the symbols sorted
and de-duplicated, the time bounds parsed. They keep these names in every dataset method."""

SYMBOLS_SHOWN: Final = 5
"""The most symbols a query's repr lists; `… (N more)` stands for the rest."""


def _set_place(item: object) -> tuple[int, str]:
    """Return where a set's item goes in a query's repr, so a set prints the same every run.

    Args:
        item: An item of a set argument, such as a `dm.FuturesVenue` member.

    Returns:
        An enum member's position in its enum, then the item's text; any other item sorts by
        its text alone.
    """
    place = list(type(item)).index(item) if isinstance(item, Enum) else 0
    return place, repr(item)


def _shown(value: object) -> str:
    """Write one argument value as a query's repr shows it, the way a user would type it.

    Args:
        value: The argument's normalized value.

    Returns:
        `dm.<Enum>.<MEMBER>` for an enum member; a date or time, Python, Java, or numpy, as
        quoted ISO-8601 text (`_shown_time`); a set as braces, enum members in their enum's
        order and other items sorted by their text; a list or tuple as a list; and anything
        else as its `repr`; a Deephaven table, such as a table of underlyings, as `<table>`.
    """
    if isinstance(value, Enum):
        return f"dm.{type(value).__name__}.{value.name}"
    if isinstance(value, date | datetime):
        return repr(value.isoformat())
    if isinstance(value, np.datetime64) or hasattr(type(value), "jclassname"):
        return _shown_time(value)
    if isinstance(value, AbstractSet):
        items = sorted(value, key=_set_place)
        return "{" + ", ".join(map(_shown, items)) + "}"
    if isinstance(value, list | tuple):
        return "[" + ", ".join(map(_shown, value)) + "]"
    return "<table>" if isinstance(value, Table) else repr(value)


def _shown_time(value: object) -> str:
    """Write a Java object or a `numpy.datetime64` as a query's repr shows it.

    A day or time argument the request does not hold parsed, such as a daily summary's `date`
    or a reference dataset's `as_of`, is kept as the caller passed it; a Java `LocalDate`,
    `Instant`, or `ZonedDateTime`, or a `numpy.datetime64`, would otherwise print as a jpy or
    numpy object reference.

    Args:
        value: A Java object (jpy sets `jclassname` on its type) or a `numpy.datetime64`.

    Returns:
        The value as `parse_date_like` reads it, as quoted ISO-8601 text: `'2025-01-02'` for a
        day, an instant with its offset otherwise; or its `repr` when it is no time
        `parse_date_like` reads (a Java object of another class, a `NaT`).
    """
    try:
        parsed = parse_date_like(value, "value")
    except (TypeError, ValueError):
        return repr(value)
    return repr(parsed.isoformat())


def _shown_symbols(symbols: object) -> str:
    """Write the `symbols` argument as a query's repr shows it, in its positional place.

    Args:
        symbols: The normalized symbols (a sorted tuple, `ALL`, or a symbol table), or, for a
            request that does not store them, the argument as passed (a string or a tuple).

    Returns:
        One symbol as `'AAPL'`; several as a list of their first `SYMBOLS_SHOWN`, followed by
        `… (N more)` when there are more; `ALL` as `dm.ALL`, and a symbol table, static or
        refreshing, as `<symbol table>`.
    """
    if symbols is ALL:
        return "dm.ALL"
    if isinstance(symbols, str):
        return repr(symbols)
    if not isinstance(symbols, tuple):
        return "<symbol table>"
    if len(symbols) == 1:
        return repr(symbols[0])
    listed = [repr(symbol) for symbol in symbols[:SYMBOLS_SHOWN]]
    if len(symbols) > SYMBOLS_SHOWN:
        listed.append(f"… ({len(symbols) - SYMBOLS_SHOWN} more)")
    return "[" + ", ".join(listed) + "]"


@dataclass(frozen=True, slots=True)
class QueryCall:
    """The dataset-method call that built a query, as its repr shows it.

    Created by the dataset method itself, through `shows_call`, when it returns its query; a
    query built any other way carries only its dataset's path. Not part of the query's
    equality: two calls that normalize to one request are one query, and since `text` writes
    the normalized values, they print alike. Immutable and thread-safe.

    Attributes:
        path: The method's path from the session, such as `"m.options.contracts"`.
        arguments: The arguments that differ from their defaults, in signature order, as
            `(public name, value as passed)` pairs; a list is kept as a tuple, a set as a
            `frozenset`, and a mapping as the tuple of its items.
        positional: The parameters the method requires, such as a bar's `timespan`, which the
            repr writes by position, as a user types them, never as `name=value`.
    """

    path: str
    arguments: tuple[tuple[str, object], ...] = ()
    positional: frozenset[str] = frozenset()

    def text(self, request: Request) -> str:
        """Write the call as a user would type it, from its query's normalized request.

        An argument in `_FROM_REQUEST` is written as the request holds it, when the dataset
        stored it there, and `filters` as the request's parsed filters (left out when there are
        none, as an empty mapping selects nothing); every other argument as passed.

        Args:
            request: The query's request.

        Returns:
            For example `m.options.contracts(underlying='AAPL')` or
            `m.stocks.bars(['AAPL', 'MSFT'], dm.Timespan.MINUTE, multiplier=5)`: `symbols`
            comes first and positionally (`_shown_symbols`), a required argument by position,
            `filters` as a dictionary, and every other argument as `name=value` (`_shown`).
        """
        parts = []
        for name, value in self.arguments:
            stored = getattr(request, name) if name in _FROM_REQUEST else None
            if name == "symbols":
                parts.append(_shown_symbols(value if stored is None else stored))
            elif name == "filters" and request.filters:
                pairs = ", ".join(f"{key!r}: {_shown(item)}" for key, item in request.filters)
                parts.append(f"filters={{{pairs}}}")
            elif name in self.positional:
                parts.append(_shown(value if stored is None else stored))
            elif name != "filters":
                parts.append(f"{name}={_shown(value if stored is None else stored)}")
        return f"{self.path}({', '.join(parts)})"


def _is_default(value: object, default: object) -> bool:
    """Tell whether an argument has its parameter's default, so a query's repr leaves it out.

    Args:
        value: The argument as passed.
        default: The parameter's default, or `inspect.Parameter.empty` for none.

    Returns:
        True for the default object itself, or a value equal to a default other than None
        (`{dm.FuturesVenue.CME, ...}` equals the default `frozenset`). None is compared by
        identity, so a symbol table is never compared with `==`. An array-like value, whose
        `==` answers element by element (a numpy array, a pandas series), is never the
        default: only a plain `bool` answer counts, since the truth of an array raises.
    """
    if value is default:
        return True
    if default is None or default is inspect.Parameter.empty:
        return False
    equal: object = value == default  # an array's `==` is an array
    return isinstance(equal, bool) and equal


def _frozen(value: object) -> object:
    """Return an argument as a query's call keeps it, safe from later changes by the caller.

    Args:
        value: The argument as passed.

    Returns:
        A list as a tuple, a set as a `frozenset`, a mapping as the tuple of its items, and
        anything else unchanged.
    """
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, AbstractSet):
        return frozenset(value)
    if isinstance(value, Mapping):
        return tuple(value.items())
    return value


def shows_call(path: str) -> Callable[[Callable[_P, _Q]], Callable[_P, _Q]]:
    """Make a dataset method record its call on the query it returns, as the query's `repr`.

    The wrapper binds the arguments to the method's signature after the method has returned
    (so a refused call raises the method's own error) and stores a `QueryCall` on the query:
    the method's path from the session, and the arguments that differ from their defaults
    (`_is_default`), in signature order (`_frozen`), and the parameters it requires, which the
    repr writes by position; the repr writes each argument from the query's normalized request
    where that holds it (`QueryCall.text`). Equality of queries is
    unchanged: the call is not part of it. `Namespace` applies it to every public method of
    each namespace class, and `MassiveSession` to its own dataset method, `snapshots`. An
    exception the method raises, a refused argument's `TypeError` or `ValueError`, is logged
    at DEBUG with the method's path and re-raised.

    Args:
        path: The method's owner as reached from a session, such as `"m.options"`.

    Returns:
        A decorator; its wrapper has the method's name, docstring, signature, and
        `raises_through` helpers (`functools.wraps`).
    """

    def decorate(method: Callable[_P, _Q]) -> Callable[_P, _Q]:
        signature = inspect.signature(method)
        called = f"{path}.{method.__name__}"
        required = frozenset(
            name
            for name, param in list(signature.parameters.items())[1:]
            if param.default is inspect.Parameter.empty
            and param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        )

        @functools.wraps(method)
        def factory(*args: _P.args, **kwargs: _P.kwargs) -> _Q:
            try:
                query = method(*args, **kwargs)
            except Exception as error:
                # The caller receives the refusal; the log keeps which method refused, and why.
                logger.debug("%s refused: %s: %s", called, type(error).__name__, error)
                raise
            given = signature.bind(*args, **kwargs).arguments
            arguments = tuple(
                (name, _frozen(given[name]))
                for name, param in list(signature.parameters.items())[1:]
                if name in given and not _is_default(given[name], param.default)
            )
            return shown_as(query, QueryCall(called, arguments, required))

        return factory

    return decorate
