"""Type aliases of the public signatures and the `ALL` sentinel.

- `Symbols` is what every market-data dataset accepts as its instruments: one symbol, a
  sequence of symbols, a Deephaven `Table` with a `Symbol` column (a reactive watchlist), or
  `dm.ALL`.
- `ALL` (`dm.ALL`) means every symbol on the stream (the `<channel>.*` topic), in the flat file,
  or in the snapshot. It is the only instance of `_All`.
- `DateLike` and `FilterValue` type the time-axis bounds and the `filters` mapping.
- `override` marks the few dunder methods the library overrides, on every supported Python.
- `StreamSpan` and `FuturesSpan` are the `Literal` timespan sets the typed `bars()`
  overloads use to pick a query class statically. They are `Literal`s of enum members, never of
  strings.

Users import `Symbols`, `DateLike`, and `FilterValue` for annotations from the public module
`deephaven_massive.queries`, and never need to construct anything here except through `dm.ALL`.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Final, Literal, TypeAlias, TypeVar

import numpy as np
from deephaven.table import Table

from deephaven_massive._enums import Timespan

__all__ = [
    "ALL",
    "DateLike",
    "FilterValue",
    "FuturesSpan",
    "StreamSpan",
    "Symbols",
    "override",
]

logger = logging.getLogger(__name__)

_F = TypeVar("_F")

if TYPE_CHECKING:
    from typing_extensions import override
else:

    def override(method: _F) -> _F:
        """Return `method` unchanged: `typing.override` exists only from Python 3.12.

        mypy reads `typing_extensions.override` instead, so overrides stay checked.
        """
        return method


class _All(Enum):
    """The type of `dm.ALL`, the "every symbol" sentinel.

    A one-member enum, so exactly one instance exists: copying or unpickling `dm.ALL` returns
    the same object, `symbols is dm.ALL` is the reliable test, and the value is immutable and
    hashable. It is deliberately not a `str`, so it can never be mistaken for a symbol.
    """

    ALL = "ALL"

    @override
    def __repr__(self) -> str:
        """Show the public spelling, so the console prints `dm.ALL`, not the private class.

        Returns:
            `"dm.ALL"`.
        """
        return "dm.ALL"

    @override
    def __str__(self) -> str:
        """Render as `ALL` in messages and logs, as a query's `repr` does.

        Returns:
            `"ALL"`.
        """
        return "ALL"


ALL: Final = _All.ALL
"""Every symbol: the `<channel>.*` stream topic, a whole flat file, or a full-market snapshot.

`ALL` is accepted by stream verbs, by every per-class `snapshots()`, and by `history()` when a
flat file or a grouped-daily REST path exists. Everywhere else it raises `ValueError` naming the
alternative.
"""

Symbols: TypeAlias = str | Sequence[str] | Table | _All
"""The instruments of a market-data dataset.

A `str` is one symbol. A `Sequence[str]` is materialized to a tuple when the query is built (an
empty or exhausted iterable raises `ValueError("no symbols")`). A `Table` must have a `Symbol`
column; streams follow its rows as they are added and removed, and polls re-read it each cycle.
`ALL` means every symbol. Every symbol, and every row of a table, must be Massive's exact REST
ticker (`"C:EURUSD"`, not `"EUR/USD"`); `dm.canonical_symbol` and `dm.canonicalize_symbols`
convert other spellings.
"""

DateLike: TypeAlias = str | date | datetime | np.datetime64
"""A time bound: `history()`'s `start` and `end`, a list's `start`, `end`, and `as_of`, a day.

One of:

- an ISO-8601 string, `"2025-03-03"` or `"2025-03-03T09:30:00-05:00"`;
- a `date`, or a timezone-aware `datetime`;
- a Java `java.time.LocalDate` (a date), or a Java `Instant` or `ZonedDateTime` (an instant),
  the values Deephaven's own time functions return, such as `deephaven.time.dh_now()` or
  `dh_today()` and the cells of an `Instant` column;
- a `numpy.datetime64` of day precision (a date), such as `numpy.datetime64("2025-03-03")`.

Type checkers see Deephaven's Java values as `Any`, so the alias does not name them. A naive
`datetime`, an ISO string with a time but no UTC offset, or a `numpy.datetime64` finer than a
day raises a `ValueError` that names the parameter and the fix ("needs a timezone: pass an
aware datetime or a date"), since a time without a zone cannot be placed. A date-only value
is a whole day in the asset class's day zone.
"""

FilterValue: TypeAlias = str | int | float | bool | date | datetime | Sequence[str]
"""A value in the `filters` mapping, keyed by a wire parameter name with its dots written as
underscores, such as `strike_price_gte` or `ticker_any_of`."""

StreamSpan: TypeAlias = Literal[Timespan.SECOND, Timespan.MINUTE]
"""Bar timespans that have a WebSocket channel (with `multiplier=1`)."""

FuturesSpan: TypeAlias = Literal[
    Timespan.SECOND,
    Timespan.MINUTE,
    Timespan.HOUR,
    Timespan.SESSION,
    Timespan.WEEK,
    Timespan.MONTH,
    Timespan.QUARTER,
    Timespan.YEAR,
]
"""The bar timespans the futures aggregates endpoint accepts: every unit except `DAY`."""
