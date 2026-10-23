"""Type aliases of the public signatures and the `ALL` sentinel.

- `Symbols` is what every market-data dataset accepts as its instruments: one symbol, a
  sequence of symbols, a Deephaven `Table` with a `Symbol` column (a reactive watchlist), or
  `dm.ALL`.
- `ALL` (`dm.ALL`) means every symbol on the stream (the `<channel>.*` topic), in the flat file,
  or in the snapshot. It is the only instance of `_All`.
- `DateLike` and `FilterValue` type the time-axis bounds and the `filters` mapping.
- `StreamSpan`, `DaySpan`, and `FuturesSpan` are the `Literal` timespan sets the typed `bars()`
  overloads use to pick a query class statically (DESIGN § 7.1). They are `Literal`s of enum
  members, never of strings.

The aliases are importable from this private module for annotations; users never need to
construct anything here except through `dm.ALL`.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime
from enum import Enum
from typing import Final, Literal, TypeAlias

from deephaven.table import Table

from deephaven_massive._enums import Timespan

__all__ = [
    "ALL",
    "DateLike",
    "DaySpan",
    "FilterValue",
    "FuturesSpan",
    "StreamSpan",
    "Symbols",
]

logger = logging.getLogger(__name__)


class _All(Enum):
    """The type of `dm.ALL`, the "every symbol" sentinel.

    A one-member enum, so exactly one instance exists: copying or unpickling `dm.ALL` returns
    the same object, `symbols is dm.ALL` is the reliable test, and the value is immutable and
    hashable. It is deliberately not a `str`, so it can never be mistaken for a symbol.
    """

    ALL = "ALL"


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
`ALL` means every symbol. Input forms are normalized to Massive's canonical form, so
`"EUR/USD"` and `"C:EURUSD"` name the same pair.
"""

DateLike: TypeAlias = str | date | datetime
"""A time-axis bound: an ISO-8601 string, a `date`, or a timezone-aware `datetime`.

A naive `datetime`, or an ISO string with a time but no UTC offset, raises
`ValueError("timezone required")`. A date-only value is a whole day in the asset class's day
zone (DESIGN § 12).
"""

FilterValue: TypeAlias = str | int | float | bool | date | datetime | Sequence[str]
"""A value in the `filters` mapping, keyed by the SDK's parameter names such as
`strike_price_gte` or `ticker_any_of`."""

StreamSpan: TypeAlias = Literal[Timespan.SECOND, Timespan.MINUTE]
"""Bar timespans that have a WebSocket channel (with `multiplier=1`)."""

DaySpan: TypeAlias = Literal[Timespan.DAY]
"""The bar timespan that has the previous-session `fetch()` endpoint."""

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
