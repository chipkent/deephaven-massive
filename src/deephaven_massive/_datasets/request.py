"""The request record a verb hands to a transport, and the other plain records of a request.

A dataset method normalizes its arguments once, when the query is created, into a `Request`:
an immutable, hashable record, so equal calls give equal queries and share streams. The verbs
pass it, with the dataset's `DatasetSpec`, to the transports. The module also names the types a
request is built from (`ParamValue`), the identity of a live stream (`StreamKey`), and the
REST transport's answer to which WebSocket host serves a channel family (`HostDecision`).

Everything here is a plain value: no I/O, no threads, and no Deephaven call. The query classes
that build requests live in `_query.py`; the transport interfaces that receive them live in
`_tables/core.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, TypeAlias

from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._types import ALL

if TYPE_CHECKING:
    from enum import StrEnum

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import FuturesVenue, Recency, Timespan

__all__ = ["HostDecision", "ParamValue", "Request", "StreamKey"]

ParamValue: TypeAlias = (
    "str | int | float | bool | date | datetime | StrEnum | tuple[str | date, ...]"
)
"""A normalized parameter or filter value (sequences become tuples). A sequence may hold dates
for a list of dates (`date.any_of`); the filter check writes them as ISO-8601 text, so a
`Request` stores lists of text."""

StreamKey: TypeAlias = "tuple[str, str, NormalizedSymbols]"
"""The identity of a stream: `(dataset name, channel, symbols)`. The symbols are the sorted
tuple of canonical symbols, `ALL`, or a `SymbolTable` box, which compares by the identity of the
reactive table's wrapper and holds it strongly while the key exists."""


@dataclass(frozen=True, slots=True)
class Request:
    """The normalized arguments of a dataset call, shared by every verb of its query.

    Built by `_namespaces/base.py` when the query is created; immutable and hashable, so equal
    calls give equal queries and share streams.

    Attributes:
        symbols: The normalized symbols, or None for datasets without instruments.
        timespan: The bar timespan (bars and indicators only).
        multiplier: The bar multiplier.
        venue: The futures venue, which selects flat-file prefixes only.
        adjusted: Whether bars are adjusted for splits, for datasets with an `adjusted`
            parameter; None for datasets without one. Flat files hold only unadjusted bars.
            The REST transport sends it, when not None, as the `adjusted` query parameter;
            `params` never holds `adjusted`.
        start: The lower bound of the dataset's time axis (`…T` datasets), or of `history()`.
        end: The upper bound of the dataset's time axis, or of `history()`.
        params: The dataset's other typed parameters, by wire parameter name, sorted by name.
        filters: The `filters` mapping, by key (a wire parameter name with its dots written as
            underscores), sorted by name. Each value is the one sent, as
            `Catalog.check_filters` parsed it: ISO-8601 text of a date or timestamp parameter
            is held as a `date` or an aware `datetime`, never as the text.
        limit: The cap on the total number of rows across pages, or None for every row. It is
            not a wire parameter: the page size is internal (`DatasetSpec.page_size`), and
            the REST transport stops paging once `limit` rows have arrived.
    """

    symbols: NormalizedSymbols | None = None
    timespan: Timespan | None = None
    multiplier: int = 1
    venue: FuturesVenue | None = None
    adjusted: bool | None = None
    start: date | datetime | None = None
    end: date | datetime | None = None
    params: tuple[tuple[str, ParamValue], ...] = ()
    filters: tuple[tuple[str, ParamValue], ...] = ()
    limit: int | None = None

    def symbol_key(self) -> NormalizedSymbols:
        """Return the symbol part of a stream key.

        Returns:
            The sorted symbol tuple, `ALL`, or the reactive table's `SymbolTable` box.

        Raises:
            ValueError: If the request has no symbols (a stream always has some).
        """
        if self.symbols is None:
            raise ValueError("a stream needs symbols")
        return self.symbols

    def describe(self) -> str:
        """Render the fields that differ from their defaults, for a query's `repr`.

        Returns:
            Space-separated `name=value` pairs, such as `symbols=('AAPL', 'MSFT')`; `ALL`
            renders as `ALL`, a reactive table as `<reactive table>`, enums by member name,
            dates in ISO form, and parameters and filters as dictionaries.
        """
        parts = []
        for f in fields(self):
            value = getattr(self, f.name)
            if value != f.default:
                parts.append(f"{f.name}={_render(value)}")
        return " ".join(parts)


def _render(value: object) -> str:
    """Render one request value for a `repr`.

    Args:
        value: A field value of a `Request`.

    Returns:
        Its display form.
    """
    if value is ALL:
        return "ALL"
    if isinstance(value, SymbolTable):
        return "<reactive table>"
    if isinstance(value, Enum):
        return f"{type(value).__name__}.{value.name}"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple) and value and isinstance(value[0], tuple):
        return "{" + ", ".join(f"{k!r}: {_render(v)}" for k, v in value) + "}"
    return repr(value)


@dataclass(frozen=True, slots=True)
class HostDecision:
    """Which WebSocket host serves a (asset class, channel family), and why.

    Returned by `RestTransport.resolve_host`; immutable, so it can be cached and shared between
    threads.

    Attributes:
        host: The hostname, such as `"delayed.massive.com"`.
        recency: Whether the host serves real-time or delayed data, or `UNKNOWN` when the
            evidence did not say (only then may a server verdict re-route the connection).
        evidence: Why: a human-readable account of the decision, shown in `m.connections()`.
    """

    host: str
    recency: Recency
    evidence: str
