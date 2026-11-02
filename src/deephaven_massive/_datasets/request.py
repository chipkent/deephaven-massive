"""The request record a verb hands to a transport, and the other plain records of a request.

A dataset method normalizes its arguments once, when the query is created, into a `Request`:
an immutable, hashable record, so equal calls give equal queries and share streams. The verbs
pass it, with the dataset's `DatasetSpec`, to the transports. The module also names the types a
request is built from (`ParamValue`) and the identity of a live stream (`StreamKey`).

Everything here is a plain value: no I/O, no threads, and no Deephaven call. The namespaces'
shared dataset factory builds requests (`build_query` in `_namespaces/query_building.py`), the
query classes of `_query.py` hand them to the transports, and the transport interfaces that
receive them live in `_tables/transports.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from datetime import date, datetime
    from enum import StrEnum

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import FuturesVenue, Timespan

__all__ = ["ParamValue", "Request", "StreamKey"]

ParamValue: TypeAlias = (
    "str | int | float | bool | date | datetime | StrEnum | tuple[str | int | float | date, ...]"
)
"""A normalized parameter or filter value (sequences become tuples). A sequence may hold dates
for a list of dates (`date.any_of`) and numbers for a list of numbers (`strike_price.any_of`);
the filter check writes each item as its text, so a `Request` stores lists of text."""

StreamKey: TypeAlias = "tuple[str, str, NormalizedSymbols]"
"""The identity of a stream: `(dataset name, channel, symbols)`. The symbols are the sorted
tuple of canonical symbols, `ALL`, or the `SymbolTable` box of a refreshing table, which compares
by the identity of the table's wrapper and holds it strongly while the key exists. A static
table never reaches a key: it is read first, and its symbols become the tuple."""


@dataclass(frozen=True, slots=True)
class Request:
    """The normalized arguments of a dataset call, shared by every verb of its query.

    Built by `_namespaces/query_building.py` when the query is created; immutable and hashable, so
    equal calls give equal queries and share streams.

    Attributes:
        symbols: The normalized symbols, or None for datasets without instruments.
        timespan: The bar timespan (bars and indicators only).
        multiplier: The bar multiplier.
        venues: The futures venues whose flat-file prefixes `history()` reads, in
            `FuturesVenue` order; None for a dataset without venues. They select flat-file
            prefixes only, never a stream.
        adjusted: Whether bars are adjusted for splits, for datasets with an `adjusted`
            parameter; None for datasets without one. Flat files hold only unadjusted bars.
            The REST transport sends it, when not None, as the `adjusted` query parameter;
            `params` never holds `adjusted`.
        start: The lower bound of the dataset's time axis (`…T` datasets), or of `history()`.
        end: The upper bound of the dataset's time axis, or of `history()`.
        params: The dataset's other typed parameters, by wire parameter name, sorted by name.
        filters: The `filters` mapping, by key (a wire parameter name with its dots written as
            underscores), sorted by name. Each value is the one sent, as
            `filters.check_filters` parsed it: ISO-8601 text of a date or timestamp parameter
            is held as a `date` or an aware `datetime`, never as the text.
        limit: The cap on the total number of rows across pages, or None for every row. It is
            not a wire parameter: the page size is internal (`DatasetSpec.page_size`), and
            the REST transport stops paging once `limit` rows have arrived.
    """

    symbols: NormalizedSymbols | None = None
    timespan: Timespan | None = None
    multiplier: int = 1
    venues: tuple[FuturesVenue, ...] | None = None
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
