"""The request record a verb hands to a transport, and the other plain records of a request.

A dataset method normalizes its arguments once, when the query is created, into a `Request`:
an immutable, hashable record, so equal calls give equal queries and share streams. The verbs
pass it, with the dataset's `DatasetSpec`, to the transports. The module also names the types a
request is built from (`ParamValue`), the identity of a live stream (`StreamKey`), and a table
passed where a dataset takes one value, such as an option chain's underlying (`ValueTable`).

Everything here is a plain value: no I/O, no threads, and no Deephaven call. The namespaces'
shared dataset factory builds requests (`build_query` in `namespaces/_query_building.py`), the
query classes of `deephaven_massive.queries` hand them to the transports, and the transport
interfaces that receive them live in `_tables/transports.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, TypeAlias, cast

if TYPE_CHECKING:
    from datetime import date, datetime
    from enum import StrEnum

    from deephaven.table import Table

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import FuturesVenue, Timespan

__all__ = ["ParamValue", "Request", "StreamKey", "ValueTable"]

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
class ValueTable:
    """A table passed where a dataset takes one value: an underlying, or a futures product code.

    `m.options.chain(underlying=t)` and every futures `product_code=t` take a Deephaven table
    instead of one value. Each of its values is read as if it had been passed alone, one
    request series per value (`Request.for_value`); a refreshing table is followed, so a value
    added to it is read and a value deleted from it leaves the result. Built by the dataset
    method (`value_table` in `namespaces/_query_building.py`); immutable. Like `SymbolTable`,
    it compares and hashes by the identity of the table's Python wrapper, which it holds
    strongly.

    Attributes:
        argument: The dataset method's argument, `"underlying"` or `"product_code"`, for
            messages.
        parameter: The wire parameter each value is sent as, such as `"underlyingAsset"` (the
            chain's path segment), `"underlying_ticker"`, or `"product_code"`.
        column: The table's column that holds the values: `"Symbol"` for underlyings,
            `"ProductCode"` for product codes, as `m.futures.products()` rows carry them.
        table: The user's table.
        pairs_refused: True for the options contracts list, which answers a currency or crypto
            pair with no error and no rows: a pair row is refused as the pair passed alone is.
        table_id: `id(table)`, the identity for equality and hashing.
    """

    argument: str
    parameter: str
    column: str
    table: Table = field(compare=False)
    pairs_refused: bool = False
    table_id: int = field(init=False)

    def __post_init__(self) -> None:
        """Record the wrapper's identity."""
        object.__setattr__(self, "table_id", id(self.table))


@dataclass(frozen=True, slots=True)
class Request:
    """The normalized arguments of a dataset call, shared by every verb of its query.

    Built by `namespaces/_query_building.py` when the query is created; immutable and hashable, so
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
        value_table: The table passed where the dataset takes one value (`ValueTable`), or
            None. Its parameter is then absent from `params`: each verb reads the table's
            values and sends each one through `for_value`.
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
    value_table: ValueTable | None = None

    def for_value(self, value: str) -> Request:
        """Return the request that reads one value of the value table, as if passed alone.

        Called only on a request with a value table.

        Args:
            value: One value of `value_table`, such as `"AAPL"` or `"ES"`, already checked.

        Returns:
            The request with no value table and `value` sent as its parameter, exactly the
            request the dataset method builds for that one value.
        """
        parameter = cast("ValueTable", self.value_table).parameter
        params = {**dict(self.params), parameter: value}
        return replace(self, value_table=None, params=tuple(sorted(params.items())))

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
