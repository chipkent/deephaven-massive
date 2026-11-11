"""The query classes: what a dataset method returns, and the methods that turn it into a table.

Every dataset method, such as `m.stocks.trades(["AAPL"])`, returns a query. A query describes a
read and reads nothing until you call one of its methods, and each method returns a Deephaven
table of one shape: `fetch()` reads what Massive has now, `poll(interval)` reads it again on a
timer, the stream methods (`blink()`, `append_only()`, `ring(capacity)`, `last_by()`) follow a
live feed, and `history(start, end)` reads a past range. Which of these a query has depends on
what Massive serves for its dataset, and the query's class says so:

- `FetchQuery`: a REST dataset, with `fetch()` and `poll()`.
- `StreamQuery`: a live stream, with `blink()`, `append_only()`, and `ring()`.
- `SymbolStreamQuery`: a live stream whose rows name their symbol, which adds `last_by()`.
- `HistoryQuery`: bars that don't stream, with `history()` only.
- `StreamHistoryQuery`: a stream that also has history, with all five of those methods.
- `Query`: the base class of all five, with `columns()`, for code that takes any query.

You never construct a query: you get one from a dataset method. Import a class from here to
annotate your own code, such as a helper that keeps each symbol's latest row of any stream::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.queries import SymbolStreamQuery


    def latest(query: SymbolStreamQuery) -> Table:
        return query.last_by()


    m = dm.MassiveSession()
    quotes = latest(m.stocks.quotes(["AAPL", "MSFT"]))

`dm.datasets()` lists every dataset with its methods, and `help()` on a class describes each
method.
"""

from deephaven_massive.queries._fetch import FetchQuery
from deephaven_massive.queries._history import HistoryQuery, StreamHistoryQuery
from deephaven_massive.queries._query import Query
from deephaven_massive.queries._stream import StreamQuery, SymbolStreamQuery

__all__ = [
    "FetchQuery",
    "HistoryQuery",
    "Query",
    "StreamHistoryQuery",
    "StreamQuery",
    "SymbolStreamQuery",
]
