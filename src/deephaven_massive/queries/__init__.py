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
- `HistoryQuery`: any query with `history()`, and the query of bars Massive doesn't stream.
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

A `bars()` call returns a `StreamHistoryQuery` for the bars Massive streams, 1-second and
1-minute bars at `multiplier=1`, and a `HistoryQuery` for any other bars, and declares both.
A type checker allows a stream method on its result only after you check the class::

    from deephaven_massive.queries import StreamHistoryQuery

    minute = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
    assert isinstance(minute, StreamHistoryQuery)  # 1-minute bars stream
    live = minute.last_by()

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
