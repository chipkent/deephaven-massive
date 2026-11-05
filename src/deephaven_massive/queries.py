"""The query classes and argument types, public for type annotations.

A dataset method such as `m.stocks.trades("AAPL")` returns a query, and the query's class says
which methods it has. Import the classes from here to annotate code of your own, such as a helper
that takes any dataset with a live stream and keeps each symbol's latest row::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.queries import StreamQuery


    def latest(query: StreamQuery) -> Table:
        return query.last_by()


    m = dm.MassiveSession()
    quotes = latest(m.stocks.quotes(["AAPL", "MSFT"]))

| Name | What it is |
|---|---|
| `Query` | A REST dataset: `fetch()` and `poll(interval)`. |
| `StreamQuery` | A WebSocket dataset: `blink()`, `append_only()`, `ring(capacity)`, `last_by()`. |
| `HistoryQuery` | A dataset with history only: `history(start, end=None)`. |
| `StreamHistoryQuery` | The stream methods and `history()`; a subclass of both. |
| `Symbols` | What a market-data dataset takes as its instruments. |
| `DateLike` | What a time bound (`start`, `end`, `as_of`, a day) takes. |
| `FilterValue` | What a value of a `filters` mapping takes. |

Every query also has `columns()`, and each class's docstring describes its methods. Users never
construct a query: the dataset methods do, after checking their arguments. `dm.datasets()`
lists the methods of every dataset.

`import deephaven_massive` does not import this module, so annotating costs nothing until it
is used. Like the session, it needs a running Deephaven JVM.
"""

from __future__ import annotations

from deephaven_massive._query import HistoryQuery, Query, StreamHistoryQuery, StreamQuery
from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = [
    "DateLike",
    "FilterValue",
    "HistoryQuery",
    "Query",
    "StreamHistoryQuery",
    "StreamQuery",
    "Symbols",
]
