"""The query classes and argument types, public for type annotations.

A dataset method such as `m.stocks.trades("AAPL")` returns a query, and the query's class says
which methods it has. Import the classes from here to annotate code of your own, such as a helper
that takes any query whose stream has a `Symbol` column and keeps each symbol's latest row::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.queries import SymbolStreamQuery


    def latest(query: SymbolStreamQuery) -> Table:
        return query.last_by()


    m = dm.MassiveSession()
    quotes = latest(m.stocks.quotes(["AAPL", "MSFT"]))

| Name | What it is |
|---|---|
| `Query` | A REST dataset: `fetch()` and `poll(interval)`. |
| `StreamQuery` | Any WebSocket dataset: `blink()`, `append_only()`, `ring(capacity)`. |
| `SymbolStreamQuery` | A stream with a `Symbol` column: those methods and `last_by()`. |
| `HistoryQuery` | A dataset with history only: `history(start, end=None)`. |
| `StreamHistoryQuery` | The `SymbolStreamQuery` methods and `history()`; a subclass of each. |
| `Symbols` | What a market-data dataset takes as its instruments. |
| `AllSymbols` | The type of `dm.ALL`, one of the types `Symbols` accepts. |
| `DateLike` | What a time bound (`start`, `end`, `as_of`, a day) takes. |
| `FilterValue` | What a value of a `filters` mapping takes. |
| `StreamBarTimespan` | The `bars()` timespans with a live stream, at `multiplier=1`. |
| `FuturesBarTimespan` | The timespans `m.futures.bars()` takes: all but `DAY`. |

Every query also has `columns()`, and each class's docstring describes its methods. Users never
construct a query: the dataset methods do, after checking their arguments. `dm.datasets()`
lists the methods of every dataset. The classes of `m.stocks` and the other namespaces are in
`deephaven_massive.namespaces`.

`import deephaven_massive` does not import this module, so annotating costs nothing until it
is used. Like the session, it needs a running Deephaven JVM.
"""

from __future__ import annotations

from deephaven_massive._query import (
    HistoryQuery,
    Query,
    StreamHistoryQuery,
    StreamQuery,
    SymbolStreamQuery,
)
from deephaven_massive._types import (
    AllSymbols,
    DateLike,
    FilterValue,
    FuturesBarTimespan,
    StreamBarTimespan,
    Symbols,
)

__all__ = [
    "AllSymbols",
    "DateLike",
    "FilterValue",
    "FuturesBarTimespan",
    "HistoryQuery",
    "Query",
    "StreamBarTimespan",
    "StreamHistoryQuery",
    "StreamQuery",
    "SymbolStreamQuery",
    "Symbols",
]
