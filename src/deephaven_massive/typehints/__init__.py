"""The types of the query classes and the dataset arguments, for type hints.

A dataset method such as `m.stocks.trades("AAPL")` returns a query, and the query's class says
which methods it has. Import the classes from here to annotate code of your own, such as a helper
that takes any query whose stream has a `Symbol` column and keeps each symbol's latest row::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.typehints import SymbolStreamQuery


    def latest(query: SymbolStreamQuery) -> Table:
        return query.last_by()


    m = dm.MassiveSession()
    quotes = latest(m.stocks.quotes(["AAPL", "MSFT"]))

| Name | What it is |
|---|---|
| `Query` | Any query: the base class of the five below, with `columns()`. |
| `FetchQuery` | A REST dataset: `fetch()` and `poll(interval)`. |
| `StreamQuery` | Any WebSocket dataset: `blink()`, `append_only()`, `ring(capacity)`. |
| `SymbolStreamQuery` | A stream with a `Symbol` column: those methods and `last_by()`. |
| `HistoryQuery` | A dataset with history only: `history(start, end=None)`. |
| `StreamHistoryQuery` | The `SymbolStreamQuery` methods and `history()`; a subclass of each. |
| `Symbols` | What a market-data dataset takes as its instruments. |
| `AllSymbols` | The type of `dm.ALL`, one of the types `Symbols` accepts. |
| `DateLike` | What a time bound (`start`, `end`, `as_of`, a day) takes. |
| `FilterValue` | What a value of a `filters` mapping takes. |

Each class's docstring describes its methods. Users never construct a query: the dataset
methods do, after checking their arguments. `dm.datasets()` lists the methods of every dataset.
The classes of `m.stocks` and the other namespaces are in `deephaven_massive.namespaces`.

`import deephaven_massive` does not load this package. Importing it needs a running Deephaven
JVM, as the session does.

The query classes are implemented in the package's private module `_queries.py`. `Symbols` is
defined here. `AllSymbols`, `DateLike`, and `FilterValue` are defined with the code that reads
them (`_datasets/symbols.py`, `_datasets/dates.py`, `_datasets/filters.py`), below the layer of
this package, since the lower layers use them too; the package exports them, as the package
root exports the enums.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeAlias

from deephaven.table import Table

from deephaven_massive._datasets.dates import DateLike
from deephaven_massive._datasets.filters import FilterValue
from deephaven_massive._datasets.symbols import AllSymbols
from deephaven_massive.typehints._queries import (
    FetchQuery,
    HistoryQuery,
    Query,
    StreamHistoryQuery,
    StreamQuery,
    SymbolStreamQuery,
)

__all__ = [
    "AllSymbols",
    "DateLike",
    "FetchQuery",
    "FilterValue",
    "HistoryQuery",
    "Query",
    "StreamHistoryQuery",
    "StreamQuery",
    "SymbolStreamQuery",
    "Symbols",
]

Symbols: TypeAlias = str | Sequence[str] | Table | AllSymbols
"""The instruments of a market-data dataset.

A `str` is one symbol. A `Sequence[str]` is materialized to a tuple when the query is built (an
empty or exhausted iterable raises `ValueError("no symbols")`). A `Table` must have a `Symbol`
column; streams follow its rows as they are added and removed, and polls re-read it each cycle.
`ALL` means every symbol. Every symbol, and every row of a table, must be Massive's exact REST
ticker (`"C:EURUSD"`, not `"EUR/USD"`); `dm.canonical_symbol` and `dm.canonicalize_symbols`
convert other spellings.
"""
