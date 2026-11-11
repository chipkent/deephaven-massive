"""`SymbolsLike`: every form the `symbols` argument of a dataset method takes.

A dataset method that takes its instruments as `symbols` accepts them in whichever form suits
the caller: one ticker, a list, a Deephaven table with a `Symbol` column, or `dm.ALL`. This
module names that union once, so the dataset methods' signatures, the type checker, and a user
annotating their own code all mean the same thing by it. Users import it as `dm.SymbolsLike`.
It lives in the API layer, apart from the symbol checks in `_datasets/symbols.py`, because it
names Deephaven's `Table`, which the datasets layer imports only for type checking.
"""

from collections.abc import Sequence
from typing import TypeAlias

from deephaven.table import Table

from deephaven_massive._datasets.symbols import AllSymbols

__all__ = ["SymbolsLike"]

SymbolsLike: TypeAlias = str | Sequence[str] | Table | AllSymbols
"""What a market-data dataset method takes as its `symbols`: the instruments it reads.

Annotate your own code with it when you pass symbols through to a dataset method, such as a
helper that takes either a list of tickers or a watchlist table. It is one of:

- a `str`: one symbol, such as `"AAPL"`;
- a sequence of `str`: several symbols, read when the query is built;
- a Deephaven `Table` with a `Symbol` column: a static table is read when a query method runs,
  and a live table works as a watchlist, which the query's methods follow as you add and
  delete rows;
- `dm.ALL`: every symbol, in the datasets that take it.

Each symbol, and each row of a table, is Massive's exact ticker, such as `"C:EURUSD"`, not
`"EUR/USD"`; `dm.canonical_symbol` and `dm.canonicalize_symbols` convert other spellings.

Example:
    ::

        from deephaven.table import Table

        import deephaven_massive as dm


        def latest_quotes(m: dm.MassiveSession, symbols: dm.SymbolsLike) -> Table:
            return m.stocks.quotes(symbols).last_by()
"""
