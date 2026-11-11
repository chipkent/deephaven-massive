"""The classes of a session's namespaces, such as `m.stocks`, for type annotations.

Each namespace on a session (`m.stocks`, `m.options`, and the rest) is an instance of a class
from here. Import one to annotate your own code, for example a helper that takes `m.stocks` and
reads one symbol's trades::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.namespaces import Stocks


    def trades(stocks: Stocks, symbol: str) -> Table:
        return stocks.trades(symbol).append_only()


    m = dm.MassiveSession()
    apple = trades(m.stocks, "AAPL")

| Class | Reached as |
|---|---|
| `Stocks` | `m.stocks` |
| `StockFinancials` | `m.stocks.financials` |
| `StockFilings` | `m.stocks.filings` |
| `Options` | `m.options` |
| `Futures` | `m.futures` |
| `Indices` | `m.indices` |
| `Forex` | `m.forex` |
| `Crypto` | `m.crypto` |
| `Reference` | `m.reference` |
| `Indicators` | `m.indicators` |
| `Economy` | `m.economy` |
| `Partners` | `m.partners` |
| `Benzinga` | `m.partners.benzinga` |
| `EtfGlobal` | `m.partners.etf_global` |
| `Tmx` | `m.partners.tmx` |
| `Fable` | `m.partners.fable` |

Users never construct a namespace: each session creates its own. Each class's docstring
describes its datasets or nested namespaces, and `dm.datasets()` lists every dataset with its
query methods. The classes of the queries these methods return are in
`deephaven_massive.queries`.

`import deephaven_massive` does not load this module. Importing it needs a running Deephaven
JVM, as the session does.
"""

from __future__ import annotations

from deephaven_massive._namespaces.crypto import Crypto
from deephaven_massive._namespaces.economy import Economy
from deephaven_massive._namespaces.forex import Forex
from deephaven_massive._namespaces.futures import Futures
from deephaven_massive._namespaces.indicators import Indicators
from deephaven_massive._namespaces.indices import Indices
from deephaven_massive._namespaces.options import Options
from deephaven_massive._namespaces.partners import Benzinga, EtfGlobal, Fable, Partners, Tmx
from deephaven_massive._namespaces.reference import Reference
from deephaven_massive._namespaces.stock_filings import StockFilings
from deephaven_massive._namespaces.stock_financials import StockFinancials
from deephaven_massive._namespaces.stocks import Stocks

__all__ = [
    "Benzinga",
    "Crypto",
    "Economy",
    "EtfGlobal",
    "Fable",
    "Forex",
    "Futures",
    "Indicators",
    "Indices",
    "Options",
    "Partners",
    "Reference",
    "StockFilings",
    "StockFinancials",
    "Stocks",
    "Tmx",
]
