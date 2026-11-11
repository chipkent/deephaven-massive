"""The dataset namespaces of a session, such as `m.stocks`, and their classes for annotations.

Each namespace on a session (`m.stocks`, `m.options`, and the rest) is an instance of a class
from here, named for its path on the session. Import one to annotate your own code, for example
a helper that takes `m.stocks` and reads one symbol's trades::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.namespaces import StocksNamespace


    def trades(stocks: StocksNamespace, symbol: str) -> Table:
        return stocks.trades(symbol).append_only()


    m = dm.MassiveSession()
    apple = trades(m.stocks, "AAPL")

| Class | Reached as |
|---|---|
| `StocksNamespace` | `m.stocks` |
| `StocksFinancialsNamespace` | `m.stocks.financials` |
| `StocksFilingsNamespace` | `m.stocks.filings` |
| `OptionsNamespace` | `m.options` |
| `FuturesNamespace` | `m.futures` |
| `IndicesNamespace` | `m.indices` |
| `ForexNamespace` | `m.forex` |
| `CryptoNamespace` | `m.crypto` |
| `ReferenceNamespace` | `m.reference` |
| `IndicatorsNamespace` | `m.indicators` |
| `EconomyNamespace` | `m.economy` |
| `PartnersNamespace` | `m.partners` |
| `PartnersBenzingaNamespace` | `m.partners.benzinga` |
| `PartnersEtfGlobalNamespace` | `m.partners.etf_global` |
| `PartnersTmxNamespace` | `m.partners.tmx` |
| `PartnersFableNamespace` | `m.partners.fable` |

Users never construct a namespace: each session creates its own. Each class's docstring
describes its datasets or nested namespaces, and `dm.datasets()` lists every dataset with its
query methods. `import deephaven_massive` does not load this package. Importing it needs a
running Deephaven JVM, as the session does.

The classes live in the package's private modules, one per namespace (`_stocks.py`, with
`_stock_financials.py` and `_stock_filings.py` for its nested namespaces, ...,
`_partners.py`), each with its namespace's `DatasetSpec`s (`DATASETS`), which `_session.py`
composes into the process's dataset catalog. `_query_building.py` holds `Namespace`, the
private base class of every namespace class, and the shared dataset factory (`build_query`).
Layer: API; the modules may import every layer below except the ingest code (`_ingest`), which
only `_session.py` imports.
"""

from __future__ import annotations

from deephaven_massive.namespaces._crypto import CryptoNamespace
from deephaven_massive.namespaces._economy import EconomyNamespace
from deephaven_massive.namespaces._forex import ForexNamespace
from deephaven_massive.namespaces._futures import FuturesNamespace
from deephaven_massive.namespaces._indicators import IndicatorsNamespace
from deephaven_massive.namespaces._indices import IndicesNamespace
from deephaven_massive.namespaces._options import OptionsNamespace
from deephaven_massive.namespaces._partners import (
    PartnersBenzingaNamespace,
    PartnersEtfGlobalNamespace,
    PartnersFableNamespace,
    PartnersNamespace,
    PartnersTmxNamespace,
)
from deephaven_massive.namespaces._reference import ReferenceNamespace
from deephaven_massive.namespaces._stock_filings import StocksFilingsNamespace
from deephaven_massive.namespaces._stock_financials import StocksFinancialsNamespace
from deephaven_massive.namespaces._stocks import StocksNamespace

__all__ = [
    "CryptoNamespace",
    "EconomyNamespace",
    "ForexNamespace",
    "FuturesNamespace",
    "IndicatorsNamespace",
    "IndicesNamespace",
    "OptionsNamespace",
    "PartnersBenzingaNamespace",
    "PartnersEtfGlobalNamespace",
    "PartnersFableNamespace",
    "PartnersNamespace",
    "PartnersTmxNamespace",
    "ReferenceNamespace",
    "StocksFilingsNamespace",
    "StocksFinancialsNamespace",
    "StocksNamespace",
]
