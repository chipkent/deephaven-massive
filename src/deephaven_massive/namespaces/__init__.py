"""The namespace classes: the objects that hold a session's datasets, such as `m.stocks`.

A session groups its datasets by asset class or topic, and each group is a namespace: `m.stocks`
holds the stock datasets, `m.options` the options datasets, and so on. Each method of a
namespace is one dataset and returns a query of it. You reach a namespace through a session and
never construct one; import its class from here only to annotate your own code, such as a
helper that takes `m.stocks` and reads one symbol's trades::

    from deephaven.table import Table

    import deephaven_massive as dm
    from deephaven_massive.namespaces import StocksNamespace


    def trades(stocks: StocksNamespace, symbol: str) -> Table:
        return stocks.trades(symbol).append_only()


    m = dm.MassiveSession()
    apple = trades(m.stocks, "AAPL")

Each class is named for its path on the session:

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

`help()` on a class describes its datasets, and `dm.datasets()` lists every dataset with its
query methods. The classes of the queries are in `deephaven_massive.queries`.
"""

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
