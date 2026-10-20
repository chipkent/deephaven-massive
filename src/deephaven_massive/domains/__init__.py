"""Domain sub-clients attached to ``MassiveSession``.

Each domain client in this package exposes a fluent ``FeedBuilder`` catalog for
a specific asset class or data provider:
* ``StocksClient`` (``m.stocks``): U.S. equities market data, snapshots,
  indicators, reference data, fundamentals, short data, and SEC filings.
* ``OptionsClient`` (``m.options``): OPRA options trades, quotes, bars, chain
  snapshots with Greeks/IV, contracts, and indicators.
* ``FuturesClient`` (``m.futures``): CME, CBOT, NYMEX, and COMEX futures trades,
  quotes, bars, snapshots, contracts, products, and schedules.
* ``IndicesClient`` (``m.indices``): Market index values, bars, snapshots, and
  technical indicators.
* ``ForexClient`` (``m.forex``): Global spot FX quotes, bars, snapshots,
  conversions, and indicators.
* ``CryptoClient`` (``m.crypto``): Spot cryptocurrency trades, quotes, bars,
  snapshots, and indicators.
* ``EconomyClient`` (``m.economy``): Federal Reserve and U.S. Treasury
  macroeconomic time series.
* ``PartnersClient`` (``m.partners``): Benzinga, ETF Global, and TMX partner
  datasets.
* ``FlatFilesClient`` (``m.flat_files``): Direct S3 Flat File discovery and
  parallel bulk loading.
"""

from __future__ import annotations

from deephaven_massive.domains.base import BaseDomainClient
from deephaven_massive.domains.crypto import CryptoClient
from deephaven_massive.domains.economy import EconomyClient
from deephaven_massive.domains.flat_files import FlatFilesClient
from deephaven_massive.domains.forex import ForexClient
from deephaven_massive.domains.futures import FuturesClient
from deephaven_massive.domains.indices import IndicesClient
from deephaven_massive.domains.options import OptionsClient
from deephaven_massive.domains.partners import (
    BenzingaClient,
    EtfGlobalClient,
    PartnersClient,
    TmxClient,
)
from deephaven_massive.domains.stocks import StocksClient

__all__ = [
    "BaseDomainClient",
    "BenzingaClient",
    "CryptoClient",
    "EconomyClient",
    "EtfGlobalClient",
    "FlatFilesClient",
    "ForexClient",
    "FuturesClient",
    "IndicesClient",
    "OptionsClient",
    "PartnersClient",
    "StocksClient",
    "TmxClient",
]
