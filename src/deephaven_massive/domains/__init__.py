"""Domain sub-clients for MassiveSession."""

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
