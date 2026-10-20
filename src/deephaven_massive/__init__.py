"""deephaven-massive: Massive.com real-time, delayed, historical, fundamental, and flat-file feeds in Deephaven tables."""

from __future__ import annotations

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.engine.poller import SnapshotPoller
from deephaven_massive.engine.publisher import MicroBatchPublisher
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.engine.watchlist import ReactiveWatchlistController
from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer
from deephaven_massive.session import MassiveSession

__version__ = "0.1.0"

__all__ = [
    "FeedBuilder",
    "MassiveConfig",
    "MassiveEntitlementError",
    "MassiveSession",
    "MicroBatchPublisher",
    "ReactiveWatchlistController",
    "SnapshotPoller",
    "SymbolNormalizer",
    "WebSocketMultiplexer",
    "__version__",
]
