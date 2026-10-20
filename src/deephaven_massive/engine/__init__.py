"""Core streaming, polling, symbol normalization, and Arrow bridge engine."""

from __future__ import annotations

from deephaven_massive.engine.arrow_bridge import (
    DATE32_TYPE,
    INT32_LIST_TYPE,
    STRING_LIST_TYPE,
    UTC_NS_TYPE,
    SchemaSpec,
    arrow_to_deephaven_table,
    flatten_dict,
    parse_int32_list,
    parse_str_list,
    record_batches_to_arrow_table,
    rows_to_record_batch,
    to_epoch_days,
    to_pascal_case,
    to_utc_ns,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.engine.poller import SnapshotPoller
from deephaven_massive.engine.publisher import MicroBatchPublisher
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.engine.watchlist import ReactiveWatchlistController
from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer

__all__ = [
    "DATE32_TYPE",
    "INT32_LIST_TYPE",
    "STRING_LIST_TYPE",
    "UTC_NS_TYPE",
    "FeedBuilder",
    "MicroBatchPublisher",
    "ReactiveWatchlistController",
    "SchemaSpec",
    "SnapshotPoller",
    "SymbolNormalizer",
    "WebSocketMultiplexer",
    "arrow_to_deephaven_table",
    "flatten_dict",
    "parse_int32_list",
    "parse_str_list",
    "record_batches_to_arrow_table",
    "rows_to_record_batch",
    "to_epoch_days",
    "to_pascal_case",
    "to_utc_ns",
]
