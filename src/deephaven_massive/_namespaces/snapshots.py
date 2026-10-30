"""`m.snapshots()`: the dataset of Massive's unified snapshot, which the session serves itself.

The unified snapshot (`/v3/snapshot`) takes tickers of any asset classes in one call, so it
hangs off the session (`MassiveSession.snapshots()`) rather than off a namespace, and its
dataset is named after the method, `snapshots`, with no namespace: the name `m.streams()`,
messages, and the column reference (`docs/datasets/snapshots.md`) show. Each ticker's prefix
selects its class (`O:` options, `I:` indices, `C:` forex, `X:` crypto, none for stocks), so the
spec declares no asset class (`ClassFromPrefix.ANY`). Its rows are the union of what every class
sends; an options row also has `Underlying` and `OptionRoot`, as `m.options.snapshots` has.

This module owns only the `DatasetSpec`, exported as `DATASETS` and composed into the dataset
catalog (`CATALOG` in `_session.py`); the factory method is `MassiveSession.snapshots`.
"""

from __future__ import annotations

from typing import Final

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import SNAPSHOT_SYMBOLS_PER_REQUEST

__all__ = ["DATASETS", "SNAPSHOTS"]

SNAPSHOTS: Final = DatasetSpec(
    name="snapshots",
    summary="The current state of tickers of any asset classes, from Massive's unified snapshot.",
    fetch_path="/v3/snapshot",
    keys={
        (Transport.REST, "/v3/snapshot"): SourceKeys(
            symbol=KeySpec("Ticker"),
            # The underlying of an options row, `Underlying` as on `m.options.snapshots`; the
            # rows of other classes have none.
            fanout=KeySpec("UnderlyingAssetTicker", required=False),
        )
    },
    option_rows=True,
    symbol_param="ticker.any_of",
    page_size=250,
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    # The update times that cover every asset class: an index's own, and a ticker's last quote
    # and trade. An explicit APPEND poll appends each new state of a ticker.
    revision_keys=("LastUpdated", "LastQuoteLastUpdated", "LastTradeLastUpdated"),
)
"""The unified snapshot, served by `MassiveSession.snapshots()`; at most 250 tickers a page."""

DATASETS: Final[tuple[DatasetSpec, ...]] = (SNAPSHOTS,)
"""The one dataset the session serves itself."""
