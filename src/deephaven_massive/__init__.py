"""Massive.com real-time, delayed, historical, fundamental, and flat-file feeds in Deephaven tables.

`deephaven-massive` bridges Massive.com's entire market data platform—WebSockets, REST
endpoints, and S3-compatible `.csv.gz` Flat Files—directly into live and static
Deephaven tables via a zero-dataclass PyArrow RecordBatch pipeline.

Every domain endpoint returns a fluent `FeedBuilder` that lets you choose how the
data should be materialized in Deephaven:

- `.latest()`: Live updating current-state table (`last_by(key_cols)`), seeded via REST before streaming.
- `.history()`: Live updating append-only history table (`blink_to_append_only`).
- `.ring(capacity)`: Bounded live history table retaining the most recent rows (`ring_table`).
- `.blink()`: Raw stateless single-cycle Deephaven blink table.
- `.snapshot()`: One-shot static point-in-time Deephaven table fetched via REST.
- `.poll(interval_s, mode)`: Periodic background REST poller (`"replace"`, `"upsert"`, or `"append"`).
- `.historical(start, end, source)`: Static historical Deephaven table via REST pagination or S3 Flat Files.

Example:
    ```python
    from deephaven_massive import MassiveSession

    m = MassiveSession()  # Reads MASSIVE_API_KEY / POLYGON_API_KEY from environment
    quotes = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).latest()
    trades = m.stocks.trades(["AAPL", "MSFT", "NVDA"]).ring(100_000)
    ```
"""

from __future__ import annotations

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.session import MassiveSession

__version__ = "0.1.0"

__all__ = [
    "FeedBuilder",
    "MassiveConfig",
    "MassiveEntitlementError",
    "MassiveSession",
    "__version__",
]
