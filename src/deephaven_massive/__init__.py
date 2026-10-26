"""Massive.com market data as live and static Deephaven tables.

`deephaven-massive` connects a Deephaven server to Massive.com (formerly Polygon.io). It turns
Massive's three transports into ordinary Deephaven tables:

- **WebSocket streams** (trades, quotes, bars, fair market value, index values, and more)
  become refreshing tables: blink, append-only, ring, or one row per symbol.
- **REST endpoints** (snapshots, reference data, fundamentals, filings, economy, and partner
  datasets) become static tables, or refreshing tables that re-poll on an interval.
- **S3 flat files** (full-day trades, quotes, and bars) become static historical tables.

Every dataset is a method on a namespace grouped by asset class (`stocks`, `options`,
`futures`, `indices`, `forex`, `crypto`) or by topic (`reference`, `indicators`, `economy`,
`partners`). Each method returns an immutable query, and a verb on that query picks the table
shape: `blink()`, `append_only()`, `ring(n)`, `last_by()`, `fetch()`, `poll(interval)`, or
`history(start, end)`. The same code runs on every Massive plan: the library finds the right
WebSocket host for your key, and a dataset your plan lacks fails the table with a message that
names the alternative. The library runs inside an existing Deephaven server (it needs an active
`ExecutionContext`) and never starts one. Importing it requires a running Deephaven JVM: import
it inside a Deephaven worker, or after starting `deephaven_server.Server`.

Public API:
    - `MassiveSession(config=None)`: the entry point; namespaces, `snapshots()`,
      `connections()`, `streams()`, `release(table)`, `close()`, and use as a context
      manager.
    - `MassiveConfig`: the frozen configuration; `MassiveConfig()` reads `MASSIVE_API_KEY`.
    - `MassiveError`, `AuthenticationError`, `EntitlementError`: the exceptions.
    - `ALL`: every symbol on a stream, in a flat file, or in a snapshot.
    - The enums (all `StrEnum`s whose values are Massive's wire strings): `AssetClass`,
      `Timespan`, `PollMode`, `HistorySource`, `FuturesVenue`, `Direction`, `ContractType`,
      `SeriesType`, `Timeframe`, `Order`, `Market`, `Locale`, `ConditionDataType`, `Sip`.
      Parameters take only enum members; convert text with, for example,
      `Timespan("minute")`.

Status:
    The library is being rebuilt from the specification in `plans/rework/DESIGN.md`, in the
    work packages listed in `plans/rework/PLAN.md`. The public surface, configuration,
    session lifecycle, and query classes are in place; the REST, WebSocket, and flat-file
    transports and the datasets of each namespace arrive in later packages. Until a transport
    lands, the verbs that need it raise `NotImplementedError`. Read `DESIGN.md` for the full
    API and its semantics.

Example:
    A session with a pinned stocks host, inside a Deephaven worker::

        import deephaven_massive as dm

        cfg = dm.MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "delayed.massive.com"})
        m = dm.MassiveSession(cfg)
        print(m, dm.__version__)  # <MassiveSession #1 open> 0.1.0
        m.close()

Attributes:
    __version__: The installed distribution's version string, such as `"0.1.0"`. It comes from
        the package metadata (`importlib.metadata`), so it always matches what pip or uv
        installed.
"""

from importlib.metadata import version as _version

from deephaven_massive._config import MassiveConfig
from deephaven_massive._enums import (
    AssetClass,
    ConditionDataType,
    ContractType,
    Direction,
    FuturesVenue,
    HistorySource,
    Locale,
    Market,
    Order,
    PollMode,
    SeriesType,
    Sip,
    Timeframe,
    Timespan,
)
from deephaven_massive._errors import AuthenticationError, EntitlementError, MassiveError
from deephaven_massive._session import MassiveSession
from deephaven_massive._types import ALL

__all__ = [
    "ALL",
    "AssetClass",
    "AuthenticationError",
    "ConditionDataType",
    "ContractType",
    "Direction",
    "EntitlementError",
    "FuturesVenue",
    "HistorySource",
    "Locale",
    "Market",
    "MassiveConfig",
    "MassiveError",
    "MassiveSession",
    "Order",
    "PollMode",
    "SeriesType",
    "Sip",
    "Timeframe",
    "Timespan",
    "__version__",
]

__version__: str = _version("deephaven-massive")
