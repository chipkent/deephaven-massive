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
`partners`). Each method returns a query object, and a verb on that query picks the table shape
(`blink()`, `append_only()`, `ring(n)`, `last_by()`, `fetch()`, `poll()`, `history()`). The same
code runs on every Massive plan: the library finds the right WebSocket host for your key, and a
dataset your plan lacks fails the table with a message that names the alternative. The library
runs inside an existing Deephaven server; it never starts one.

Status:
    The library is being rebuilt from the specification in `plans/rework/DESIGN.md`, in the
    work packages listed in `plans/rework/PLAN.md`. This release exposes only `__version__`;
    `MassiveSession`, `MassiveConfig`, the enums, and the dataset namespaces arrive in the core
    and transport packages. Until then, read `DESIGN.md` for the API and its semantics.

Example:
    The installed version, read from the package metadata::

        import deephaven_massive

        print(deephaven_massive.__version__)

Attributes:
    __version__: The installed distribution's version string, such as `"0.1.0"`. It comes from
        the package metadata (`importlib.metadata`), so it always matches what pip or uv
        installed.
"""

from importlib.metadata import version

__version__: str = version("deephaven-massive")
