"""`MassiveSession`, the user's entry point, and the composition of a session.

`MassiveSession` is what users create: it exposes the dataset namespaces (`m.stocks`, `m.options`,
...), the diagnostics tables, and the lifecycle verbs `release()` and `close()`, and nothing else.
It checks where it is constructed (an execution context whose query scope is the script
session's own, never an update-graph thread), builds the session's `SessionCore`
(`_tables/core.py`), which owns everything stateful and which queries and namespaces share,
builds the session's `SessionClients` (`_ingest/clients.py`), which owns every wire client, and
attaches them to the core once; then it hands the core to one object per namespace.

This module is the composition root, the one place that sees every layer: it is the only API
module that imports the ingest code, and it builds `CATALOG`, the process's dataset catalog,
once, at import, from the `DATASETS` of every namespace module. Every core holds that catalog,
so the dataset layer never has to find the namespace modules itself.

The module also numbers sessions (`MassiveSession #1`, a per-process counter, used in the
`repr`, in error messages, and in the same-key WARNING) and keeps the weak registry of open
sessions that warns when a second open session uses the same API key.
"""

from __future__ import annotations

import itertools
import logging
import threading
import weakref
from typing import TYPE_CHECKING, Final, Self, cast

from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table

from deephaven_massive._config import MassiveConfig
from deephaven_massive._datasets.catalog import Catalog
from deephaven_massive._errors import MassiveError
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._namespaces import (
    crypto,
    economy,
    forex,
    futures,
    indicators,
    indices,
    options,
    partners,
    reference,
    stocks,
)
from deephaven_massive._namespaces.base import build_query, list_params
from deephaven_massive._namespaces.crypto import _Crypto
from deephaven_massive._namespaces.economy import _Economy
from deephaven_massive._namespaces.forex import _Forex
from deephaven_massive._namespaces.futures import _Futures
from deephaven_massive._namespaces.indicators import _Indicators
from deephaven_massive._namespaces.indices import _Indices
from deephaven_massive._namespaces.options import _Options
from deephaven_massive._namespaces.partners import _Partners
from deephaven_massive._namespaces.reference import _Reference
from deephaven_massive._namespaces.stocks import _Stocks
from deephaven_massive._tables.core import (
    ON_UPDATE_GRAPH,
    SessionCore,
    has_execution_context,
    has_script_query_scope,
    on_update_graph,
)
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from deephaven_massive._enums import Order
    from deephaven_massive._query import Query
    from deephaven_massive._types import FilterValue, Symbols

__all__ = ["CATALOG", "NAMESPACE_MODULES", "MassiveSession"]

logger = logging.getLogger(__name__)

NAMESPACE_MODULES: Final = (
    stocks,
    options,
    futures,
    indices,
    forex,
    crypto,
    reference,
    indicators,
    economy,
    partners,
)
"""Every namespace module, each exporting `DATASETS`, imported statically. A test asserts that
this tuple names every module of `_namespaces/` except `base`."""

CATALOG: Final = Catalog(spec for module in NAMESPACE_MODULES for spec in module.DATASETS)
"""Every dataset of every namespace module, built once, at import, and held by every session
core."""

_keys_lock = threading.Lock()
_session_numbers = itertools.count(1)
# Plain weak references without callbacks, pruned only in `_register` under `_keys_lock`. A
# WeakSet removes entries from garbage-collection callbacks, which run on any thread at any
# allocation and could change the set while `_register` iterates it.
_open_sessions: list[weakref.ref[SessionCore]] = []


def _next_label() -> str:
    """Return the next session label of the process.

    Returns:
        `"MassiveSession #<n>"`, numbered from 1.
    """
    with _keys_lock:
        return f"MassiveSession #{next(_session_numbers)}"


def _register(core: SessionCore) -> None:
    """Record a new open session and warn if another open session uses the same key.

    Individual Massive plans allow one WebSocket connection per cluster per account, so a
    second session with the same key would make the two compete for connection slots. Sessions
    are held weakly; closed and collected ones do not count.

    Args:
        core: The new session's core.
    """
    with _keys_lock:
        alive = [other for other in (ref() for ref in _open_sessions) if other is not None]
        _open_sessions[:] = [weakref.ref(other) for other in alive]
        _open_sessions.append(weakref.ref(core))
        others = sorted(
            other.label
            for other in alive
            if other.config.api_key == core.config.api_key and not other.closed
        )
    if others:
        logger.warning(
            "%s uses the same API key as %s; individual plans allow one WebSocket connection "
            "per cluster per account, so the sessions will compete for connection slots",
            core.label,
            others[0],
        )


class MassiveSession:
    """A connection to Massive for one API key: the namespaces, diagnostics, and lifecycle.

    Create it inside a Deephaven worker (a server console, an application script, or anywhere
    an `ExecutionContext` is active); the library captures that context for its own threads and
    never starts a JVM. Configuration comes only from `MassiveConfig`, and the session does not
    expose it again: keeping the API key out of the session's attributes keeps it one step
    further from console globals that every user of a worker can read.

    Dataset methods hang off the namespaces (`m.stocks.trades(...)`) and return queries; verbs
    on a query return Deephaven tables. Live results are released automatically when garbage
    collected, deterministically with `m.release(table)`, and all at once from the network
    side with `m.close()`. As a context manager the session closes on exit, which suits
    scripts whose results are static.

    The session's label (`MassiveSession #1`, a per-process counter) appears in its `repr`, in
    the WARNING logged when a second open session uses the same key (individual plans allow
    one WebSocket connection per cluster per account), and in error messages. Thread-safe.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()  # MASSIVE_API_KEY from the environment
            m  # <MassiveSession #1 open>
            m.stocks  # <MassiveSession #1 stocks>, the namespace of stock datasets
            conns = m.connections()  # live diagnostics: connections and host decisions
            streams = m.streams()  # live diagnostics: one row per stream
            m.close()  # live tables go static; verbs now raise MassiveError

            with dm.MassiveSession() as m2:  # closes on exit
                m2.partners.benzinga  # <MassiveSession #2 partners.benzinga>
    """

    __slots__ = (
        "_core",
        "_crypto",
        "_economy",
        "_forex",
        "_futures",
        "_indicators",
        "_indices",
        "_options",
        "_partners",
        "_reference",
        "_stocks",
    )

    def __init__(self, config: MassiveConfig | None = None) -> None:
        """Start a session.

        When `config` is None, `MassiveConfig()` reads the environment and raises its own
        `AuthenticationError` if no API key is configured (it is not repeated under Raises
        because this method does not raise it itself).

        Args:
            config: The configuration; None means `MassiveConfig()`, which reads the API key
                and S3 credentials from the environment.

        Raises:
            TypeError: If `config` is not a `MassiveConfig`.
            MassiveError: If no Deephaven `ExecutionContext` is active on this thread (the
                session must be created inside a Deephaven worker), or if the context's query
                scope is not the Python script session's own, as with
                `make_user_exec_ctx(freeze_vars=[...])`.
            ValueError: If called on an update-graph thread or under the update-graph lock.
        """
        if config is None:
            config = MassiveConfig()
        elif not isinstance(config, MassiveConfig):
            raise TypeError(f"config must be a dm.MassiveConfig, not {type(config).__name__}")
        exec_ctx = get_exec_ctx()
        if not has_execution_context(exec_ctx):
            raise MassiveError(
                "MassiveSession must be created inside a Deephaven worker: no Deephaven "
                "ExecutionContext is active on this thread. Create it from the Deephaven "
                "console or an application script, or enter a context first "
                "(with deephaven.execution_context.get_exec_ctx(): ...)."
            )
        if on_update_graph(exec_ctx):
            raise ValueError(ON_UPDATE_GRAPH)
        if not has_script_query_scope(exec_ctx):
            raise MassiveError(
                "MassiveSession needs an ExecutionContext whose query scope is the Python script "
                "session's own: the library's formulas call Python functions through it. A "
                "context from make_user_exec_ctx(freeze_vars=[...]) has a frozen copy instead; "
                "create the session from the console or an application script, or use "
                "make_user_exec_ctx() without freeze_vars."
            )
        core = SessionCore(config, exec_ctx, _next_label(), catalog=CATALOG)
        core.attach(SessionClients(core))
        _register(core)
        self._core = core
        self._stocks = _Stocks(core)
        self._options = _Options(core)
        self._futures = _Futures(core)
        self._indices = _Indices(core)
        self._forex = _Forex(core)
        self._crypto = _Crypto(core)
        self._reference = _Reference(core)
        self._indicators = _Indicators(core)
        self._economy = _Economy(core)
        self._partners = _Partners(core)

    @override
    def __repr__(self) -> str:
        """Show the session's label and whether it is open.

        Returns:
            `"<MassiveSession #1 open>"` or `"<MassiveSession #1 closed>"`, or
            `"<MassiveSession (not started)>"` for an object whose `__init__` raised (tracebacks
            show such objects).
        """
        core: SessionCore | None = getattr(self, "_core", None)
        if core is None:  # __init__ raised before the session existed
            return "<MassiveSession (not started)>"
        state = "closed" if core.closed else "open"
        return f"<{core.label} {state}>"

    @property
    def stocks(self) -> _Stocks:
        """US equities: trades, quotes, bars, snapshots, fundamentals, and filings."""
        return self._stocks

    @property
    def options(self) -> _Options:
        """US options: trades, quotes, bars, chains, snapshots, and contracts."""
        return self._options

    @property
    def futures(self) -> _Futures:
        """US futures: trades, quotes, bars, snapshots, and reference data."""
        return self._futures

    @property
    def indices(self) -> _Indices:
        """Indices: values, bars, and snapshots."""
        return self._indices

    @property
    def forex(self) -> _Forex:
        """Foreign exchange: quotes, bars, snapshots, movers, and conversion."""
        return self._forex

    @property
    def crypto(self) -> _Crypto:
        """Crypto pairs: trades, quotes, bars, FMV, snapshots, movers, and daily open/close."""
        return self._crypto

    @property
    def reference(self) -> _Reference:
        """Reference data: tickers, news, exchanges, conditions, and market status."""
        return self._reference

    @property
    def indicators(self) -> _Indicators:
        """Technical indicators: SMA, EMA, RSI, and MACD."""
        return self._indicators

    @property
    def economy(self) -> _Economy:
        """US economic data: treasury yields, inflation, labor market, funding conditions."""
        return self._economy

    @property
    def partners(self) -> _Partners:
        """Partner datasets, by vendor: `benzinga`, `etf_global`, `tmx`, `fable`."""
        return self._partners

    def snapshots(
        self,
        symbols: Symbols,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The current state of tickers of any asset classes, from Massive's unified snapshot.

        One row per ticker: its latest trade, quote, minute and session bars, and, for options,
        the greeks, implied volatility, open interest, and underlying asset. Rows are the union
        schema of the unified snapshot, so a column another class fills is null. The catalog
        record is `reference.snapshots`, the name `m.streams()` and messages show. Served over
        REST (`/v3/snapshot` with `ticker.any_of`, up to 250 tickers per request); `fetch()`
        returns one row per requested ticker and `poll()` re-fetches them, keyed by `Symbol`.
        A ticker Massive does not know or the plan does not cover keeps its row, with `Error`
        and `Message` set. `dm.ALL` is not accepted: the endpoint needs tickers, and each
        class's `snapshots(dm.ALL)` covers a whole market. Its columns, and what each means,
        are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#snapshots,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, in any accepted form of their classes (`AAPL`,
                `O:SPY251219C00600000`, `EUR/USD`, `X:BTCUSD`, `I:SPX`), or a Deephaven table
                with a `Symbol` column; each prefix selects the class.
            sort: The field to sort by (`"ticker"`); None for the endpoint's default. With
                more than 250 tickers it orders the rows within each request of 250, not
                across them.
            order: The sort direction, within each request the same way; None for the
                endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"type": "options"}`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, an invalid symbol, or an unknown filter raises `ValueError`; an argument of
        the wrong type raises `TypeError`.
        """
        query = build_query(
            self._core,
            reference.SNAPSHOTS,
            symbols=symbols,
            all_alternative=(
                "a class's snapshots(dm.ALL), such as m.stocks.snapshots(dm.ALL), for a whole "
                "market"
            ),
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    def connections(self) -> Table:
        """Return the live diagnostics table of connections and host decisions.

        One row per WebSocket connection plus one `Transport=REST` row per resolved (asset
        class, channel family), with the host, recency, evidence, state, topic counts, the
        last error, and `SchemaDrift`: the fields a connection's frames had beyond the
        documented schema (`channel:field+`) or lacked (`channel:field-`). The table is the
        session's; do not edit it. After `close()` this raises
        `MassiveError` (see `SessionCore.require_open`).

        Returns:
            The `connections` table, keyed by `Transport`, `AssetClass`, and `Family`;
            its columns are `CONNECTIONS` in `_tables/diagnostics.py`.
        """
        self._core.require_open()
        return self._core.diagnostics.connections

    def streams(self) -> Table:
        """Return the live diagnostics table of streams and `poll()` tables.

        One row per stream: dataset, symbols, `HeldResults`, deferred option contracts,
        host, rows published, requests per poll cycle, state, error, and `SchemaDrift`: the
        fields the stream's payloads had beyond the documented schema (`transport:field+`) or
        lacked (`transport:field-`), shown apart from the error because drift is not a
        failure. After `close()` this raises `MassiveError`.

        Returns:
            The `streams` table, keyed by `StreamId`; its columns are
            `STREAMS` in `_tables/diagnostics.py`.
        """
        self._core.require_open()
        return self._core.diagnostics.streams

    def release(self, table: Table) -> None:
        """Release one live result now instead of waiting for garbage collection.

        The library's hold on the table is released on the session's reaper thread, right
        away. The table keeps ticking only while something else holds it (a derived table, an
        open UI panel); once nothing does, its stream unsubscribes. Releasing a result twice
        does nothing, and `release()` keeps working after `close()`. Static results
        (`fetch()`, `history()`) are not tracked.

        Args:
            table: A live table returned by a verb of this session.

        Raises:
            TypeError: If `table` is not a Deephaven `Table`.
            ValueError: If `table` is not a live result of this session (checked by identity).
        """
        if not isinstance(table, Table):
            raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
        if not self._core.results.release(table):
            raise ValueError("table is not a live result of this session")

    def close(self) -> None:
        """Close every connection and poller; live tables go static. Idempotent.

        Publishers are not failed, so live tables keep their rows and stop updating;
        `m.streams()` shows every stream `CLOSED` before the diagnostics tables are released.
        "session closed; N live tables are now static" is logged, at WARNING when N > 0. After
        `close()`, verbs, `connections()`, and `streams()` raise
        `MassiveError("MassiveSession #1 is closed")`; building queries, reaching namespaces,
        and `release()` keep working. On an update-graph thread it raises `ValueError`, even
        when the session is already closed.
        """
        self._core.close()

    def __enter__(self) -> Self:
        """Enter a `with` block; the session closes when the block exits.

        Use it for scripts whose results are static: live tables stop updating on exit.

        Returns:
            The session.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session at the end of a `with` block.

        Args:
            exc_type: The exception type, if the block raised.
            exc: The exception, if the block raised.
            traceback: Its traceback.
        """
        self.close()
