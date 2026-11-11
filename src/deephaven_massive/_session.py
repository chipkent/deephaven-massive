"""`MassiveSession`, the user's entry point, and the composition of a session.

`MassiveSession` is what users create: it exposes the dataset namespaces (`m.stocks`, `m.options`,
...), the diagnostics tables, and the lifecycle verbs `release()` and `close()`, and nothing else.
It checks where it is constructed (an execution context whose query scope is the script
session's own, never an update-graph thread), builds the session's `SessionCore`
(`_tables/session_core.py`), which owns everything stateful and which queries and namespaces share,
builds the session's `SessionClients` (`_ingest/clients.py`), which owns every wire client, and
attaches them to the core once; then it hands the core to one object per namespace.

This module is the composition root, the one place that sees every layer: it is the only API
module that imports the ingest code, and it builds `CATALOG`, the process's dataset catalog,
once, at import, from the `DATASETS` of every namespace module. Every core holds that catalog,
so the dataset layer never has to find the namespace modules itself.

The module also numbers sessions (`MassiveSession #1`, a per-process counter, used in the
`repr`, in error messages, and in the same-key WARNING, and `massive #1` at the start of the
session's own threads' names) and keeps the weak registry of open sessions that warns when a
second open session uses the same API key with different connection settings, so the two
cannot share their WebSocket connections. Creating a session applies the logging environment
variables once per process (`_debug_log.configure_from_environment`) and logs, at INFO, the
session's label and its configuration without its secrets, so a log attached to a bug report
says what ran.
"""

from __future__ import annotations

import dataclasses
import itertools
import logging
import threading
import weakref
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final, Self, cast

from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table

import deephaven_massive.namespaces._crypto as crypto
import deephaven_massive.namespaces._economy as economy
import deephaven_massive.namespaces._forex as forex
import deephaven_massive.namespaces._futures as futures
import deephaven_massive.namespaces._indicators as indicators
import deephaven_massive.namespaces._indices as indices
import deephaven_massive.namespaces._options as options
import deephaven_massive.namespaces._partners as partners
import deephaven_massive.namespaces._reference as reference
import deephaven_massive.namespaces._stock_filings as stock_filings
import deephaven_massive.namespaces._stock_financials as stock_financials
import deephaven_massive.namespaces._stocks as stocks
from deephaven_massive import _snapshots as snapshots
from deephaven_massive._config import MassiveConfig, shown_setting
from deephaven_massive._datasets.catalog import Catalog
from deephaven_massive._debug_log import configure_from_environment
from deephaven_massive._errors import MassiveError, raises_through
from deephaven_massive._ingest.clients import CONNECTION_SETTINGS, SessionClients
from deephaven_massive._override import override
from deephaven_massive._tables.diagnostics import CONNECTIONS, RATE_LIMITS, STREAMS
from deephaven_massive._tables.execution_context import (
    has_execution_context,
    has_script_query_scope,
    refuse_update_graph_thread,
)
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.symbol_read_rows import SYMBOL_READS
from deephaven_massive.namespaces._crypto import CryptoNamespace
from deephaven_massive.namespaces._economy import EconomyNamespace
from deephaven_massive.namespaces._forex import ForexNamespace
from deephaven_massive.namespaces._futures import FuturesNamespace
from deephaven_massive.namespaces._indicators import IndicatorsNamespace
from deephaven_massive.namespaces._indices import IndicesNamespace
from deephaven_massive.namespaces._options import OptionsNamespace
from deephaven_massive.namespaces._partners import PartnersNamespace
from deephaven_massive.namespaces._query_building import build_query, list_params, shows_call
from deephaven_massive.namespaces._reference import ReferenceNamespace
from deephaven_massive.namespaces._stocks import StocksNamespace

if TYPE_CHECKING:
    from types import TracebackType

    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._enums import Order
    from deephaven_massive.typehints import Symbols
    from deephaven_massive.typehints._queries import FetchQuery

__all__ = ["CATALOG", "NAMESPACE_MODULES", "MassiveSession"]

logger = logging.getLogger(__name__)

NAMESPACE_MODULES: Final = (
    stocks,
    stock_financials,
    stock_filings,
    options,
    futures,
    indices,
    forex,
    crypto,
    reference,
    snapshots,
    indicators,
    economy,
    partners,
)
"""Every namespace module, each exporting `DATASETS`, imported statically, and `snapshots`, the
module of the dataset the session serves itself. A test asserts that this tuple names every
private module of `namespaces/` except `_query_building`, and `_snapshots`."""

CATALOG: Final = Catalog(spec for module in NAMESPACE_MODULES for spec in module.DATASETS)
"""Every dataset of every namespace module, built once, at import, and held by every session
core."""

_keys_lock = threading.Lock()
_session_numbers = itertools.count(1)
# Plain weak references without callbacks, pruned only in `_register` under `_keys_lock`. A
# WeakSet removes entries from garbage-collection callbacks, which run on any thread at any
# allocation and could change the set while `_register` iterates it.
_open_sessions: list[weakref.ref[SessionCore]] = []


def _next_number() -> int:
    """Return the next session number of the process.

    Returns:
        The number, from 1: the session is `MassiveSession #<n>`, and its threads' names start
        with `massive #<n>`.
    """
    with _keys_lock:
        return next(_session_numbers)


def _log_session(label: str, config: MassiveConfig) -> None:
    """Log, at INFO, a new session's label and its configuration, without its credentials.

    The line holds every field the configuration's `repr` shows except `s3_access_key_id`, each
    as `shown_setting` shows it, so a URL never shows a user and password; the
    API key, the S3 access key id, and the S3 secret are each shown only as set or unset, since
    a log is attached to bug reports. The two S3 credentials are set together or not at all.
    The process itself (versions, the GIL, the snapshot, the proxy and CA settings) is
    described once, when the log is turned on.

    Args:
        label: The session's label, such as `"MassiveSession #1"`.
        config: The session's configuration.
    """
    shown = ", ".join(
        f"{field.name}={shown_setting(field.name, getattr(config, field.name))}"
        for field in dataclasses.fields(config)
        if field.repr and field.name != "s3_access_key_id"
    )
    # One value serves both S3 credentials: `MassiveConfig` refuses one without the other, so
    # the access key id being set means the secret is set too.
    s3 = "set" if config.s3_access_key_id is not None else "unset"
    logger.info(
        "%s: MassiveConfig(%s) with api_key set, s3_access_key_id %s, s3_secret_access_key %s",
        label,
        shown,
        s3,
        s3,
    )


def _register(core: SessionCore) -> None:
    """Record a new open session; warn if one with the same key cannot share its connections.

    Individual Massive plans allow one WebSocket connection per cluster per account, so
    sessions of one process with the same key share their connections. They can only when
    every connection setting (`CONNECTION_SETTINGS`) is equal: a connection carries one value
    of each. A new session whose settings differ from those of an open session with the same
    key gets connections of its own, which compete with the other session's for the slot:
    Massive keeps the newer connection and closes the older, whose live tables then follow
    their session's `connection_taken`. So a WARNING names the first such session and the
    settings that differ. Sessions on
    different Deephaven update graphs never share either; that is not warned about, since a
    server normally has one graph. Sessions are held weakly; closed and collected ones do not
    count.

    Args:
        core: The new session's core.
    """
    with _keys_lock:
        alive = [other for other in (ref() for ref in _open_sessions) if other is not None]
        _open_sessions[:] = [weakref.ref(other) for other in alive]
        _open_sessions.append(weakref.ref(core))
        others = [
            (other.label, other.config)
            for other in alive  # in order of creation, so the oldest such session is named
            if other.config.api_key == core.config.api_key and not other.closed
        ]
    for label, config in others:
        differ = [n for n in CONNECTION_SETTINGS if getattr(config, n) != getattr(core.config, n)]
        if differ:
            logger.warning(
                "%s uses the same API key as %s with different connection settings (%s), so "
                "the two cannot share WebSocket connections; Massive allows an account one "
                "connection per asset class and keeps the newer, so the live tables on the "
                "older follow their session's connection_taken",
                core.label,
                label,
                ", ".join(differ),
            )
            return


class MassiveSession:  # noqa: PLR0904 - the API's top level: ten dataset namespaces, cross-asset snapshots, four diagnostics tables, release/close and the context-manager protocol; each member delegates to the core
    """Your connection to Massive: its datasets and the live tables you make from them.

    Create a session inside a Deephaven server, in its console or an application script. Its
    settings come from `dm.MassiveConfig`. Without one, it reads your API key from
    `MASSIVE_API_KEY`.

    Each dataset is a method of a namespace, such as `m.stocks.bars`, and returns a query. A method
    of the query, such as `history()` or `last_by()`, returns a table. A live table updates while
    anything uses it. `m.release(table)` drops your variable's hold on one, and `m.close()` stops
    them all. In a `with` block, the session closes when the block ends. See
    https://github.com/chipkent/deephaven-massive/blob/main/README.md#stop-live-tables.

    No public attribute or `repr` of the session shows your key, so the key stays out of the
    console's variables.

    You can use a session from several threads at once.

    Messages name the session by its label, such as `MassiveSession #1`.

    Use one session per API key. Massive allows your account one streaming connection per asset
    class. Two sessions with the same key share it only when their streaming settings match, such
    as `websocket_hosts` and `max_outage`. Otherwise each opens its own, and the newer connection
    takes the older one's place. The live tables on the older one then fail, or reconnect if their
    session's `MassiveConfig.connection_taken` is `RECONNECT`. A WARNING names the clash when you
    create the second session.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            apple = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
            streams = m.streams()  # each live table's state and errors
            m.close()
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

    @raises_through(
        MassiveConfig.__post_init__, configure_from_environment, refuse_update_graph_thread
    )
    def __init__(self, config: MassiveConfig | None = None) -> None:
        """Create a session.

        Args:
            config: The session's settings. Leave it out for `dm.MassiveConfig()`, which reads your
                keys from the environment.

        Raises:
            TypeError: If `config` isn't a `dm.MassiveConfig`.
            AuthenticationError: If `config` is left out and `MASSIVE_API_KEY` isn't set.
            ValueError: If:
                - it runs on a Deephaven update-graph thread, such as inside a table listener;
                - for the first session of the process, `DEEPHAVEN_MASSIVE_LOG` isn't a log level
                  name, or either log variable has spaces around it;
                - `config` is left out and an environment variable isn't valid, such as a key with
                  spaces around it, or only one of the two `MASSIVE_S3_*` variables set.
            OSError: For the first session of the process, if `DEEPHAVEN_MASSIVE_LOG_FILE` names a
                file that can't be opened.
            MassiveError: If it runs on a thread with no Deephaven execution context, or in one made
                by `make_user_exec_ctx(freeze_vars=...)`, which the library can't use.
        """
        if config is None:
            config = MassiveConfig()
        elif not isinstance(config, MassiveConfig):
            raise TypeError(f"config must be a dm.MassiveConfig, not {type(config).__name__}")
        exec_ctx = get_exec_ctx()
        if not has_execution_context(exec_ctx):
            raise MassiveError(
                "MassiveSession must be created inside a Deephaven server: no Deephaven "
                "ExecutionContext is active on this thread. Create it from the Deephaven "
                "console or an application script, or enter a context first "
                "(with deephaven.execution_context.get_exec_ctx(): ...)."
            )
        refuse_update_graph_thread(exec_ctx)
        if not has_script_query_scope(exec_ctx):
            raise MassiveError(
                "MassiveSession needs an ExecutionContext whose query scope is the Python script "
                "session's own: the library's formulas call Python functions through it. A "
                "context from make_user_exec_ctx(freeze_vars=[...]) has a frozen copy instead; "
                "create the session from the console or an application script, or use "
                "make_user_exec_ctx() without freeze_vars."
            )
        configure_from_environment()
        number = _next_number()
        label = f"MassiveSession #{number}"
        _log_session(label, config)
        core = SessionCore(
            config, exec_ctx, label, catalog=CATALOG, thread_prefix=f"massive #{number}"
        )
        core.attach(SessionClients(core))
        _register(core)
        self._core = core
        self._stocks = StocksNamespace(core)
        self._options = OptionsNamespace(core)
        self._futures = FuturesNamespace(core)
        self._indices = IndicesNamespace(core)
        self._forex = ForexNamespace(core)
        self._crypto = CryptoNamespace(core)
        self._reference = ReferenceNamespace(core)
        self._indicators = IndicatorsNamespace(core)
        self._economy = EconomyNamespace(core)
        self._partners = PartnersNamespace(core)

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
    def stocks(self) -> StocksNamespace:
        """US equities: trades, quotes, bars, snapshots, fundamentals, and filings."""
        return self._stocks

    @property
    def options(self) -> OptionsNamespace:
        """US options: trades, quotes, bars, chains, snapshots, and contracts."""
        return self._options

    @property
    def futures(self) -> FuturesNamespace:
        """US futures: trades, quotes, bars, snapshots, contracts, products, and schedules."""
        return self._futures

    @property
    def indices(self) -> IndicesNamespace:
        """Indices: values, bars, and snapshots."""
        return self._indices

    @property
    def forex(self) -> ForexNamespace:
        """Forex: quotes, bars, snapshots, movers, and currency conversion."""
        return self._forex

    @property
    def crypto(self) -> CryptoNamespace:
        """Crypto: trades, quotes, bars, fair market value, snapshots, and movers."""
        return self._crypto

    @property
    def reference(self) -> ReferenceNamespace:
        """Reference data: tickers, news, exchanges, conditions, market status, and holidays."""
        return self._reference

    @property
    def indicators(self) -> IndicatorsNamespace:
        """Technical indicators: SMA, EMA, RSI, and MACD."""
        return self._indicators

    @property
    def economy(self) -> EconomyNamespace:
        """US economic data: treasury yields, inflation, labor market, and funding conditions."""
        return self._economy

    @property
    def partners(self) -> PartnersNamespace:
        """Datasets from Massive's partners: `benzinga`, `etf_global`, `tmx`, and `fable`."""
        return self._partners

    @shows_call("m")
    @raises_through(build_query, list_params)
    def snapshots(
        self,
        symbols: Symbols,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """The current state of tickers of any asset class, from Massive's unified snapshot.

        Each row holds one ticker's latest trade, quote, and minute bar, and its session's open,
        high, low, close, and change. An option's row adds its greeks, implied volatility, and open
        interest. A column of another asset class is null. A ticker Massive doesn't know, or your
        plan doesn't cover, keeps its row. Its `Error` and `Message` give the reason.

        Args:
            symbols: Tickers of any asset class, such as `["AAPL", "X:BTCUSD", "I:SPX"]`. `dm.ALL`
                is refused; use one class's `snapshots(dm.ALL)` instead.
            sort: The field to sort by, such as `"ticker"`. Massive sorts each request of up to 250
                tickers on its own, so a longer list isn't sorted as a whole.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, except `type`, which Massive refuses beside
                tickers.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/snapshots.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None, `dm.ALL`, or empty;
                - a ticker isn't Massive's exact ticker, or a table has no `Symbol` column;
                - `limit` isn't positive;
                - `filters` holds `type`, a key the endpoint doesn't take, or a value that doesn't
                  fit.

        Example:
            ::

                mixed = m.snapshots(["AAPL", "C:EURUSD", "X:BTCUSD", "I:SPX"]).fetch()
        """
        if isinstance(filters, Mapping) and "type" in filters:
            logger.debug("snapshots: refused the type filter %r, sent beside tickers", filters)
            raise ValueError(
                "snapshots takes no type filter: Massive refuses type beside tickers, and each "
                "ticker's prefix selects its class"
            )
        query = build_query(
            self._core,
            snapshots.SNAPSHOTS,
            symbols=symbols,
            all_alternative=(
                "use a class's snapshots(dm.ALL), such as m.stocks.snapshots(dm.ALL), for a whole "
                "market"
            ),
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
        )
        return cast("FetchQuery", query)

    @raises_through(SessionCore.diagnostics_view)
    def connections(self) -> Table:
        """Return a live table of the session's streaming connections to Massive.

        Look here when a stream gets no data. Each row says whether a connection is up, whether its
        data is real-time or delayed, and its last error. A connection that another session with the
        same key shares shows in both sessions' tables. The table also has a `REST` row for each
        asset class and kind of data whose feed the session has found. Each call returns a new
        table, which keeps its final rows after `close()`. What to do about each state:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#diagnostics.

        Returns:
            A live table keyed by `Transport`, `AssetClass`, and `Family`, with these columns:

            - `Host` and `Path`: where the connection goes.
            - `Recency`: `REALTIME`, `DELAYED`, or `UNKNOWN`. `UNKNOWN` means the library can't
              tell, and it never treats such data as real-time.
            - `Evidence`: what showed the library that recency.
            - `State`: `RESOLVING`, `CONNECTING`, `CONNECTED`, `WAITING_SLOT`, `RECONNECTING`,
              `FAILED`, or `CLOSED`.
            - `Topics`, `AckedTopics`, `PendingTopics`, and `NoAckTopics`: the subscriptions sent,
              confirmed by Massive, waiting for confirmation, and never confirmed.
            - `BufferedRows`: rows waiting to reach their tables. Past 1,000,000, a WARNING is
              logged.
            - `Reconnects`, `LastMessageAt`, `NextReconnectAt`, `UpdatedAt`, and `LastError`.
            - `SchemaDrift`: how Massive's messages differed from its documentation.
              `channel:field+` is a field the documentation lacks. `channel:field-` is a documented
              field that was missing. `channel:field?` is a value that didn't fit its column and was
              read as null. `channel:field~` is a whole number past 2**53, read rounded.
              `channel:<n> rows dropped` counts rows whose symbol or time couldn't be read.

        Raises:
            MassiveError: If the session is closed.
        """
        return self._core.diagnostics_view(self._core.diagnostics.connections, CONNECTIONS)

    @raises_through(SessionCore.diagnostics_view)
    def streams(self) -> Table:
        """Return a live table of the session's streams, polls, and background reads.

        Start here when a live table looks wrong. Each row is one stream, `poll()` table, or
        background read. The tables of identical queries share one stream and one row. Each call
        returns a new table, which keeps its final rows after `close()`.
        What to do about each state:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#diagnostics.

        Returns:
            A live table keyed by `StreamId`, with these columns:

            - `Kind`: `stream` for a stream method's table, and `poll` for a `poll()` table. `read`
              is a background read (`wait=False`), or a `history()` or `fetch()` that follows a
              watchlist.
            - `Dataset`: the dataset's name, such as `stocks.trades`.
            - `Query`: the call that built the query, such as `m.stocks.trades('AAPL')`.
            - `Symbols`: the symbols it reads now; empty for a dataset that takes none.
            - `SymbolCount`: how many symbols it reads; null for `dm.ALL` and for a dataset that
              takes none.
            - `HeldResults`: how many of the stream's tables are still in use.
            - `Deferred`: how many option contracts wait for room on the options connection.
            - `Transport`: `WebSocket`, `REST`, `flat files`, or `flat files and REST`.
            - `Host` and `Path`: where a stream connects.
            - `Recency`: `REALTIME`, `DELAYED`, or `UNKNOWN`. `UNKNOWN` means the library can't
              tell, and it never treats such data as real-time. The column is null for a background
              read and for reference, economy, and partner data.
            - `RowsPublished`: how many rows it has delivered.
            - `Loading`: how many symbols a watchlist's read has yet to read.
            - `RequestsPerCycle`: the REST requests of the latest poll.
            - `State`: `RUNNING`, `LOADING`, `DONE`, `FAILED`, `RELEASED`, or `CLOSED`.
            - `Error`: why it failed, or the warnings that leave it running. A connection that is
              down is one such warning.
            - `Gaps`: the latest five spans in which rows may be missing, as `<start> to <end>` in
              UTC. A recovered outage leaves one. A gap stays listed after `Error` clears.
            - `CreatedAt`: when it started.
            - `SchemaDrift`: how Massive's data differed from its documentation, in the notation
              `connections()` describes, with each entry named by transport.

        Raises:
            MassiveError: If the session is closed.
        """
        return self._core.diagnostics_view(self._core.diagnostics.streams, STREAMS)

    @raises_through(SessionCore.diagnostics_view)
    def rate_limits(self) -> Table:
        """Return a live table of Massive's REST request limits, one row per subscription.

        Massive limits each subscription's REST requests. A free Basic plan allows five a minute,
        and a paid plan has no limit. The limit counts every request your account makes, from every
        key and session. Each row is one subscription, or `other` for the datasets Massive assigns
        to none. Sessions in this server that use the same key share these rows. Each call returns a
        new table, which keeps its final rows after `close()`. What to do about each state:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#diagnostics.

        Returns:
            A live table keyed by `Bucket`, which is `stocks`, `options`, `indices`, `currencies`,
            `futures`, or `other`. It has these columns:

            - `State`: `READY`; `PACED`, to fit a known limit; or `PAUSED` until `PausedUntil`,
              after Massive refused a request for coming too fast.
            - `RequestsPerMinute`: the limit the library keeps to.
            - `LimitSource`: `DECLARED` in `MassiveConfig.rest_basic_subscriptions`, or `LEARNED`
              from Massive's refusal.
            - `InFlight`, `Waiting`, `RecentRequests` (in the last 61 seconds), `Attempts`, and
              `Refusals`.
            - `PausedUntil`, `LastRefusal`, and `UpdatedAt`.

        Raises:
            MassiveError: If the session is closed.
        """
        return self._core.diagnostics_view(self._core.diagnostics.rate_limits, RATE_LIMITS)

    @raises_through(SessionCore.diagnostics_view)
    def symbol_reads(self) -> Table:
        """Return a live table of each symbol of each `history()` or `fetch()` of a watchlist.

        A `history()` or `fetch()` of a watchlist reads each symbol you add and drops each one you
        delete. This table shows each symbol's read, so you can find the ones that failed and why.

        A read that fails for a reason that can clear, such as a rate limit or a network error, is
        `RETRYING`. It's tried again 1, 5, and 15 minutes later, then marked `FAILED`. Any other
        failure is `FAILED` at once. To read a failed symbol again, delete its row from the
        watchlist and add it back.

        The rows stay for the session's life. Each call returns a new table, which keeps its final
        rows after `close()`. What to do about each state:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#diagnostics.

        Returns:
            A live table keyed by `StreamId` and `Symbol`, with these columns:

            - `StreamId`: the result's row in `m.streams()`.
            - `Dataset`: the dataset's name.
            - `State`: `QUEUED`, `LOADING`, `RETRYING`, `DONE`, `FAILED`, `REMOVED`, `STOPPED`, or
              `CLOSED`. `DONE` can mean 0 rows, when Massive has no data for the symbol. `REMOVED`
              means its row left the watchlist, and `STOPPED` that the result was released.
            - `Transport`: `REST`, `flat files`, or `flat files and REST` when some days' files were
              missing.
            - `AddedAt`: when its row last joined the watchlist.
            - `ReadAt`: when the download behind its rows began.
            - `Rows`: its rows in the result.
            - `Attempts`: reads started since it was last added.
            - `NextAttemptAt`: the next try, while it's `RETRYING`.
            - `Error`: its latest failure, with the HTTP status, path, and `request_id`. It's null
              once a read succeeds.
            - `UpdatedAt`: when the row last changed.

        Raises:
            MassiveError: If the session is closed.

        Example:
            ::

                failed = m.symbol_reads().where("State = `FAILED`")
        """
        return self._core.diagnostics_view(self._core.diagnostics.symbol_reads, SYMBOL_READS)

    def release(self, table: Table) -> None:
        """Release one live table now, instead of waiting for Python to reclaim it.

        The table stops updating once nothing else uses it, such as a table made from it or a panel
        open in the Deephaven IDE. Its poll or background read then stops. Its stream unsubscribes
        once no other table of the stream is in use. In a console command, the release takes effect
        when the command ends. Releasing a table twice does nothing, and `release()` works after
        `close()`. A static table needs no release. See
        https://github.com/chipkent/deephaven-massive/blob/main/README.md#stop-live-tables.

        Args:
            table: A live table that a query method of this session returned. For a table made from
                one, release the table it came from.

        Raises:
            TypeError: If `table` isn't a Deephaven `Table`.
            ValueError: If `table` isn't a live table of this session.
        """
        if not isinstance(table, Table):
            raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
        if not self._core.results.release(table):
            raise ValueError(
                "table is not a live result of this session: release() takes a table that this "
                "session returned from blink(), append_only(), ring(), last_by(), poll(), "
                "fetch(wait=False), history(..., wait=False), history() or fetch() of a "
                "refreshing symbol table, connections(), streams(), rate_limits(), or "
                "symbol_reads(). For a table derived from one, release the live table it came "
                "from; a static fetch() or history() table has nothing to release"
            )

    @raises_through(SessionCore.close, refuse_update_graph_thread)
    def close(self) -> None:
        """Stop every live table of the session. Calling it again does nothing.

        Each live table, and each table made from one, fails with
        `MassiveError("MassiveSession #1 was closed; this table no longer updates")`. It keeps its
        rows, and `t.snapshot()` gives a static copy to keep working with. In a console command,
        `close()` returns at once. The tables show their final state when the command ends.

        Connections that no other session uses are closed. Afterward, the query methods and the
        diagnostics methods, such as `m.streams()`, raise `MassiveError`. Building queries and
        `release()` still work. See
        https://github.com/chipkent/deephaven-massive/blob/main/README.md#stop-live-tables.

        Raises:
            ValueError: If called on a Deephaven update-graph thread, such as inside a table
                listener.

        Example:
            ::

                trades = m.stocks.trades("AAPL").append_only()
                m.close()
                kept = trades.snapshot()  # a static copy of the rows received before the close
        """
        self._core.close()

    def __enter__(self) -> Self:
        """Enter a `with` block; the session closes when the block ends.

        Use it in a script whose results are static: its live tables fail when the block ends.

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
