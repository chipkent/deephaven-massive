"""`MassiveSession`, the user's entry point, and `SessionCore`, its internal engine.

`MassiveSession` is what users create: it holds the configuration, exposes the dataset
namespaces (`m.stocks`, `m.options`, ...), the diagnostics tables, and the lifecycle verbs
`release()` and `close()` (DESIGN § 6). It delegates everything stateful to a `SessionCore`,
which queries and namespaces share. The core owns:

- the captured Deephaven `ExecutionContext`, entered on every library thread;
- `registry_lock`, the one leaf lock that guards every session registry (DESIGN § 18): no
  Deephaven call, network call, or blocking wait happens while it is held;
- the stream registry, one stream per `(dataset, channel, symbol key)`, each with its count of
  library-held results;
- the result registry and the reaper thread (`_liveness.py`);
- the diagnostics tables (`_diagnostics.py`);
- one factory hook per transport (`rest_transport`, `stream_transport`,
  `flat_file_transport`), which the transport work packages fill in. Until then each raises
  `NotImplementedError`, so the verbs that need it do too.

Lock order: the update-graph (UG) lock, then `registry_lock`. The core also has two non-leaf
locks taken *before* either: `_open_lock`, which makes stream creation single-flight, and
`_publish_lock`, which serializes diagnostics writes so the last write carries the latest state.
Neither is ever taken on a UG thread or while `registry_lock` is held.
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import threading
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Self

from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.update_graph import has_exclusive_lock, has_shared_lock

from deephaven_massive._config import MassiveConfig, log_json_codec_once
from deephaven_massive._diagnostics import Diagnostics, StreamRow, StreamState, describe_symbols
from deephaven_massive._domains.base import list_params
from deephaven_massive._domains.crypto import _Crypto
from deephaven_massive._domains.economy import _Economy
from deephaven_massive._domains.forex import _Forex
from deephaven_massive._domains.futures import _Futures
from deephaven_massive._domains.indicators import _Indicators
from deephaven_massive._domains.indices import _Indices
from deephaven_massive._domains.options import _Options
from deephaven_massive._domains.partners import _Partners
from deephaven_massive._domains.reference import _Reference
from deephaven_massive._domains.stocks import _Stocks
from deephaven_massive._errors import MassiveError
from deephaven_massive._liveness import Reaper, ResultRegistry, make_result
from deephaven_massive._timeunits import Transport
from deephaven_massive._types import ALL

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import TracebackType

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._enums import Order
    from deephaven_massive._liveness import StreamHandle
    from deephaven_massive._query import (
        FlatFileTransport,
        Query,
        Request,
        RestTransport,
        StreamKey,
        StreamTransport,
    )
    from deephaven_massive._types import FilterValue, Symbols

__all__ = ["MassiveSession", "SessionCore"]

logger = logging.getLogger(__name__)

_POISONED_UPDATE_GRAPH: Final = "PoisonedUpdateGraph"
"""The update graph Deephaven gives a thread that has no execution context."""

_session_numbers = itertools.count(1)
_keys_lock = threading.Lock()
_sessions_by_key: dict[str, list[weakref.ref[SessionCore]]] = {}


def _has_execution_context(exec_ctx: ExecutionContext) -> bool:
    """Tell whether an execution context is a real one, not the thread's empty default.

    A thread without a context gets Deephaven's default context, whose update graph is a
    `PoisonedUpdateGraph` on which no refreshing table can be created.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True if live tables can be created under it.
    """
    j_update_graph = exec_ctx.update_graph.j_update_graph
    return str(j_update_graph.getClass().getSimpleName()) != _POISONED_UPDATE_GRAPH


def _on_update_graph(exec_ctx: ExecutionContext) -> bool:
    """Tell whether the calling thread is an update-graph thread or holds the UG lock.

    Args:
        exec_ctx: The session's execution context.

    Returns:
        True on a UG refresh thread (for example inside a table listener), or while the
        thread holds the UG's shared or exclusive lock.
    """
    update_graph = exec_ctx.update_graph
    return (
        bool(update_graph.j_update_graph.currentThreadProcessesUpdates())
        or has_exclusive_lock(update_graph)
        or has_shared_lock(update_graph)
    )


def _require_not_update_graph(exec_ctx: ExecutionContext) -> None:
    """Reject a library call made on an update-graph thread or under the update-graph lock.

    Verbs create tables and take the registry lock; on a UG thread (for example inside a table
    listener) that would deadlock against the update cycle (DESIGN § 18.3).

    Args:
        exec_ctx: The session's execution context.

    Raises:
        ValueError: If the calling thread is a UG thread or holds the UG lock.
    """
    if _on_update_graph(exec_ctx):
        raise ValueError(
            "deephaven_massive calls must not run on a Deephaven update-graph thread "
            "(for example inside a table listener) or while holding the update-graph lock"
        )


def _key_digest(api_key: str) -> str:
    """Return a digest of an API key, so the in-process session registry holds no secret.

    Args:
        api_key: The key.

    Returns:
        Its SHA-256 hex digest.
    """
    return hashlib.sha256(api_key.encode()).hexdigest()


def _register_key(core: SessionCore) -> None:
    """Record a new session's key and warn if another open session already uses it.

    Individual Massive plans allow one WebSocket connection per cluster per account, so a
    second session with the same key would make the two fight over connection slots.

    Args:
        core: The new session's core.
    """
    digest = _key_digest(core.config.api_key)
    with _keys_lock:
        refs = [ref for ref in _sessions_by_key.get(digest, []) if ref() is not None]
        others = [other for ref in refs if (other := ref()) is not None and not other.closed]
        refs.append(weakref.ref(core))
        _sessions_by_key[digest] = refs
    if others:
        logger.warning(
            "%s uses the same API key as %s; individual plans allow one WebSocket connection "
            "per cluster per account, so the sessions will compete for connection slots",
            core.name,
            others[0].name,
        )


@dataclass(frozen=True, slots=True)
class _StreamState:
    """One open stream in the registry; replaced, never mutated, under `registry_lock`.

    Attributes:
        key: The stream's key.
        handle: What the WebSocket transport returned.
        held: Library-held results of the stream (at least 1 while registered).
    """

    key: StreamKey
    handle: StreamHandle
    held: int


class SessionCore:
    """The internal engine of one `MassiveSession`, shared by its namespaces and queries.

    Created by `MassiveSession.__init__` on the caller thread, with an execution context
    active. Thread-safe: every mutable registry is guarded by `registry_lock`.
    """

    def __init__(self, config: MassiveConfig, exec_ctx: ExecutionContext, name: str) -> None:
        """Create the core, its reaper thread, and the diagnostics tables.

        Args:
            config: The validated configuration.
            exec_ctx: The caller's execution context, captured for library threads.
            name: The session's display name, such as `"MassiveSession#1"`.
        """
        self.config: Final = config
        self.exec_ctx: Final = exec_ctx
        self.name: Final = name
        self.registry_lock: Final = threading.RLock()
        self._open_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._reaper = Reaper(exec_ctx, f"{name}-reaper")
        weakref.finalize(self, self._reaper.stop)
        self.results: Final = ResultRegistry(self.registry_lock, self._reaper)
        self.diagnostics: Final = Diagnostics()
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamState] = {}
        self._rows: dict[int, StreamRow] = {}
        self._closers: list[Callable[[], None]] = []
        self._closed = False

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        with self.registry_lock:
            return self._closed

    def require_verb_thread(self) -> None:
        """The guard every verb runs first.

        On an update-graph thread (for example inside a table listener), or while holding the
        update-graph lock, it raises `ValueError` through `require_not_update_graph`: verbs
        create tables and take the registry lock, which would deadlock there.

        Raises:
            MassiveError: If the session is closed.
        """
        if self.closed:
            raise MassiveError(f"{self.name} is closed")
        self.require_not_update_graph()

    def require_not_update_graph(self) -> None:
        """Reject a call from an update-graph thread, or one holding the update-graph lock.

        Raises `ValueError` in that case (see `_require_not_update_graph`).
        """
        _require_not_update_graph(self.exec_ctx)

    # --- Transport factory hooks: one per transport, filled by the transport packages. -------

    def rest_transport(self) -> RestTransport:
        """Return the session's REST transport.

        Raises:
            NotImplementedError: Always, until the REST transport is implemented.
        """
        raise NotImplementedError("the REST transport is not implemented yet")

    def stream_transport(self) -> StreamTransport:
        """Return the session's WebSocket transport.

        Raises:
            NotImplementedError: Always, until the WebSocket transport is implemented.
        """
        raise NotImplementedError("the WebSocket transport is not implemented yet")

    def flat_file_transport(self) -> FlatFileTransport:
        """Return the session's flat-file transport.

        Raises:
            NotImplementedError: Always, until the flat-file transport is implemented.
        """
        raise NotImplementedError("the flat-file transport is not implemented yet")

    # --- Lifecycle ------------------------------------------------------------------------

    def on_close(self, closer: Callable[[], None]) -> None:
        """Register a transport's cleanup (close connections, stop pollers) for `close()`.

        Args:
            closer: Called once, on the thread that closes the session, without any lock
                held. An exception it raises is logged at ERROR and the remaining closers
                still run.
        """
        with self.registry_lock:
            self._closers.append(closer)

    def stream_result(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        channel: str | None,
        build: Callable[[Table], Table],
        wrap: bool = False,
    ) -> Table:
        """Create one live result over the stream of `(spec, channel, symbols)`.

        The stream is reused when one with the same key is open; otherwise the WebSocket
        transport opens it (single-flight per session). The result is built from the stream's
        blink table in its own result scope on this thread, inside the session's execution
        context, and tracked so that garbage collection or `release()` frees it. If the
        session closes while the stream is opening, the call raises `MassiveError`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            build: Turns the stream's blink table into the result.
            wrap: True when `build` returns a new wrapper of the blink table (`blink()`).

        Returns:
            The live result, a distinct Python object per call.
        """
        key: StreamKey = (spec.name, channel, request.symbol_key())
        handle = self._acquire(key)
        if handle is None:
            with self._open_lock:
                handle = self._acquire(key) or self._open(spec, request, channel=channel, key=key)
        blink = handle.blink
        try:
            with self.exec_ctx:
                result, scope = make_result(lambda: build(blink), wrap=wrap)
        except Exception:
            self._release_hold(handle.stream_id)
            raise
        self.results.track(result, scope, functools.partial(self._release_hold, handle.stream_id))
        return result

    def _acquire(self, key: StreamKey) -> StreamHandle | None:
        """Take one more hold on the open stream with `key`, if there is one.

        Args:
            key: The stream key.

        Returns:
            The stream's handle, or None when no stream with that key is open.
        """
        with self.registry_lock:
            stream_id = self._stream_ids.get(key)
            if stream_id is None:
                return None
            state = self._streams[stream_id]
            self._streams[stream_id] = replace(state, held=state.held + 1)
            self._rows[stream_id] = replace(self._rows[stream_id], held_results=state.held + 1)
        self._publish(stream_id)
        return state.handle

    def _open(
        self, spec: DatasetSpec, request: Request, *, channel: str | None, key: StreamKey
    ) -> StreamHandle:
        """Open a new stream through the WebSocket transport and register it with one hold.

        Called with `_open_lock` held and `registry_lock` free.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            key: The stream key.

        Returns:
            The new stream's handle.

        Raises:
            MassiveError: If the session was closed while the transport opened the stream; the
                new stream's scope is released.
        """
        with self.exec_ctx:
            handle = self.stream_transport().open_stream(spec, request, channel)
        count, text = describe_symbols(request.symbols) if request.symbols else (None, "")
        row = StreamRow(
            stream_id=handle.stream_id,
            dataset=spec.name,
            symbol_count=count,
            symbols=text,
            held_results=1,
            transport=Transport.WS.value,
            host=handle.host,
            path=handle.path,
            created_at=time.time_ns(),
        )
        with self.registry_lock:
            closed = self._closed
            if not closed:
                self._stream_ids[key] = handle.stream_id
                self._streams[handle.stream_id] = _StreamState(key, handle, 1)
                self._rows[handle.stream_id] = row
        if closed:
            with self.exec_ctx:
                handle.scope.release()
            raise MassiveError(f"{self.name} was closed while a stream was opening")
        self._publish(handle.stream_id)
        return handle

    def _release_hold(self, stream_id: int) -> None:
        """Drop one hold on a stream; at zero, forget it and release its stream scope.

        Runs on the reaper thread when a result is released, or on the caller thread when
        building a result failed. Releasing the stream scope drops the library's reference to
        the blink table; the table dies, and its shutdown callback releases the topics, when
        nothing else (a derived table, an open panel) refers to it.

        Args:
            stream_id: The stream whose result was released.
        """
        with self.registry_lock:
            state = self._streams[stream_id]
            held = state.held - 1
            row = self._rows[stream_id]
            if held:
                self._streams[stream_id] = replace(state, held=held)
            else:
                # A key maps to its open stream until that stream's last hold is dropped.
                del self._streams[stream_id]
                del self._stream_ids[state.key]
            released = not held and row.state is StreamState.RUNNING
            self._rows[stream_id] = replace(
                row, held_results=held, state=StreamState.RELEASED if released else row.state
            )
        if not held:
            with self.exec_ctx:
                state.handle.scope.release()
        self._publish(stream_id)

    def _publish(self, stream_id: int) -> None:
        """Write a stream's current row to `m.streams()`.

        Args:
            stream_id: The stream.
        """
        with self._publish_lock:
            with self.registry_lock:
                row = self._rows[stream_id]
            with self.exec_ctx:
                self.diagnostics.write_stream(row)

    def close(self) -> None:
        """Close the session: run the closers, mark streams closed, release the diagnostics.

        Idempotent. Publishers are not failed: live tables stop receiving rows and go static,
        and keep their contents. Results stay tracked, so garbage collection and `release()`
        still free them. Logs one WARNING with the number of live tables that became static.
        A call on an update-graph thread raises `ValueError`.
        """
        self.require_not_update_graph()
        with self.registry_lock:
            if self._closed:
                return
            self._closed = True
            closers = tuple(self._closers)
            stream_ids = tuple(self._rows)
            for stream_id in stream_ids:
                self._rows[stream_id] = replace(self._rows[stream_id], state=StreamState.CLOSED)
        for closer in closers:
            try:
                closer()
            except Exception:
                logger.exception("%s: a transport failed to close", self.name)
        for stream_id in stream_ids:
            self._publish(stream_id)
        self.diagnostics.release()
        logger.warning("session closed; %d live tables are now static", self.results.held())


class MassiveSession:
    """A connection to Massive for one API key: the namespaces, diagnostics, and lifecycle.

    Create it inside a Deephaven worker (a server console, an application script, or anywhere
    an `ExecutionContext` is active); the library captures that context for its own threads and
    never starts a JVM. Configuration comes only from `MassiveConfig`.

    Dataset methods hang off the namespaces (`m.stocks.trades(...)`) and return queries; verbs
    on a query return Deephaven tables. Live results are released automatically when garbage
    collected, deterministically with `m.release(table)`, and all at once from the network
    side with `m.close()`. As a context manager the session closes on exit, which suits
    scripts whose results are static.

    Creating a second session with the same key in one process logs a WARNING: individual
    plans allow one WebSocket connection per cluster per account. Thread-safe.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()  # MASSIVE_API_KEY from the environment
            trades = m.stocks.trades(["AAPL", "MSFT"]).ring(10_000)
            conns = m.connections()
            m.release(trades)

            with dm.MassiveSession() as m2:
                bars = m2.stocks.bars(["AAPL"], dm.Timespan.DAY).history("2025-01-01", "2025-06-30")
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

        `MassiveConfig()` (used when `config` is None) raises `AuthenticationError` when no API
        key is configured, and a call on an update-graph thread raises `ValueError`.

        Args:
            config: The configuration; None means `MassiveConfig()`, which reads the API key
                and S3 credentials from the environment.

        Raises:
            TypeError: If `config` is not a `MassiveConfig`.
            MassiveError: If no Deephaven `ExecutionContext` is active on this thread: the
                session must be created inside a Deephaven worker.
        """
        if config is None:
            config = MassiveConfig()
        elif not isinstance(config, MassiveConfig):
            raise TypeError(f"config must be a dm.MassiveConfig, not {type(config).__name__}")
        exec_ctx = get_exec_ctx()
        if not _has_execution_context(exec_ctx):
            raise MassiveError(
                "MassiveSession must be created inside a Deephaven worker: no Deephaven "
                "ExecutionContext is active on this thread. Create it from the Deephaven "
                "console or an application script, or enter a context first "
                "(with deephaven.execution_context.get_exec_ctx(): ...)."
            )
        _require_not_update_graph(exec_ctx)
        log_json_codec_once()
        core = SessionCore(config, exec_ctx, f"MassiveSession#{next(_session_numbers)}")
        _register_key(core)
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

    @property
    def name(self) -> str:
        """The session's name in logs and warnings, such as `"MassiveSession#1"`."""
        return self._core.name

    @property
    def config(self) -> MassiveConfig:
        """The session's configuration."""
        return self._core.config

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
        """Crypto pairs: trades, quotes, bars, snapshots, and movers."""
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
        """Return the unified snapshot (`GET /v3/snapshot`) of tickers of any asset classes.

        Requests carry up to 250 tickers each; rows are the union schema of the unified
        snapshot. `dm.ALL` is not accepted: the endpoint needs tickers, and each class's
        `snapshots(dm.ALL)` covers a whole market. `limit` caps the total number of rows.
        Invalid `sort`, `order`, or `limit` values raise `TypeError` or `ValueError`.

        Args:
            symbols: The tickers, in canonical form with their class prefixes (`AAPL`,
                `O:SPY251219C00600000`, `C:EURUSD`, ...).
            sort: The SDK's sort field, or None.
            order: The sort direction, or None.
            limit: The maximum number of rows, or None for all.
            filters: Further SDK parameters by name, such as `{"type": "stocks"}`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        Raises:
            ValueError: If `symbols` is `dm.ALL`.
            NotImplementedError: Always, until the unified snapshot dataset is implemented.
        """
        list_params(sort=sort, order=order, limit=limit)
        if symbols is ALL:
            raise ValueError(
                "m.snapshots() needs tickers; use a class's snapshots(dm.ALL), such as "
                "m.stocks.snapshots(dm.ALL), for a whole market"
            )
        del filters  # validated against the dataset's SDK parameters once it is implemented
        raise NotImplementedError("m.snapshots() is not implemented yet")

    def connections(self) -> Table:
        """Return the live diagnostics table of connections and host decisions.

        One row per WebSocket connection plus one `Transport=REST` row per resolved (asset
        class, channel family), with the host, recency, evidence, state, topic counts, and the
        last error. The table is the session's; do not edit it.

        Returns:
            The keyed `connections` table (columns in DESIGN § 17).

        Raises:
            MassiveError: If the session is closed.
        """
        if self._core.closed:
            raise MassiveError(f"{self.name} is closed")
        return self._core.diagnostics.connections

    def streams(self) -> Table:
        """Return the live diagnostics table of streams and `poll()` tables.

        One row per stream: dataset, symbols, `HeldResults`, deferred option contracts,
        host, rows published, requests per poll cycle, state, and error.

        Returns:
            The keyed `streams` table (columns in DESIGN § 17).

        Raises:
            MassiveError: If the session is closed.
        """
        if self._core.closed:
            raise MassiveError(f"{self.name} is closed")
        return self._core.diagnostics.streams

    def release(self, table: Table) -> None:
        """Release one live result now instead of waiting for garbage collection.

        The library's hold on the table is released on the session's reaper thread, right
        away. The table keeps ticking only while something else holds it (a derived table, an
        open UI panel); once nothing does, its stream unsubscribes. Releasing a result twice
        does nothing. Static results (`fetch()`, `history()`) are not tracked, and a table
        that is not a live result of this session raises `ValueError`.

        Args:
            table: A live table returned by a verb of this session.

        Raises:
            TypeError: If `table` is not a Deephaven `Table`.
        """
        if not isinstance(table, Table):
            raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
        self._core.results.release(table)

    def close(self) -> None:
        """Close every connection and poller; live tables go static. Idempotent.

        Publishers are not failed, so live tables keep their rows and stop updating;
        `m.streams()` shows every stream `CLOSED` before the diagnostics tables are released.
        One WARNING is logged: "session closed; N live tables are now static". After
        `close()`, verbs and the diagnostics accessors raise `MassiveError`.

        A call on an update-graph thread raises `ValueError`.
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
