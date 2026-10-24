"""`MassiveSession`, the user's entry point, and `SessionCore`, its internal engine.

`MassiveSession` is what users create: it exposes the dataset namespaces (`m.stocks`,
`m.options`, ...), the diagnostics tables, and the lifecycle verbs `release()` and `close()`
(DESIGN § 6), and nothing else. It delegates everything stateful to a `SessionCore`, which
queries and namespaces share. The core owns:

- the session's label (`MassiveSession #1`, a per-process counter), used in its `repr`, the
  same-key WARNING, and error messages;
- the configuration and the captured Deephaven `ExecutionContext`, entered on every library
  thread;
- `registry_lock`, the one leaf lock that guards every session registry (DESIGN § 18): no
  Deephaven call, network call, or blocking wait happens while it is held;
- the stream registry: every stream the library still holds, one per `(dataset, channel,
  symbols)`, each with its diagnostics row (which carries its count of library-held results);
  every diagnostics row is queued while `registry_lock` is held, so rows reach the tables in
  the order the registry changed;
- the result registry and the reaper thread (`_liveness.py`);
- the diagnostics tables and their writer thread (`_diagnostics.py`);
- one factory hook per transport (`rest_transport`, `stream_transport`,
  `flat_file_transport`), which the transport work packages fill in. Until then each raises
  `NotImplementedError`, so the verbs that need it do too.

Lock order: the update-graph (UG) lock, then `registry_lock`. Stream creation takes no lock
while the transport works: it opens optimistically and registers under `registry_lock`; if
another thread registered the same key meanwhile, the new stream is released and the
registered one is used (a rare, harmless, wasted publisher).
"""

from __future__ import annotations

import functools
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
from deephaven_massive._enums import Transport
from deephaven_massive._errors import MassiveError
from deephaven_massive._liveness import Reaper, ResultRegistry, entered, make_result
from deephaven_massive._types import ALL, override

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

# A thread with no execution context gets Deephaven's empty default context, whose update graph
# is a PoisonedUpdateGraph: creating any refreshing table under it fails. That class is the
# only signal Deephaven offers for "no context is active".
_POISONED_UPDATE_GRAPH: Final = "PoisonedUpdateGraph"

_keys_lock = threading.Lock()
_session_numbers = itertools.count(1)
_open_sessions: weakref.WeakSet[SessionCore] = weakref.WeakSet()


def _has_execution_context(exec_ctx: ExecutionContext) -> bool:
    """Tell whether an execution context is a real one, not the thread's empty default.

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


_ON_UPDATE_GRAPH: Final = (
    "deephaven_massive calls must not run on a Deephaven update-graph thread "
    "(for example inside a table listener) or while holding the update-graph lock"
)
"""The `ValueError` text for a call on an update-graph thread: verbs create tables and take the
registry lock, which would deadlock against the update cycle there (DESIGN § 18.3)."""


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
        others = sorted(
            other.label
            for other in _open_sessions
            if other.config.api_key == core.config.api_key and not other.closed
        )
        _open_sessions.add(core)
    if others:
        logger.warning(
            "%s uses the same API key as %s; individual plans allow one WebSocket connection "
            "per cluster per account, so the sessions will compete for connection slots",
            core.label,
            others[0],
        )


def _stop_threads(reaper: Reaper, diagnostics: Diagnostics) -> None:
    """Finalizer of a collected session: end its reaper and diagnostics writer, without waiting.

    Args:
        reaper: The session's reaper.
        diagnostics: The session's diagnostics.
    """
    reaper.stop()
    diagnostics.stop()


@dataclass(frozen=True, slots=True)
class _StreamState:
    """One stream the library holds; replaced, never mutated, under `registry_lock`.

    A stream stays in the registry while it has at least one library-held result
    (`row.held_results`) and is removed when the last one is released.

    Attributes:
        key: The stream's key.
        handle: What the WebSocket transport returned.
        row: The stream's current diagnostics row, including its held-result count.
    """

    key: StreamKey
    handle: StreamHandle
    row: StreamRow


class SessionCore:
    """The internal engine of one `MassiveSession`, shared by its namespaces and queries.

    Created by `MassiveSession.__init__` on the caller thread, with an execution context
    active. Thread-safe: every mutable registry is guarded by `registry_lock`.
    """

    def __init__(self, config: MassiveConfig, exec_ctx: ExecutionContext, label: str) -> None:
        """Create the core, its reaper thread, and the diagnostics tables and writer.

        Args:
            config: The validated configuration.
            exec_ctx: The caller's execution context, captured for library threads.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self.config: Final = config
        self.exec_ctx: Final = exec_ctx
        self.label: Final = label
        self.registry_lock: Final = threading.RLock()
        # The diagnostics come first: creating their tables is the step that can fail, and
        # nothing has started a thread yet if it does.
        self.diagnostics: Final = Diagnostics(exec_ctx, f"{label} diagnostics")
        self._reaper = Reaper(exec_ctx, f"{label} reaper")
        self.results: Final = ResultRegistry(self.registry_lock, self._reaper)
        weakref.finalize(self, _stop_threads, self._reaper, self.diagnostics)
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamState] = {}
        self._closers: list[Callable[[], None]] = []
        self._closed = False

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        with self.registry_lock:
            return self._closed

    def require_open(self) -> None:
        """Reject a call on a closed session.

        Raises:
            MassiveError: `MassiveError("MassiveSession #1 is closed")` after `close()`.
        """
        if self.closed:
            raise MassiveError(f"{self.label} is closed")

    def require_verb_thread(self) -> None:
        """The guard every verb runs first: this is not a UG thread, and the session is open.

        The thread is checked first, so a verb on an update-graph thread raises `ValueError`
        whether or not the session is closed (DESIGN § 6).

        Raises:
            ValueError: On an update-graph thread, or while holding the update-graph lock.
            MassiveError: If the session is closed.
        """
        if _on_update_graph(self.exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
        if self.closed:
            raise MassiveError(f"{self.label} is closed")

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

    # --- Streams --------------------------------------------------------------------------

    def on_close(self, closer: Callable[[], None]) -> None:
        """Register a transport's cleanup (close connections, stop pollers) for `close()`.

        If the session is already closed (a transport that passed `require_open` just before
        `close()` ran), the closer runs at once, on this thread and outside the lock, so it is
        still cleaned up. Either way it runs exactly once.

        Args:
            closer: Called once, without any lock held: on the thread that closes the session,
                or here when the session is already closed. An exception it raises during
                `close()` is logged at ERROR and the remaining closers still run; raised here,
                it propagates to the caller.
        """
        with self.registry_lock:
            closed = self._closed
            if not closed:
                self._closers.append(closer)
        if closed:
            closer()

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
        transport opens it. The result is built from the stream's blink table in its own result
        scope on this thread, inside the session's execution context, and tracked so that
        garbage collection or `release()` frees it. If the session closes while the stream is
        opening, the call raises `MassiveError`.

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
        handle = self._acquire(key) or self._open(spec, request, channel=channel, key=key)
        blink = handle.blink
        try:
            with entered(self.exec_ctx):
                result, scope = make_result(lambda: build(blink), wrap=wrap)
        except Exception:
            self._release_hold(handle.stream_id)
            raise
        self.results.track(result, scope, functools.partial(self._release_hold, handle.stream_id))
        return result

    def _hold(self, stream_id: int) -> tuple[StreamHandle, StreamRow]:
        """Take one more hold on a registered stream. Call with `registry_lock` held.

        Args:
            stream_id: An open stream.

        Returns:
            The stream's handle and its updated row.
        """
        state = self._streams[stream_id]
        row = replace(state.row, held_results=state.row.held_results + 1)
        self._streams[stream_id] = replace(state, row=row)
        return state.handle, row

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
            handle, row = self._hold(stream_id)
            self.diagnostics.write(row)
        return handle

    def _open(
        self, spec: DatasetSpec, request: Request, *, channel: str | None, key: StreamKey
    ) -> StreamHandle:
        """Open a new stream through the WebSocket transport and register it with one hold.

        Runs with no lock held while the transport works. If another thread registered the
        same key in the meantime, the new stream's scope is released (its publisher is wasted;
        this is rare) and a hold is taken on the registered stream instead.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            key: The stream key.

        Returns:
            The handle of the stream that now serves `key`.

        Raises:
            MassiveError: If the session was closed while the transport opened the stream; the
                new stream's scope is released.
        """
        with entered(self.exec_ctx):
            opened = self.stream_transport().open_stream(spec, request, channel)
        count, text = describe_symbols(request.symbol_key())
        row = StreamRow(
            stream_id=opened.stream_id,
            dataset=spec.name,
            symbol_count=count,
            symbols=text,
            held_results=1,
            transport=Transport.WS,
            host=opened.host,
            path=opened.path,
            created_at=time.time_ns(),
        )
        with self.registry_lock:
            closed = self._closed
            existing = self._stream_ids.get(key)
            if closed:
                handle = opened
            elif existing is not None:
                handle, row = self._hold(existing)
                self.diagnostics.write(row)
            else:
                handle = opened
                self._stream_ids[key] = opened.stream_id
                self._streams[opened.stream_id] = _StreamState(key, opened, row)
                self.diagnostics.write(row)
        if closed or existing is not None:
            logger.debug("%s: releasing an unused stream for %s", self.label, spec.name)
            with entered(self.exec_ctx):
                opened.scope.release()
        if closed:
            raise MassiveError(f"{self.label} was closed while a stream was opening")
        return handle

    def _release_hold(self, stream_id: int) -> None:
        """Drop one hold on a stream; at zero, unregister its key and release its stream scope.

        Runs on the reaper thread when a result is released, or on the caller thread when
        building a result failed. Releasing the stream scope drops the library's reference to
        the blink table; the table dies, and its shutdown callback releases the topics, when
        nothing else (a derived table, an open panel) refers to it.

        Args:
            stream_id: The stream whose result was released.
        """
        with self.registry_lock:
            state = self._streams[stream_id]
            held = state.row.held_results - 1
            released = not held and state.row.state is StreamState.RUNNING
            row = replace(
                state.row,
                held_results=held,
                state=StreamState.RELEASED if released else state.row.state,
            )
            if held:
                self._streams[stream_id] = replace(state, row=row)
            else:
                # The library no longer holds the stream: forget it, and its key unless the key
                # already serves a newer stream (after `fail_stream`).
                del self._streams[stream_id]
                if self._stream_ids.get(state.key) == stream_id:
                    del self._stream_ids[state.key]
            self.diagnostics.write(row)
        if not held:
            with entered(self.exec_ctx):
                state.handle.scope.release()

    def fail_stream(self, stream_id: int, error: str) -> None:
        """Record that a stream failed and stop reusing it for new verbs.

        Called by the transport after it fails the stream's table (`publish_failure`). The
        stream's row becomes `FAILED` with `error`, and its key is unregistered, so the next
        verb with the same key opens a fresh stream instead of returning the dead table. The
        failed stream stays in the registry until its results are released.

        A stream whose last library-held result was already released has left the registry
        (and its key), though a derived table or an open panel may still keep its publisher
        alive until the blink table shuts down. A failure reported for such a stream needs no
        bookkeeping: the transport's `publish_failure` already fails the live table, and no
        new verb can reuse the stream. The call then does nothing.

        Args:
            stream_id: The failed stream.
            error: The failure text for the `Error` column.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None:
                return
            row = replace(state.row, state=StreamState.FAILED, error=error)
            self._streams[stream_id] = replace(state, row=row)
            if self._stream_ids.get(state.key) == stream_id:
                del self._stream_ids[state.key]
            self.diagnostics.write(row)

    def close(self) -> None:
        """Close the session: run the closers, mark streams closed, release the diagnostics.

        Idempotent. Publishers are not failed: live tables stop receiving rows and go static,
        and keep their contents. Results stay tracked, so garbage collection and `release()`
        still free them. Every stream the library still holds is marked `CLOSED` in
        `m.streams()`. Logs the number of live tables that became static: a WARNING when there
        are any, INFO otherwise.

        Raises:
            ValueError: On an update-graph thread, even when the session is already closed.
        """
        if _on_update_graph(self.exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
        with self.registry_lock:
            if self._closed:
                return
            self._closed = True
            closers = tuple(self._closers)
            closed = {
                stream_id: replace(state, row=replace(state.row, state=StreamState.CLOSED))
                for stream_id, state in self._streams.items()
            }
            self._streams.update(closed)
            for state in closed.values():
                self.diagnostics.write(state.row)
        for closer in closers:
            try:
                closer()
            except Exception:
                logger.exception("%s: a transport failed to close", self.label)
        self.diagnostics.release()
        live = self.results.held()
        level = logging.WARNING if live else logging.INFO
        logger.log(level, "session closed; %d live tables are now static", live)


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
            trades = m.stocks.trades(["AAPL", "MSFT"]).ring(10_000)
            conns = m.connections()
            m.release(trades)
            m  # <MassiveSession #1 open>

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

        When `config` is None, `MassiveConfig()` reads the environment and raises its own
        `AuthenticationError` if no API key is configured (it is not repeated under Raises
        because this method does not raise it itself).

        Args:
            config: The configuration; None means `MassiveConfig()`, which reads the API key
                and S3 credentials from the environment.

        Raises:
            TypeError: If `config` is not a `MassiveConfig`.
            MassiveError: If no Deephaven `ExecutionContext` is active on this thread: the
                session must be created inside a Deephaven worker.
            ValueError: If called on an update-graph thread or under the update-graph lock.
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
        if _on_update_graph(exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
        log_json_codec_once()
        core = SessionCore(config, exec_ctx, _next_label())
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
        last error. The table is the session's; do not edit it. After `close()` this raises
        `MassiveError` (see `SessionCore.require_open`).

        Returns:
            The keyed `connections` table (columns in DESIGN § 17).
        """
        self._core.require_open()
        return self._core.diagnostics.connections

    def streams(self) -> Table:
        """Return the live diagnostics table of streams and `poll()` tables.

        One row per stream: dataset, symbols, `HeldResults`, deferred option contracts,
        host, rows published, requests per poll cycle, state, and error. After `close()` this
        raises `MassiveError`.

        Returns:
            The keyed `streams` table (columns in DESIGN § 17).
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
        and `release()` keep working.

        Raises:
            ValueError: On an update-graph thread, even when the session is already closed.
        """
        if _on_update_graph(self._core.exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
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
