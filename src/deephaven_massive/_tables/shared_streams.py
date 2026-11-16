"""`SharedStreams`, a session's WebSocket streams: one per key, opened once, shared by every verb.

A stream's key is `(dataset, channel, symbols)`. Every verb of the session that asks for a key
gets a result over the one stream that serves it, with a hold of its own: the registry keeps
every stream from just before it opens until its holds are released and its blink table has
shut down, each with its `m.streams()` row (which carries its count of library-held results),
and the key index maps each key to the one opening or held, unfailed, live stream that serves
it. Every diagnostics row is queued while `registry_lock` is held, so rows reach the tables in
the order the registry changed.

The registry belongs to one `SessionCore`, which creates it and alone holds it; it refers to the
core weakly. Every entry point that can change what keeps the session alive (`result`,
`_release_hold`, `shut_down`) holds the core in a local variable until it returns and updates the
live set (`live_set.sync`) in the same `registry_lock` section as the change. The core reads and
changes the registry's rows through the by-id methods (`row`, `change_row`, `record_failure`,
`live_fails`, `close_rows`, and `in`), always under `registry_lock`, when a transport reports a row
change, a failure, or drift by stream id, and when the session closes.

Lock order: the update-graph (UG) lock, then `registry_lock`, then the live set's leaf lock. A
stream is registered under `registry_lock` before it opens (with an id from `stream_ids.py`),
then opened with no lock held; a second verb on the same key waits for that open on its own
thread, outside every lock, instead of opening a second stream. A second verb on a thread that
holds the UG lock cannot wait (the open may need that lock, to start a watchlist's listener),
so it takes the key over: it registers and opens a stream of its own, which later verbs on the
key reuse, while the first open finishes for its own caller alone.
"""

from __future__ import annotations

import logging
import threading
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final

from deephaven.dherror import DHError

from deephaven_massive._errors import (
    NEW_SESSION,
    MassiveError,
    SessionClosedError,
    copy_error,
    required,
)
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._massive import Transport
from deephaven_massive._tables import live_set
from deephaven_massive._tables.diagnostics import (
    FINAL_STATES,
    StreamKind,
    StreamRow,
    StreamState,
    describe_symbols,
    shown_query,
)
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.liveness import make_result, release_scope, weak_owner_callback
from deephaven_massive._tables.stream_ids import next_stream_id

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request, StreamKey
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import StreamHandle

__all__ = ["SharedStreams"]

logger = logging.getLogger(__name__)


def _shown_symbols(spec: DatasetSpec, symbols: NormalizedSymbols) -> tuple[int | None, str, str]:
    """Show a stream's symbols in its `m.streams()` row and in its open's log line.

    Args:
        spec: The stream's dataset.
        symbols: Its stream key's symbols.

    Returns:
        `(count, text, logged)`: the row's `SymbolCount` and `Symbols` (`describe_symbols`),
        and the log's words, the first symbols of a list (`bounded_list`) or the row's text.
        A stream whose rows name no symbol, whose key holds `ALL` only to subscribe every
        topic, shows none, as a read without symbols does: `(None, "", "none")`.
    """
    if not spec.names_symbols:
        return None, "", "none"
    count, text = describe_symbols(symbols)
    return count, text, bounded_list(symbols) if isinstance(symbols, tuple) else text


@dataclass(frozen=True, slots=True)
class _StreamEntry:
    """One stream of the registry; replaced, never mutated, under `registry_lock`.

    A record is registered before the transport opens the stream (`OPENING`, `handle` None),
    with one hold for the opener, so a failure or a shutdown reported while the transport is
    still opening finds it. It leaves the registry only once both its holds have reached zero
    and its blink table has shut down (or the open failed): Deephaven's shutdown callback and
    the library's holds run on different clocks, so either may come last. Until then its row
    keeps reporting row counts and a late failure, even after the library's last result was
    released while a derived table or an open panel keeps the publisher alive.

    Its key maps to it while it is opening (so a second verb on the key waits on `ready`
    instead of opening a second stream) and then while it is held, not failed, and not shut
    down; `_release_hold`, `record_failure`, and `shut_down` unmap it. A verb that holds the
    update graph's lock and finds the key's stream still opening takes the key over for a
    record of its own (`_claim`), so the first record then lives and ends unmapped.

    Attributes:
        key: The stream's key.
        handle: What the WebSocket transport returned; None while the stream is opening.
        row: The stream's current diagnostics row; `held_results` counts every hold, the
            opener's included.
        ready: Set once the open finished, whether it succeeded or not, and by `close()`.
        shut_down: True once the blink table has shut down while holds were outstanding.
        failure: The error the stream failed with, once it has failed; None before.
    """

    key: StreamKey
    handle: StreamHandle | None
    row: StreamRow
    ready: threading.Event
    shut_down: bool = False
    failure: MassiveError | None = None


class SharedStreams:
    """The stream registry of one session, and the verbs' way to a result over a shared stream.

    Created by `SessionCore.__init__`, which alone holds it, as `core.streams`. Thread-safe:
    its state is guarded by the core's `registry_lock`.
    """

    def __init__(self, core: SessionCore) -> None:
        """Create the empty registry of a core, which it refers to weakly.

        Args:
            core: The session's core, with its `registry_lock`, result registry, diagnostics,
                and drift registry already created.
        """
        self._core: Final = weakref.ref(core)
        self._lock: Final = core.registry_lock
        self._label: Final = core.label
        self._exec_ctx: Final = core.exec_ctx
        self._results: Final = core.results
        self._diagnostics: Final = core.diagnostics
        self._drift: Final = core.drift
        self._defer: Final = core.defer
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamEntry] = {}

    @property
    def active(self) -> bool:
        """True while any stream is registered, which keeps the session in the live set.

        Read with `registry_lock` held (`live_set.sync`).
        """
        return bool(self._streams)

    def __contains__(self, stream_id: object) -> bool:
        """Return whether a stream is registered; `registry_lock` is held.

        Args:
            stream_id: The stream id.

        Returns:
            True while the stream is in the registry.
        """
        return stream_id in self._streams

    def row(self, stream_id: int) -> StreamRow | None:
        """Return a registered stream's current row; `registry_lock` is held.

        Args:
            stream_id: The stream id.

        Returns:
            The row, or None when the id is not a registered stream.
        """
        state = self._streams.get(stream_id)
        return None if state is None else state.row

    def result(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        channel: str,
        query: str,
        verb: str,
        build: Callable[[StreamHandle], Table],
    ) -> Table:
        """Create one live result over the stream of `(spec, channel, symbols)`.

        The stream is reused when one with the same key is open. When another thread is opening it,
        this call waits for that open to finish (outside every lock) and then reuses the stream, or
        opens it itself if that open failed. Otherwise the WebSocket transport opens it here. The
        result is built from the stream's blink table in its own result scope on this thread, inside
        the session's execution context, and tracked so that garbage collection or `release()` frees
        it. If building or tracking the result raises anything, even `KeyboardInterrupt` (including
        one landing between `make_result` and `track`), the result scope, if it exists, and the
        stream hold are released before the exception propagates. Asynchronous exceptions such as
        `KeyboardInterrupt` are handled on a best-effort basis: every window between the library's
        own steps is covered, and the windows that remain are inside the cleanup code itself (an
        interrupt while a hold or a scope is being released).

        The stream subscribes before its first result exists, so a new stream holds the rows it
        receives meanwhile, and this call adds them right after it built the result
        (`StreamHandle.publish_held`, also when the build failed, for a result another verb
        built on the stream meanwhile): an aggregation such as `last_by` sees only rows that
        reach the blink table after it exists, and these reach it in a later update cycle. A
        reused stream holds nothing; its new result starts from the rows that arrive next.

        A stream that failed while it was opening, or between the claim and the build, serves no
        result: every shape, `blink()` included, raises `MassiveError` naming the stream's
        failure, with the hold released. A failure that lands while the shape is being derived
        makes Deephaven raise `DHError` ("Can not listen to failed table"), or jpy's
        `RuntimeError` from the described copy; that error is wrapped in the same
        `MassiveError`. Either way `m.streams()` shows `FAILED`. When the session has closed
        by the time such a Deephaven error reaches this call, the error is wrapped in the
        closed session's `MassiveError` (`"MassiveSession #1 is closed; ..."`) instead, whether or
        not the stream's row records the close yet. A session that closes while the result is
        built is checked once more after the result is tracked: the result is released and the
        closed session's `MassiveError` raised, so a close that started before that check never
        leaves the verb returning a table about to fail; a close that starts after it fails the
        returned table like any other.

        The result record's owner callback holds the registry weakly (`weak_owner_callback`);
        the stream's record, and through it the live set, keeps the session alive while the
        stream runs.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            query: The `repr` of the query whose verb asks, for the `Query` column of
                `m.streams()`; a reused stream keeps the text of the query that opened it.
            verb: The verb that asks, such as `"last"`, for the log line of the open.
            build: Turns the stream (its handle's blink table, host, and recency) into the
                result. It must create a new
                table, never return the blink table itself, so the result scope manages the
                result and each call returns a distinct table.

        Returns:
            The live result, a distinct Python object per call.

        Raises:
            MassiveError: If the stream has failed (`"stocks.trades stream 7 failed: ..."`).
            SessionClosedError: If the session closed while the result was built
                (`"MassiveSession #1 is closed; ..."`).
        """
        core = required(self._core(), "the session")  # held until the call returns: live_set.py
        key: StreamKey = (spec.name, channel, request.symbol_key(), request.on_error)
        scope: LivenessScope | None = None
        handle, opened = self._acquire(
            core, spec, request, key=key, channel=channel, query=query, verb=verb
        )
        try:
            # Another verb may hold the stream already, so its held rows are added whatever
            # happens from here on, an interrupt included; otherwise they would pile up forever
            # and that verb's result would show none of them.
            try:
                with entered(self._exec_ctx):
                    self._require_unfailed(spec, handle.stream_id)
                    result, scope = make_result(self._exec_ctx, lambda: build(handle))
            finally:
                if opened:  # the rows received while the stream opened, after the result
                    with entered(self._exec_ctx):
                        handle.publish_held()
            self._results.track(
                result,
                scope,
                weak_owner_callback(self, SharedStreams._release_hold, handle.stream_id),
            )
        except BaseException as error:  # nothing was tracked: the scope and the hold are ours
            # Read first: releasing the last hold of a shut-down stream deletes its record.
            # A failed table fails a shape with `DHError`, or with jpy's `RuntimeError` when the
            # Java call is direct (the described copy every result ends with).
            deephaven = isinstance(error, DHError | RuntimeError)
            failure = self._failure(handle.stream_id) if deephaven else None
            try:
                if scope is not None:
                    release_scope(self._exec_ctx, scope)
            finally:
                self._release_hold(handle.stream_id)
            # The close failed the stream's tables: its row may not read `CLOSED` yet, or may
            # be gone already, so the close is named whether or not a failure was recorded.
            if deephaven and core.closed:
                raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}") from error
            if failure is not None:
                named = copy_error(
                    failure, f"{spec.name} stream {handle.stream_id} failed: {failure}"
                )
                raise named from error
            raise
        if core.closed:  # close() ran while the result was built; it is released on the reaper
            self._results.release(result)
            raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}")
        return result

    def _require_unfailed(self, spec: DatasetSpec, stream_id: int) -> None:
        """Refuse to build a result over a stream that has failed.

        Args:
            spec: The stream's dataset, named in the message.
            stream_id: The stream.

        Raises:
            MassiveError: If the stream has failed: a copy of its failure, of the same class and
                attributes (`copy_error`), whose message names the failure.
        """
        failure = self._failure(stream_id)
        if failure is not None:
            named = copy_error(failure, f"{spec.name} stream {stream_id} failed: {failure}")
            raise named

    def _failure(self, stream_id: int) -> MassiveError | None:
        """Return the error a stream failed with, if it has failed and is still in the registry.

        Args:
            stream_id: The stream.

        Returns:
            The error it failed with when its state is `FAILED`; a `SessionClosedError` with the
            close's text when it is `CLOSED`; None otherwise.
        """
        with self._lock:
            state = self._streams.get(stream_id)
            if state is None or state.row.state not in {StreamState.FAILED, StreamState.CLOSED}:
                return None
            if state.row.state is StreamState.CLOSED:
                return SessionClosedError(
                    state.row.error or f"{self._label} is closed; {NEW_SESSION}"
                )
            return state.failure

    def _acquire(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        key: StreamKey,
        channel: str,
        query: str,
        verb: str,
    ) -> tuple[StreamHandle, bool]:
        """Take a hold on the stream of `key`: reuse it, wait for its open, or open it.

        Each attempt (`_acquire_once`) reuses the stream, opens it, or finds another thread's
        open in progress and waits for it, outside every lock, before the next attempt. The
        handle an attempt returns reaches the caller with no checkpoint between the attempt's
        `try` and the caller's own cleanup, so an interrupt never leaks the hold it took.

        A caller that holds the update graph's lock never waits for another thread's open, which
        may need that lock to finish (starting a watchlist's listener does): it takes the key
        over and opens a stream of its own (`_claim`). The lock is checked once, here, outside
        `registry_lock`, since the check is a Java call.

        Args:
            core: The session's core, which the caller holds.
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            channel: The WebSocket channel.
            query: The `repr` of the asking query, for a new stream's `Query` column.
            verb: The asking verb, such as `"last"`.

        Returns:
            The stream's handle, with one hold taken for the caller, and whether this call
            opened the stream (so the caller must call its `publish_held` once it has built its
            result).

        A closed session, or one that closes while the stream opens, raises the `MassiveError`
        of `_claim` or `_open`.
        """
        may_wait = not holds_update_graph_lock(self._exec_ctx)
        while True:
            acquired = self._acquire_once(
                core,
                spec,
                request,
                key=key,
                channel=channel,
                query=query,
                verb=verb,
                may_wait=may_wait,
            )
            if acquired is not None:
                return acquired  # no checkpoint between the try and the caller's own cleanup

    def _acquire_once(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        key: StreamKey,
        channel: str,
        query: str,
        verb: str,
        may_wait: bool,
    ) -> tuple[StreamHandle, bool] | None:
        """Make one attempt to take a hold on the stream of `key` (`_acquire`).

        Every hold `_claim` takes is reported in `holds` right after it is stored, and the claim,
        the open, and the wake-up of the waiters run inside one `try`/`except`; the handle is
        returned after it, with no checkpoint between the `try` and the caller's own cleanup. So
        whatever interrupts them, even a `KeyboardInterrupt` landing between two calls, the cleanup
        is exact: each hold this call took is released (a reused stream's extra hold; or the
        opener's hold, which deletes a never-opened record and unmaps its key, or releases an opened
        stream's scope), and a new record's `ready` event is set so waiting verbs wake. Waiting
        happens outside every lock. Asynchronous exceptions such as `KeyboardInterrupt` are handled
        on a best-effort basis: every window between the library's own steps is covered, and the
        windows that remain are inside the cleanup code itself (an interrupt while a hold or a scope
        is being released).

        Args:
            core: The session's core, which the caller holds.
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            channel: The WebSocket channel.
            query: The `repr` of the asking query, for a new stream's `Query` column.
            verb: The asking verb, such as `"last"`.
            may_wait: False when the caller holds the update graph's lock, so it may not wait
                for another thread's open.

        Logs at DEBUG, with the session's label, the stream id, the query (cut to
        `QUERY_SHOWN` characters, as `shown_query` cuts it) and verb, and the
        channel, that the stream opened, with its host and path, why that host
        (`HostDecision.evidence`), and its symbols (the count and the first twenty); or that it
        was reused.

        Returns:
            The stream's handle, with one hold taken for the caller, and whether this call
            opened the stream; None after waiting for another thread's open, to try again.

        A closed session, or one that closes while the stream opens, raises the `MassiveError`
        of `_claim` or `_open`.
        """
        holds: list[tuple[int, threading.Event | None]] = []
        opened_handle: StreamHandle | None = None
        try:
            claim = self._claim(
                core, spec, request, key=key, query=query, may_wait=may_wait, holds=holds
            )
            if isinstance(claim, threading.Event):
                claim.wait()  # another thread is opening the stream; never under a lock
                return None
            opened = isinstance(claim, int)
            if isinstance(claim, int):
                claim = opened_handle = self._open(
                    core, spec, request, channel=channel, stream_id=claim
                )
                shown = _shown_symbols(spec, request.symbol_key())[2]
                logger.debug(
                    "%s: stream %d opened: %s.%s -> channel %s on %s%s (%s); symbols %s",
                    self._label,
                    claim.stream_id,
                    shown_query(query),
                    verb,
                    channel,
                    claim.host,
                    claim.path,
                    claim.evidence,
                    shown,
                )
            else:
                logger.debug(
                    "%s: stream %d reused: %s.%s -> channel %s",
                    self._label,
                    claim.stream_id,
                    shown_query(query),
                    verb,
                    channel,
                )
            for _, ready in holds:  # inside the try: an interrupt here still cleans up
                if ready is not None:
                    ready.set()
        except BaseException:  # the hold did not reach the caller
            try:
                # The stream opened, and another verb may reuse it already: add the rows it
                # held, or they pile up forever and that verb's result shows none of them.
                if opened_handle is not None:
                    with entered(self._exec_ctx):
                        opened_handle.publish_held()
            finally:
                for stream_id, ready in holds:
                    try:
                        self._release_hold(stream_id)
                    finally:
                        if ready is not None:
                            ready.set()  # waiters must wake whatever happens
            raise
        return claim, opened  # no checkpoint between the try and the caller's own cleanup

    def _claim(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        key: StreamKey,
        query: str,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        """Take a hold on the stream with `key`, find its open in progress, or register it.

        One critical section under `registry_lock`; it never waits and makes no Deephaven
        call.

        A caller that may not wait, because it holds the update graph's lock, takes over the key
        of a stream still opening on another thread: it registers a new record and maps the key
        to it. The first stream finishes opening for its own caller and is never reused, and
        the verbs waiting for it wake, claim again, and find the new record. Every unmapping
        checks that the key still maps to the stream being unmapped, so the first stream's end
        leaves the key alone. A take-over is logged at INFO, after the lock is released, so
        the two rows of the key in `m.streams()` are explained.

        Args:
            core: The session's core, which the caller holds.
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            query: The `repr` of the asking query, the `Query` of a new record's row.
            may_wait: False when the caller holds the update graph's lock, so it may not wait
                for another thread's open.
            holds: Receives one `(stream_id, ready)` entry per hold this call takes, appended
                inside the lock right after the registry stores it, so the caller cleans up
                exactly what was taken: `(stream_id, None)` for a hold on an open stream it
                reuses, `(stream_id, ready)` for a new `OPENING` record and the opener's hold.
                An interrupt before the stores leaves nothing to clean up.

        Returns:
            The handle of an open stream, with one more hold taken on it; the `ready` event of
            a stream another thread is opening, to wait on before claiming again (only when
            `may_wait`); or the id of a new `OPENING` record registered for this caller, with
            the opener's hold, which the caller must pass to `_open`.

        Raises:
            SessionClosedError: If the session is closed (a verb that passed the guard just before
                `close()` ran, or a waiter woken by `close()`); a closed stream is never reused.
        """
        with self._lock:
            if core.closed:
                raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}")
            existing = self._stream_ids.get(key)
            if existing is not None:
                state = self._streams[existing]
                if state.handle is not None:
                    row = replace(state.row, held_results=state.row.held_results + 1)
                    self._streams[existing] = replace(state, row=row)
                    holds.append((existing, None))  # right after the store: the caller cleans up
                    self._diagnostics.write(row)
                    return state.handle
                if may_wait:
                    return state.ready
                # Opening on another thread, which may need the UG lock this caller holds to
                # finish: take the key over below rather than wait.
            stream_id = next_stream_id()
            count, text, _ = _shown_symbols(spec, request.symbol_key())
            row = StreamRow(
                stream_id=stream_id,
                kind=StreamKind.STREAM,
                dataset=spec.name,
                query=shown_query(query),
                symbol_count=count,
                symbols=text,
                held_results=1,
                transport=Transport.WS,
                created_at=time.time_ns(),
            )
            ready = threading.Event()
            state = _StreamEntry(key, None, row, ready)
            # The key first: hashing it can run Python (a SymbolTable box), so it may be
            # interrupted; the int-keyed store and the append after it run no Python code.
            # No row is written until the open finishes: m.streams() has no OPENING state.
            self._stream_ids[key] = stream_id
            self._streams[stream_id] = state
            holds.append((stream_id, ready))  # right after the stores: the caller cleans up
            live_set.sync(core)
        if existing is not None:  # the key was taken over from a stream still opening
            logger.info(
                "%s: %s stream %d opens alongside stream %d for %s, which another thread is "
                "still opening: this call holds the update graph's lock (a console command) "
                "and cannot wait for that open, so m.streams() shows both; later calls reuse "
                "stream %d",
                self._label,
                spec.name,
                stream_id,
                existing,
                text,
                stream_id,
            )
        return stream_id

    def _open(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        channel: str,
        stream_id: int,
    ) -> StreamHandle:
        """Open the stream of a registered `OPENING` record through the WebSocket transport.

        Runs with no lock held while the transport works. The caller (`_acquire_once`) sets the
        record's `ready` event afterwards and, if this raises anything, even
        `KeyboardInterrupt`, drops the opener's hold, which deletes a never-opened record and
        unmaps its key; a waiting verb then opens the stream itself. If the interrupt lands
        after the transport returned the stream but before its handle is stored in the
        registry, nothing else knows the stream, so this method releases its scope itself. A
        failure or a shutdown the transport reported during the open has already been
        recorded on the record, and the stream is returned as is: `result` then raises
        the `MassiveError` naming the failure instead of building a result. A reactive stream
        that shut down during the open has its watchlist listener stopped on the reaper here,
        since the shutdown had no handle to stop it through. The row takes the host, path,
        and recency the stream opened on.

        Args:
            core: The session's core, which the caller holds; its `stream_transport` opens
                the stream.
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            stream_id: The id `_claim` registered.

        Returns:
            The opened stream, with the opener's hold.

        Raises:
            SessionClosedError: If the session was closed while the transport opened the
                stream; the caller then releases the opener's hold, which releases the new
                stream's scope.
        """
        opened: StreamHandle | None = None
        stored = False
        try:
            with entered(self._exec_ctx):
                opened = core.stream_transport().open_stream(
                    spec, request, channel, stream_id=stream_id
                )
            with self._lock:
                closed = core.closed  # before the store, which `stored` must follow at once
                state = self._streams[stream_id]
                row = replace(state.row, host=opened.host, path=opened.path, recency=opened.recency)
                self._streams[stream_id] = replace(state, handle=opened, row=row)
                stored = True  # from here on, releasing the opener's hold releases the stream
                self._diagnostics.write(row)
                if state.shut_down and opened.stop_watch is not None:
                    # The shutdown found no handle to stop the watchlist listener through.
                    self._defer(opened.stop_watch)  # a Deephaven call: never here
        except BaseException:
            if opened is not None and not stored:  # the stream never reached the registry
                release_scope(self._exec_ctx, opened.scope)
            raise
        if closed:
            logger.debug("%s: releasing a stream opened during close()", self._label)
            raise SessionClosedError(
                f"{self._label} was closed while a stream was opening; {NEW_SESSION}"
            )
        return opened

    def _release_hold(self, stream_id: int) -> None:
        """Drop one hold on a stream; at zero, unmap its key and release its stream scope.

        Runs on the reaper thread when a result is released, or on the caller thread when
        building a result or opening the stream failed. At zero holds a running stream's row
        becomes `RELEASED` (a failed or closed one keeps its state). The record stays in the
        registry until the blink table shuts down; if it already has (`shut_down` ran
        while holds were outstanding) or the stream never opened, the record is deleted now,
        and the drift registry forgets the stream. Releasing the stream scope drops the
        library's reference to the blink table; the table dies, and its shutdown callback
        releases the topics, when nothing else (a derived table, an open panel) refers to it.
        Releasing the scope of a stream that has already shut down is a harmless decrement. The
        scope is released after `registry_lock` is let go, under the update graph's shared lock
        (`release_scope`).

        Args:
            stream_id: The stream whose hold is dropped.
        """
        with self._lock:
            core = required(self._core(), "the session")  # held until the call returns: live_set.py
            state = self._streams[stream_id]
            held = state.row.held_results - 1
            opened = state.handle is not None
            released = not held and opened and state.row.state is StreamState.RUNNING
            row = replace(
                state.row,
                held_results=held,
                state=StreamState.RELEASED if released else state.row.state,
            )
            if not held and self._stream_ids.get(state.key) == stream_id:
                # No new verb may reuse a stream the library no longer holds. The key may
                # already serve a newer stream (after `record_failure`); then it stays.
                del self._stream_ids[state.key]
            if not held and (state.shut_down or not opened):
                del self._streams[stream_id]
                self._drift.forget(stream_id)
            else:
                self._streams[stream_id] = replace(state, row=row)
            if opened:
                self._diagnostics.write(row)
            live_set.sync(core)
        logger.debug(
            "stream %d: a hold released, %d left%s",
            stream_id,
            held,
            "; its stream scope is released" if not held and opened else "",
        )
        if not held and state.handle is not None:
            release_scope(self._exec_ctx, state.handle.scope)

    def shut_down(self, stream_id: int) -> None:
        """Record that a stream's blink table has shut down.

        The WebSocket transport calls this first from the blink table's `on_shutdown_callback`,
        before it releases the stream's topics. That callback runs on whichever thread drops
        the last reference to the table: the reaper, an update-graph thread (`publish_failure`
        shuts the table down on the next cycle), a gRPC thread, or Deephaven's liveness-cleanup
        thread. The stream's key is unmapped, so no new verb reuses the dead table, and a
        reactive stream's watchlist listener is stopped on the reaper. With no holds left the
        record is deleted and the drift registry forgets the stream; with holds outstanding (a
        failure shut the table down while results are still held) the record is marked shut
        down and deleted when the last hold is released. It takes only `registry_lock` (and the
        leaf locks of the drift registry and the live set), makes no Deephaven call, and writes
        no row: the stream's last row stays in `m.streams()`. An unknown id is ignored.

        Args:
            stream_id: The stream whose blink table shut down.
        """
        with self._lock:
            core = required(self._core(), "the session")  # held until the call returns: live_set.py
            state = self._streams.get(stream_id)
            if state is None:
                return
            if self._stream_ids.get(state.key) == stream_id:
                del self._stream_ids[state.key]
            handle = state.handle
            if handle is not None and handle.stop_watch is not None:
                self._defer(handle.stop_watch)  # a Deephaven call: never here
            if state.row.held_results:
                self._streams[stream_id] = replace(state, shut_down=True)
            else:
                del self._streams[stream_id]
                self._drift.forget(stream_id)
            live_set.sync(core)

    # --- By-id reports and the close: the core calls these with `registry_lock` held. -------

    def change_row(self, stream_id: int, change: Callable[[StreamRow], StreamRow]) -> bool:
        """Change a registered stream's row, and write it once the stream has opened.

        `registry_lock` is held. An opening stream's row is first written when its open
        returns (`_open`), so an open that raises leaves no row behind.

        Args:
            stream_id: The stream id.
            change: Returns the new row from the current one; called once.

        Returns:
            True if the id is a registered stream.
        """
        state = self._streams.get(stream_id)
        if state is None:
            return False
        row = change(state.row)
        self._streams[stream_id] = replace(state, row=row)
        if state.handle is not None:  # an opening stream's row is written by _open
            self._diagnostics.write(row)
        return True

    def record_failure(self, stream_id: int, error: MassiveError) -> bool:
        """Mark a registered stream `FAILED` and stop reusing it; `registry_lock` is held.

        Its key is unmapped (when it still maps to this stream), so the next verb with the
        same key opens a fresh stream. The row is written once the stream has opened; an
        opening stream's `FAILED` row is written when its open returns. The caller
        (`SessionCore.fail_stream`) has checked that the row is not `CLOSED`.

        Args:
            stream_id: The stream id.
            error: The failure; its text fills the `Error` column, and a later verb on the
                stream raises a copy of it (`_failure`).

        Returns:
            True if the id is a registered stream.
        """
        state = self._streams.get(stream_id)
        if state is None:
            return False
        row = replace(state.row, state=StreamState.FAILED, error=str(error))
        self._streams[stream_id] = replace(state, row=row, failure=error)
        if self._stream_ids.get(state.key) == stream_id:
            del self._stream_ids[state.key]
        if state.handle is not None:  # an opening stream's row is written by _open
            self._diagnostics.write(row)
        return True

    def live_fails(self) -> list[tuple[int, Callable[[MassiveError], bool]]]:
        """Return the stream id and fail hook of every running stream; `registry_lock` is held.

        Returns:
            `(stream id, hook)` of each opened stream whose table has not shut down and has
            not failed or closed (`FINAL_STATES`), in registration order.
        """
        return [
            (stream_id, state.handle.fail)
            for stream_id, state in self._streams.items()
            if state.handle is not None
            and not state.shut_down
            and state.row.state not in FINAL_STATES
        ]

    def close_rows(self, close_row: Callable[[StreamRow], StreamRow]) -> list[threading.Event]:
        """Mark every registered stream's row closed, for `close()`; `registry_lock` is held.

        Each row is replaced by `close_row`'s and written, in registration order, except an
        opening stream's, which has no row yet: its open writes it when it returns.

        Args:
            close_row: Returns a stream's closed row from its current one.

        Returns:
            The `ready` events of the streams still opening, which the caller sets once it
            has let go of `registry_lock`, so the verbs waiting for them wake and raise.
        """
        opening = []
        for stream_id, state in tuple(self._streams.items()):
            if state.handle is None:
                opening.append(state.ready)
            closed = close_row(state.row)
            self._streams[stream_id] = replace(state, row=closed)
            if state.handle is not None:  # an opening stream has no row yet
                self._diagnostics.write(closed)
        return opening
