"""Reactive Deephaven Table watchlist listener driving dynamic WebSocket subscriptions."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
import threading
import time
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    UTC_NS_TYPE,
    arrow_to_deephaven_table,
    exec_ctx_scope,
)

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table
    from deephaven_massive.engine.publisher import MicroBatchPublisher
    from deephaven_massive.engine.symbols import SymbolNormalizer
    from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer

_ACTIVE_SUBS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("Symbol", pa.string()),
        pa.field("SubscribedAt", UTC_NS_TYPE),
    ]
)

_ACTIVE_SUBS_KEY_SCHEMA = pa.schema(
    [
        pa.field("Symbol", pa.string()),
    ]
)


class ReactiveWatchlistController:
    """Bridges a live Deephaven `Table` watchlist to WebSocket subscriptions and `.latest()` / `.history()` eviction.

    Guarantees:
    - Lock-free listener callback: Deephaven `table_listener.listen` callbacks never block the UpdateGraph thread;
      diffs are dispatched via `ws_multiplexer.call_soon_threadsafe`.
    - Strict subscribe ordering: `(Symbol, SubscribedAt)` is added to `active_subs` BEFORE the symbol is enabled
      in `MicroBatchPublisher` and subscribed on the WebSocket multiplexer.
    - Strict unsubscribe ordering: The symbol is disabled in `MicroBatchPublisher` and unsubscribed on the
      WebSocket multiplexer BEFORE being deleted from `active_subs`.
    """

    def __init__(
        self,
        watchlist_table: Table,
        ws_url: str,
        channel: str,
        publisher: MicroBatchPublisher,
        ws_multiplexer: WebSocketMultiplexer,
        symbol_normalizer: SymbolNormalizer,
        *,
        symbol_col: str = "Symbol",
        exec_ctx: ExecutionContext | None = None,
        seed_fn: Callable[[list[str]], Sequence[Mapping[str, Any]]] | None = None,
    ) -> None:
        from deephaven import dtypes as dht
        from deephaven import input_table
        from deephaven.table_listener import listen

        self.watchlist_table = watchlist_table
        self.ws_url = ws_url
        self.channel = channel
        self.publisher = publisher
        self.ws_multiplexer = ws_multiplexer
        self.symbol_normalizer = symbol_normalizer
        self.symbol_col = symbol_col
        self._exec_ctx = exec_ctx
        self.seed_fn = seed_fn

        self._lock = threading.Lock()
        self._active_symbols: set[str] = set()
        self._pending_ops = 0
        self._idle_Condition = threading.Condition()

        # Initialize publisher with empty allowed set so only watchlist symbols pass through
        self.publisher.set_allowed_symbols(())
        self.ws_multiplexer.register_publisher(self.ws_url, self.channel, self.publisher)

        with exec_ctx_scope(self._exec_ctx):
            self._active_subs = input_table(
                col_defs={"Symbol": dht.string, "SubscribedAt": dht.Instant},
                key_cols=["Symbol"],
            )
            self._listener_handle = listen(
                self.watchlist_table,
                self._on_table_update,
                description=f"massive-watchlist-{self.channel}",
                do_replay=True,
            )

    @property
    def active_subs_table(self) -> Table:
        """Return the keyed Deephaven `InputTable` containing `(Symbol, SubscribedAt)`."""
        return self._active_subs

    @property
    def active_symbols(self) -> frozenset[str]:
        """Return a snapshot of currently active canonical symbols."""
        with self._lock:
            return frozenset(self._active_symbols)

    def _extract_symbols(self, col_map: Mapping[str, Any] | None) -> list[str]:
        if not col_map or self.symbol_col not in col_map:
            return []
        raw_seq = col_map[self.symbol_col]
        result: list[str] = []
        for item in raw_seq:
            if item is not None:
                s = str(item).strip()
                if s:
                    result.append(self.symbol_normalizer.canonical(s))
        return result

    def _on_table_update(self, update: Any, is_replay: bool) -> None:
        """Non-blocking Deephaven UpdateGraph listener callback."""
        added = self._extract_symbols(update.added())
        removed = self._extract_symbols(update.removed())
        modified = self._extract_symbols(update.modified())
        modified_prev = self._extract_symbols(update.modified_prev())

        unchanged_mod = set(modified) & set(modified_prev)
        to_add = [s for s in (added + modified) if s not in unchanged_mod]
        to_remove = [s for s in (removed + modified_prev) if s not in unchanged_mod]

        if not to_add and not to_remove:
            return

        with self._idle_Condition:
            self._pending_ops += 1

        self.ws_multiplexer.call_soon_threadsafe(
            self._apply_diff_safe, to_add, to_remove
        )

    def _apply_diff_safe(
        self, to_add: Iterable[str], to_remove: Iterable[str]
    ) -> None:
        try:
            self.apply_diff(to_add, to_remove)
        finally:
            with self._idle_Condition:
                self._pending_ops = max(0, self._pending_ops - 1)
                self._idle_Condition.notify_all()

    def apply_diff(
        self, to_add: Iterable[str], to_remove: Iterable[str]
    ) -> None:
        """Apply symbol additions and removals with strict ordering across `active_subs`, publisher, and WS."""
        add_set = {self.symbol_normalizer.canonical(s) for s in to_add if s}
        rem_set = {self.symbol_normalizer.canonical(s) for s in to_remove if s} - add_set

        with self._lock:
            net_add = sorted(add_set - self._active_symbols)
            net_rem = sorted(rem_set & self._active_symbols)
            self._active_symbols.update(net_add)
            self._active_symbols.difference_update(net_rem)

        # 1. Handle removals first: disable in publisher -> unsubscribe WS -> delete from active_subs
        if net_rem:
            self.publisher.disable_symbols(net_rem)
            rem_topics = [
                self.symbol_normalizer.ws_topic(self.channel, s) for s in net_rem
            ]
            self.ws_multiplexer.unsubscribe(self.ws_url, self.channel, rem_topics)

            del_batch = pa.RecordBatch.from_arrays(
                [pa.array(net_rem, type=pa.string())],
                schema=_ACTIVE_SUBS_KEY_SCHEMA,
            )
            del_dh = arrow_to_deephaven_table(
                pa.Table.from_batches([del_batch]), self._exec_ctx
            )
            with exec_ctx_scope(self._exec_ctx):
                self._active_subs.delete(del_dh)

        # 2. Handle additions: write (Symbol, SubscribedAt) to active_subs -> enable in publisher -> subscribe WS -> seed
        if net_add:
            sub_ts = time.time_ns()
            add_batch = pa.RecordBatch.from_arrays(
                [
                    pa.array(net_add, type=pa.string()),
                    pa.array([sub_ts] * len(net_add), type=UTC_NS_TYPE),
                ],
                schema=_ACTIVE_SUBS_ARROW_SCHEMA,
            )
            add_dh = arrow_to_deephaven_table(
                pa.Table.from_batches([add_batch]), self._exec_ctx
            )
            with exec_ctx_scope(self._exec_ctx):
                self._active_subs.add(add_dh)

            self.publisher.enable_symbols(net_add)
            add_topics = [
                self.symbol_normalizer.ws_topic(self.channel, s) for s in net_add
            ]
            self.ws_multiplexer.subscribe(
                self.ws_url, self.channel, add_topics, self.publisher
            )

            if self.seed_fn is not None:
                seed_rows = self.seed_fn(net_add)
                if seed_rows:
                    self.publisher.enqueue(seed_rows)
                    self.publisher.flush_now()

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """Block until all scheduled watchlist diff operations have completed."""
        deadline = time.monotonic() + timeout
        with self._idle_Condition:
            while self._pending_ops > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle_Condition.wait(timeout=remaining)
            return True

    def stop(self) -> None:
        """Stop listening to the watchlist table and unsubscribe all active symbols."""
        if self._listener_handle is not None:
            try:
                self._listener_handle.stop()
            except Exception:  # pragma: no cover
                pass
            self._listener_handle = None

        with self._lock:
            current = sorted(self._active_symbols)

        if current:
            self.apply_diff((), current)
        self.ws_multiplexer.unregister_publisher(
            self.ws_url, self.channel, self.publisher
        )
