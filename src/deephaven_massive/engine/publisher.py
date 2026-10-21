"""Micro-batching Deephaven `TablePublisher` with lossless startup seed cutover deduplication.

`MicroBatchPublisher` bridges high-frequency WebSocket JSON frames into Deephaven's
update graph. It buffers incoming rows into columnar `pyarrow.RecordBatch` chunks,
flushes them periodically (or when `max_batch_size` is reached), and coordinates
startup seeding so that `.latest()` and `.history(seed_since=...)` transition from
REST snapshots to live WebSocket ticks without dropping or duplicating events.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    UTC_NS_TYPE,
    SchemaSpec,
    arrow_to_deephaven_table,
    exec_ctx_scope,
    rows_to_record_batch,
    to_utc_ns,
)

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive.engine.symbols import SymbolNormalizer


def _row_timestamp_ns(row: Mapping[str, Any]) -> int | None:
    """Extract the primary event timestamp in UTC nanoseconds from a normalized row."""
    for key in ("Timestamp", "WindowStart", "LastUpdated"):
        val = row.get(key)
        if val is not None:
            ns = to_utc_ns(val)
            if ns is not None:
                return ns
    return None


class MicroBatchPublisher:
    """Thread-safe micro-batching wrapper around Deephaven's `table_publisher`.

    Key responsibilities:
    - Coalesces incoming WebSocket events into `pyarrow.RecordBatch` chunks flushed
      every `flush_interval_ms` or immediately upon reaching `max_batch_size`.
    - Binds the captured Deephaven `ExecutionContext` on background threads before
      invoking `TablePublisher.add`.
    - Enforces per-publisher symbol filtering so multiple feeds sharing a multiplexed
      WebSocket channel receive only their subscribed symbols.
    - Implements two-phase startup seeding (`begin_seeding` / `complete_seeding`) that
      buffers live WebSocket events while the REST seed is in flight, deduplicates by
      `(Symbol, TradeId)` and `(key_cols, Timestamp, SequenceNumber)`, and flushes in
      strict FIFO order under `_publish_lock`.
    """

    def __init__(
        self,
        name: str,
        schema_spec: SchemaSpec,
        symbol_normalizer: SymbolNormalizer | None = None,
        exec_ctx: ExecutionContext | None = None,
        *,
        flush_interval_ms: int = 50,
        max_batch_size: int = 10_000,
        allowed_symbols: Iterable[str] | None = None,
    ) -> None:
        """Initialize a `MicroBatchPublisher` and allocate its Deephaven blink table.

        Args:
            name: Logical name passed to `deephaven.stream.table_publisher`.
            schema_spec: `SchemaSpec` governing column types and row normalization.
            symbol_normalizer: Optional `SymbolNormalizer` for canonicalizing `Symbol` values.
            exec_ctx: Optional Deephaven `ExecutionContext` bound during table creation
                and flushes.
            flush_interval_ms: Milliseconds between background flush cycles.
            max_batch_size: Maximum buffered rows before an immediate flush is triggered.
            allowed_symbols: Optional initial iterable of symbols permitted through the
                publisher filter (`None` disables filtering).
        """
        from deephaven.stream.table_publisher import table_publisher

        self.name = name
        self.schema_spec = schema_spec
        self.symbol_normalizer = symbol_normalizer
        self._exec_ctx = exec_ctx
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_size = max_batch_size

        self.full_schema = schema_spec.with_extra_fields([pa.field("__ReceivedAt", UTC_NS_TYPE)])

        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._allowed_symbols: set[str] | None = None
        if allowed_symbols is not None:
            self.set_allowed_symbols(allowed_symbols)

        self._buffer: list[dict[str, Any]] = []
        self._seed_buffer: list[dict[str, Any]] = []
        self._seeding = False
        self._seeded = False
        self._has_snapshot_seed = False
        self._consumer_count = 0
        self._rows_published = 0
        self._last_error: Exception | None = None
        self._latest_ts_by_symbol: dict[str | None, int] = {}
        self._published_latest_ts_by_symbol: dict[str | None, int] = {}
        self._earliest_ts_by_symbol: dict[str | None, int] = {}
        self._post_attach_sigs: set[tuple[Any, ...]] = set()
        self._stop_event = threading.Event()
        self._flush_wakeup = threading.Event()
        self._flush_thread: threading.Thread | None = None

        empty_dh = arrow_to_deephaven_table(self.full_schema.empty_table(), self._exec_ctx)
        col_defs = {c.name: c.data_type for c in empty_dh.columns}
        with exec_ctx_scope(self._exec_ctx):
            self._blink_table, self._dh_publisher = table_publisher(name, col_defs)

    @property
    def is_seeded(self) -> bool:
        """Return `True` if startup seeding (`complete_seeding`) has completed."""
        with self._lock:
            return self._seeded

    @property
    def has_snapshot_seed(self) -> bool:
        """Return `True` if a REST snapshot or historical seed has been applied."""
        with self._lock:
            return self._has_snapshot_seed

    def mark_snapshot_seeded(self) -> None:
        """Record that a REST snapshot or historical seed has been applied to this publisher."""
        with self._lock:
            self._has_snapshot_seed = True

    def acquire_consumer(self) -> int:
        """Increment and return the active `FeedBuilder` consumer reference count."""
        with self._lock:
            self._consumer_count += 1
            return self._consumer_count

    def release_consumer(self) -> int:
        """Decrement and return the remaining active `FeedBuilder` consumer reference count."""
        with self._lock:
            self._consumer_count = max(0, self._consumer_count - 1)
            return self._consumer_count

    @property
    def rows_published(self) -> int:
        """Return the cumulative number of rows published to Deephaven so far."""
        with self._lock:
            return self._rows_published

    @property
    def last_error(self) -> Exception | None:
        """Return the most recent exception raised in the background flush thread, if any."""
        with self._lock:
            return self._last_error

    def _record_row_timestamp(self, sym: str | None, ts_ns: int | None) -> None:
        """Update per-symbol min and max observed event timestamps (caller holds `_lock`)."""
        if ts_ns is None:
            return
        prev_max = self._latest_ts_by_symbol.get(sym)
        if prev_max is None or ts_ns > prev_max:
            self._latest_ts_by_symbol[sym] = ts_ns
        prev_min = self._earliest_ts_by_symbol.get(sym)
        if prev_min is None or ts_ns < prev_min:
            self._earliest_ts_by_symbol[sym] = ts_ns

    def _row_event_signature(self, nr: Mapping[str, Any]) -> tuple[Any, ...]:
        """Compute a deterministic event identity tuple for deduplicating seed and live rows."""
        key_tuple = tuple(nr.get(k) for k in self.schema_spec.key_cols)
        return (
            key_tuple,
            _row_timestamp_ns(nr),
            nr.get("TradeId"),
            nr.get("SequenceNumber"),
        )

    def _record_event_signatures(
        self,
        nr: Mapping[str, Any],
        target: set[tuple[Any, ...]],
    ) -> None:
        """Add event-identity signatures (and TradeId key if present) for `nr` into `target`."""
        target.add(self._row_event_signature(nr))
        if nr.get("TradeId") is not None:
            target.add(("__trade_id__", nr.get("Symbol"), str(nr["TradeId"])))

    def attach_downstream(
        self,
        build_fn: Callable[[], Table],
    ) -> tuple[Table, dict[str | None, int]]:
        """Atomically coordinate with the UpdateGraph, attach a downstream view, and flush pending rows.

        Acquires `_publish_lock` and Deephaven's UpdateGraph `exclusive_lock`, captures the
        already-published timestamp cutoff before attaching `build_fn()`, and then flushes
        any buffered rows into the newly attached view while recording their event identities
        in `_post_attach_sigs` for backfill deduplication.

        Args:
            build_fn: Callable that constructs and returns a downstream Deephaven `Table`.

        Returns:
            Tuple of `(table, cutoff_by_symbol)`.
        """
        from deephaven.update_graph import exclusive_lock

        with self._publish_lock:
            with self._lock:
                cutoff = dict(self._published_latest_ts_by_symbol)
                self._post_attach_sigs.clear()
            with exec_ctx_scope(self._exec_ctx), exclusive_lock(self._blink_table):
                tbl = build_fn()
            self._flush_locked()
        return tbl, cutoff

    @property
    def blink_table(self) -> Table:
        """Return the raw Deephaven blink `Table` including the internal `__ReceivedAt` column."""
        return self._blink_table

    @property
    def user_blink_table(self) -> Table:
        """Return the Deephaven blink `Table` with internal `__ReceivedAt` stripped."""
        with exec_ctx_scope(self._exec_ctx):
            return self._blink_table.drop_columns(["__ReceivedAt"])

    def _canonicalize(self, sym: str) -> str:
        """Canonicalize `sym` using `self.symbol_normalizer` if present."""
        return (
            self.symbol_normalizer.canonical(sym)
            if self.symbol_normalizer is not None
            else sym.strip().upper()
        )

    def set_allowed_symbols(self, symbols: Iterable[str] | None) -> None:
        """Replace the set of allowed symbols (`None` disables symbol filtering).

        Args:
            symbols: Iterable of symbol strings to permit, or `None` to accept all rows.
        """
        with self._lock:
            if symbols is None:
                self._allowed_symbols = None
            else:
                self._allowed_symbols = {self._canonicalize(s) for s in symbols}

    def enable_symbols(self, symbols: Iterable[str]) -> None:
        """Add one or more symbols to the publisher's allowed symbol filter.

        Args:
            symbols: Iterable of symbol strings to enable.
        """
        canon = {self._canonicalize(s) for s in symbols}
        with self._lock:
            if self._allowed_symbols is None:
                self._allowed_symbols = set(canon)
            else:
                self._allowed_symbols.update(canon)

    def disable_symbols(self, symbols: Iterable[str]) -> None:
        """Remove one or more symbols from the publisher's allowed symbol filter.

        Args:
            symbols: Iterable of symbol strings to disable.
        """
        canon = {self._canonicalize(s) for s in symbols}
        with self._lock:
            if self._allowed_symbols is not None:
                self._allowed_symbols.difference_update(canon)

    @property
    def allowed_symbols(self) -> frozenset[str] | None:
        """Return an immutable snapshot of currently allowed symbols, or `None` if unfiltered."""
        with self._lock:
            return frozenset(self._allowed_symbols) if self._allowed_symbols is not None else None

    def begin_seeding(self) -> None:
        """Enter startup seeding mode so incoming WebSocket rows are buffered until `complete_seeding`."""
        with self._lock:
            self._seeding = True

    def complete_seeding(
        self,
        seed_rows: Sequence[Mapping[str, Any]],
        *,
        seed_since: Any = None,
    ) -> int:
        """Deduplicate buffered WebSocket rows against `seed_rows` and flush both to Deephaven.

        Args:
            seed_rows: Raw REST snapshot or historical backfill rows.
            seed_since: Optional minimum timestamp cutoff (`str`, `datetime`, or epoch
                number); seed rows older than `seed_since` are discarded.

        Returns:
            Total number of merged seed and buffered WebSocket rows flushed to Deephaven.
        """
        now_ns = time.time_ns()
        min_seed_ts_ns = to_utc_ns(seed_since) if seed_since is not None else None
        norm_seeds: list[dict[str, Any]] = []
        seen_trade_ids: set[tuple[str | None, str]] = set()
        seen_event_sigs: set[tuple[Any, ...]] = set()
        max_ts_by_symbol: dict[str | None, int] = {}
        has_trade_id = "TradeId" in self.schema_spec.arrow_schema.names

        for raw in seed_rows:
            nr = dict(self.schema_spec.normalizer(raw, self.symbol_normalizer))
            ts_ns = _row_timestamp_ns(nr)
            if min_seed_ts_ns is not None and ts_ns is not None and ts_ns < min_seed_ts_ns:
                continue
            nr["__ReceivedAt"] = now_ns
            sym = nr.get("Symbol")
            if has_trade_id and nr.get("TradeId") is not None:
                seen_trade_ids.add((sym, str(nr["TradeId"])))
            if self.schema_spec.key_cols:
                seen_event_sigs.add(self._row_event_signature(nr))
            if ts_ns is not None:
                prev_max = max_ts_by_symbol.get(sym)
                if prev_max is None or ts_ns > prev_max:
                    max_ts_by_symbol[sym] = ts_ns
            norm_seeds.append(nr)

        with self._lock:
            buffered = self._seed_buffer
            self._seed_buffer = []
            self._seeding = False
            self._seeded = True

            older_ws: list[dict[str, Any]] = []
            newer_ws: list[dict[str, Any]] = []
            for ws_row in buffered:
                sym = ws_row.get("Symbol")
                if (
                    has_trade_id
                    and ws_row.get("TradeId") is not None
                    and (sym, str(ws_row["TradeId"])) in seen_trade_ids
                ):
                    continue
                ws_ts = _row_timestamp_ns(ws_row)
                if min_seed_ts_ns is not None and ws_ts is not None and ws_ts < min_seed_ts_ns:
                    continue
                if (
                    self.schema_spec.key_cols
                    and self._row_event_signature(ws_row) in seen_event_sigs
                ):
                    continue
                max_seed_ts = max_ts_by_symbol.get(sym)
                if ws_ts is not None and max_seed_ts is not None and ws_ts < max_seed_ts:
                    older_ws.append(ws_row)
                else:
                    newer_ws.append(ws_row)

            merged = older_ws + norm_seeds + newer_ws
            for r in merged:
                self._record_row_timestamp(r.get("Symbol"), _row_timestamp_ns(r))
            self._buffer = merged + self._buffer

        return self.flush_now()

    def seed_late_snapshot(
        self,
        seed_rows: Sequence[Mapping[str, Any]],
        *,
        auto_flush: bool = False,
    ) -> int:
        """Apply a REST snapshot seed to an already-live publisher without regressing newer ticks.

        Timestamp validation, buffer insertion, and optional publication are serialized
        under `_publish_lock` and `_lock` against both already-published and queued live
        ticks so `.latest()` never regresses.

        Args:
            seed_rows: Raw REST snapshot rows.
            auto_flush: Whether to immediately flush accepted snapshot rows.

        Returns:
            Number of snapshot rows accepted.
        """
        now_ns = time.time_ns()
        accepted: list[dict[str, Any]] = []
        with self._publish_lock:
            self._flush_locked()
            with self._lock:
                for raw in seed_rows:
                    nr = dict(self.schema_spec.normalizer(raw, self.symbol_normalizer))
                    sym = nr.get("Symbol")
                    ts_ns = _row_timestamp_ns(nr)
                    latest_ts = self._latest_ts_by_symbol.get(sym)
                    if latest_ts is not None and (ts_ns is None or ts_ns <= latest_ts):
                        continue
                    nr.setdefault("__ReceivedAt", now_ns)
                    self._record_row_timestamp(sym, ts_ns)
                    accepted.append(nr)
                self._has_snapshot_seed = True
                if accepted:
                    self._buffer.extend(accepted)
            if auto_flush and accepted:
                self._flush_locked()
        return len(accepted)

    def filter_historical_backfill(
        self,
        seed_rows: Sequence[Mapping[str, Any]],
        *,
        seed_since: Any = None,
        cutoff_by_symbol: Mapping[str | None, int] | None = None,
    ) -> list[dict[str, Any]]:
        """Normalize and filter historical backfill rows for a newly attached history/ring view.

        Retains rows at or after `seed_since` up to the per-symbol cutoff timestamp
        captured when the view was attached, while deduplicating by event identity against
        any rows flushed after attachment.

        Args:
            seed_rows: Raw historical rows fetched for `seed_since`.
            seed_since: Optional minimum timestamp cutoff.
            cutoff_by_symbol: Optional per-symbol maximum timestamp snapshot captured
                when the downstream view was created. Defaults to the publisher's
                current `_published_latest_ts_by_symbol`.

        Returns:
            Normalized rows belonging to the backfill window.
        """
        self.flush_now()
        min_seed_ts_ns = to_utc_ns(seed_since) if seed_since is not None else None
        filtered: list[dict[str, Any]] = []
        with self._lock:
            seen_sigs = set(self._post_attach_sigs)
            cutoffs = (
                self._published_latest_ts_by_symbol
                if cutoff_by_symbol is None
                else cutoff_by_symbol
            )
            for raw in seed_rows:
                nr = dict(self.schema_spec.normalizer(raw, self.symbol_normalizer))
                sym = nr.get("Symbol")
                if (
                    nr.get("TradeId") is not None
                    and ("__trade_id__", sym, str(nr["TradeId"])) in seen_sigs
                ):
                    continue
                if self._row_event_signature(nr) in seen_sigs:
                    continue
                ts_ns = _row_timestamp_ns(nr)
                if min_seed_ts_ns is not None and ts_ns is not None and ts_ns < min_seed_ts_ns:
                    continue
                cutoff_ts = cutoffs.get(sym)
                if cutoff_ts is not None:
                    if ts_ns is not None and ts_ns > cutoff_ts:
                        continue
                else:
                    first_live_ts = self._earliest_ts_by_symbol.get(sym)
                    if first_live_ts is not None and ts_ns is not None and ts_ns >= first_live_ts:
                        continue
                self._record_event_signatures(nr, seen_sigs)
                filtered.append(nr)
        return filtered

    def enqueue(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        already_normalized: bool = False,
    ) -> int:
        """Normalize, filter, and enqueue incoming rows, flushing if `max_batch_size` is reached.

        Args:
            rows: Sequence of raw wire dictionaries or pre-normalized row dictionaries.
            already_normalized: If `True`, skips running `schema_spec.normalizer`.

        Returns:
            Number of rows accepted after symbol filtering.
        """
        if not rows:
            return 0

        now_ns = time.time_ns()
        accepted: list[dict[str, Any]] = []

        with self._lock:
            allowed = set(self._allowed_symbols) if self._allowed_symbols is not None else None

        for raw in rows:
            nr = (
                dict(raw)
                if already_normalized
                else dict(self.schema_spec.normalizer(raw, self.symbol_normalizer))
            )
            if allowed is not None and "*" not in allowed:
                sym = nr.get("Symbol")
                if sym not in allowed:
                    continue
            nr.setdefault("__ReceivedAt", now_ns)
            accepted.append(nr)

        if not accepted:
            return 0

        should_flush = False
        thread_alive = False
        with self._lock:
            if self._seeding:
                self._seed_buffer.extend(accepted)
            else:
                for r in accepted:
                    self._record_row_timestamp(r.get("Symbol"), _row_timestamp_ns(r))
                self._buffer.extend(accepted)
                should_flush = len(self._buffer) >= self.max_batch_size
                thread_alive = self._flush_thread is not None and self._flush_thread.is_alive()

        if should_flush:
            if thread_alive:
                self._flush_wakeup.set()
            else:
                self.flush_now()

        return len(accepted)

    def _flush_locked(self) -> int:
        """Drain `_buffer` and publish a `RecordBatch` to Deephaven (caller holds `_publish_lock`)."""
        with self._lock:
            if self._seeding or not self._buffer:
                return 0
            batch_rows = self._buffer
            self._buffer = []
            for r in batch_rows:
                sym = r.get("Symbol")
                ts_ns = _row_timestamp_ns(r)
                self._record_row_timestamp(sym, ts_ns)
                if ts_ns is not None:
                    prev_pub = self._published_latest_ts_by_symbol.get(sym)
                    if prev_pub is None or ts_ns > prev_pub:
                        self._published_latest_ts_by_symbol[sym] = ts_ns
                self._record_event_signatures(r, self._post_attach_sigs)

        received_vals = [r.get("__ReceivedAt") for r in batch_rows]
        rb = rows_to_record_batch(
            batch_rows,
            self.schema_spec,
            self.symbol_normalizer,
            extra_columns={"__ReceivedAt": (UTC_NS_TYPE, received_vals)},
            already_normalized=True,
        )
        pa_tbl = pa.Table.from_batches([rb])
        dh_tbl = arrow_to_deephaven_table(pa_tbl, self._exec_ctx)

        with exec_ctx_scope(self._exec_ctx):
            self._dh_publisher.add(dh_tbl)

        with self._lock:
            self._rows_published += len(batch_rows)
        return len(batch_rows)

    def flush_now(self) -> int:
        """Atomically drain the micro-batch buffer and publish a `RecordBatch` in FIFO order.

        Returns:
            Number of rows published in this flush (0 if seeding is active or buffer is empty).
        """
        with self._publish_lock:
            return self._flush_locked()

    def start(self) -> None:
        """Start the background periodic flush daemon thread (idempotent)."""
        with self._lock:
            if self._flush_thread is not None and self._flush_thread.is_alive():
                return
            self._stop_event.clear()
            self._flush_wakeup.clear()
            interval_s = self.flush_interval_ms / 1000.0

            def _loop() -> None:
                while not self._stop_event.is_set():
                    self._flush_wakeup.wait(interval_s)
                    self._flush_wakeup.clear()
                    try:
                        self.flush_now()
                    except Exception as exc:
                        with self._lock:
                            self._last_error = exc

            self._flush_thread = threading.Thread(
                target=_loop,
                name=f"massive-flush-{self.name}",
                daemon=True,
            )
            self._flush_thread.start()

    def stop(self) -> None:
        """Stop the background flush thread and flush any remaining buffered rows."""
        self._stop_event.set()
        self._flush_wakeup.set()
        thread = self._flush_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._flush_thread = None
        with self._lock:
            self._seeding = False
        self.flush_now()
