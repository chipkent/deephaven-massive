"""Micro-batching Deephaven TablePublisher with startup seed cutover deduplication."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import threading
import time
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
    """Thread-safe micro-batching wrapper around Deephaven's :func:`table_publisher`.

    Features:
    - Batches incoming WebSocket rows into Arrow :class:`pyarrow.RecordBatch` chunks.
    - Wraps all Deephaven JNI / publisher calls in the captured :class:`ExecutionContext`.
    - Supports per-feed symbol filtering for multiplexed WebSocket connections and reactive watchlists.
    - Supports deterministic startup seed buffering and deduplication (`begin_seeding` / `complete_seeding`).
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
        from deephaven.stream.table_publisher import table_publisher

        self.name = name
        self.schema_spec = schema_spec
        self.symbol_normalizer = symbol_normalizer
        self._exec_ctx = exec_ctx
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_size = max_batch_size

        self.full_schema = schema_spec.with_extra_fields(
            [pa.field("__ReceivedAt", UTC_NS_TYPE)]
        )

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
        self._stop_event = threading.Event()
        self._flush_wakeup = threading.Event()
        self._flush_thread: threading.Thread | None = None

        empty_dh = arrow_to_deephaven_table(self.full_schema.empty_table(), self._exec_ctx)
        col_defs = {c.name: c.data_type for c in empty_dh.columns}
        with exec_ctx_scope(self._exec_ctx):
            self._blink_table, self._dh_publisher = table_publisher(name, col_defs)

    @property
    def is_seeded(self) -> bool:
        """Return True if startup seeding has completed on this publisher."""
        with self._lock:
            return self._seeded

    @property
    def has_snapshot_seed(self) -> bool:
        """Return True if a REST snapshot or historical seed has been applied to this publisher."""
        with self._lock:
            return self._has_snapshot_seed

    def mark_snapshot_seeded(self) -> None:
        """Record that a REST snapshot or historical seed has been applied to this publisher."""
        with self._lock:
            self._has_snapshot_seed = True

    def acquire_consumer(self) -> int:
        """Increment and return the active consumer reference count on this publisher."""
        with self._lock:
            self._consumer_count += 1
            return self._consumer_count

    def release_consumer(self) -> int:
        """Decrement and return the remaining active consumer reference count on this publisher."""
        with self._lock:
            self._consumer_count = max(0, self._consumer_count - 1)
            return self._consumer_count

    @property
    def rows_published(self) -> int:
        """Return the total number of rows published to Deephaven so far."""
        with self._lock:
            return self._rows_published

    @property
    def blink_table(self) -> Table:
        """Return the raw blink table including the internal `__ReceivedAt` column."""
        return self._blink_table

    @property
    def user_blink_table(self) -> Table:
        """Return the blink table with internal `__ReceivedAt` stripped."""
        with exec_ctx_scope(self._exec_ctx):
            return self._blink_table.drop_columns(["__ReceivedAt"])

    def _canonicalize(self, sym: str) -> str:
        return (
            self.symbol_normalizer.canonical(sym)
            if self.symbol_normalizer is not None
            else sym.strip().upper()
        )

    def set_allowed_symbols(self, symbols: Iterable[str] | None) -> None:
        """Replace the set of allowed symbols (`None` disables symbol filtering)."""
        with self._lock:
            if symbols is None:
                self._allowed_symbols = None
            else:
                self._allowed_symbols = {self._canonicalize(s) for s in symbols}

    def enable_symbols(self, symbols: Iterable[str]) -> None:
        """Enable one or more symbols in the publisher's symbol filter."""
        canon = {self._canonicalize(s) for s in symbols}
        with self._lock:
            if self._allowed_symbols is None:
                self._allowed_symbols = set(canon)
            else:
                self._allowed_symbols.update(canon)

    def disable_symbols(self, symbols: Iterable[str]) -> None:
        """Disable one or more symbols in the publisher's symbol filter."""
        canon = {self._canonicalize(s) for s in symbols}
        with self._lock:
            if self._allowed_symbols is not None:
                self._allowed_symbols.difference_update(canon)

    @property
    def allowed_symbols(self) -> frozenset[str] | None:
        """Return a snapshot of currently allowed symbols, or `None` if unfiltered."""
        with self._lock:
            return (
                frozenset(self._allowed_symbols)
                if self._allowed_symbols is not None
                else None
            )

    def begin_seeding(self) -> None:
        """Enter startup seeding mode: buffer incoming WebSocket rows until `complete_seeding`."""
        with self._lock:
            self._seeding = True

    def complete_seeding(
        self,
        seed_rows: Sequence[Mapping[str, Any]],
        *,
        seed_since: Any = None,
    ) -> int:
        """Deduplicate buffered WebSocket rows against `seed_rows` and flush both to Deephaven."""
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
            if (
                min_seed_ts_ns is not None
                and ts_ns is not None
                and ts_ns < min_seed_ts_ns
            ):
                continue
            nr["__ReceivedAt"] = now_ns
            sym = nr.get("Symbol")
            if has_trade_id and nr.get("TradeId") is not None:
                seen_trade_ids.add((sym, str(nr["TradeId"])))
            if self.schema_spec.key_cols:
                key_tuple = tuple(nr.get(k) for k in self.schema_spec.key_cols)
                seen_event_sigs.add(
                    (key_tuple, ts_ns, nr.get("TradeId"), nr.get("SequenceNumber"))
                )
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

            deduped_ws: list[dict[str, Any]] = []
            for ws_row in buffered:
                sym = ws_row.get("Symbol")
                if has_trade_id and ws_row.get("TradeId") is not None:
                    if (sym, str(ws_row["TradeId"])) in seen_trade_ids:
                        continue
                ws_ts = _row_timestamp_ns(ws_row)
                max_seed_ts = max_ts_by_symbol.get(sym)
                if ws_ts is not None and max_seed_ts is not None and ws_ts < max_seed_ts:
                    continue
                if self.schema_spec.key_cols:
                    key_tuple = tuple(ws_row.get(k) for k in self.schema_spec.key_cols)
                    ws_sig = (
                        key_tuple,
                        ws_ts,
                        ws_row.get("TradeId"),
                        ws_row.get("SequenceNumber"),
                    )
                    if (
                        ws_ts is not None
                        and max_seed_ts is not None
                        and ws_ts == max_seed_ts
                        and ws_sig in seen_event_sigs
                    ):
                        continue
                deduped_ws.append(ws_row)

            self._buffer = norm_seeds + deduped_ws + self._buffer

        return self.flush_now()

    def enqueue(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        already_normalized: bool = False,
    ) -> int:
        """Normalize, filter, and enqueue incoming rows. Flushes immediately if `max_batch_size` is reached."""
        if not rows:
            return 0

        now_ns = time.time_ns()
        accepted: list[dict[str, Any]] = []

        with self._lock:
            allowed = (
                set(self._allowed_symbols)
                if self._allowed_symbols is not None
                else None
            )

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
                self._buffer.extend(accepted)
                should_flush = len(self._buffer) >= self.max_batch_size
                thread_alive = (
                    self._flush_thread is not None and self._flush_thread.is_alive()
                )

        if should_flush:
            if thread_alive:
                self._flush_wakeup.set()
            else:
                self.flush_now()

        return len(accepted)

    def flush_now(self) -> int:
        """Atomically drain the micro-batch buffer and publish a RecordBatch to Deephaven in strict FIFO order."""
        with self._publish_lock:
            with self._lock:
                if self._seeding or not self._buffer:
                    return 0
                batch_rows = self._buffer
                self._buffer = []

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

    def start(self) -> None:
        """Start the background periodic flush thread."""
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
                    self.flush_now()

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
