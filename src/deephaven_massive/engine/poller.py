"""Periodic REST endpoint poller producing live updating Deephaven tables.

`SnapshotPoller` powers `FeedBuilder.poll()`, turning any REST snapshot, reference,
economy, or partner endpoint into a live updating Deephaven `Table` with configurable
eviction semantics (`"replace"`, `"upsert"`, or `"append"`).
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    exec_ctx_scope,
    rows_to_record_batch,
)

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive.engine.symbols import SymbolNormalizer

_VALID_POLL_MODES: frozenset[str] = frozenset({"replace", "upsert", "append"})


class SnapshotPoller:
    """Periodically polls a REST endpoint and publishes updates into a Deephaven `Table`.

    Three update modes are supported:

    - `"replace"`: Evicts stale keys that are no longer present in the latest poll
      cycle by joining `last_by(key_cols)` against the current monotonic `__PollId`
      epoch. Ideal for top-movers (`gainers_losers`) or active market status tables.
    - `"upsert"`: Retains all previously seen keys and updates rows in place via
      `last_by(key_cols)`. Ideal for full-universe snapshots or reference tables.
    - `"append"`: Appends every poll batch into an append-only history table via
      `blink_to_append_only`. Ideal for recording periodic snapshots over time.
    """

    def __init__(
        self,
        name: str,
        schema_spec: SchemaSpec,
        fetch_fn: Callable[[], Sequence[Mapping[str, Any]]],
        *,
        interval_s: float = 60.0,
        mode: str = "replace",
        key_cols: Sequence[str] | None = None,
        symbol_normalizer: SymbolNormalizer | None = None,
        exec_ctx: ExecutionContext | None = None,
    ) -> None:
        """Initialize a `SnapshotPoller` and construct its Deephaven query DAG.

        Args:
            name: Logical name for the underlying Deephaven `table_publisher`.
            schema_spec: `SchemaSpec` governing column types, key columns, and row normalization.
            fetch_fn: Zero-argument callable returning a sequence of raw row dictionaries.
            interval_s: Seconds between background poll cycles (must be > 0).
            mode: Poll materialization mode (`"replace"`, `"upsert"`, or `"append"`).
            key_cols: Optional override for primary key columns (defaults to
                `schema_spec.key_cols`).
            symbol_normalizer: Optional `SymbolNormalizer` passed to the row normalizer.
            exec_ctx: Optional Deephaven `ExecutionContext` bound during table creation
                and background publishing.

        Raises:
            ValueError: If `mode` is not in `{"replace", "upsert", "append"}` or
                `interval_s <= 0`.
        """
        from deephaven.stream import blink_to_append_only
        from deephaven.stream.table_publisher import table_publisher

        clean_mode = mode.strip().lower()
        if clean_mode not in _VALID_POLL_MODES:
            raise ValueError(
                f"Unsupported poll mode '{mode}'. Expected one of {sorted(_VALID_POLL_MODES)}."
            )
        if interval_s <= 0:
            raise ValueError("interval_s must be > 0")

        self.name = name
        self.schema_spec = schema_spec
        self.fetch_fn = fetch_fn
        self.interval_s = float(interval_s)
        self.mode = clean_mode
        self.key_cols: tuple[str, ...] = (
            tuple(key_cols) if key_cols is not None else schema_spec.key_cols
        )
        self.symbol_normalizer = symbol_normalizer
        self._exec_ctx = exec_ctx

        self.full_schema = schema_spec.with_extra_fields(
            [
                pa.field("__PollId", pa.int64()),
                pa.field("__RowIdx", pa.int64()),
            ]
        )
        self._poll_id = 0
        self._lock = threading.Lock()
        self._poll_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._poll_thread: threading.Thread | None = None

        empty_dh = arrow_to_deephaven_table(self.full_schema.empty_table(), self._exec_ctx)
        col_defs = {c.name: c.data_type for c in empty_dh.columns}

        def _build_tables() -> tuple[Table, Any, Table]:
            blink_tbl, dh_pub = table_publisher(name, col_defs)
            if self.mode == "replace":
                replace_keys = list(self.key_cols) if self.key_cols else ["__RowIdx"]
                raw_keyed = blink_tbl.last_by(replace_keys)
                current_epoch = blink_tbl.view(["__PollId"]).last_by()
                out_tbl = (
                    raw_keyed.natural_join(current_epoch, on=[], joins=["CurrentPollId = __PollId"])
                    .where("__PollId == CurrentPollId")
                    .drop_columns(["__PollId", "__RowIdx", "CurrentPollId"])
                )
            elif self.mode == "upsert":
                stripped = blink_tbl.drop_columns(["__PollId", "__RowIdx"])
                out_tbl = (
                    stripped.last_by(list(self.key_cols)) if self.key_cols else stripped.last_by()
                )
            else:
                out_tbl = blink_to_append_only(blink_tbl.drop_columns(["__PollId", "__RowIdx"]))
            return blink_tbl, dh_pub, out_tbl

        with exec_ctx_scope(self._exec_ctx):
            self._blink_table, self._dh_publisher, self._table = _build_tables()

    @property
    def table(self) -> Table:
        """Return the live Deephaven `Table` updated by this poller."""
        return self._table

    @property
    def poll_id(self) -> int:
        """Return the current monotonic poll epoch counter."""
        with self._lock:
            return self._poll_id

    def poll_once(self) -> int:
        """Execute a single synchronous poll fetch and publish the resulting batch to Deephaven.

        Serialized under `_poll_lock` so that monotonic `__PollId` epochs are always
        published to Deephaven in strictly increasing order even if `poll_once()` is
        called concurrently with the background polling thread.

        Returns:
            The number of rows fetched and published in this poll cycle.
        """
        with self._poll_lock:
            rows = self.fetch_fn()
            with self._lock:
                self._poll_id += 1
                epoch = self._poll_id

            if not rows:
                return 0

            row_indices = list(range(len(rows)))
            rb = rows_to_record_batch(
                rows,
                self.schema_spec,
                self.symbol_normalizer,
                extra_columns={
                    "__PollId": (pa.int64(), epoch),
                    "__RowIdx": (pa.int64(), row_indices),
                },
            )
            pa_tbl = pa.Table.from_batches([rb])
            dh_tbl = arrow_to_deephaven_table(pa_tbl, self._exec_ctx)

            with exec_ctx_scope(self._exec_ctx):
                self._dh_publisher.add(dh_tbl)

            return len(rows)

    def start(self, *, initial_poll: bool = True) -> None:
        """Perform an optional synchronous initial poll and start the background polling loop.

        Args:
            initial_poll: If `True`, executes `poll_once()` synchronously before
                spawning the background daemon thread so that `self.table` is
                immediately populated upon return.
        """
        if initial_poll:
            self.poll_once()

        with self._lock:
            if self._poll_thread is not None and self._poll_thread.is_alive():
                return
            self._stop_event.clear()

            def _loop() -> None:
                while not self._stop_event.wait(self.interval_s):
                    self.poll_once()

            self._poll_thread = threading.Thread(
                target=_loop,
                name=f"massive-poll-{self.name}",
                daemon=True,
            )
            self._poll_thread.start()

    def stop(self) -> None:
        """Stop the background polling thread and wait up to 2 seconds for termination."""
        self._stop_event.set()
        thread = self._poll_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._poll_thread = None
