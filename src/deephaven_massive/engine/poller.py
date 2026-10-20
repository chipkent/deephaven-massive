"""Periodic REST endpoint poller producing live updating Deephaven tables."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import threading
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

VALID_POLL_MODES: frozenset[str] = frozenset({"replace", "upsert", "append"})


class SnapshotPoller:
    """Periodically polls a REST endpoint and publishes updates into a Deephaven table.

    Modes:
    - ``"replace"``: Evicts stale keys not present in the latest poll cycle using a monotonic `__PollId` join.
    - ``"upsert"``: Retains all historical keys and updates rows by `key_cols` (`last_by(key_cols)`).
    - ``"append"``: Appends every poll batch into an append-only history table (`blink_to_append_only`).
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
        from deephaven.stream import blink_to_append_only
        from deephaven.stream.table_publisher import table_publisher

        clean_mode = mode.strip().lower()
        if clean_mode not in VALID_POLL_MODES:
            raise ValueError(
                f"Unsupported poll mode '{mode}'. Expected one of {sorted(VALID_POLL_MODES)}."
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
            [pa.field("__PollId", pa.int64())]
        )
        self._poll_id = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._poll_thread: threading.Thread | None = None

        empty_dh = arrow_to_deephaven_table(self.full_schema.empty_table(), self._exec_ctx)
        col_defs = {c.name: c.data_type for c in empty_dh.columns}

        def _build_tables() -> tuple[Table, Any, Table]:
            blink_tbl, dh_pub = table_publisher(name, col_defs)
            if self.mode == "replace":
                raw_keyed = (
                    blink_tbl.last_by(list(self.key_cols))
                    if self.key_cols
                    else blink_tbl
                )
                current_epoch = blink_tbl.view(["__PollId"]).last_by()
                out_tbl = (
                    raw_keyed.natural_join(
                        current_epoch, on=[], joins=["CurrentPollId = __PollId"]
                    )
                    .where("__PollId == CurrentPollId")
                    .drop_columns(["__PollId", "CurrentPollId"])
                )
            elif self.mode == "upsert":
                stripped = blink_tbl.drop_columns(["__PollId"])
                out_tbl = (
                    stripped.last_by(list(self.key_cols))
                    if self.key_cols
                    else stripped.last_by()
                )
            else:
                out_tbl = blink_to_append_only(blink_tbl.drop_columns(["__PollId"]))
            return blink_tbl, dh_pub, out_tbl

        with exec_ctx_scope(self._exec_ctx):
            self._blink_table, self._dh_publisher, self._table = _build_tables()

    @property
    def table(self) -> Table:
        """Return the live Deephaven Table updated by this poller."""
        return self._table

    @property
    def poll_id(self) -> int:
        """Return the current monotonic poll epoch counter."""
        with self._lock:
            return self._poll_id

    def poll_once(self) -> int:
        """Execute a single synchronous poll fetch and publish the resulting batch to Deephaven."""
        rows = self.fetch_fn()
        with self._lock:
            self._poll_id += 1
            epoch = self._poll_id

        if not rows:
            return 0

        rb = rows_to_record_batch(
            rows,
            self.schema_spec,
            self.symbol_normalizer,
            extra_columns={"__PollId": (pa.int64(), epoch)},
        )
        pa_tbl = pa.Table.from_batches([rb])
        dh_tbl = arrow_to_deephaven_table(pa_tbl, self._exec_ctx)

        with exec_ctx_scope(self._exec_ctx):
            self._dh_publisher.add(dh_tbl)

        return len(rows)

    def start(self, *, initial_poll: bool = True) -> None:
        """Perform an optional initial poll and start the background polling loop."""
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
        """Stop the background polling thread."""
        self._stop_event.set()
        thread = self._poll_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._poll_thread = None
