"""Fluent `FeedBuilder` unifying WebSocket streaming, REST polling, snapshots, and historical tables.

Every domain method on `MassiveSession` returns a `FeedBuilder`. Rather than learning
separate APIs for WebSockets, REST snapshots, periodic pollers, and S3 Flat Files,
users chain one of seven terminal verbs onto the builder to materialize a Deephaven
`Table` with identical `PascalCase` column names and types.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    exec_ctx_scope,
    record_batches_to_arrow_table,
    rows_to_record_batch,
)
from deephaven_massive.engine.poller import SnapshotPoller
from deephaven_massive.engine.publisher import MicroBatchPublisher
from deephaven_massive.engine.watchlist import ReactiveWatchlistController

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive.engine.symbols import SymbolNormalizer
    from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer


def _is_deephaven_table(obj: Any) -> bool:
    """Return `True` if `obj` is a Deephaven `Table` or `InputTable` instance."""
    if obj is None or isinstance(obj, (str, bytes, list, tuple, set, frozenset, dict)):
        return False
    return hasattr(obj, "j_table") or hasattr(obj, "column_names")


class FeedBuilder:
    """Fluent builder that materializes a Massive data feed as a Deephaven `Table`.

    A `FeedBuilder` encapsulates the target `SchemaSpec`, symbol normalizer, WebSocket
    channel metadata, REST snapshot/seed hooks, and historical fetchers for a specific
    domain endpoint. Nothing is fetched or subscribed until one of the seven terminal
    methods is called:

    - `latest()`: Live updating current-state table (`last_by(key_cols)`), seeded via REST
      before streaming begins with zero duplicate or dropped events at cutover.
    - `history()`: Live updating in-memory append-only history table (`blink_to_append_only`).
    - `ring(capacity)`: Bounded live history table retaining the most recent `capacity`
      rows (`ring_table`).
    - `blink()`: Raw stateless single-cycle Deephaven blink table.
    - `snapshot()`: One-shot static point-in-time Deephaven table fetched via REST.
    - `poll(interval_s, mode)`: Periodic background REST poller (`"replace"`, `"upsert"`,
      or `"append"`).
    - `historical(start, end, source)`: Static historical Deephaven table fetched via REST
      pagination or S3 Flat Files.

    Example:
        ```python
        # Live current-state quotes seeded via REST snapshot
        quotes_latest = m.stocks.quotes(["AAPL", "NVDA"]).latest()

        # Bounded ring buffer of the last 50,000 trades
        trades_ring = m.stocks.trades(["AAPL", "NVDA"]).ring(50_000)

        # Static historical bars from S3 Flat Files or REST
        hist_bars = m.stocks.bars(["AAPL", "NVDA"], 1, "minute").historical(
            "2025-01-01", "2025-01-31"
        )
        ```
    """

    def __init__(
        self,
        name: str,
        schema_spec: SchemaSpec,
        *,
        symbol_normalizer: SymbolNormalizer | None = None,
        exec_ctx: ExecutionContext | None = None,
        ws_multiplexer: WebSocketMultiplexer | None = None,
        ws_url: str | None = None,
        ws_channel: str | None = None,
        symbols: str | Iterable[str] | Table | None = None,
        symbol_col: str = "Symbol",
        seed_fn: Callable[[list[str]], Sequence[Mapping[str, Any]]] | None = None,
        seed_rows_fn: Callable[[list[str], Any], Sequence[Mapping[str, Any]]] | None = None,
        snapshot_fn: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
        historical_fn: Callable[..., Table] | None = None,
        flush_interval_ms: int = 50,
        max_batch_size: int = 10_000,
        auto_flush: bool = True,
        on_register_lifecycle: Callable[[FeedBuilder], None] | None = None,
        stream_registry: dict[tuple[str, str, tuple[str, ...]], MicroBatchPublisher] | None = None,
    ) -> None:
        """Initialize a `FeedBuilder`.

        Args:
            name: Logical feed name (e.g., `"stocks.quotes"`).
            schema_spec: `SchemaSpec` defining the PyArrow schema, key columns, and normalizer.
            symbol_normalizer: Domain `SymbolNormalizer` for cross-transport symbol conversion.
            exec_ctx: Host Deephaven `ExecutionContext`.
            ws_multiplexer: Session `WebSocketMultiplexer` for streaming channels.
            ws_url: Resolved `wss://` endpoint URL if the feed supports WebSocket streaming.
            ws_channel: WebSocket event prefix (e.g., `"T"`, `"Q"`, `"AM"`).
            symbols: Target symbol(s) as a `str`, `Iterable[str]`, reactive Deephaven
                `Table` watchlist, or `None` (`"*"`).
            symbol_col: Column name to observe when `symbols` is a reactive Deephaven `Table`.
            seed_fn: Optional callable `(_canonical_symbols) -> rows` used to seed `.latest()`
                or `.history(seed=True)` before streaming cutover.
            seed_rows_fn: Optional callable `(_canonical_symbols, seed_since) -> rows` used
                by `.history(seed_since=...)` to backfill intraday history before cutover.
            snapshot_fn: Optional callable `() -> rows` used by `.snapshot()` and `.poll()`.
            historical_fn: Optional callable `(start, end, limit, source) -> Table` used
                by `.historical()`.
            flush_interval_ms: Micro-batch flush interval in milliseconds.
            max_batch_size: Maximum buffered rows before immediate flush.
            auto_flush: Whether to start the background flush thread automatically.
            on_register_lifecycle: Callback invoked to register active builders with the session.
            stream_registry: Shared session registry deduplicating `MicroBatchPublisher`
                instances across identical `(ws_url, ws_channel, symbols)` streams.
        """
        self.name = name
        self.schema_spec = schema_spec
        self.symbol_normalizer = symbol_normalizer
        self._exec_ctx = exec_ctx
        self.ws_multiplexer = ws_multiplexer
        self.ws_url = ws_url
        self.ws_channel = ws_channel
        self.symbols = symbols
        self._symbol_col = symbol_col
        self._seed_fn = seed_fn
        self._seed_rows_fn = seed_rows_fn
        self._snapshot_fn = snapshot_fn
        self._historical_fn = historical_fn
        self._flush_interval_ms = flush_interval_ms
        self._max_batch_size = max_batch_size
        self._auto_flush = auto_flush
        self._on_register_lifecycle = on_register_lifecycle
        self._stream_registry = stream_registry

        self._publisher: MicroBatchPublisher | None = None
        self._acquired_publisher: bool = False
        self._poller: SnapshotPoller | None = None
        self._watchlist_controller: ReactiveWatchlistController | None = None
        self._static_topics: list[str] = []
        self._stream_key: tuple[str, str, tuple[str, ...]] | None = None

    @property
    def publisher(self) -> MicroBatchPublisher | None:
        """Return the active `MicroBatchPublisher` if streaming has been started, else `None`."""
        return self._publisher

    @property
    def poller(self) -> SnapshotPoller | None:
        """Return the active `SnapshotPoller` if `.poll()` has been started, else `None`."""
        return self._poller

    @property
    def watchlist_controller(self) -> ReactiveWatchlistController | None:
        """Return the active `ReactiveWatchlistController` if streaming a table watchlist, else `None`."""
        return self._watchlist_controller

    def _normalize_symbols(self, symbols: str | Iterable[str] | Table | None) -> list[str]:
        """Normalize `symbols` using this builder's `SymbolNormalizer` (or active watchlist symbols).

        Args:
            symbols: A single symbol string, an iterable of symbols, a Deephaven `Table`
                watchlist, or `None` (which normalizes to `["*"]`).

        Returns:
            Deduplicated list of canonical symbol strings.
        """
        if _is_deephaven_table(symbols):
            if self._watchlist_controller is not None:
                return sorted(self._watchlist_controller.active_symbols)
            from deephaven import arrow as dh_arrow

            with exec_ctx_scope(self._exec_ctx):
                pa_tbl = dh_arrow.to_arrow(symbols.view([self._symbol_col]))  # type: ignore[union-attr]
            raw_vals = [
                str(v).strip()
                for v in pa_tbl.column(self._symbol_col).to_pylist()
                if v is not None and str(v).strip()
            ]
            if not raw_vals:
                return []
            symbols = raw_vals
        if symbols is None:
            return ["*"]
        if self.symbol_normalizer is not None:
            return self.symbol_normalizer.canonical_list(symbols)  # type: ignore[arg-type]
        if isinstance(symbols, str):
            return [symbols.strip().upper()]
        return [str(s).strip().upper() for s in symbols]  # type: ignore[union-attr]

    def _canonical_symbol_list(self) -> list[str]:
        """Return the canonical symbol list for `self.symbols`."""
        return self._normalize_symbols(self.symbols)

    def _setup_streaming(
        self, *, seed: bool, seed_since: Any = None
    ) -> tuple[
        MicroBatchPublisher,
        ReactiveWatchlistController | None,
        Callable[[], None],
    ]:
        """Initialize or reuse the `MicroBatchPublisher` and prepare the startup seed finalizer."""
        if self.ws_url is None or self.ws_channel is None or self.ws_multiplexer is None:
            raise NotImplementedError(
                f"Feed '{self.name}' does not support WebSocket streaming. "
                "Use .snapshot(), .poll(), or .historical() instead."
            )

        effective_seed = bool(seed or seed_since is not None)
        watchlist_ctrl: ReactiveWatchlistController | None = None

        if _is_deephaven_table(self.symbols):
            assert self.symbol_normalizer is not None
            wl_seed_fn: Callable[[list[str]], Sequence[Mapping[str, Any]]] | None = None
            if seed_since is not None and self._seed_rows_fn is not None:
                seed_rows_ref = self._seed_rows_fn

                def _wl_seed_since(syms: list[str]) -> Sequence[Mapping[str, Any]]:
                    return seed_rows_ref(syms, seed_since)

                wl_seed_fn = _wl_seed_since
            elif effective_seed:
                wl_seed_fn = self._seed_fn

            if self._publisher is not None and self._watchlist_controller is not None:
                pub = self._publisher
                watchlist_ctrl = self._watchlist_controller
                if wl_seed_fn is not None and watchlist_ctrl.seed_fn is None:
                    watchlist_ctrl.seed_fn = wl_seed_fn
            else:
                pub = MicroBatchPublisher(
                    name=self.name,
                    schema_spec=self.schema_spec,
                    symbol_normalizer=self.symbol_normalizer,
                    exec_ctx=self._exec_ctx,
                    flush_interval_ms=self._flush_interval_ms,
                    max_batch_size=self._max_batch_size,
                )
                self._publisher = pub
                pub.acquire_consumer()
                self._acquired_publisher = True
                watchlist_ctrl = ReactiveWatchlistController(
                    watchlist_table=self.symbols,  # type: ignore[arg-type]
                    ws_url=self.ws_url,
                    channel=self.ws_channel,
                    publisher=pub,
                    ws_multiplexer=self.ws_multiplexer,
                    symbol_normalizer=self.symbol_normalizer,
                    symbol_col=self._symbol_col,
                    exec_ctx=self._exec_ctx,
                    seed_fn=wl_seed_fn,
                    auto_start=False,
                )
                self._watchlist_controller = watchlist_ctrl
            wl_ref = watchlist_ctrl

            def _finalize_watchlist() -> None:
                wl_ref.start()
                if self._auto_flush:
                    pub.start()

            finalize_seed: Callable[[], None] = _finalize_watchlist
        else:
            canon_syms = self._canonical_symbol_list()
            if self.symbol_normalizer is not None:
                topics = [self.symbol_normalizer.ws_topic(self.ws_channel, s) for s in canon_syms]
            else:
                topics = [f"{self.ws_channel}.{s}" for s in canon_syms]
            self._static_topics = topics
            stream_key = (self.ws_url, self.ws_channel, tuple(canon_syms))
            self._stream_key = stream_key

            existing_pub = self._publisher or (
                self._stream_registry.get(stream_key) if self._stream_registry is not None else None
            )
            if existing_pub is not None:
                pub = existing_pub
                self._publisher = pub
                if not self._acquired_publisher:
                    pub.acquire_consumer()
                    self._acquired_publisher = True

                def _finalize_reused() -> None:
                    if seed and self._seed_fn is not None and not pub.has_snapshot_seed:
                        snap_rows = self._seed_fn(canon_syms)
                        pub.seed_late_snapshot(snap_rows, auto_flush=True)
                    if self._auto_flush:
                        pub.start()

                finalize_seed = _finalize_reused
            else:
                pub = MicroBatchPublisher(
                    name=self.name,
                    schema_spec=self.schema_spec,
                    symbol_normalizer=self.symbol_normalizer,
                    exec_ctx=self._exec_ctx,
                    flush_interval_ms=self._flush_interval_ms,
                    max_batch_size=self._max_batch_size,
                )
                self._publisher = pub
                pub.acquire_consumer()
                self._acquired_publisher = True
                if self._stream_registry is not None:
                    self._stream_registry[stream_key] = pub
                pub.set_allowed_symbols(canon_syms)

                pub.begin_seeding()
                self.ws_multiplexer.subscribe(self.ws_url, self.ws_channel, topics, pub)
                seed_fn_ref = self._seed_fn if effective_seed else None
                seed_rows_fn_ref = (
                    self._seed_rows_fn
                    if (seed_since is not None and self._seed_rows_fn is not None)
                    else None
                )
                if seed_fn_ref is not None or seed_rows_fn_ref is not None:
                    pub.mark_snapshot_seeded()

                def _finalize_static() -> None:
                    seed_rows: Sequence[Mapping[str, Any]] = ()
                    try:
                        if seed_rows_fn_ref is not None:
                            seed_rows = seed_rows_fn_ref(canon_syms, seed_since)
                        elif seed_fn_ref is not None:
                            seed_rows = seed_fn_ref(canon_syms)
                    finally:
                        pub.complete_seeding(seed_rows, seed_since=seed_since)
                    if self._auto_flush:
                        pub.start()

                finalize_seed = _finalize_static

        if self._on_register_lifecycle is not None:
            self._on_register_lifecycle(self)

        return pub, watchlist_ctrl, finalize_seed

    def latest(
        self,
        key_cols: Sequence[str] | None = None,
        *,
        by: str | Sequence[str] | None = None,
        seed: bool = True,
    ) -> Table:
        """Return a live updating current-state table (`last_by(key_cols)`), seeded before streaming.

        When `seed=True` (the default) and the feed supports REST seeding, the builder
        first buffers incoming WebSocket events, fetches the initial state via REST,
        deduplicates the buffered events against the REST seed, and flushes the merged
        batch only after the downstream `last_by` DAG node is registered.

        When `self.symbols` is a reactive Deephaven `Table` watchlist, rows for
        unsubscribed symbols are automatically evicted from the returned table.

        Args:
            key_cols: Optional sequence of grouping key columns for `last_by`. Defaults
                to `schema_spec.key_cols` (typically `("Symbol",)`).
            by: Convenient alias for `key_cols` accepting a single column name or sequence
                (for example, `by=["Symbol", "BidExchange"]`).
            seed: Whether to pre-populate the table from the REST snapshot endpoint
                before cutting over to live WebSocket ticks.

        Returns:
            A live updating Deephaven `Table` holding the latest row per key.

        Raises:
            NotImplementedError: If this feed does not support WebSocket streaming.
        """
        if by is not None and key_cols is None:
            key_cols = [by] if isinstance(by, str) else list(by)

        pub, watchlist_ctrl, finalize_seed = self._setup_streaming(seed=seed)
        effective_keys = list(key_cols) if key_cols is not None else list(self.schema_spec.key_cols)

        def _build() -> Table:
            base = (
                pub.blink_table.last_by(effective_keys)
                if effective_keys
                else pub.blink_table.last_by()
            )
            if watchlist_ctrl is not None:
                return (
                    base.natural_join(
                        watchlist_ctrl.active_subs_table,
                        on=["Symbol"],
                        joins=["SubscribedAt"],
                    )
                    .where(
                        [
                            "!isNull(SubscribedAt)",
                            "__ReceivedAt >= SubscribedAt",
                        ]
                    )
                    .drop_columns(["SubscribedAt", "__ReceivedAt"])
                )
            return base.drop_columns(["__ReceivedAt"])

        with exec_ctx_scope(self._exec_ctx):
            tbl = _build()
        finalize_seed()
        return tbl

    def _build_history_or_ring(
        self,
        *,
        seed: bool = False,
        seed_since: Any = None,
        capacity: int | None = None,
    ) -> Table:
        """Construct either an unbounded `blink_to_append_only` table or a bounded `ring_table`."""
        from deephaven import merge, ring_table
        from deephaven.stream import blink_to_append_only

        was_reused = self._publisher is not None or (
            not _is_deephaven_table(self.symbols)
            and self._stream_registry is not None
            and self.ws_url is not None
            and self.ws_channel is not None
            and (self.ws_url, self.ws_channel, tuple(self._canonical_symbol_list()))
            in self._stream_registry
        )
        pub, watchlist_ctrl, finalize_seed = self._setup_streaming(seed=seed, seed_since=seed_since)
        col_names = set(self.schema_spec.arrow_schema.names)
        if "Timestamp" in col_names:
            ts_expr = "Timestamp >= SubscribedAt || __ReceivedAt >= SubscribedAt"
        elif "WindowStart" in col_names:
            ts_expr = "WindowStart >= SubscribedAt || __ReceivedAt >= SubscribedAt"
        else:
            ts_expr = "__ReceivedAt >= SubscribedAt"

        def _build() -> Table:
            stream_tbl = (
                ring_table(pub.blink_table, capacity)
                if capacity is not None
                else blink_to_append_only(pub.blink_table)
            )
            if watchlist_ctrl is not None:
                return (
                    stream_tbl.natural_join(
                        watchlist_ctrl.active_subs_table,
                        on=["Symbol"],
                        joins=["SubscribedAt"],
                    )
                    .where(["!isNull(SubscribedAt)", ts_expr])
                    .drop_columns(["SubscribedAt", "__ReceivedAt"])
                )
            return stream_tbl.drop_columns(["__ReceivedAt"])

        tbl, cutoff_by_symbol = pub.attach_downstream(_build)
        finalize_seed()

        if (
            was_reused
            and watchlist_ctrl is None
            and seed_since is not None
            and self._seed_rows_fn is not None
        ):
            pub.mark_snapshot_seeded()
            raw_backfill = self._seed_rows_fn(self._canonical_symbol_list(), seed_since)
            backfill_rows = pub.filter_historical_backfill(
                raw_backfill,
                seed_since=seed_since,
                cutoff_by_symbol=cutoff_by_symbol,
            )
            if backfill_rows:
                rb = rows_to_record_batch(
                    backfill_rows,
                    self.schema_spec,
                    self.symbol_normalizer,
                    already_normalized=True,
                )
                pa_tbl = record_batches_to_arrow_table([rb], self.schema_spec)
                backfill_dh = arrow_to_deephaven_table(pa_tbl, self._exec_ctx)
                dedup_cols = [
                    c
                    for c in (
                        *self.schema_spec.key_cols,
                        "Timestamp",
                        "WindowStart",
                        "LastUpdated",
                        "TradeId",
                        "SequenceNumber",
                    )
                    if c in col_names
                ]
                with exec_ctx_scope(self._exec_ctx):
                    tbl = merge([backfill_dh.where_not_in(tbl, dedup_cols), tbl])
        return tbl

    def history(
        self,
        seed_since: Any = None,
        *,
        seed: bool = False,
    ) -> Table:
        """Return a live updating append-only history table (`blink_to_append_only`).

        Every incoming WebSocket tick (and optional startup seed batch) is appended
        in memory for the lifetime of the table. For high-throughput tick feeds where
        bounded memory is desired, consider `.ring(capacity)` instead.

        Args:
            seed_since: Optional start timestamp (`str`, `datetime`, or epoch `int`)
                from which to backfill historical rows via REST before cutting over to
                live WebSocket streaming without duplicates.
            seed: If `True` and `seed_since` is not provided, seeds the initial rows
                using the feed's default REST snapshot seed function.

        Returns:
            A live updating append-only Deephaven `Table`.

        Raises:
            NotImplementedError: If this feed does not support WebSocket streaming.
        """
        return self._build_history_or_ring(seed=seed, seed_since=seed_since, capacity=None)

    def ring(self, capacity: int = 100_000, *, seed: bool = False) -> Table:
        """Return a bounded live history table retaining the most recent `capacity` rows (`ring_table`).

        Args:
            capacity: Maximum number of rows retained in the ring table (must be > 0).
            seed: Whether to seed initial rows via REST before streaming begins.

        Returns:
            A live updating bounded Deephaven `ring_table`.

        Raises:
            ValueError: If `capacity <= 0`.
            NotImplementedError: If this feed does not support WebSocket streaming.
        """
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        return self._build_history_or_ring(seed=seed, seed_since=None, capacity=capacity)

    def blink(self) -> Table:
        """Return the raw stateless single-cycle Deephaven blink table.

        Blink tables retain rows only for the current Deephaven update-graph cycle and
        consume near-zero steady-state memory. They are ideal as inputs to downstream
        streaming aggregations (`update_by`, `ema_time`, `last_by`, etc.).

        Returns:
            A live Deephaven blink `Table` with internal metadata columns stripped.

        Raises:
            NotImplementedError: If this feed does not support WebSocket streaming.
        """
        pub, _, finalize_seed = self._setup_streaming(seed=False)
        finalize_seed()
        return pub.user_blink_table

    def _resolve_snapshot_fetch(self) -> Callable[[], Sequence[Mapping[str, Any]]] | None:
        """Resolve the callable used by `.snapshot()` and `.poll()`."""
        if self._snapshot_fn is not None:
            return self._snapshot_fn
        if self._seed_fn is not None:
            seed_fn_ref = self._seed_fn

            def _seed_snapshot() -> Sequence[Mapping[str, Any]]:
                syms = self._canonical_symbol_list()
                return seed_fn_ref(syms) if syms else []

            return _seed_snapshot
        return None

    def snapshot(self) -> Table:
        """Fetch a one-shot static point-in-time Deephaven `Table` via REST.

        Returns:
            A static Deephaven `Table` containing the current REST snapshot.

        Raises:
            NotImplementedError: If this feed does not support REST snapshots.
        """
        fetch = self._resolve_snapshot_fetch()
        if fetch is None:
            raise NotImplementedError(
                f"Feed '{self.name}' does not support .snapshot(). Use .historical() or .latest()."
            )

        rows = fetch()
        if not rows:
            pa_tbl = record_batches_to_arrow_table([], self.schema_spec)
        else:
            rb = rows_to_record_batch(rows, self.schema_spec, self.symbol_normalizer)
            pa_tbl = record_batches_to_arrow_table([rb], self.schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self._exec_ctx)

    def poll(
        self,
        interval_s: float = 60.0,
        *,
        mode: str = "replace",
        key_cols: Sequence[str] | None = None,
    ) -> Table:
        """Start a periodic background REST poller and return its live updating Deephaven `Table`.

        Args:
            interval_s: Seconds between background REST poll cycles (must be > 0).
            mode: How poll batches are materialized in Deephaven:
                - `"replace"`: Evicts keys absent from the latest poll cycle via a
                  monotonic `__PollId` join.
                - `"upsert"`: Retains all previously seen keys and updates matching rows
                  via `last_by(key_cols)`.
                - `"append"`: Appends every poll batch into an append-only history table.
            key_cols: Optional override for primary key columns used in `"replace"` and
                `"upsert"` modes.

        Returns:
            A live updating Deephaven `Table` driven by the background `SnapshotPoller`.

        Raises:
            NotImplementedError: If this feed does not support REST polling.
            ValueError: If `interval_s <= 0` or `mode` is not one of `"replace"`,
                `"upsert"`, or `"append"`.
        """
        fetch = self._resolve_snapshot_fetch()
        if fetch is None:
            raise NotImplementedError(f"Feed '{self.name}' does not support .poll().")

        if self._poller is not None:
            self._poller.stop()
            self._poller = None

        poller = SnapshotPoller(
            name=self.name,
            schema_spec=self.schema_spec,
            fetch_fn=fetch,
            interval_s=interval_s,
            mode=mode,
            key_cols=key_cols,
            symbol_normalizer=self.symbol_normalizer,
            exec_ctx=self._exec_ctx,
        )
        self._poller = poller
        poller.start(initial_poll=True)

        if self._on_register_lifecycle is not None:
            self._on_register_lifecycle(self)

        return poller.table

    def historical(
        self,
        start: Any = None,
        end: Any = None,
        *,
        limit: int | None = None,
        source: str = "auto",
    ) -> Table:
        """Fetch a static historical Deephaven `Table` via REST pagination or S3 Flat Files.

        Args:
            start: Optional start date (`"YYYY-MM-DD"`), ISO-8601 timestamp, `date`,
                `datetime`, or epoch number.
            end: Optional end date (`"YYYY-MM-DD"`), ISO-8601 timestamp, `date`,
                `datetime`, or epoch number.
            limit: Optional maximum number of rows or page size limit.
            source: Transport selection strategy:
                - `"auto"`: Uses S3 Flat Files when S3 credentials are configured and
                  either a wildcard (`"*"`) or at least `flat_file_ticker_threshold`
                  symbols are requested for a supported dataset; otherwise uses
                  concurrent REST pagination.
                - `"rest"`: Forces concurrent REST pagination.
                - `"flat_files"` / `"s3"`: Forces S3 `.csv.gz` Flat File streaming.

        Returns:
            A static Deephaven `Table` containing the historical dataset.

        Raises:
            NotImplementedError: If this feed does not support historical queries.
            ValueError: If `source` is invalid or required parameters are missing.
        """
        if self._historical_fn is not None:
            return self._historical_fn(start=start, end=end, limit=limit, source=source)
        if (
            start is None
            and end is None
            and (self._snapshot_fn is not None or self._seed_fn is not None)
        ):
            return self.snapshot()
        raise NotImplementedError(f"Feed '{self.name}' does not support .historical(start, end).")

    def stop(self) -> None:
        """Stop all background publishers, pollers, and watchlist listeners associated with this builder."""
        if self._watchlist_controller is not None:
            self._watchlist_controller.stop()
            self._watchlist_controller = None

        pub = self._publisher
        self._publisher = None
        if pub is not None and self._acquired_publisher:
            self._acquired_publisher = False
            if pub.release_consumer() == 0:
                if self._stream_registry is not None and self._stream_key is not None:
                    self._stream_registry.pop(self._stream_key, None)
                if (
                    self._static_topics
                    and self.ws_multiplexer is not None
                    and self.ws_url is not None
                    and self.ws_channel is not None
                ):
                    self.ws_multiplexer.unsubscribe(
                        self.ws_url,
                        self.ws_channel,
                        self._static_topics,
                        pub,
                    )
                pub.stop()

        self._stream_key = None
        self._static_topics = []

        if self._poller is not None:
            self._poller.stop()
            self._poller = None
