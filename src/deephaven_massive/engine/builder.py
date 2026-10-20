"""Fluent FeedBuilder unifying WebSocket streaming, REST polling, snapshots, and historical tables."""

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
    """Return True if `obj` is a Deephaven `Table` or `InputTable` instance."""
    if obj is None or isinstance(obj, (str, bytes, list, tuple, set, frozenset, dict)):
        return False
    return hasattr(obj, "j_table") or hasattr(obj, "column_names")


class FeedBuilder:
    """Fluent builder that materializes a Massive data feed as a Deephaven :class:`Table`.

    Terminal methods:
    - :meth:`latest`: Live updating current-state table (`last_by(key_cols)`), optionally seeded via REST.
    - :meth:`history`: Live updating in-memory append-only history table (`blink_to_append_only`).
    - :meth:`ring`: Bounded live history table retaining the most recent `capacity` rows (`.tail(capacity)`).
    - :meth:`blink`: Raw stateless single-cycle Deephaven blink table.
    - :meth:`snapshot`: One-shot static point-in-time Deephaven table fetched via REST.
    - :meth:`poll`: Periodic background REST poller with ``mode="replace" | "upsert" | "append"``.
    - :meth:`historical`: Static historical table fetched via REST pagination or S3 Flat Files.
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
        snapshot_fn: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
        historical_fn: Callable[..., Table] | None = None,
        flush_interval_ms: int = 50,
        max_batch_size: int = 10_000,
        auto_flush: bool = True,
        on_register_lifecycle: Callable[[FeedBuilder], None] | None = None,
    ) -> None:
        self.name = name
        self.schema_spec = schema_spec
        self.symbol_normalizer = symbol_normalizer
        self._exec_ctx = exec_ctx
        self.ws_multiplexer = ws_multiplexer
        self.ws_url = ws_url
        self.ws_channel = ws_channel
        self.symbols = symbols
        self.symbol_col = symbol_col
        self.seed_fn = seed_fn
        self.snapshot_fn = snapshot_fn
        self.historical_fn = historical_fn
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_size = max_batch_size
        self.auto_flush = auto_flush
        self._on_register_lifecycle = on_register_lifecycle

        self._publisher: MicroBatchPublisher | None = None
        self._poller: SnapshotPoller | None = None
        self._watchlist_controller: ReactiveWatchlistController | None = None
        self._static_topics: list[str] = []

    @property
    def publisher(self) -> MicroBatchPublisher | None:
        """Return the active :class:`MicroBatchPublisher`, if started."""
        return self._publisher

    @property
    def poller(self) -> SnapshotPoller | None:
        """Return the active :class:`SnapshotPoller`, if started."""
        return self._poller

    @property
    def watchlist_controller(self) -> ReactiveWatchlistController | None:
        """Return the active :class:`ReactiveWatchlistController`, if started."""
        return self._watchlist_controller

    def _canonical_symbol_list(self) -> list[str]:
        if self.symbols is None:
            return ["*"]
        if self.symbol_normalizer is not None:
            return self.symbol_normalizer.canonical_list(self.symbols)  # type: ignore[arg-type]
        if isinstance(self.symbols, str):
            return [self.symbols.strip().upper()]
        return [str(s).strip().upper() for s in self.symbols]  # type: ignore[union-attr]

    def _setup_streaming(
        self, *, seed: bool
    ) -> tuple[
        MicroBatchPublisher,
        ReactiveWatchlistController | None,
        Callable[[], None],
    ]:
        if (
            self.ws_url is None
            or self.ws_channel is None
            or self.ws_multiplexer is None
        ):
            raise NotImplementedError(
                f"Feed '{self.name}' does not support WebSocket streaming. "
                "Use .snapshot(), .poll(), or .historical() instead."
            )

        pub = MicroBatchPublisher(
            name=self.name,
            schema_spec=self.schema_spec,
            symbol_normalizer=self.symbol_normalizer,
            exec_ctx=self._exec_ctx,
            flush_interval_ms=self.flush_interval_ms,
            max_batch_size=self.max_batch_size,
        )
        self._publisher = pub

        watchlist_ctrl: ReactiveWatchlistController | None = None
        finalize_seed: Callable[[], None] = lambda: None

        if _is_deephaven_table(self.symbols):
            assert self.symbol_normalizer is not None
            watchlist_ctrl = ReactiveWatchlistController(
                watchlist_table=self.symbols,  # type: ignore[arg-type]
                ws_url=self.ws_url,
                channel=self.ws_channel,
                publisher=pub,
                ws_multiplexer=self.ws_multiplexer,
                symbol_normalizer=self.symbol_normalizer,
                symbol_col=self.symbol_col,
                exec_ctx=self._exec_ctx,
                seed_fn=self.seed_fn if seed else None,
            )
            self._watchlist_controller = watchlist_ctrl
        else:
            canon_syms = self._canonical_symbol_list()
            pub.set_allowed_symbols(canon_syms)
            if self.symbol_normalizer is not None:
                topics = [
                    self.symbol_normalizer.ws_topic(self.ws_channel, s)
                    for s in canon_syms
                ]
            else:
                topics = [f"{self.ws_channel}.{s}" for s in canon_syms]
            self._static_topics = topics

            if seed and self.seed_fn is not None:
                pub.begin_seeding()
                self.ws_multiplexer.subscribe(
                    self.ws_url, self.ws_channel, topics, pub
                )
                seed_fn_ref = self.seed_fn

                def _do_seed() -> None:
                    seed_rows = seed_fn_ref(canon_syms)
                    pub.complete_seeding(seed_rows)

                finalize_seed = _do_seed
            else:
                self.ws_multiplexer.subscribe(
                    self.ws_url, self.ws_channel, topics, pub
                )

        if self.auto_flush:
            pub.start()

        if self._on_register_lifecycle is not None:
            self._on_register_lifecycle(self)

        return pub, watchlist_ctrl, finalize_seed

    def latest(
        self,
        key_cols: Sequence[str] | None = None,
        *,
        seed: bool = True,
    ) -> Table:
        """Return a live updating current-state table (`last_by(key_cols)`), seeded before streaming."""
        pub, watchlist_ctrl, finalize_seed = self._setup_streaming(seed=seed)
        effective_keys = (
            list(key_cols)
            if key_cols is not None
            else list(self.schema_spec.key_cols)
        )

        def _build() -> Table:
            base = (
                pub.user_blink_table.last_by(effective_keys)
                if effective_keys
                else pub.user_blink_table.last_by()
            )
            if watchlist_ctrl is not None:
                base = (
                    base.natural_join(
                        watchlist_ctrl.active_subs_table,
                        on=["Symbol"],
                        joins=["SubscribedAt"],
                    )
                    .where("!isNull(SubscribedAt)")
                    .drop_columns(["SubscribedAt"])
                )
            return base

        with exec_ctx_scope(self._exec_ctx):
            tbl = _build()
        finalize_seed()
        return tbl

    def history(self, *, seed: bool = False) -> Table:
        """Return a live updating append-only history table (`blink_to_append_only`)."""
        from deephaven.stream import blink_to_append_only

        pub, watchlist_ctrl, finalize_seed = self._setup_streaming(seed=seed)
        col_names = set(self.schema_spec.arrow_schema.names)
        if "Timestamp" in col_names:
            ts_expr = "Timestamp >= SubscribedAt || __ReceivedAt >= SubscribedAt"
        elif "WindowStart" in col_names:
            ts_expr = "WindowStart >= SubscribedAt || __ReceivedAt >= SubscribedAt"
        else:
            ts_expr = "__ReceivedAt >= SubscribedAt"

        def _build() -> Table:
            append_tbl = blink_to_append_only(pub.blink_table)
            if watchlist_ctrl is not None:
                return (
                    append_tbl.natural_join(
                        watchlist_ctrl.active_subs_table,
                        on=["Symbol"],
                        joins=["SubscribedAt"],
                    )
                    .where(f"!isNull(SubscribedAt) && ({ts_expr})")
                    .drop_columns(["SubscribedAt", "__ReceivedAt"])
                )
            return append_tbl.drop_columns(["__ReceivedAt"])

        with exec_ctx_scope(self._exec_ctx):
            tbl = _build()
        finalize_seed()
        return tbl

    def ring(self, capacity: int = 100_000, *, seed: bool = False) -> Table:
        """Return a bounded live history table retaining the most recent `capacity` rows."""
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        hist = self.history(seed=seed)
        with exec_ctx_scope(self._exec_ctx):
            return hist.tail(capacity)

    def blink(self) -> Table:
        """Return the raw stateless single-cycle Deephaven blink table."""
        pub, _, _ = self._setup_streaming(seed=False)
        return pub.user_blink_table

    def snapshot(self) -> Table:
        """Fetch a one-shot static point-in-time Deephaven table via REST."""
        fetch = self.snapshot_fn
        if fetch is None and self.seed_fn is not None:
            canon_syms = self._canonical_symbol_list()
            fetch = lambda: self.seed_fn(canon_syms)  # type: ignore[misc]
        if fetch is None:
            raise NotImplementedError(
                f"Feed '{self.name}' does not support .snapshot(). Use .historical() or .latest()."
            )

        rows = fetch()
        if not rows:
            pa_tbl = record_batches_to_arrow_table([], self.schema_spec)
        else:
            rb = rows_to_record_batch(
                rows, self.schema_spec, self.symbol_normalizer
            )
            pa_tbl = record_batches_to_arrow_table([rb], self.schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self._exec_ctx)

    def poll(
        self,
        interval_s: float = 60.0,
        *,
        mode: str = "replace",
        key_cols: Sequence[str] | None = None,
    ) -> Table:
        """Start a periodic background REST poller and return its live updating Deephaven table."""
        fetch = self.snapshot_fn
        if fetch is None and self.seed_fn is not None:
            canon_syms = self._canonical_symbol_list()
            fetch = lambda: self.seed_fn(canon_syms)  # type: ignore[misc]
        if fetch is None:
            raise NotImplementedError(
                f"Feed '{self.name}' does not support .poll()."
            )

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
        """Fetch a static historical Deephaven table via REST pagination or S3 Flat Files."""
        if self.historical_fn is not None:
            return self.historical_fn(
                start=start, end=end, limit=limit, source=source
            )
        if start is None and end is None and (
            self.snapshot_fn is not None or self.seed_fn is not None
        ):
            return self.snapshot()
        raise NotImplementedError(
            f"Feed '{self.name}' does not support .historical(start, end)."
        )

    def stop(self) -> None:
        """Stop all background publishers, pollers, and watchlist listeners associated with this feed."""
        if self._watchlist_controller is not None:
            self._watchlist_controller.stop()
            self._watchlist_controller = None

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
                self._publisher,
            )
            self._static_topics = []

        if self._publisher is not None:
            self._publisher.stop()
            self._publisher = None

        if self._poller is not None:
            self._poller.stop()
            self._poller = None
