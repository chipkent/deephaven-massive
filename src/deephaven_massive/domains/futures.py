"""Futures (CME, CBOT, NYMEX, COMEX) domain client (`m.futures`)."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import BaseDomainClient, format_date_param
from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    record_batches_to_arrow_table,
    rows_to_record_batch,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    FUTURES_SNAPSHOT_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
)
from deephaven_massive.schemas.reference import (
    FUTURES_CONTRACTS_SCHEMA,
    FUTURES_PRODUCTS_SCHEMA,
    FUTURES_SCHEDULES_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class FuturesClient(BaseDomainClient):
    """Domain client for US Futures (`cme`, `cbot`, `nymex`, `comex`) streaming, snapshots, and reference data."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="futures")

    def _fetch_futures_snapshots(
        self, symbols: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["ticker.any_of"] = ",".join(symbols)
        return self.fetch_rest_rows("/futures/vX/snapshot", params)

    def _historical_futures(
        self,
        contracts: Any,
        schema_spec: SchemaSpec,
        s3_dataset: str | None,
        rest_path_fn: Any,
        *,
        start: Any = None,
        end: Any = None,
        limit: int | None = None,
        source: str = "auto",
    ) -> Table:
        canon_syms = (
            None
            if hasattr(contracts, "j_table") or hasattr(contracts, "column_names")
            else self.canonical_symbols(contracts)
        )
        if self.should_use_flat_files(source, canon_syms, s3_dataset):
            assert s3_dataset is not None
            return self.session.flat_files.load(
                s3_dataset,
                start=start,
                end=end,
                symbols=canon_syms,
                schema_spec=schema_spec,
                domain="futures",
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(
                "Specific futures contract(s) are required for REST historical queries."
            )

        if len(target_syms) == 1:
            sym = target_syms[0]
            path, params = rest_path_fn(sym, start, end)
            return self.fetch_chunked_table(
                path, schema_spec, params, limit=limit, inject_symbol=sym
            )

        all_rows: list[dict[str, Any]] = []
        for sym in target_syms:
            path, params = rest_path_fn(sym, start, end)
            all_rows.extend(
                self.fetch_rest_rows(path, params, limit=limit, inject_symbol=sym)
            )
        batches = (
            [rows_to_record_batch(all_rows, schema_spec, self.symbol_normalizer)]
            if all_rows
            else []
        )
        pa_tbl = record_batches_to_arrow_table(batches, schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def trades(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Futures trades (`T.*` on `/futures/{venue}`, `/futures/vX/trades`)."""
        ws_url = self.session.config.resolve_ws_url(
            "futures", feed_override=feed, venue=venue
        )

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/trades/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name=f"futures_trades_{venue.lower()}",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_futures_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_futures(
                contracts,
                TRADES_SCHEMA,
                "us_futures/trades_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def quotes(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Futures top-of-book quotes (`Q.*` on `/futures/{venue}`)."""
        ws_url = self.session.config.resolve_ws_url(
            "futures", feed_override=feed, venue=venue
        )

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/quotes/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name=f"futures_quotes_{venue.lower()}",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_futures_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_futures(
                contracts,
                QUOTES_SCHEMA,
                "us_futures/quotes_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def bars(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Futures OHLCV + settlement bars (`A.*` / `AM.*`, `/futures/vX/aggs`)."""
        clean_span = timespan.strip().lower()
        ws_channel = "A" if clean_span == "second" else "AM"
        ws_url = self.session.config.resolve_ws_url(
            "futures", feed_override=feed, venue=venue
        )

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_futures/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_futures/day_aggs_v1"

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/aggs/{sym}", {
                "resolution": f"{multiplier}_{clean_span}",
                "window_start.gte": format_date_param(start),
                "window_start.lte": format_date_param(end),
            }

        return self.build_feed(
            name=f"futures_bars_{venue.lower()}_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_futures_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_futures(
                contracts,
                BARS_SCHEMA,
                s3_ds,
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def snapshots(
        self, contracts: str | Iterable[str] | None = None
    ) -> FeedBuilder:
        """Snapshot or poll Futures contracts (`/futures/vX/snapshot`)."""
        canon = self.canonical_symbols(contracts) if contracts is not None else None
        return self.build_feed(
            name="futures_snapshots",
            schema_spec=FUTURES_SNAPSHOT_SCHEMA,
            symbols=contracts,
            snapshot_fn=lambda: self._fetch_futures_snapshots(canon),
        )

    def contracts(
        self, product_code: str | None = None, *, active: bool = True
    ) -> FeedBuilder:
        """Futures contract specifications, tick size, and expiration (`/futures/vX/contracts`)."""
        params = {
            "product_code": product_code.strip().upper()
            if product_code
            else None,
            "active": str(active).lower(),
        }
        return self.build_feed(
            name="futures_contracts",
            schema_spec=FUTURES_CONTRACTS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/futures/vX/contracts", params
            ),
        )

    def products(self, *, venue: str | None = None) -> FeedBuilder:
        """Futures product catalog (`/futures/vX/products`)."""
        params = {"market_sector": venue.upper() if venue else None}
        return self.build_feed(
            name="futures_products",
            schema_spec=FUTURES_PRODUCTS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/futures/vX/products", params
            ),
        )

    def schedules(self, product_code: str | None = None) -> FeedBuilder:
        """Trading session schedules by futures product and exchange (`/futures/vX/schedules`)."""
        params = {
            "product_code": product_code.strip().upper()
            if product_code
            else None
        }
        return self.build_feed(
            name="futures_schedules",
            schema_spec=FUTURES_SCHEDULES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/futures/vX/schedules", params
            ),
        )
