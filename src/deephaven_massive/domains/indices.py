"""Market Indices domain client (`m.indices`) covering values, bars, snapshots, and technical indicators."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime
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
    DAILY_SUMMARY_SCHEMA,
    INDEX_VALUES_SCHEMA,
    INDICATOR_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class IndicesClient(BaseDomainClient):
    """Domain client for Market Indices (`I:SPX`, `I:NDX`, `I:VIX`, etc.)."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="indices")

    def _fetch_indices_snapshots(
        self, symbols: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["ticker.any_of"] = ",".join(
                self.symbol_normalizer.rest_ticker(s)  # type: ignore[union-attr]
                for s in symbols
            )
        return self.fetch_rest_rows("/v3/snapshot/indices", params)

    def _historical_indices(
        self,
        symbols: Any,
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
            if hasattr(symbols, "j_table") or hasattr(symbols, "column_names")
            else self.canonical_symbols(symbols)
        )
        if self.should_use_flat_files(source, canon_syms, s3_dataset):
            assert s3_dataset is not None
            return self.session.flat_files.load(
                s3_dataset,
                start=start,
                end=end,
                symbols=canon_syms,
                schema_spec=schema_spec,
                domain="indices",
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(
                "Specific index symbol(s) are required for REST historical queries."
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

    def values(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Index values (`V.I:*`, `/v3/snapshot/indices`, `us_indices/values_v1`)."""
        ws_url = self.session.config.resolve_ws_url("indices", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return "/v3/snapshot/indices", {"ticker.any_of": sym}

        return self.build_feed(
            name="indices_values",
            schema_spec=INDEX_VALUES_SCHEMA,
            ws_url=ws_url,
            ws_channel="V",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_indices_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_indices(
                symbols,
                INDEX_VALUES_SCHEMA,
                "us_indices/values_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def bars(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Index bars (`A.I:*` / `AM.I:*`, `/v2/aggs/...`)."""
        clean_span = timespan.strip().lower()
        ws_channel = "A" if clean_span == "second" else "AM"
        ws_url = self.session.config.resolve_ws_url("indices", feed_override=feed)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_indices/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_indices/day_aggs_v1"

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            s_str = format_date_param(start) or "1970-01-01"
            e_str = format_date_param(end) or s_str
            return (
                f"/v2/aggs/ticker/{sym}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {},
            )

        return self.build_feed(
            name=f"indices_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_indices_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_indices(
                symbols,
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
        self, symbols: str | Iterable[str] | None = None
    ) -> FeedBuilder:
        """Snapshot or poll Index values and daily sessions (`/v3/snapshot/indices`)."""
        canon = self.canonical_symbols(symbols) if symbols is not None else None
        return self.build_feed(
            name="indices_snapshots",
            schema_spec=INDEX_VALUES_SCHEMA,
            symbols=symbols,
            snapshot_fn=lambda: self._fetch_indices_snapshots(canon),
        )

    def previous_close(self, symbol: str) -> FeedBuilder:
        """Previous day's OHLC bar for an Index (`/v2/aggs/ticker/{indicesTicker}/prev`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self.build_feed(
            name=f"indices_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v2/aggs/ticker/{canon}/prev", inject_symbol=canon
            ),
        )

    def daily_open_close(
        self, symbol: str, target_date: str | date | datetime
    ) -> FeedBuilder:
        """Daily open/close for an Index (`/v1/open-close/{indicesTicker}/{date}`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        d_str = format_date_param(target_date)
        return self.build_feed(
            name=f"indices_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/open-close/{canon}/{d_str}", inject_symbol=canon
            ),
        )

    def _indicator_feed(
        self,
        indicator: str,
        symbol: str,
        params: dict[str, Any],
        window: int,
        timespan: str,
        series_type: str,
    ) -> FeedBuilder:
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        extra = {
            "Indicator": indicator.upper(),
            "Window": window,
            "Timespan": timespan,
            "SeriesType": series_type,
        }
        return self.build_feed(
            name=f"indices_{indicator.lower()}_{canon}",
            schema_spec=INDICATOR_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/indicators/{indicator.lower()}/{canon}",
                params,
                inject_symbol=canon,
                extra_fields=extra,
            ),
        )

    def sma(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """SMA indicator for an Index."""
        return self._indicator_feed(
            "SMA",
            symbol,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def ema(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """EMA indicator for an Index."""
        return self._indicator_feed(
            "EMA",
            symbol,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def macd(
        self,
        symbol: str,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """MACD indicator for an Index."""
        return self._indicator_feed(
            "MACD",
            symbol,
            {
                "short_window": short_window,
                "long_window": long_window,
                "signal_window": signal_window,
                "timespan": timespan,
                "series_type": series_type,
            },
            short_window,
            timespan,
            series_type,
        )

    def rsi(
        self,
        symbol: str,
        *,
        window: int = 14,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """RSI indicator for an Index."""
        return self._indicator_feed(
            "RSI",
            symbol,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
