"""Market Indices domain client (`m.indices`) covering values, bars, snapshots, and technical indicators."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    compute_bar_span_ns,
    format_date_param,
    normalize_filter_params,
)
from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    INDEX_VALUES_SCHEMA,
    INDICATOR_SCHEMA,
)
from deephaven_massive.schemas.reference import (
    HOLIDAYS_SCHEMA,
    MARKET_STATUS_SCHEMA,
    TICKERS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class IndicesClient(BaseDomainClient):
    """Domain client for Market Indices (`I:SPX`, `I:NDX`, `I:VIX`, etc.)."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="indices")

    def _fetch_indices_snapshots(
        self,
        symbols: Sequence[str] | None = None,
        *,
        extra_fields: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["ticker.any_of"] = ",".join(
                self.symbol_normalizer.rest_ticker(s)  # type: ignore[union-attr]
                for s in symbols
            )
        return self.fetch_rest_rows(
            "/v3/snapshot/indices", params, extra_fields=extra_fields
        )

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
        return self.fetch_multi_symbol_historical(
            symbols,
            schema_spec,
            s3_dataset,
            rest_path_fn,
            empty_error="Specific index symbol(s) are required for REST historical queries.",
            start=start,
            end=end,
            limit=limit,
            source=source,
        )

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
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, _rest_path
            ),
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

        span_ns = compute_bar_span_ns(multiplier, clean_span)
        return self.build_feed(
            name=f"indices_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=lambda syms=None: self._fetch_indices_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
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

    previous_day_bar = previous_close

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

    daily_ticker_summary = daily_open_close

    def tickers(
        self,
        *,
        active: bool = True,
        search: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Index reference directory (`/v3/reference/tickers?market=indices`)."""
        params = normalize_filter_params(
            {
                "market": "indices",
                "active": str(active).lower(),
                "search": search,
            },
            filters,
        )
        return self.build_feed(
            name="indices_tickers",
            schema_spec=TICKERS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/tickers", params
            ),
        )

    def market_holidays(self) -> FeedBuilder:
        """Upcoming market holidays and early closes (`/v1/marketstatus/upcoming`)."""
        return self.build_feed(
            name="indices_market_holidays",
            schema_spec=HOLIDAYS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v1/marketstatus/upcoming"
            ),
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Current index and market group open/closed status (`/v1/marketstatus/now`)."""
        return self.build_feed(
            name="indices_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/v1/marketstatus/now"),
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

        def _fetch(
            start: Any = None,
            end: Any = None,
            limit: int | None = None,
            source: str = "auto",
        ) -> Table:
            q = dict(params)
            q["timestamp.gte"] = format_date_param(start)
            q["timestamp.lte"] = format_date_param(end)
            return self.fetch_chunked_table(
                f"/v1/indicators/{indicator.lower()}/{canon}",
                INDICATOR_SCHEMA,
                q,
                limit=limit,
                inject_symbol=canon,
                extra_fields=extra,
            )

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
            historical_fn=_fetch,
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
