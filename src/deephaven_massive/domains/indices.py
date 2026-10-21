"""Market Indices domain client (`m.indices`).

This module implements :class:`IndicesClient`, which provides real-time and
delayed WebSocket streams, REST snapshots, historical backfills (via S3 Flat
Files or REST), reference directories, and technical indicators (`SMA`, `EMA`,
`MACD`, `RSI`) for US and global market indices (`I:SPX`, `I:NDX`, `I:VIX`,
`I:DJI`, etc.).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    _compute_bar_span_ns,
    _format_date_param,
    _normalize_filter_params,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    INDEX_VALUES_SCHEMA,
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
    """Domain client for Market Indices (`I:SPX`, `I:NDX`, `I:VIX`, etc.).

    User-supplied index symbols may be passed with or without the `"I:"`
    prefix (for example, `"SPX"` or `"I:SPX"`). ``IndicesClient`` normalizes
    every symbol to its canonical `"I:<TICKER>"` form across all tables and
    outbound subscriptions.

    Example:
        Stream live S&P 500 and VIX index values seeded with the current
        snapshot::

            idx = m.indices.values(["SPX", "VIX"]).latest()
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the Market Indices domain client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
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
        return self._fetch_rest_rows("/v3/snapshot/indices", params, extra_fields=extra_fields)

    def values(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Market Index tick values feed (`V.I:*`, `/v3/snapshot/indices`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_indices/values_v1`).

        Args:
            symbols: Index symbol (`"SPX"` or `"I:SPX"`), iterable of symbols,
                `"*"` for all indices, or a reactive Deephaven `Table`
                watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `INDEX_VALUES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("indices", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            raise ValueError(
                "Historical index tick values are only available via S3 Flat Files "
                "('us_indices/values_v1'); use m.indices.bars() for REST historical aggregates."
            )

        return self._build_feed(
            name="indices_values",
            schema_spec=INDEX_VALUES_SCHEMA,
            ws_url=ws_url,
            ws_channel="V",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_indices_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    symbols,
                    INDEX_VALUES_SCHEMA,
                    "us_indices/values_v1",
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source="flat_files" if source.strip().lower() == "auto" else source,
                    symbol_col=symbol_col,
                )
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
        """Configure a Market Index OHLC bars feed (`A.I:*` / `AM.I:*`, `/v2/aggs/...`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()` (second or
        minute aggregates), `.snapshot()`, `.poll()`, and `.historical()`
        (routed to S3 `us_indices/minute_aggs_v1` or `us_indices/day_aggs_v1`
        when `multiplier == 1`, or REST
        `/v2/aggs/ticker/{indicesTicker}/range/...`).

        Args:
            symbols: Index symbol (`"SPX"` or `"I:SPX"`), iterable of symbols,
                `"*"` for all indices, or a reactive Deephaven `Table`
                watchlist.
            multiplier: Number of `timespan` units per bar. Defaults to `1`.
            timespan: Bar duration unit (`"second"`, `"minute"`, `"hour"`,
                `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`). Defaults
                to `"minute"`.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `BARS_SCHEMA`.
        """
        clean_span = timespan.strip().lower()
        if multiplier == 1 and clean_span in ("second", "minute"):
            ws_channel: str | None = "A" if clean_span == "second" else "AM"
            ws_url: str | None = self.session.config._resolve_ws_url("indices", feed_override=feed)
        else:
            ws_channel = None
            ws_url = None

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_indices/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_indices/day_aggs_v1"

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            s_str = _format_date_param(start) or "1970-01-01"
            e_str = _format_date_param(end) or datetime.now(timezone.utc).date().isoformat()
            return (
                f"/v2/aggs/ticker/{sym}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {},
            )

        span_ns = _compute_bar_span_ns(multiplier, clean_span)
        bar_extra: dict[str, Any] = {
            "_span_ns": span_ns,
            "timespan": clean_span,
            "multiplier": multiplier,
        }
        return self._build_feed(
            name=f"indices_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_indices_snapshots(syms, extra_fields=bar_extra),
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields=bar_extra,
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    symbols,
                    BARS_SCHEMA,
                    s3_ds,
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                    symbol_col=symbol_col,
                )
            ),
        )

    def snapshots(self, symbols: str | Iterable[str] | None = None) -> FeedBuilder:
        """Configure a Market Index snapshots feed (`/v3/snapshot/indices`).

        Args:
            symbols: Optional index symbol or iterable of index symbols to
                filter. When `None`, returns snapshots for all indices.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `INDEX_VALUES_SCHEMA`.
        """
        canon = self._canonical_symbols(symbols) if symbols is not None else None
        return self._build_feed(
            name="indices_snapshots",
            schema_spec=INDEX_VALUES_SCHEMA,
            symbols=symbols,
            snapshot_fn=lambda: self._fetch_indices_snapshots(canon),
        )

    def previous_close(self, symbol: str) -> FeedBuilder:
        """Configure a feed for an Index's previous trading day OHLC bar (`/v2/aggs/ticker/{indicesTicker}/prev`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `BARS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self._build_feed(
            name=f"indices_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v2/aggs/ticker/{canon}/prev", inject_symbol=canon
            ),
        )

    previous_day_bar = previous_close

    def daily_open_close(self, symbol: str, target_date: str | date | datetime) -> FeedBuilder:
        """Configure a feed for an Index's daily open/close summary on `target_date` (`/v1/open-close/{indicesTicker}/{date}`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        d_str = _format_date_param(target_date)
        return self._build_feed(
            name=f"indices_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
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
        """Configure a Market Index reference directory feed (`/v3/reference/tickers?market=indices`).

        Args:
            active: If `True` (default), return only active index tickers.
            search: Optional keyword search query across index names/tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `TICKERS_SCHEMA`.
        """
        params = _normalize_filter_params(
            {
                "market": "indices",
                "active": str(active).lower(),
                "search": search,
            },
            filters,
        )
        return self._build_feed(
            name="indices_tickers",
            schema_spec=TICKERS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v3/reference/tickers", params),
        )

    def market_holidays(self) -> FeedBuilder:
        """Configure an upcoming market holidays and early-closes feed (`/v1/marketstatus/upcoming`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `HOLIDAYS_SCHEMA`.
        """
        return self._build_feed(
            name="indices_market_holidays",
            schema_spec=HOLIDAYS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v1/marketstatus/upcoming"),
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Configure a current index and market-group open/closed status feed (`/v1/marketstatus/now`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `MARKET_STATUS_SCHEMA`.
        """
        return self._build_feed(
            name="indices_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v1/marketstatus/now"),
        )

    def sma(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Simple Moving Average (`SMA`) indicator feed for an Index (`/v1/indicators/sma/{indicesTicker}`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
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
        """Configure an Exponential Moving Average (`EMA`) indicator feed for an Index (`/v1/indicators/ema/{indicesTicker}`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
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
        """Configure a Moving Average Convergence/Divergence (`MACD`) indicator feed for an Index (`/v1/indicators/macd/{indicesTicker}`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).
            short_window: Fast EMA period length. Defaults to `12`.
            long_window: Slow EMA period length. Defaults to `26`.
            signal_window: Signal line EMA period length. Defaults to `9`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
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
        """Configure a Relative Strength Index (`RSI`) indicator feed for an Index (`/v1/indicators/rsi/{indicesTicker}`).

        Args:
            symbol: Index symbol (e.g., `"SPX"` or `"I:SPX"`).
            window: Rolling lookback window length. Defaults to `14`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
        return self._indicator_feed(
            "RSI",
            symbol,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
