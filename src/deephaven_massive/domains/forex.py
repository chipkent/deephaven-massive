"""Global Forex domain client (`m.forex`).

This module implements :class:`ForexClient`, which provides real-time WebSocket
streams (`quotes`, `bars`, `fmv`), REST snapshots, historical backfills (via S3
Flat Files or REST), real-time currency conversion, and technical indicators
for global foreign exchange pairs (`EUR/USD`, `GBP/JPY`, etc.).
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
    CURRENCY_CONVERSION_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
)
from deephaven_massive.schemas.reference import (
    EXCHANGES_SCHEMA,
    HOLIDAYS_SCHEMA,
    MARKET_STATUS_SCHEMA,
    TICKERS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive.session import MassiveSession


class ForexClient(BaseDomainClient):
    """Domain client for Global Forex (`EUR/USD`, `GBP/JPY`, etc.) feeds (`m.forex`).

    Currency pairs may be passed in `"EUR/USD"`, `"EUR-USD"`, `"C:EURUSD"`, or
    `"C:EUR-USD"` notation. ``ForexClient`` normalizes all pairs to the
    canonical `"BASE/QUOTE"` representation (e.g., `"EUR/USD"`) in every
    Deephaven table while translating to Massive's wire format (`"C.EUR/USD"`
    on WebSockets and `"C:EURUSD"` on REST/S3) automatically.

    Example:
        Stream live G10 currency BBO quotes in a `last_by` state table seeded with the latest snapshot:

        ```python
        fx = m.forex.quotes(["EUR/USD", "USD/JPY", "GBP/USD"]).latest()
        ```
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the Global Forex domain client.

        Args:
            session: Active [`MassiveSession`][deephaven_massive.session.MassiveSession].
        """
        super().__init__(session, domain="forex")

    def _fetch_forex_snapshots(
        self,
        pairs: Sequence[str] | None = None,
        *,
        extra_fields: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if pairs and "*" not in pairs:
            params["tickers"] = ",".join(
                self.symbol_normalizer.rest_ticker(p)  # type: ignore[union-attr]
                for p in pairs
            )
        return self._fetch_rest_rows(
            "/v2/snapshot/locale/global/markets/forex/tickers",
            params,
            extra_fields=extra_fields,
        )

    def quotes(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Forex BBO quotes feed (`C.*`, `/v3/quotes/{fxTicker}`).

        Supports `.latest(seed=True)`, `.history(seed=True)`, `.ring(capacity, seed=True)`,
        `.blink()`, `.snapshot()`, `.poll()`, and `.historical()` (backed by S3
        `global_forex/quotes_v1` or REST `/v3/quotes/{fxTicker}`).

        Args:
            pairs: Currency pair (`"EUR/USD"`), iterable of pairs, `"*"` for
                all Forex pairs, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `QUOTES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("forex", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            return f"/v3/quotes/{ticker}", {
                "timestamp.gte": _format_date_param(start),
                "timestamp.lte": _format_date_param(end),
            }

        return self._build_feed(
            name="forex_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="C",
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=self._fetch_forex_snapshots,
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    pairs,
                    QUOTES_SCHEMA,
                    "global_forex/quotes_v1",
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                    symbol_col=symbol_col,
                )
            ),
        )

    def bars(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        adjusted: bool = True,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Forex OHLCV bars feed (`CAS.*` / `CA.*`, `/v2/aggs/...`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()` (second or minute
        aggregates), `.snapshot()`, `.poll()`, and `.historical()` (routed to S3
        `global_forex/minute_aggs_v1` or `global_forex/day_aggs_v1` when
        `multiplier == 1`, or REST `/v2/aggs/...`).

        Args:
            pairs: Currency pair (`"EUR/USD"`), iterable of pairs, `"*"` for
                all Forex pairs, or a reactive Deephaven `Table` watchlist.
            multiplier: Number of `timespan` units per bar. Defaults to `1`.
            timespan: Bar duration unit (`"second"`, `"minute"`, `"hour"`,
                `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`). Defaults
                to `"minute"`.
            adjusted: Whether to adjust historical bars. Defaults to `True`.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `BARS_SCHEMA`.
        """
        clean_span = timespan.strip().lower()
        if multiplier == 1 and clean_span in ("second", "minute"):
            ws_channel: str | None = "CAS" if clean_span == "second" else "CA"
            ws_url: str | None = self.session.config._resolve_ws_url("forex", feed_override=feed)
        else:
            ws_channel = None
            ws_url = None

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "global_forex/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "global_forex/day_aggs_v1"

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            s_str = _format_date_param(start) or "1970-01-01"
            e_str = _format_date_param(end) or datetime.now(timezone.utc).date().isoformat()
            return (
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {"adjusted": str(adjusted).lower()},
            )

        span_ns = _compute_bar_span_ns(multiplier, clean_span)
        bar_extra: dict[str, Any] = {
            "_span_ns": span_ns,
            "timespan": clean_span,
            "multiplier": multiplier,
        }
        return self._build_feed(
            name=f"forex_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_forex_snapshots(syms, extra_fields=bar_extra),
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields=bar_extra,
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    pairs,
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

    def fmv(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Forex Fair Market Value (`FMV.*`) stream on `business.massive.com`.

        Args:
            pairs: Currency pair (`"EUR/USD"`), iterable of pairs, `"*"` for
                all Forex pairs, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.latest()`, `.history()`,
            `.ring()`, and `.blink()` with `FMV_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("forex", feed_override=feed, is_fmv=True)
        return self._build_feed(
            name="forex_fmv",
            schema_spec=FMV_SCHEMA,
            ws_url=ws_url,
            ws_channel="FMV",
            symbols=pairs,
            symbol_col=symbol_col,
        )

    def snapshots(
        self,
        pairs: str | Iterable[str] | None = None,
        *,
        direction: str | None = None,
    ) -> FeedBuilder:
        """Configure a Global Forex market snapshots feed (`/v2/snapshot/locale/global/markets/forex/...`).

        Args:
            pairs: Optional currency pair or iterable of pairs to filter.
                When `None` and `direction` is `None`, returns snapshots for
                all active currency pairs.
            direction: Optional top-movers filter (`"gainers"` or `"losers"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.

        Raises:
            ValueError: If `direction` is not `"gainers"` or `"losers"` when
                evaluated.
        """
        canon_syms = self._canonical_symbols(pairs) if pairs is not None else None

        def _fetch() -> list[dict[str, Any]]:
            if direction is not None:
                clean_dir = direction.strip().lower()
                if clean_dir not in ("gainers", "losers"):
                    raise ValueError("direction must be 'gainers' or 'losers'")
                return self._fetch_rest_rows(
                    f"/v2/snapshot/locale/global/markets/forex/{clean_dir}"
                )
            return self._fetch_forex_snapshots(canon_syms)

        return self._build_feed(
            name="forex_snapshots",
            schema_spec=STOCK_SNAPSHOT_SCHEMA,
            symbols=pairs,
            snapshot_fn=_fetch,
        )

    def top_movers(self, direction: str = "gainers") -> FeedBuilder:
        """Configure a top Forex market movers feed (`/v2/snapshot/locale/global/markets/forex/{direction}`).

        Args:
            direction: `"gainers"` (default) or `"losers"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.
        """
        return self.snapshots(direction=direction)

    def last_quote(self, from_currency: str, to_currency: str | None = None) -> FeedBuilder:
        """Configure a latest BBO quote feed for a currency pair (`/v1/last_quote/currencies/{from}/{to}`).

        Args:
            from_currency: Base currency (`"EUR"`) or full pair (`"EUR/USD"`
                when `to_currency` is omitted).
            to_currency: Optional quote currency (`"USD"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `QUOTES_SCHEMA`.
        """
        pair = f"{from_currency}/{to_currency}" if to_currency is not None else from_currency
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        fc, tc = canon.split("/")
        return self._build_feed(
            name=f"forex_last_quote_{fc}_{tc}",
            schema_spec=QUOTES_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v1/last_quote/currencies/{fc}/{tc}", inject_symbol=canon
            ),
        )

    def daily_market_summary(
        self, target_date: str | date | datetime, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Configure a grouped daily OHLCV summary feed for all Forex pairs on `target_date` (`/v2/aggs/grouped/locale/global/market/fx/{date}`).

        Args:
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        d_str = _format_date_param(target_date)
        return self._build_feed(
            name="forex_daily_market_summary",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v2/aggs/grouped/locale/global/market/fx/{d_str}",
                {"adjusted": str(adjusted).lower()},
                extra_fields={"Date": d_str},
            ),
        )

    def previous_close(self, pair: str, *, adjusted: bool = True) -> FeedBuilder:
        """Configure a feed for a Forex pair's previous trading day OHLCV bar (`/v2/aggs/ticker/{forexTicker}/prev`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `BARS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
        return self._build_feed(
            name=f"forex_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v2/aggs/ticker/{ticker}/prev",
                {"adjusted": str(adjusted).lower()},
                inject_symbol=canon,
            ),
        )

    previous_day_bar = previous_close

    def daily_open_close(
        self,
        pair: str,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a daily open/close summary feed for a Forex pair (`/v1/open-close/{forexTicker}/{date}`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
        d_str = _format_date_param(target_date)
        return self._build_feed(
            name=f"forex_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v1/open-close/{ticker}/{d_str}",
                {"adjusted": str(adjusted).lower()},
                inject_symbol=canon,
            ),
        )

    daily_ticker_summary = daily_open_close

    def convert(
        self,
        from_currency: str,
        to_currency: str,
        *,
        amount: float = 100.0,
        precision: int = 2,
    ) -> FeedBuilder:
        """Configure a real-time currency conversion feed (`/v1/conversion/{from}/{to}`).

        Args:
            from_currency: 3-letter ISO source currency code (e.g., `"USD"`).
            to_currency: 3-letter ISO target currency code (e.g., `"EUR"`).
            amount: Monetary amount of `from_currency` to convert. Defaults
                to `100.0`.
            precision: Decimal places for the converted amount. Defaults to `2`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `CURRENCY_CONVERSION_SCHEMA`.
        """
        fc = from_currency.strip().upper()
        tc = to_currency.strip().upper()
        return self._build_feed(
            name=f"forex_convert_{fc}_{tc}",
            schema_spec=CURRENCY_CONVERSION_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v1/conversion/{fc}/{tc}",
                {"amount": amount, "precision": precision},
            ),
        )

    convert_currency = convert

    def tickers(
        self,
        *,
        active: bool = True,
        search: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a Forex currency pairs reference directory feed (`/v3/reference/tickers?market=fx`).

        Args:
            active: If `True` (default), return only active currency pairs.
            search: Optional keyword search query across pair tickers/names.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `TICKERS_SCHEMA`.
        """
        params = _normalize_filter_params(
            {"market": "fx", "active": str(active).lower(), "search": search},
            filters,
        )
        return self._build_feed(
            name="forex_tickers",
            schema_spec=TICKERS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v3/reference/tickers", params),
        )

    def exchanges(self) -> FeedBuilder:
        """Configure a Forex institutional venue directory feed (`/v3/reference/exchanges?asset_class=fx`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `EXCHANGES_SCHEMA`.
        """
        return self._build_feed(
            name="forex_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                "/v3/reference/exchanges", {"asset_class": "fx"}
            ),
        )

    def market_holidays(self) -> FeedBuilder:
        """Configure an upcoming market holidays feed (`/v1/marketstatus/upcoming`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `HOLIDAYS_SCHEMA`.
        """
        return self._build_feed(
            name="forex_market_holidays",
            schema_spec=HOLIDAYS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v1/marketstatus/upcoming"),
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Configure a current Forex and currency market status feed (`/v1/marketstatus/now`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `MARKET_STATUS_SCHEMA`.
        """
        return self._build_feed(
            name="forex_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows("/v1/marketstatus/now"),
        )

    def sma(
        self,
        pair: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Simple Moving Average (`SMA`) indicator feed for a Forex pair (`/v1/indicators/sma/{forexTicker}`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
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
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def ema(
        self,
        pair: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure an Exponential Moving Average (`EMA`) indicator feed for a Forex pair (`/v1/indicators/ema/{forexTicker}`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
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
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def macd(
        self,
        pair: str,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Moving Average Convergence/Divergence (`MACD`) indicator feed for a Forex pair (`/v1/indicators/macd/{forexTicker}`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
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
            pair,
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
        pair: str,
        *,
        window: int = 14,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Relative Strength Index (`RSI`) indicator feed for a Forex pair (`/v1/indicators/rsi/{forexTicker}`).

        Args:
            pair: Currency pair (e.g., `"EUR/USD"` or `"C:EURUSD"`).
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
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
