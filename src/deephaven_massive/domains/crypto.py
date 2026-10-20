"""Global Crypto domain client (`m.crypto`).

This module implements :class:`CryptoClient`, which provides 24/7 real-time
WebSocket streams (`trades`, `quotes`, `bars`, `fmv`), REST snapshots,
historical backfills (via S3 Flat Files or REST), exchange directories, and
technical indicators for global cryptocurrency pairs (`BTC-USD`, `ETH-USD`,
`SOL-USD`, etc.).
"""

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
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
)
from deephaven_massive.schemas.reference import (
    CONDITIONS_SCHEMA,
    EXCHANGES_SCHEMA,
    HOLIDAYS_SCHEMA,
    MARKET_STATUS_SCHEMA,
    TICKERS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class CryptoClient(BaseDomainClient):
    """Domain client for Global Crypto (`BTC-USD`, `ETH-USD`, etc.) feeds (`m.crypto`).

    Cryptocurrency pairs may be passed in `"BTC-USD"`, `"BTC/USD"`,
    `"X:BTCUSD"`, or `"X:BTC-USD"` notation. ``CryptoClient`` normalizes all
    pairs to the canonical `"BASE-QUOTE"` representation (e.g., `"BTC-USD"`)
    in every Deephaven table while translating to Massive's wire format
    (`"XT.BTC-USD"` on WebSockets and `"X:BTCUSD"` on REST/S3) automatically.

    Example:
        Stream live Bitcoin and Ethereum trades seeded with the latest snapshot::

            crypto = m.crypto.trades(["BTC-USD", "ETH-USD"]).with_seed().stream()
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the Global Crypto domain client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
        super().__init__(session, domain="crypto")

    def _fetch_crypto_snapshots(
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
        return self.fetch_rest_rows(
            "/v2/snapshot/locale/global/markets/crypto/tickers",
            params,
            extra_fields=extra_fields,
        )

    def trades(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Crypto tick-by-tick trades feed (`XT.*`, `/v3/trades/{cryptoTicker}`).

        Supports `.stream()` (with optional `.with_seed()`), `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `global_crypto/trades_v1`
        or REST `/v3/trades/{cryptoTicker}`).

        Args:
            pairs: Crypto pair (`"BTC-USD"`), iterable of pairs, `"*"` for all
                Crypto pairs, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `TRADES_SCHEMA`.
        """
        ws_url = self.session.config.resolve_ws_url("crypto", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            return f"/v3/trades/{ticker}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name="crypto_trades",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="XT",
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=self._fetch_crypto_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, _rest_path
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_multi_symbol_historical(
                pairs,
                TRADES_SCHEMA,
                "global_crypto/trades_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def quotes(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Crypto BBO quotes feed (`XQ.*`).

        Args:
            pairs: Crypto pair (`"BTC-USD"`), iterable of pairs, `"*"` for all
                Crypto pairs, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.stream()` (with
            optional `.with_seed()`) with `QUOTES_SCHEMA`.
        """
        ws_url = self.session.config.resolve_ws_url("crypto", feed_override=feed)
        return self.build_feed(
            name="crypto_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="XQ",
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=self._fetch_crypto_snapshots,
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
        """Configure a Global Crypto OHLCV bars feed (`XAS.*` / `XA.*`, `/v2/aggs/...`).

        Supports `.stream()` (second or minute aggregates), `.snapshot()`,
        `.poll()`, and `.historical()` (routed to S3
        `global_crypto/minute_aggs_v1` or `global_crypto/day_aggs_v1` when
        `multiplier == 1`, or REST `/v2/aggs/...`).

        Args:
            pairs: Crypto pair (`"BTC-USD"`), iterable of pairs, `"*"` for all
                Crypto pairs, or a reactive Deephaven `Table` watchlist.
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
        ws_channel = "XAS" if clean_span == "second" else "XA"
        ws_url = self.session.config.resolve_ws_url("crypto", feed_override=feed)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "global_crypto/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "global_crypto/day_aggs_v1"

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            s_str = format_date_param(start) or "1970-01-01"
            e_str = format_date_param(end) or s_str
            return (
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {"adjusted": str(adjusted).lower()},
            )

        span_ns = compute_bar_span_ns(multiplier, clean_span)
        return self.build_feed(
            name=f"crypto_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=lambda syms=None: self._fetch_crypto_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_multi_symbol_historical(
                pairs,
                BARS_SCHEMA,
                s3_ds,
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def fmv(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a Global Crypto Fair Market Value (`FMV.*`) stream on `business.massive.com`.

        Args:
            pairs: Crypto pair (`"BTC-USD"`), iterable of pairs, `"*"` for all
                Crypto pairs, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `pairs` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.stream()` with
            `FMV_SCHEMA`.
        """
        ws_url = self.session.config.resolve_ws_url(
            "crypto", feed_override=feed, is_fmv=True
        )
        return self.build_feed(
            name="crypto_fmv",
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
        """Configure a Global Crypto market snapshots feed (`/v2/snapshot/locale/global/markets/crypto/...`).

        Args:
            pairs: Optional crypto pair or iterable of pairs to filter. When
                `None` and `direction` is `None`, returns snapshots for all
                active crypto pairs.
            direction: Optional top-movers filter (`"gainers"` or `"losers"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.

        Raises:
            ValueError: If `direction` is not `"gainers"` or `"losers"` when
                evaluated.
        """
        canon_syms = self.canonical_symbols(pairs) if pairs is not None else None

        def _fetch() -> list[dict[str, Any]]:
            if direction is not None:
                clean_dir = direction.strip().lower()
                if clean_dir not in ("gainers", "losers"):
                    raise ValueError("direction must be 'gainers' or 'losers'")
                return self.fetch_rest_rows(
                    f"/v2/snapshot/locale/global/markets/crypto/{clean_dir}"
                )
            return self._fetch_crypto_snapshots(canon_syms)

        return self.build_feed(
            name="crypto_snapshots",
            schema_spec=STOCK_SNAPSHOT_SCHEMA,
            symbols=pairs,
            snapshot_fn=_fetch,
        )

    def top_movers(self, direction: str = "gainers") -> FeedBuilder:
        """Configure a top Crypto market movers feed (`/v2/snapshot/locale/global/markets/crypto/{direction}`).

        Args:
            direction: `"gainers"` (default) or `"losers"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.
        """
        return self.snapshots(direction=direction)

    def last_trade(
        self, from_currency: str, to_currency: str | None = None
    ) -> FeedBuilder:
        """Configure a latest trade feed for a Crypto currency pair (`/v1/last/crypto/{from}/{to}`).

        Args:
            from_currency: Base asset (`"BTC"`) or full pair (`"BTC-USD"` when
                `to_currency` is omitted).
            to_currency: Optional quote asset (`"USD"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `TRADES_SCHEMA`.
        """
        pair = (
            f"{from_currency}-{to_currency}"
            if to_currency is not None
            else from_currency
        )
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        fc, tc = canon.split("-")
        return self.build_feed(
            name=f"crypto_last_trade_{fc}_{tc}",
            schema_spec=TRADES_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/last/crypto/{fc}/{tc}", inject_symbol=canon
            ),
        )

    def daily_market_summary(
        self, target_date: str | date | datetime, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Configure a grouped daily OHLCV summary feed for all Crypto pairs on `target_date` (`/v2/aggs/grouped/locale/global/market/crypto/{date}`).

        Args:
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        d_str = format_date_param(target_date)
        return self.build_feed(
            name="crypto_daily_market_summary",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v2/aggs/grouped/locale/global/market/crypto/{d_str}",
                {"adjusted": str(adjusted).lower()},
                extra_fields={"Date": d_str},
            ),
        )

    def previous_close(
        self, pair: str, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Configure a feed for a Crypto pair's previous day OHLCV bar (`/v2/aggs/ticker/{cryptoTicker}/prev`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `BARS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
        return self.build_feed(
            name=f"crypto_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Configure a daily open/close summary feed for a Crypto pair (`/v1/open-close/crypto/{from}/{to}/{date}`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        base, quote = canon.split("-")
        d_str = format_date_param(target_date)
        return self.build_feed(
            name=f"crypto_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/open-close/crypto/{base}/{quote}/{d_str}",
                {"adjusted": str(adjusted).lower()},
                inject_symbol=canon,
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
        """Configure a Crypto currency pairs reference directory feed (`/v3/reference/tickers?market=crypto`).

        Args:
            active: If `True` (default), return only active crypto pairs.
            search: Optional keyword search query across pair tickers/names.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `TICKERS_SCHEMA`.
        """
        params = normalize_filter_params(
            {
                "market": "crypto",
                "active": str(active).lower(),
                "search": search,
            },
            filters,
        )
        return self.build_feed(
            name="crypto_tickers",
            schema_spec=TICKERS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/tickers", params
            ),
        )

    def conditions(self) -> FeedBuilder:
        """Configure a Crypto trade and quote condition codes feed (`/v3/reference/conditions?asset_class=crypto`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `CONDITIONS_SCHEMA`.
        """
        return self.build_feed(
            name="crypto_conditions",
            schema_spec=CONDITIONS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/conditions", {"asset_class": "crypto"}
            ),
        )

    def exchanges(self) -> FeedBuilder:
        """Configure a Crypto exchange directory feed (`/v3/reference/exchanges?asset_class=crypto`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `EXCHANGES_SCHEMA`.
        """
        return self.build_feed(
            name="crypto_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/exchanges", {"asset_class": "crypto"}
            ),
        )

    def market_holidays(self) -> FeedBuilder:
        """Configure an upcoming market holidays feed (`/v1/marketstatus/upcoming`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `HOLIDAYS_SCHEMA`.
        """
        return self.build_feed(
            name="crypto_market_holidays",
            schema_spec=HOLIDAYS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v1/marketstatus/upcoming"
            ),
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Configure a current Crypto and currency market status feed (`/v1/marketstatus/now`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `MARKET_STATUS_SCHEMA`.
        """
        return self.build_feed(
            name="crypto_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/v1/marketstatus/now"),
        )

    def sma(
        self,
        pair: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Simple Moving Average (`SMA`) indicator feed for a Crypto pair (`/v1/indicators/sma/{cryptoTicker}`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATORS_SCHEMA`.
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
        """Configure an Exponential Moving Average (`EMA`) indicator feed for a Crypto pair (`/v1/indicators/ema/{cryptoTicker}`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATORS_SCHEMA`.
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
        """Configure a Moving Average Convergence/Divergence (`MACD`) indicator feed for a Crypto pair (`/v1/indicators/macd/{cryptoTicker}`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            short_window: Fast EMA period length. Defaults to `12`.
            long_window: Slow EMA period length. Defaults to `26`.
            signal_window: Signal line EMA period length. Defaults to `9`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATORS_SCHEMA`.
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
        """Configure a Relative Strength Index (`RSI`) indicator feed for a Crypto pair (`/v1/indicators/rsi/{cryptoTicker}`).

        Args:
            pair: Crypto pair (e.g., `"BTC-USD"` or `"X:BTCUSD"`).
            window: Rolling lookback window length. Defaults to `14`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATORS_SCHEMA`.
        """
        return self._indicator_feed(
            "RSI",
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
