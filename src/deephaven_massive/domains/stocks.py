"""US Stocks domain client (`m.stocks`).

This module implements :class:`StocksClient`, which provides complete coverage
of Massive's US Equities (`SIP` and `Business` tiers):

- **Real-Time & Delayed Market Data**: Trades (`T.*`), NBBO quotes (`Q.*`),
  second and minute OHLCV bars (`A.*` / `AM.*`), Limit Up - Limit Down bands
  (`LULD.*`), Net Order Imbalances (`NOI.*`), and Fair Market Value (`FMV.*`).
- **Snapshots & Summaries**: Full-market and filtered snapshots, top gainers
  and losers, grouped daily summaries, previous close, and daily open/close.
- **Technical Indicators**: Server-computed `SMA`, `EMA`, `MACD`, and `RSI`.
- **Fundamentals & Corporate Actions**: Balance sheets, income statements,
  cash flow statements, financial ratios, short interest, short volume, share
  float, dividends, stock splits, and IPO calendars.
- **SEC EDGAR & Reference**: EDGAR filings index, 10-K sections, 8-K text,
  risk factors, structured disclosures, 13F institutional holdings, Form 3/4/5
  insider transactions, ticker directory, profiles, rebranding events, peer
  companies, news with sentiment, exchanges, conditions, and market status.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    _format_date_param,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    IMBALANCES_SCHEMA,
    LULD_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
    _compute_bar_span_ns,
)
from deephaven_massive.schemas.reference import (
    CONDITIONS_SCHEMA,
    DIVIDENDS_SCHEMA,
    EXCHANGES_SCHEMA,
    FINANCIAL_STATEMENT_SCHEMA,
    HOLIDAYS_SCHEMA,
    IPOS_SCHEMA,
    MARKET_STATUS_SCHEMA,
    NEWS_SCHEMA,
    RATIOS_SCHEMA,
    RELATED_COMPANIES_SCHEMA,
    SEC_13F_SCHEMA,
    SEC_FILINGS_SCHEMA,
    SEC_INSIDER_SCHEMA,
    SHARE_FLOAT_SCHEMA,
    SHORT_INTEREST_SCHEMA,
    SHORT_VOLUME_SCHEMA,
    SPLITS_SCHEMA,
    TICKER_DETAILS_SCHEMA,
    TICKER_EVENTS_SCHEMA,
    TICKER_TYPES_SCHEMA,
    TICKERS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive.session import MassiveSession


class StocksClient(BaseDomainClient):
    """Domain client for US Equities (`SIP` / `OTC`) feeds (`m.stocks`).

    ``StocksClient`` normalizes all equity tickers to uppercase strings (for
    example, ``"aapl"`` -> ``"AAPL"``, ``"brk.b"`` -> ``"BRK.B"``) and returns
    a :class:`~deephaven_massive.engine.builder.FeedBuilder` from every method
    so you can choose `.latest()`, `.history()`, `.ring()`, `.blink()`,
    `.snapshot()`, `.poll()`, or `.historical()` with a uniform, chainable API.

    Example:
        Stream live US Stock trades and NBBO quotes for a reactive watchlist
        of today's top gainers::

            gainers = m.stocks.gainers().poll(interval_s=30.0)
            trades = m.stocks.trades(gainers).latest()
            quotes = m.stocks.quotes(gainers).latest()
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the US Stocks domain client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
        super().__init__(session, domain="stocks")

    def _fetch_stock_snapshots(
        self,
        symbols: Sequence[str] | None = None,
        *,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["tickers"] = ",".join(
                self.symbol_normalizer.rest_ticker(s)  # type: ignore[union-attr]
                for s in symbols
            )
        return self._fetch_rest_rows(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params,
            extra_fields=extra_fields,
        )

    def trades(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock SIP tick-by-tick trades feed (`T.*`, `/v3/trades/{ticker}`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_stocks_sip/trades_v1`
        or REST `/v3/trades/{ticker}`).

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `TRADES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("stocks", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            return f"/v3/trades/{ticker}", {
                "timestamp.gte": _format_date_param(start),
                "timestamp.lte": _format_date_param(end),
            }

        return self._build_feed(
            name="stocks_trades",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_stock_snapshots,
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    symbols,
                    TRADES_SCHEMA,
                    "us_stocks_sip/trades_v1",
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                    symbol_col=symbol_col,
                )
            ),
        )

    def quotes(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock SIP NBBO quotes feed (`Q.*`, `/v3/quotes/{ticker}`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_stocks_sip/quotes_v1`
        or REST `/v3/quotes/{ticker}`).

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `QUOTES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("stocks", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            return f"/v3/quotes/{ticker}", {
                "timestamp.gte": _format_date_param(start),
                "timestamp.lte": _format_date_param(end),
            }

        return self._build_feed(
            name="stocks_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_stock_snapshots,
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    symbols,
                    QUOTES_SCHEMA,
                    "us_stocks_sip/quotes_v1",
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
        symbols: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        adjusted: bool = True,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock OHLCV bars feed (`A.*` / `AM.*`, `/v2/aggs/...`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()` (second or
        minute aggregates), `.snapshot()`, `.poll()`, and `.historical()`
        (routed to S3 `us_stocks_sip/minute_aggs_v1` or
        `us_stocks_sip/day_aggs_v1` when `multiplier == 1` and
        `adjusted is True`, or REST `/v2/aggs/ticker/{ticker}/range/...`).

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            multiplier: Number of `timespan` units per bar. Defaults to `1`.
            timespan: Bar duration unit (`"second"`, `"minute"`, `"hour"`,
                `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`). Defaults
                to `"minute"`.
            adjusted: Whether to adjust historical bars for splits. Defaults
                to `True`.
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
            ws_url: str | None = self.session.config._resolve_ws_url("stocks", feed_override=feed)
        else:
            ws_channel = None
            ws_url = None

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_stocks_sip/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_stocks_sip/day_aggs_v1"

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            s_str = _format_date_param(start) or "1970-01-01"
            e_str = _format_date_param(end) or datetime.now(timezone.utc).date().isoformat()
            return (
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {"adjusted": str(adjusted).lower()},
            )

        span_ns = _compute_bar_span_ns(multiplier, clean_span)
        return self._build_feed(
            name=f"stocks_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_stock_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
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
                    adjusted=adjusted,
                    symbol_col=symbol_col,
                )
            ),
        )

    def luld(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock Limit Up - Limit Down (`LULD.*`) price bands stream.

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.latest()`,
            `.history()`, `.ring()`, and `.blink()` with `LULD_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("stocks", feed_override=feed)
        return self._build_feed(
            name="stocks_luld",
            schema_spec=LULD_SCHEMA,
            ws_url=ws_url,
            ws_channel="LULD",
            symbols=symbols,
            symbol_col=symbol_col,
        )

    def imbalances(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock Net Order Imbalances (`NOI.*`) auction stream.

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.latest()`,
            `.history()`, `.ring()`, and `.blink()` with `IMBALANCES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("stocks", feed_override=feed)
        return self._build_feed(
            name="stocks_imbalances",
            schema_spec=IMBALANCES_SCHEMA,
            ws_url=ws_url,
            ws_channel="NOI",
            symbols=symbols,
            symbol_col=symbol_col,
        )

    def fmv(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Stock Fair Market Value (`FMV.*`) stream on `business.massive.com`.

        Args:
            symbols: Equity ticker (`"AAPL"`), iterable of tickers, `"*"` for
                all US equities, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `symbols` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.latest()`,
            `.history()`, `.ring()`, and `.blink()` with `FMV_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("stocks", feed_override=feed, is_fmv=True)
        return self._build_feed(
            name="stocks_fmv",
            schema_spec=FMV_SCHEMA,
            ws_url=ws_url,
            ws_channel="FMV",
            symbols=symbols,
            symbol_col=symbol_col,
        )

    def snapshots(
        self,
        symbols: str | Iterable[str] | None = None,
        *,
        direction: str | None = None,
        include_otc: bool = False,
    ) -> FeedBuilder:
        """Configure a US Stock market snapshots feed (`/v2/snapshot/locale/us/markets/stocks/...`).

        Args:
            symbols: Optional equity ticker or iterable of tickers to filter.
                When `None` and `direction` is `None`, returns snapshots for
                all ~10,000+ active US equities.
            direction: Optional top-movers filter (`"gainers"` or `"losers"`).
            include_otc: Whether to include OTC securities. Defaults to `False`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.

        Raises:
            ValueError: If `direction` is not `"gainers"` or `"losers"` when
                evaluated.
        """
        canon_syms = self._canonical_symbols(symbols) if symbols is not None else None

        def _fetch() -> list[dict[str, Any]]:
            params: dict[str, Any] = {}
            if include_otc:
                params["include_otc"] = "true"
            if direction is not None:
                clean_dir = direction.strip().lower()
                if clean_dir not in ("gainers", "losers"):
                    raise ValueError("direction must be 'gainers' or 'losers'")
                return self._fetch_rest_rows(
                    f"/v2/snapshot/locale/us/markets/stocks/{clean_dir}", params
                )
            if canon_syms and "*" not in canon_syms:
                params["tickers"] = ",".join(canon_syms)
            return self._fetch_rest_rows("/v2/snapshot/locale/us/markets/stocks/tickers", params)

        return self._build_feed(
            name="stocks_snapshots",
            schema_spec=STOCK_SNAPSHOT_SCHEMA,
            symbols=symbols,
            snapshot_fn=_fetch,
        )

    def top_movers(self, direction: str = "gainers", *, include_otc: bool = False) -> FeedBuilder:
        """Configure a top US Stock market movers feed (`/v2/snapshot/locale/us/markets/stocks/{direction}`).

        Args:
            direction: `"gainers"` (default) or `"losers"`.
            include_otc: Whether to include OTC securities. Defaults to `False`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.
        """
        return self.snapshots(direction=direction, include_otc=include_otc)

    def gainers(self, *, include_otc: bool = False) -> FeedBuilder:
        """Configure a top US Stock gainers feed (`/v2/snapshot/locale/us/markets/stocks/gainers`).

        Args:
            include_otc: Whether to include OTC securities. Defaults to `False`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.
        """
        return self.snapshots(direction="gainers", include_otc=include_otc)

    def losers(self, *, include_otc: bool = False) -> FeedBuilder:
        """Configure a top US Stock losers feed (`/v2/snapshot/locale/us/markets/stocks/losers`).

        Args:
            include_otc: Whether to include OTC securities. Defaults to `False`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `STOCK_SNAPSHOT_SCHEMA`.
        """
        return self.snapshots(direction="losers", include_otc=include_otc)

    def daily_market_summary(
        self,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
        include_otc: bool = False,
    ) -> FeedBuilder:
        """Configure a grouped daily OHLCV market summary feed for all US stocks on `target_date` (`/v2/aggs/grouped/locale/us/market/stocks/{date}`).

        Args:
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.
            include_otc: Whether to include OTC securities. Defaults to `False`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        d_str = _format_date_param(target_date)
        params = {
            "adjusted": str(adjusted).lower(),
            "include_otc": str(include_otc).lower(),
        }
        return self._build_feed(
            name="stocks_daily_market_summary",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v2/aggs/grouped/locale/us/market/stocks/{d_str}",
                params,
                extra_fields={"Date": d_str},
            ),
        )

    def previous_close(self, symbol: str, *, adjusted: bool = True) -> FeedBuilder:
        """Configure a feed for a US Stock's previous trading day OHLCV bar (`/v2/aggs/ticker/{ticker}/prev`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `BARS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        params = {"adjusted": str(adjusted).lower()}
        return self._build_feed(
            name=f"stocks_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v2/aggs/ticker/{canon}/prev",
                params,
                inject_symbol=canon,
            ),
        )

    previous_day_bar = previous_close

    def daily_open_close(
        self,
        symbol: str,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a daily open/close summary feed with pre-market and after-hours prices (`/v1/open-close/{ticker}/{date}`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        d_str = _format_date_param(target_date)
        params = {"adjusted": str(adjusted).lower()}
        return self._build_feed(
            name=f"stocks_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v1/open-close/{canon}/{d_str}",
                params,
                inject_symbol=canon,
            ),
        )

    daily_ticker_summary = daily_open_close

    def sma(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a Simple Moving Average (`SMA`) indicator feed (`/v1/indicators/sma/{ticker}`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.
            adjusted: Whether to use split-adjusted prices. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed("SMA", symbol, params, window, timespan, series_type)

    def ema(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure an Exponential Moving Average (`EMA`) indicator feed (`/v1/indicators/ema/{ticker}`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            window: Rolling window period length. Defaults to `50`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.
            adjusted: Whether to use split-adjusted prices. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed("EMA", symbol, params, window, timespan, series_type)

    def macd(
        self,
        symbol: str,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a Moving Average Convergence/Divergence (`MACD`) indicator feed (`/v1/indicators/macd/{ticker}`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            short_window: Fast EMA period length. Defaults to `12`.
            long_window: Slow EMA period length. Defaults to `26`.
            signal_window: Signal line EMA period length. Defaults to `9`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.
            adjusted: Whether to use split-adjusted prices. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
        params = {
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed("MACD", symbol, params, short_window, timespan, series_type)

    def rsi(
        self,
        symbol: str,
        *,
        window: int = 14,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a Relative Strength Index (`RSI`) indicator feed (`/v1/indicators/rsi/{ticker}`).

        Args:
            symbol: Equity ticker (e.g., `"AAPL"`).
            window: Rolling lookback window length. Defaults to `14`.
            timespan: Aggregation bar size (`"minute"`, `"hour"`, `"day"`,
                `"week"`, `"month"`). Defaults to `"day"`.
            series_type: Price series component (`"close"`, `"open"`, `"high"`,
                `"low"`). Defaults to `"close"`.
            adjusted: Whether to use split-adjusted prices. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `INDICATOR_SCHEMA`.
        """
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed("RSI", symbol, params, window, timespan, series_type)

    def short_interest(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for bi-weekly FINRA short interest reports (`/stocks/v1/short-interest`).

        Tracks total shares sold short, average daily trading volume, and days-to-cover
        reported on FINRA's semi-monthly settlement schedule.

        Args:
            symbol: Optional single equity ticker (for example, `"GME"`).
            tickers: Optional multi-ticker filter (`"GME,AMC"` or `["GME", "AMC"]`).
            **filters: Additional REST query parameters (for example,
                `settlement_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_short_interest",
            "/stocks/v1/short-interest",
            SHORT_INTEREST_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="settlement_date",
        )

    def short_volume(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for daily off-exchange and exchange short sale volume (`/stocks/v1/short-volume`).

        Reports daily short sale volume, total volume, and short volume ratio across
        FINRA TRFs and exchange venues.

        Args:
            symbol: Optional single equity ticker (for example, `"NVDA"`).
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example, `date_gte="2026-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_short_volume",
            "/stocks/v1/short-volume",
            SHORT_VOLUME_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="date",
        )

    def float(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for public share float and free-float percentage (`/stocks/vX/float`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `effective_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_float",
            "/stocks/vX/float",
            SHARE_FLOAT_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="effective_date",
        )

    share_float = float

    def balance_sheets(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        timeframe: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for standardized quarterly and annual balance sheets (`/stocks/financials/v1/balance-sheets`).

        Args:
            symbol: Optional single equity ticker (for example, `"AAPL"`).
            tickers: Optional multi-ticker filter.
            timeframe: Optional reporting period filter (`"quarterly"`, `"annual"`, or `"ttm"`).
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_balance_sheets",
            "/stocks/financials/v1/balance-sheets",
            FINANCIAL_STATEMENT_SCHEMA,
            {"timeframe": timeframe},
            symbol=symbol,
            tickers=tickers,
            ticker_param="tickers",
            filters=filters,
            date_param="period_of_report_date",
        )

    def income_statements(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        timeframe: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for standardized quarterly and annual income statements (`/stocks/financials/v1/income-statements`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            timeframe: Optional reporting period filter (`"quarterly"`, `"annual"`, or `"ttm"`).
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_income_statements",
            "/stocks/financials/v1/income-statements",
            FINANCIAL_STATEMENT_SCHEMA,
            {"timeframe": timeframe},
            symbol=symbol,
            tickers=tickers,
            ticker_param="tickers",
            filters=filters,
            date_param="period_of_report_date",
        )

    def cash_flow_statements(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        timeframe: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for standardized quarterly and annual cash flow statements (`/stocks/financials/v1/cash-flow-statements`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            timeframe: Optional reporting period filter (`"quarterly"`, `"annual"`, or `"ttm"`).
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_cash_flow_statements",
            "/stocks/financials/v1/cash-flow-statements",
            FINANCIAL_STATEMENT_SCHEMA,
            {"timeframe": timeframe},
            symbol=symbol,
            tickers=tickers,
            ticker_param="tickers",
            filters=filters,
            date_param="period_of_report_date",
        )

    def financials(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        timeframe: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for combined financial statements (`/vX/reference/financials`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            timeframe: Optional reporting period filter (`"quarterly"`, `"annual"`, or `"ttm"`).
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_financials",
            "/vX/reference/financials",
            FINANCIAL_STATEMENT_SCHEMA,
            {"timeframe": timeframe},
            symbol=symbol,
            tickers=tickers,
            ticker_param="ticker",
            filters=filters,
            date_param="period_of_report_date",
        )

    financials_vx = financials

    def ratios(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for valuation, profitability, liquidity, and leverage ratios (`/stocks/financials/v1/ratios`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_ratios",
            "/stocks/financials/v1/ratios",
            RATIOS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="date",
        )

    def dividends(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for cash and stock dividend distributions (`/stocks/v1/dividends`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `ex_dividend_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_dividends",
            "/stocks/v1/dividends",
            DIVIDENDS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="ex_dividend_date",
        )

    def splits(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for historical and upcoming forward/reverse stock splits (`/stocks/v1/splits`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `execution_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_splits",
            "/stocks/v1/splits",
            SPLITS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="execution_date",
        )

    def ipos(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for upcoming and historical initial public offerings (`/vX/reference/ipos`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `listing_date_gte="2025-01-01"`, `ipo_status="upcoming"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_ipos",
            "/vX/reference/ipos",
            IPOS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="listing_date",
        )

    def tickers(
        self,
        *,
        market: str = "stocks",
        active: bool = True,
        search: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for reference ticker directory entries (`/v3/reference/tickers`).

        Args:
            market: Asset market filter (defaults to `"stocks"`).
            active: If `True`, include only actively traded symbols; if `False`,
                include delisted historical symbols.
            search: Optional free-text search query matching symbol or company name.
            **filters: Additional REST query parameters (for example, `type="CS"`,
                `exchange="XNAS"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_tickers",
            "/v3/reference/tickers",
            TICKERS_SCHEMA,
            {"market": market, "active": str(active).lower(), "search": search},
            filters=filters,
        )

    def ticker_details(self, symbol: str) -> FeedBuilder:
        """Build a feed for a single company's detailed profile (`/v3/reference/tickers/{ticker}`).

        Includes SIC industry classification, market capitalization, share count,
        headquarters address, and branding metadata.

        Args:
            symbol: Equity ticker symbol (for example, `"AAPL"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self._rest_collection_feed(
            f"stocks_ticker_details_{canon}",
            f"/v3/reference/tickers/{canon}",
            TICKER_DETAILS_SCHEMA,
            inject_symbol=canon,
        )

    def ticker_events(self, symbol: str) -> FeedBuilder:
        """Build a feed for a symbol's corporate rebranding and ticker-change timeline (`/vX/reference/tickers/{ticker}/events`).

        Args:
            symbol: Equity ticker symbol (for example, `"META"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]

        def _fetch() -> list[dict[str, Any]]:
            payload = self.session._request_json(f"/vX/reference/tickers/{canon}/events")
            results = payload.get("results", {}) if isinstance(payload, Mapping) else {}
            events = results.get("events", []) if isinstance(results, Mapping) else []
            return [{**ev, "Symbol": canon} for ev in events if isinstance(ev, Mapping)]

        return self._build_feed(
            name=f"stocks_ticker_events_{canon}",
            schema_spec=TICKER_EVENTS_SCHEMA,
            symbols=canon,
            snapshot_fn=_fetch,
        )

    def related_companies(self, symbol: str) -> FeedBuilder:
        """Build a feed for peer and related company tickers (`/v1/related-companies/{ticker}`).

        Identifies comparable tickers based on news co-mention frequency, institutional
        holdings overlap, and sector classification.

        Args:
            symbol: Equity ticker symbol (for example, `"NVDA"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self._rest_collection_feed(
            f"stocks_related_companies_{canon}",
            f"/v1/related-companies/{canon}",
            RELATED_COMPANIES_SCHEMA,
            inject_symbol=canon,
        )

    def ticker_types(self, *, asset_class: str = "stocks") -> FeedBuilder:
        """Build a feed for supported security type codes (`/v3/reference/tickers/types`).

        Args:
            asset_class: Asset class filter (defaults to `"stocks"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_ticker_types",
            "/v3/reference/tickers/types",
            TICKER_TYPES_SCHEMA,
            {"asset_class": asset_class},
        )

    def sec_filings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        form_type: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for the SEC EDGAR filings index (`/stocks/filings/vX/index`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            form_type: Optional SEC form filter (for example, `"10-K"`, `"10-Q"`, `"8-K"`).
            **filters: Additional REST query parameters (for example,
                `filing_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_filings",
            "/stocks/filings/vX/index",
            SEC_FILINGS_SCHEMA,
            {"form_type": form_type},
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    sec_edgar_index = sec_filings

    def sec_10k_sections(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        section: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for parsed Form 10-K narrative sections (`/stocks/filings/10-K/vX/sections`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            section: Optional 10-K section identifier (for example, `"item_1"`,
                `"item_1a"`, `"item_7"`).
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_10k_sections",
            "/stocks/filings/10-K/vX/sections",
            SEC_FILINGS_SCHEMA,
            {"section": section},
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    def sec_8k_text(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for parsed Form 8-K material event disclosures (`/stocks/filings/8-K/vX/text`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_8k_text",
            "/stocks/filings/8-K/vX/text",
            SEC_FILINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    def sec_risk_factors(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for categorized SEC filing risk-factor disclosures (`/stocks/filings/vX/risk-factors`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_risk_factors",
            "/stocks/filings/vX/risk-factors",
            SEC_FILINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    def sec_disclosures(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for structured SEC text and taxonomy disclosures (`/stocks/filings/vX/disclosures`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_disclosures",
            "/stocks/filings/vX/disclosures",
            SEC_FILINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    def sec_13f_holdings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        cik: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for institutional Form 13F equity holdings (`/stocks/filings/13f/vX/holdings`).

        Args:
            symbol: Optional single equity ticker held by institutional managers.
            tickers: Optional multi-ticker filter.
            cik: Optional institutional manager CIK number.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_13f",
            "/stocks/filings/13f/vX/holdings",
            SEC_13F_SCHEMA,
            {"cik": cik},
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="period_of_report_date",
        )

    def sec_form3_ownership(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for initial beneficial ownership Form 3 holdings (`/stocks/filings/insider-transactions/vX/holdings`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_form3",
            "/stocks/filings/insider-transactions/vX/holdings",
            SEC_INSIDER_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="filing_date",
        )

    def sec_insider_transactions(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for Form 3, Form 4, and Form 5 insider transactions (`/stocks/filings/insider-transactions/vX/transactions`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `transaction_date_gte="2025-01-01"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_sec_insider",
            "/stocks/filings/insider-transactions/vX/transactions",
            SEC_INSIDER_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="transaction_date",
        )

    sec_form4_transactions = sec_insider_transactions

    def news(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Build a feed for real-time financial news articles with sentiment and entity insights (`/v2/reference/news`).

        Args:
            symbol: Optional single equity ticker.
            tickers: Optional multi-ticker filter.
            **filters: Additional REST query parameters (for example,
                `published_utc_gte="2026-01-01"`, `order="desc"`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()`, `.historical()`, and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_news",
            "/v2/reference/news",
            NEWS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="published_utc",
        )

    def exchanges(self, *, asset_class: str = "stocks") -> FeedBuilder:
        """Build a feed for the global exchange and trading venue directory (`/v3/reference/exchanges`).

        Args:
            asset_class: Asset class filter (`"stocks"`, `"options"`, `"crypto"`, `"fx"`,
                or `"futures"`). Defaults to `"stocks"`.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_exchanges",
            "/v3/reference/exchanges",
            EXCHANGES_SCHEMA,
            {"asset_class": asset_class},
        )

    def conditions(self, *, asset_class: str = "stocks") -> FeedBuilder:
        """Build a feed for SIP/exchange trade and quote condition code mappings (`/v3/reference/conditions`).

        Args:
            asset_class: Asset class filter (`"stocks"`, `"options"`, `"crypto"`, or `"fx"`).
                Defaults to `"stocks"`.

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_conditions",
            "/v3/reference/conditions",
            CONDITIONS_SCHEMA,
            {"asset_class": asset_class},
        )

    def market_holidays(self) -> FeedBuilder:
        """Build a feed for upcoming exchange holidays and early-close schedules (`/v1/marketstatus/upcoming`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_market_holidays",
            "/v1/marketstatus/upcoming",
            HOLIDAYS_SCHEMA,
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Build a feed for real-time exchange, market-group, and currency session status (`/v1/marketstatus/now`).

        Returns:
            A [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] supporting
            `.snapshot()` and `.poll()`.
        """
        return self._rest_collection_feed(
            "stocks_market_status",
            "/v1/marketstatus/now",
            MARKET_STATUS_SCHEMA,
        )
