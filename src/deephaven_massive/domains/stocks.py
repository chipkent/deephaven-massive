"""US Stocks domain client (`m.stocks`) covering trades, quotes, bars, snapshots, fundamentals, SEC, and reference."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    format_date_param,
    normalize_filter_params,
)
from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    IMBALANCES_SCHEMA,
    INDICATOR_SCHEMA,
    LULD_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
    compute_bar_span_ns,
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
    """Domain client for US Stocks real-time, delayed, historical, fundamental, and reference feeds."""

    def __init__(self, session: MassiveSession) -> None:
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
        return self.fetch_rest_rows(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params,
            extra_fields=extra_fields,
        )

    def _historical_multi_symbol(
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
        adjusted: bool | None = None,
        extra_params: Mapping[str, Any] | None = None,
    ) -> Table:
        return self.fetch_multi_symbol_historical(
            symbols,
            schema_spec,
            s3_dataset,
            lambda sym, s, e: rest_path_fn(sym, s, e, extra_params),
            start=start,
            end=end,
            limit=limit,
            source=source,
            adjusted=adjusted,
            empty_error="Specific symbol(s) are required for REST historical queries (or configure S3 credentials for '*' flat files).",
        )

    def trades(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, poll, or fetch historical US Stock trades (`T.*`, `/v3/trades/{ticker}`)."""
        ws_url = self.session.config.resolve_ws_url("stocks", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any, extra: Mapping[str, Any] | None
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            params: dict[str, Any] = dict(extra or {})
            params["timestamp.gte"] = format_date_param(start)
            params["timestamp.lte"] = format_date_param(end)
            return f"/v3/trades/{ticker}", params

        return self.build_feed(
            name="stocks_trades",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_stock_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, lambda sym, s, e: _rest_path(sym, s, e, None)
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_multi_symbol(
                symbols,
                TRADES_SCHEMA,
                "us_stocks_sip/trades_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def quotes(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, poll, or fetch historical US Stock NBBO quotes (`Q.*`, `/v3/quotes/{ticker}`)."""
        ws_url = self.session.config.resolve_ws_url("stocks", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any, extra: Mapping[str, Any] | None
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            params: dict[str, Any] = dict(extra or {})
            params["timestamp.gte"] = format_date_param(start)
            params["timestamp.lte"] = format_date_param(end)
            return f"/v3/quotes/{ticker}", params

        return self.build_feed(
            name="stocks_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_stock_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, lambda sym, s, e: _rest_path(sym, s, e, None)
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_multi_symbol(
                symbols,
                QUOTES_SCHEMA,
                "us_stocks_sip/quotes_v1",
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
        adjusted: bool = True,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, poll, or fetch historical US Stock OHLCV bars (`A.*` / `AM.*`, `/v2/aggs/...`)."""
        clean_span = timespan.strip().lower()
        ws_channel = "A" if clean_span == "second" else "AM"
        ws_url = self.session.config.resolve_ws_url("stocks", feed_override=feed)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_stocks_sip/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_stocks_sip/day_aggs_v1"

        def _rest_path(
            sym: str, start: Any, end: Any, extra: Mapping[str, Any] | None
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            s_str = format_date_param(start) or "1970-01-01"
            e_str = format_date_param(end) or s_str
            params: dict[str, Any] = {
                "adjusted": str(adjusted).lower(),
                **(extra or {}),
            }
            return (
                f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                params,
            )

        span_ns = compute_bar_span_ns(multiplier, clean_span)
        return self.build_feed(
            name=f"stocks_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_stock_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                lambda sym, s, e: _rest_path(sym, s, e, None),
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_multi_symbol(
                symbols,
                BARS_SCHEMA,
                s3_ds,
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
                adjusted=adjusted,
            ),
        )

    def luld(
        self,
        symbols: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream US Stock Limit Up - Limit Down (LULD) price bands (`LULD.*`)."""
        ws_url = self.session.config.resolve_ws_url("stocks", feed_override=feed)
        return self.build_feed(
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
        """Stream US Stock Net Order Imbalances (`NOI.*`)."""
        ws_url = self.session.config.resolve_ws_url("stocks", feed_override=feed)
        return self.build_feed(
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
        """Stream US Stock Fair Market Value (`FMV.*` on `business.massive.com`)."""
        ws_url = self.session.config.resolve_ws_url(
            "stocks", feed_override=feed, is_fmv=True
        )
        return self.build_feed(
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
        """Snapshot or poll full-market or filtered US Stock snapshots (including top gainers/losers)."""
        canon_syms = self.canonical_symbols(symbols) if symbols is not None else None

        def _fetch() -> list[dict[str, Any]]:
            params: dict[str, Any] = {}
            if include_otc:
                params["include_otc"] = "true"
            if direction is not None:
                clean_dir = direction.strip().lower()
                if clean_dir not in ("gainers", "losers"):
                    raise ValueError("direction must be 'gainers' or 'losers'")
                return self.fetch_rest_rows(
                    f"/v2/snapshot/locale/us/markets/stocks/{clean_dir}", params
                )
            if canon_syms and "*" not in canon_syms:
                params["tickers"] = ",".join(canon_syms)
            return self.fetch_rest_rows(
                "/v2/snapshot/locale/us/markets/stocks/tickers", params
            )

        return self.build_feed(
            name="stocks_snapshots",
            schema_spec=STOCK_SNAPSHOT_SCHEMA,
            symbols=symbols,
            snapshot_fn=_fetch,
        )

    def top_movers(
        self, direction: str = "gainers", *, include_otc: bool = False
    ) -> FeedBuilder:
        """Top US Stock market movers (`direction='gainers'` or `'losers'`, `/v2/snapshot/locale/us/markets/stocks/{direction}`)."""
        return self.snapshots(direction=direction, include_otc=include_otc)

    def gainers(self, *, include_otc: bool = False) -> FeedBuilder:
        """Top US Stock gainers (`/v2/snapshot/locale/us/markets/stocks/gainers`)."""
        return self.snapshots(direction="gainers", include_otc=include_otc)

    def losers(self, *, include_otc: bool = False) -> FeedBuilder:
        """Top US Stock losers (`/v2/snapshot/locale/us/markets/stocks/losers`)."""
        return self.snapshots(direction="losers", include_otc=include_otc)

    def daily_market_summary(
        self,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
        include_otc: bool = False,
    ) -> FeedBuilder:
        """Fetch grouped daily OHLCV market summary for all US stocks on `target_date`."""
        d_str = format_date_param(target_date)
        params = {
            "adjusted": str(adjusted).lower(),
            "include_otc": str(include_otc).lower(),
        }
        return self.build_feed(
            name="stocks_daily_market_summary",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v2/aggs/grouped/locale/us/market/stocks/{d_str}",
                params,
                extra_fields={"Date": d_str},
            ),
        )

    def previous_close(
        self, symbol: str, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Fetch previous day's OHLCV bar for `symbol` (`/v2/aggs/ticker/{ticker}/prev`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        params = {"adjusted": str(adjusted).lower()}
        return self.build_feed(
            name=f"stocks_prev_close_{canon}",
            schema_spec=BARS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Fetch daily open/close summary with pre-market and after-hours prices (`/v1/open-close/{ticker}/{date}`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        d_str = format_date_param(target_date)
        params = {"adjusted": str(adjusted).lower()}
        return self.build_feed(
            name=f"stocks_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/open-close/{canon}/{d_str}",
                params,
                inject_symbol=canon,
            ),
        )

    daily_ticker_summary = daily_open_close

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
            name=f"stocks_{indicator.lower()}_{canon}",
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
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Simple Moving Average (SMA) indicator feed (`/v1/indicators/sma/{ticker}`)."""
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed(
            "SMA", symbol, params, window, timespan, series_type
        )

    def ema(
        self,
        symbol: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Exponential Moving Average (EMA) indicator feed (`/v1/indicators/ema/{ticker}`)."""
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed(
            "EMA", symbol, params, window, timespan, series_type
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
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Moving Average Convergence/Divergence (MACD) indicator feed (`/v1/indicators/macd/{ticker}`)."""
        params = {
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed(
            "MACD", symbol, params, short_window, timespan, series_type
        )

    def rsi(
        self,
        symbol: str,
        *,
        window: int = 14,
        timespan: str = "day",
        series_type: str = "close",
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Relative Strength Index (RSI) indicator feed (`/v1/indicators/rsi/{ticker}`)."""
        params = {
            "window": window,
            "timespan": timespan,
            "series_type": series_type,
            "adjusted": str(adjusted).lower(),
        }
        return self._indicator_feed(
            "RSI", symbol, params, window, timespan, series_type
        )

    def _rest_collection_feed(
        self,
        name: str,
        path: str,
        schema_spec: SchemaSpec,
        params: Mapping[str, Any] | None = None,
        *,
        symbol: str | None = None,
        tickers: str | Iterable[str] | None = None,
        ticker_param: str = "ticker",
        filters: Mapping[str, Any] | None = None,
        date_param: str = "date",
        inject_symbol: str | None = None,
    ) -> FeedBuilder:
        base_params = normalize_filter_params(
            params,
            filters,
            symbol=symbol,
            tickers=tickers,
            normalizer=self.symbol_normalizer,
            ticker_param=ticker_param,
        )

        def _hist(
            start: Any = None,
            end: Any = None,
            limit: int | None = None,
            source: str = "auto",
        ) -> Table:
            q = dict(base_params)
            q[f"{date_param}.gte"] = format_date_param(start)
            q[f"{date_param}.lte"] = format_date_param(end)
            return self.fetch_chunked_table(
                path, schema_spec, q, limit=limit, inject_symbol=inject_symbol
            )

        return self.build_feed(
            name=name,
            schema_spec=schema_spec,
            snapshot_fn=lambda: self.fetch_rest_rows(
                path, base_params, inject_symbol=inject_symbol
            ),
            historical_fn=_hist,
        )

    def short_interest(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Bi-weekly FINRA short interest (`/stocks/v1/short-interest`)."""
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
        """Daily short sale volume & ratio (`/stocks/v1/short-volume`)."""
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
        """Public share float & free float percentage (`/stocks/vX/float`)."""
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
        """Quarterly and annual balance sheets (`/stocks/financials/v1/balance-sheets`)."""
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
        """Quarterly and annual income statements (`/stocks/financials/v1/income-statements`)."""
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
        """Quarterly and annual cash flow statements (`/stocks/financials/v1/cash-flow-statements`)."""
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
        """Combined quarterly and annual financial statements (`/vX/reference/financials`)."""
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
        """Valuation, profitability, liquidity, and leverage ratios (`/stocks/financials/v1/ratios`)."""
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
        """Cash and stock dividends (`/stocks/v1/dividends`)."""
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
        """Forward and reverse stock splits (`/stocks/v1/splits`)."""
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
        """Upcoming and historical IPO events (`/vX/reference/ipos`)."""
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
        """All active and historical tickers (`/v3/reference/tickers`)."""
        return self._rest_collection_feed(
            "stocks_tickers",
            "/v3/reference/tickers",
            TICKERS_SCHEMA,
            {"market": market, "active": str(active).lower(), "search": search},
            filters=filters,
        )

    def ticker_details(self, symbol: str) -> FeedBuilder:
        """Company profile, SIC, market cap, and address (`/v3/reference/tickers/{ticker}`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self._rest_collection_feed(
            f"stocks_ticker_details_{canon}",
            f"/v3/reference/tickers/{canon}",
            TICKER_DETAILS_SCHEMA,
            inject_symbol=canon,
        )

    def ticker_events(self, symbol: str) -> FeedBuilder:
        """Symbol change and rebranding history (`/vX/reference/tickers/{ticker}/events`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]

        def _fetch() -> list[dict[str, Any]]:
            payload = self.session.request_json(
                f"/vX/reference/tickers/{canon}/events"
            )
            results = (
                payload.get("results", {})
                if isinstance(payload, Mapping)
                else {}
            )
            events = (
                results.get("events", [])
                if isinstance(results, Mapping)
                else []
            )
            return [
                {**ev, "Symbol": canon}
                for ev in events
                if isinstance(ev, Mapping)
            ]

        return self.build_feed(
            name=f"stocks_ticker_events_{canon}",
            schema_spec=TICKER_EVENTS_SCHEMA,
            symbols=canon,
            snapshot_fn=_fetch,
        )

    def related_companies(self, symbol: str) -> FeedBuilder:
        """Peer/related ticker relationships (`/v1/related-companies/{ticker}`)."""
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
        return self._rest_collection_feed(
            f"stocks_related_companies_{canon}",
            f"/v1/related-companies/{canon}",
            RELATED_COMPANIES_SCHEMA,
            inject_symbol=canon,
        )

    def ticker_types(self, *, asset_class: str = "stocks") -> FeedBuilder:
        """Supported security types (`/v3/reference/tickers/types`)."""
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
        """SEC EDGAR filings index (`/stocks/filings/vX/index`)."""
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
        """Parsed 10-K narrative sections (`/stocks/filings/10-K/vX/sections`)."""
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
        """Parsed 8-K item disclosures (`/stocks/filings/8-K/vX/text`)."""
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
        """Categorized SEC filing risk factor disclosures (`/stocks/filings/vX/risk-factors`)."""
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
        """Structured SEC text & taxonomy disclosures (`/stocks/filings/vX/disclosures`)."""
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
        """Institutional 13F holdings (`/stocks/filings/13f/vX/holdings`)."""
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
        """Initial beneficial ownership Form 3 holdings (`/stocks/filings/insider-transactions/vX/holdings`)."""
        return self._rest_collection_feed(
            "stocks_sec_form3",
            "/stocks/filings/insider-transactions/vX/holdings",
            SEC_13F_SCHEMA,
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
        """Form 3/4/5 insider transactions (`/stocks/filings/insider-transactions/vX/transactions`)."""
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
        """Real-time news articles with sentiment & insights (`/v2/reference/news`)."""
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
        """Exchange directory (`/v3/reference/exchanges`)."""
        return self._rest_collection_feed(
            "stocks_exchanges",
            "/v3/reference/exchanges",
            EXCHANGES_SCHEMA,
            {"asset_class": asset_class},
        )

    def conditions(self, *, asset_class: str = "stocks") -> FeedBuilder:
        """Trade and quote condition code mappings (`/v3/reference/conditions`)."""
        return self._rest_collection_feed(
            "stocks_conditions",
            "/v3/reference/conditions",
            CONDITIONS_SCHEMA,
            {"asset_class": asset_class},
        )

    def market_holidays(self) -> FeedBuilder:
        """Upcoming market holidays and early closes (`/v1/marketstatus/upcoming`)."""
        return self._rest_collection_feed(
            "stocks_market_holidays",
            "/v1/marketstatus/upcoming",
            HOLIDAYS_SCHEMA,
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Current exchange and market group open/closed status (`/v1/marketstatus/now`)."""
        return self._rest_collection_feed(
            "stocks_market_status",
            "/v1/marketstatus/now",
            MARKET_STATUS_SCHEMA,
        )
