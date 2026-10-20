"""US Stocks domain client (`m.stocks`) covering trades, quotes, bars, snapshots, fundamentals, SEC, and reference."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
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
    FMV_SCHEMA,
    IMBALANCES_SCHEMA,
    INDICATOR_SCHEMA,
    LULD_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
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
        self, symbols: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["tickers"] = ",".join(
                self.symbol_normalizer.rest_ticker(s)  # type: ignore[union-attr]
                for s in symbols
            )
        return self.fetch_rest_rows(
            "/v2/snapshot/locale/us/markets/stocks/tickers", params
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
        extra_params: Mapping[str, Any] | None = None,
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
                domain="stocks",
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(
                "Specific symbol(s) are required for REST historical queries (or configure S3 credentials for '*' flat files)."
            )

        if len(target_syms) == 1:
            sym = target_syms[0]
            path, params = rest_path_fn(sym, start, end, extra_params)
            return self.fetch_chunked_table(
                path, schema_spec, params, limit=limit, inject_symbol=sym
            )

        all_rows: list[dict[str, Any]] = []
        for sym in target_syms:
            path, params = rest_path_fn(sym, start, end, extra_params)
            rows = self.fetch_rest_rows(
                path, params, limit=limit, inject_symbol=sym
            )
            all_rows.extend(rows)

        batches = (
            [rows_to_record_batch(all_rows, schema_spec, self.symbol_normalizer)]
            if all_rows
            else []
        )
        pa_tbl = record_batches_to_arrow_table(batches, schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

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

        return self.build_feed(
            name=f"stocks_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=self._fetch_stock_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_multi_symbol(
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
        date_param: str = "date",
        inject_symbol: str | None = None,
    ) -> FeedBuilder:
        base_params = {k: v for k, v in (params or {}).items() if v is not None}

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

    def short_interest(self, symbol: str | None = None) -> FeedBuilder:
        """Bi-weekly FINRA short interest (`/stocks/v1/short-interest`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_short_interest",
            "/stocks/v1/short-interest",
            SHORT_INTEREST_SCHEMA,
            {"ticker": canon},
            date_param="settlement_date",
        )

    def short_volume(self, symbol: str | None = None) -> FeedBuilder:
        """Daily short sale volume & ratio (`/stocks/v1/short-volume`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_short_volume",
            "/stocks/v1/short-volume",
            SHORT_VOLUME_SCHEMA,
            {"ticker": canon},
            date_param="date",
        )

    def float(self, symbol: str | None = None) -> FeedBuilder:
        """Public share float & free float percentage (`/stocks/vX/float`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_float",
            "/stocks/vX/float",
            SHARE_FLOAT_SCHEMA,
            {"ticker": canon},
            date_param="effective_date",
        )

    def balance_sheets(
        self, symbol: str | None = None, *, timeframe: str | None = None
    ) -> FeedBuilder:
        """Quarterly and annual balance sheets (`/stocks/financials/v1/balance-sheets`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_balance_sheets",
            "/stocks/financials/v1/balance-sheets",
            FINANCIAL_STATEMENT_SCHEMA,
            {"tickers": canon, "timeframe": timeframe},
            date_param="period_of_report_date",
        )

    def income_statements(
        self, symbol: str | None = None, *, timeframe: str | None = None
    ) -> FeedBuilder:
        """Quarterly and annual income statements (`/stocks/financials/v1/income-statements`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_income_statements",
            "/stocks/financials/v1/income-statements",
            FINANCIAL_STATEMENT_SCHEMA,
            {"tickers": canon, "timeframe": timeframe},
            date_param="period_of_report_date",
        )

    def cash_flow_statements(
        self, symbol: str | None = None, *, timeframe: str | None = None
    ) -> FeedBuilder:
        """Quarterly and annual cash flow statements (`/stocks/financials/v1/cash-flow-statements`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_cash_flow_statements",
            "/stocks/financials/v1/cash-flow-statements",
            FINANCIAL_STATEMENT_SCHEMA,
            {"tickers": canon, "timeframe": timeframe},
            date_param="period_of_report_date",
        )

    def ratios(self, symbol: str | None = None) -> FeedBuilder:
        """Valuation, profitability, liquidity, and leverage ratios (`/stocks/financials/v1/ratios`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_ratios",
            "/stocks/financials/v1/ratios",
            RATIOS_SCHEMA,
            {"ticker": canon},
            date_param="date",
        )

    def dividends(self, symbol: str | None = None) -> FeedBuilder:
        """Cash and stock dividends (`/stocks/v1/dividends`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_dividends",
            "/stocks/v1/dividends",
            DIVIDENDS_SCHEMA,
            {"ticker": canon},
            date_param="ex_dividend_date",
        )

    def splits(self, symbol: str | None = None) -> FeedBuilder:
        """Forward and reverse stock splits (`/stocks/v1/splits`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_splits",
            "/stocks/v1/splits",
            SPLITS_SCHEMA,
            {"ticker": canon},
            date_param="execution_date",
        )

    def ipos(self, symbol: str | None = None) -> FeedBuilder:
        """Upcoming and historical IPO events (`/vX/reference/ipos`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_ipos",
            "/vX/reference/ipos",
            IPOS_SCHEMA,
            {"ticker": canon},
            date_param="listing_date",
        )

    def tickers(
        self,
        *,
        market: str = "stocks",
        active: bool = True,
        search: str | None = None,
    ) -> FeedBuilder:
        """All active and historical tickers (`/v3/reference/tickers`)."""
        return self._rest_collection_feed(
            "stocks_tickers",
            "/v3/reference/tickers",
            TICKERS_SCHEMA,
            {"market": market, "active": str(active).lower(), "search": search},
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
        self, symbol: str | None = None, *, form_type: str | None = None
    ) -> FeedBuilder:
        """SEC EDGAR filings index (`/stocks/filings/vX/index`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_sec_filings",
            "/stocks/filings/vX/index",
            SEC_FILINGS_SCHEMA,
            {"ticker": canon, "form_type": form_type},
            date_param="filing_date",
        )

    def sec_13f_holdings(
        self, symbol: str | None = None, *, cik: str | None = None
    ) -> FeedBuilder:
        """Institutional 13F holdings (`/stocks/filings/13f/vX/holdings`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_sec_13f",
            "/stocks/filings/13f/vX/holdings",
            SEC_13F_SCHEMA,
            {"ticker": canon, "cik": cik},
            date_param="period_of_report_date",
        )

    def sec_insider_transactions(
        self, symbol: str | None = None
    ) -> FeedBuilder:
        """Form 3/4/5 insider transactions (`/stocks/filings/insider-transactions/vX/transactions`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_sec_insider",
            "/stocks/filings/insider-transactions/vX/transactions",
            SEC_INSIDER_SCHEMA,
            {"ticker": canon},
            date_param="transaction_date",
        )

    def news(self, symbol: str | None = None) -> FeedBuilder:
        """Real-time news articles with sentiment & insights (`/v2/reference/news`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._rest_collection_feed(
            "stocks_news",
            "/v2/reference/news",
            NEWS_SCHEMA,
            {"ticker": canon},
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

    def market_status(self) -> FeedBuilder:
        """Current exchange and market group open/closed status (`/v1/marketstatus/now`)."""
        return self._rest_collection_feed(
            "stocks_market_status",
            "/v1/marketstatus/now",
            MARKET_STATUS_SCHEMA,
        )
