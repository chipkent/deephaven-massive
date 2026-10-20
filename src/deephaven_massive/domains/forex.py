"""Global Forex domain client (`m.forex`) covering quotes, bars, FMV, snapshots, currency conversion, and indicators."""

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
    CURRENCY_CONVERSION_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    INDICATOR_SCHEMA,
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
    """Domain client for Global Forex (`EUR/USD`, `GBP/JPY`, etc.)."""

    def __init__(self, session: MassiveSession) -> None:
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
        return self.fetch_rest_rows(
            "/v2/snapshot/locale/global/markets/forex/tickers",
            params,
            extra_fields=extra_fields,
        )

    def _historical_forex(
        self,
        pairs: Any,
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
            pairs,
            schema_spec,
            s3_dataset,
            rest_path_fn,
            empty_error="Specific currency pair(s) are required for REST historical queries.",
            start=start,
            end=end,
            limit=limit,
            source=source,
        )

    def quotes(
        self,
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Forex BBO quotes (`C.*`, `/v3/quotes/{fxTicker}`)."""
        ws_url = self.session.config.resolve_ws_url("forex", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            ticker = self.symbol_normalizer.rest_ticker(sym)  # type: ignore[union-attr]
            return f"/v3/quotes/{ticker}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name="forex_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="C",
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=self._fetch_forex_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, _rest_path
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_forex(
                pairs,
                QUOTES_SCHEMA,
                "global_forex/quotes_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
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
        """Stream, snapshot, or fetch historical Forex OHLCV bars (`CAS.*` / `CA.*`, `/v2/aggs/...`)."""
        clean_span = timespan.strip().lower()
        ws_channel = "CAS" if clean_span == "second" else "CA"
        ws_url = self.session.config.resolve_ws_url("forex", feed_override=feed)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "global_forex/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "global_forex/day_aggs_v1"

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
            name=f"forex_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=lambda syms=None: self._fetch_forex_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_forex(
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
        """Stream Forex Fair Market Value (`FMV.*` on `business.massive.com`)."""
        ws_url = self.session.config.resolve_ws_url(
            "forex", feed_override=feed, is_fmv=True
        )
        return self.build_feed(
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
        """Snapshot or poll Forex pairs (including top gainers/losers)."""
        canon_syms = self.canonical_symbols(pairs) if pairs is not None else None

        def _fetch() -> list[dict[str, Any]]:
            if direction is not None:
                clean_dir = direction.strip().lower()
                if clean_dir not in ("gainers", "losers"):
                    raise ValueError("direction must be 'gainers' or 'losers'")
                return self.fetch_rest_rows(
                    f"/v2/snapshot/locale/global/markets/forex/{clean_dir}"
                )
            return self._fetch_forex_snapshots(canon_syms)

        return self.build_feed(
            name="forex_snapshots",
            schema_spec=STOCK_SNAPSHOT_SCHEMA,
            symbols=pairs,
            snapshot_fn=_fetch,
        )

    def top_movers(self, direction: str = "gainers") -> FeedBuilder:
        """Top Forex market movers (`direction='gainers'` or `'losers'`, `/v2/snapshot/locale/global/markets/forex/{direction}`)."""
        return self.snapshots(direction=direction)

    def last_quote(
        self, from_currency: str, to_currency: str | None = None
    ) -> FeedBuilder:
        """Last BBO quote for a currency pair (`/v1/last_quote/currencies/{from}/{to}`)."""
        pair = (
            f"{from_currency}/{to_currency}"
            if to_currency is not None
            else from_currency
        )
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        fc, tc = canon.split("/")
        return self.build_feed(
            name=f"forex_last_quote_{fc}_{tc}",
            schema_spec=QUOTES_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/last_quote/currencies/{fc}/{tc}", inject_symbol=canon
            ),
        )

    def daily_market_summary(
        self, target_date: str | date | datetime, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Grouped daily summary for all Forex pairs on `target_date` (`/v2/aggs/grouped/locale/global/market/fx/{date}`)."""
        d_str = format_date_param(target_date)
        return self.build_feed(
            name="forex_daily_market_summary",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v2/aggs/grouped/locale/global/market/fx/{d_str}",
                {"adjusted": str(adjusted).lower()},
                extra_fields={"Date": d_str},
            ),
        )

    def previous_close(
        self, pair: str, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Previous day's OHLCV bar for a Forex pair (`/v2/aggs/ticker/{forexTicker}/prev`)."""
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
        return self.build_feed(
            name=f"forex_prev_close_{canon}",
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
        """Daily open/close summary for a Forex pair (`/v1/open-close/{forexTicker}/{date}`)."""
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
        d_str = format_date_param(target_date)
        return self.build_feed(
            name=f"forex_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Real-time currency conversion (`/v1/conversion/{from}/{to}`)."""
        fc = from_currency.strip().upper()
        tc = to_currency.strip().upper()
        return self.build_feed(
            name=f"forex_convert_{fc}_{tc}",
            schema_spec=CURRENCY_CONVERSION_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Forex currency pairs directory (`/v3/reference/tickers?market=fx`)."""
        params = normalize_filter_params(
            {"market": "fx", "active": str(active).lower(), "search": search},
            filters,
        )
        return self.build_feed(
            name="forex_tickers",
            schema_spec=TICKERS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/tickers", params
            ),
        )

    def exchanges(self) -> FeedBuilder:
        """Forex exchange directory (`/v3/reference/exchanges?asset_class=fx`)."""
        return self.build_feed(
            name="forex_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/exchanges", {"asset_class": "fx"}
            ),
        )

    def market_holidays(self) -> FeedBuilder:
        """Upcoming market holidays (`/v1/marketstatus/upcoming`)."""
        return self.build_feed(
            name="forex_market_holidays",
            schema_spec=HOLIDAYS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v1/marketstatus/upcoming"
            ),
        )

    holidays = market_holidays

    def market_status(self) -> FeedBuilder:
        """Current Forex and currency market status (`/v1/marketstatus/now`)."""
        return self.build_feed(
            name="forex_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/v1/marketstatus/now"),
        )

    def _indicator_feed(
        self,
        indicator: str,
        pair: str,
        params: dict[str, Any],
        window: int,
        timespan: str,
        series_type: str,
    ) -> FeedBuilder:
        canon = self.symbol_normalizer.canonical(pair)  # type: ignore[union-attr]
        ticker = self.symbol_normalizer.rest_ticker(canon)  # type: ignore[union-attr]
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
                f"/v1/indicators/{indicator.lower()}/{ticker}",
                INDICATOR_SCHEMA,
                q,
                limit=limit,
                inject_symbol=canon,
                extra_fields=extra,
            )

        return self.build_feed(
            name=f"forex_{indicator.lower()}_{canon}",
            schema_spec=INDICATOR_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/indicators/{indicator.lower()}/{ticker}",
                params,
                inject_symbol=canon,
                extra_fields=extra,
            ),
            historical_fn=_fetch,
        )

    def sma(
        self,
        pair: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """SMA indicator for a Forex pair."""
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
        """EMA indicator for a Forex pair."""
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
        """MACD indicator for a Forex pair."""
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
        """RSI indicator for a Forex pair."""
        return self._indicator_feed(
            "RSI",
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
