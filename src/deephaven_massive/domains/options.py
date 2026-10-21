"""US Options (`OPRA`) domain client (`m.options`).

This module implements :class:`OptionsClient`, which provides real-time and
delayed OPRA WebSocket streams (`trades`, `quotes`, `bars`, `fmv`), full
option-chain snapshots with implied volatility and Greeks, historical backfills
(via S3 Flat Files or REST), contract directories, and technical indicators.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    _format_date_param,
    _normalize_filter_params,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    OPTION_CHAIN_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
    _compute_bar_span_ns,
)
from deephaven_massive.schemas.reference import (
    CONDITIONS_SCHEMA,
    EXCHANGES_SCHEMA,
    OPTION_CONTRACTS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive.session import MassiveSession


def _extract_option_underlying(sym: str) -> str | None:
    """Extract the underlying root ticker from an OCC option symbol.

    Args:
        sym: Canonical or raw OCC option symbol (e.g.,
            `"O:AAPL250117C00150000"` or `"AAPL250117C00150000"`).

    Returns:
        str | None: Leading alphabetic underlying ticker (e.g., `"AAPL"`), or
        `None` if no valid root prefix followed by an expiration digit exists.
    """
    body = sym.removeprefix("O:")
    alpha: list[str] = []
    for ch in body:
        if ch.isalpha():
            alpha.append(ch)
        else:
            break
    return "".join(alpha) if alpha and len(alpha) < len(body) else None


class OptionsClient(BaseDomainClient):
    """Domain client for US Options (`OPRA`) feeds (`m.options`).

    OCC option contract identifiers may be passed with or without the `"O:"`
    prefix (for example, `"O:AAPL250117C00150000"` or `"AAPL250117C00150000"`).
    ``OptionsClient`` normalizes every contract ticker to its canonical
    `"O:<OCC_SYMBOL>"` form across all tables and subscriptions.

    Example:
        Fetch the live option chain for `AAPL` and stream live quotes for its
        active contracts using a reactive Deephaven watchlist::

            chain = m.options.chain_snapshot("AAPL").snapshot()
            near_atm = chain.where("abs(Delta) > 0.25 && abs(Delta) < 0.75")
            live_quotes = m.options.quotes(near_atm).latest()
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the US Options (`OPRA`) domain client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
        super().__init__(session, domain="options")

    def _fetch_option_seed(
        self,
        symbols: Sequence[str],
        *,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        concrete = [s for s in symbols if s != "*"]
        if not concrete:
            return []

        def _fetch_one(sym: str) -> list[dict[str, Any]]:
            under = _extract_option_underlying(sym)
            path = (
                f"/v3/snapshot/options/{under}/{sym}"
                if under is not None
                else f"/v3/snapshot/options/{sym}"
            )
            return self._fetch_rest_rows(
                path,
                inject_symbol=sym,
                extra_fields=extra_fields,
            )

        if len(concrete) == 1:
            return _fetch_one(concrete[0])

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=min(len(concrete), 8)) as pool:
            for sym_rows in pool.map(_fetch_one, concrete):
                rows.extend(sym_rows)
        return rows

    def trades(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure an OPRA option tick-by-tick trades feed (`T.O:*`, `/v3/trades/{optionsTicker}`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_options_opra/trades_v1`
        or REST `/v3/trades/{optionsTicker}`).

        Args:
            contracts: OCC contract symbol (`"O:AAPL250117C00150000"`),
                iterable of symbols, `"*"` for all OPRA contracts, or a
                reactive Deephaven `Table` watchlist (such as a filtered
                `chain_snapshot` table).
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `TRADES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("options", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            return f"/v3/trades/{sym}", {
                "timestamp.gte": _format_date_param(start),
                "timestamp.lte": _format_date_param(end),
            }

        return self._build_feed(
            name="options_trades",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_option_seed,
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    contracts,
                    TRADES_SCHEMA,
                    "us_options_opra/trades_v1",
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
        contracts: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure an OPRA option NBBO quotes feed (`Q.O:*`, `/v3/quotes/{optionsTicker}`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_options_opra/quotes_v1`
        or REST `/v3/quotes/{optionsTicker}`).

        Args:
            contracts: OCC contract symbol (`"O:AAPL250117C00150000"`),
                iterable of symbols, `"*"` for all OPRA contracts, or a
                reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `QUOTES_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("options", feed_override=feed)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            return f"/v3/quotes/{sym}", {
                "timestamp.gte": _format_date_param(start),
                "timestamp.lte": _format_date_param(end),
            }

        return self._build_feed(
            name="options_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_option_seed,
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    contracts,
                    QUOTES_SCHEMA,
                    "us_options_opra/quotes_v1",
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
        contracts: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        adjusted: bool = True,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure an OPRA option OHLCV bars feed (`A.O:*` / `AM.O:*`, `/v2/aggs/...`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()` (second or
        minute aggregates), `.snapshot()`, `.poll()`, and `.historical()`
        (routed to S3 `us_options_opra/minute_aggs_v1` or
        `us_options_opra/day_aggs_v1` when `multiplier == 1` and
        `adjusted is False`, or REST `/v2/aggs/...`).

        Args:
            contracts: OCC contract symbol, iterable of symbols, `"*"` for all
                contracts, or a reactive Deephaven `Table` watchlist.
            multiplier: Number of `timespan` units per bar. Defaults to `1`.
            timespan: Bar duration unit (`"second"`, `"minute"`, `"hour"`,
                `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`). Defaults
                to `"minute"`.
            adjusted: Whether to adjust historical bars for splits. Defaults
                to `True`.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `BARS_SCHEMA`.
        """
        clean_span = timespan.strip().lower()
        if multiplier == 1 and clean_span in ("second", "minute"):
            ws_channel: str | None = "A" if clean_span == "second" else "AM"
            ws_url: str | None = self.session.config._resolve_ws_url("options", feed_override=feed)
        else:
            ws_channel = None
            ws_url = None

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_options_opra/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_options_opra/day_aggs_v1"

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            s_str = _format_date_param(start) or "1970-01-01"
            e_str = _format_date_param(end) or datetime.now(timezone.utc).date().isoformat()
            return (
                f"/v2/aggs/ticker/{sym}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {"adjusted": str(adjusted).lower()},
            )

        span_ns = _compute_bar_span_ns(multiplier, clean_span)
        return self._build_feed(
            name=f"options_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_option_seed(syms, extra_fields={"_span_ns": span_ns}),
            seed_rows_fn=lambda syms, since: self._fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self._fetch_multi_symbol_historical(
                    contracts,
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

    def fmv(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure an OPRA Option Fair Market Value (`FMV.O:*`) stream on `business.massive.com`.

        Args:
            contracts: OCC contract symbol, iterable of symbols, `"*"` for all
                contracts, or a reactive Deephaven `Table` watchlist.
            feed: Optional feed tier override (`"realtime"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.latest()`,
            `.history()`, `.ring()`, and `.blink()` with `FMV_SCHEMA`.
        """
        ws_url = self.session.config._resolve_ws_url("options", feed_override=feed, is_fmv=True)
        return self._build_feed(
            name="options_fmv",
            schema_spec=FMV_SCHEMA,
            ws_url=ws_url,
            ws_channel="FMV",
            symbols=contracts,
            symbol_col=symbol_col,
        )

    def chain_snapshot(
        self,
        underlying: str,
        *,
        strike_price: float | None = None,
        expiration_date: str | None = None,
        contract_type: str | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a full option-chain snapshot feed for `underlying` (`/v3/snapshot/options/{underlyingAsset}`).

        Returns every matching option contract along with Greeks (`Delta`,
        `Gamma`, `Theta`, `Vega`), `ImpliedVolatility`, `OpenInterest`,
        `BreakEvenPrice`, latest trade/quote, and underlying price.

        Args:
            underlying: Underlying equity or index ticker (e.g., `"AAPL"`).
            strike_price: Optional exact strike price filter. Range filters
                such as `strike_price__gte=150.0` may be passed via `**filters`.
            expiration_date: Optional expiration date (`"YYYY-MM-DD"`). Range
                filters such as `expiration_date__lte="2025-06-20"` may be
                passed via `**filters`.
            contract_type: Optional `"call"` or `"put"` filter.
            **filters: Additional Massive REST query parameters (supports
                `__gte`, `__lte`, `__gt`, `__lt` suffixes).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `OPTION_CHAIN_SCHEMA`.
        """
        clean_under = underlying.strip().upper()
        params = _normalize_filter_params(
            {
                "strike_price": strike_price,
                "expiration_date": expiration_date,
                "contract_type": contract_type,
            },
            filters,
        )
        return self._build_feed(
            name=f"options_chain_{clean_under}",
            schema_spec=OPTION_CHAIN_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v3/snapshot/options/{clean_under}",
                params,
                extra_fields={"UnderlyingSymbol": clean_under},
            ),
        )

    chain = chain_snapshot

    def contract_snapshot(self, underlying: str, contract: str) -> FeedBuilder:
        """Configure a single-contract option snapshot feed (`/v3/snapshot/options/{underlyingAsset}/{optionContract}`).

        Args:
            underlying: Underlying equity or index ticker (e.g., `"AAPL"`).
            contract: OCC option contract symbol (with or without `"O:"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `OPTION_CHAIN_SCHEMA`.
        """
        clean_under = underlying.strip().upper()
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        return self._build_feed(
            name=f"options_contract_snapshot_{canon}",
            schema_spec=OPTION_CHAIN_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v3/snapshot/options/{clean_under}/{canon}",
                inject_symbol=canon,
                extra_fields={"UnderlyingSymbol": clean_under},
            ),
        )

    def contracts(
        self,
        underlying: str | None = None,
        *,
        expiration_date: str | None = None,
        contract_type: str | None = None,
        active: bool = True,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure an OPRA option contracts reference feed (`/v3/reference/options/contracts`).

        Args:
            underlying: Optional underlying root ticker filter (e.g., `"AAPL"`).
            expiration_date: Optional expiration date filter (`"YYYY-MM-DD"`).
            contract_type: Optional `"call"` or `"put"` filter.
            active: If `True` (default), return unexpired contracts; if
                `False`, return expired contracts.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `OPTION_CONTRACTS_SCHEMA`.
        """
        return self._rest_collection_feed(
            "options_contracts",
            "/v3/reference/options/contracts",
            OPTION_CONTRACTS_SCHEMA,
            {
                "underlying_ticker": underlying.strip().upper() if underlying else None,
                "expiration_date": expiration_date,
                "contract_type": contract_type,
                "expired": str(not active).lower(),
            },
            filters=filters,
            date_param="expiration_date",
        )

    def contract_details(self, contract: str) -> FeedBuilder:
        """Configure a reference metadata feed for a single OPRA option contract (`/v3/reference/options/contracts/{options_ticker}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `OPTION_CONTRACTS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        return self._build_feed(
            name=f"options_contract_details_{canon}",
            schema_spec=OPTION_CONTRACTS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v3/reference/options/contracts/{canon}",
                inject_symbol=canon,
            ),
        )

    def previous_close(self, contract: str, *, adjusted: bool = True) -> FeedBuilder:
        """Configure a feed for an option contract's previous trading day OHLCV bar (`/v2/aggs/ticker/{optionsTicker}/prev`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `BARS_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        params = {"adjusted": str(adjusted).lower()}
        return self._build_feed(
            name=f"options_prev_close_{canon}",
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
        contract: str,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Configure a daily open/close summary feed for an option contract (`/v1/open-close/{optionsTicker}/{date}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
            target_date: Calendar date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            adjusted: Whether to adjust for splits. Defaults to `True`.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `DAILY_SUMMARY_SCHEMA`.
        """
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        d_str = _format_date_param(target_date)
        params = {"adjusted": str(adjusted).lower()}
        return self._build_feed(
            name=f"options_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self._fetch_rest_rows(
                f"/v1/open-close/{canon}/{d_str}",
                params,
                inject_symbol=canon,
            ),
        )

    daily_ticker_summary = daily_open_close

    def exchanges(self) -> FeedBuilder:
        """Configure an Options exchange directory feed (`/v3/reference/exchanges?asset_class=options`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `EXCHANGES_SCHEMA`.
        """
        return self._build_feed(
            name="options_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                "/v3/reference/exchanges", {"asset_class": "options"}
            ),
        )

    def conditions(self) -> FeedBuilder:
        """Configure an Options trade and quote condition codes feed (`/v3/reference/conditions?asset_class=options`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `CONDITIONS_SCHEMA`.
        """
        return self._build_feed(
            name="options_conditions",
            schema_spec=CONDITIONS_SCHEMA,
            snapshot_fn=lambda: self._fetch_rest_rows(
                "/v3/reference/conditions", {"asset_class": "options"}
            ),
        )

    def sma(
        self,
        contract: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Simple Moving Average (`SMA`) indicator feed for an option contract (`/v1/indicators/sma/{optionsTicker}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
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
            contract,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def ema(
        self,
        contract: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure an Exponential Moving Average (`EMA`) indicator feed for an option contract (`/v1/indicators/ema/{optionsTicker}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
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
            contract,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )

    def macd(
        self,
        contract: str,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Moving Average Convergence/Divergence (`MACD`) indicator feed for an option contract (`/v1/indicators/macd/{optionsTicker}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
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
            contract,
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
        contract: str,
        *,
        window: int = 14,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """Configure a Relative Strength Index (`RSI`) indicator feed for an option contract (`/v1/indicators/rsi/{optionsTicker}`).

        Args:
            contract: OCC option contract symbol (with or without `"O:"`).
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
            contract,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
