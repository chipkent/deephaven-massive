"""US Futures (`CME`, `CBOT`, `NYMEX`, `COMEX`) domain client (`m.futures`).

This module implements :class:`FuturesClient`, which exposes real-time and
delayed WebSocket streams, REST snapshots, historical backfills (via S3 Flat
Files or REST), and contract/product reference catalogs across CME Group
exchanges (`cme`, `cbot`, `nymex`, and `comex`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    format_date_param,
    normalize_filter_params,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    FUTURES_SNAPSHOT_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
    compute_bar_span_ns,
)
from deephaven_massive.schemas.reference import (
    EXCHANGES_SCHEMA,
    FUTURES_CONTRACTS_SCHEMA,
    FUTURES_PRODUCTS_SCHEMA,
    FUTURES_SCHEDULES_SCHEMA,
    MARKET_STATUS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive.session import MassiveSession


class FuturesClient(BaseDomainClient):
    """Domain client for US Futures (`cme`, `cbot`, `nymex`, `comex`) feeds.

    ``FuturesClient`` routes streaming subscriptions to the venue-specific
    WebSocket endpoint (for example, ``wss://socket.massive.com/futures/cme``)
    and normalizes futures contract tickers (such as ``"ESZ5"`` or ``"CLF6"``)
    to canonical uppercase strings.

    Example:
        Stream live CME E-mini S&P 500 trades seeded with the latest snapshot::

            es_trades = m.futures.trades(["ESZ5", "NQZ5"], venue="cme").latest()
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the US Futures domain client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
        super().__init__(session, domain="futures")

    def _fetch_futures_snapshots(
        self,
        symbols: Sequence[str] | None = None,
        *,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbols and "*" not in symbols:
            params["ticker.any_of"] = ",".join(symbols)
        return self.fetch_rest_rows("/futures/vX/snapshot", params, extra_fields=extra_fields)

    def trades(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Futures tick-by-tick trades feed (`T.*`, `/futures/vX/trades`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_futures/trades_v1` or
        REST `/futures/vX/trades/{contract}`).

        Args:
            contracts: Contract symbol (`"ESZ5"`), iterable of symbols,
                `"*"` for all contracts on `venue`, or a reactive Deephaven
                `Table` watchlist.
            venue: CME Group exchange code (`"cme"`, `"cbot"`, `"nymex"`, or
                `"comex"`). Defaults to `"cme"`.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `TRADES_SCHEMA`.
        """
        ws_url = self.session.config.resolve_ws_url("futures", feed_override=feed, venue=venue)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/trades/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name=f"futures_trades_{venue.lower()}",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_futures_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self.fetch_multi_symbol_historical(
                    contracts,
                    TRADES_SCHEMA,
                    "us_futures/trades_v1",
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                )
            ),
        )

    def quotes(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Futures top-of-book quotes feed (`Q.*`, `/futures/vX/quotes`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()`, `.snapshot()`,
        `.poll()`, and `.historical()` (backed by S3 `us_futures/quotes_v1` or
        REST `/futures/vX/quotes/{contract}`).

        Args:
            contracts: Contract symbol (`"ESZ5"`), iterable of symbols,
                `"*"` for all contracts on `venue`, or a reactive Deephaven
                `Table` watchlist.
            venue: CME Group exchange code (`"cme"`, `"cbot"`, `"nymex"`, or
                `"comex"`). Defaults to `"cme"`.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `QUOTES_SCHEMA`.
        """
        ws_url = self.session.config.resolve_ws_url("futures", feed_override=feed, venue=venue)

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/quotes/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name=f"futures_quotes_{venue.lower()}",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_futures_snapshots,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(syms, since, _rest_path),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self.fetch_multi_symbol_historical(
                    contracts,
                    QUOTES_SCHEMA,
                    "us_futures/quotes_v1",
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                )
            ),
        )

    def bars(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        multiplier: int = 1,
        timespan: str = "minute",
        venue: str = "cme",
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Configure a US Futures OHLCV + settlement bars feed (`A.*` / `AM.*`, `/futures/vX/aggs`).

        Supports `.latest()`, `.history()`, `.ring()`, `.blink()` (second or
        minute aggregates), `.snapshot()`, `.poll()`, and `.historical()`
        (routed to S3 `us_futures/minute_aggs_v1`, `us_futures/day_aggs_v1`, or
        `us_futures/session_aggs_v1` when `multiplier == 1`, or REST
        `/futures/vX/aggs/{contract}`).

        Args:
            contracts: Contract symbol (`"ESZ5"`), iterable of symbols,
                `"*"` for all contracts on `venue`, or a reactive Deephaven
                `Table` watchlist.
            multiplier: Number of `timespan` units per bar. Defaults to `1`.
            timespan: Bar duration unit (`"second"`, `"minute"`, `"hour"`,
                `"day"`, or `"session"`). Defaults to `"minute"`.
            venue: CME Group exchange code (`"cme"`, `"cbot"`, `"nymex"`, or
                `"comex"`). Defaults to `"cme"`.
            feed: Optional feed tier override (`"realtime"` or `"delayed"`).
            symbol_col: Column name to watch when `contracts` is a Deephaven
                `Table`. Defaults to `"Symbol"`.

        Returns:
            FeedBuilder: Configured feed builder producing a table matching
            `BARS_SCHEMA` (including `SettlementPrice` when reported).
        """
        clean_span = timespan.strip().lower()
        ws_channel = "A" if clean_span == "second" else "AM"
        ws_url = self.session.config.resolve_ws_url("futures", feed_override=feed, venue=venue)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_futures/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_futures/day_aggs_v1"
        elif multiplier == 1 and clean_span == "session":
            s3_ds = "us_futures/session_aggs_v1"

        span_unit = "day" if clean_span == "session" else clean_span

        def _rest_path(sym: str, start: Any, end: Any) -> tuple[str, dict[str, Any]]:
            return f"/futures/vX/aggs/{sym}", {
                "resolution": f"{multiplier}_{span_unit}",
                "window_start.gte": format_date_param(start),
                "window_start.lte": format_date_param(end),
            }

        span_ns = compute_bar_span_ns(multiplier, span_unit)
        return self.build_feed(
            name=f"futures_bars_{venue.lower()}_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_futures_snapshots(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": (
                self.fetch_multi_symbol_historical(
                    contracts,
                    BARS_SCHEMA,
                    s3_ds,
                    _rest_path,
                    start=start,
                    end=end,
                    limit=limit,
                    source=source,
                )
            ),
        )

    def snapshots(self, contracts: str | Iterable[str] | None = None) -> FeedBuilder:
        """Configure a US Futures contract snapshot feed (`/futures/vX/snapshot`).

        Args:
            contracts: Optional contract symbol or iterable of contract symbols
                to filter. When `None`, returns snapshots for all active
                futures contracts.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `FUTURES_SNAPSHOT_SCHEMA`.
        """
        canon = self.canonical_symbols(contracts) if contracts is not None else None
        return self.build_feed(
            name="futures_snapshots",
            schema_spec=FUTURES_SNAPSHOT_SCHEMA,
            symbols=contracts,
            snapshot_fn=lambda: self._fetch_futures_snapshots(canon),
        )

    def contracts(
        self,
        product_code: str | None = None,
        *,
        active: bool = True,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a Futures contract specifications feed (`/futures/vX/contracts`).

        Returns tick size, point value multiplier, settlement type, and
        first/last trade dates for listed futures contracts.

        Args:
            product_code: Optional root product code filter (e.g., `"ES"`,
                `"CL"`, `"GC"`).
            active: If `True` (default), restrict to currently tradable
                contracts.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `FUTURES_CONTRACTS_SCHEMA`.
        """
        params = normalize_filter_params(
            {
                "product_code": product_code.strip().upper() if product_code else None,
                "active": str(active).lower(),
            },
            filters,
        )
        return self.build_feed(
            name="futures_contracts",
            schema_spec=FUTURES_CONTRACTS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/futures/vX/contracts", params),
        )

    def products(self, *, venue: str | None = None, **filters: Any) -> FeedBuilder:
        """Configure a Futures root product catalog feed (`/futures/vX/products`).

        Args:
            venue: Optional exchange/market sector filter (e.g., `"CME"`,
                `"CBOT"`, `"NYMEX"`, `"COMEX"`).
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `FUTURES_PRODUCTS_SCHEMA`.
        """
        params = normalize_filter_params(
            {"market_sector": venue.upper() if venue else None},
            filters,
        )
        return self.build_feed(
            name="futures_products",
            schema_spec=FUTURES_PRODUCTS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/futures/vX/products", params),
        )

    def schedules(self, product_code: str | None = None, **filters: Any) -> FeedBuilder:
        """Configure a Futures trading session schedules feed (`/futures/vX/schedules`).

        Args:
            product_code: Optional root product code filter (e.g., `"ES"`).
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `FUTURES_SCHEDULES_SCHEMA`.
        """
        params = normalize_filter_params(
            {"product_code": product_code.strip().upper() if product_code else None},
            filters,
        )
        return self.build_feed(
            name="futures_schedules",
            schema_spec=FUTURES_SCHEDULES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/futures/vX/schedules", params),
        )

    def exchanges(self) -> FeedBuilder:
        """Configure a Futures exchange directory feed (`/futures/vX/exchanges`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `EXCHANGES_SCHEMA`.
        """
        return self.build_feed(
            name="futures_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/futures/vX/exchanges"),
        )

    def market_status(self) -> FeedBuilder:
        """Configure a Futures exchange and product-group status feed (`/futures/vX/market-status`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()` and
            `.poll()` with `MARKET_STATUS_SCHEMA`.
        """
        return self.build_feed(
            name="futures_market_status",
            schema_spec=MARKET_STATUS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows("/futures/vX/market-status"),
        )
