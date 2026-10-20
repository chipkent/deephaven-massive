"""US Options (OPRA) domain client (`m.options`) covering trades, quotes, bars, FMV, chain snapshots, and contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    format_date_param,
    normalize_filter_params,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    OPTION_CHAIN_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
    compute_bar_span_ns,
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
    """Extract the underlying root ticker from an OCC option symbol (`O:AAPL250117C00150000` -> `AAPL`)."""
    body = sym[2:] if sym.startswith("O:") else sym
    alpha: list[str] = []
    for ch in body:
        if ch.isalpha():
            alpha.append(ch)
        else:
            break
    return "".join(alpha) if alpha and len(alpha) < len(body) else None


class OptionsClient(BaseDomainClient):
    """Domain client for US Options (OPRA) real-time, delayed, historical, chain snapshot, and contract feeds."""

    def __init__(self, session: MassiveSession) -> None:
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
            return self.fetch_rest_rows(
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
        """Stream, snapshot, or fetch historical OPRA option trades (`T.O:*`, `/v3/trades/{optionsTicker}`)."""
        ws_url = self.session.config.resolve_ws_url("options", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return f"/v3/trades/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name="options_trades",
            schema_spec=TRADES_SCHEMA,
            ws_url=ws_url,
            ws_channel="T",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_option_seed,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, _rest_path
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_multi_symbol_historical(
                contracts,
                TRADES_SCHEMA,
                "us_options_opra/trades_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
            ),
        )

    def quotes(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical OPRA option quotes (`Q.O:*`, `/v3/quotes/{optionsTicker}`)."""
        ws_url = self.session.config.resolve_ws_url("options", feed_override=feed)

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            return f"/v3/quotes/{sym}", {
                "timestamp.gte": format_date_param(start),
                "timestamp.lte": format_date_param(end),
            }

        return self.build_feed(
            name="options_quotes",
            schema_spec=QUOTES_SCHEMA,
            ws_url=ws_url,
            ws_channel="Q",
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_option_seed,
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms, since, _rest_path
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_multi_symbol_historical(
                contracts,
                QUOTES_SCHEMA,
                "us_options_opra/quotes_v1",
                _rest_path,
                start=start,
                end=end,
                limit=limit,
                source=source,
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
        """Stream, snapshot, or fetch historical OPRA option OHLCV bars (`A.O:*` / `AM.O:*`)."""
        clean_span = timespan.strip().lower()
        ws_channel = "A" if clean_span == "second" else "AM"
        ws_url = self.session.config.resolve_ws_url("options", feed_override=feed)

        s3_ds: str | None = None
        if multiplier == 1 and clean_span == "minute":
            s3_ds = "us_options_opra/minute_aggs_v1"
        elif multiplier == 1 and clean_span == "day":
            s3_ds = "us_options_opra/day_aggs_v1"

        def _rest_path(
            sym: str, start: Any, end: Any
        ) -> tuple[str, dict[str, Any]]:
            s_str = format_date_param(start) or "1970-01-01"
            e_str = format_date_param(end) or s_str
            return (
                f"/v2/aggs/ticker/{sym}/range/{multiplier}/{clean_span}/{s_str}/{e_str}",
                {"adjusted": str(adjusted).lower()},
            )

        span_ns = compute_bar_span_ns(multiplier, clean_span)
        return self.build_feed(
            name=f"options_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=lambda syms: self._fetch_option_seed(
                syms, extra_fields={"_span_ns": span_ns}
            ),
            seed_rows_fn=lambda syms, since: self.fetch_seed_rows(
                syms,
                since,
                _rest_path,
                extra_fields={"_span_ns": span_ns},
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_multi_symbol_historical(
                contracts,
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

    def fmv(
        self,
        contracts: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream OPRA Option Fair Market Value (`FMV.O:*` on `business.massive.com`)."""
        ws_url = self.session.config.resolve_ws_url(
            "options", feed_override=feed, is_fmv=True
        )
        return self.build_feed(
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
        """Snapshot or poll the full option chain (with Greeks, IV, OI, break-even) for `underlying`."""
        clean_under = underlying.strip().upper()
        params = normalize_filter_params(
            {
                "strike_price": strike_price,
                "expiration_date": expiration_date,
                "contract_type": contract_type,
            },
            filters,
        )
        return self.build_feed(
            name=f"options_chain_{clean_under}",
            schema_spec=OPTION_CHAIN_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v3/snapshot/options/{clean_under}",
                params,
                extra_fields={"UnderlyingSymbol": clean_under},
            ),
        )

    chain = chain_snapshot

    def contract_snapshot(
        self, underlying: str, contract: str
    ) -> FeedBuilder:
        """Snapshot or poll a single OPRA option contract (`/v3/snapshot/options/{underlyingAsset}/{optionContract}`)."""
        clean_under = underlying.strip().upper()
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        return self.build_feed(
            name=f"options_contract_snapshot_{canon}",
            schema_spec=OPTION_CHAIN_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Fetch active or historical OPRA option contracts (`/v3/reference/options/contracts`)."""
        return self._rest_collection_feed(
            "options_contracts",
            "/v3/reference/options/contracts",
            OPTION_CONTRACTS_SCHEMA,
            {
                "underlying_ticker": underlying.strip().upper()
                if underlying
                else None,
                "expiration_date": expiration_date,
                "contract_type": contract_type,
                "expired": str(not active).lower(),
            },
            filters=filters,
            date_param="expiration_date",
        )

    def contract_details(self, contract: str) -> FeedBuilder:
        """Fetch reference metadata for a single OPRA option contract (`/v3/reference/options/contracts/{options_ticker}`)."""
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        return self.build_feed(
            name=f"options_contract_details_{canon}",
            schema_spec=OPTION_CONTRACTS_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v3/reference/options/contracts/{canon}",
                inject_symbol=canon,
            ),
        )

    def previous_close(
        self, contract: str, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Previous day's OHLCV bar for an option contract (`/v2/aggs/ticker/{optionsTicker}/prev`)."""
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        params = {"adjusted": str(adjusted).lower()}
        return self.build_feed(
            name=f"options_prev_close_{canon}",
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
        contract: str,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Daily open/close summary for an option contract (`/v1/open-close/{optionsTicker}/{date}`)."""
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        d_str = format_date_param(target_date)
        params = {"adjusted": str(adjusted).lower()}
        return self.build_feed(
            name=f"options_open_close_{canon}",
            schema_spec=DAILY_SUMMARY_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/open-close/{canon}/{d_str}",
                params,
                inject_symbol=canon,
            ),
        )

    daily_ticker_summary = daily_open_close

    def exchanges(self) -> FeedBuilder:
        """Options exchange directory (`/v3/reference/exchanges`)."""
        return self.build_feed(
            name="options_exchanges",
            schema_spec=EXCHANGES_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/exchanges", {"asset_class": "options"}
            ),
        )

    def conditions(self) -> FeedBuilder:
        """Options trade and quote condition codes (`/v3/reference/conditions`)."""
        return self.build_feed(
            name="options_conditions",
            schema_spec=CONDITIONS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
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
        """Simple Moving Average (SMA) for an option contract."""
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
        """Exponential Moving Average (EMA) for an option contract."""
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
        """MACD indicator for an option contract."""
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
        """RSI indicator for an option contract."""
        return self._indicator_feed(
            "RSI",
            contract,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
