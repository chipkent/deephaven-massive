"""US Options (OPRA) domain client (`m.options`) covering trades, quotes, bars, FMV, chain snapshots, and contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
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
    FMV_SCHEMA,
    INDICATOR_SCHEMA,
    OPTION_CHAIN_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
)
from deephaven_massive.schemas.reference import OPTION_CONTRACTS_SCHEMA

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class OptionsClient(BaseDomainClient):
    """Domain client for US Options (OPRA) real-time, delayed, historical, chain snapshot, and contract feeds."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="options")

    def _fetch_option_seed(self, symbols: Sequence[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for sym in symbols:
            if sym == "*":
                continue
            rows.extend(
                self.fetch_rest_rows(
                    f"/v3/snapshot/options/{sym}", inject_symbol=sym
                )
            )
        return rows

    def _historical_options(
        self,
        contracts: Any,
        schema_spec: SchemaSpec,
        s3_dataset: str | None,
        rest_path_fn: Any,
        *,
        start: Any = None,
        end: Any = None,
        limit: int | None = None,
        source: str = "auto",
    ) -> Table:
        canon_syms = (
            None
            if hasattr(contracts, "j_table") or hasattr(contracts, "column_names")
            else self.canonical_symbols(contracts)
        )
        if self.should_use_flat_files(source, canon_syms, s3_dataset):
            assert s3_dataset is not None
            return self.session.flat_files.load(
                s3_dataset,
                start=start,
                end=end,
                symbols=canon_syms,
                schema_spec=schema_spec,
                domain="options",
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(
                "Specific option contract(s) are required for REST historical queries."
            )

        if len(target_syms) == 1:
            sym = target_syms[0]
            path, params = rest_path_fn(sym, start, end)
            return self.fetch_chunked_table(
                path, schema_spec, params, limit=limit, inject_symbol=sym
            )

        all_rows: list[dict[str, Any]] = []
        for sym in target_syms:
            path, params = rest_path_fn(sym, start, end)
            all_rows.extend(
                self.fetch_rest_rows(path, params, limit=limit, inject_symbol=sym)
            )
        batches = (
            [rows_to_record_batch(all_rows, schema_spec, self.symbol_normalizer)]
            if all_rows
            else []
        )
        pa_tbl = record_batches_to_arrow_table(batches, schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

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
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_options(
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
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_options(
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

        return self.build_feed(
            name=f"options_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=contracts,
            symbol_col=symbol_col,
            seed_fn=self._fetch_option_seed,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_options(
                contracts,
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
    ) -> FeedBuilder:
        """Snapshot or poll the full option chain (with Greeks, IV, OI, break-even) for `underlying`."""
        clean_under = underlying.strip().upper()
        params: dict[str, Any] = {
            "strike_price": strike_price,
            "expiration_date": expiration_date,
            "contract_type": contract_type,
        }
        return self.build_feed(
            name=f"options_chain_{clean_under}",
            schema_spec=OPTION_CHAIN_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v3/snapshot/options/{clean_under}",
                params,
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
    ) -> FeedBuilder:
        """Fetch active or historical OPRA option contracts (`/v3/reference/options/contracts`)."""
        params: dict[str, Any] = {
            "underlying_ticker": underlying.strip().upper()
            if underlying
            else None,
            "expiration_date": expiration_date,
            "contract_type": contract_type,
            "expired": str(not active).lower(),
        }
        return self.build_feed(
            name="options_contracts",
            schema_spec=OPTION_CONTRACTS_SCHEMA,
            snapshot_fn=lambda: self.fetch_rest_rows(
                "/v3/reference/options/contracts", params
            ),
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self.fetch_chunked_table(
                "/v3/reference/options/contracts",
                OPTION_CONTRACTS_SCHEMA,
                {
                    **params,
                    **(
                        {"expiration_date.gte": format_date_param(start)}
                        if start is not None
                        else {}
                    ),
                    **(
                        {"expiration_date.lte": format_date_param(end)}
                        if end is not None
                        else {}
                    ),
                },
                limit=limit,
            ),
        )

    def _indicator_feed(
        self,
        indicator: str,
        contract: str,
        params: dict[str, Any],
        window: int,
        timespan: str,
        series_type: str,
    ) -> FeedBuilder:
        canon = self.symbol_normalizer.canonical(contract)  # type: ignore[union-attr]
        extra = {
            "Indicator": indicator.upper(),
            "Window": window,
            "Timespan": timespan,
            "SeriesType": series_type,
        }
        return self.build_feed(
            name=f"options_{indicator.lower()}_{canon}",
            schema_spec=INDICATOR_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/indicators/{indicator.lower()}/{canon}",
                params,
                inject_symbol=canon,
                extra_fields=extra,
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
