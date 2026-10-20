"""Global Crypto domain client (`m.crypto`) covering trades, quotes, bars, FMV, snapshots, and indicators."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
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
    INDICATOR_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class CryptoClient(BaseDomainClient):
    """Domain client for Global Crypto (`BTC-USD`, `ETH-USD`, etc.)."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="crypto")

    def _fetch_crypto_snapshots(
        self, pairs: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if pairs and "*" not in pairs:
            params["tickers"] = ",".join(
                self.symbol_normalizer.rest_ticker(p)  # type: ignore[union-attr]
                for p in pairs
            )
        return self.fetch_rest_rows(
            "/v2/snapshot/locale/global/markets/crypto/tickers", params
        )

    def _historical_crypto(
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
        canon_syms = (
            None
            if hasattr(pairs, "j_table") or hasattr(pairs, "column_names")
            else self.canonical_symbols(pairs)
        )
        if self.should_use_flat_files(source, canon_syms, s3_dataset):
            assert s3_dataset is not None
            return self.session.flat_files.load(
                s3_dataset,
                start=start,
                end=end,
                symbols=canon_syms,
                schema_spec=schema_spec,
                domain="crypto",
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(
                "Specific crypto pair(s) are required for REST historical queries."
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
        pairs: str | Iterable[str] | Table = "*",
        *,
        feed: str | None = None,
        symbol_col: str = "Symbol",
    ) -> FeedBuilder:
        """Stream, snapshot, or fetch historical Crypto trades (`XT.*`, `/v3/trades/{cryptoTicker}`)."""
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
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_crypto(
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
        """Stream or snapshot Crypto BBO quotes (`XQ.*`)."""
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
        """Stream, snapshot, or fetch historical Crypto OHLCV bars (`XAS.*` / `XA.*`, `/v2/aggs/...`)."""
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

        return self.build_feed(
            name=f"crypto_bars_{multiplier}_{clean_span}",
            schema_spec=BARS_SCHEMA,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=pairs,
            symbol_col=symbol_col,
            seed_fn=self._fetch_crypto_snapshots,
            historical_fn=lambda start=None, end=None, limit=None, source="auto": self._historical_crypto(
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
        """Stream Crypto Fair Market Value (`FMV.*` on `business.massive.com`)."""
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
        """Snapshot or poll Crypto pairs (including top gainers/losers)."""
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

    def daily_market_summary(
        self, target_date: str | date | datetime, *, adjusted: bool = True
    ) -> FeedBuilder:
        """Grouped daily summary for all Crypto pairs on `target_date` (`/v2/aggs/grouped/locale/global/market/crypto/{date}`)."""
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

    def daily_open_close(
        self,
        pair: str,
        target_date: str | date | datetime,
        *,
        adjusted: bool = True,
    ) -> FeedBuilder:
        """Daily open/close for a Crypto pair (`/v1/open-close/crypto/{from}/{to}/{date}`)."""
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
        return self.build_feed(
            name=f"crypto_{indicator.lower()}_{canon}",
            schema_spec=INDICATOR_SCHEMA,
            symbols=canon,
            snapshot_fn=lambda: self.fetch_rest_rows(
                f"/v1/indicators/{indicator.lower()}/{ticker}",
                params,
                inject_symbol=canon,
                extra_fields=extra,
            ),
        )

    def sma(
        self,
        pair: str,
        *,
        window: int = 50,
        timespan: str = "day",
        series_type: str = "close",
    ) -> FeedBuilder:
        """SMA indicator for a Crypto pair."""
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
        """EMA indicator for a Crypto pair."""
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
        """MACD indicator for a Crypto pair."""
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
        """RSI indicator for a Crypto pair."""
        return self._indicator_feed(
            "RSI",
            pair,
            {"window": window, "timespan": timespan, "series_type": series_type},
            window,
            timespan,
            series_type,
        )
