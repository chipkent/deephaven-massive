"""Base domain client with raw-JSON REST pagination, chunked Arrow conversion, and S3 flat-file routing."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    record_batches_to_arrow_table,
    rows_to_record_batch,
)
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.market import compute_bar_span_ns

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession

VALID_HISTORICAL_SOURCES: frozenset[str] = frozenset({"auto", "rest", "flat_files"})


def _resolve_bar_extra_fields(
    path: str,
    params: Mapping[str, Any] | None,
    extra_fields: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """Attach `_span_ns` for aggregate bar REST requests when `path` or `params` encode a bar span."""
    span_ns: int | None = None
    if "/range/" in path:
        parts = path.split("/range/", 1)[1].split("/")
        span_ns = compute_bar_span_ns(int(parts[0]), parts[1])
    elif path.endswith("/prev"):
        span_ns = compute_bar_span_ns(1, "day")
    elif params and "resolution" in params:
        mult_s, span_s = str(params["resolution"]).split("_", 1)
        span_ns = compute_bar_span_ns(int(mult_s), span_s)
    if span_ns is None:
        return extra_fields
    merged = dict(extra_fields or {})
    merged.setdefault("_span_ns", span_ns)
    return merged


def format_date_param(val: str | date | datetime | int | None) -> str | None:
    """Format a start/end date or timestamp parameter for Massive REST endpoints."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    return str(val).strip()


_FILTER_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("_any_of", ".any_of"),
    ("_gte", ".gte"),
    ("_lte", ".lte"),
    ("_gt", ".gt"),
    ("_lt", ".lt"),
)


def normalize_filter_params(
    base_params: Mapping[str, Any] | None = None,
    filters: Mapping[str, Any] | None = None,
    *,
    symbol: str | None = None,
    tickers: str | Iterable[str] | None = None,
    normalizer: SymbolNormalizer | None = None,
    ticker_param: str = "ticker",
) -> dict[str, Any]:
    """Normalize REST query parameters, `tickers` lists, and `*_gte`/`*_lte`/`*_gt`/`*_lt`/`*_any_of` filter kwargs."""
    out: dict[str, Any] = {}
    if base_params:
        for k, v in base_params.items():
            if v is not None:
                out[k] = format_date_param(v) if isinstance(v, (date, datetime)) else v

    def _norm_sym(s: str) -> str:
        return (
            normalizer.rest_ticker(s)
            if normalizer is not None
            else s.strip().upper()
        )

    if tickers is not None:
        if isinstance(tickers, str):
            t_list = [_norm_sym(s) for s in tickers.split(",") if s.strip()]
        else:
            t_list = [_norm_sym(str(s)) for s in tickers if str(s).strip()]
        if len(t_list) == 1 and symbol is None:
            out[ticker_param] = t_list[0]
        elif len(t_list) > 1:
            key = (
                f"{ticker_param}.any_of"
                if ticker_param in ("ticker", "cik")
                else ticker_param
            )
            out[key] = ",".join(t_list)
    elif symbol is not None:
        out[ticker_param] = _norm_sym(symbol)

    if filters:
        for raw_k, raw_v in filters.items():
            if raw_v is None:
                continue
            mapped_k = raw_k
            for suffix, dot_suffix in _FILTER_SUFFIXES:
                if raw_k.endswith(suffix) and len(raw_k) > len(suffix):
                    mapped_k = raw_k[: -len(suffix)] + dot_suffix
                    break
            if isinstance(raw_v, (date, datetime)):
                out[mapped_k] = format_date_param(raw_v)
            elif isinstance(raw_v, Sequence) and not isinstance(raw_v, (str, bytes)):
                out[mapped_k] = ",".join(str(x) for x in raw_v)
            else:
                out[mapped_k] = raw_v
    return out


def extract_rows_from_payload(
    payload: Any,
    *,
    inject_symbol: str | None = None,
    extra_fields: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Extract a list of row dicts from a Massive REST JSON response payload."""
    if payload is None:
        return []

    raw_items: list[Any] = []
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, Mapping):
        if "results" in payload:
            res = payload["results"]
            if isinstance(res, list):
                raw_items = res
            elif isinstance(res, Mapping):
                if "values" in res and isinstance(res["values"], list):
                    raw_items = res["values"]
                else:
                    raw_items = [res]
        elif "tickers" in payload and isinstance(payload["tickers"], list):
            raw_items = payload["tickers"]
        elif "last" in payload and isinstance(payload["last"], Mapping):
            item = dict(payload["last"])
            if "symbol" in payload and "symbol" not in item:
                item["symbol"] = payload["symbol"]
            raw_items = [item]
        elif "ticker" in payload and isinstance(payload["ticker"], Mapping):
            raw_items = [payload["ticker"]]
        elif "status" in payload and len(payload) <= 2 and "serverTime" not in payload:
            raw_items = []
        else:
            raw_items = [payload]

    rows: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, Mapping):
            continue
        d = dict(item)
        if inject_symbol is not None and not any(
            k in d and d[k] is not None
            for k in ("Symbol", "sym", "ticker", "T", "S", "pair")
        ):
            d["Symbol"] = inject_symbol
        if extra_fields:
            for ek, ev in extra_fields.items():
                d.setdefault(ek, ev)
        rows.append(d)
    return rows


class BaseDomainClient:
    """Shared base class for Massive domain sub-clients (`stocks`, `options`, `futures`, etc.)."""

    def __init__(
        self,
        session: MassiveSession,
        domain: str | None = None,
    ) -> None:
        self.session = session
        self.domain = domain
        self.symbol_normalizer = (
            SymbolNormalizer(domain) if domain is not None else None
        )

    def canonical_symbols(
        self, symbols: str | Iterable[str] | None
    ) -> list[str]:
        """Normalize `symbols` into a deduplicated list of canonical symbols."""
        if symbols is None:
            return ["*"]
        if self.symbol_normalizer is not None:
            return self.symbol_normalizer.canonical_list(symbols)
        if isinstance(symbols, str):
            return [symbols.strip().upper()]
        return [str(s).strip().upper() for s in symbols]

    def fetch_rest_rows(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        inject_symbol: str | None = None,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch and paginate REST JSON results into a list of row dicts."""
        collected: list[dict[str, Any]] = []
        resolved_extra = _resolve_bar_extra_fields(path, params, extra_fields)
        next_path: str | None = path
        next_params: dict[str, Any] | None = (
            {k: v for k, v in params.items() if v is not None}
            if params
            else {}
        )
        if limit is not None and next_params is not None and "limit" not in next_params:
            next_params["limit"] = min(limit, 50_000)

        while next_path is not None:
            payload = self.session.request_json(next_path, next_params)
            page_rows = extract_rows_from_payload(
                payload,
                inject_symbol=inject_symbol,
                extra_fields=resolved_extra,
            )
            collected.extend(page_rows)
            if limit is not None and len(collected) >= limit:
                return collected[:limit]

            next_url = (
                payload.get("next_url") if isinstance(payload, Mapping) else None
            )
            if next_url and isinstance(next_url, str):
                next_path = next_url
                next_params = None
            else:
                next_path = None

        return collected

    def fetch_chunked_table(
        self,
        path: str,
        schema_spec: SchemaSpec,
        params: Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        inject_symbol: str | None = None,
        extra_fields: Mapping[str, Any] | None = None,
        chunk_size: int = 50_000,
    ) -> Table:
        """Paginate a REST endpoint and convert pages in bounded RecordBatch chunks into a Deephaven Table."""
        batches: list[pa.RecordBatch] = []
        buffer: list[dict[str, Any]] = []
        total_rows = 0
        resolved_extra = _resolve_bar_extra_fields(path, params, extra_fields)

        next_path: str | None = path
        next_params: dict[str, Any] | None = (
            {k: v for k, v in params.items() if v is not None}
            if params
            else {}
        )
        if limit is not None and next_params is not None and "limit" not in next_params:
            next_params["limit"] = min(limit, chunk_size)

        while next_path is not None:
            payload = self.session.request_json(next_path, next_params)
            page_rows = extract_rows_from_payload(
                payload,
                inject_symbol=inject_symbol,
                extra_fields=resolved_extra,
            )
            if limit is not None and total_rows + len(page_rows) > limit:
                page_rows = page_rows[: limit - total_rows]

            buffer.extend(page_rows)
            total_rows += len(page_rows)

            while len(buffer) >= chunk_size:
                chunk = buffer[:chunk_size]
                buffer = buffer[chunk_size:]
                batches.append(
                    rows_to_record_batch(
                        chunk, schema_spec, self.symbol_normalizer
                    )
                )

            if limit is not None and total_rows >= limit:
                break

            next_url = (
                payload.get("next_url") if isinstance(payload, Mapping) else None
            )
            if next_url and isinstance(next_url, str):
                next_path = next_url
                next_params = None
            else:
                next_path = None

        if buffer:
            batches.append(
                rows_to_record_batch(
                    buffer, schema_spec, self.symbol_normalizer
                )
            )

        pa_tbl = record_batches_to_arrow_table(batches, schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def should_use_flat_files(
        self,
        source: str,
        symbols: Sequence[str] | None,
        s3_dataset: str | None,
        *,
        adjusted: bool | None = None,
    ) -> bool:
        """Determine whether `.historical()` should route to S3 Flat Files or REST pagination."""
        clean_source = source.strip().lower()
        if clean_source not in VALID_HISTORICAL_SOURCES:
            raise ValueError(
                f"Unsupported historical source '{source}'. Expected one of {sorted(VALID_HISTORICAL_SOURCES)}."
            )
        if clean_source == "rest":
            return False
        if clean_source == "flat_files":
            if s3_dataset is None:
                raise ValueError("S3 Flat Files are not supported for this feed.")
            if not self.session.config.has_s3_credentials:
                raise ValueError(
                    "S3 credentials (s3_access_key_id / s3_secret_access_key) are required when source='flat_files'."
                )
            return True

        # source == "auto"
        if s3_dataset is None or not self.session.config.has_s3_credentials:
            return False
        if symbols is None or "*" in symbols:
            return True
        if adjusted is True:
            return False
        return len(symbols) >= self.session.config.flat_file_ticker_threshold

    def fetch_multi_symbol_historical(
        self,
        symbols: Any,
        schema_spec: SchemaSpec,
        s3_dataset: str | None,
        rest_path_fn: Callable[[str, Any, Any], tuple[str, dict[str, Any]]],
        *,
        start: Any = None,
        end: Any = None,
        limit: int | None = None,
        source: str = "auto",
        adjusted: bool | None = None,
        empty_error: str = "Specific symbol(s) are required for REST historical queries.",
        chunk_size: int = 50_000,
    ) -> Table:
        """Route a single- or multi-symbol `.historical()` query to S3 Flat Files or chunked REST pagination."""
        canon_syms = (
            None
            if hasattr(symbols, "j_table") or hasattr(symbols, "column_names")
            else self.canonical_symbols(symbols)
        )
        if self.should_use_flat_files(
            source, canon_syms, s3_dataset, adjusted=adjusted
        ):
            assert s3_dataset is not None
            return self.session.flat_files.load(
                s3_dataset,
                start=start,
                end=end,
                symbols=canon_syms,
                schema_spec=schema_spec,
                domain=self.domain,
                limit=limit,
            )

        target_syms = [s for s in (canon_syms or []) if s != "*"]
        if not target_syms:
            raise ValueError(empty_error)

        if len(target_syms) == 1:
            sym = target_syms[0]
            path, params = rest_path_fn(sym, start, end)
            return self.fetch_chunked_table(
                path,
                schema_spec,
                params,
                limit=limit,
                inject_symbol=sym,
                chunk_size=chunk_size,
            )

        batches: list[pa.RecordBatch] = []
        buffer: list[dict[str, Any]] = []
        total_rows = 0

        if limit is None:
            def _fetch_one(sym: str) -> list[dict[str, Any]]:
                path, params = rest_path_fn(sym, start, end)
                return self.fetch_rest_rows(path, params, inject_symbol=sym)

            with ThreadPoolExecutor(max_workers=min(len(target_syms), 8)) as pool:
                per_sym_rows = list(pool.map(_fetch_one, target_syms))
            for rows in per_sym_rows:
                buffer.extend(rows)
                while len(buffer) >= chunk_size:
                    chunk = buffer[:chunk_size]
                    buffer = buffer[chunk_size:]
                    batches.append(
                        rows_to_record_batch(
                            chunk, schema_spec, self.symbol_normalizer
                        )
                    )
        else:
            for sym in target_syms:
                rem = limit - total_rows
                path, params = rest_path_fn(sym, start, end)
                rows = self.fetch_rest_rows(
                    path, params, limit=rem, inject_symbol=sym
                )
                buffer.extend(rows)
                total_rows += len(rows)
                while len(buffer) >= chunk_size:
                    chunk = buffer[:chunk_size]
                    buffer = buffer[chunk_size:]
                    batches.append(
                        rows_to_record_batch(
                            chunk, schema_spec, self.symbol_normalizer
                        )
                    )
                if total_rows >= limit:
                    break

        if buffer:
            batches.append(
                rows_to_record_batch(
                    buffer, schema_spec, self.symbol_normalizer
                )
            )
        pa_tbl = record_batches_to_arrow_table(batches, schema_spec)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def build_feed(
        self,
        name: str,
        schema_spec: SchemaSpec,
        *,
        ws_url: str | None = None,
        ws_channel: str | None = None,
        symbols: Any = None,
        symbol_col: str = "Symbol",
        seed_fn: Callable[[list[str]], Sequence[Mapping[str, Any]]] | None = None,
        seed_rows_fn: Callable[[list[str], Any], Sequence[Mapping[str, Any]]]
        | None = None,
        snapshot_fn: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
        historical_fn: Callable[..., Table] | None = None,
    ) -> FeedBuilder:
        """Create a :class:`FeedBuilder` wired to the session's execution context and multiplexer."""
        return FeedBuilder(
            name=name,
            schema_spec=schema_spec,
            symbol_normalizer=self.symbol_normalizer,
            exec_ctx=self.session.exec_ctx,
            ws_multiplexer=self.session.ws_multiplexer,
            ws_url=ws_url,
            ws_channel=ws_channel,
            symbols=symbols,
            symbol_col=symbol_col,
            seed_fn=seed_fn,
            seed_rows_fn=seed_rows_fn,
            snapshot_fn=snapshot_fn,
            historical_fn=historical_fn,
            flush_interval_ms=self.session.config.flush_interval_ms,
            max_batch_size=self.session.config.max_batch_size,
            auto_flush=self.session.auto_flush,
            on_register_lifecycle=self.session.register_builder,
            stream_registry=self.session._stream_registry,
        )

    def fetch_seed_rows(
        self,
        syms: Sequence[str],
        seed_since: Any,
        rest_path_fn: Callable[[str, Any, Any], tuple[str, dict[str, Any]]],
        *,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch historical REST backfill rows since `seed_since` for each canonical symbol in `syms`."""
        concrete = [s for s in syms if s != "*"]
        if not concrete:
            return []

        def _fetch_one(sym: str) -> list[dict[str, Any]]:
            path, params = rest_path_fn(sym, seed_since, None)
            return self.fetch_rest_rows(
                path,
                params,
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
