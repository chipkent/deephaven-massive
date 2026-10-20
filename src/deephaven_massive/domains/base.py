"""Base domain client with raw-JSON REST pagination, chunked Arrow conversion, and S3 flat-file routing.

This module defines :class:`BaseDomainClient` and its supporting query/payload
helpers. Every asset-class and data-provider domain client (`stocks`, `options`,
`futures`, `indices`, `forex`, `crypto`, `economy`, `partners`) inherits from
:class:`BaseDomainClient` to share:
1. **Cursor Pagination with Bounded Memory**: ``fetch_chunked_table`` follows
   Massive ``next_url`` cursors and converts rows in 50,000-row PyArrow
   ``RecordBatch`` chunks so multi-million-row historical pulls never exhaust
   heap memory.
2. **Automatic S3 vs. REST Routing**: ``should_use_flat_files`` and
   ``fetch_multi_symbol_historical`` transparently route wildcard (``"*"``) and
   wide multi-symbol historical queries to S3 Flat Files when credentials are
   configured, or parallelize per-symbol REST requests across a bounded
   ``ThreadPoolExecutor``.
3. **Ergonomic Query Filter Translation**: ``normalize_filter_params`` converts
   Pythonic keyword arguments (such as ``filing_date_gte="2025-01-01"`` or
   ``ticker_any_of=["AAPL", "MSFT"]``) into Massive's dotted query syntax
   (``filing_date.gte``, ``ticker.any_of``).
"""

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
from deephaven_massive.schemas.market import (
    INDICATOR_SCHEMA,
    compute_bar_span_ns,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession

VALID_HISTORICAL_SOURCES: frozenset[str] = frozenset({"auto", "rest", "flat_files"})

_EMPTY_HISTORICAL_ERRORS: dict[str, str] = {
    "stocks": "Specific symbol(s) are required for REST historical queries (or configure S3 credentials for '*' flat files).",
    "options": "Specific option contract(s) are required for REST historical queries.",
    "futures": "Specific futures contract(s) are required for REST historical queries.",
    "indices": "Specific index symbol(s) are required for REST historical queries.",
    "forex": "Specific currency pair(s) are required for REST historical queries.",
    "crypto": "Specific crypto pair(s) are required for REST historical queries.",
}


def _resolve_bar_extra_fields(
    path: str,
    params: Mapping[str, Any] | None,
    extra_fields: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """Attach ``_span_ns`` for aggregate bar REST requests when ``path`` or ``params`` encode a bar span.

    Args:
        path: REST endpoint path (e.g., ``/v2/aggs/ticker/AAPL/range/5/minute/...``).
        params: Optional query parameter mapping (e.g., ``{"resolution": "1_minute"}``).
        extra_fields: Existing extra fields dictionary to augment.

    Returns:
        Mapping[str, Any] | None: Augmented extra fields mapping containing
        ``_span_ns`` if a bar duration could be inferred, otherwise
        ``extra_fields`` unchanged.
    """
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
    """Format a start/end date or timestamp parameter for Massive REST endpoints.

    Args:
        val: ISO-8601 string, ``datetime.date``, ``datetime.datetime``, epoch
            integer, or ``None``.

    Returns:
        str | None: ISO-8601 string or stringified epoch suitable for Massive
        REST query parameters, or ``None`` if ``val`` is ``None``.
    """
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
    """Normalize REST query parameters, ``tickers`` lists, and comparison filter kwargs.

    Translates Pythonic underscore filter suffixes (``_gte``, ``_lte``, ``_gt``,
    ``_lt``, ``_any_of``) into Massive's dotted REST query parameters
    (``.gte``, ``.lte``, ``.gt``, ``.lt``, ``.any_of``), formats ``date`` and
    ``datetime`` objects into ISO-8601 strings, and normalizes single or
    multi-ticker parameters via ``normalizer``.

    Args:
        base_params: Baseline query parameter dictionary.
        filters: Additional keyword filter arguments supplied by the caller.
        symbol: Optional single symbol filter.
        tickers: Optional comma-separated string or iterable of symbols.
        normalizer: Optional :class:`SymbolNormalizer` to translate canonical
            symbols into REST ticker parameters.
        ticker_param: Target query parameter name for symbols (e.g., ``"ticker"``
            or ``"composite_ticker"``).

    Returns:
        dict[str, Any]: Normalized query parameter dictionary ready for
        ``session.request_json``.
    """
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
    """Extract a uniform list of row dictionaries from a Massive REST JSON payload.

    Handles all envelope variations used across Massive v1, v2, v3, and vX
    endpoints—including ``{"results": [...]}``, ``{"results": {"values": [...]}}``
    (technical indicators), ``{"tickers": [...]}`` (snapshots), ``{"last": {...}}``
    (last trade/quote), ``{"ticker": {...}}`` (single-ticker snapshot), and
    top-level JSON arrays.

    Args:
        payload: Parsed JSON response object returned by ``session.request_json``.
        inject_symbol: Optional canonical symbol to inject as ``"Symbol"`` when
            individual row items omit a ticker field.
        extra_fields: Optional default key-value pairs to attach to every
            extracted row.

    Returns:
        list[dict[str, Any]]: Extracted and enriched row dictionaries.
    """
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
    """Shared base class for all Massive domain sub-clients.

    Attributes:
        session: Owning :class:`MassiveSession` instance.
        domain: Canonical domain identifier (e.g., ``"stocks"``, ``"options"``,
            ``"futures"``, ``"indices"``, ``"forex"``, ``"crypto"``), or ``None``
            for non-instrument domains such as ``economy``.
        symbol_normalizer: Domain-configured :class:`SymbolNormalizer` instance,
            or ``None`` when ``domain`` is ``None``.
    """

    def __init__(
        self,
        session: MassiveSession,
        domain: str | None = None,
    ) -> None:
        """Initialize the domain client.

        Args:
            session: Owning :class:`MassiveSession` instance.
            domain: Optional domain identifier used to configure symbol
                normalization and S3 routing.
        """
        self.session = session
        self.domain = domain
        self.symbol_normalizer = (
            SymbolNormalizer(domain) if domain is not None else None
        )

    def canonical_symbols(
        self, symbols: str | Iterable[str] | None
    ) -> list[str]:
        """Normalize ``symbols`` into a deduplicated list of canonical symbols.

        Args:
            symbols: A single symbol string, comma-separated symbol list,
                iterable of symbols, or ``None`` (which defaults to ``["*"]``).

        Returns:
            list[str]: Ordered list of canonical symbol strings.
        """
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
        """Fetch and paginate REST JSON results into a list of row dictionaries.

        Automatically follows ``next_url`` cursors until exhausted or until
        ``limit`` rows have been collected.

        Args:
            path: Initial REST path or URL.
            params: Optional query parameter mapping for the first page.
            limit: Optional maximum number of rows to return across all pages.
            inject_symbol: Optional canonical symbol to inject into rows that
                omit a symbol field.
            extra_fields: Optional extra key-value pairs to attach to each row.

        Returns:
            list[dict[str, Any]]: Collected row dictionaries.
        """
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
        """Paginate a REST endpoint and convert pages in bounded RecordBatch chunks into a Deephaven Table.

        Args:
            path: Initial REST endpoint path.
            schema_spec: Target :class:`SchemaSpec` governing column types and
                normalization.
            params: Optional query parameters for the initial request.
            limit: Optional maximum number of rows to fetch.
            inject_symbol: Optional canonical symbol to inject into each row.
            extra_fields: Optional extra key-value pairs to attach to each row.
            chunk_size: Number of rows per intermediate PyArrow ``RecordBatch``.

        Returns:
            Table: Static Deephaven table containing all paginated rows.
        """
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
        """Determine whether ``.historical()`` should route to S3 Flat Files or REST pagination.

        When ``source="auto"``, S3 Flat Files are selected if S3 credentials are
        configured, ``s3_dataset`` is available, ``adjusted`` is not ``True``
        (since S3 archives contain unadjusted raw exchange prints/bars), and
        either ``symbols`` is a wildcard (``"*"``) or contains at least
        ``config.flat_file_ticker_threshold`` symbols.

        Args:
            source: Routing policy (``"auto"``, ``"rest"``, or ``"flat_files"``).
            symbols: Sequence of canonical symbols, or ``None`` for a reactive
                table watchlist.
            s3_dataset: S3 dataset identifier (e.g., ``"us_stocks_sip/trades_v1"``),
                or ``None`` if the feed has no S3 archive.
            adjusted: Whether split-adjusted prices were requested.

        Returns:
            bool: ``True`` to route to S3 Flat Files; ``False`` to use REST.

        Raises:
            ValueError: If ``source`` is invalid, or if ``source="flat_files"``
                is requested without S3 support or S3 credentials.
        """
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
        chunk_size: int = 50_000,
    ) -> Table:
        """Route a single- or multi-symbol ``.historical()`` query to S3 Flat Files or chunked REST pagination.

        When routed to REST for multiple symbols without a row ``limit``, fetches
        each symbol concurrently using up to 8 worker threads while streaming
        into bounded PyArrow ``RecordBatch`` chunks.

        Args:
            symbols: Target symbol(s) or wildcard ``"*"``.
            schema_spec: Target :class:`SchemaSpec` for Arrow conversion.
            s3_dataset: S3 dataset path if S3 Flat Files are supported.
            rest_path_fn: Callback ``(symbol, start, end) -> (path, params)``
                producing the per-symbol REST endpoint and query parameters.
            start: Start date or timestamp filter.
            end: End date or timestamp filter.
            limit: Optional maximum number of rows to return.
            source: Routing policy (``"auto"``, ``"rest"``, or ``"flat_files"``).
            adjusted: Optional split-adjustment flag used by ``should_use_flat_files``.
            chunk_size: Maximum rows per PyArrow ``RecordBatch``.

        Returns:
            Table: Static Deephaven table containing the historical dataset.

        Raises:
            ValueError: If REST routing is selected without concrete symbols.
        """
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
            raise ValueError(_EMPTY_HISTORICAL_ERRORS[self.domain or "stocks"])

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
        """Create a [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder] wired to the session's execution context and multiplexer.

        Args:
            name: Descriptive feed name used for logging and stream deduplication.
            schema_spec: Target [`SchemaSpec`][deephaven_massive.engine.arrow_bridge.SchemaSpec]
                for this feed.
            ws_url: Optional WebSocket endpoint URL for `.blink()`, `.history()`,
                `.ring()`, and `.latest()`.
            ws_channel: Optional WebSocket event prefix (for example, `"T"`, `"Q"`,
                `"AM"`).
            symbols: Target symbols, iterable of symbols, or reactive Deephaven
                watchlist `Table`.
            symbol_col: Column name when `symbols` is a Deephaven `Table`.
            seed_fn: Optional snapshot seed callback for `.latest(seed=True)`,
                `.history(seed=True)`, `.ring(seed=True)`, and symbol-scoped `.snapshot()`.
            seed_rows_fn: Optional historical backfill callback for
                `.history(seed_since=...)`.
            snapshot_fn: Optional zero-argument snapshot callback for
                non-symbol-scoped `.snapshot()` and `.poll()`.
            historical_fn: Optional historical query callback for
                `.historical()`.

        Returns:
            A configured [`FeedBuilder`][deephaven_massive.engine.builder.FeedBuilder].
        """
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
        """Fetch historical REST backfill rows since ``seed_since`` for each canonical symbol in ``syms``.

        Args:
            syms: Sequence of canonical symbols to backfill.
            seed_since: Start timestamp or date for the backfill window.
            rest_path_fn: Callback ``(symbol, start, end) -> (path, params)``.
            extra_fields: Optional extra key-value pairs to attach to each row.

        Returns:
            list[dict[str, Any]]: Combined backfill rows across all concrete
            symbols.
        """
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
        """Build a :class:`FeedBuilder` backed by a paginated REST collection endpoint.

        Wires both ``snapshot_fn`` (for ``.snapshot()`` and ``.poll()``) and
        ``historical_fn`` (for ``.historical(start=..., end=..., limit=...)``
        using ``{date_param}.gte`` and ``{date_param}.lte``).

        Args:
            name: Descriptive feed name.
            path: REST endpoint path.
            schema_spec: Target :class:`SchemaSpec`.
            params: Baseline query parameters.
            symbol: Optional single symbol filter.
            tickers: Optional multi-symbol filter.
            ticker_param: Query parameter name for ticker filtering.
            filters: Additional comparison filter keyword arguments.
            date_param: Date field name used by ``.historical(start=..., end=...)``.
            inject_symbol: Optional canonical symbol to inject into returned rows.

        Returns:
            FeedBuilder: Configured feed builder.
        """
        base_params = normalize_filter_params(
            params,
            filters,
            symbol=symbol,
            tickers=tickers,
            ticker_param=ticker_param,
            normalizer=self.symbol_normalizer,
        )

        def _hist(
            start: Any = None,
            end: Any = None,
            limit: int | None = None,
            source: str = "auto",
        ) -> Table:
            q: dict[str, Any] = {
                **base_params,
                f"{date_param}.gte": format_date_param(start),
                f"{date_param}.lte": format_date_param(end),
            }
            return self.fetch_chunked_table(
                path,
                schema_spec,
                q,
                limit=limit,
                inject_symbol=inject_symbol,
            )

        return self.build_feed(
            name=name,
            schema_spec=schema_spec,
            snapshot_fn=lambda: self.fetch_rest_rows(
                path, base_params, inject_symbol=inject_symbol
            ),
            historical_fn=_hist,
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
        """Build a :class:`FeedBuilder` for a technical indicator (``SMA``, ``EMA``, ``MACD``, ``RSI``).

        Args:
            indicator: Indicator code (``"SMA"``, ``"EMA"``, ``"MACD"``, or ``"RSI"``).
            symbol: Target symbol, contract, or pair.
            params: Query parameters for ``/v1/indicators/{indicator}/{ticker}``.
            window: Primary indicator lookback window.
            timespan: Bar timespan (e.g., ``"day"``, ``"minute"``).
            series_type: Price series input (e.g., ``"close"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``INDICATOR_SCHEMA``.
        """
        canon = self.symbol_normalizer.canonical(symbol)  # type: ignore[union-attr]
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
            name=f"{self.domain}_{indicator.lower()}_{canon}",
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
