"""S3 Flat Files (`.csv.gz`) streaming loader (`m.flat_files`).

This module implements :class:`FlatFilesClient`, which streams daily gzipped
CSV archives from Massive's S3-compatible object store
(`https://files.massive.com`) directly into PyArrow `RecordBatch` streams using
16 MB C++ chunks (`pyarrow.csv.open_csv`), applies vectorized predicate
pushdown (`pyarrow.compute.is_in`) before Python materialization, and converts
the filtered batches into a single Deephaven `Table`.
"""

from __future__ import annotations

import gzip
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, BinaryIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    record_batches_to_arrow_table,
    rows_to_record_batch,
    to_utc_ns,
)
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    INDEX_VALUES_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
    _compute_bar_span_ns,
)

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive.session import MassiveSession

_DATASET_PREFIX_BY_DOMAIN: dict[str, str] = {
    "stocks": "us_stocks_sip",
    "options": "us_options_opra",
    "futures": "us_futures",
    "indices": "us_indices",
    "forex": "global_forex",
    "crypto": "global_crypto",
}


def _subday_bound_ns(val: Any) -> int | None:
    """Return UTC nanoseconds if `val` specifies a sub-day timestamp rather than a calendar date."""
    if isinstance(val, (datetime, int, float)) or (isinstance(val, str) and len(val.strip()) > 10):
        ns = to_utc_ns(val)
        if ns is None:
            raise ValueError(f"Unparseable sub-day timestamp bound: {val!r}")
        return ns
    return None


def _parse_date(val: Any) -> date:
    """Coerce an ISO string, `date`, `datetime`, or epoch number into a UTC `datetime.date`.

    Args:
        val: Date or timestamp value (`"YYYY-MM-DD"`, `date`, `datetime`, or epoch number).

    Returns:
        date: Parsed UTC calendar date.
    """
    ns = _subday_bound_ns(val)
    if ns is not None:
        return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).date()
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val).strip()[:10])


def _date_range(
    start: str | date | datetime | None,
    end: str | date | datetime | None,
    single_date: str | date | datetime | None = None,
) -> list[date]:
    """Expand a single date or inclusive `[start, end]` range into a list of `date` objects.

    Args:
        start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
        end: Inclusive end date; defaults to `start` when `None`.
        single_date: Optional single date override.

    Returns:
        list[date]: Chronological list of calendar dates.

    Raises:
        ValueError: If neither `single_date` nor `start` is provided, or if
            `end < start`.
    """
    if single_date is not None:
        d = _parse_date(single_date)
        return [d]
    if start is None:
        raise ValueError("Either `date` or `start` must be provided for S3 Flat Files.")
    d_start = _parse_date(start)
    d_end = _parse_date(end) if end is not None else d_start
    if d_end < d_start:
        raise ValueError(f"end date ({d_end}) must be >= start date ({d_start}).")
    days: list[date] = []
    cur = d_start
    while cur <= d_end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _infer_schema_and_domain(
    dataset: str,
    schema_spec: SchemaSpec | None,
    domain: str | None,
) -> tuple[SchemaSpec, str]:
    """Infer the target `SchemaSpec` and domain name from an S3 dataset path.

    Args:
        dataset: S3 dataset prefix (e.g., `"us_stocks_sip/trades_v1"`).
        schema_spec: Optional explicit `SchemaSpec` override.
        domain: Optional explicit domain override (`"stocks"`, `"options"`, etc.).

    Returns:
        tuple[SchemaSpec, str]: Resolved `(SchemaSpec, domain)` pair.
    """
    inferred_domain = domain
    if inferred_domain is None:
        for dom, prefix in _DATASET_PREFIX_BY_DOMAIN.items():
            if dataset.startswith(prefix):
                inferred_domain = dom
                break
    if inferred_domain is None:
        inferred_domain = "stocks"

    if schema_spec is not None:
        return schema_spec, inferred_domain

    if "trades" in dataset:
        return TRADES_SCHEMA, inferred_domain
    if "quotes" in dataset:
        return QUOTES_SCHEMA, inferred_domain
    if "values" in dataset:
        return INDEX_VALUES_SCHEMA, inferred_domain
    return BARS_SCHEMA, inferred_domain


class _PrependReader:
    """Binary stream wrapper that replays peeked magic bytes before delegating to `stream`.

    Used to inspect the first two bytes of an S3 response body (`0x1f 0x8b`)
    for gzip magic detection without consuming or seeking the underlying network
    stream.
    """

    closed = False

    def __init__(self, prefix: bytes, stream: BinaryIO) -> None:
        """Initialize the prepend reader.

        Args:
            prefix: Initial peeked bytes to replay first.
            stream: Underlying binary stream for subsequent reads.
        """
        self._prefix = prefix
        self._stream = stream

    def readable(self) -> bool:
        """Return `True` to satisfy `gzip.GzipFile` and `pyarrow.csv` stream checks."""
        return True

    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes, draining `prefix` before reading from `stream`."""
        if self._prefix:
            out = self._prefix + self._stream.read(max(0, size - len(self._prefix)))
            self._prefix = b""
            return out
        return self._stream.read(size)


class FlatFilesClient:
    """High-throughput S3 Flat Files (`.csv.gz`) loader (`m.flat_files`).

    ``FlatFilesClient`` downloads daily gzipped CSV archives from Massive's S3
    endpoint (`https://files.massive.com`), parses them in 16 MB blocks using
    PyArrow's multi-threaded C++ CSV reader, filters rows in C++ via
    `pyarrow.compute.is_in` when `symbols` are specified, and normalizes the
    result into the identical PascalCase Deephaven schema used by live
    WebSocket and REST feeds.

    Multi-day ranges (`start` through `end` without a row `limit`) are fetched
    and parsed concurrently across a 4-worker `ThreadPoolExecutor`.

    Example:
        Load three days of US SIP trades for `AAPL` and `NVDA` directly from S3::

            trades_hist = m.flat_files.stocks_trades(
                start="2025-01-13",
                end="2025-01-15",
                symbols=["AAPL", "NVDA"],
            )
    """

    def __init__(
        self,
        session: MassiveSession,
        *,
        s3_opener: Callable[[str, str], BinaryIO] | None = None,
    ) -> None:
        """Initialize the S3 Flat Files client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
            s3_opener: Optional callable `(bucket, key) -> BinaryIO` for custom
                transport or unit testing. When `None`, uses `boto3` configured
                with `session.config` S3 credentials.
        """
        self.session = session
        self._s3_opener = s3_opener
        self._s3_client: Any = None
        self._client_lock = threading.Lock()

    def _open_s3_object(self, bucket: str, key: str) -> BinaryIO:
        if self._s3_opener is not None:
            return self._s3_opener(bucket, key)
        if not self.session.config.has_s3_credentials:
            raise ValueError(
                "Massive S3 credentials are required to load Flat Files. "
                "Set MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY."
            )
        with self._client_lock:
            if self._s3_client is None:
                import boto3

                self._s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.session.config.s3_endpoint_url,
                    aws_access_key_id=self.session.config.s3_access_key_id,
                    aws_secret_access_key=self.session.config.s3_secret_access_key,
                )
            s3 = self._s3_client
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"]

    @staticmethod
    def _build_s3_key(dataset: str, target_date: date) -> str:
        """Build the canonical Massive S3 flat-file object key for a dataset and date.

        Args:
            dataset: Dataset prefix (e.g., `"us_stocks_sip/trades_v1"`).
            target_date: Calendar date of the archive.

        Returns:
            str: Canonical S3 object key of the form
            `"<dataset>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz"`.
        """
        clean_ds = dataset.strip("/")
        return f"{clean_ds}/{target_date.year:04d}/{target_date.month:02d}/{target_date.isoformat()}.csv.gz"

    def _read_day_batches(
        self,
        dataset: str,
        target_date: date,
        resolved_schema: SchemaSpec,
        normalizer: SymbolNormalizer,
        value_set: pa.Array | None,
        bar_span_ns: int | None,
        remaining_limit: int | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> list[pa.RecordBatch]:
        """Download and parse a single day's S3 `.csv.gz` flat file into normalized `RecordBatch` objects."""
        key = self._build_s3_key(dataset, target_date)
        raw_stream = self._open_s3_object(self.session.config.s3_bucket, key)
        try:
            head = raw_stream.read(2)
            prep = _PrependReader(head, raw_stream)
            csv_stream: Any = (
                gzip.GzipFile(fileobj=prep, mode="rb")  # type: ignore[call-overload]
                if head == b"\x1f\x8b"
                else prep
            )

            reader = pa_csv.open_csv(
                csv_stream,
                read_options=pa_csv.ReadOptions(block_size=16 * 1024 * 1024),
                convert_options=pa_csv.ConvertOptions(strings_can_be_null=True),
            )

            day_batches: list[pa.RecordBatch] = []
            day_rows = 0
            for batch in reader:
                if value_set is not None:
                    sym_col = next(
                        (c for c in ("ticker", "sym", "Symbol", "pair") if c in batch.schema.names),
                        batch.schema.names[0],
                    )
                    mask = pc.is_in(batch[sym_col], value_set=value_set)
                    batch = batch.filter(mask)

                if batch.num_rows > 0:
                    raw_rows = batch.to_pylist()
                    if bar_span_ns is not None:
                        for r in raw_rows:
                            r.setdefault("_span_ns", bar_span_ns)
                    rb = rows_to_record_batch(raw_rows, resolved_schema, normalizer)
                    if start_ns is not None and "Timestamp" in rb.schema.names:
                        rb = rb.filter(pc.greater_equal(rb["Timestamp"].cast(pa.int64()), start_ns))
                    if end_ns is not None and "Timestamp" in rb.schema.names:
                        rb = rb.filter(pc.less_equal(rb["Timestamp"].cast(pa.int64()), end_ns))
                    if remaining_limit is not None and day_rows + rb.num_rows > remaining_limit:
                        rb = rb.slice(0, remaining_limit - day_rows)
                    if rb.num_rows > 0:
                        day_batches.append(rb)
                        day_rows += rb.num_rows

                if remaining_limit is not None and day_rows >= remaining_limit:
                    break

            return day_batches
        finally:
            raw_stream.close()

    def load(
        self,
        dataset: str,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        end_pos: str | date | datetime | None = None,
        *,
        table_type: str | None = None,
        date: str | date | datetime | None = None,
        start_date: str | date | datetime | None = None,
        end_date: str | date | datetime | None = None,
        symbols: str | Iterable[str] | None = None,
        tickers: str | Iterable[str] | None = None,
        schema_spec: SchemaSpec | None = None,
        domain: str | None = None,
        limit: int | None = None,
        max_rows: int | None = None,
    ) -> Table:
        """Stream one or more daily `.csv.gz` S3 Flat Files into a single Deephaven `Table`.

        Supports both combined dataset paths (`load("us_stocks_sip/trades_v1", "2025-01-15")`)
        and two-part positional calls (`load("us_stocks_sip", "trades_v1", "2025-01-15")`).

        Args:
            dataset: S3 dataset path (e.g., `"us_stocks_sip/trades_v1"`) or
                domain prefix (`"us_stocks_sip"`) when `table_type` or a
                non-date second positional argument is supplied.
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`),
                or `table_type` when called with three positional arguments.
            end: Inclusive end date; defaults to `start` when `None`.
            end_pos: Inclusive end date when `start` is used as a positional
                `table_type`.
            table_type: Optional table type suffix (e.g., `"trades_v1"`,
                `"quotes_v1"`, `"minute_aggs_v1"`, `"day_aggs_v1"`).
            date: Single calendar date alias for `start`.
            start_date: Keyword alias for `start`.
            end_date: Keyword alias for `end`.
            symbols: Optional symbol or iterable of symbols for C++ predicate
                pushdown filtering (`pyarrow.compute.is_in`).
            tickers: Keyword alias for `symbols`.
            schema_spec: Optional explicit `SchemaSpec` override; inferred from
                `dataset` when `None`.
            domain: Optional domain name override (`"stocks"`, `"options"`,
                `"futures"`, `"indices"`, `"forex"`, `"crypto"`).
            limit: Optional maximum number of rows to materialize across the
                date range.
            max_rows: Keyword alias for `limit`.

        Returns:
            Table: Static Deephaven `Table` populated with normalized rows from
            the requested S3 archive(s).

        Raises:
            ValueError: If S3 credentials are not configured or date bounds are
                missing/invalid.
        """
        if "/" not in dataset and isinstance(start, str) and not start.strip()[:4].isdigit():
            dataset = f"{dataset.strip('/')}/{start.strip('/')}"
            start = end
            end = end_pos
        elif table_type is not None and "/" not in dataset:
            dataset = f"{dataset.strip('/')}/{table_type.strip('/')}"

        resolved_schema, resolved_domain = _infer_schema_and_domain(dataset, schema_spec, domain)
        normalizer = SymbolNormalizer(resolved_domain)

        effective_start = start if start is not None else start_date
        effective_end = end if end is not None else end_date
        effective_limit = limit if limit is not None else max_rows
        effective_symbols = symbols if symbols is not None else tickers

        value_set: pa.Array | None = None
        if effective_symbols is not None:
            canon_list = normalizer.canonical_list(effective_symbols)
            if "*" not in canon_list:
                allowed_raw: set[str] = set()
                for s in canon_list:
                    allowed_raw.add(s)
                    allowed_raw.add(normalizer.rest_ticker(s))
                    allowed_raw.add(s.removeprefix("O:").removeprefix("I:"))
                    allowed_raw.add(s.replace("/", "").replace("-", ""))
                value_set = pa.array(sorted(allowed_raw), type=pa.string())

        bar_span_ns: int | None = None
        if resolved_schema.name == "Bars":
            bar_span_ns = (
                _compute_bar_span_ns(1, "day")
                if ("day_aggs" in dataset or "session_aggs" in dataset)
                else _compute_bar_span_ns(1, "minute")
            )

        dates = _date_range(effective_start, effective_end, single_date=date)
        start_ns = _subday_bound_ns(effective_start if date is None else date)
        end_ns = _subday_bound_ns(effective_end)
        batches: list[pa.RecordBatch] = []

        if len(dates) > 1 and effective_limit is None:
            with ThreadPoolExecutor(max_workers=min(len(dates), 4)) as pool:
                day_results = list(
                    pool.map(
                        lambda d: self._read_day_batches(
                            dataset,
                            d,
                            resolved_schema,
                            normalizer,
                            value_set,
                            bar_span_ns,
                            None,
                            start_ns,
                            end_ns,
                        ),
                        dates,
                    )
                )
            for day_b in day_results:
                batches.extend(day_b)
        else:
            total_rows = 0
            for d in dates:
                rem = (effective_limit - total_rows) if effective_limit is not None else None
                day_b = self._read_day_batches(
                    dataset,
                    d,
                    resolved_schema,
                    normalizer,
                    value_set,
                    bar_span_ns,
                    rem,
                    start_ns,
                    end_ns,
                )
                for rb in day_b:
                    batches.append(rb)
                    total_rows += rb.num_rows
                if effective_limit is not None and total_rows >= effective_limit:
                    break

        pa_tbl = record_batches_to_arrow_table(batches, resolved_schema)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def _dataset_for(self, domain: str, feed_kind: str) -> str:
        """Resolve the canonical S3 flat-file dataset path for a domain and feed kind.

        Args:
            domain: Asset domain (`"stocks"`, `"options"`, `"futures"`,
                `"indices"`, `"forex"`, or `"crypto"`).
            feed_kind: Feed table kind (`"trades_v1"`, `"quotes_v1"`,
                `"minute_aggs_v1"`, `"day_aggs_v1"`, `"session_aggs_v1"`, or
                `"values_v1"`).

        Returns:
            str: Full S3 dataset prefix (e.g., `"us_stocks_sip/trades_v1"`).

        Raises:
            ValueError: If `domain` is not a supported S3 flat-file domain.
        """
        prefix = _DATASET_PREFIX_BY_DOMAIN.get(domain.strip().lower())
        if prefix is None:
            raise ValueError(f"Unsupported S3 flat-file domain '{domain}'.")
        return f"{prefix}/{feed_kind}"

    def _load_bars(
        self,
        domain: str,
        start: Any = None,
        end: Any = None,
        *,
        timespan: str = "minute",
        **kwargs: Any,
    ) -> Table:
        kind = "day_aggs_v1" if timespan.strip().lower() == "day" else "minute_aggs_v1"
        return self.load(self._dataset_for(domain, kind), start, end, domain=domain, **kwargs)

    def stocks_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Stocks SIP trades (`us_stocks_sip/trades_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `TRADES_SCHEMA`.
        """
        return self.load("us_stocks_sip/trades_v1", start, end, domain="stocks", **kwargs)

    def stocks_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Stocks SIP NBBO quotes (`us_stocks_sip/quotes_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `QUOTES_SCHEMA`.
        """
        return self.load("us_stocks_sip/quotes_v1", start, end, domain="stocks", **kwargs)

    def stocks_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Stocks SIP bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("stocks", start, end, timespan=timespan, **kwargs)

    def options_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Options OPRA trades (`us_options_opra/trades_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `TRADES_SCHEMA`.
        """
        return self.load("us_options_opra/trades_v1", start, end, domain="options", **kwargs)

    def options_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Options OPRA quotes (`us_options_opra/quotes_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `QUOTES_SCHEMA`.
        """
        return self.load("us_options_opra/quotes_v1", start, end, domain="options", **kwargs)

    def options_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Options OPRA bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("options", start, end, timespan=timespan, **kwargs)

    def futures_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures trades (`us_futures/trades_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `TRADES_SCHEMA`.
        """
        return self.load("us_futures/trades_v1", start, end, domain="futures", **kwargs)

    def futures_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures quotes (`us_futures/quotes_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `QUOTES_SCHEMA`.
        """
        return self.load("us_futures/quotes_v1", start, end, domain="futures", **kwargs)

    def futures_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Futures bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("futures", start, end, timespan=timespan, **kwargs)

    def futures_session_bars(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures full-session bars (`us_futures/session_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self.load("us_futures/session_aggs_v1", start, end, domain="futures", **kwargs)

    def indices_values(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Indices tick values (`us_indices/values_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `INDEX_VALUES_SCHEMA`.
        """
        return self.load("us_indices/values_v1", start, end, domain="indices", **kwargs)

    def indices_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Indices bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("indices", start, end, timespan=timespan, **kwargs)

    def forex_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load Global Forex BBO quotes (`global_forex/quotes_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `QUOTES_SCHEMA`.
        """
        return self.load("global_forex/quotes_v1", start, end, domain="forex", **kwargs)

    def forex_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load Global Forex bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("forex", start, end, timespan=timespan, **kwargs)

    def crypto_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load Global Crypto trades (`global_crypto/trades_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `TRADES_SCHEMA`.
        """
        return self.load("global_crypto/trades_v1", start, end, domain="crypto", **kwargs)

    def crypto_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load Global Crypto bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files.

        Args:
            start: Inclusive start date (`"YYYY-MM-DD"`, `date`, or `datetime`).
            end: Optional inclusive end date; defaults to `start`.
            timespan: `"minute"` (default) or `"day"`.
            **kwargs: Forwarded to :meth:`load` (e.g., `symbols`, `limit`).

        Returns:
            Table: Deephaven `Table` conforming to `BARS_SCHEMA`.
        """
        return self._load_bars("crypto", start, end, timespan=timespan, **kwargs)
