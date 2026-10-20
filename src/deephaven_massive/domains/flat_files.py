"""S3 Flat Files (.csv.gz) streaming loader using PyArrow CSV and predicate pushdown."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import gzip
from typing import TYPE_CHECKING, Any, BinaryIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    record_batches_to_arrow_table,
    rows_to_record_batch,
)
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    INDEX_VALUES_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
    compute_bar_span_ns,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession

DATASET_PREFIX_BY_DOMAIN: dict[str, str] = {
    "stocks": "us_stocks_sip",
    "options": "us_options_opra",
    "futures": "us_futures",
    "indices": "us_indices",
    "forex": "global_forex",
    "crypto": "global_crypto",
}


def _parse_date(val: str | date | datetime) -> date:
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val).strip()[:10])


def _date_range(
    start: str | date | datetime | None,
    end: str | date | datetime | None,
    single_date: str | date | datetime | None = None,
) -> list[date]:
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
    inferred_domain = domain
    if inferred_domain is None:
        for dom, prefix in DATASET_PREFIX_BY_DOMAIN.items():
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
    """Minimal stream wrapper that prepends peeked magic bytes before delegating to `stream`."""

    closed = False

    def __init__(self, prefix: bytes, stream: BinaryIO) -> None:
        self._prefix = prefix
        self._stream = stream

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._prefix:
            out = self._prefix + self._stream.read(max(0, size - len(self._prefix)))
            self._prefix = b""
            return out
        return self._stream.read(size)


class FlatFilesClient:
    """Streams Massive S3 Flat Files (`.csv.gz`) into Deephaven tables with predicate pushdown."""

    def __init__(
        self,
        session: MassiveSession,
        *,
        s3_opener: Callable[[str, str], BinaryIO] | None = None,
    ) -> None:
        self.session = session
        self._s3_opener = s3_opener

    def _open_s3_object(self, bucket: str, key: str) -> BinaryIO:
        if self._s3_opener is not None:
            return self._s3_opener(bucket, key)
        if not self.session.config.has_s3_credentials:
            raise ValueError(
                "Massive S3 credentials are required to load Flat Files. "
                "Set MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY."
            )
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=self.session.config.s3_endpoint_url,
            aws_access_key_id=self.session.config.s3_access_key_id,
            aws_secret_access_key=self.session.config.s3_secret_access_key,
        )
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"]  # type: ignore[no-any-return]

    @staticmethod
    def build_s3_key(dataset: str, target_date: date) -> str:
        """Build the canonical Massive S3 flat-file object key for a dataset and date."""
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
    ) -> list[pa.RecordBatch]:
        """Download and parse a single day's S3 `.csv.gz` flat file into RecordBatches."""
        key = self.build_s3_key(dataset, target_date)
        raw_stream = self._open_s3_object(self.session.config.s3_bucket, key)
        head = raw_stream.read(2)
        prep = _PrependReader(head, raw_stream)
        csv_stream: Any = (
            gzip.GzipFile(fileobj=prep, mode="rb")  # type: ignore[arg-type]
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
                    (
                        c
                        for c in ("ticker", "sym", "Symbol", "pair")
                        if c in batch.schema.names
                    ),
                    batch.schema.names[0],
                )
                mask = pc.is_in(batch[sym_col], value_set=value_set)
                batch = batch.filter(mask)

            if (
                remaining_limit is not None
                and day_rows + batch.num_rows > remaining_limit
            ):
                batch = batch.slice(0, remaining_limit - day_rows)

            if batch.num_rows > 0:
                raw_rows = batch.to_pylist()
                if bar_span_ns is not None:
                    for r in raw_rows:
                        r.setdefault("_span_ns", bar_span_ns)
                rb = rows_to_record_batch(
                    raw_rows, resolved_schema, normalizer
                )
                day_batches.append(rb)
                day_rows += len(raw_rows)

            if remaining_limit is not None and day_rows >= remaining_limit:
                break

        return day_batches

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
        """Stream one or more daily `.csv.gz` S3 Flat Files into a single Deephaven Table."""
        if (
            "/" not in dataset
            and isinstance(start, str)
            and not start.strip()[:4].isdigit()
        ):
            dataset = f"{dataset.strip('/')}/{start.strip('/')}"
            start = end
            end = end_pos
        elif table_type is not None and "/" not in dataset:
            dataset = f"{dataset.strip('/')}/{table_type.strip('/')}"

        resolved_schema, resolved_domain = _infer_schema_and_domain(
            dataset, schema_spec, domain
        )
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
                compute_bar_span_ns(1, "day")
                if ("day_aggs" in dataset or "session_aggs" in dataset)
                else compute_bar_span_ns(1, "minute")
            )

        dates = _date_range(effective_start, effective_end, single_date=date)
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
                        ),
                        dates,
                    )
                )
            for day_b in day_results:
                batches.extend(day_b)
        else:
            total_rows = 0
            for d in dates:
                rem = (
                    (effective_limit - total_rows)
                    if effective_limit is not None
                    else None
                )
                day_b = self._read_day_batches(
                    dataset,
                    d,
                    resolved_schema,
                    normalizer,
                    value_set,
                    bar_span_ns,
                    rem,
                )
                for rb in day_b:
                    batches.append(rb)
                    total_rows += rb.num_rows
                if effective_limit is not None and total_rows >= effective_limit:
                    break

        pa_tbl = record_batches_to_arrow_table(batches, resolved_schema)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def dataset_for(self, domain: str, feed_kind: str) -> str:
        """Resolve the S3 flat-file dataset path for a domain and feed kind."""
        prefix = DATASET_PREFIX_BY_DOMAIN.get(domain.strip().lower())
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
        return self.load(self.dataset_for(domain, kind), start, end, domain=domain, **kwargs)

    def stocks_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Stocks SIP trades (`us_stocks_sip/trades_v1`) from S3 Flat Files."""
        return self.load("us_stocks_sip/trades_v1", start, end, domain="stocks", **kwargs)

    def stocks_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Stocks SIP quotes (`us_stocks_sip/quotes_v1`) from S3 Flat Files."""
        return self.load("us_stocks_sip/quotes_v1", start, end, domain="stocks", **kwargs)

    def stocks_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Stocks SIP bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("stocks", start, end, timespan=timespan, **kwargs)

    def options_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Options OPRA trades (`us_options_opra/trades_v1`) from S3 Flat Files."""
        return self.load("us_options_opra/trades_v1", start, end, domain="options", **kwargs)

    def options_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Options OPRA quotes (`us_options_opra/quotes_v1`) from S3 Flat Files."""
        return self.load("us_options_opra/quotes_v1", start, end, domain="options", **kwargs)

    def options_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Options OPRA bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("options", start, end, timespan=timespan, **kwargs)

    def futures_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures trades (`us_futures/trades_v1`) from S3 Flat Files."""
        return self.load("us_futures/trades_v1", start, end, domain="futures", **kwargs)

    def futures_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures quotes (`us_futures/quotes_v1`) from S3 Flat Files."""
        return self.load("us_futures/quotes_v1", start, end, domain="futures", **kwargs)

    def futures_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Futures bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("futures", start, end, timespan=timespan, **kwargs)

    def futures_session_bars(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Futures session bars (`us_futures/session_aggs_v1`) from S3 Flat Files."""
        return self.load("us_futures/session_aggs_v1", start, end, domain="futures", **kwargs)

    def indices_values(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load US Indices values (`us_indices/values_v1`) from S3 Flat Files."""
        return self.load("us_indices/values_v1", start, end, domain="indices", **kwargs)

    def indices_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load US Indices bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("indices", start, end, timespan=timespan, **kwargs)

    def forex_quotes(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load Global Forex quotes (`global_forex/quotes_v1`) from S3 Flat Files."""
        return self.load("global_forex/quotes_v1", start, end, domain="forex", **kwargs)

    def forex_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load Global Forex bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("forex", start, end, timespan=timespan, **kwargs)

    def crypto_trades(self, start: Any = None, end: Any = None, **kwargs: Any) -> Table:
        """Load Global Crypto trades (`global_crypto/trades_v1`) from S3 Flat Files."""
        return self.load("global_crypto/trades_v1", start, end, domain="crypto", **kwargs)

    def crypto_bars(
        self, start: Any = None, end: Any = None, *, timespan: str = "minute", **kwargs: Any
    ) -> Table:
        """Load Global Crypto bars (`minute_aggs_v1` or `day_aggs_v1`) from S3 Flat Files."""
        return self._load_bars("crypto", start, end, timespan=timespan, **kwargs)
