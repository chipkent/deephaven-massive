"""S3 Flat Files (.csv.gz) streaming loader using PyArrow CSV and predicate pushdown."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta
import gzip
import io
from typing import TYPE_CHECKING, Any, BinaryIO

import pyarrow as pa
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

    def load(
        self,
        dataset: str,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        *,
        date: str | date | datetime | None = None,
        symbols: str | Iterable[str] | None = None,
        schema_spec: SchemaSpec | None = None,
        domain: str | None = None,
        limit: int | None = None,
    ) -> Table:
        """Stream one or more daily `.csv.gz` S3 Flat Files into a single Deephaven Table."""
        resolved_schema, resolved_domain = _infer_schema_and_domain(
            dataset, schema_spec, domain
        )
        normalizer = SymbolNormalizer(resolved_domain)

        allowed_canon: set[str] | None = None
        allowed_raw: set[str] | None = None
        if symbols is not None:
            canon_list = normalizer.canonical_list(symbols)
            if "*" not in canon_list:
                allowed_canon = set(canon_list)
                allowed_raw = set(canon_list) | {
                    normalizer.rest_ticker(s) for s in canon_list
                }

        dates = _date_range(start, end, single_date=date)
        batches: list[pa.RecordBatch] = []
        total_rows = 0

        for d in dates:
            key = self.build_s3_key(dataset, d)
            raw_stream = self._open_s3_object(self.session.config.s3_bucket, key)
            raw_bytes = raw_stream.read()
            if raw_bytes[:2] == b"\x1f\x8b":
                csv_bytes = gzip.decompress(raw_bytes)
            else:
                csv_bytes = raw_bytes

            if not csv_bytes.strip():
                continue

            csv_table = pa_csv.read_csv(
                io.BytesIO(csv_bytes),
                convert_options=pa_csv.ConvertOptions(strings_can_be_null=True),
            )
            raw_rows = csv_table.to_pylist()

            # Predicate pushdown on ticker/symbol BEFORE normalizing Conditions/Indicators arrays
            if allowed_raw is not None:
                filtered_rows: list[dict[str, Any]] = []
                for r in raw_rows:
                    raw_sym = (
                        r.get("ticker")
                        or r.get("sym")
                        or r.get("Symbol")
                        or r.get("pair")
                    )
                    if raw_sym is None:
                        continue
                    s_str = str(raw_sym).strip()
                    if s_str in allowed_raw or normalizer.canonical(s_str) in (
                        allowed_canon or set()
                    ):
                        filtered_rows.append(r)
                raw_rows = filtered_rows

            if limit is not None and total_rows + len(raw_rows) > limit:
                raw_rows = raw_rows[: limit - total_rows]

            if raw_rows:
                rb = rows_to_record_batch(
                    raw_rows, resolved_schema, normalizer
                )
                batches.append(rb)
                total_rows += len(raw_rows)

            if limit is not None and total_rows >= limit:
                break

        pa_tbl = record_batches_to_arrow_table(batches, resolved_schema)
        return arrow_to_deephaven_table(pa_tbl, self.session.exec_ctx)

    def dataset_for(self, domain: str, feed_kind: str) -> str:
        """Resolve the S3 flat-file dataset path for a domain and feed kind."""
        prefix = DATASET_PREFIX_BY_DOMAIN.get(domain.strip().lower())
        if prefix is None:
            raise ValueError(f"Unsupported S3 flat-file domain '{domain}'.")
        return f"{prefix}/{feed_kind}"
