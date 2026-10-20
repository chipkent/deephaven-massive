"""Zero-dataclass JSON/CSV-to-PyArrow `RecordBatch`-to-Deephaven `Table` bridge.

Rather than instantiating intermediate Python objects or `massive` SDK dataclasses on
the hot path, `deephaven-massive` normalizes incoming JSON dictionaries and CSV rows
directly into columnar `pyarrow.RecordBatch` chunks and converts them to Deephaven
tables via `deephaven.arrow.to_table` inside a captured `ExecutionContext`.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive.engine.symbols import SymbolNormalizer

UTC_NS_TYPE = pa.timestamp("ns", tz="UTC")
DATE32_TYPE = pa.date32()
INT32_LIST_TYPE = pa.list_(pa.int32())
STRING_LIST_TYPE = pa.list_(pa.string())

_EPOCH_DATE = date(1970, 1, 1)
_SNAKE_BOUNDARY_RE = re.compile(r"[^a-zA-Z0-9]+")
_CAMEL_BOUNDARY_RE = re.compile(r"([a-z0-9])([A-Z])")


def to_pascal_case(name: str) -> str:
    """Convert `snake_case`, `kebab-case`, or `camelCase` field names to `PascalCase`.

    If the resulting identifier begins with a numeric digit (for example, `"52_week_high"`),
    it is prefixed with `"F"` (`"F52WeekHigh"`) so that it remains a valid Deephaven
    query language identifier.

    Args:
        name: Raw JSON or CSV field name.

    Returns:
        Normalized `PascalCase` column name (`"Field"` if `name` contains no alphanumeric
        characters).
    """
    spaced = _CAMEL_BOUNDARY_RE.sub(r"\1_\2", name.strip())
    parts = [p for p in _SNAKE_BOUNDARY_RE.split(spaced) if p]
    if not parts:
        return "Field"
    result = "".join(p[:1].upper() + p[1:].lower() for p in parts)
    if result[0].isdigit():
        return f"F{result}"
    return result


def to_utc_ns(val: Any) -> int | None:
    """Normalize Unix epoch numbers (`s`, `ms`, `us`, `ns`), `datetime` objects, or ISO-8601 strings to UTC nanoseconds.

    Massive endpoints emit timestamps in varying precisions (seconds, milliseconds,
    microseconds, nanoseconds, or ISO-8601 strings). Numeric timestamps are classified
    by magnitude (`>= 1e17` for nanoseconds, `>= 1e14` for microseconds, `>= 1e11` for
    milliseconds, and seconds otherwise) and scaled to UTC nanoseconds since
    `1970-01-01T00:00:00Z`.

    Args:
        val: Raw timestamp value (`int`, `float`, `datetime`, `date`, or `str`).

    Returns:
        Integer nanoseconds since the Unix epoch in UTC, or `None` if `val` is null,
        non-positive, or unparseable.
    """
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        if val <= 0:
            return None
        abs_v = abs(val)
        if abs_v >= 100_000_000_000_000_000:  # >= 1e17: nanoseconds
            return val
        if abs_v >= 100_000_000_000_000:  # >= 1e14: microseconds
            return val * 1_000
        if abs_v >= 100_000_000_000:  # >= 1e11: milliseconds
            return val * 1_000_000
        return val * 1_000_000_000  # seconds
    if isinstance(val, float):
        if val <= 0.0:
            return None
        abs_f = abs(val)
        if abs_f >= 1e17:
            return int(val)
        if abs_f >= 1e14:
            return int(val * 1_000)
        if abs_f >= 1e11:
            return int(val * 1_000_000)
        return int(val * 1_000_000_000)
    if isinstance(val, datetime):
        dt = val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    if isinstance(val, date):
        dt = datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if s.isdigit():
            return to_utc_ns(int(s))
        try:
            iso = s[:-1] + "+00:00" if s.endswith("Z") else s
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000_000)
        except ValueError:
            return None
    return None


def to_epoch_days(val: Any) -> int | None:
    """Normalize `'YYYY-MM-DD'` strings, `date`/`datetime` objects, or epoch numbers to days since `1970-01-01`.

    Args:
        val: Raw calendar date value (`str`, `date`, `datetime`, `int`, or `float`).

    Returns:
        Integer day offset from `1970-01-01` suitable for `pa.date32()`, or `None` if
        `val` is null or unparseable.
    """
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return (val.date() - _EPOCH_DATE).days
    if isinstance(val, date):
        return (val - _EPOCH_DATE).days
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        date_part = s[:10]
        try:
            d = date.fromisoformat(date_part)
            return (d - _EPOCH_DATE).days
        except ValueError:
            ns = to_utc_ns(s)
            if ns is not None:
                dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
                return (dt.date() - _EPOCH_DATE).days
            return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        ns = to_utc_ns(val)
        if ns is not None:
            dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
            return (dt.date() - _EPOCH_DATE).days
    return None


def to_float(val: Any) -> float | None:
    """Convert a scalar value to `float` or `None` if null, boolean, or unparseable.

    Args:
        val: Raw scalar value.

    Returns:
        Coerced `float` value, or `None`.
    """
    if val is None or val == "" or isinstance(val, bool):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def to_int(val: Any) -> int | None:
    """Convert a scalar value to `int` or `None` if null, boolean, or unparseable.

    Args:
        val: Raw scalar value.

    Returns:
        Coerced `int` value, or `None`.
    """
    if val is None or val == "" or isinstance(val, bool):
        return None
    try:
        return int(float(val)) if isinstance(val, str) and "." in val else int(val)
    except (TypeError, ValueError):
        return None


def to_bool(val: Any) -> bool | None:
    """Convert a scalar or string literal (`'true'`, `'1'`, `'yes'`) to `bool` or `None`.

    Args:
        val: Raw scalar value.

    Returns:
        Coerced `bool` value, or `None`.
    """
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        low = val.strip().lower()
        if low in ("true", "1", "yes", "t"):
            return True
        if low in ("false", "0", "no", "f"):
            return False
        return None
    return bool(val)


def to_str(val: Any) -> str | None:
    """Convert a scalar value to a whitespace-stripped `str` or `None` if empty.

    Args:
        val: Raw scalar value.

    Returns:
        Stripped non-empty `str`, or `None`.
    """
    if val is None:
        return None
    s = str(val).strip()
    return s if s != "" else None


def parse_int32_list(val: Any) -> list[int] | None:
    """Parse a sequence of integers or delimited string (`'[12, 37]'`, `'12,37'`, `'12 37'`) into `list[int]`.

    Used primarily for trade and quote condition code arrays across WebSocket JSON
    arrays, REST JSON lists, and S3 Flat File string representations.

    Args:
        val: Raw sequence, delimited string, or single integer value.

    Returns:
        A `list[int]` of parsed condition codes, or `None` if `val` is null.
    """
    if val is None or val == "":
        return None
    if isinstance(val, (list, tuple)):
        out: list[int] = []
        for item in val:
            iv = to_int(item)
            if iv is not None:
                out.append(iv)
        return out
    if isinstance(val, str):
        cleaned = val.strip().strip("[](){}")
        if not cleaned:
            return []
        tokens = [t.strip() for t in re.split(r"[,\s;|]+", cleaned) if t.strip()]
        out_str: list[int] = []
        for tok in tokens:
            iv = to_int(tok)
            if iv is not None:
                out_str.append(iv)
        return out_str
    iv_single = to_int(val)
    return [iv_single] if iv_single is not None else None


def parse_str_list(val: Any) -> list[str] | None:
    """Parse a sequence of strings or comma-delimited string into `list[str]`.

    Args:
        val: Raw sequence of strings or comma-delimited string.

    Returns:
        A `list[str]` of stripped non-empty strings, or `None` if `val` is null.
    """
    if val is None or val == "":
        return None
    if isinstance(val, (list, tuple)):
        return [str(x).strip() for x in val if x is not None and str(x).strip()]
    if isinstance(val, str):
        cleaned = val.strip()
        if not cleaned:
            return []
        return [t.strip() for t in cleaned.split(",") if t.strip()]
    return [str(val)]


def flatten_dict(data: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested JSON dictionaries (including financial statement `{value, unit}` objects) into `PascalCase` keys.

    Args:
        data: Nested JSON mapping to flatten.
        prefix: Optional `PascalCase` prefix prepended to each flattened key.

    Returns:
        A single-level dictionary mapping `PascalCase` column names to scalar or list values.
    """
    flat: dict[str, Any] = {}
    for k, v in data.items():
        pascal_k = f"{prefix}{to_pascal_case(str(k))}"
        if isinstance(v, Mapping):
            if "value" in v and len(v) <= 4 and not isinstance(v["value"], Mapping):
                flat[pascal_k] = v["value"]
            else:
                flat.update(flatten_dict(v, prefix=pascal_k))
        elif isinstance(v, list):
            if all(not isinstance(item, Mapping) for item in v):
                flat[pascal_k] = v
            else:
                flat[pascal_k] = str(v)
        else:
            flat[pascal_k] = v
    return flat


@dataclass(frozen=True)
class SchemaSpec:
    """Immutable specification binding a PyArrow schema, primary key columns, and row normalizer.

    Attributes:
        name: Logical schema name (e.g., `"stocks.trades"`).
        arrow_schema: Canonical `pyarrow.Schema` with `PascalCase` fields.
        key_cols: Default key column names used by `.latest()` (`last_by(key_cols)`)
            and `.poll(mode="replace" | "upsert")`.
        normalizer: Callable `(raw_row, symbol_normalizer) -> dict[str, Any]` that maps
            wire-level JSON or CSV keys into canonical `PascalCase` keys.
        allow_dynamic_columns: If `True`, additional `PascalCase` keys emitted by the
            normalizer (such as dynamic line items in financial statements) are
            automatically inferred and appended to the resulting `RecordBatch`.
    """

    name: str
    arrow_schema: pa.Schema
    key_cols: tuple[str, ...]
    normalizer: Callable[[Mapping[str, Any], SymbolNormalizer | None], dict[str, Any]]
    allow_dynamic_columns: bool = False

    def with_extra_fields(self, extra_fields: Sequence[pa.Field]) -> pa.Schema:
        """Return a copy of `arrow_schema` with extra internal fields appended.

        Args:
            extra_fields: Additional `pyarrow.Field` definitions (such as `__ReceivedAt`
                or `__PollId`).

        Returns:
            A new `pyarrow.Schema` containing the base fields followed by `extra_fields`.
        """
        fields = list(self.arrow_schema) + list(extra_fields)
        return pa.schema(fields)


def _coerce_column_array(values: list[Any], field_type: pa.DataType) -> pa.Array:
    """Build a typed `pyarrow.Array` for a single column according to `field_type`."""
    if field_type == UTC_NS_TYPE:
        ns_vals = [to_utc_ns(v) for v in values]
        return pa.array(ns_vals, type=UTC_NS_TYPE)
    if field_type == DATE32_TYPE:
        day_vals = [to_epoch_days(v) for v in values]
        return pa.array(day_vals, type=DATE32_TYPE)
    if field_type == INT32_LIST_TYPE:
        list_vals = [parse_int32_list(v) for v in values]
        return pa.array(list_vals, type=INT32_LIST_TYPE)
    if field_type == STRING_LIST_TYPE:
        slist_vals = [parse_str_list(v) for v in values]
        return pa.array(slist_vals, type=STRING_LIST_TYPE)
    if pa.types.is_floating(field_type):
        f_vals = [to_float(v) for v in values]
        return pa.array(f_vals, type=field_type)
    if pa.types.is_integer(field_type):
        i_vals = [to_int(v) for v in values]
        return pa.array(i_vals, type=field_type)
    if pa.types.is_boolean(field_type):
        b_vals = [to_bool(v) for v in values]
        return pa.array(b_vals, type=field_type)
    s_vals = [to_str(v) for v in values]
    return pa.array(s_vals, type=pa.string())


def _infer_dynamic_column(values: list[Any]) -> pa.Array:
    """Infer a typed `pyarrow.Array` (`bool_`, `float64`, or `string`) for a dynamic column."""
    non_null = [v for v in values if v is not None and v != ""]
    if not non_null:
        return pa.array([None] * len(values), type=pa.string())
    if all(isinstance(v, bool) for v in non_null):
        return _coerce_column_array(values, pa.bool_())
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
        return _coerce_column_array(values, pa.float64())
    return _coerce_column_array(values, pa.string())


def rows_to_record_batch(
    rows: Sequence[Mapping[str, Any]],
    schema_spec: SchemaSpec,
    symbol_normalizer: SymbolNormalizer | None = None,
    *,
    extra_columns: Mapping[str, tuple[pa.DataType, Any]] | None = None,
    already_normalized: bool = False,
) -> pa.RecordBatch:
    """Convert raw or normalized row dictionaries into a typed `pyarrow.RecordBatch`.

    Args:
        rows: Sequence of raw wire dictionaries or pre-normalized row dictionaries.
        schema_spec: Target `SchemaSpec` governing column names, types, and row normalization.
        symbol_normalizer: Optional `SymbolNormalizer` passed to `schema_spec.normalizer`.
        extra_columns: Optional mapping of `col_name -> (pa.DataType, scalar_or_list)`
            appended to the batch (for example, `__ReceivedAt` or `__PollId`).
        already_normalized: If `True`, skips calling `schema_spec.normalizer` on `rows`.

    Returns:
        A typed `pyarrow.RecordBatch` matching `schema_spec` (plus any dynamic or extra columns).
    """
    normalized_rows = (
        list(rows)
        if already_normalized
        else [schema_spec.normalizer(r, symbol_normalizer) for r in rows]
    )
    num_rows = len(normalized_rows)

    arrays: list[pa.Array] = []
    fields: list[pa.Field] = list(schema_spec.arrow_schema)
    known_names = {f.name for f in fields}

    for field in schema_spec.arrow_schema:
        col_vals = [r.get(field.name) for r in normalized_rows]
        arrays.append(_coerce_column_array(col_vals, field.type))

    if schema_spec.allow_dynamic_columns and normalized_rows:
        dynamic_keys: list[str] = []
        seen_dyn: set[str] = set()
        for r in normalized_rows:
            for k in r:
                if k not in known_names and not k.startswith("__") and k not in seen_dyn:
                    seen_dyn.add(k)
                    dynamic_keys.append(k)
        for dk in dynamic_keys:
            col_vals = [r.get(dk) for r in normalized_rows]
            arr = _infer_dynamic_column(col_vals)
            fields.append(pa.field(dk, arr.type))
            arrays.append(arr)

    if extra_columns:
        for col_name, (col_type, col_val_or_seq) in extra_columns.items():
            if isinstance(col_val_or_seq, list) and len(col_val_or_seq) == num_rows:
                vals = col_val_or_seq
            else:
                vals = [col_val_or_seq] * num_rows
            fields.append(pa.field(col_name, col_type))
            arrays.append(_coerce_column_array(vals, col_type))

    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def record_batches_to_arrow_table(
    batches: Sequence[pa.RecordBatch],
    schema_spec: SchemaSpec,
    *,
    extra_fields: Sequence[pa.Field] | None = None,
) -> pa.Table:
    """Combine one or more `RecordBatch` instances into a `pyarrow.Table`.

    When `batches` is empty, returns an empty `pyarrow.Table` preserving the exact
    column names and types of `schema_spec`. When `schema_spec.allow_dynamic_columns`
    is `True`, schemas across multiple batches are unified via `promote_options="default"`.

    Args:
        batches: Sequence of `pyarrow.RecordBatch` chunks.
        schema_spec: `SchemaSpec` used to construct an empty table when `batches` is empty.
        extra_fields: Optional extra `pyarrow.Field` definitions to include on empty tables.

    Returns:
        A consolidated `pyarrow.Table`.
    """
    base_schema = (
        schema_spec.with_extra_fields(extra_fields) if extra_fields else schema_spec.arrow_schema
    )
    if not batches:
        return base_schema.empty_table()

    if len(batches) == 1 and not schema_spec.allow_dynamic_columns:
        return pa.Table.from_batches(batches, schema=batches[0].schema)

    tables = [pa.Table.from_batches([b]) for b in batches]
    return pa.concat_tables(tables, promote_options="default")


@contextlib.contextmanager
def exec_ctx_scope(exec_ctx: ExecutionContext | None) -> Iterator[None]:
    """Enter a thread-safe and re-entrant Deephaven `ExecutionContext` scope.

    Deephaven requires background threads (such as WebSocket flushers, REST pollers,
    and watchlist executors) to bind the host `ExecutionContext` before invoking table
    operations or `TablePublisher.add`. Calling `j_exec_ctx.open()` directly returns a
    re-entrant token that can be safely closed in a `finally` block across concurrent
    threads.

    Args:
        exec_ctx: Deephaven `ExecutionContext` to bind, or `None` for a no-op scope.

    Yields:
        Control to the wrapped block with `exec_ctx` bound on the current thread.
    """
    if exec_ctx is not None:
        token = exec_ctx.j_exec_ctx.open()
        try:
            yield
        finally:
            token.close()
    else:
        yield


def arrow_to_deephaven_table(
    pa_table: pa.Table,
    exec_ctx: ExecutionContext | None = None,
) -> Table:
    """Convert a `pyarrow.Table` into a `deephaven.table.Table` inside `exec_ctx`.

    Args:
        pa_table: Source `pyarrow.Table`.
        exec_ctx: Optional Deephaven `ExecutionContext` to bind during conversion.

    Returns:
        A materialized Deephaven `Table`.
    """
    from deephaven import arrow as dh_arrow

    with exec_ctx_scope(exec_ctx):
        return dh_arrow.to_table(pa_table)
