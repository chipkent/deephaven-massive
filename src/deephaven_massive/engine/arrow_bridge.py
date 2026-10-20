"""Zero-dataclass JSON/CSV -> PyArrow RecordBatch -> Deephaven Table bridge."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
import contextlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
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
    """Convert snake_case, kebab-case, or camelCase field names to PascalCase."""
    spaced = _CAMEL_BOUNDARY_RE.sub(r"\1_\2", name.strip())
    parts = [p for p in _SNAKE_BOUNDARY_RE.split(spaced) if p]
    if not parts:
        return "Field"
    result = "".join(p[:1].upper() + p[1:].lower() for p in parts)
    if result[0].isdigit():
        return f"F{result}"
    return result


def to_utc_ns(val: Any) -> int | None:
    """Normalize Unix s/ms/us/ns numbers, datetime objects, or ISO-8601 strings to UTC nanoseconds."""
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
        abs_v = abs(val)
        if abs_v >= 1e17:
            return int(val)
        if abs_v >= 1e14:
            return int(val * 1_000)
        if abs_v >= 1e11:
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
    """Normalize 'YYYY-MM-DD' strings, date/datetime objects, or timestamps to days since 1970-01-01."""
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
    """Convert scalar value to float64 or None."""
    if val is None or val == "" or isinstance(val, bool):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def to_int(val: Any) -> int | None:
    """Convert scalar value to int or None."""
    if val is None or val == "" or isinstance(val, bool):
        return None
    try:
        return int(float(val)) if isinstance(val, str) and "." in val else int(val)
    except (TypeError, ValueError):
        return None


def to_bool(val: Any) -> bool | None:
    """Convert scalar value to bool or None."""
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
    """Convert scalar value to stripped string or None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s != "" else None


def parse_int32_list(val: Any) -> list[int] | None:
    """Parse list of integers or CSV string like '[12, 37]' / '12,37' / '12 37' into list[int]."""
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
    """Parse list of strings or delimited string into list[str]."""
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


def flatten_dict(
    data: Mapping[str, Any], prefix: str = ""
) -> dict[str, Any]:
    """Flatten nested JSON dictionaries (including financial statement `{value, unit}` objects) into PascalCase keys."""
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
    """Specification binding a PyArrow schema, key columns, and row normalizer."""

    name: str
    arrow_schema: pa.Schema
    key_cols: tuple[str, ...]
    normalizer: Callable[[Mapping[str, Any], SymbolNormalizer | None], dict[str, Any]]
    allow_dynamic_columns: bool = False

    def with_extra_fields(self, extra_fields: Sequence[pa.Field]) -> pa.Schema:
        """Return a copy of arrow_schema with extra fields (such as __ReceivedAt or __PollId) appended."""
        fields = list(self.arrow_schema) + list(extra_fields)
        return pa.schema(fields)


def _coerce_column_array(values: list[Any], field_type: pa.DataType) -> pa.Array:
    """Build a typed PyArrow array for a single column."""
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
    """Infer a sensible PyArrow array for a dynamic fundamental/partner column."""
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
    """Convert raw or normalized row dicts into a typed :class:`pyarrow.RecordBatch`."""
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
            for k in r.keys():
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
    """Combine one or more RecordBatches (or create an empty typed table if empty) into a PyArrow Table."""
    base_schema = (
        schema_spec.with_extra_fields(extra_fields)
        if extra_fields
        else schema_spec.arrow_schema
    )
    if not batches:
        return base_schema.empty_table()

    if len(batches) == 1 and not schema_spec.allow_dynamic_columns:
        return pa.Table.from_batches(batches, schema=batches[0].schema)

    tables = [pa.Table.from_batches([b]) for b in batches]
    return pa.concat_tables(tables, promote_options="default")


@contextlib.contextmanager
def exec_ctx_scope(exec_ctx: ExecutionContext | None) -> Iterator[None]:
    """Thread-safe and re-entrant ExecutionContext context manager."""
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
    """Convert a :class:`pyarrow.Table` into a :class:`deephaven.table.Table` under the execution context."""
    from deephaven import arrow as dh_arrow

    with exec_ctx_scope(exec_ctx):
        return dh_arrow.to_table(pa_table)
