"""REST page rows to batches of a dataset's schema: conversion, error rows, and the history keep.

Each page a REST read returns is a list of decoded JSON rows. This module turns them into Arrow
batches of the dataset's merged schema, one page at a time:

- `RowConverter` decodes a page against the path's generated fields (`_massive.to_batch`), renames
  its columns for the dataset (`source_columns.rename`), converts its timestamps
  (`_datasets/timestamps.py`), widens each column to its merged type
  (`source_columns.widen`), and reads its symbols into the canonical form (`WireSymbols`). A row
  that carries an `error` (`is_error_row`: a ticker the plan does not include) keeps only its
  symbol key and its `Error` and `Message` columns, or is dropped on a path that joins its
  dataset through a projection.
- `stamped` applies the stamp rule of bars of a day or longer (`row_times.stamp_rule`), which
  serves every bar and warns on a stamp Massive is not known to use; `keep_window` and
  `whole_bars` keep the rows a `history()` window holds.

An odd value never costs a page its other rows: a value that does not fit, outside the key
columns, is null and reported as drift, and a row whose key cannot be read is dropped and
counted. A page whose every row was dropped raises `RowDataError`, since that is the sign of a
change in Massive's format. Everything here is pure: no request, no thread, and no state, and
a converter is shared read-only by the
worker threads of one read. The REST transport (`rest.py`) reads the pages, and the host
decision (`host_decision.py`) uses the same error-row rule.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.column_descriptions import OPTION_ROOT, RAW_SYMBOL
from deephaven_massive._datasets.row_times import DATE, StampRule, time_column, within
from deephaven_massive._datasets.source_columns import rename, required_fields, widen
from deephaven_massive._datasets.symbols import option_roots
from deephaven_massive._datasets.timestamps import first_failure, masked_instants, mismatch_message
from deephaven_massive._datasets.wire_symbols import WireSymbols, unreadable_symbol_message
from deephaven_massive._enums import OnError
from deephaven_massive._errors import RowDataError
from deephaven_massive._massive import DriftReport, DriftSample, to_batch

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.schema import ColumnType
    from deephaven_massive._datasets.source_columns import SourceColumn
    from deephaven_massive._massive import RestPath

__all__ = [
    "RowConverter",
    "is_error_row",
    "keep_window",
    "stamped",
    "whole_bars",
]


@dataclass(frozen=True, eq=False, slots=True)
class RowConverter:
    """How one dataset turns the rows of one REST path into batches of its schema.

    Built per read and shared, read-only, by the read's worker threads.

    Attributes:
        dataset: The dataset name, for messages.
        rest_path: The path.
        columns: Per schema column, in order: its type, its name, and the source column that
            fills it, or None for a column the path does not carry (null).
        arrow_schema: The dataset's Arrow schema.
        required: The long-form columns every row must carry: the dataset's keys for the path
            that are not filled from the request.
        required_on_error: The required columns of an error row (`is_error_row`): only
            its symbol key, so a ticker the plan does not include keeps its row.
        symbols: How a symbol read from the rows becomes canonical (`WireSymbols`), and
            what a spelling the library cannot read does.
        symbol_key: The wire key of the rows' symbol field, or None when the symbol is filled
            from the request.
        time_name: The dataset's time column (`row_times.time_column`), whose epoch `0` is
            kept, and so fails the timestamp bound, rather than read as absent.
    """

    dataset: str
    rest_path: RestPath
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    required: frozenset[str]
    required_on_error: frozenset[str]
    symbols: WireSymbols
    symbol_key: str | None
    time_name: str

    @classmethod
    def build(
        cls,
        catalog: Catalog,
        spec: DatasetSpec,
        rest_path: RestPath,
        *,
        unreadable_symbols: OnError,
    ) -> RowConverter:
        """Prepare the conversion from the path's generated fields and the dataset's schema.

        Args:
            catalog: The catalog, for the dataset's merged schema.
            spec: The dataset.
            rest_path: The path.
            unreadable_symbols: The session's setting for a symbol spelling the library
                cannot read; `LOG` and `IGNORE` also add the `RawSymbol` column to the schema.

        Returns:
            The converter.
        """
        schema = catalog.dataset_schema(spec, raw_symbol=unreadable_symbols is not OnError.RAISE)
        renamed = {c.column: c for c in rename(spec, rest_path)}
        symbol = renamed.get("Symbol")
        # The rows' own symbol field, unless the key is filled from the request.
        symbol_field = None if symbol is None else symbol.field
        return cls(
            dataset=spec.name,
            rest_path=rest_path,
            columns=tuple((c.type, c.name, renamed.get(c.name)) for c in schema.columns),
            arrow_schema=schema.arrow_schema(),
            required=frozenset(f.column for f in required_fields(spec, rest_path)),
            required_on_error=frozenset(() if symbol_field is None else (symbol_field.column,)),
            symbols=WireSymbols(
                spec.name, spec.asset_class, unreadable_symbols, spec.symbols_as_written
            ),
            symbol_key=None if symbol_field is None else symbol_field.wire_key,
            time_name=time_column(spec.times),
        )

    def convert(
        self, rows: Sequence[Mapping[str, object]], symbol: str | None
    ) -> tuple[list[pa.RecordBatch], DriftReport, list[Mapping[str, object]]]:
        """Convert one page's rows into batches of the dataset's schema.

        Error rows (`is_error_row`: a ticker the plan does not include) are decoded
        apart, with only their symbol required, and come after the page's other rows.

        A row is dropped, and counted in the drift (`DriftReport.dropped_rows`), when a key the
        dataset requires is absent, null, or does not fit (`to_batch`), or when its time column
        overflows or lies outside 1990 to 2100. Any other value that does not fit, or any other
        timestamp out of bounds, is null and reported in `DriftReport.unconvertible`.

        Args:
            rows: The page's rows.
            symbol: The request's one symbol, which fills the keys taken from the request.

        Returns:
            The batches (none for an empty page), the page's drift, and the error rows.

        Raises:
            RowDataError: If the page had rows and every one of them was dropped, the sign of
                a change in Massive's format (a renamed key, a new timestamp unit) rather than
                of one odd row; the message names the dataset and the first row's reason. Also
                the `RowDataError` of `_apply`, for a symbol spelling the library cannot read.
        """
        flagged = [row for row in rows if is_error_row(row)]
        good = [row for row in rows if not is_error_row(row)] if flagged else rows
        batches: list[pa.RecordBatch] = []
        drift = DriftReport()
        for part, required in ((good, self.required), (flagged, self.required_on_error)):
            if not part:
                continue
            batch, decoded = to_batch(part, self.rest_path, required=required)
            batch, checked = self._apply(batch, symbol)
            batches.append(batch)
            drift = drift.merge(decoded).merge(checked)
        if rows and not any(batch.num_rows for batch in batches):
            raise RowDataError(
                f"{self.dataset}: every row of a page of {len(rows)} was dropped, so Massive's "
                f"format may have changed; the first: {drift.dropped_reason}"
            )
        return batches, drift, flagged

    def _apply(
        self, batch: pa.RecordBatch, symbol: str | None
    ) -> tuple[pa.RecordBatch, DriftReport]:
        """Turn one decoded batch into a batch of the dataset's schema.

        Each epoch column runs through the masked timestamp routine
        (`timestamps.masked_instants`): a row whose time column fails it is dropped, and any
        other value that fails it is null.

        Args:
            batch: The path's fields, by long-form column (`to_batch`).
            symbol: The value of the keys taken from the request.

        Returns:
            The batch in the dataset's schema, with the derived columns (`OptionRoot`,
            `RawSymbol`) computed from the symbols; and the drift of its timestamps: the rows
            dropped for their time column, and the wire keys of the other epoch fields that
            held a value out of bounds, each with its first such value.

        Raises:
            RowDataError: If a row's symbol is a spelling the library cannot read and the
                session's `unreadable_symbols` is `RAISE`; under `LOG` and `IGNORE` such a
                row keeps a null `Symbol` and the text in `RawSymbol`.
        """
        rows = batch.num_rows
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(rows, pa.string())
        dropped: pa.Array | None = None
        reason = ""
        odd: dict[str, DriftSample] = {}
        for column_type, name, source in self.columns:
            target = column_type.arrow_type
            if name == OPTION_ROOT:
                values = option_roots(arrays["Symbol"])
            elif name == RAW_SYMBOL:
                values = raw
            elif source is None:
                values = pa.nulls(rows, target)
            elif source.field is None:
                values = pa.array([symbol] * rows, target)
            else:
                values = batch.column(source.field.column)
                if name == "Symbol":
                    read = self.symbols.read(values, {})
                    if read.unreadable and not self.symbols.keeps:
                        raise RowDataError(unreadable_symbol_message(self.dataset, read.unreadable))
                    values, raw = read.symbols, read.raw
                if source.unit is not None:
                    sent = values
                    values, failed = masked_instants(
                        source.unit,
                        sent,
                        time_column=name == self.time_name,
                        label=f"{self.dataset}.{name}",
                    )
                    first = first_failure(sent, failed)
                    if first is not None and name == self.time_name:
                        dropped = failed
                        reason = mismatch_message(f"{self.dataset}.{name}", source.unit, first)
                    elif first is not None:
                        odd[source.field.wire_key] = DriftSample(None, str(first))
            arrays[name] = widen(values, target)
        converted = pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        drift = DriftReport(unconvertible=frozenset(odd), samples=odd)
        if dropped is not None:
            converted = converted.filter(pc.invert(dropped))
            count = rows - converted.num_rows
            drift = drift.merge(DriftReport(dropped_rows=count, dropped_reason=reason))
        return converted, drift


def whole_bars(rows: pa.RecordBatch, window: HistoryWindow | None) -> pa.RecordBatch:
    """Keep the bars of a `history()` window whose filled window lies in it (`row_times.within`).

    Args:
        rows: Rows with their time columns filled.
        window: The `history()` window, or None for `fetch()` and `poll()`.

    Returns:
        The rows kept; every row where the window bounds no bar (ticks, `fetch()`, `poll()`).
    """
    if window is None or window.bars is None:
        return rows
    return within(rows, window.bars)


def keep_window(
    batch: pa.RecordBatch, window: HistoryWindow | None, *, time: str
) -> pa.RecordBatch:
    """Keep the rows of a `history()` window: on their time, and for sessions on `Date`.

    A request can return rows outside the window: the aggregates' `{from}/{to}` reach whole
    periods, a bar may not lie wholly inside the range, and futures bars named by date are
    requested over a window widened for holidays. A row is kept when its time column lies in
    `HistoryWindow.times` and its session date, `Date` (Massive's `session_end_date`, as ISO
    text, which orders as dates do), in `HistoryWindow.sessions`, where the window sets them.
    Without the column a bound is about, that bound keeps every row. It runs before the time
    columns are filled, so a bar of several sessions still carries its label here.

    Args:
        batch: Rows in the dataset's schema.
        window: The `history()` window, or None for `fetch()` and `poll()`.
        time: The dataset's time column: `Timestamp`, or a bar's `TimestampStart`.

    Returns:
        The rows inside the window.
    """
    if window is None:
        return batch
    names = batch.schema.names
    masks = []
    if window.sessions is not None and DATE in names:
        sessions = batch.column(DATE)
        masks.append(pc.greater_equal(sessions, pa.scalar(window.sessions.first.isoformat())))
        masks.append(pc.less_equal(sessions, pa.scalar(window.sessions.last.isoformat())))
    if window.times is not None and time in names:
        times = batch.column(time)
        masks.append(pc.greater_equal(times, pa.scalar(window.times.start_ns, times.type)))
        masks.append(pc.less(times, pa.scalar(window.times.end_ns, times.type)))
    if not masks:
        return batch
    return batch.filter(functools.reduce(pc.and_, masks))


def stamped(
    spec: DatasetSpec, source: str, batch: pa.RecordBatch, *, rule: StampRule
) -> pa.RecordBatch:
    """Apply a stamp rule to a batch of bars (`row_times.stamp_rule`), keeping every bar.

    Args:
        spec: The dataset, for the WARNING.
        source: The REST path template the bars came from, for the WARNING.
        batch: Rows in the dataset's schema.
        rule: The day zone, the stamps Massive is known to use besides 00:00, and the
            session's warnings.

    Returns:
        The batch, with its `TimestampStart` at 00:00 of each bar's day in the class day zone.

    A stamp at another time or on another day is moved the same way and logs the WARNING of
    `row_times.StampRule.stamp`, once per dataset and timespan per session.
    """
    name = time_column(spec.times)
    index = batch.schema.get_field_index(name)
    moved = rule.stamp(spec.name, batch.column(index), source=source)
    return batch.set_column(index, name, moved)


def is_error_row(row: Mapping[str, object]) -> bool:
    """Tell whether a snapshot row reports a ticker the plan does not include.

    Massive answers such a ticker with a row that carries an `error` (`NOT_ENTITLED`) and a
    `message` in place of its data. The host decision counts the row as absent, and the REST
    transport keeps or drops it apart from the data rows; both use this one test.

    Args:
        row: One decoded row.

    Returns:
        True when the row's `error` is set.
    """
    return row.get("error") is not None
