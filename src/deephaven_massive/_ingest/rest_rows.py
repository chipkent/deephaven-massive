"""REST page rows to batches of a dataset's schema: conversion, error rows, and the history keep.

Each page a REST read returns is a list of decoded JSON rows. This module turns them into Arrow
batches of the dataset's merged schema, one page at a time:

- `RowConverter` decodes a page against the path's generated fields (`_massive.to_batch`), renames
  its columns for the dataset (`source_columns.rename`), converts its timestamps
  (`_datasets/timestamps.py`), widens each column to its merged type
  (`source_columns.widen`), and reads its symbols into the canonical form (`WireSymbols`), an
  options row's index underlying included (`wire_symbols.index_underlyings`). A row
  that carries an `error` (`is_error_row`: a ticker the plan does not include) keeps only its
  symbol key and its `Error` and `Message` columns, or is dropped on a path that joins its
  dataset through a projection.
- `keep_window` keeps the rows a `history()` window holds; the transports judge a bar's whole
  window (`row_times.whole`) once its time columns are filled.

An odd row never costs a page its other rows: a row the library cannot read whole (a key, a
symbol, or a value) is marked by its fault (`withheld.mark`), and the REST transport withholds
it and fails a page none of whose rows inside the window it could read. Everything here is
pure: no request, no thread, and no state, and a converter is shared read-only by the worker
threads of one read. The REST transport (`rest.py`) reads the pages, and the host decision
(`host_decision.py`) uses the same error-row rule.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.column_descriptions import OPTION_ROOT
from deephaven_massive._datasets.row_times import DATE, time_column
from deephaven_massive._datasets.source_columns import (
    KEY_COLUMNS,
    RoundedFields,
    rename,
    required_fields,
)
from deephaven_massive._datasets.symbols import option_roots
from deephaven_massive._datasets.timestamps import first_failure, masked_instants, mismatch_message
from deephaven_massive._datasets.wire_symbols import (
    WireSymbols,
    index_underlyings,
    unreadable_symbol_reason,
)
from deephaven_massive._massive import DriftReport, DriftSample, RowFault, to_batch
from deephaven_massive._tables.withheld import (
    MARK_COLUMNS,
    Failure,
    in_window,
    mark_failures,
    with_marks,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.schema import ColumnType
    from deephaven_massive._datasets.source_columns import SourceColumn
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._massive import RestPath

__all__ = [
    "CFI",
    "UNDERLYING",
    "RowConverter",
    "is_error_row",
    "keep_window",
]

logger = logging.getLogger(__name__)

UNDERLYING: Final = KEY_COLUMNS["fanout"]
"""The column naming the instrument a row is linked to, such as an option's underlying."""

CFI: Final = "Cfi"
"""The column of a row's CFI code (ISO 10962), which the options contracts list carries."""


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
        symbols: How a symbol read from the rows becomes canonical (`WireSymbols`).
        symbol_column: The long-form column of the rows' symbol field, or None when the symbol
            is filled from the request.
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
    symbol_column: str | None
    symbol_key: str | None
    time_name: str

    @classmethod
    def build(cls, catalog: Catalog, spec: DatasetSpec, rest_path: RestPath) -> RowConverter:
        """Prepare the conversion from the path's generated fields and the dataset's schema.

        Args:
            catalog: The catalog, for the dataset's merged schema.
            spec: The dataset.
            rest_path: The path.

        Returns:
            The converter.
        """
        schema = catalog.dataset_schema(spec)
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
            symbols=WireSymbols(spec.name, spec.symbols_class, spec.symbols_as_written),
            symbol_column=None if symbol_field is None else symbol_field.column,
            symbol_key=None if symbol_field is None else symbol_field.wire_key,
            time_name=time_column(spec.times),
        )

    def convert(
        self, rows: Sequence[Mapping[str, object]], symbol: str | None
    ) -> tuple[list[pa.RecordBatch], DriftReport, list[Mapping[str, object]]]:
        """Convert one page's rows into batches of the dataset's schema, every row marked or whole.

        Error rows (`is_error_row`: a ticker the plan does not include) are decoded
        apart, with only their symbol required, and come after the page's other rows.

        Every row the page held is in the batches, with the mark columns
        (`withheld.with_marks`): a row is marked when a key the dataset requires is absent,
        null, or does not fit, or its time column overflows or lies outside 1990 to 2100
        (`unreadable_keys`); when its symbol is a spelling the library cannot read
        (`unreadable_symbols`); or when another value does not fit, an epoch value fails the
        timestamp routine, or a whole number its `double` column would round
        (`unconvertible_values`). The caller withholds the marked rows (`withhold_rows`).

        Args:
            rows: The page's rows.
            symbol: The request's one symbol, which fills the keys taken from the request.

        Returns:
            The batches (none for an empty page), with the mark columns; the page's drift (the
            fields that drifted, with a sample each); and the error rows.
        """
        flagged = [row for row in rows if is_error_row(row)]
        good = [row for row in rows if not is_error_row(row)] if flagged else rows
        batches: list[pa.RecordBatch] = []
        drift = DriftReport()
        for part, required in ((good, self.required), (flagged, self.required_on_error)):
            if not part:
                continue
            decoded, found, marks = to_batch(
                part, self.rest_path, required=required, symbol=self.symbol_column
            )
            sent = (
                pa.array([symbol] * decoded.num_rows, pa.string())
                if self.symbol_column is None
                else decoded.column(self.symbol_column)
            )
            batch, checked = self._apply(with_marks(decoded, marks, sent=sent), symbol)
            batches.append(batch)
            drift = drift.merge(found).merge(checked)
        return batches, drift, flagged

    def _apply(
        self, batch: pa.RecordBatch, symbol: str | None
    ) -> tuple[pa.RecordBatch, DriftReport]:
        """Turn one decoded batch into a batch of the dataset's schema, marking what fails.

        Each epoch column runs through the masked timestamp routine
        (`timestamps.masked_instants`): a row whose time column fails it is marked
        `unreadable_keys`, and one whose other epoch value fails it `unconvertible_values`. A
        symbol spelling the library cannot read marks its row `unreadable_symbols`, and a whole
        number a `double` column would round marks its row `unconvertible_values`.

        Args:
            batch: The path's fields, by long-form column (`to_batch`), with the mark columns.
            symbol: The value of the keys taken from the request.

        Returns:
            The batch in the dataset's schema, with the mark columns and the derived column
            (`OptionRoot`) computed from the symbols unless the path carries it as Massive's
            `root`, and an index `Underlying` given its `I:`
            prefix by the row's CFI code (`_canonical_underlyings`); and the drift of its
            timestamps and widening: the wire keys of the epoch fields other than the time
            column that held a value out of bounds, and those of the `long` fields whose cast
            to a `double` column rounded a value (`RoundedFields`), each with its first such
            value.
        """
        rows = batch.num_rows
        arrays: dict[str, pa.Array] = {}
        found = DriftReport()
        rounded = RoundedFields()
        failures: list[Failure] = []
        for column_type, name, source in self.columns:
            if name == OPTION_ROOT and source is None:
                values = option_roots(arrays["Symbol"])
            elif source is None:
                values = pa.nulls(rows, column_type.arrow_type)
            elif source.field is None:
                values = pa.array([symbol] * rows, column_type.arrow_type)
            else:
                values = batch.column(source.field.column)
                if name == "Symbol":
                    values, unread = self._read_symbols(values)
                    failures.append(unread)
                if source.unit is not None:
                    values, failed, report = self._converted_instants(
                        values, name=name, unit=source.unit, wire_key=source.field.wire_key
                    )
                    found = found.merge(report)
                    failures.extend(failed)
            arrays[name] = rounded.widen(
                values,
                column_type.arrow_type,
                wire_key=None if source is None else source.wire_key,
                column=name,
            )
        self._canonical_underlyings(arrays)
        converted = pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        for name in MARK_COLUMNS:
            converted = converted.append_column(name, batch.column(name))
        converted = mark_failures(converted, (*failures, *rounded.failures()))
        return converted, found.merge(rounded.drift())

    def _read_symbols(self, sent: pa.Array) -> tuple[pa.Array, Failure]:
        """Read a page's symbols into the library's spellings (`symbols.read`).

        Args:
            sent: The `Symbol` column's values as sent.

        Returns:
            The `Symbol` column, null for a spelling the library cannot read; and that failure,
            to mark the rows `unreadable_symbols`.
        """
        read = self.symbols.read(sent, {})

        def reason(row: int) -> str:
            return unreadable_symbol_reason(sent[row].as_py())

        return read.symbols, (RowFault.UNREADABLE_SYMBOLS, read.unreadable_rows, "Symbol", reason)

    def _converted_instants(
        self, sent: pa.Array, *, name: str, unit: TimeUnit, wire_key: str
    ) -> tuple[pa.Array, list[Failure], DriftReport]:
        """Convert one epoch column to instants, and classify the values that fail.

        Each value runs through the masked timestamp routine (`timestamps.masked_instants`),
        and a value that fails it is null. A failure in the time column makes its row's key
        unreadable; one in any other column is an unconvertible value, and drift of its field.

        Args:
            sent: The column's values as sent.
            name: The column's name.
            unit: The unit of the column's epochs.
            wire_key: The wire key of the field behind the column.

        Returns:
            The instants; the failure to mark, or none; and the drift of a column other than
            the time column, its wire key as unconvertible with the first failing value.
        """
        label = f"{self.dataset}.{name}"
        values, failed = masked_instants(
            unit, sent, time_column=name == self.time_name, label=label
        )
        first = first_failure(sent, failed)
        if first is None:
            return values, [], DriftReport()

        def reason(row: int) -> str:
            return mismatch_message(label, unit, sent[row].as_py())

        if name == self.time_name:
            return values, [(RowFault.UNREADABLE_KEYS, failed, name, reason)], DriftReport()
        sample = DriftSample(None, str(first))
        drift = DriftReport(unconvertible=frozenset({wire_key}), samples={wire_key: sample})
        return values, [(RowFault.UNCONVERTIBLE_VALUES, failed, name, reason)], drift

    def _canonical_underlyings(self, arrays: dict[str, pa.Array]) -> None:
        """Give a page's index underlyings their `I:` prefix (`wire_symbols.index_underlyings`).

        Massive's options contracts list writes an index underlying bare (`SPX`); the CFI code
        of each row says whether its underlying is an index. Only a page with both an
        `Underlying` and a `Cfi` column is read. Logs one DEBUG line for a page where any value
        changed, with the count and the first value.

        Args:
            arrays: The page's columns by name; its `Underlying` is replaced in place.
        """
        if UNDERLYING not in arrays or CFI not in arrays:
            return
        underlyings = arrays[UNDERLYING]
        canonical = index_underlyings(underlyings, arrays[CFI])
        changed = pc.fill_null(pc.not_equal(underlyings, canonical), fill_value=False)
        count = pc.sum(changed).as_py() or 0
        if count:
            logger.debug(
                "%s: %d of %d %s values from %s are indices by their CFI code and get the "
                "I: prefix, the first %r",
                self.dataset,
                count,
                len(underlyings),
                UNDERLYING,
                self.rest_path.template,
                pc.filter(underlyings, changed)[0].as_py(),
            )
        arrays[UNDERLYING] = canonical


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

    A marked row whose time or session could not be read stays (`withheld.in_window`), to be
    withheld and reported.

    Args:
        batch: Rows in the dataset's schema, with the mark columns.
        window: The `history()` window, or None for `fetch()` and `poll()`.
        time: The dataset's time column: `Timestamp`, or a bar's `TimestampStart`.

    Returns:
        The rows inside the window, and the marked rows it cannot place.
    """
    if window is None:
        return batch
    names = batch.schema.names
    bounds = []
    if window.sessions is not None and DATE in names:
        sessions = batch.column(DATE)
        first = pc.greater_equal(sessions, pa.scalar(window.sessions.first.isoformat()))
        last = pc.less_equal(sessions, pa.scalar(window.sessions.last.isoformat()))
        bounds.append((DATE, pc.and_(first, last)))
    if window.times is not None and time in names:
        times = batch.column(time)
        start = pc.greater_equal(times, pa.scalar(window.times.start_ns, times.type))
        end = pc.less(times, pa.scalar(window.times.end_ns, times.type))
        bounds.append((time, pc.and_(start, end)))
    if not bounds:
        return batch
    return batch.filter(in_window(batch, bounds))


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
