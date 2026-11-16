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
- `keep_window` and `whole_bars` keep the rows a `history()` window holds.

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
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.column_descriptions import OPTION_ROOT, RAW_SYMBOL
from deephaven_massive._datasets.row_times import DATE, time_column, within
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
    log_unreadable,
    unreadable_symbol_message,
)
from deephaven_massive._errors import RowDataError
from deephaven_massive._massive import DriftReport, DriftSample, to_batch
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.withheld import without_unread_keys

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.schema import ColumnType
    from deephaven_massive._datasets.source_columns import SourceColumn
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._enums import OnError
    from deephaven_massive._massive import RestPath

__all__ = [
    "CFI",
    "UNDERLYING",
    "RowConverter",
    "is_error_row",
    "keep_window",
    "whole_bars",
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
        unreadable: The read's `unreadable_symbols` setting: what a spelling the library
            cannot read does.
        raw_symbol: Whether the schema has `RawSymbol`, which the session decides.
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
    unreadable: OnError
    raw_symbol: bool
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
        raw_symbol: bool,
    ) -> RowConverter:
        """Prepare the conversion from the path's generated fields and the dataset's schema.

        Args:
            catalog: The catalog, for the dataset's merged schema.
            spec: The dataset.
            rest_path: The path.
            unreadable_symbols: The read's setting for a symbol spelling the library cannot
                read.
            raw_symbol: Whether the schema has the `RawSymbol` column
                (`SessionCore.raw_symbol`).

        Returns:
            The converter.
        """
        schema = catalog.dataset_schema(spec, raw_symbol=raw_symbol)
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
            symbols=WireSymbols(spec.name, spec.asset_class, spec.symbols_as_written),
            unreadable=unreadable_symbols,
            raw_symbol=raw_symbol,
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
            decoded_rows, decoded, marks = to_batch(part, self.rest_path, required=required)
            batch, unread = without_unread_keys(decoded_rows, marks)
            batch, checked = self._apply(batch, symbol)
            batches.append(batch)
            drift = drift.merge(decoded).merge(unread).merge(checked)
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
            `RawSymbol`) computed from the symbols and an index `Underlying` given its `I:`
            prefix by the row's CFI code (`_canonical_underlyings`); and the drift of its
            timestamps and widening: the rows dropped for their time column, the wire keys of
            the other epoch fields that held a value out of bounds, and those of the `long`
            fields whose cast to a `double` column rounded a value (`RoundedFields`), each with
            its first such value.

        Raises:
            RowDataError: If a row's symbol is a spelling the library cannot read and the
                read's `unreadable_symbols` is `RAISE`; under `LOG` and `IGNORE` such a row
                keeps a null `Symbol`, and the text in `RawSymbol` where the schema has it.
        """
        rows = batch.num_rows
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(rows, pa.string())
        dropped: pa.Array | None = None
        found = DriftReport()
        rounded = RoundedFields()
        for column_type, name, source in self.columns:
            if name == OPTION_ROOT:
                values = option_roots(arrays["Symbol"])
            elif name == RAW_SYMBOL:
                values = raw
            elif source is None:
                values = pa.nulls(rows, column_type.arrow_type)
            elif source.field is None:
                values = pa.array([symbol] * rows, column_type.arrow_type)
            else:
                values = batch.column(source.field.column)
                if name == "Symbol":
                    values, raw = self._read_symbols(values)
                if source.unit is not None:
                    values, failed, report = self._converted_instants(
                        values, name=name, unit=source.unit, wire_key=source.field.wire_key
                    )
                    found = found.merge(report)
                    if failed is not None:
                        dropped = failed
            arrays[name] = rounded.widen(
                values, column_type.arrow_type, wire_key=None if source is None else source.wire_key
            )
        self._canonical_underlyings(arrays)
        converted = pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        drift = found.merge(rounded.drift())
        if dropped is not None:
            converted = converted.filter(pc.invert(dropped))
            drift = drift.merge(DriftReport(dropped_rows=rows - converted.num_rows))
        return converted, drift

    def _read_symbols(self, sent: pa.Array) -> tuple[pa.Array, pa.Array]:
        """Read a page's symbols into the library's spellings (`symbols.read`).

        Args:
            sent: The `Symbol` column's values as sent.

        Returns:
            The `Symbol` column and the `RawSymbol` column: the text of each symbol the
            library cannot read, whose `Symbol` is null.

        Raises:
            RowDataError: If a symbol is a spelling the library cannot read and the read's
                `unreadable_symbols` is `RAISE`; otherwise each such spelling is logged once
                (`log_unreadable`).
        """
        read = self.symbols.read(sent, {})
        if read.unreadable:
            if effect_of(self.unreadable).fails:
                raise RowDataError(unreadable_symbol_message(self.dataset, read.unreadable))
            log_unreadable(
                self.dataset, read.unreadable, self.unreadable, raw_symbol=self.raw_symbol
            )
        return read.symbols, read.raw

    def _converted_instants(
        self, sent: pa.Array, *, name: str, unit: TimeUnit, wire_key: str
    ) -> tuple[pa.Array, pa.Array | None, DriftReport]:
        """Convert one epoch column to instants, and classify the values that fail.

        Each value runs through the masked timestamp routine (`timestamps.masked_instants`),
        and a value that fails it is null. A failure in the time column drops its row; one in
        any other column is drift of its field.

        Args:
            sent: The column's values as sent.
            name: The column's name.
            unit: The unit of the column's epochs.
            wire_key: The wire key of the field behind the column.

        Returns:
            The instants; the mask of the rows to drop, set only for the time column when a
            value failed, else None; and the failures' drift: for the time column the
            dropped rows' reason (their count is the caller's), for another column its wire key
            as unconvertible with the first failing value; empty when nothing failed.
        """
        values, failed = masked_instants(
            unit, sent, time_column=name == self.time_name, label=f"{self.dataset}.{name}"
        )
        first = first_failure(sent, failed)
        if first is None:
            return values, None, DriftReport()
        if name == self.time_name:
            reason = mismatch_message(f"{self.dataset}.{name}", unit, first)
            return values, failed, DriftReport(dropped_reason=reason)
        sample = DriftSample(None, str(first))
        return (
            values,
            None,
            DriftReport(unconvertible=frozenset({wire_key}), samples={wire_key: sample}),
        )

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
