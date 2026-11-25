"""The rows the library could not read whole: marked as they are read, withheld in one place.

A row of Massive's data is published only when the library read it whole. Every decoder (a
REST page, a flat-file block, a WebSocket frame) returns every row with the rows it could not
read whole marked by fault (`_massive.RowMarks`); `with_marks` carries those marks with the
rows as four columns, so a filter or a `take` keeps each mark with its row. Each later step that
can find a row it cannot read (a symbol, an instant, a widening that rounds, a bar's stamp, a
ticker list) marks the row with `mark`, which keeps a row's first fault. A row whose own time
cannot be read stays through a window (`in_window`), since no window can place it.
`withhold_rows`, called once per payload after the last step that marks, takes every marked
row out and groups the withheld rows by fault, canonical symbol, symbol as sent, and field, for
counting and for the payload's `ProblemEvent`s. Whether they fail the read is the caller's
call, by `failure_of` and the read's policy.

Pure Arrow and plain values: no lock, no thread, and no Deephaven call, so a REST worker, a
flat-file reader, and a flush thread under its flush lock may all call it.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._enums import OnError
from deephaven_massive._error_policy import ResolvedPolicy, policy_hint
from deephaven_massive._log_limits import counted, prose_list
from deephaven_massive._massive import DriftReport, RowFault
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.problem_events import (
    EVENTS_PER_PAYLOAD,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from deephaven_massive._massive import RowMarks, Transport

__all__ = [
    "FAULT",
    "MARK_COLUMNS",
    "REASON",
    "WITHHELD_LISTED",
    "Failure",
    "Withheld",
    "failure_of",
    "in_window",
    "logs_withheld",
    "mark",
    "mark_failures",
    "marked_count",
    "merged",
    "placed",
    "reason_sentence",
    "setting_of",
    "with_marks",
    "withhold_rows",
]

_Rows: TypeAlias = "pa.RecordBatch | pa.Table"

FAULT: Final = "__fault"
"""The mark column of each row's `RowFault` value, null for a row read whole."""
FIELD: Final = "__field"
"""The mark column of the column whose value a marked row could not read."""
REASON: Final = "__reason"
"""The mark column of why a marked row could not be read."""
SENT: Final = "__sent"
"""The mark column of each row's symbol as Massive sent it, null when it has none."""
MARK_COLUMNS: Final = (FAULT, FIELD, REASON, SENT)
"""The columns `with_marks` adds after a batch's own, which `withhold_rows` removes."""

_TEXT: Final = pa.string()

Failure: TypeAlias = "tuple[RowFault, pa.Array, str, Callable[[int], str]]"
"""The rows one step could not read, for `mark_failures`: the fault, the mask of the rows, the
column, and each row's reason by its index."""


def with_marks(
    rows: _Rows, marks: RowMarks | Sequence[tuple[int, RowMarks]], *, sent: pa.Array | None
) -> _Rows:
    """Add a decoder's marks to its rows, as the mark columns after the rows' own.

    Args:
        rows: Every row the decoder read, in its order; possibly converted, never filtered.
        marks: The decoder's marks of those rows; or, for rows gathered from several payloads
            (a flush's WebSocket frames), each payload's first row in `rows` with its marks.
        sent: Each row's symbol as Massive sent it, or None when the rows carry none, in which
            case a marked row takes the symbol its decoder quoted (`RowMark.symbols`).

    Returns:
        The rows with the mark columns.
    """
    count = rows.num_rows
    faults: list[str | None] = [None] * count
    fields: list[str | None] = [None] * count
    reasons: list[str | None] = [None] * count
    quoted: list[str | None] = [None] * count
    parts = ((0, marks),) if isinstance(marks, Mapping) else marks
    for offset, part in parts:
        for fault, found in part.items():
            for at, row in enumerate(found.rows):
                faults[offset + row] = fault.value
                fields[offset + row] = found.fields[at]
                reasons[offset + row] = found.reasons[at]
                quoted[offset + row] = found.symbols[at]
    texts = pa.array(quoted, _TEXT) if sent is None else pc.cast(sent, _TEXT)
    for name, values in (
        (FAULT, pa.array(faults, _TEXT)),
        (FIELD, pa.array(fields, _TEXT)),
        (REASON, pa.array(reasons, _TEXT)),
        (SENT, texts),
    ):
        rows = rows.append_column(name, values)
    return rows


def mark(
    rows: _Rows,
    fault: RowFault,
    failed: pa.Array,
    *,
    field: str,
    reason: Callable[[int], str],
) -> _Rows:
    """Mark the rows a step could not read, unless an earlier step marked them.

    Args:
        rows: The rows, with the mark columns (`with_marks`).
        fault: The fault.
        failed: Where the step failed, one boolean per row; a null is no failure.
        field: The column the step could not read.
        reason: Why, for one row's index; called only for a row marked now.

    Returns:
        The rows, each newly marked row's fault, field, and reason set.
    """
    if not rows.num_rows:  # pyarrow 23.0.1, the floor, crashes on an empty table here
        return rows
    faults = rows.column(FAULT)
    new = pc.and_(pc.fill_null(failed, fill_value=False), pc.is_null(faults))
    found = pc.indices_nonzero(new).to_pylist()
    if not found:
        return rows
    why = {row: reason(row) for row in found}
    reasons = pa.array([why.get(row) for row in range(rows.num_rows)], _TEXT)
    updates = (
        (FAULT, pc.if_else(new, pa.scalar(fault.value, _TEXT), faults)),
        (FIELD, pc.if_else(new, pa.scalar(field, _TEXT), rows.column(FIELD))),
        (REASON, pc.if_else(new, reasons, rows.column(REASON))),
    )
    for name, values in updates:
        rows = rows.set_column(rows.schema.get_field_index(name), name, values)
    return rows


def mark_failures(rows: _Rows, failures: Iterable[Failure]) -> _Rows:
    """Mark the rows of several steps, in order, each row with its first fault (`mark`).

    Args:
        rows: The rows, with the mark columns.
        failures: Each step's failure.

    Returns:
        The rows, marked.
    """
    for fault, failed, field, reason in failures:
        rows = mark(rows, fault, failed, field=field, reason=reason)
    return rows


def marked_count(rows: _Rows) -> int:
    """Return how many rows are marked.

    Args:
        rows: The rows, with the mark columns.

    Returns:
        The count of rows with a fault.
    """
    return int(rows.num_rows - rows.column(FAULT).null_count)


def in_window(rows: _Rows, bounds: Sequence[tuple[str, pa.Array]]) -> pa.Array:
    """Return the rows a window keeps: those inside every bound, a marked unreadable key aside.

    A window bounds rows by their time, their session, or both. A marked row whose bounded key
    is null could not have that key read, and no bound can place it, so that bound keeps it,
    to be withheld and reported; every other bound still judges it. A row outside a bound is
    neither published nor reported, whatever else is wrong with it.

    Args:
        rows: The rows, with the mark columns.
        bounds: Each bounded key column, with where each row's value lies inside the bound
            (null where the value is null).

    Returns:
        The mask of the rows to keep.
    """
    marked = pc.is_valid(rows.column(FAULT))
    kept = (
        pc.or_(
            pc.fill_null(inside, fill_value=False),
            pc.and_(pc.is_null(rows.column(key)), marked),
        )
        for key, inside in bounds
    )
    return functools.reduce(pc.and_, kept)


def placed(rows: _Rows, keep: Callable[[_Rows], pa.Array]) -> _Rows:
    """Keep the rows a judgment of their keys keeps; a row whose key could not be read stays.

    Some windows are judged only after the time columns are filled, such as the whole-bar
    window (`row_times.whole`), and a row whose key could not be read cannot be filled. So
    the rows whose keys read, marked for another fault or not, are judged without their mark
    columns, and a row with an unreadable key stays, to be withheld and reported, as
    `in_window` keeps it.

    Args:
        rows: The rows, with the mark columns.
        keep: Given the rows whose keys read, without the mark columns, the mask of those to
            keep.

    Returns:
        The rows kept, in order, with their mark columns.
    """
    if not rows.num_rows:
        return rows
    faults = rows.column(FAULT)
    unplaced = _one_array(
        pc.fill_null(pc.equal(faults, RowFault.UNREADABLE_KEYS.value), fill_value=False)
    )
    keyed = rows.filter(pc.invert(unplaced))
    verdict = _one_array(keep(keyed.drop_columns(list(MARK_COLUMNS))))
    return rows.filter(pc.replace_with_mask(unplaced, pc.invert(unplaced), verdict))


def _one_array(values: pa.Array | pa.ChunkedArray) -> pa.Array:
    """Return a table column's chunks as one array, and a batch's column as it is."""
    if isinstance(values, pa.ChunkedArray):
        return values.combine_chunks()
    return values


@dataclass(frozen=True, slots=True)
class WithheldGroup:
    """The withheld rows of one payload that share a fault, a symbol, and a field.

    Attributes:
        fault: Their fault.
        symbol: Their canonical symbol, or None when it could not be read or they have none.
        sent: Their symbol as Massive sent it, or None.
        field: The column that could not be read.
        rows: How many.
        reason: Why the first of them could not be read.
    """

    fault: RowFault
    symbol: str | None
    sent: str | None
    field: str
    rows: int
    reason: str


@dataclass(frozen=True, slots=True)
class Withheld:
    """One payload's rows split into those to publish and those withheld (`withhold_rows`).

    Attributes:
        rows: The rows to publish, every row read whole, in order, without the mark columns.
        groups: The withheld rows by fault, canonical symbol, symbol as sent, and field, in the
            order of their first row; empty when every row was read whole.
        first_reason: Why the first withheld row was withheld; `""` when none was.
    """

    rows: _Rows
    groups: tuple[WithheldGroup, ...] = ()
    first_reason: str = ""

    @property
    def total(self) -> int:
        """How many rows were withheld."""
        return sum(group.rows for group in self.groups)

    @property
    def drift(self) -> DriftReport:
        """The withheld rows as drift: their count and the first one's reason."""
        return DriftReport(dropped_rows=self.total, dropped_reason=self.first_reason)

    def counts(self, holds: Callable[[str], bool] | None = None) -> Mapping[RowFault, int]:
        """Count the withheld rows by fault: all of them, or those one live table would get.

        Args:
            holds: For a live table that receives some symbols only, whether it holds a
                canonical symbol (`StreamSink.holds`); None for every row. A row whose symbol
                could not be read could have been any table's, so it counts for every one.

        Returns:
            Each fault with withheld rows, and their count.
        """
        found: dict[RowFault, int] = {}
        for group in self._groups_for(holds):
            found[group.fault] = found.get(group.fault, 0) + group.rows
        return found

    def by_symbol(self) -> Mapping[str, int]:
        """Count the withheld rows of each canonical symbol, for each symbol's run.

        Returns:
            Each canonical symbol with withheld rows, and their count; a row whose symbol
            could not be read is left out.
        """
        found: dict[str, int] = {}
        for group in self.groups:
            if group.symbol is not None:
                found[group.symbol] = found.get(group.symbol, 0) + group.rows
        return found

    def events(
        self,
        *,
        dataset: str,
        transport: Transport,
        where: str,
        stream_id: int | None,
        policy: ResolvedPolicy,
        holds: Callable[[str], bool] | None = None,
    ) -> tuple[ProblemEvent, ...]:
        """Return the payload's events: one per fault, symbol as sent, and field.

        At most `EVENTS_PER_PAYLOAD` such events; the rows of a fault past that are one
        `ProblemAction.OVERFLOW` event, with no symbol or field.

        Args:
            dataset: The dataset.
            transport: The transport.
            where: The request, the WebSocket URL and channel, or the file.
            stream_id: The table or read the events are for (`ProblemEvent.stream_id`).
            policy: The table's or read's policy, whose setting of each fault's cause the
                event records.
            holds: For a live table that receives some symbols only, whether it holds a
                canonical symbol; None for every row (`counts`).

        Returns:
            The events, in the order of their first row.
        """
        at = time.time_ns()

        def event(
            group: WithheldGroup | None, fault: RowFault, *, rows: int, reason: str
        ) -> ProblemEvent:
            return ProblemEvent(
                cause=ProblemCause(fault.value),
                action=ProblemAction.OVERFLOW if group is None else ProblemAction.WITHHELD,
                dataset=dataset,
                stream_id=stream_id,
                where=f"{transport.value} {where}",
                symbol=None if group is None else group.sent,
                field=None if group is None else group.field,
                rows=rows,
                detail=reason,
                at=at,
                policy=setting_of(policy, fault),
            )

        groups = self._groups_for(holds)
        shown = [
            event(g, g.fault, rows=g.rows, reason=g.reason) for g in groups[:EVENTS_PER_PAYLOAD]
        ]
        rest: dict[RowFault, tuple[int, str]] = {}
        for group in groups[EVENTS_PER_PAYLOAD:]:
            count, reason = rest.get(group.fault, (0, group.reason))
            rest[group.fault] = (count + group.rows, reason)
        shown.extend(
            event(None, fault, rows=count, reason=why) for fault, (count, why) in rest.items()
        )
        return tuple(shown)

    def _groups_for(self, holds: Callable[[str], bool] | None) -> list[WithheldGroup]:
        """Return the groups one live table would have received.

        Args:
            holds: Whether the table holds a canonical symbol, or None for every group.

        Returns:
            The groups of its symbols, and those whose symbol could not be read.
        """
        return [
            group
            for group in self.groups
            if holds is None or group.symbol is None or holds(group.symbol)
        ]


def withhold_rows(rows: _Rows, *, symbol: str | None) -> Withheld:
    """Take a payload's marked rows out, and group them for counting and reporting.

    Call it once per payload, after the last step that can mark a row and before any that only
    computes (the time columns, the whole-bar test). The grouping runs in Arrow on the marked
    rows only, so a payload whose every row was read whole costs one null count.

    Args:
        rows: The payload's rows, with the mark columns.
        symbol: The rows' canonical symbol column, or None for a dataset whose rows name none.

    Returns:
        The rows to publish, and the withheld rows grouped.
    """
    names = [name for name in rows.schema.names if name not in MARK_COLUMNS]
    if not marked_count(rows):
        return Withheld(rows.select(names))
    marked = pc.is_valid(rows.column(FAULT))
    kept = rows.filter(pc.invert(marked)).select(names)
    taken = rows.filter(marked)
    count = taken.num_rows
    symbols = pa.nulls(count, _TEXT) if symbol is None else pc.cast(taken.column(symbol), _TEXT)
    table = pa.table(
        {
            "fault": taken.column(FAULT),
            "symbol": symbols,
            "sent": taken.column(SENT),
            "field": taken.column(FIELD),
            "reason": taken.column(REASON),
        }
    )
    keys = ["fault", "symbol", "sent", "field"]
    grouped = table.group_by(keys, use_threads=False).aggregate(
        [("reason", "first"), ([], "count_all")]
    )
    aggregated = (*keys, "reason_first", "count_all")
    columns = (grouped.column(name).to_pylist() for name in aggregated)
    groups = tuple(
        WithheldGroup(RowFault(fault), found, sent, field, rows, reason)
        for fault, found, sent, field, reason, rows in zip(*columns, strict=True)
    )
    first: str = table.column("reason")[0].as_py()
    return Withheld(kept, groups, first)


_SETTINGS: Final[Mapping[RowFault, Callable[[ResolvedPolicy], OnError]]] = MappingProxyType(
    {
        RowFault.UNREADABLE_KEYS: lambda policy: policy.unreadable_keys,
        RowFault.UNREADABLE_SYMBOLS: lambda policy: policy.unreadable_symbols,
        RowFault.UNCONVERTIBLE_VALUES: lambda policy: policy.unconvertible_values,
    }
)
"""Each fault's `dm.ErrorPolicy` field, read from a resolved policy."""


def setting_of(policy: ResolvedPolicy, fault: RowFault) -> OnError:
    """Return a policy's setting of the cause a fault follows.

    Args:
        policy: The read's or the stream's policy.
        fault: The fault.

    Returns:
        The setting of the `dm.ErrorPolicy` field the fault is named for.
    """
    return _SETTINGS[fault](policy)


WITHHELD_LISTED: Final = "m.problems() counts the withheld rows by cause and symbol."
"""What ends the message of a read or stream that failed because it withheld every row: where
the user finds the rows, since no setting would publish them."""


def reason_sentence(reason: str) -> str:
    """Return why a row was withheld as a sentence of a message.

    Args:
        reason: A withheld row's reason, such as `Withheld.first_reason`.

    Returns:
        The reason with a capital first letter and one final period.
    """
    text = reason.rstrip(".")
    return f"{text[:1].upper()}{text[1:]}."


def failure_of(
    counts: Mapping[RowFault, int],
    policy: ResolvedPolicy,
    *,
    dataset: str,
    first_reason: str,
    where: str | None = None,
) -> str | None:
    """Say why withheld rows fail a read, when a cause they follow is `RAISE`.

    The rows are Massive's data the library cannot read, so the message names the setting
    that goes on without them and asks for no bug report.

    Args:
        counts: The withheld rows by fault (`Withheld.counts`).
        policy: The read's or the stream's policy.
        dataset: The dataset, for the message.
        first_reason: Why the payload's first withheld row was withheld.
        where: Where the rows came from, such as a flat file's location, when the message
            needs to say; None when the dataset says enough.

    Returns:
        The failure's message, naming the dataset, the count of rows under the failing causes
        and each cause's count, where the rows came from, the first reason, and the setting
        of every failing cause that would go on without the rows; None when no cause with
        withheld rows is `RAISE`. For example: `"stocks.trades: Massive sent 12 rows the
        library cannot read whole, so it withheld them: 9 under unreadable_keys and 3 under
        unconvertible_values, from s3://flatfiles/us_stocks_sip/trades_v1/2025/06/2025-06-06.csv.gz.
        The required column sip_timestamp is absent or null. To go on without such rows, pass
        on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG,
        unconvertible_values=dm.OnError.LOG) to the call or to dm.MassiveConfig."`.
    """
    failing = [
        (fault, count)
        for fault, count in counts.items()
        if effect_of(setting_of(policy, fault)).fails
    ]
    if not failing:
        return None
    total = sum(count for _, count in failing)
    rows, them = counted(total, "row"), "it" if total == 1 else "them"
    listed = prose_list((f"{count} under {fault.value}" for fault, count in failing), "causes")
    came = "" if where is None else f", from {where}"
    hint = policy_hint(
        OnError.LOG, *(fault.value for fault, _ in failing), purpose="To go on without such rows"
    )
    return (
        f"{dataset}: Massive sent {rows} the library cannot read whole, so it withheld {them}: "
        f"{listed}{came}. {reason_sentence(first_reason)} {hint}"
    )


def logs_withheld(counts: Mapping[RowFault, int], policy: ResolvedPolicy) -> bool:
    """Tell whether withheld rows are logged above DEBUG: some cause they follow is `LOG`.

    Args:
        counts: The withheld rows by fault (`Withheld.counts`).
        policy: The read's or the stream's policy.

    Returns:
        True when a fault with withheld rows is set to log at WARNING; False when each is
        `IGNORE`, whose rows the log names only at DEBUG.
    """
    return any(effect_of(setting_of(policy, fault)).level > logging.DEBUG for fault in counts)


def merged(parts: Sequence[Withheld]) -> Withheld:
    """Return several payloads' withheld rows as one, such as a flat file's blocks.

    Args:
        parts: Each payload's withheld rows, in order.

    Returns:
        The groups of every part, those of one fault, symbol, and field added together, in
        the order of their first row, with the first part's first reason; no rows.
    """
    found: dict[tuple[RowFault, str | None, str | None, str], WithheldGroup] = {}
    for part in parts:
        for group in part.groups:
            key = (group.fault, group.symbol, group.sent, group.field)
            seen = found.get(key)
            found[key] = group if seen is None else replace(seen, rows=seen.rows + group.rows)
    first = next((part.first_reason for part in parts if part.first_reason), "")
    return Withheld(pa.table({}), tuple(found.values()), first)
