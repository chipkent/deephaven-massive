"""The rows of `m.problems()`: every problem with the feed, merged into one row per key.

The session's transports and tables hand their problems to the session as plain data
(`problem_events.py`), and the diagnostics writer (`diagnostics.py`) passes each batch to the
session's `ProblemLedger`, which merges them into rows and returns the rows that changed, for
the writer to add to the table. The table users see is the `last_by` of those rows on
`PROBLEM_KEYS`, so a changed row replaces its key's earlier one:

- An event adds to its key's row: `Rows` and `Occurrences` grow, and `LastSeen` moves.
- A span snapshot opens a row for each span it newly holds, updates each one it still holds,
  and ends (`EndedAt`) each one it no longer holds.
- Each `(StreamId, Cause)` keeps at most `KEYS_PER_CAUSE` keys. The problems past that add up
  in one row with no symbol, field, or span, whose `Action` is `OVERFLOW`.
- `Query` comes from the table's `m.streams()` row, which the writer also writes: a problem
  that arrives before its table's row is written again with the query once the row arrives.
  A table whose row is final (failed, closed, or a read whose rows landed) ends its open
  spans.
- Every text a row shows passes through the session's `Redactor`, so no secret reaches the
  table.

Only the writer thread calls a ledger, so it takes no lock. No row is ever deleted: a table
that ended keeps its problems, as `m.streams()` keeps its row.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, fields, replace
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent

if TYPE_CHECKING:
    from collections.abc import Iterable

    from deephaven_massive._enums import OnError
    from deephaven_massive._massive import Redactor
    from deephaven_massive._tables.problem_events import Problem, ProblemSpans, Span

__all__ = [
    "KEYS_PER_CAUSE",
    "PROBLEMS",
    "PROBLEM_KEYS",
    "ProblemKey",
    "ProblemLedger",
    "ProblemRow",
    "problem_values",
]

KEYS_PER_CAUSE: Final = 10_000
"""The most keys one table keeps of one cause; the rest add up in its `OVERFLOW` row."""

_S, _L, _T = ColumnType.STRING, ColumnType.LONG, ColumnType.INSTANT

PROBLEMS: Final = Schema(
    "problems",
    tuple(
        Column(name, column_type)
        for name, column_type in (
            ("StreamId", _L),
            ("Cause", _S),
            ("Symbol", _S),
            ("Field", _S),
            ("SpanStart", _T),
            ("FirstSeen", _T),
            ("LastSeen", _T),
            ("EndedAt", _T),
            ("Dataset", _S),
            ("Query", _S),
            ("Action", _S),
            ("Rows", _L),
            ("Occurrences", _L),
            ("Policy", _S),
            ("Source", _S),
            ("Detail", _S),
        )
    ),
)
"""The columns of `m.problems()`, keys first, as `last_by` orders them."""

PROBLEM_KEYS: Final = ("StreamId", "Cause", "Symbol", "Field", "SpanStart")
"""The key columns of `m.problems()`."""

ProblemKey: TypeAlias = "tuple[int | None, ProblemCause, str | None, str | None, int | None]"
"""A row's key, in `PROBLEM_KEYS` order."""
_Group: TypeAlias = "tuple[int | None, ProblemCause]"


@dataclass(frozen=True, kw_only=True, slots=True)
class ProblemRow:
    """One row of `m.problems()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        first_seen: When the key's first problem happened; a span's start.
        last_seen: When its latest did.
        span_start: When a span began, which names it; None for a problem that is no span.
        ended_at: When a span ended; None while it holds, and for a problem that is no span.
        dataset: The dataset, or None for a problem of the session.
        stream_id: The table's `StreamId`, or a `wait=True` read's call id; None for a
            problem of the session.
        query: The `repr` of the query that made the table, as `m.streams()` shows it.
        symbol: The symbol, as Massive sent it or the caller gave it, or None.
        field: The column, or None.
        cause: The cause.
        action: What the library did; `OVERFLOW` for the row of a cause's problems past the
            cap.
        rows: The rows the key counts: added up over its events, or a span's latest count.
        occurrences: How many events or spans the key merged.
        policy: The setting of the cause's `dm.ErrorPolicy` field, or None.
        detail: Why: the key's first problem's reason, or a span's latest.
    """

    first_seen: int
    last_seen: int
    span_start: int | None = None
    ended_at: int | None = None
    dataset: str | None
    stream_id: int | None
    query: str | None = None
    symbol: str | None
    field: str | None
    cause: ProblemCause
    action: ProblemAction
    rows: int
    occurrences: int = 1
    policy: OnError | None
    detail: str

    @property
    def key(self) -> ProblemKey:
        """The row's key, in `PROBLEM_KEYS` order."""
        return (self.stream_id, self.cause, self.symbol, self.field, self.span_start)


def problem_values(row: ProblemRow) -> dict[str, object]:
    """Return a row keyed by its column names, as `PROBLEMS` takes it.

    Args:
        row: The row.

    Returns:
        Each field under its column's name, and the cause's `Source`.
    """
    values: dict[str, object] = {
        "".join(word.capitalize() for word in f.name.split("_")): getattr(row, f.name)
        for f in fields(row)
    }
    values["Source"] = row.cause.traits.source
    return {name: values[name] for name in PROBLEMS.column_names}


class ProblemLedger:
    """The diagnostics writer's merge of problems into the rows of `m.problems()`.

    Example:
        ::

            ledger = ProblemLedger(Redactor("my-key"))
            for row in ledger.apply([event]):  # the rows to add to the table
                ...
    """

    __slots__ = (
        "_by_stream",
        "_counts",
        "_final",
        "_open",
        "_overflowed",
        "_queries",
        "_redactor",
        "_rows",
    )

    def __init__(self, redactor: Redactor) -> None:
        """Start with no problem.

        Args:
            redactor: The session's secrets, removed from every text a row shows.
        """
        self._redactor = redactor
        self._rows: dict[ProblemKey, ProblemRow] = {}
        # Each table's keys, to write its rows again once its query is known.
        self._by_stream: dict[int, list[ProblemKey]] = {}
        self._counts: Counter[_Group] = Counter()
        # The open spans of each (table, cause), by the name its snapshots give each.
        self._open: dict[_Group, dict[str, ProblemKey]] = {}
        self._queries: dict[int, str] = {}
        # The tables that ended, whose later snapshots open no span.
        self._final: set[int] = set()
        # Per (table, cause), the spans counted in its overflow row while they hold: each
        # name, with the start it was counted for.
        self._overflowed: dict[_Group, dict[str, int]] = {}

    def stream_seen(self, stream_id: int, query: str) -> list[ProblemRow]:
        """Learn a table's query from its `m.streams()` row; fill it into its earlier rows.

        Args:
            stream_id: The table's `StreamId`, or a `wait=True` read's call id.
            query: Its `Query`, as `m.streams()` shows it.

        Returns:
            The table's rows that had no query, now with it; empty once the query is known.
        """
        if stream_id in self._queries:
            return []
        self._queries[stream_id] = query = self._redactor.received(query)
        # Each of the table's rows was added while its query was unknown, so has none yet.
        changed = []
        for key in self._by_stream.get(stream_id, ()):
            self._rows[key] = row = replace(self._rows[key], query=query)
            changed.append(row)
        return changed

    def stream_ended(self, stream_id: int, at: int) -> list[ProblemRow]:
        """End each open span of a table that failed, closed, got its rows, or is gone.

        A snapshot that arrives for the table later opens no span: nothing would end it.

        Args:
            stream_id: The table's `StreamId`.
            at: When its row became final, or it left its registry, epoch nanoseconds.

        Returns:
            The spans that ended.
        """
        self._final.add(stream_id)
        ended: list[ProblemRow] = []
        for group in [g for g in self._open if g[0] == stream_id]:
            ended.extend(self._end(key, at) for key in self._open.pop(group).values())
        return ended

    def apply(self, problems: Iterable[Problem]) -> list[ProblemRow]:
        """Merge problems into the rows.

        Args:
            problems: Events and span snapshots, in the order they were queued.

        Returns:
            The rows that changed, one per key, each in its latest state.
        """
        changed: dict[ProblemKey, ProblemRow] = {}
        for problem in problems:
            if isinstance(problem, ProblemEvent):
                rows = [self._event(problem)]
            else:
                rows = self._snapshot(problem)
            for row in rows:
                changed[row.key] = row
        return list(changed.values())

    def _event(self, event: ProblemEvent) -> ProblemRow:
        """Add one event to its key's row, or past the cap to its cause's overflow row.

        Args:
            event: The event.

        Returns:
            The changed row.
        """
        symbol = self._clean(event.symbol)
        key: ProblemKey = (event.stream_id, event.cause, symbol, event.field, None)
        found = self._rows.get(key)
        action = event.action
        if found is None and not self._admits(key):
            key, action = self._overflow(key), ProblemAction.OVERFLOW
            found = self._rows.get(key)
        if found is None:
            row = self._new(
                key,
                first_seen=event.at,
                dataset=event.dataset,
                action=action,
                rows=event.rows,
                policy=event.policy,
                detail=event.detail,
            )
        else:
            overflow = action is ProblemAction.OVERFLOW
            row = replace(
                found,
                last_seen=max(found.last_seen, event.at),
                rows=found.rows + event.rows,
                occurrences=found.occurrences + 1,
                action=ProblemAction.OVERFLOW if overflow else found.action,
                policy=event.policy,
            )
            self._rows[key] = row
        return row

    def _snapshot(self, spans: ProblemSpans) -> list[ProblemRow]:
        """Replace one table's open spans of one cause with a snapshot's.

        Args:
            spans: The snapshot.

        Returns:
            The rows of the spans it opened, changed, or ended.
        """
        if spans.stream_id in self._final:
            return []
        group: _Group = (spans.stream_id, spans.cause)
        was = self._open.pop(group, {})
        counted = self._overflowed.pop(group, {})
        now: dict[str, ProblemKey] = {}
        overflowed: dict[str, int] = {}
        changed: list[ProblemRow] = []
        for name, span in spans.spans.items():
            if counted.get(name) == span.since:  # counted in the overflow row while it holds
                overflowed[name] = span.since
                continue
            key = was.pop(name, None)
            if key is not None and key[4] != span.since:  # a new span of the same name
                changed.append(self._end(key, spans.at))
                key = None
            if key is not None:
                row = replace(
                    self._rows[key],
                    last_seen=spans.at,
                    rows=span.rows,
                    policy=spans.policy if span.policy is None else span.policy,
                    detail=self._redactor.received(span.detail),
                )
                self._rows[key] = row
                now[name] = key
            else:
                row, opened = self._open_span(spans, span)
                if opened:
                    now[name] = row.key
                else:  # a span renewed from a new start counts again
                    overflowed[name] = span.since
            changed.append(row)
        changed.extend(self._end(key, spans.at) for key in was.values())
        if now:
            self._open[group] = now
        if overflowed:
            self._overflowed[group] = overflowed
        return changed

    def _open_span(self, spans: ProblemSpans, span: Span) -> tuple[ProblemRow, bool]:
        """Open a span's row, or past the cap count it in its cause's overflow row.

        Args:
            spans: The snapshot that holds it.
            span: The span.

        Returns:
            The changed row, and True when it is the span's own row.
        """
        symbol = self._clean(span.symbol)
        key: ProblemKey = (spans.stream_id, spans.cause, symbol, span.field, span.since)
        found = self._rows.get(key)
        if found is not None:  # it ended, and holds again from the same start
            row = replace(
                found,
                ended_at=None,
                last_seen=spans.at,
                rows=span.rows,
                policy=spans.policy if span.policy is None else span.policy,
                detail=self._redactor.received(span.detail),
            )
            self._rows[key] = row
            return row, True
        if self._admits(key):
            row = self._new(
                key,
                first_seen=span.since,
                dataset=spans.dataset,
                action=spans.cause.traits.action,
                rows=span.rows,
                policy=spans.policy if span.policy is None else span.policy,
                detail=span.detail,
                last_seen=spans.at,
            )
            return row, True
        key = self._overflow(key)
        found = self._rows.get(key)
        if found is None:
            row = self._new(
                key,
                first_seen=spans.at,
                dataset=spans.dataset,
                action=ProblemAction.OVERFLOW,
                rows=span.rows,
                policy=spans.policy if span.policy is None else span.policy,
                detail=span.detail,
            )
        else:
            row = replace(
                found,
                last_seen=spans.at,
                rows=found.rows + span.rows,
                occurrences=found.occurrences + 1,
            )
            self._rows[key] = row
        return row, False

    def _end(self, key: ProblemKey, at: int) -> ProblemRow:
        """End a span.

        Args:
            key: The span's key.
            at: When it ended, epoch nanoseconds.

        Returns:
            Its row, ended.
        """
        row = replace(self._rows[key], ended_at=at, last_seen=at)
        self._rows[key] = row
        return row

    def _admits(self, key: ProblemKey) -> bool:
        """Tell whether a new key fits under its table's and cause's cap.

        Args:
            key: The new key.

        Returns:
            True while the table has fewer than `KEYS_PER_CAUSE` keys of the cause.
        """
        return self._counts[key[0], key[1]] < KEYS_PER_CAUSE

    def _clean(self, text: str | None) -> str | None:
        """Return a symbol with the session's secrets removed.

        Args:
            text: The symbol as sent or given, or None.

        Returns:
            The symbol, each secret replaced; None for None.
        """
        return None if text is None else self._redactor.received(text)

    @staticmethod
    def _overflow(key: ProblemKey) -> ProblemKey:
        """Return the key of the overflow row of a key's table and cause.

        Args:
            key: A key past the cap.

        Returns:
            The key with no symbol, field, or span.
        """
        return (key[0], key[1], None, None, None)

    def _new(
        self,
        key: ProblemKey,
        *,
        first_seen: int,
        dataset: str | None,
        action: ProblemAction,
        rows: int,
        policy: OnError | None,
        detail: str,
        last_seen: int | None = None,
    ) -> ProblemRow:
        """Add and count a new key's row.

        Args:
            key: The key.
            first_seen: Its first problem's time, or its span's start.
            dataset: The dataset.
            action: What the library did.
            rows: The rows it counts.
            policy: The cause's setting.
            detail: Why, before redaction.
            last_seen: Its latest problem's time; `first_seen` when None.

        Returns:
            The row.
        """
        stream_id, cause, symbol, field, span_start = key
        query = None if stream_id is None else self._queries.get(stream_id)
        row = ProblemRow(
            first_seen=first_seen,
            last_seen=first_seen if last_seen is None else last_seen,
            span_start=span_start,
            dataset=dataset,
            stream_id=stream_id,
            query=query,
            symbol=symbol,
            field=field,
            cause=cause,
            action=action,
            rows=rows,
            policy=policy,
            detail=self._redactor.received(detail),
        )
        self._rows[key] = row
        self._counts[stream_id, cause] += 1
        if stream_id is not None:
            self._by_stream.setdefault(stream_id, []).append(key)
        return row
