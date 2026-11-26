"""The spans of `m.problems()` that a read's retrying, refused, or failed symbols hold.

A read that judges each symbol on its own (a static `fetch()` or `history()` of symbols, a
followed one, a `poll()`) lists every symbol that stops getting rows as a span under its cause
(`symbol_retry.SPAN_CAUSES`): `symbol_read_retrying`, `refused_symbols`, which carries the
read's setting of that cause, or `symbol_read_failed`, so no symbol goes without rows silently.
`SymbolSpans` keeps one `SpanTracker` per cause for one table and turns the symbols that hold a
span now into the snapshots that changed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._tables.problem_events import ProblemCause, SpanTracker
from deephaven_massive._tables.symbol_retry import SPAN_CAUSES

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import OnError
    from deephaven_massive._tables.problem_events import ProblemSpans

__all__ = ["SymbolSpans"]


class SymbolSpans:
    """One table's spans of its symbols' failures, one `SpanTracker` per cause.

    Not thread-safe: the read's one thread (or lock) owns it, as it owns its trackers.

    Example:
        ::

            spans = SymbolSpans(
                dataset="stocks.trades", stream_id=7, where="call 7", refused_symbols=OnError.LOG
            )
            core.problems(spans.update({ProblemCause.REFUSED_SYMBOLS: {"XYZ": "Massive ..."}}))
    """

    __slots__ = ("_trackers",)

    def __init__(
        self, *, dataset: str, stream_id: int | None, where: str, refused_symbols: OnError
    ) -> None:
        """Start with no span.

        Args:
            dataset: The dataset, which the spans name.
            stream_id: The table's `StreamId`, or the call's id, which the spans carry; None
                for a read that has neither.
            where: Where the spans were found, such as `"stocks.trades poll() stream 7"`.
            refused_symbols: The read's `refused_symbols` setting, which decides how loudly a
                refused symbol's span is logged.
        """
        self._trackers = {
            cause: SpanTracker(
                cause,
                dataset=dataset,
                stream_id=stream_id,
                where=where,
                policy=refused_symbols if cause is ProblemCause.REFUSED_SYMBOLS else None,
            )
            for cause in SPAN_CAUSES
        }

    def update(
        self,
        held: Mapping[ProblemCause, Mapping[str, str]],
        *,
        field: str | None = None,
        at: int | None = None,
        since: int | None = None,
    ) -> tuple[ProblemSpans, ...]:
        """Take the symbols that hold a span now; return the snapshot of each cause that changed.

        Args:
            held: Each cause's symbols that hold its span now, each with its detail, such as
                its error's text; a cause left out holds none.
            field: The column of the value table for a read of values, which the spans name;
                None for a read of symbols.
            at: When, epoch nanoseconds; now when None.
            since: Where a span that holds anew starts, epoch nanoseconds; `at` when None.

        Returns:
            The changed snapshots, in the order of `SPAN_CAUSES`.

        Raises:
            BrokenGuaranteeError: `held` names a cause that is not a span cause, whose symbols
                would otherwise go unlisted.
        """
        unknown = held.keys() - self._trackers.keys()
        if unknown:
            raise BrokenGuaranteeError(f"no span of a symbol's failure has the cause {unknown}")
        changed = (
            tracker.update(
                {
                    name: (name, field, detail, 0)
                    # A cause left out holds no span: its tracker ends any it held.
                    for name, detail in held.get(cause, {}).items()
                },
                at=at,
                since=since,
            )
            for cause, tracker in self._trackers.items()
        )
        return tuple(spans for spans in changed if spans is not None)
