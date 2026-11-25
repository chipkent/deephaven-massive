"""The values `canonicalize_symbols` refuses in a table, kept for the tables that read it.

`dm.canonicalize_symbols` gives a row it cannot convert a null symbol, which every dataset
skips like any null. So that such a row still shows in `m.problems()`, the function keeps, for
each table it returns, a `RefusedValues`: the values of the input table it refuses now. For a
live input, a watch on the input table keeps them current (`watchlist.SymbolWatch`, with a
`ConversionRule` that accepts what the converter converts); a static input's are found once,
since its rows never change. A live table that follows the returned table looks
it up (`refused_values_of`) and joins it to its own skipped values (`watchlist.SkippedValues`),
so each refused value is a `noncanonical_symbols` span of that table while some row holds it,
and the span ends once every such row is fixed or deleted. A table follows each converted table
it reads: its symbol table, and a `poll()`'s table of values too, whose spans name its column.
A one-time read (a static verb's call) lists each value refused when it reads, once
(`RefusedValues.events`), as it lists its own skipped values.

Only the table `canonicalize_symbols` returns is known here, held weakly: a table derived from
it is not.

Thread-safety: a watch's listener changes the values on an update-graph thread, and a follower
joins from its caller's thread; one leaf lock guards the values and the followers. A follower
is called under that lock, so it sees every change in order; it must only compute and queue
(`SkippedValues.refused_changed`).
"""

from __future__ import annotations

import threading
import time
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._enums import OnError
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

    from deephaven_massive._tables.watchlist import SkippedValues, SymbolWatch

__all__ = [
    "ConversionRule",
    "RefusedValues",
    "refused_detail",
    "refused_values_of",
    "track_refused",
]

_CONVERTED: Final[weakref.WeakKeyDictionary[Table, tuple[str, RefusedValues]]] = (
    weakref.WeakKeyDictionary()
)
"""Each table `canonicalize_symbols` returned without failing, with the column it converted and
the values it refuses now; guarded by `_CONVERTED_LOCK`, a leaf lock."""

_CONVERTED_LOCK: Final = threading.Lock()


@dataclass(frozen=True, slots=True)
class ConversionRule:
    """The rule a `SymbolWatch` of a converted table's input follows: what converts is good.

    It has the members of `watchlist.SymbolRowRule` the watch reads. A value the converter
    refuses is "skipped" (`noncanonical` is `LOG`), so the watch reports the set of refused
    values as it changes; it never logs a value (`skip`), since `canonicalize_symbols` logs
    each one itself.

    Attributes:
        dataset: The name the watch's log records use, `canonicalize_symbols`.
        column: The input table's column of symbols.
        why: Each value's reason it does not convert, without the fix, or None for a value
            that does.
    """

    dataset: str
    column: str
    why: Callable[[str], str | None]
    noncanonical: OnError = OnError.LOG

    def accepts(self, symbol: str) -> bool:
        """Tell whether the converter converts a value.

        Args:
            symbol: The value, never null.

        Returns:
            True when it converts.
        """
        return self.why(symbol) is None

    def refusal(self, symbol: str) -> str:
        """Say why a value does not convert.

        Args:
            symbol: A value `accepts` refuses.

        Returns:
            The converter's reason.
        """
        return self.why(symbol) or ""

    def skip(self, symbol: str, *, stream_id: int | None) -> None:
        """Log nothing: `canonicalize_symbols` logs each refused value itself.

        Args:
            symbol: The value.
            stream_id: Unused.
        """
        del symbol, stream_id


class RefusedValues:
    """The values a converted table's input holds now that the converter refuses.

    Example:
        ::

            refused = RefusedValues(dm.OnError.LOG, why=reasons.get)
            refused.change(("^GSPC",))  # the values refused now, from the watch
            refused.follow(skipped)  # a follower gets them now and on every change
    """

    __slots__ = ("__weakref__", "_followers", "_lock", "_values", "_watch", "_why", "policy")

    def __init__(self, policy: OnError, *, why: Callable[[str], str | None]) -> None:
        """Start with no value refused.

        Args:
            policy: The `on_error` of `canonicalize_symbols`, which the spans record.
            why: Each value's reason it does not convert, without the fix: the `Detail` of
                its `noncanonical_symbols` problem.
        """
        self.policy: Final = policy
        self._why = why
        self._lock = threading.Lock()
        self._values: dict[str, str] = {}
        # Each follower, held weakly, with the `field` it follows the table as; a follower may
        # follow the same table twice, as its symbol table and as its table of values.
        self._followers: list[tuple[weakref.ref[SkippedValues], str | None]] = []
        self._watch: SymbolWatch | None = None

    def keep(self, watch: SymbolWatch) -> None:
        """Hold the watch that keeps the values current, for as long as the values live.

        Args:
            watch: The watch of the input table.
        """
        self._watch = watch

    def change(self, values: tuple[str, ...]) -> None:
        """Take the values refused now, and hand them to every follower.

        Args:
            values: The distinct values refused now; empty once none is.
        """
        with self._lock:
            self._values = {value: self._why(value) or "" for value in values}
            alive = []
            for ref, field in self._followers:
                follower = ref()
                if follower is not None:  # a follower that is gone is dropped
                    alive.append((ref, field))
                    follower.refused_changed(self._values, self.policy, field=field)
            self._followers = alive

    def follow(self, follower: SkippedValues, *, field: str | None = None) -> None:
        """Hand a follower the values refused now, then every change, while it lives.

        Args:
            follower: A table's skipped values, held weakly.
            field: The column of a `poll()`'s table of values this table is, which the spans
                name in `Field`; None when it is the follower's symbol table.
        """
        with self._lock:
            # A follower that is gone is dropped here too, so readers made and released while
            # the values never change don't pile up.
            self._followers = [(ref, f) for ref, f in self._followers if ref() is not None]
            self._followers.append((weakref.ref(follower), field))
            follower.refused_changed(self._values, self.policy, field=field)

    def events(
        self, *, dataset: str, stream_id: int, where: str, field: str | None
    ) -> list[ProblemEvent]:
        """Return the `noncanonical_symbols` problems of one read of the values refused now.

        A static verb reads its table once, so each refused value is one problem of the call,
        not a span, as `SymbolRowRule.skipped_events` lists its own skipped values.

        Args:
            dataset: The call's dataset.
            stream_id: The call's id, its `StreamId`.
            where: The table the values came from, such as `the symbol table of <query>`.
            field: None for the call's symbols; the column of a table of values.

        Returns:
            One `SKIPPED` event per value refused now, sorted, with the converter's reason and
            its `on_error`.
        """
        with self._lock:
            values = dict(self._values)
        at = time.time_ns()
        return [
            ProblemEvent(
                cause=ProblemCause.NONCANONICAL_SYMBOLS,
                action=ProblemAction.SKIPPED,
                dataset=dataset,
                stream_id=stream_id,
                where=where,
                symbol=value,
                field=field,
                rows=0,
                detail=refused_detail(why),
                at=at,
                policy=self.policy,
            )
            for value, why in sorted(values.items())
        ]


def refused_detail(why: str) -> str:
    """Return the `Detail` of a value `canonicalize_symbols` refused.

    Args:
        why: The converter's reason, without its fix.

    Returns:
        Such as `"canonicalize_symbols refused it: 'BTC' is too short to hold a base currency
        and a quote currency."`.
    """
    return f"canonicalize_symbols refused it: {why}"


def track_refused(table: Table, refused: RefusedValues, *, column: str) -> None:
    """Remember the values a table `canonicalize_symbols` returned refuses.

    Args:
        table: The returned table.
        refused: Its refused values.
        column: The column it converted.
    """
    with _CONVERTED_LOCK:
        _CONVERTED[table] = (column, refused)


def refused_values_of(table: Table, column: str) -> RefusedValues | None:
    """Return the refused values of a table `canonicalize_symbols` returned, read by a column.

    A dataset reads one column of the table; only when `canonicalize_symbols` converted that
    column are its refused values the dataset's skipped values.

    Args:
        table: A table a dataset reads.
        column: The column the dataset reads.

    Returns:
        Its refused values, or None for any other table or column.
    """
    with _CONVERTED_LOCK:
        found = _CONVERTED.get(table)
    if found is None or found[0] != column:
        return None
    return found[1]
