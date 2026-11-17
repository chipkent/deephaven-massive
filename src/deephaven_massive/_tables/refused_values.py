"""The values `canonicalize_symbols` refuses in a live table, kept for the tables that follow it.

`dm.canonicalize_symbols` gives a row it cannot convert a null symbol, which every dataset
skips like any null. So that such a row still shows in `m.problems()`, the function keeps, for
each live table it returns, a `RefusedValues`: the values of the input table it refuses now,
kept current by a watch on the input table (`watchlist.SymbolWatch`, with a `ConversionRule`
that accepts what the converter converts). A live table that follows the returned table looks
it up (`refused_values_of`) and joins it to its own skipped values (`watchlist.SkippedValues`),
so each refused value is a `noncanonical_symbols` span of that table while some row holds it,
and the span ends once every such row is fixed or deleted.

Only the table `canonicalize_symbols` returns is known here, held weakly: a table derived from
it is not, and a static input has no watch, since its rows never change.

Thread-safety: a watch's listener changes the values on an update-graph thread, and a follower
joins from its caller's thread; one leaf lock guards the values and the followers. A follower
is called under that lock, so it sees every change in order; it must only compute and queue
(`SkippedValues.refused_changed`).
"""

from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._enums import OnError

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

    from deephaven_massive._tables.watchlist import SkippedValues, SymbolWatch

__all__ = ["ConversionRule", "RefusedValues", "refused_values_of", "track_refused"]

_CONVERTED: Final[weakref.WeakKeyDictionary[Table, RefusedValues]] = weakref.WeakKeyDictionary()
"""Each live table `canonicalize_symbols` returned, with the values it refuses now; guarded by
`_CONVERTED_LOCK`, a leaf lock."""

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
        why: Each value's reason it does not convert, or None for a value that does.
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
            why: Each value's reason it does not convert.
        """
        self.policy: Final = policy
        self._why = why
        self._lock = threading.Lock()
        self._values: dict[str, str] = {}
        self._followers: weakref.WeakSet[SkippedValues] = weakref.WeakSet()
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
            for follower in list(self._followers):
                follower.refused_changed(self._values, self.policy)

    def follow(self, follower: SkippedValues) -> None:
        """Hand a follower the values refused now, then every change, while it lives.

        Args:
            follower: A table's skipped values, held weakly.
        """
        with self._lock:
            self._followers.add(follower)
            follower.refused_changed(self._values, self.policy)


def track_refused(table: Table, refused: RefusedValues) -> None:
    """Remember the values a live table `canonicalize_symbols` returned refuses.

    Args:
        table: The returned table.
        refused: Its refused values.
    """
    with _CONVERTED_LOCK:
        _CONVERTED[table] = refused


def refused_values_of(table: Table) -> RefusedValues | None:
    """Return the refused values of a live table `canonicalize_symbols` returned.

    Args:
        table: A table a dataset follows.

    Returns:
        Its refused values, or None for any other table.
    """
    with _CONVERTED_LOCK:
        return _CONVERTED.get(table)
