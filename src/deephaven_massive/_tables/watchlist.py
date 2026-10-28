"""Reactive symbol tables: the canonical view every consumer reads, and the watch that follows it.

A user may pass a Deephaven `Table` with a `Symbol` column as a dataset's symbols, and edit it
while results are live. Its rows may use any accepted spelling (`EUR/USD`, `BTC-USD`, `SPX`),
while the rows a stream publishes always carry the canonical form. Two pieces turn such a table
into what the library needs:

- `canonical_view` puts the table's `Symbol` column in canonical form once, through the per-row
  canonicalizer of `_datasets/symbols.py`, so every consumer agrees on what a row of the
  watchlist means. `last_by` filters a stream's rows against it with `where_in`.
- `SymbolWatch` listens to that view and reports each change of the *set* of symbols it holds
  (the symbols added and the symbols removed, canonical), so a WebSocket stream subscribes and
  unsubscribes exactly the topics the watchlist names. A symbol listed in two rows counts once;
  it is removed only when its last row goes.

This is table code: it creates a refreshing view and a listener, so it lives on the Deephaven
side, while the symbol rules themselves stay in `_datasets/symbols.py`, which makes no Deephaven
call. The listener runs on an update-graph thread with the update-graph lock held, so it only
computes the change and hands it on; it never blocks, never waits on a library thread, and
makes no table operation.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, Final

from deephaven.table_listener import TableListener, TableListenerHandle

from deephaven_massive._datasets.symbols import symbol_canonicalizer
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._enums import AssetClass

__all__ = ["SymbolWatch", "canonical_view"]

logger = logging.getLogger(__name__)

_SYMBOL: Final = "Symbol"


def canonical_view(asset_class: AssetClass | None, table: Table) -> Table:
    """Return a view of a reactive symbol table whose `Symbol` column is canonical.

    Every consumer of a reactive symbol table reads it through this view, so the canonical
    form is decided in one place: `last_by` filters the stream's rows (which carry canonical
    symbols) with `where_in` against it, and `SymbolWatch` subscribes the symbols it holds. A
    row that is not a valid symbol of the class becomes null, so it matches nothing, and is
    logged once per class at WARNING. Must be called inside the liveness scope that should own
    the view, on a thread with the session's execution context. Deephaven resolves the
    formula's function from this call's local variables.

    Args:
        asset_class: The class whose symbol rules apply, or None to select it by each symbol's
            prefix.
        table: The user's table with a `Symbol` column.

    Returns:
        A refreshing view with the same rows and a canonical (or null) `Symbol` column. The view
        is created in the caller's current liveness scope.
    """
    canonical_symbol = symbol_canonicalizer(asset_class)
    return table.update_view([f"Symbol = {canonical_symbol.__name__}(Symbol)"])


class SymbolWatch:
    """The live set of canonical symbols a reactive symbol table holds, reported as it changes.

    Created by the WebSocket transport when a stream's symbols are a `Table`, on the caller
    thread, inside the stream's liveness scope, which then manages the canonical view (without
    its null rows) and the listener. The watch starts reporting only when `start()` is called,
    so its owner can first make ready whatever receives the changes. `start()` replays the
    table's current rows as one change of added symbols; every later change of the table is
    reported from the update-graph thread that delivers it.

    `on_change(added, removed)` receives two disjoint frozensets of canonical symbols: those the
    table now holds and did not before, and those it no longer holds. It is never called with
    both empty. Rows whose symbol is not valid for the class (null in the view) are ignored. The
    callback runs on the replaying caller thread or an update-graph thread, with the
    update-graph lock held: it must not block, wait on a library thread, or make a table
    operation, and an exception it raises is logged at ERROR by the listener's error callback.

    `keep_alive_with(table)` ties the view and the listener to another table's liveness, so
    they live exactly as long as that table: the transport passes the stream's blink table,
    so a stream that a derived table keeps alive keeps following its watchlist after the
    library has released its own results.

    Thread-safety: the symbol counts are touched only by the listener, whose calls Deephaven
    serializes, so they need no lock; `start` and `keep_alive_with` run once, on the caller
    thread.

    Example:
        ::

            from deephaven import input_table
            from deephaven import dtypes as dht

            from deephaven_massive._enums import AssetClass
            from deephaven_massive._tables.watchlist import SymbolWatch

            watchlist = input_table({"Symbol": dht.string})
            watch = SymbolWatch(AssetClass.CRYPTO, watchlist, lambda added, removed: None)
            watch.start()  # reports the current rows, then every change
    """

    __slots__ = ("_handle", "_view")

    def __init__(
        self,
        asset_class: AssetClass | None,
        table: Table,
        on_change: Callable[[frozenset[str], frozenset[str]], None],
    ) -> None:
        """Create the canonical view and the listener, without starting it.

        Must run on a thread with the session's execution context, inside the liveness scope
        that should own the view and the listener, and outside every library lock.

        Args:
            asset_class: The class whose symbol rules apply, or None to select it by each
                symbol's prefix.
            table: The user's refreshing table with a `Symbol` column.
            on_change: Receives each change of the set of symbols; see the class documentation.
        """
        # Without its null rows (symbols not valid for the class): `TableUpdate` hands a String
        # column over as NumPy text, in which a null reads as the text "None".
        self._view = canonical_view(asset_class, table).where(f"!isNull({_SYMBOL})")
        self._handle = TableListenerHandle(
            self._view, _SymbolCounter(on_change), description="deephaven_massive symbol watch"
        )

    def start(self) -> None:
        """Report the table's current symbols, then every later change.

        Runs on the caller thread with the session's execution context, outside every library
        lock: it takes the update-graph lock while it replays the table. `on_change` is called
        once for the replay when the table holds a valid symbol.
        """
        self._handle.start(do_replay=True)

    def keep_alive_with(self, table: Table) -> None:
        """Keep the view and the listener alive for exactly as long as `table` lives.

        `table` manages both, so neither is destroyed while `table` is alive, whatever scope
        created them, and both are released when `table` is.

        Args:
            table: The table whose liveness the watch should share, such as a stream's blink
                table.
        """
        table.j_table.manage(self._handle.j_object)
        table.j_table.manage(self._view.j_table)


class _SymbolCounter(TableListener):
    """The listener of a `SymbolWatch`: counts the rows of each symbol and reports set changes.

    Deephaven serializes its calls (the replay on the caller thread happens before any update
    is delivered), so its counts need no lock.
    """

    def __init__(self, on_change: Callable[[frozenset[str], frozenset[str]], None]) -> None:
        """Start with no symbol counted.

        Args:
            on_change: Receives each change of the set of symbols.
        """
        self._counts: Counter[str] = Counter()
        self._on_change = on_change

    @override
    def on_update(self, update: TableUpdate, is_replay: object) -> None:
        """Turn one update of the view into a change of the symbol set.

        A replay of the initial rows is the same change as rows being added; it is only
        logged at DEBUG with the number of symbols found.

        Args:
            update: The update; its added, removed, and modified rows' `Symbol` values.
            is_replay: True for the replay of the initial rows (Deephaven passes a `bool`).
        """
        before = set(self._counts)
        self._count(update.added(cols=[_SYMBOL]), 1)
        self._count(update.modified(cols=[_SYMBOL]), 1)
        self._count(update.modified_prev(cols=[_SYMBOL]), -1)
        self._count(update.removed(cols=[_SYMBOL]), -1)
        after = set(self._counts)
        added, removed = frozenset(after - before), frozenset(before - after)
        if is_replay:
            logger.debug("a symbol table holds %d valid symbols", len(after))
        if added or removed:
            self._on_change(added, removed)

    @override
    def on_error(self, e: Exception) -> None:
        """Log a failure of the listener at ERROR; the watch stops receiving updates.

        Args:
            e: What Deephaven reported.
        """
        logger.error("a symbol table's watch failed and stops following it: %s", e)

    def _count(self, columns: Mapping[str, Iterable[object]], step: int) -> None:
        """Add `step` to the count of each valid symbol, forgetting counts that reach zero.

        Args:
            columns: The `Symbol` values of some rows, as `TableUpdate` returns them: NumPy
                text, never null (the watched view has no null row).
            step: 1 for rows that now exist, -1 for rows that no longer do.
        """
        for value in columns.get(_SYMBOL, ()):
            symbol = str(value)  # a plain str, not NumPy's subclass, leaves the listener
            count = self._counts[symbol] + step
            if count > 0:
                self._counts[symbol] = count
            else:
                del self._counts[symbol]
