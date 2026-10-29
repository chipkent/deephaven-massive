"""Symbol tables: the check of their rows, and the watch that follows them.

A user may pass a Deephaven `Table` with a `Symbol` column as a dataset's symbols, and edit it
while results are live. Every row must hold Massive's exact ticker, as `symbols=` must
(`check_symbol` in `_datasets/symbols.py`): the library never rewrites a watchlist, so what the
table says is what the stream subscribes and what `last_by()` matches. A null row is ignored.
Two pieces enforce that rule:

- `read_symbols` reads a table's current rows once, on the caller thread, and raises
  `ValueError` for a row that is not canonical, so a verb fails before it starts. The static
  verbs read their symbols with it; the live verbs check the table with it first.
- `SymbolWatch` listens to the table and reports each change of the *set* of symbols it holds,
  so a WebSocket stream subscribes and unsubscribes exactly the topics the watchlist names. A
  row that arrives later and is not canonical is not subscribed: the watch reports it through
  `on_invalid`, and the stream fails with a message that names `canonicalize_symbols`.

A table in other spellings is converted by `canonicalize_symbols` in `_tables/symbols.py`, the
table form of the symbol converters.

This is table code: it reads tables and creates listeners, so it lives on the
Deephaven side, while the symbol rules themselves stay in `_datasets/symbols.py`, which makes
no Deephaven call. The listener runs on an update-graph thread with the update-graph lock held,
so it only computes the change and hands it on; it never blocks, never waits on a library
thread, and makes no table operation.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, Final

from deephaven.table_listener import TableListener, TableListenerHandle

from deephaven_massive._datasets.symbols import (
    check_symbol,
    is_canonical,
)
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._enums import AssetClass

__all__ = ["SymbolWatch", "invalid_row_message", "read_symbols"]

logger = logging.getLogger(__name__)

_SYMBOL: Final = "Symbol"
_SHOWN: Final = 5
"""The most bad values one message lists."""


def invalid_row_message(
    asset_class: AssetClass | None, symbol: str, *, as_written: bool = False
) -> str:
    """Say why a symbol table's row is refused, and how to convert such a table.

    Args:
        asset_class: The dataset's class, or None for a dataset that spans classes.
        symbol: The row's symbol, which `check_symbol` refuses.
        as_written: True for a dataset whose symbols are kept as Massive writes them.

    Returns:
        The reason (with the canonical form when the class's converter gives one) and the
        remedy: fix the row, or convert the table. For a dataset of one class the remedy is
        `dm.canonicalize_symbols` with `on_error=dm.OnError.NULL`; for a dataset that spans
        classes, whose rows may mix them, it is `dm.canonical_symbol` per row with each row's
        class.
    """
    reason = ""
    try:
        check_symbol(asset_class, symbol, as_written=as_written)
    except ValueError as error:
        reason = f" ({error})"
    if asset_class is None:
        remedy = (
            "convert each row with dm.canonical_symbol(text, asset_class) and the row's own "
            "class, since this dataset's rows may mix classes"
        )
    else:
        remedy = (
            f"pass the table through dm.canonicalize_symbols(table, "
            f"dm.AssetClass.{asset_class.name}, on_error=dm.OnError.NULL), which converts what "
            "it can and nulls the rest"
        )
    return f"a symbol table holds {symbol!r}{reason}. Fix the row, or {remedy}"


_FAILED_TABLE: Final = (
    "the symbol table failed, so the result can no longer follow it: {cause}. A table made by "
    "dm.canonicalize_symbols fails on a row it cannot convert unless "
    "on_error=dm.OnError.NULL"
)
"""The message of a result whose symbol table failed upstream, formatted with the cause."""


def read_symbols(spec: DatasetSpec, table: Table) -> tuple[str, ...]:
    """Read a symbol table's current rows once, refusing any that is not canonical.

    The table is read with `Table.iter_tuple`, which holds the update graph's shared lock while
    it reads, so the rows are one consistent snapshot. Runs on the caller thread, or a poller's,
    inside the session's execution context, with no library lock held. Null rows are ignored.

    Args:
        spec: The dataset, whose class and `symbols_as_written` decide the grammar.
        table: The user's table with a `Symbol` column.

    Returns:
        The table's symbols, sorted and de-duplicated; empty for a table of no or only null
        rows.

    Raises:
        ValueError: If the table has failed (a `canonicalize_symbols` table under
            `OnError.RAISE` that met a bad row), or a row is not Massive's exact ticker of the
            class (`invalid_row_message`, for the first such row in sorted order).
    """
    if table.j_table.isFailed():
        raise ValueError(_FAILED_TABLE.format(cause="it is failed"))
    found = sorted({row[0] for row in table.iter_tuple(cols=_SYMBOL) if row[0] is not None})
    written = spec.symbols_as_written
    for symbol in found:
        if not is_canonical(spec.asset_class, symbol, as_written=written):
            raise ValueError(invalid_row_message(spec.asset_class, symbol, as_written=written))
    return tuple(found)


class SymbolWatch:
    """The live set of symbols a reactive symbol table holds, reported as it changes.

    Created by the WebSocket transport when a stream's symbols are a `Table`, on the caller
    thread, inside the stream's liveness scope, which then manages the watched view (the
    table's non-null `Symbol` values) and the listener. The watch starts reporting only when
    `start()` is called, so its owner can first make ready whatever receives the changes.
    `start()` replays the table's current rows as one change of added symbols; every later
    change of the table is reported from the update-graph thread that delivers it.

    `on_change(added, removed)` receives two disjoint frozensets of symbols: those the table now
    holds and did not before, and those it no longer holds. It is never called with both empty. A
    row whose symbol is not Massive's exact ticker of the class is never counted (the transport
    looks up a crypto pair's split before it subscribes): `on_invalid(message)` receives the reason
    each time such a row arrives, and its owner fails the stream. When the watched table itself
    fails (a `canonicalize_symbols` table under `OnError.RAISE` that met a bad row), `on_invalid`
    receives that too, so the stream never follows a frozen table. Both callbacks run on the
    replaying caller thread or an update-graph thread, with the update-graph lock held: they must
    not block, wait on a library thread, or make a table operation, and an exception one raises is
    logged at ERROR by the listener's error callback.

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
            watch = SymbolWatch(
                AssetClass.CRYPTO,
                watchlist,
                lambda added, removed: None,
                on_invalid=lambda message: None,
            )
            watch.start()  # reports the current rows, then every change
    """

    __slots__ = ("_handle", "_view")

    def __init__(
        self,
        asset_class: AssetClass | None,
        table: Table,
        on_change: Callable[[frozenset[str], frozenset[str]], None],
        *,
        on_invalid: Callable[[str], None],
    ) -> None:
        """Create the watched view and the listener, without starting it.

        Must run on a thread with the session's execution context, inside the liveness scope
        that should own the view and the listener, and outside every library lock.

        Args:
            asset_class: The class whose grammar the rows must follow, or None for a dataset
                that spans classes (each symbol's prefix selects its class).
            table: The user's refreshing table with a `Symbol` column.
            on_change: Receives each change of the set of symbols; see the class documentation.
            on_invalid: Receives the message of a row that is not canonical.
        """
        # Without its null rows: `TableUpdate` hands a String column over as NumPy text, in
        # which a null reads as the text "None".
        self._view = table.view([_SYMBOL]).where(f"!isNull({_SYMBOL})")
        self._handle = TableListenerHandle(
            self._view,
            _SymbolCounter(asset_class, on_change, on_invalid),
            description="deephaven_massive symbol watch",
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

    def __init__(
        self,
        asset_class: AssetClass | None,
        on_change: Callable[[frozenset[str], frozenset[str]], None],
        on_invalid: Callable[[str], None],
    ) -> None:
        """Start with no symbol counted.

        Args:
            asset_class: The class whose grammar the rows must follow, or None.
            on_change: Receives each change of the set of symbols.
            on_invalid: Receives the message of a row that is not canonical.
        """
        self._asset_class = asset_class
        self._counts: Counter[str] = Counter()
        self._on_change = on_change
        self._on_invalid = on_invalid

    @override
    def on_update(self, update: TableUpdate, is_replay: object) -> None:
        """Turn one update of the view into a change of the symbol set.

        A replay of the initial rows is the same change as rows being added; it is only
        logged at DEBUG with the number of symbols found. A row that is not canonical is
        reported to `on_invalid` before the change is.

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
        """Fail the stream: the watched table failed (or the listener did) and stops updating.

        A table made by `canonicalize_symbols` under `OnError.RAISE` fails on a later row it
        cannot convert; following it no further would leave the stream on stale symbols, so the
        failure reaches `on_invalid`, whose owner fails the stream. Logged at ERROR too.

        Args:
            e: What Deephaven reported.
        """
        logger.error("a symbol table's watch failed and stops following it: %s", e)
        self._on_invalid(_FAILED_TABLE.format(cause=e))

    def _count(self, columns: Mapping[str, Iterable[object]], step: int) -> None:
        """Add `step` to the count of each canonical symbol, forgetting counts that reach zero.

        A symbol that is not canonical is never counted: one that arrives is reported to
        `on_invalid`, and one that leaves is ignored.

        Args:
            columns: The `Symbol` values of some rows, as `TableUpdate` returns them: NumPy
                text, never null (the watched view has no null row).
            step: 1 for rows that now exist, -1 for rows that no longer do.
        """
        for value in columns.get(_SYMBOL, ()):
            symbol = str(value)  # a plain str, not NumPy's subclass, leaves the listener
            if not is_canonical(self._asset_class, symbol):
                if step > 0:
                    self._on_invalid(invalid_row_message(self._asset_class, symbol))
                continue
            count = self._counts[symbol] + step
            if count > 0:
                self._counts[symbol] = count
            else:
                del self._counts[symbol]
