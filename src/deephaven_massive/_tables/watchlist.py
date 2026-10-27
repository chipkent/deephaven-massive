"""Reactive symbol tables: the canonical view every consumer of a watchlist reads.

A user may pass a Deephaven `Table` with a `Symbol` column as a dataset's symbols, and edit it
while results are live. Its rows may use any accepted spelling (`EUR/USD`, `BTC-USD`, `SPX`),
while the rows a stream publishes always carry the canonical form. `canonical_view` puts the
table's `Symbol` column in canonical form once, through the per-row canonicalizer of
`_datasets/symbols.py`, so every consumer agrees on what a row of the watchlist means.

This is table code: it creates a refreshing view, so it lives on the Deephaven side, while the
symbol rules themselves stay in `_datasets/symbols.py`, which makes no Deephaven call.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from deephaven_massive._datasets.symbols import symbol_canonicalizer

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive._enums import AssetClass

__all__ = ["canonical_view"]

logger = logging.getLogger(__name__)


def canonical_view(asset_class: AssetClass | None, table: Table) -> Table:
    """Return a view of a reactive symbol table whose `Symbol` column is canonical.

    Every consumer of a reactive symbol table reads it through this view, so the canonical
    form is decided in one place: `last_by` filters the stream's rows (which carry canonical
    symbols) with `where_in` against it. A row that is not a valid symbol of the class becomes
    null, so it matches nothing, and is logged once per class at WARNING. Must be called inside
    the liveness scope that should own the view, on a thread with the session's execution
    context. Deephaven resolves the formula's function from this call's local variables.

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
