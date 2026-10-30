"""The table form of the symbol converters: `canonicalize_symbols`.

`_datasets/symbols.py` holds the symbol grammar and the scalar converters (`canonical_symbol`
and one function per class and convention), which make no Deephaven call. A user whose
watchlist is written in another spelling (`EUR/USD`, Yahoo's `BRK-B`) needs the same conversion
over a whole table, static or refreshing, before passing it as `symbols=`; that is table code,
which reads rows and adds a formula column, so it lives here, on the Deephaven side.
`canonicalize_symbols` is the one lenient path of the library: under `OnError.NULL` it nulls
the rows it cannot convert instead of failing. The tables it returns are what the symbol-table
check in `_tables/watchlist.py` (`read_symbols`, `SymbolWatch`) then accepts.

Every function here runs on the caller's thread, inside a Deephaven execution context, and the
formula it installs is pure, so Deephaven may run it on several threads at once.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from deephaven import dtypes
from deephaven.table import Table

from deephaven_massive._datasets.symbols import canonical_symbol
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention, _require_enum
from deephaven_massive._log_limits import BoundedWarnings

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = ["canonicalize_symbols"]

logger = logging.getLogger(__name__)

_SHOWN: Final = 5
"""The most bad values one message lists."""
_NULL_WARNINGS: Final = 20
"""The most bad values one `canonicalize_symbols` table logs under `OnError.NULL`."""


def _refused(values: Iterable[str], convert: Callable[[str], str | None]) -> dict[str, str]:
    """Return the distinct values that do not convert, with their reasons.

    Args:
        values: The distinct non-null values.
        convert: The class's converter.

    Returns:
        Each value that raised, mapped to its `ValueError` message.
    """
    refused: dict[str, str] = {}
    for value in values:
        try:
            convert(value)
        except ValueError as error:
            refused[value] = str(error)
    return refused


def canonicalize_symbols(
    table: Table,
    asset_class: AssetClass,
    *,
    column: str = "Symbol",
    convention: SymbolConvention = SymbolConvention.MASSIVE,
    on_error: OnError = OnError.RAISE,
) -> Table:
    """Return a table whose symbol column holds Massive's exact tickers, converted row by row.

    The table form of `canonical_symbol`, and the one lenient path in the library: use it to
    turn a watchlist written in another spelling (`EUR/USD`, Yahoo's `BRK-B`) into one a
    dataset accepts as `symbols=`. Each row converts as `canonical_symbol(value, asset_class,
    convention=convention)` would; a null stays null.

    The table's current rows are checked in Python first, one call per distinct value, so a
    bad value raises a clean `ValueError` here, whether the table is static or refreshing;
    every table is then converted by a formula (`Table.update`), on every row of a static
    table and every row that ticks in a refreshing one. Under `OnError.RAISE` a later bad row
    of a refreshing table fails the table, and a dataset reading it as a watchlist fails too;
    under `OnError.NULL` a bad row gets a null symbol, which every dataset ignores, and each
    bad value is logged once at WARNING, at most 20 per table. Call it on a thread with a
    Deephaven execution context, as any table operation; the result belongs to the caller's
    liveness scope. The conversion is pure, so
    Deephaven may run it on several threads at once.

    Example:
        ::

            from deephaven import new_table
            from deephaven.column import string_col

            import deephaven_massive as dm

            pairs = new_table([string_col("Symbol", ["EUR/USD", "GBP-USD"])])
            watchlist = dm.canonicalize_symbols(pairs, dm.AssetClass.FOREX)
            # watchlist.Symbol: C:EURUSD, C:GBPUSD

    Args:
        table: Any table with a `String` column of symbols.
        asset_class: The class of every symbol in the column. Required: nothing is inferred.
        column: The column to convert; the result keeps every other column unchanged.
        convention: The convention the symbols come from: `SymbolConvention.MASSIVE`, the
            default, for Massive's own spelling and the unambiguous spellings of the class, or,
            for stocks only, `YAHOO`, `NASDAQ`, or `CMS`.
        on_error: `OnError.RAISE` (the default) fails on a value that does not convert;
            `OnError.NULL` writes a null symbol for it.

    Returns:
        A table with the same rows and columns, whose `column` holds canonical tickers (or
        nulls): static for a static table, refreshing for a refreshing one.

    Raises:
        ValueError: Under `OnError.RAISE`, if a current value of the table does not convert
            (the message lists up to five values and their reasons).

    A `table` that is not a Deephaven `Table`, a `column` that is not a `str`, or an
    `asset_class`, `convention`, or `on_error` of the wrong type raises `TypeError` (an enum's
    names the member to use); a `column` that is not a `String` column of the table, or a
    stock convention for a class other than stocks, raises `ValueError`.
    """
    resolved, chosen, mode = _arguments(
        table, asset_class=asset_class, column=column, convention=convention, on_error=on_error
    )
    warnings = BoundedWarnings(logger, limit=_NULL_WARNINGS)

    def convert(text: str) -> str | None:
        return canonical_symbol(text, resolved, convention=chosen)

    def refuse(value: str, reason: str) -> None:
        warnings.warn(value, "canonicalize_symbols: %s; its rows get a null %s", reason, column)

    # The current rows are converted in Python first, over one consistent snapshot of their
    # values (iter_tuple holds the shared lock), so a bad one raises a clean ValueError here,
    # static table or refreshing. They are de-duplicated here, not by a select_distinct, which
    # on a refreshing table would leave a hidden aggregation ticking for the caller's scope.
    values = {row[0] for row in table.iter_tuple(cols=column) if row[0] is not None}
    refused = _refused(values, convert)
    if refused and mode is OnError.RAISE:
        shown = "; ".join(refused[value] for value in sorted(refused)[:_SHOWN])
        raise ValueError(
            f"{len(refused)} value(s) of {column} are not {resolved.value} symbols: "
            f"{shown}. Pass on_error=dm.OnError.NULL to null them instead"
        )
    for value in sorted(refused):  # logged here, in sorted order; the formula repeats none
        refuse(value, refused[value])
    massive_canonical_symbol = _row_converter(convert, refuse, mode)
    # Deephaven resolves the formula's function from this call's local variables, by the name
    # the helper gives it.
    return table.update([f"{column} = {massive_canonical_symbol.__name__}({column})"])


def _arguments(
    table: object, *, asset_class: object, column: object, convention: object, on_error: object
) -> tuple[AssetClass, SymbolConvention, OnError]:
    """Check the arguments of `canonicalize_symbols`.

    Args:
        table: The `table` argument.
        asset_class: The `asset_class` argument.
        column: The `column` argument.
        convention: The `convention` argument.
        on_error: The `on_error` argument.

    Returns:
        The class, the convention, and the error mode.

    Raises:
        TypeError: If `table` is not a Deephaven `Table` or `column` is not a `str`.
        ValueError: If `column` is not a `String` column of the table, or `convention` is a
            stock convention and the class is not stocks.

    An enum argument of the wrong type raises the `TypeError` of `_require_enum`.
    """
    if not isinstance(table, Table):
        raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
    resolved = _require_enum(asset_class, AssetClass, "asset_class")
    chosen = _require_enum(convention, SymbolConvention, "convention")
    if chosen is not SymbolConvention.MASSIVE and resolved is not AssetClass.STOCKS:
        raise ValueError(
            f"the {chosen.name} convention applies to stocks only, not {resolved.value}"
        )
    mode = _require_enum(on_error, OnError, "on_error")
    if not isinstance(column, str):
        raise TypeError(f"column must be a str, not {type(column).__name__}")
    types = {c.name: c.data_type for c in table.columns}
    if types.get(column) != dtypes.string:
        raise ValueError(f"column {column!r} must be a String column of the table")
    return resolved, chosen, mode


def _row_converter(
    convert: Callable[[str], str | None], refuse: Callable[[str, str], None], mode: OnError
) -> Callable[[str | None], str | None]:
    """Return the formula function of `canonicalize_symbols`: one conversion per row.

    Args:
        convert: The class's converter.
        refuse: Logs a value that does not convert (bounded).
        mode: `RAISE` lets the converter's `ValueError` fail the table; `NULL` logs the value
            and returns None.

    Returns:
        A function from a row's text (or None) to its canonical symbol (or None).
    """

    def massive_canonical_symbol(text: str | None) -> str | None:
        if text is None:
            return None
        if mode is OnError.RAISE:
            return convert(text)
        try:
            return convert(text)
        except ValueError as error:
            refuse(text, str(error))
            return None

    return massive_canonical_symbol
