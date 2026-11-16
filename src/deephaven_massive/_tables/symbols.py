"""The table form of the symbol converters: `canonicalize_symbols`.

`_datasets/symbols.py` holds the symbol grammar and the scalar converters (`canonical_symbol`
and one function per class and convention), which make no Deephaven call. A user whose
watchlist is written in another spelling (`EUR/USD`, Yahoo's `BRK-B`) needs the same conversion
over a whole table, static or refreshing, before passing it as `symbols=`; that is table code,
which reads rows and adds a formula column, so it lives here, on the Deephaven side.
Under `OnError.LOG`, the default, or `OnError.IGNORE`, `canonicalize_symbols` nulls the rows
it cannot convert instead of failing, so one bad row never fails the table; `OnError.RAISE`
fails instead. It takes its own `on_error` argument, a `dm.OnError` member, rather than a
session's `dm.ErrorPolicy`, since it needs no session. Each row's verdict and the
effect of `on_error` on it are the scalar converters' own (`strict_converter`, `refusal`,
`convert_under`), so a value converts, or is refused with one reason, the same way in a cell
and in `canonical_symbol`. The tables it returns are what the symbol-table check in
`_tables/watchlist.py` (`read_symbols`, `SymbolWatch`) then accepts.

Every function here runs on the caller's thread, inside a Deephaven execution context, and the
formula it installs is pure, so Deephaven may run it on several threads at once.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from deephaven import dtypes
from deephaven.table import Table

from deephaven_massive._datasets.symbols import (
    LOGGED_VALUES,
    check_convention,
    convert_under,
    refusal,
    strict_converter,
)
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention, _require_enum
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._on_error import effect_of

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from deephaven_massive._on_error import OnErrorEffect

__all__ = ["canonicalize_symbols"]

logger = logging.getLogger(__name__)

_SHOWN: Final = 5
"""The most bad values one message lists."""


def _refused(values: Iterable[str], convert: Callable[[str], str]) -> dict[str, str]:
    """Return the distinct values that do not convert, with their reasons.

    Args:
        values: The distinct non-null values.
        convert: The class's strict verdict (`strict_converter`).

    Returns:
        Each value that does not convert, mapped to its reason (`refusal`).
    """
    refused: dict[str, str] = {}
    for value in values:
        reason = refusal(value, convert)
        if reason is not None:
            refused[value] = reason
    return refused


def canonicalize_symbols(
    table: Table,
    asset_class: AssetClass,
    *,
    column: str = "Symbol",
    convention: SymbolConvention = SymbolConvention.MASSIVE,
    on_error: OnError = OnError.LOG,
) -> Table:
    """Return a table whose symbol column holds Massive's exact tickers, converted row by row.

    The table form of `canonical_symbol`: use it to turn a watchlist written in another
    spelling (`EUR/USD`, Yahoo's `BRK-B`) into one a dataset accepts as `symbols=`. The
    converted column keeps its name. Each row converts as `canonical_symbol(value, asset_class,
    convention=convention)` would; a null stays null.

    The table's current rows are checked when you call, and on a live table every row added
    later converts as it arrives. Under `OnError.LOG`, the default, a bad row gets a null
    symbol, which every dataset ignores, and each bad value is logged once at WARNING, at most
    20 per table, so the table and every table made from it keep updating. Under
    `OnError.IGNORE` it gets a null symbol the same way, logged at DEBUG only. Under
    `OnError.RAISE`, a current bad value raises `ValueError` here, and a bad row added later
    fails the live table, along with any dataset that follows it as a watchlist. This
    function's `on_error` is its own: a session's `dm.ErrorPolicy` doesn't apply to it.

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
        column: The column to convert, `"Symbol"` by default. The converted values replace
            the column's values and keep its name, so a column named anything but `Symbol`
            must be renamed before the table is passed as `symbols=`
            (`.rename_columns("Symbol = Ticker")`). Every other column is kept unchanged.
        convention: The convention the symbols come from: `SymbolConvention.MASSIVE`, the
            default, for Massive's own spelling and the unambiguous spellings of the class, or,
            for stocks only, `YAHOO`, `NASDAQ`, or `CMS`.
        on_error: What a value that does not convert does. `OnError.LOG`, the default,
            writes a null symbol for it and logs the value once at WARNING; `OnError.IGNORE`
            writes a null symbol and logs it at DEBUG only; `OnError.RAISE` fails.

    Returns:
        A table with the same rows and columns, whose `column` holds canonical tickers (or
        nulls): static for a static table, live for a live one.

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
    effect = effect_of(mode)
    # Under RAISE nothing logs to `bad`: a bad value raises below or fails the formula.
    bad = BoundedLog(logger, limit=LOGGED_VALUES, level=effect.level)
    convert = strict_converter(resolved, chosen)
    # The current rows are converted in Python first, over one consistent snapshot of their
    # values (iter_tuple holds the shared lock), so a bad one raises a clean ValueError here,
    # static table or refreshing. They are de-duplicated here, not by a select_distinct, which
    # on a refreshing table would leave a hidden aggregation ticking for the caller's scope.
    values = {row[0] for row in table.iter_tuple(cols=column) if row[0] is not None}
    refused = _refused(values, convert)
    if refused and effect.fails:
        shown = "; ".join(refused[value] for value in sorted(refused)[:_SHOWN])
        raise ValueError(
            f"{len(refused)} value(s) of {column} are not {resolved.value} symbols: "
            f"{shown}. Pass on_error=dm.OnError.LOG to null them instead"
        )
    for value in sorted(refused):  # logged here, in sorted order; the formula repeats none
        bad.log(value, "canonicalize_symbols: %s; its rows get a null %s", refused[value], column)
    massive_canonical_symbol = _row_converter(
        convert, effect=effect, log=bad, consequence=f"its rows get a null {column}"
    )
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
        ValueError: If the table has no `column` (the message lists its columns and names a
            `String` one to pass), or `column` is not a `String` column.

    An enum argument of the wrong type raises the `TypeError` of `_require_enum`, and a stock
    convention for a class other than stocks the `ValueError` of `check_convention`.
    """
    if not isinstance(table, Table):
        raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
    resolved, chosen = check_convention(asset_class, convention)
    mode = _require_enum(on_error, OnError, "on_error")
    if not isinstance(column, str):
        raise TypeError(f"column must be a str, not {type(column).__name__}")
    types = {c.name: c.data_type for c in table.columns}
    if column not in types:
        texts = [name for name, data_type in types.items() if data_type == dtypes.string]
        # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
        logger.debug("canonicalize_symbols refused: no column %r in %s", column, list(types))
        remedy = (
            f"pass column={texts[0]!r}, or the name of another String column of symbols"
            if texts
            else "it has no String column to read symbols from"
        )
        raise ValueError(f"table has no column {column!r} (columns: {', '.join(types)}); {remedy}")
    if types[column] != dtypes.string:
        raise ValueError(f"column {column!r} must be a String column of the table")
    return resolved, chosen, mode


def _row_converter(
    convert: Callable[[str], str], *, effect: OnErrorEffect, log: BoundedLog, consequence: str
) -> Callable[[str | None], str | None]:
    """Return the formula function of `canonicalize_symbols`: one conversion per row.

    Args:
        convert: The class's strict verdict.
        effect: What `on_error` does with a value that does not convert: `RAISE` lets the
            verdict's `ValueError` fail the table; `LOG` and `IGNORE` log the value and give
            the row None (`convert_under`).
        log: The table's bounded log of such values.
        consequence: What the row gets, for the log record.

    Returns:
        A function from a row's text (or None) to its canonical symbol (or None).
    """

    def massive_canonical_symbol(text: str | None) -> str | None:
        if text is None:
            return None
        return convert_under(
            text,
            convert,
            effect=effect,
            log=log,
            caller="canonicalize_symbols",
            consequence=consequence,
        )

    return massive_canonical_symbol
