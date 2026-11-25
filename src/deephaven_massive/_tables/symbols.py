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
and in `canonical_symbol`. For a live input, a watch keeps the values it refuses now
(`refused_values.py`), so a live table that follows the result reports them in
`m.problems()`. The tables it returns are what the symbol-table check in `_tables/watchlist.py`
(`read_symbols`, `SymbolWatch`) then accepts.

Every function here runs on the caller's thread, inside a Deephaven execution context, and the
formula it installs is pure, so Deephaven may run it on several threads at once.
"""

from __future__ import annotations

import functools
import logging
from typing import TYPE_CHECKING, Final

from deephaven import dtypes
from deephaven.table import Table

from deephaven_massive._datasets.symbols import (
    CLASS_NOUNS,
    LOGGED_VALUES,
    SymbolRefusalError,
    check_convention,
    convert_under,
    refusal,
    strict_converter,
)
from deephaven_massive._enums import AssetClass, OnError, SymbolConvention, _require_enum
from deephaven_massive._log_limits import BoundedLog, prose_list
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.refused_values import ConversionRule, RefusedValues, track_refused
from deephaven_massive._tables.watchlist import SymbolWatch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from deephaven_massive._on_error import OnErrorEffect

__all__ = ["canonicalize_symbols"]

logger = logging.getLogger(__name__)

_SHOWN: Final = 5
"""The most bad values one message lists."""


def _refused(values: Iterable[str], convert: Callable[[str], str]) -> dict[str, SymbolRefusalError]:
    """Return the distinct values that do not convert, with their refusals.

    Args:
        values: The distinct non-null values.
        convert: The class's strict verdict (`strict_converter`).

    Returns:
        Each value that does not convert, mapped to its refusal (`refusal`): the reason and
        the fix apart, and the message they make.
    """
    refused: dict[str, SymbolRefusalError] = {}
    for value in values:
        refused_as = refusal(value, convert)
        if refused_as is not None:
            refused[value] = refused_as
    return refused


def _reason(value: str, convert: Callable[[str], str]) -> str | None:
    """Return the reason a value does not convert, without the fix, or None when it does.

    The `why` of a `RefusedValues` and a `ConversionRule`: `m.problems()` shows it as the
    `Detail` of the value's `noncanonical_symbols` row, whose `Cause` leads to the fix.

    Args:
        value: The value.
        convert: The class's strict verdict (`strict_converter`).

    Returns:
        The converter's reason, or None for a value that converts.
    """
    refused = refusal(value, convert)
    return None if refused is None else refused.reason


def canonicalize_symbols(
    table: Table,
    asset_class: AssetClass,
    *,
    column: str = "Symbol",
    convention: SymbolConvention = SymbolConvention.MASSIVE,
    on_error: OnError = OnError.LOG,
) -> Table:
    """Return a table whose symbol column holds Massive's exact tickers, converted row by row.

    Use it to turn a watchlist written in another ticker format (`EUR/USD`, Yahoo's `BRK-B`)
    into one you can pass as `symbols=`. Each row converts as `dm.canonical_symbol(value,
    asset_class, convention=convention)` would. A null stays null. The current rows convert
    when you call. On a live table, each row added or changed later converts as it arrives.

    By default, a value that doesn't convert gets a null symbol. Every query skips a null
    symbol, so one such value never stops your table.

    A query that reads the returned table lists each such value in `m.problems()` under the
    cause `noncanonical_symbols`. A table you make from the returned one, with `where` or
    `rename_columns`, doesn't list them, so filter or rename before you convert.

    Set `on_error=dm.OnError.RAISE` to fail instead. A value in the current rows then raises
    `ValueError` from this call. A row added later with such a value fails the returned table
    and every live table that reads it as `symbols=`.

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
        asset_class: The class of every symbol in the column. Required: nothing is inferred,
            so a table that mixes classes is converted one class's rows at a time.
        column: The column to convert, `"Symbol"` by default. `symbols=` and `underlying=`
            read `Symbol`, so rename yours first: `table.rename_columns("Symbol = Ticker")`.
        convention: The convention the symbols come from: `SymbolConvention.MASSIVE`, the
            default, for Massive's own spelling and the unambiguous spellings of the class, or,
            for stocks only, `YAHOO`, `NASDAQ`, or `CMS`: the source the tickers came from,
            not the exchange they trade on.
        on_error: What happens to a value that doesn't convert:

            - `OnError.LOG`, the default: a null symbol. Each value is logged once at `WARNING`,
              up to 20 values per call.
            - `OnError.IGNORE`: a null symbol, and the value is logged at `DEBUG` only.
            - `OnError.RAISE`: `ValueError`, or a failed live table, as above.

            The `noncanonical_symbols` setting of `dm.ErrorPolicy` doesn't apply here.

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
        count = len(refused)
        first = f" The first {_SHOWN}:" if count > _SHOWN else ""
        shown = " ".join(str(refused[value]) for value in sorted(refused)[:_SHOWN])
        raise ValueError(
            f"canonicalize_symbols cannot convert {count} {'value' if count == 1 else 'values'} "
            f"of the {column} column to {CLASS_NOUNS[resolved]} tickers.{first} {shown} Correct "
            "those values, or pass on_error=dm.OnError.LOG to give their rows a null symbol."
        )
    consequence = f"becomes a null {column}"
    for value in sorted(refused):  # logged here, in sorted order; the formula repeats none
        bad.log(value, "canonicalize_symbols: %r %s. %s", value, consequence, refused[value])
    massive_canonical_symbol = _row_converter(
        convert, effect=effect, log=bad, consequence=consequence
    )
    # Deephaven resolves the formula's function from this call's local variables, by the name
    # the helper gives it.
    converted = table.update([f"{column} = {massive_canonical_symbol.__name__}({column})"])
    if table.is_refreshing and not effect.fails:
        _track_refused(table, converted, column=column, mode=mode, convert=convert)
    elif not effect.fails:  # a static input's refused values are the ones found above
        _keep_refused(converted, tuple(refused), column=column, mode=mode, convert=convert)
    return converted


def _keep_refused(
    converted: Table,
    values: tuple[str, ...],
    *,
    column: str,
    mode: OnError,
    convert: Callable[[str], str],
) -> None:
    """Keep the values a static input table holds that the converter refuses.

    A table that reads `converted` reports each one in `m.problems()` (`refused_values.py`);
    the input never changes, so no watch keeps them current.

    Args:
        converted: The table `canonicalize_symbols` returns.
        values: The input's refused values.
        column: The column it converted.
        mode: The call's `on_error`, `LOG` or `IGNORE`.
        convert: The class's strict verdict (`strict_converter`).
    """
    refused = RefusedValues(mode, why=functools.partial(_reason, convert=convert))
    refused.change(values)
    track_refused(converted, refused, column=column)


def _track_refused(
    table: Table, converted: Table, *, column: str, mode: OnError, convert: Callable[[str], str]
) -> None:
    """Keep the values a live input table holds now that the converter refuses.

    A live table that follows `converted` reports each such value in `m.problems()` until its
    rows are fixed or deleted (`refused_values.py`). A watch of the input table keeps them
    current; `converted` keeps the watch alive, and the values are forgotten with it.

    Args:
        table: The input table, refreshing.
        converted: The table `canonicalize_symbols` returns.
        column: The input's column of symbols.
        mode: The call's `on_error`, `LOG` or `IGNORE`.
        convert: The class's strict verdict (`strict_converter`).
    """
    why = functools.partial(_reason, convert=convert)
    refused = RefusedValues(mode, why=why)
    watch = SymbolWatch(
        ConversionRule("canonicalize_symbols", column, why),
        table,
        _no_change,
        stream_id=0,
        on_invalid=lambda _message: None,  # the converted table fails with its input on its own
        on_skipped=refused.change,
    )
    watch.keep_alive_with(converted)
    watch.start()
    refused.keep(watch)
    track_refused(converted, refused, column=column)


def _no_change(added: frozenset[str], removed: frozenset[str]) -> None:
    """Ignore the input's convertible values: only the refused ones are kept.

    Args:
        added: Unused.
        removed: Unused.
    """
    del added, removed


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
            f"Pass column={texts[0]!r}, or the name of another String column of symbols."
            if texts
            else "Pass a table with a String column of symbols."
        )
        named = prose_list(types, "columns", shown=len(types))
        raise ValueError(f"table has no column {column!r}. Its columns are {named}. {remedy}")
    if types[column] != dtypes.string:
        raise ValueError(
            f"column={column!r} names a column of {types[column]}, not of String. Pass the "
            "name of a String column of symbols."
        )
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
