"""Symbol tables: the check of their rows, and the watch that follows them.

A user may pass a Deephaven `Table` with a `Symbol` column as a dataset's symbols, and edit it
while results are live. A table passed where a dataset takes one value (`ValueTable`: an option
chain's underlyings in `Symbol`, futures product codes in `ProductCode`) is read and watched the
same way, through a rule made by `SymbolRowRule.for_values`. A row should hold Massive's exact
ticker, as `symbols=` must (`check_symbol` in `_datasets/symbols.py`): the library never rewrites
a watchlist, so what the table says is what the stream subscribes and what `last_by()` matches. A
null row is ignored.
A row that is not canonical is never subscribed or requested; what else it does follows the
call's `noncanonical_symbols` setting, which `SymbolRowRule` carries:

- `OnError.RAISE`: a bad row present at the call raises `ValueError`, and one that arrives
  later fails the result, with a message that names the row and `canonicalize_symbols`.
- `OnError.LOG` and `OnError.IGNORE`: the row is skipped and the result keeps following the
  others. Each skipped value is a `noncanonical_symbols` span of the result's problems in
  `m.problems()` until its rows are fixed or deleted (`SkippedValues`); the session's problem
  reporter logs the first at WARNING under `LOG`. A static verb's read lists each skipped value
  as a problem of its call (`SymbolRowRule.skipped_events`).

Two pieces apply that rule:

- `read_symbols` reads a table's current rows once, on the caller thread or a poller's, and
  returns its good and its skipped values (`CurrentSymbols`), or raises `ValueError` under `RAISE`.
  The static verbs read their symbols with it; the live verbs check the table with it first.
- `SymbolWatch` listens to the table and reports each change of the *set* of symbols it holds,
  so a WebSocket stream subscribes and unsubscribes exactly the topics the watchlist names. A
  bad row that arrives later is reported through `on_invalid` under `RAISE`, and the stream
  fails; under `LOG` and `IGNORE` each change of the set of skipped values goes to
  `on_skipped`.

A third piece, `watched_rows`, keeps the rows of a live table whose symbol a watchlist holds,
as `last_by()` with keys that span symbols does before its aggregation; a fourth,
`latest_rows_since_added`, builds `last_by()` keyed by `Symbol`: each key's latest row, shown
only while the watchlist holds its symbol and only if the row reached the table in or after the
update cycle in which the symbol last joined, so a symbol deleted and added again shows no stale
row.

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
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from deephaven.table_listener import TableListener, TableListenerHandle
from typing_extensions import override

from deephaven_massive._datasets.row_times import ISSUES_URL
from deephaven_massive._datasets.symbols import (
    UNDERLYING_CLASSES,
    canonical_symbol,
    check_symbol,
    is_canonical,
    symbol_class,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.problem_events import (
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    SpanTracker,
)
from deephaven_massive._tables.refused_values import refused_detail

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import ValueTable
    from deephaven_massive._tables.problem_events import Problem

__all__ = [
    "CURRENT_STEP",
    "CurrentSymbols",
    "RowRule",
    "SkippedValues",
    "SymbolRowRule",
    "SymbolWatch",
    "invalid_row_message",
    "latest_rows_since_added",
    "read_symbols",
    "skipped_rows_text",
    "watched_rows",
]

logger = logging.getLogger(__name__)

_SYMBOL: Final = "Symbol"
_SHOWN: Final = 5
"""The most bad values one message lists."""
_WATCHED: Final = "__MassiveWatched"
"""The hidden column `watched_rows` joins on and drops. Dataset columns are PascalCase wire names,
so none can take this name."""
_ADDED_STEP: Final = "__MassiveAddedStep"
"""The hidden column `latest_rows_since_added` joins on and drops: the update cycle in which a
symbol last joined the watchlist."""
_ROW_STEP: Final = "__MassiveRowStep"
"""The hidden column `latest_rows_since_added` stamps and drops: the update cycle in which a row
reached the blink table."""
CURRENT_STEP: Final = (
    "io.deephaven.engine.context.ExecutionContext.getContext().getUpdateGraph().clock()"
    ".currentStep()"
)
"""A formula for the update graph's current cycle number (its logical clock's step), computed
where the formula runs. It grows by one with each cycle and does not change while a thread
holds the update graph's lock."""
_SKIPPED_LIMIT: Final = 100
"""The most skipped values logged per process under each of `LOG` and `IGNORE`."""
_logged_rows: Final = BoundedLog(logger, limit=_SKIPPED_LIMIT, level=logging.WARNING)
"""The skipped values of `OnError.LOG`: a row the user must fix, logged at WARNING."""
_ignored_rows: Final = BoundedLog(logger, limit=_SKIPPED_LIMIT, level=logging.DEBUG)
"""The skipped values of `OnError.IGNORE`: logged at DEBUG only."""


def invalid_row_message(
    asset_class: AssetClass | ClassFromPrefix, symbol: str, *, as_written: bool = False
) -> str:
    """Say why a symbol table's row is refused, and how to convert such a table.

    Args:
        asset_class: The dataset's class, or `ClassFromPrefix.ANY` for a dataset that spans
            classes.
        symbol: The row's symbol, which `check_symbol` refuses.
        as_written: True for a dataset whose symbols are kept as Massive writes them.

    Returns:
        The reason (with the canonical form when the class's converter gives one) and the
        remedy: fix the row, or convert the table; and, unless the class's converter turns the
        row into a ticker, where to report it, since the value can also be a ticker Massive
        writes in a form the library does not know yet. For a
        dataset of one class the remedy is `dm.canonicalize_symbols`, which by default nulls
        the rows it cannot convert and, with `on_error=dm.OnError.RAISE`, fails on them, so a
        caller who chose to fail on a bad row keeps failing; for a dataset that spans classes,
        whose rows may mix them, it is `dm.canonical_symbol` per row with each row's class. For
        stocks the remedy names `convention=`, since under the default, Massive's own spelling,
        no ticker with another source's separators converts.
    """
    reason = ""
    try:
        check_symbol(asset_class, symbol, as_written=as_written)
    except ValueError as error:
        reason = f" ({error})"
    if asset_class is ClassFromPrefix.ANY:
        remedy = (
            "convert each row with dm.canonical_symbol(text, asset_class) and the row's own "
            "class, since this dataset's rows may mix classes"
        )
    else:
        convention = (
            ", convention=dm.SymbolConvention.YAHOO), or with the convention the tickers come from"
            if asset_class is AssetClass.STOCKS
            else ")"
        )
        remedy = (
            f"pass the table through dm.canonicalize_symbols(table, "
            f"dm.AssetClass.{asset_class.name}{convention}, which converts what it can and "
            "nulls the rest, or, given on_error=dm.OnError.RAISE, fails on the rest"
        )
    message = f"a symbol table holds {symbol!r}{reason}. Fix the row, or {remedy}"
    if as_written or _converts(asset_class, symbol):
        return message
    return (
        f"{message}. If Massive itself writes {symbol!r}, the library does not know its form "
        f"yet: report it at {ISSUES_URL}"
    )


def _converts(asset_class: AssetClass | ClassFromPrefix, symbol: str) -> bool:
    """Tell whether a refused row is another spelling the class's converter turns into a ticker.

    Such a row (`EUR/USD`) is the caller's spelling, not an unknown form of Massive's, so its
    message needs no request to report the form.

    Args:
        asset_class: The dataset's class, or `ClassFromPrefix.ANY`, where the row's prefix
            selects it.
        symbol: The refused row.

    Returns:
        True when `dm.canonical_symbol` converts the row for its class.
    """
    resolved = symbol_class(symbol) if asset_class is ClassFromPrefix.ANY else asset_class
    try:
        canonical_symbol(symbol, resolved)
    except ValueError:
        return False
    return True


_FAILED_TABLE: Final = (
    "the symbol table passed to {dataset} failed upstream, so the result can no longer follow "
    "it: {cause}. Fix that table and call {dataset} again; a table made by "
    "dm.canonicalize_symbols fails on a row it cannot convert only under on_error=dm.OnError.RAISE"
)
"""The message of a result whose symbol table failed upstream, formatted with the dataset and
the cause."""


def skipped_rows_text(values: Iterable[str]) -> str:
    """Say which values of a symbol table are skipped, for the error of a table that holds none.

    Args:
        values: The distinct skipped values; at least one.

    Returns:
        Their count and the first five in sorted order, and what to do, such as
        `skipping 1 symbol-table value this dataset does not take: 'aapl'; fix or delete that
        row, or report it at <issues> if Massive itself writes it`. The value is not called
        wrong: Massive may write a form the library does not know yet.
    """
    texts = sorted(values)
    shown = ", ".join(repr(text) for text in texts[:_SHOWN])
    more = f", and {len(texts) - _SHOWN} more" if len(texts) > _SHOWN else ""
    if texts[1:]:
        return (
            f"skipping {len(texts)} symbol-table values this dataset does not take: "
            f"{shown}{more}; fix or delete those rows, or report them at {ISSUES_URL} if "
            "Massive itself writes them"
        )
    return (
        f"skipping 1 symbol-table value this dataset does not take: {shown}; fix or delete "
        f"that row, or report it at {ISSUES_URL} if Massive itself writes it"
    )


class RowRule(Protocol):
    """What a `SymbolWatch` asks of the rule its table's rows follow.

    `SymbolRowRule` is a dataset's rule; `refused_values.ConversionRule` is the rule of the
    input of a table `canonicalize_symbols` returned, whose skipped values are those the
    converter refuses.
    """

    @property
    def dataset(self) -> str:
        """The name the watch's log records use."""
        ...

    @property
    def column(self) -> str:
        """The table's column of values."""
        ...

    @property
    def noncanonical(self) -> OnError:
        """What a value the rule refuses does."""
        ...

    def accepts(self, symbol: str) -> bool:
        """Tell whether a value is good.

        Args:
            symbol: The value, never null.
        """
        ...

    def refusal(self, symbol: str) -> str:
        """Say why a value is refused.

        Args:
            symbol: A value `accepts` refuses.
        """
        ...

    def skip(self, symbol: str, *, stream_id: int | None) -> None:
        """Log a refused value, as the rule does.

        Args:
            symbol: The value.
            stream_id: The table that met it, or None.
        """
        ...


@dataclass(frozen=True, slots=True)
class SymbolRowRule:
    """How one dataset treats the rows of a symbol table: the grammar and the session's setting.

    Built by the transport that reads the table (`SymbolRowRule.of`), on any thread; immutable,
    so a watch's listener and a poller share it freely.

    Attributes:
        dataset: The dataset's name, such as `stocks.quotes`, for messages and log records.
        asset_class: The class whose grammar the rows must follow, or `ClassFromPrefix.ANY`
            for a dataset that spans classes (each symbol's prefix selects its class).
        as_written: True for a dataset whose symbols are kept as Massive writes them.
        noncanonical: The call's `noncanonical_symbols` setting (`Request.on_error`).
        column: The table's column that holds the values: `Symbol`, or `ProductCode` for a
            table of futures product codes.
        underlying: True when each value must be a stock or index ticker, the underlying of
            an option chain; False for any value of the grammar.
    """

    dataset: str
    asset_class: AssetClass | ClassFromPrefix
    as_written: bool
    noncanonical: OnError
    column: str = _SYMBOL
    underlying: bool = False

    @classmethod
    def of(cls, spec: DatasetSpec, noncanonical: OnError) -> SymbolRowRule:
        """Return the rule of a dataset in a session.

        Args:
            spec: The dataset.
            noncanonical: The call's `noncanonical_symbols` setting (`Request.on_error`).

        Returns:
            The rule.
        """
        return cls(spec.name, spec.asset_class, spec.symbols_as_written, noncanonical)

    @classmethod
    def for_values(
        cls, spec: DatasetSpec, values: ValueTable, noncanonical: OnError
    ) -> SymbolRowRule:
        """Return the rule of a table passed where a dataset takes one value.

        An underlying must be Massive's exact stock or index ticker, as `underlying=` takes
        it in code. A product code is kept as written: any text without surrounding
        whitespace or a class prefix (`AS_WRITTEN`), since Massive publishes no grammar for
        product codes. A value the rule refuses follows `noncanonical`, as a symbol table's
        row does.

        Args:
            spec: The dataset.
            values: The table and the argument it stands for.
            noncanonical: The call's `noncanonical_symbols` setting (`Request.on_error`).

        Returns:
            The rule, reading the table's `values.column`.
        """
        if values.underlying:
            return cls(
                spec.name,
                ClassFromPrefix.ANY,
                as_written=False,
                noncanonical=noncanonical,
                column=values.column,
                underlying=True,
            )
        return cls(
            spec.name,
            AssetClass.FUTURES,
            as_written=True,
            noncanonical=noncanonical,
            column=values.column,
        )

    def accepts(self, symbol: str) -> bool:
        """Tell whether a row's value is one the dataset takes.

        Args:
            symbol: The row's value, never null.

        Returns:
            True for Massive's exact ticker of the dataset's class; for a rule of
            underlyings, a stock or index ticker only.
        """
        if self.underlying and symbol_class(symbol) not in UNDERLYING_CLASSES:
            return False
        return is_canonical(self.asset_class, symbol, as_written=self.as_written)

    def refusal(self, symbol: str) -> str:
        """Say why a row is refused and how to fix it (`invalid_row_message`).

        Args:
            symbol: The row's value, which `accepts` refuses.

        Returns:
            The message, prefixed with the dataset's name.
        """
        return f"{self.dataset}: {self._reason(symbol)}"

    def skipped_events(
        self, values: Iterable[str], *, stream_id: int, where: str, field: str | None
    ) -> list[ProblemEvent]:
        """Return the `noncanonical_symbols` problems of one read's skipped values.

        A static verb reads its table once, so each skipped value is one problem of the call,
        not a span: the table never changes after the read.

        Args:
            values: The distinct values the read skipped.
            stream_id: The call's id, its `StreamId`.
            where: The table the values came from, such as `the symbol table of <query>`.
            field: None for the call's symbols; the column of a table of values.

        Returns:
            One `SKIPPED` event per value, with its reason and the call's setting.
        """
        at = time.time_ns()
        return [
            ProblemEvent(
                cause=ProblemCause.NONCANONICAL_SYMBOLS,
                action=ProblemAction.SKIPPED,
                dataset=self.dataset,
                stream_id=stream_id,
                where=where,
                symbol=value,
                field=field,
                rows=0,
                detail=self.refusal(value),
                at=at,
                policy=self.noncanonical,
            )
            for value in values
        ]

    def _reason(self, symbol: str) -> str:
        """Say why a row is refused, without the dataset's name.

        Args:
            symbol: The row's value, which `accepts` refuses.

        Returns:
            For a rule of underlyings, that an underlying is a stock or index ticker; else
            the reason of `invalid_row_message`.
        """
        if self.underlying:
            return (
                f"a table of underlyings holds {symbol!r}, which is not a stock or index ticker "
                "such as 'AAPL' or 'I:SPX'. Fix the row, or convert it with "
                "dm.canonical_symbol(text, asset_class)"
            )
        return invalid_row_message(self.asset_class, symbol, as_written=self.as_written)

    def skip(self, symbol: str, *, stream_id: int | None, reported: bool = False) -> None:
        """Log a skipped row once per value: at DEBUG for a table, else as `LOG` or `IGNORE` say.

        A table's skipped values are `noncanonical_symbols` problems of `m.problems()`, whose
        first WARNING per table the session's problem reporter logs (`SkippedValues`), so a
        table's reader logs each value at DEBUG only. So do the check a live verb makes before
        its table exists and a static verb's read, which lists its values itself (`reported`).
        A read no problem row covers logs under `LOG` at WARNING and under `IGNORE` at DEBUG.
        Each value is logged once per dataset, a bounded number of values per process, so a
        table edited for weeks keeps the log and the memory small. The record names the dataset
        and the stream that first met the value, such as `stocks.quotes stream 7: ...; the row
        is skipped until it is fixed or deleted`. Never called under `RAISE`.

        Args:
            symbol: The row's symbol, which `accepts` refuses.
            stream_id: The stream or `poll()` table that read the row, or None for a verb's
                read of the table before any stream exists (a static verb's, or the check a
                live verb makes first).
            reported: True for a read whose value `m.problems()` lists, with its first WARNING:
                the check a live verb makes first, and a static verb's read.
        """
        level = effect_of(self.noncanonical).level
        alarm = level > logging.DEBUG and stream_id is None and not reported
        rows = _logged_rows if alarm else _ignored_rows
        reader = "a call's read" if stream_id is None else f"stream {stream_id}"
        rows.log(
            f"{self.dataset}\0{symbol}",
            "%s %s: %s; the row is skipped until it is fixed or deleted",
            self.dataset,
            reader,
            self._reason(symbol),
        )


class SkippedValues:
    """A table's skipped symbol-table values, as its `noncanonical_symbols` spans.

    Each value a table skips is one span of `m.problems()` while some row holds it, which ends
    once every row of it is fixed or deleted. Its sources are the table's own reader (a
    watch's listener, a poller), with the values its rule skips (`update`), and, for each table
    it reads that `canonicalize_symbols` returned (its symbol table, a `poll()`'s value table),
    the values the converter refuses there (`refused_changed`, from
    `refused_values.RefusedValues`). A value table's spans name its column in `Field`, as its
    own skipped values do. Each change of a source reports the joined spans through `report`.
    One leaf lock orders the changes of every source, and the report is made under it, so the
    snapshots reach the session in order.
    """

    __slots__ = (
        "__weakref__",
        "_lock",
        "_own",
        "_refused",
        "_report",
        "_rule",
        "_tracker",
        "_values",
    )

    def __init__(
        self,
        rule: SymbolRowRule,
        *,
        stream_id: int,
        report: Callable[[Sequence[Problem]], None],
        values: SymbolRowRule | None = None,
    ) -> None:
        """Start with no value skipped.

        Args:
            rule: The rule that refuses the symbol table's values, for each one's reason.
            stream_id: The table's `StreamId`.
            report: Hands the spans to the table's session (`SessionCore.problems`); it must
                not block.
            values: The rule of a value table the table also reads (`SymbolRowRule.for_values`),
                or None.
        """
        self._rule = rule
        self._values = values
        self._report = report
        self._lock = threading.Lock()
        self._own: dict[str, tuple[str | None, str | None, str, int]] = {}
        # Each converted table's refused values, by its `field` (`refused_changed`): their
        # spans, and the converter's `on_error`.
        self._refused: dict[
            str | None, tuple[dict[str, tuple[str | None, str | None, str, int]], OnError]
        ] = {}
        self._tracker = SpanTracker(
            ProblemCause.NONCANONICAL_SYMBOLS,
            dataset=rule.dataset,
            stream_id=stream_id,
            where=f"the symbol table of stream {stream_id}",
            policy=rule.noncanonical,
        )

    def update(self, symbols: Iterable[str], *, values: Iterable[str] = ()) -> None:
        """Take the values the table's own rule skips now; report the spans if they changed.

        Args:
            symbols: The symbol table's distinct values skipped now; empty once none is.
            values: The value table's, skipped by the `values` rule.
        """
        # Named by the source and the value, quoted, as `refused_changed` names its spans, so
        # one text skipped in both tables is two spans.
        own: dict[str, tuple[str | None, str | None, str, int]] = {
            f"skipped {None!r} {value!r}": (value, None, self._rule.refusal(value), 0)
            for value in symbols
        }
        if self._values is not None:
            rule = self._values
            own.update(
                {
                    f"skipped {rule.column!r} {value!r}": (
                        value,
                        rule.column,
                        rule.refusal(value),
                        0,
                    )
                    for value in values
                }
            )
        with self._lock:
            self._own = dict(own)
            self._show()

    def update_values(self, values: Iterable[str]) -> None:
        """Take the values a table of values skips now, for a table that reads only that table.

        Args:
            values: Its distinct values skipped now, by the `values` rule; empty once none is.
        """
        self.update((), values=values)

    def refused_changed(
        self, refused: Mapping[str, str], policy: OnError, *, field: str | None = None
    ) -> None:
        """Take the values `canonicalize_symbols` refuses now in one table the table reads.

        Reports the spans if they changed. Each converted table is a source of its own, so a
        change of one leaves the other's spans as they were.

        Args:
            refused: Each value the converter refuses now, with its reason.
            policy: The converter's `on_error`.
            field: The column of a value table, which its spans name in `Field`; None for the
                symbol table.
        """
        # Named by the source and the value, quoted, so no two sources' names can meet.
        spans: dict[str, tuple[str | None, str | None, str, int]] = {
            f"refused {field!r} {value!r}": (
                value,
                field,
                refused_detail(why),
                0,
            )
            for value, why in refused.items()
        }
        with self._lock:
            self._refused[field] = (spans, policy)
            self._show()

    def _show(self) -> None:
        """Report the joined spans when they changed; the lock is held."""
        current: dict[str, tuple[str | None, str | None, str, int]] = {}
        policies: dict[str, OnError] = {}
        for refused, policy in self._refused.values():
            current.update(refused)
            policies.update(dict.fromkeys(refused, policy))
        current.update(self._own)
        spans = self._tracker.update(current, policies=policies)
        if spans is not None:
            self._report([spans])


@dataclass(frozen=True, slots=True)
class CurrentSymbols:
    """A symbol table's current canonical symbols and skipped values, as one read found them.

    Attributes:
        symbols: The canonical symbols, sorted and de-duplicated.
        skipped: The distinct values that are not canonical, sorted; always empty under
            `OnError.RAISE`, which raises instead.
    """

    symbols: tuple[str, ...]
    skipped: tuple[str, ...] = ()


def read_symbols(
    rule: SymbolRowRule, table: Table, *, stream_id: int | None, reported: bool = False
) -> CurrentSymbols:
    """Read a symbol table's current rows once, sorting them into good and skipped values.

    The table is read with `Table.iter_tuple`, which holds the update graph's shared lock while
    it reads, so the rows are one consistent snapshot. Runs on the caller thread, or a poller's,
    inside the session's execution context, with no library lock held. Null rows are ignored.
    Under `LOG` and `IGNORE` each skipped value is logged once (`SymbolRowRule.skip`).

    Args:
        rule: The dataset's rule: its grammar and the call's `noncanonical_symbols`.
        table: The user's table with the rule's column (`Symbol`, or `ProductCode`).
        stream_id: The stream or `poll()` table the read is for, named in a skipped row's
            record; None for a verb's read before any stream exists.
        reported: True for a read whose skipped values `m.problems()` lists: the check a live
            verb makes before its table exists, whose table reports them, and a static verb's
            read, which reports them as its call's (`SymbolRowRule.skip`).

    Returns:
        The table's canonical symbols and, under `LOG` and `IGNORE`, its skipped values; both
        empty for a table of no or only null rows.

    Raises:
        ValueError: If the table has failed (a `canonicalize_symbols` table under
            `OnError.RAISE` that met a bad row), or, under `OnError.RAISE`, a row is not
            Massive's exact ticker of the class (`invalid_row_message`, for the first such row
            in sorted order).
    """
    if table.j_table.isFailed():
        raise ValueError(_FAILED_TABLE.format(dataset=rule.dataset, cause="it is failed"))
    found = sorted({row[0] for row in table.iter_tuple(cols=rule.column) if row[0] is not None})
    good: list[str] = []
    skipped: list[str] = []
    for symbol in found:
        if rule.accepts(symbol):
            good.append(symbol)
        elif effect_of(rule.noncanonical).fails:
            raise ValueError(rule.refusal(symbol))
        else:
            rule.skip(symbol, stream_id=stream_id, reported=reported)
            skipped.append(symbol)
    return CurrentSymbols(tuple(good), tuple(skipped))


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
    looks up a crypto pair's split before it subscribes). What else it does follows the rule's
    `noncanonical` setting: under `OnError.RAISE`, `on_invalid(message)` receives the reason each
    time such a row arrives, and its owner fails the stream; under `LOG` and `IGNORE` the value
    is logged once (`SymbolRowRule.skip`), and `on_skipped(values)` receives the sorted distinct
    values skipped now each time that set changes, empty once every bad row is fixed or deleted.
    A fixed row is an added symbol like any other. When the watched table itself fails (a
    `canonicalize_symbols` table under `OnError.RAISE` that met a bad row), `on_invalid` receives
    that too, whatever the setting, so the stream never follows a frozen table. The callbacks run
    on the replaying caller thread or an update-graph thread, with the update-graph lock held:
    they must not block, wait on a library thread, or make a table operation, and an exception
    one raises is logged at ERROR by the listener's error callback.

    `keep_alive_with(table)` ties the view and the listener to another table's liveness, so
    they are not destroyed while that table lives: the transport passes the stream's blink
    table, so a stream that a derived table keeps alive keeps following its watchlist after the
    library has released its own results. Deephaven holds a table's listeners only weakly, so
    the owner also holds the watch itself (the stream's record does) until it calls `stop()`,
    rather than resting the stream's correctness on how the keeper manages its referents.

    Thread-safety: the symbol counts are touched only by the listener, whose calls Deephaven
    serializes, so they need no lock; `start` and `keep_alive_with` run once, on the caller
    thread, and `stop` once, on any thread with an execution context.

    Example:
        ::

            from deephaven import time_table

            from deephaven_massive._enums import AssetClass, OnError
            from deephaven_massive._tables.watchlist import SymbolRowRule, SymbolWatch

            # A live watchlist that adds a row every second, alternating two pairs.
            watchlist = time_table("PT1s").view("Symbol = ii % 2 == 0 ? `X:BTCUSD` : `X:ETHUSD`")
            rule = SymbolRowRule(
                "crypto.trades", AssetClass.CRYPTO, as_written=False, noncanonical=OnError.LOG
            )
            watch = SymbolWatch(
                rule,
                watchlist,
                lambda added, removed: None,
                stream_id=7,
                on_invalid=lambda message: None,
                on_skipped=lambda values: None,
            )
            watch.start()  # reports the current rows, then every change
            watch.stop()  # no more reports
    """

    __slots__ = ("_handle", "_view")

    def __init__(
        self,
        rule: RowRule,
        table: Table,
        on_change: Callable[[frozenset[str], frozenset[str]], None],
        *,
        stream_id: int,
        on_invalid: Callable[[str], None],
        on_skipped: Callable[[tuple[str, ...]], None],
    ) -> None:
        """Create the watched view and the listener, without starting it.

        Must run on a thread with the session's execution context, inside the liveness scope
        that should own the view and the listener, and outside every library lock.

        Args:
            rule: The dataset's grammar and the call's `noncanonical_symbols`.
            table: The user's refreshing table with the rule's column (`Symbol`, or
                `ProductCode`).
            on_change: Receives each change of the set of symbols; see the class documentation.
            stream_id: The stream that follows the table, named in the watch's log records.
            on_invalid: Receives the message of a bad row under `RAISE`, or of a failed table.
            on_skipped: Receives the values skipped now under `LOG` and `IGNORE`, each time
                that set changes.
        """
        # Without its null rows: `TableUpdate` hands a String column over as NumPy text, in
        # which a null reads as the text "None". A column of another name is watched as
        # `Symbol`, the name the counter reads.
        values = _SYMBOL if rule.column == _SYMBOL else f"{_SYMBOL} = {rule.column}"
        self._view = table.view([values]).where(f"!isNull({_SYMBOL})")
        self._handle = TableListenerHandle(
            self._view,
            _SymbolCounter(
                rule,
                on_change,
                stream_id=stream_id,
                on_invalid=on_invalid,
                on_skipped=on_skipped,
            ),
            description="deephaven_massive symbol watch",
        )

    def start(self) -> None:
        """Report the table's current symbols, then every later change.

        Runs on the caller thread with the session's execution context, outside every library
        lock: it takes the update-graph lock while it replays the table. `on_change` is called
        once for the replay when the table holds a valid symbol, and `on_skipped` when it holds
        a skipped one.
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

    def stop(self) -> None:
        """Stop reporting changes: the listener leaves the view. Idempotent.

        A Deephaven call: run it on a thread with the session's execution context, outside
        every library lock and never in a listener (the stream's shutdown queues it on the
        reaper).
        """
        self._handle.stop()


class _SymbolCounter(TableListener):
    """The listener of a `SymbolWatch`: counts the rows of each value and reports set changes.

    Good and skipped values are counted apart, so a value leaves its set only when its last row
    is deleted or fixed. Deephaven serializes its calls (the replay on the caller thread happens
    before any update is delivered), so its counts need no lock.
    """

    def __init__(
        self,
        rule: RowRule,
        on_change: Callable[[frozenset[str], frozenset[str]], None],
        *,
        stream_id: int,
        on_invalid: Callable[[str], None],
        on_skipped: Callable[[tuple[str, ...]], None],
    ) -> None:
        """Start with no value counted.

        Args:
            rule: The dataset's grammar and the call's `noncanonical_symbols`.
            on_change: Receives each change of the set of symbols.
            stream_id: The stream that follows the table, named in the log records.
            on_invalid: Receives the message of a bad row under `RAISE`, or of a failed table.
            on_skipped: Receives the values skipped now under `LOG` and `IGNORE`.
        """
        self._rule = rule
        self._stream_id = stream_id
        self._counts: Counter[str] = Counter()
        self._skipped: Counter[str] = Counter()
        self._on_change = on_change
        self._on_invalid = on_invalid
        self._on_skipped = on_skipped

    @override
    def on_update(self, update: TableUpdate, is_replay: object) -> None:
        """Turn one update of the view into a change of the symbol set and the skipped set.

        A replay of the initial rows is the same change as rows being added; it is only
        logged at DEBUG with the number of symbols found. Under `RAISE` a bad row is reported
        to `on_invalid` before the change is; under `LOG` and `IGNORE` the skipped set is
        reported after it.

        Args:
            update: The update; its added, removed, and modified rows' `Symbol` values.
            is_replay: True for the replay of the initial rows (Deephaven passes a `bool`).
        """
        before = set(self._counts)
        skipped_before = set(self._skipped)
        self._count(update.added(cols=[_SYMBOL]), 1)
        self._count(update.modified(cols=[_SYMBOL]), 1)
        self._count(update.modified_prev(cols=[_SYMBOL]), -1)
        self._count(update.removed(cols=[_SYMBOL]), -1)
        after = set(self._counts)
        added, removed = frozenset(after - before), frozenset(before - after)
        if is_replay:
            logger.debug(
                "%s stream %d: its symbol table holds %d valid symbols",
                self._rule.dataset,
                self._stream_id,
                len(after),
            )
        if added or removed:
            self._on_change(added, removed)
        if set(self._skipped) != skipped_before:
            self._on_skipped(tuple(sorted(self._skipped)))

    @override
    def on_error(self, e: Exception) -> None:
        """Fail the stream: the watched table failed (or the listener did) and stops updating.

        A table made by `canonicalize_symbols` under `OnError.RAISE` fails on a later row it
        cannot convert; following it no further would leave the stream on stale symbols, so the
        failure reaches `on_invalid`, whose owner fails the stream. It is a defect of the
        caller's table, which the stream's failure logs once at ERROR, naming the table and
        what to fix (`StreamSink.fail`); the watch logs it only at DEBUG, so it is not
        reported twice.

        Args:
            e: What Deephaven reported.
        """
        message = _FAILED_TABLE.format(dataset=self._rule.dataset, cause=e)
        logger.debug("%s stream %d: %s", self._rule.dataset, self._stream_id, message)
        self._on_invalid(message)

    def _count(self, columns: Mapping[str, Iterable[object]], step: int) -> None:
        """Add `step` to the count of each value, forgetting counts that reach zero.

        A canonical symbol is counted among the symbols. Any other value is counted among the
        skipped ones under `LOG` and `IGNORE`, and logged once when it arrives; under `RAISE` an
        arriving one is reported to `on_invalid` and a leaving one is ignored.

        Args:
            columns: The `Symbol` values of some rows, as `TableUpdate` returns them: NumPy
                text, never null (the watched view has no null row).
            step: 1 for rows that now exist, -1 for rows that no longer do.
        """
        for value in columns.get(_SYMBOL, ()):
            symbol = str(value)  # a plain str, not NumPy's subclass, leaves the listener
            counts = self._counts
            if not self._rule.accepts(symbol):
                if effect_of(self._rule.noncanonical).fails:
                    if step > 0:
                        self._on_invalid(self._rule.refusal(symbol))
                    continue
                if step > 0:
                    self._rule.skip(symbol, stream_id=self._stream_id)
                counts = self._skipped
            count = counts[symbol] + step
            if count > 0:
                counts[symbol] = count
            else:
                del counts[symbol]


def _watched_symbols(watchlist: Table) -> Table:
    """Return the watchlist's distinct symbols, without a null.

    A join matches a null key to a null key, so a null watchlist row (a row
    `canonicalize_symbols` could not convert, or an input-table row left empty) would match
    every data row whose symbol the library could not read, which `unreadable_symbols` keeps
    with a null `Symbol`. A null row watches nothing.

    Args:
        watchlist: The user's symbol table; only its `Symbol` column is read.

    Returns:
        A table with one `Symbol` column, one row per symbol the watchlist holds.
    """
    return watchlist.where(f"!isNull({_SYMBOL})").select_distinct(_SYMBOL)


def watched_rows(table: Table, watchlist: Table, *, key: str = _SYMBOL) -> Table:
    """Keep the rows of a table whose symbol the watchlist holds, following its changes.

    A row is kept while its symbol is in the watchlist, and leaves when the symbol does, as
    `where_in` would keep it. A blink table stays a blink table. Duplicate watchlist rows count
    once. A null watchlist row watches nothing, so a row with a null symbol is never kept.

    It is built from a join instead of `where_in`, because Deephaven 42.7's `where_in` delivers
    a failure of each of its two tables to its result: when the watchlist fails and the table
    fails after it, or both in one update cycle, the second failure trips an assertion that
    ends the process. A watchlist that fails fails its stream too, so a `where_in` here would
    end the process every time a watchlist failed. A join forwards the first failure of its
    two tables and ignores the second.

    Call it where a result is built (the `build` of `SharedStreams.result`), inside the
    result's liveness scope, which then manages the tables it creates.

    Args:
        table: A live table with a column of symbols, such as a stream's blink table or its
            `last_by`.
        watchlist: The user's symbol table; only its `Symbol` column is read.
        key: The table's column of symbols, `Symbol` unless the table's own `Symbol` column
            holds something else (a followed read's vectors of rows).

    Returns:
        The table's rows whose symbol the watchlist holds, with the table's columns.
    """
    marks = _watched_symbols(watchlist).update_view(f"{_WATCHED} = true")
    joined = table.natural_join(marks, on=f"{key}={_SYMBOL}", joins=_WATCHED)
    return joined.where(f"{_WATCHED} = true").drop_columns(_WATCHED)


def latest_rows_since_added(blink: Table, watchlist: Table, keys: Sequence[str]) -> Table:
    """Return each key's latest row, while the watchlist holds its symbol and since it joined.

    The result is `blink.last_by(keys)`, keyed by columns that include `Symbol`, filtered so a
    row shows only while the watchlist holds its symbol and only if the row reached the table
    in or after the update cycle in which the symbol last joined the watchlist. A symbol the
    watchlist holds when this is called joins now, so the latest row of each shows at once,
    including a row the blink table holds from the cycle that just ran. A symbol deleted and
    added again is a new row of the distinct watchlist, stamped with a new cycle, so a row that
    entered the table before the cycle in which its symbol last joined stays hidden until the
    symbol's next row. A null watchlist row watches nothing, so a row with a null symbol never
    shows. The rule compares only the update graph's own cycle numbers, never a
    wall clock, so neither a row's `TimestampInsert` nor a clock running behind can hide it.

    Meant for `last_by()` keyed by `Symbol`: the aggregation remembers every key it has seen,
    and the filter after it hides what it remembers from before a symbol's latest join. Keys
    that span symbols filter first instead (`watched_rows`). It is built from a join, for the
    reason `watched_rows` gives.

    Call it where a result is built (the `build` of `SharedStreams.result`), inside the
    result's liveness scope, which then manages the tables it creates. It holds the update
    graph's shared lock while it creates the two stamped tables, so it blocks while an update
    cycle runs; a caller that already holds either lock (a console command) takes it again at
    once. Never call it on an update-graph thread or under a library lock.

    Args:
        blink: A stream's blink table, with a `Symbol` column.
        watchlist: The user's symbol table; only its `Symbol` column is read.
        keys: The `last_by` key columns, `Symbol` among them.

    Returns:
        The latest row of each key that passes the filter, with the blink table's columns.
    """
    # The Java lock itself, as `release_scope` takes it: Deephaven's `shared_lock` would wrap
    # an error of the table operations in a second `DHError`.
    lock = blink.update_graph.j_update_graph.sharedLock()
    lock.lock()
    try:
        # The watchlist side is created first, so a symbol's added step is never later than
        # the step of any row the aggregation will hold, even if a cycle ran between the two
        # creations (were this lock ever dropped): the step only grows. The lock additionally
        # keeps every cycle out until both sides exist, so the symbols and rows present now get
        # equal steps. `update`, never `update_view`, on both sides: a view would compute the
        # step when a row is read, not in the cycle the row changed.
        added = _watched_symbols(watchlist).update(f"{_ADDED_STEP} = {CURRENT_STEP}")
        # Stamped before the aggregation: a formula that reads no column is not computed again
        # when a row is modified, so after `last_by` it would keep the step of each key's first
        # row.
        latest = blink.update(f"{_ROW_STEP} = {CURRENT_STEP}").last_by(list(keys))
    finally:
        lock.unlock()
    joined = latest.natural_join(added, on=_SYMBOL, joins=_ADDED_STEP)
    kept = joined.where(f"!isNull({_ADDED_STEP}) && {_ROW_STEP} >= {_ADDED_STEP}")
    return kept.drop_columns([_ADDED_STEP, _ROW_STEP])
