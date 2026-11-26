"""Whether a read could read any of its rows whole: for the whole read, or for each symbol.

A `fetch()` or `history()` may span many REST pages, calls, and paths, or many flat files, read
on several threads; an `AUTO` `history()` may read both. One unreadable row withholds only that
row (`withheld.withhold_rows`). The read's entry point makes its one `ReadTally` and hands it to
every part of the read, which adds each call's, request's, or file's rows once it knows which
rows those are, and never checks it; the entry point checks it once, after the read's last
part.

- **A read of one table** (`check`): a `fetch()` or `history()` that names no symbol
  (`dm.ALL`, a path without symbols), or a static one of a list, whose every row goes to its
  one table: it fails, whatever its policy, when it had rows and the library could read none
  of them whole, a problem of every row it would publish.
- **A read by symbol** (`judge`): each symbol is judged by its own rows, so one symbol's rows
  never fail another. A symbol fails when it got no row and the library withheld rows that
  were its own (`add_request`, by the asked symbol each withheld row would have gone to;
  `add`, by the canonical symbol a flat file's row names). A withheld row whose symbol is not
  known could be any of its request's symbols': when that request kept no row at all, each of
  those symbols that got no row fails. A withheld row of another symbol fails no one. Such a
  failure (`WithheldSymbolError`) carries the faults of its rows, so it fails the whole call
  only when one of their causes is `RAISE` (`call_failure.fails_call`).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deephaven_massive._errors import RowDataError, WithheldSymbolError
from deephaven_massive._log_limits import counted, prose_list
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows
from deephaven_massive._tables.withheld import WITHHELD_LISTED, Withheld, reason_sentence

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven_massive._massive import RowFault
    from deephaven_massive._tables.transports import SymbolOutcome
    from deephaven_massive._tables.withheld import WithheldGroup

__all__ = ["ReadTally"]

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Withholding:
    """Withheld rows one symbol, or some symbols of one request together, would have received.

    Attributes:
        rows: How many.
        reason: Why the first of them was withheld.
        sources: What they were read from, in the order first seen.
        faults: Their faults, in the order first seen.
    """

    rows: int = 0
    reason: str = ""
    sources: dict[str, None] = field(default_factory=dict)
    faults: dict[RowFault, None] = field(default_factory=dict)

    def take(self, group: WithheldGroup, source: str) -> None:
        """Count withheld rows.

        Args:
            group: The rows.
            source: What they were read from.
        """
        self.rows += group.rows
        self.reason = self.reason or group.reason
        self.sources.setdefault(source, None)
        self.faults.setdefault(group.fault, None)


@dataclass(slots=True)
class _Request:
    """One request's, or one flat-file scan's, rows kept and rows of no known symbol.

    Attributes:
        kept: Its rows given to the symbols it named.
        unknown: Its withheld rows whose symbol is not known, by the symbols they could be;
            the empty tuple stands for every symbol the read judges (a scan's).
    """

    kept: int = 0
    unknown: dict[tuple[str, ...], _Withholding] = field(default_factory=dict)


class ReadTally:
    """The rows of one read, how many were withheld, where from, and whose.

    Shared by the read's worker threads; one leaf lock guards it.
    """

    __slots__ = (
        "_asked",
        "_first_reason",
        "_lock",
        "_owned",
        "_requests",
        "_scan",
        "_sources",
        "_withheld",
    )

    def __init__(self) -> None:
        """Start with no rows counted."""
        self._lock = threading.Lock()
        self._asked = 0
        self._withheld = 0
        self._first_reason = ""
        self._sources: dict[str, None] = {}
        self._owned: dict[str, _Withholding] = {}
        self._requests: list[_Request] = []
        self._scan = _Request()

    def add(self, asked: int, withheld: Withheld, *, source: str) -> None:
        """Count one static call's or one file's rows.

        Every row is the read's, for `check`. For `judge`, the rows are one flat-file scan's:
        a withheld row counts for the canonical symbol it names (`WithheldGroup.symbol`), or,
        when that is not known, for the scan.

        Args:
            asked: The call's or file's rows of the read's symbols and window, withheld ones
                included.
            withheld: The rows of those the library withheld; its first reason is the first
                such row's.
            source: What the call or file was read from, for the failure's message, such as
                `"GET /v2/aggs/..."` or `"the flat files of us_stocks_sip/trades_v1"`.
        """
        with self._lock:
            self._asked += asked
            self._withheld += withheld.total
            if asked:
                self._sources.setdefault(source, None)
            if withheld.total and not self._first_reason:
                self._first_reason = withheld.first_reason
            self._scan.kept += asked - withheld.total
            for group in withheld.groups:
                if group.symbol is None:
                    known = self._scan.unknown.setdefault((), _Withholding())
                else:
                    known = self._owned.setdefault(group.symbol, _Withholding())
                known.take(group, source)

    def add_request(
        self,
        *,
        kept: int,
        withheld: Withheld,
        owners: Callable[[str | None], tuple[str, ...]],
        source: str,
    ) -> None:
        """Count one request's rows of a read by symbol, for `judge` and for `check`.

        The rows that count are those of the asked symbols: its kept rows, and the withheld rows
        `owners` gives to one or more of them. A withheld row of another symbol counts for
        neither, since no table of the read would have received it.

        Args:
            kept: The request's rows given to the symbols it named.
            withheld: The request's withheld rows, grouped.
            owners: The asked symbols a withheld group's rows could be, from its
                `WithheldGroup.symbol`: one symbol for its own, several when the symbol is
                not known, none for another symbol's.
            source: What the request was read from, such as `"GET /v3/trades/AAPL"`.
        """
        request = _Request(kept)
        with self._lock:
            asked = kept
            for group in withheld.groups:
                found = owners(group.symbol)
                if not found:
                    continue  # another symbol's row is none of the read's
                asked += group.rows
                self._withheld += group.rows
                self._first_reason = self._first_reason or group.reason
                if len(found) == 1:
                    owned = self._owned.setdefault(found[0], _Withholding())
                    owned.take(group, source)
                else:
                    unknown = request.unknown.setdefault(found, _Withholding())
                    unknown.take(group, source)
            self._asked += asked
            if asked:
                self._sources.setdefault(source, None)
            self._requests.append(request)

    def failure(self, dataset: str) -> RowDataError | None:
        """Return the read's failure if the library could read none of its rows (`add`) whole.

        Logs the read's counts at DEBUG.

        Args:
            dataset: The dataset, for the message.

        Returns:
            The `RowDataError` of a read that had rows of its symbols and window and withheld
            every one of them, whatever the read's policy, naming what the rows were read
            from; None for any other read, one with no such rows included.
        """
        with self._lock:
            asked, withheld, first = self._asked, self._withheld, self._first_reason
            sources = dict(self._sources)
        logger.debug(
            "%s: %d rows of the read's symbols and window from %s, %d withheld",
            dataset,
            asked,
            ", ".join(sources) or "no source",
            withheld,
        )
        if not asked or withheld < asked:
            return None
        withholding = _Withholding(asked, first, sources)
        return RowDataError(
            _message(dataset, withholding, whose="of the read's symbols and window")
        )

    def check(self, dataset: str) -> None:
        """Fail a read of one table if the library could read none of its rows whole.

        Args:
            dataset: The dataset, for the message.

        Raises:
            RowDataError: The read's `failure`, when it has one.
        """
        failure = self.failure(dataset)
        if failure is not None:
            raise failure

    def judge(
        self, dataset: str, outcomes: Mapping[str, SymbolOutcome]
    ) -> dict[str, SymbolOutcome]:
        """Fail each symbol of a read by symbol that got no row and whose rows were withheld.

        A symbol with rows never fails here, and neither does one with no rows and no withheld
        row of its own, such as one Massive had no data for. A symbol that failed on its own
        keeps its failure.

        Args:
            dataset: The dataset, for the message.
            outcomes: Each symbol's outcome, in order.

        Returns:
            The outcomes, in the same order, judged.
        """
        everyone = tuple(outcomes)
        empty = [
            symbol
            for symbol, outcome in outcomes.items()
            if isinstance(outcome, SymbolRows) and not outcome.table.size
        ]
        failed = self.unread_symbols(dataset, empty, everyone)
        return {
            symbol: SymbolFailure(failed[symbol]) if symbol in failed else outcome
            for symbol, outcome in outcomes.items()
        }

    def unread_symbols(
        self, dataset: str, empty: Sequence[str], everyone: Sequence[str]
    ) -> dict[str, WithheldSymbolError]:
        """Return the failure of each symbol that got no row and whose rows were withheld.

        Args:
            dataset: The dataset, for the message.
            empty: The symbols that got no row and did not fail on their own, in order.
            everyone: Every symbol the read judges, for a scan's rows of no known symbol.

        Returns:
            Each such symbol's failure (`symbol_failure`), in the order of `empty`; a symbol
            with no withheld row of its own, such as one Massive had no data for, has none.
        """
        failed: dict[str, WithheldSymbolError] = {}
        for symbol in empty:
            failure = self.symbol_failure(dataset, symbol, everyone)
            if failure is None:
                continue
            # At DEBUG and not marked logged: the call's own record of the symbol, or of the
            # table it fails, is the one a user sees.
            logger.debug(
                "%s by symbol fails %s: %s",
                dataset,
                symbol,
                str(failure).removeprefix(f"{dataset}: "),
            )
            failed[symbol] = failure
        return failed

    def symbol_failure(
        self, dataset: str, symbol: str, everyone: Sequence[str]
    ) -> WithheldSymbolError | None:
        """Return a symbol's failure, if rows that were or could be its own were all withheld.

        Args:
            dataset: The dataset, for the message.
            symbol: A symbol that got no row.
            everyone: Every symbol the read judges, for a scan's rows of no known symbol.

        Returns:
            The failure for the symbol's own withheld rows, else for the rows of no known
            symbol of a request of it that kept no row; None when it has neither.
        """
        with self._lock:
            if symbol in self._owned:
                return _symbol_failure(dataset, self._owned[symbol], whose=f"for {symbol}")
            for request in (*self._requests, self._scan):
                if request.kept:
                    continue
                for could, withholding in request.unknown.items():
                    named = could or tuple(everyone)
                    if symbol in named:
                        whose = f"for {prose_list(named, 'symbols')}"
                        return _symbol_failure(dataset, withholding, whose=whose)
        return None


def _symbol_failure(dataset: str, withholding: _Withholding, *, whose: str) -> WithheldSymbolError:
    """Return the failure of a symbol whose rows the library withheld every one of.

    Args:
        dataset: The dataset.
        withholding: The rows, their first reason and faults, and what they were read from.
        whose: Whose rows they were, such as `"for MSFT"`.

    Returns:
        The failure, with the message `_message` gives and the rows' faults.
    """
    message = _message(dataset, withholding, whose=whose)
    return WithheldSymbolError(message, faults=tuple(withholding.faults))


def _message(dataset: str, withholding: _Withholding, *, whose: str) -> str:
    """Return the message of a read or symbol whose rows the library withheld every one of.

    Args:
        dataset: The dataset.
        withholding: The rows, their first reason, and what they were read from.
        whose: Whose rows they were, after "Massive sent N rows", such as `"for MSFT"`.

    Returns:
        Such as `stocks.bars: Massive sent 2 rows for MSFT from GET
        /v2/aggs/ticker/MSFT/range/1/day/..., and the library could read none of them whole,
        so it withheld them all. ...`.
    """
    rows, sources = withholding.rows, ", ".join(withholding.sources)
    read = (
        "the library could not read it whole, so it withheld it"
        if rows == 1
        else "the library could read none of them whole, so it withheld them all"
    )
    return (
        f"{dataset}: Massive sent {counted(rows, 'row')} {whose} from {sources}, and {read}. "
        f"{reason_sentence(withholding.reason)} {WITHHELD_LISTED}"
    )
