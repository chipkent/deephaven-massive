"""Whether a static read could read any of its rows whole, counted across its pages or files.

A `fetch()` or `history()` may span many REST pages, calls, and paths, or many flat files, read
on several threads; an `AUTO` `history()` may read both. One unreadable row withholds only that
row (`withheld.withhold_rows`); the read fails, whatever its policy, only when the library could
read none of the rows of its symbols and window whole, a problem of every row it would publish.
The read's entry point makes its one `ReadTally` and hands it to every part of the read, which
adds each page's or file's counts as it is read and never checks it; the entry point checks it
once, after the read's last part: `check` for a static read, `judge` for a read by symbol.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from deephaven_massive._errors import RowDataError
from deephaven_massive._log_limits import counted, log_failure
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows
from deephaven_massive._tables.withheld import WITHHELD_LISTED, Withheld, reason_sentence

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = ["ReadTally"]

logger = logging.getLogger(__name__)


class ReadTally:
    """The rows of one read's symbols and window, how many were withheld, and where from.

    Shared by the read's worker threads; one leaf lock guards it.
    """

    __slots__ = ("_asked", "_first_reason", "_lock", "_sources", "_withheld")

    def __init__(self) -> None:
        """Start with no rows counted."""
        self._lock = threading.Lock()
        self._asked = 0
        self._withheld = 0
        self._first_reason = ""
        self._sources: dict[str, None] = {}

    def add(self, asked: int, withheld: Withheld, *, source: str) -> None:
        """Count one page's or file's rows.

        Args:
            asked: The page's or file's rows of the read's symbols and window, withheld ones
                included.
            withheld: The rows of those the library withheld.
            source: What the page or file was read from, for the failure's message, such as
                `"GET /v2/aggs/..."` or `"the flat files of us_stocks_sip/trades_v1"`.
        """
        with self._lock:
            self._asked += asked
            self._withheld += withheld.total
            if asked:
                self._sources.setdefault(source, None)
            if withheld.total and not self._first_reason:
                self._first_reason = withheld.first_reason

    def exclude(self, rows: int) -> None:
        """Count out rows that were counted as the read's but belong to none of its symbols.

        A page is counted when it is read (`add`), and the rows naming a symbol the read did
        not ask for are found later, when the page's rows are given to the symbols
        (`symbol_reads._attribute`). They are no rows of the read's symbols, so a read whose
        own rows were all withheld fails although it received such rows too.

        Args:
            rows: How many rows to count out.
        """
        with self._lock:
            self._asked -= rows

    @property
    def rows(self) -> int:
        """How many rows of the read's symbols and window were counted so far, withheld included."""
        with self._lock:
            return self._asked

    def failure(self, dataset: str) -> RowDataError | None:
        """Return the read's failure if the library could read none of its rows whole.

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
            sources = ", ".join(self._sources)
        logger.debug(
            "%s: %d rows of the read's symbols and window from %s, %d withheld",
            dataset,
            asked,
            sources or "no source",
            withheld,
        )
        if not asked or withheld < asked:
            return None
        read = (
            f"could not read the 1 row of the read's symbols and window from {sources} whole, "
            "so it withheld it"
            if asked == 1
            else f"could read none of the {counted(asked, 'row')} of the read's symbols and "
            f"window from {sources} whole, so it withheld them all"
        )
        return RowDataError(
            f"{dataset}: the library {read}. {reason_sentence(first)} {WITHHELD_LISTED}"
        )

    def check(self, dataset: str) -> None:
        """Fail a static read if the library could read none of its rows whole (`failure`).

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
        """Fail a read by symbol's symbols if the library could read none of its rows whole.

        A followed table's read fails as a static read does, but by symbol: each symbol the
        read gave rows fails with the read's `failure`, and the table goes on. A symbol that
        failed on its own keeps its failure, and one Massive has no data for
        (`SymbolRows.no_data`) keeps its empty rows.

        Args:
            dataset: The dataset, for the message.
            outcomes: Each symbol's outcome, in order.

        Returns:
            The outcomes, in the same order, judged.
        """
        failure = self.failure(dataset)
        if failure is None:
            return dict(outcomes)
        log_failure(
            logger,
            failure,
            "%s by symbol fails each symbol the read gave rows: %s",
            dataset,
            expected=logging.DEBUG,
            named=dataset,
        )
        return {
            symbol: SymbolFailure(failure)
            if isinstance(outcome, SymbolRows) and not outcome.no_data
            else outcome
            for symbol, outcome in outcomes.items()
        }
