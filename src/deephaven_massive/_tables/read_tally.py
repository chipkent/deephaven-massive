"""Whether a static read could read any of its rows whole, counted across its pages or files.

A `fetch()` or `history()` may span many REST pages, calls, and paths, or many flat files, read
on several threads. One unreadable row withholds only that row (`withheld.withhold_rows`); the
read fails, whatever its policy, only when the library could read none of the rows of its
symbols and window whole, the sign of a change in Massive's format. Each page or file adds its
counts to the read's `ReadTally` as it is read, and the read checks the tally once, after its
last page or file.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from deephaven_massive._errors import RowDataError

if TYPE_CHECKING:
    from deephaven_massive._tables.withheld import Withheld

__all__ = ["ReadTally"]

logger = logging.getLogger(__name__)


class ReadTally:
    """The rows of one read's symbols and window, and how many of them were withheld.

    Shared by the read's worker threads; one leaf lock guards it.
    """

    __slots__ = ("_asked", "_first_reason", "_lock", "_withheld")

    def __init__(self) -> None:
        """Start with no rows counted."""
        self._lock = threading.Lock()
        self._asked = 0
        self._withheld = 0
        self._first_reason = ""

    def add(self, asked: int, withheld: Withheld) -> None:
        """Count one page's or file's rows.

        Args:
            asked: The page's or file's rows of the read's symbols and window, withheld ones
                included.
            withheld: The rows of those the library withheld.
        """
        with self._lock:
            self._asked += asked
            self._withheld += withheld.total
            if withheld.total and not self._first_reason:
                self._first_reason = withheld.first_reason

    def failure(self, dataset: str, *, source: str) -> RowDataError | None:
        """Return the read's failure if the library could read none of its rows whole.

        Logs the read's counts at DEBUG.

        Args:
            dataset: The dataset, for the message.
            source: What the read read, for the message, such as `"GET
                /v2/aggs/..."` or `"the flat files of us_stocks_sip/trades_v1"`.

        Returns:
            The `RowDataError` of a read that had rows of its symbols and window and withheld
            every one of them, whatever the read's policy; None for any other read, one with no
            such rows included.
        """
        with self._lock:
            asked, withheld, first = self._asked, self._withheld, self._first_reason
        logger.debug(
            "%s: %d rows of the read's symbols and window from %s, %d withheld",
            dataset,
            asked,
            source,
            withheld,
        )
        if not asked or withheld < asked:
            return None
        return RowDataError(
            f"{dataset}: the library could read none of the {asked} rows of the read's "
            f"symbols and window from {source} whole, so Massive's format may have changed; "
            f"the first: {first}"
        )

    def check(self, dataset: str, *, source: str) -> None:
        """Fail the read if the library could read none of its rows whole (`failure`).

        Args:
            dataset: The dataset, for the message.
            source: What the read read, for the message.

        Raises:
            RowDataError: The read's `failure`, when it has one.
        """
        failure = self.failure(dataset, source=source)
        if failure is not None:
            raise failure
