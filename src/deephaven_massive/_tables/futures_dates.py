"""What a futures flush asks its session calendar: each row's date, and each ticker's state.

WebSocket futures messages carry no session date. The flush dates each row through the
WebSocket transport's calendar (`_ingest/futures_calendar.py`), which implements
`FuturesSessionDates`: `correct` dates the rows whose product's schedule is loaded, and `states`
says, for each ticker whose rows got no date, whether to hold them (`Readiness.WAITING`),
publish them as they are (`Readiness.READY`), discard them (`Readiness.UNLISTED`), or fail the
streams that wait on them (`Readiness.REFUSED`). Plain values and a protocol: no I/O, no lock,
and no Deephaven call.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Final, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable

    import pyarrow as pa

    from deephaven_massive._errors import MassiveError

__all__ = [
    "UNLISTED_AFTER",
    "UNLISTED_AFTER_TEXT",
    "DateCorrector",
    "FuturesSessionDates",
    "LookupState",
    "Readiness",
]

UNLISTED_AFTER: Final = timedelta(minutes=30)
"""How long a ticker may be missing from both of Massive's futures lists, from their first
answer without it, before its rows are discarded (`Readiness.UNLISTED`)."""

UNLISTED_AFTER_TEXT: Final = "30 minutes"
"""`UNLISTED_AFTER` in words, for messages."""


class Readiness(Enum):
    """Whether a ticker's rows can be dated now, wait, or will not be dated."""

    READY = "ready"
    """Its product's schedule is loaded: `correct` dates its rows, or gives a row the loaded
    sessions cannot place a null date."""
    WAITING = "waiting"
    """A lookup it needs is queued, on the wire, failing transiently, or has not found it yet."""
    UNLISTED = "unlisted"
    """Neither of Massive's lists has held it for long enough: its rows are discarded."""
    UNSCHEDULED = "unscheduled"
    """Massive's schedules hold no session of its product at all: its rows are withheld."""
    REFUSED = "refused"
    """Massive refused a lookup it needs: the streams whose rows wait on it fail."""


@dataclass(frozen=True, slots=True)
class LookupState:
    """A ticker's state, as one `FuturesSessionDates.states` call saw it.

    Attributes:
        readiness: Whether its rows can be dated.
        reason: Why it is not ready, for the log and the problem events; empty when ready.
        failing_since: When the lookup it waits on began failing transiently, seconds since
            the epoch; None when it is not failing.
        error: Massive's refusal, for a refused ticker; None otherwise.
        loaded_at: When the last load of its product's schedule began, seconds since the
            epoch, for a ready ticker; None otherwise. A row no session holds is withheld once
            a load that began after the row was held answered without one.
    """

    readiness: Readiness
    reason: str = ""
    failing_since: float | None = None
    error: MassiveError | None = None
    loaded_at: float | None = None


class DateCorrector(Protocol):
    """Corrects the 17:00 Chicago rule's session dates of streamed futures rows; never waits."""

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        """Return the rows' session dates, corrected by the schedules.

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds,
                `int64`.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row; null for a row whose ticker is not
            ready, or that the loaded schedule cannot place.
        """
        ...


class FuturesSessionDates(DateCorrector, Protocol):
    """The schedule calendar that dates streamed futures rows.

    The WebSocket transport gives a futures connection's flusher one. No method waits on the
    network.
    """

    @property
    def version(self) -> int:
        """A count that grows each time the calendar learns something: a state may have changed."""
        ...

    def alive(self) -> bool:
        """Return whether the calendar still answers lookups.

        Returns:
            False only when its worker ended without the calendar closing: a defect, after
            which no waiting ticker is ever answered.
        """
        ...

    def unplaced(self, symbol: str, instant: int) -> str:
        """Say why no loaded session of a ready ticker's product holds one of its rows.

        Args:
            symbol: The ticker.
            instant: The row's instant, epoch nanoseconds.

        Returns:
            The product, the row's instant, and the sessions of Massive's schedule nearest it.
        """
        ...

    def states(self, symbols: Iterable[str]) -> dict[str, LookupState]:
        """Return each ticker's state, asking for the lookups it needs.

        Args:
            symbols: Canonical futures tickers.

        Returns:
            The state of each, by ticker.
        """
        ...
