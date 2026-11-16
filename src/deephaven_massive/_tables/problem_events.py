"""A problem with the feed, as plain data: what the library withheld or delays, where, and why.

Each transport turns the rows it withholds from a payload (`withheld.withhold_rows`) into
`ProblemEvent`s, one per cause, symbol as sent, and field, and a futures flush does the same
for the rows it could not date or held too long (`futures_problems`); each hands them to its
session's `ProblemSink` (`SessionCore.problems`). An event is immutable and holds no Deephaven
object, lock, or secret, so any thread may build one and any thread may receive it. Until the
session keeps a problems table, `LoggedProblems` logs each event at DEBUG.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._massive import RowFault, Transport

__all__ = [
    "EVENTS_PER_PAYLOAD",
    "LoggedProblems",
    "ProblemAction",
    "ProblemCause",
    "ProblemEvent",
    "ProblemSink",
]

logger = logging.getLogger(__name__)

EVENTS_PER_PAYLOAD: Final = 1000
"""The most events one payload gives, one per cause, symbol, and field; the rest of each cause
is one `ProblemAction.OVERFLOW` event."""


class ProblemAction(StrEnum):
    """What the library did about the rows an event counts."""

    WITHHELD = "withheld"
    """The rows were left out of the table."""
    DELAYED = "delayed"
    """The rows wait, and are not lost: they publish once they can be read whole."""
    OVERFLOW = "overflow"
    """The rows were left out of the table, and the payload had more distinct problems of the
    cause than `EVENTS_PER_PAYLOAD`: this event counts the rest, with no symbol or field."""


class ProblemCause(StrEnum):
    """Why there is a problem that no `RowFault` names: so far, dating streamed futures rows."""

    UNLISTED_CONTRACTS = "unlisted_contracts"
    """Neither Massive's futures snapshot nor its contracts list has held a ticker it streams
    for 30 minutes, so its rows were left out (`dm.ErrorPolicy.unlisted_contracts`)."""
    UNSCHEDULED_PRODUCTS = "unscheduled_products"
    """Massive's futures schedules give no session for rows it streams, none at all for their
    product or none that holds them, so they were left out
    (`dm.ErrorPolicy.unscheduled_products`)."""
    UNDATED_ROWS = "undated_rows"
    """A ticker's rows have waited over 15 seconds for its session date; they publish, dated,
    once its schedule loads. No setting decides it."""


@dataclass(frozen=True, slots=True)
class ProblemEvent:
    """One problem of one payload: the rows of one cause, symbol, and field that were withheld.

    Attributes:
        cause: The cause: a `dm.ErrorPolicy` field, or `ProblemCause.UNDATED_ROWS`.
        action: What the library did with the rows.
        dataset: The dataset, such as `"stocks.trades"`.
        transport: The transport that served the rows.
        stream_id: The live table the rows were for, or None for a read that has none (a
            `history()` or `fetch()` that waits).
        where: The request, the WebSocket URL and channel, or the flat file, as the transport
            names it in its log lines, with no secret.
        symbol: The rows' symbol exactly as Massive sent it, or None when they carry none or
            it is not text.
        field: The column of the value that could not be read, or None for an
            `ProblemAction.OVERFLOW` event.
        rows: How many rows of the payload the event counts.
        reason: Why the first of them was withheld: the field, the expected type, and the
            value's first 200 characters, or the derivation that failed.
        at: When the event was made, epoch nanoseconds.
    """

    cause: RowFault | ProblemCause
    action: ProblemAction
    dataset: str
    transport: Transport
    stream_id: int | None
    where: str
    symbol: str | None
    field: str | None
    rows: int
    reason: str
    at: int


ProblemSink: TypeAlias = "Callable[[Sequence[ProblemEvent]], None]"
"""Receives a payload's events. It never blocks, takes no lock, and calls no Java, so a flush
thread may call it."""


class LoggedProblems:
    """A `ProblemSink` that logs each event at DEBUG on this module's logger."""

    __slots__ = ()

    def __call__(self, events: Sequence[ProblemEvent]) -> None:
        """Log one line per event.

        Args:
            events: A payload's events.
        """
        for event in events:
            logger.debug(
                "%s %s %s: %s %d rows of %s, %s (stream %s): %s",
                event.dataset,
                event.transport.value,
                event.where,
                event.action.value,
                event.rows,
                event.symbol,
                event.cause.value,
                event.stream_id,
                event.reason,
            )
