"""How a futures stream reports the rows its calendar held too long or discarded.

A flush settles a futures dataset's held rows (`held_rows.HeldRows.settle`) and hands each of
its streams what concerns the tickers it receives: rows discarded because neither of Massive's
futures lists holds their ticker (`unlisted_contracts`) or because Massive's schedules hold no
session of their product (`unscheduled_products`), and rows that have waited
`held_rows.DELAYED_AFTER` for their date. This module words them: the problem events, the log
lines, and the failure under `dm.OnError.RAISE`. Plain values: no I/O, no lock, and no
Deephaven call.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._log_limits import bounded_list
from deephaven_massive._massive import Transport
from deephaven_massive._tables.futures_dates import UNLISTED_AFTER_TEXT, Readiness
from deephaven_massive._tables.held_rows import DELAYED_AFTER
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._enums import OnError
    from deephaven_massive._error_policy import ResolvedPolicy
    from deephaven_massive._tables.held_rows import Delayed

__all__ = ["DISCARDS", "Discard", "delayed_text", "futures_events"]


@dataclass(frozen=True, slots=True)
class Discard:
    """Why a futures ticker's rows are discarded, and how a stream reports it.

    Attributes:
        cause: The cause, named as its `dm.ErrorPolicy` field.
        setting: The stream's setting of that cause.
        text: What happened, with `{names}` for the tickers.
    """

    cause: ProblemCause
    setting: Callable[[ResolvedPolicy], OnError]
    text: str

    def message(self, dataset: str, tickers: Iterable[str]) -> str:
        """Word the discard of some tickers' rows for one dataset.

        Args:
            dataset: The dataset.
            tickers: The tickers.

        Returns:
            The message.
        """
        return f"{dataset}: {self.text.format(names=bounded_list(sorted(tickers)))}"


DISCARDS: Final[Mapping[Readiness, Discard]] = MappingProxyType(
    {
        Readiness.UNLISTED: Discard(
            ProblemCause.UNLISTED_CONTRACTS,
            lambda policy: policy.unlisted_contracts,
            "neither Massive's futures snapshot nor its contracts list has held {names} for "
            f"{UNLISTED_AFTER_TEXT}, so their rows are left out",
        ),
        Readiness.UNSCHEDULED: Discard(
            ProblemCause.UNSCHEDULED_PRODUCTS,
            lambda policy: policy.unscheduled_products,
            "Massive's futures schedules give no session for rows of {names}, so those rows "
            "are left out",
        ),
    }
)
"""Each state whose rows are discarded, with its cause."""


def delayed_text(dataset: str, tickers: Iterable[str]) -> str:
    """Word a wait past `DELAYED_AFTER` of some tickers' rows.

    Args:
        dataset: The dataset.
        tickers: The tickers.

    Returns:
        The message.
    """
    return (
        f"{dataset}: the rows of {bounded_list(sorted(tickers))} have waited over "
        f"{DELAYED_AFTER:g} s for their trading session; they publish once it loads"
    )


def futures_events(
    dataset: str,
    stream_id: int,
    where: str,
    *,
    discarded: Mapping[Readiness, Mapping[str, tuple[int, str]]],
    delays: Mapping[str, Delayed],
) -> tuple[ProblemEvent, ...]:
    """Return one stream's problem events for its discarded and delayed futures tickers.

    Args:
        dataset: The dataset.
        stream_id: The stream.
        where: The flush thread and channel.
        discarded: The rows discarded, by why and ticker, with their count and reason.
        delays: Each ticker whose rows waited `DELAYED_AFTER`.

    Returns:
        The events, the discards' first.
    """
    now = time.time_ns()
    found = [
        (DISCARDS[readiness].cause, ProblemAction.WITHHELD, ticker, rows, why)
        for readiness, tickers in discarded.items()
        for ticker, (rows, why) in tickers.items()
    ] + [
        (ProblemCause.UNDATED_ROWS, ProblemAction.DELAYED, ticker, delay.rows, delay.reason)
        for ticker, delay in delays.items()
    ]
    return tuple(
        ProblemEvent(cause, action, dataset, Transport.WS, stream_id, where, t, None, n, why, now)
        for cause, action, t, n, why in found
    )
