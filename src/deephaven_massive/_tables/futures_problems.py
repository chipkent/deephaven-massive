"""How a futures stream words the rows its calendar discarded.

A flush settles a futures dataset's held rows (`held_rows.HeldRows.settle`) and hands each of
its streams what concerns the tickers it receives: rows discarded because neither of Massive's
futures lists holds their ticker (`unlisted_contracts`) or because Massive's schedules hold no
session of their product (`unscheduled_products`). This module words them: the problem events
and the failure under `dm.OnError.RAISE`. The flusher keeps the spans of rows that wait for
their date (`undated_rows`). Plain values: no I/O, no lock, and no Deephaven call.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._enums import OnError
from deephaven_massive._error_policy import policy_hint
from deephaven_massive._log_limits import prose_list
from deephaven_massive._tables.futures_dates import UNLISTED_AFTER_TEXT, Readiness
from deephaven_massive._tables.problem_events import ProblemCause, ProblemEvent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._error_policy import ResolvedPolicy

__all__ = ["DISCARDS", "Discard", "futures_events"]


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
        """Word the failure of a stream that discarded some tickers' rows under `RAISE`.

        Args:
            dataset: The dataset.
            tickers: The tickers.

        Returns:
            What happened, that the cause's `RAISE` stopped the table, and the setting that
            streams the rest.
        """
        cause = self.cause.value
        names = prose_list(sorted(tickers), "tickers")
        return (
            f"{dataset}: {self.text.format(names=names)}. This table stopped because its "
            f"on_error holds dm.ErrorPolicy({cause}=dm.OnError.RAISE). "
            + policy_hint(OnError.LOG, cause, purpose="To stream the rest")
        )


DISCARDS: Final[Mapping[Readiness, Discard]] = MappingProxyType(
    {
        Readiness.UNLISTED: Discard(
            ProblemCause.UNLISTED_CONTRACTS,
            lambda policy: policy.unlisted_contracts,
            "neither Massive's futures snapshot nor its contracts list has held {names} for "
            f"{UNLISTED_AFTER_TEXT}, so the library withheld their rows",
        ),
        Readiness.UNSCHEDULED: Discard(
            ProblemCause.UNSCHEDULED_PRODUCTS,
            lambda policy: policy.unscheduled_products,
            "Massive's futures schedules give no trading session for rows of {names}, so the "
            "library withheld those rows",
        ),
    }
)
"""Each state whose rows are discarded, with its cause."""


def futures_events(
    dataset: str,
    stream_id: int,
    where: str,
    *,
    discarded: Mapping[Readiness, Mapping[str, tuple[int, str]]],
    policy: ResolvedPolicy,
) -> tuple[ProblemEvent, ...]:
    """Return one stream's problem events for its discarded futures tickers.

    Args:
        dataset: The dataset.
        stream_id: The stream.
        where: The flush thread and channel.
        discarded: The rows discarded, by why and ticker, with their count and reason.
        policy: The stream's resolved policy, whose setting of each cause the event records.

    Returns:
        One event per cause and ticker.
    """
    now = time.time_ns()
    return tuple(
        ProblemEvent(
            cause=DISCARDS[readiness].cause,
            action=DISCARDS[readiness].cause.traits.action,
            dataset=dataset,
            stream_id=stream_id,
            where=where,
            symbol=ticker,
            field=None,
            rows=rows,
            detail=why,
            at=now,
            policy=DISCARDS[readiness].setting(policy),
        )
        for readiness, tickers in discarded.items()
        for ticker, (rows, why) in tickers.items()
    )
