"""One symbol's run of reads: when a failed symbol is read again, and when it has failed for good.

A read that serves each symbol on its own, such as a `fetch()` or `history()` that follows a
symbol table, must decide what one symbol's failure does without touching the others. The
rule: a failure that may pass (the error's `is_retryable`) is read again 2, 8, and 30 seconds
after the run's first, second, and third failure (`RETRY_DELAYS`), then every `RETRY_EVERY`,
until the run of failures has lasted `MassiveConfig.max_outage`. A failure that lands that
long or longer after the run's first is final, with an error that names the limit
(`outage_failure`). A failure no retry can fix, such as Massive's refusal of the symbol, is
final at once. The limit is a threshold checked as each failure lands, not a wall-clock
bound: the retry delays and each read's own length can carry a symbol past it before the
failure that ends it lands.

A symbol that is retrying or has failed is a span of `m.problems()`, under the cause its state
and error give (`span_cause`): a refusal of the symbol (an `EntitlementError` that is not the
feed's) is `refused_symbols`, whose setting decides how loudly it is logged; any other final
failure is `symbol_read_failed`; a retry is `symbol_read_retrying`.

This module holds that rule as a function over plain values (`next_step`) and the
bookkeeping of one symbol's run (`SymbolRun`). Nothing here waits, reads, or touches a table:
the follow coordinator (`_tables/follow_coordinator.py`) keeps one `SymbolRun` per symbol,
calls it on the one thread that owns the symbol's state, and schedules the retries it decides.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, assert_never

from deephaven_massive._errors import EntitlementError, FeedRefusalError, copy_error
from deephaven_massive._tables.problem_events import ProblemCause
from deephaven_massive._tables.symbol_read_rows import SymbolReadState

if TYPE_CHECKING:
    from deephaven_massive._errors import MassiveError

__all__ = [
    "RETRY_DELAYS",
    "RETRY_EVERY",
    "SPAN_CAUSES",
    "Failed",
    "Retry",
    "SymbolRun",
    "is_symbol_refusal",
    "next_step",
    "outage_failure",
    "span_cause",
]

RETRY_DELAYS: Final = (2.0, 8.0, 30.0)
"""Seconds after a symbol's first, second, and third transient failure before it is read again:
soon, since most such failures (a rate-limit pause, a dropped connection) pass in seconds."""

RETRY_EVERY: Final = 60.0
"""Seconds between a symbol's later reads, until it has failed for `MassiveConfig.max_outage`."""

SPAN_CAUSES: Final = (
    ProblemCause.SYMBOL_READ_RETRYING,
    ProblemCause.SYMBOL_READ_FAILED,
    ProblemCause.REFUSED_SYMBOLS,
)
"""Every cause `span_cause` gives: the spans of `m.problems()` a symbol's run can hold."""


def is_symbol_refusal(error: MassiveError | None) -> bool:
    """Return whether a symbol's failure is Massive's refusal of that symbol.

    Args:
        error: The symbol's failure, or None for none.

    Returns:
        True for an `EntitlementError` that is not the feed's (`FeedRefusalError`, which no
        symbol escapes); False for any other error, and for None.
    """
    return isinstance(error, EntitlementError) and not isinstance(error, FeedRefusalError)


def span_cause(state: SymbolReadState, error: MassiveError | None) -> ProblemCause | None:
    """Return the cause of the span of `m.problems()` a symbol in `state` holds, if any.

    Args:
        state: The symbol's state.
        error: Its latest failure, or None while it has none.

    Returns:
        `symbol_read_retrying` while `RETRYING`; for `FAILED`, `refused_symbols` when Massive
        refused the symbol (`is_symbol_refusal`), else `symbol_read_failed`; None in every
        other state, which holds no span.
    """
    match state:
        case SymbolReadState.RETRYING:
            return ProblemCause.SYMBOL_READ_RETRYING
        case SymbolReadState.FAILED:
            if is_symbol_refusal(error):
                return ProblemCause.REFUSED_SYMBOLS
            return ProblemCause.SYMBOL_READ_FAILED
        case (
            SymbolReadState.QUEUED
            | SymbolReadState.LOADING
            | SymbolReadState.DONE
            | SymbolReadState.REMOVED
            | SymbolReadState.STOPPED
            | SymbolReadState.CLOSED
        ):
            return None
        case _:
            assert_never(state)


@dataclass(frozen=True, slots=True)
class Retry:
    """The step after a failure that may pass: read the symbol again at `due`.

    Attributes:
        due: When the next read is due, monotonic seconds.
        delay: Seconds from the failure to `due`.
        attempts: Reads begun in the run, the failed one included; at least one.
        failing_for: Seconds the run of failures had lasted when the failure landed.
    """

    due: float
    delay: float
    attempts: int
    failing_for: float


@dataclass(frozen=True, slots=True)
class Failed:
    """The step after a failure that ends the run: the symbol has failed for good.

    Attributes:
        error: The symbol's final error: the failure itself, or, for one that may pass but
            landed once the run outlasted `MassiveConfig.max_outage`, the `outage_failure`
            that wraps it.
        attempts: Reads begun in the run, the failed one included; at least one.
    """

    error: MassiveError
    attempts: int


def next_step(
    error: MassiveError,
    *,
    name: str,
    failing_since: float,
    attempts: int,
    limit: float,
    now: float,
) -> Retry | Failed:
    """Decide what a symbol's failure does: a retry at a later time, or its final failure.

    Args:
        error: The failure that just landed.
        name: The symbol, for the final error's message.
        failing_since: When the first failure of its run landed, monotonic seconds; `now`
            for a first failure.
        attempts: Reads begun in the run, this one included; a count below one is read as
            one, so that a failure recorded before any read began still gets the first delay.
        limit: `MassiveConfig.max_outage`, in seconds.
        now: When the failure landed, monotonic seconds.

    Returns:
        `Retry` when the error may pass and the run has lasted less than `limit`: due after
        the delay of `RETRY_DELAYS` for the attempt, or `RETRY_EVERY` after the third. Else
        `Failed`, with the error as it is, or wrapped by `outage_failure` when it may pass but
        the run outlasted `limit`.
    """
    failing_for = now - failing_since
    attempts = max(attempts, 1)
    if error.is_retryable and failing_for < limit:
        delay = RETRY_DELAYS[attempts - 1] if attempts <= len(RETRY_DELAYS) else RETRY_EVERY
        return Retry(now + delay, delay, attempts, failing_for)
    if error.is_retryable:  # final only for its run's length
        error = outage_failure(name, error, failing_for=failing_for, limit=limit)
    return Failed(error, attempts)


def outage_failure(
    name: str, error: MassiveError, *, failing_for: float, limit: float
) -> MassiveError:
    """Return a symbol's final error once its failures have lasted `MassiveConfig.max_outage`.

    A copy of the latest error (`copy_error`: its class, status, `request_id`, source, and
    whether it is retryable or a bug) whose message says how long the symbol failed and the
    limit, then quotes the latest error; chained to it, so its log record carries that error's
    traceback too.

    Args:
        name: The symbol.
        error: Its latest failure, one that may pass.
        failing_for: Seconds since the run's first failure.
        limit: `MassiveConfig.max_outage`, in seconds.

    Returns:
        The error, such as `"AAPL has failed for 340 s, as long as max_outage=300 s allows.
        The latest error: Massive answered ..."`.
    """
    failure = copy_error(
        error,
        f"{name} has failed for {failing_for:.0f} s, as long as max_outage={limit:g} s "
        f"allows. The latest error: {error}",
    )
    failure.__cause__ = error
    return failure


@dataclass(slots=True)
class SymbolRun:
    """The reads of one symbol since it was last queued, and its run of failures, if any.

    One per symbol, owned by the one thread that reads and changes the symbol's state. The
    owner tells it when a read begins (`started`) and how it ended (`succeeded`, `failed`),
    and reads its `state`, `attempts`, `due`, and `cause`; `failed` decides the next step by
    `next_step` and keeps the run's bookkeeping. The owner sets `state` itself for the
    changes no read makes: a symbol removed from its table, or stopped with its result.

    Attributes:
        state: Where the symbol stands (`SymbolReadState`): `QUEUED` until a read begins.
        attempts: Reads begun since the run was made.
        succeeded_before: Reads begun before the current run of failures, which a symbol read
            again and again (a `poll()`) has answered: its retry delays count only the reads of
            the current run.
        failing_since: When the first failure of its current run of failures landed, monotonic
            seconds; None while it has none.
        due: When its next read is due, monotonic seconds, while `RETRYING`; None otherwise.
        error: Its latest failure, as `failed` recorded it (a final one as its `Failed` step
            gives it); None until one, and again once a read succeeds.
    """

    state: SymbolReadState = SymbolReadState.QUEUED
    attempts: int = 0
    succeeded_before: int = 0
    failing_since: float | None = None
    due: float | None = None
    error: MassiveError | None = None

    @property
    def cause(self) -> ProblemCause | None:
        """The cause of the span of `m.problems()` the symbol holds now, or None (`span_cause`)."""
        return span_cause(self.state, self.error)

    def started(self) -> None:
        """A read of the symbol began: count it, and the symbol is `LOADING`."""
        self.attempts += 1
        self.state = SymbolReadState.LOADING

    def succeeded(self) -> None:
        """A read gave the symbol its rows: the symbol is `DONE`, and its run of failures ends."""
        self.state = SymbolReadState.DONE
        self.failing_since = None
        self.due = None
        self.error = None

    def failed(self, error: MassiveError, *, name: str, now: float, limit: float) -> Retry | Failed:
        """A read of the symbol failed: decide its next step, and record it.

        A first failure starts the run of failures at `now`. A `Retry` makes the symbol
        `RETRYING`, due at the step's time; a `Failed` makes it `FAILED`, due never.

        Args:
            error: The failure.
            name: The symbol, for a final error's message.
            now: When the failure landed, monotonic seconds.
            limit: `MassiveConfig.max_outage`, in seconds.

        Returns:
            The step (`next_step`).
        """
        if self.failing_since is None:
            self.failing_since = now
            self.succeeded_before = self.attempts - 1
        step = next_step(
            error,
            name=name,
            failing_since=self.failing_since,
            attempts=self.attempts - self.succeeded_before,
            limit=limit,
            now=now,
        )
        match step:
            case Retry():
                self.state = SymbolReadState.RETRYING
                self.due = step.due
                self.error = error
            case Failed():
                self.state = SymbolReadState.FAILED
                self.due = None
                self.error = step.error
            case _:
                assert_never(step)
        return step
