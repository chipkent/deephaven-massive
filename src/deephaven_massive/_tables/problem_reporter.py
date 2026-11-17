"""The session's `ProblemSink`: the log's alarm for each problem, then the problems table.

Every problem a session's transports and tables meet reaches the session's `ProblemReporter`
(`SessionCore.problems`). It does two things:

- **The alarm.** The first problem of each cause on each table logs one WARNING, naming the
  table, the cause, and its first reason, and pointing at `m.problems()`, which lists every
  problem; the problems after it log at DEBUG only, with their details. A cause set to
  `IGNORE` logs only at DEBUG, and one set to `RAISE` too, since the failure it causes logs
  itself. A cause whose source already warns of each episode (`_LOGGED_BY_SOURCE`: a
  connection's outage, a rate-limit pause, a run of failed `poll()` cycles, a table's failure)
  logs at DEBUG here, so one episode never warns once per table.
- **The table.** It queues every problem for the diagnostics writer
  (`Diagnostics.report_problems`), which merges it into `m.problems()`.

It is called on the emitting thread, so its WARNING comes before a `wait=True` read returns or
raises. Thread-safe: one leaf lock guards its set of warned (table, cause) pairs, held only to
read and update it; it logs and queues outside the lock. A caller must not hold
`registry_lock`, since a log handler may do I/O.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Final

from deephaven_massive._enums import OnError
from deephaven_massive._tables.problem_events import ProblemCause, ProblemEvent

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._massive import Redactor
    from deephaven_massive._tables.problem_events import Problem

__all__ = ["ProblemReporter"]

logger = logging.getLogger(__name__)

_LOGGED_BY_SOURCE: Final = frozenset(
    {
        ProblemCause.OUTAGE,
        ProblemCause.QUIET_FEED,
        ProblemCause.OPTIONS_CAP,
        ProblemCause.UNACKNOWLEDGED_TOPICS,
        ProblemCause.RATE_LIMITED,
        ProblemCause.POLL_CYCLE_FAILED,
        ProblemCause.POLL_CYCLE_LATE,
        ProblemCause.SYMBOL_READ_RETRYING,
        ProblemCause.SYMBOL_READ_FAILED,
        ProblemCause.STREAM_FAILED,
    }
)
"""The causes whose source logs its own WARNING once per episode, with what it knows of it:
a connection's outage, silence, cap, or unacknowledged topics, a rate-limit pause, a run of
failed or late `poll()` cycles, a symbol's failed read, and a table's failure."""

_QUIET: Final = frozenset({OnError.IGNORE, OnError.RAISE})
"""The settings whose problems log at DEBUG only: ignored, or failing loudly elsewhere."""


class ProblemReporter:
    """Logs the first problem of each cause on each table, and queues every problem.

    Example:
        ::

            reporter = ProblemReporter(diagnostics.report_problems, Redactor("my-key"))
            reporter([event])  # a WARNING the first time, then the table's row
    """

    __slots__ = ("_lock", "_queue", "_redactor", "_warned")

    def __init__(self, queue: Callable[[Sequence[Problem]], None], redactor: Redactor) -> None:
        """Start with nothing warned.

        Args:
            queue: Queues problems for the diagnostics writer; never blocks.
            redactor: The session's secrets, which no log line may hold.
        """
        self._queue = queue
        self._redactor = redactor
        self._lock = threading.Lock()
        self._warned: set[tuple[int | None, ProblemCause]] = set()

    def __call__(self, problems: Sequence[Problem]) -> None:
        """Log the problems, the first of each table and cause at WARNING, and queue them.

        Args:
            problems: The problems, from one payload, snapshot, or step.
        """
        held = [p for p in problems if isinstance(p, ProblemEvent) or p.spans]
        alarms = [p for p in held if p.cause not in _LOGGED_BY_SOURCE and _policy(p) not in _QUIET]
        with self._lock:
            first = []
            for problem in alarms:
                pair = (problem.stream_id, problem.cause)
                if pair not in self._warned:
                    self._warned.add(pair)
                    first.append(problem)
        clean = self._redactor.received
        for problem in first:
            table = "" if problem.stream_id is None else f" table {problem.stream_id}"
            logger.warning(
                "%s%s: %s: %s; m.problems() lists it and every later %s problem of this "
                "table, which the log shows at DEBUG only",
                problem.dataset,
                table,
                problem.cause.value,
                clean(_detail(problem)),
                problem.cause.value,
            )
        for problem in held:
            logger.debug("%s", clean(repr(problem)))
        self._queue(problems)


def _policy(problem: Problem) -> OnError | None:
    """Return the setting that decides how loudly a problem logs.

    Args:
        problem: An event, or a snapshot that holds a span.

    Returns:
        The event's setting, or the first span's own setting when it has one (a value
        `canonicalize_symbols` refused follows its `on_error`), else the snapshot's.
    """
    if isinstance(problem, ProblemEvent):
        return problem.policy
    span = next(iter(problem.spans.values()))
    return problem.policy if span.policy is None else span.policy


def _detail(problem: Problem) -> str:
    """Return a problem's first reason, for its WARNING.

    Args:
        problem: An event, or a snapshot that holds a span.

    Returns:
        The event's detail after its rows and symbol, or the snapshot's first span's symbol
        and detail.
    """
    if isinstance(problem, ProblemEvent):
        symbol = "" if problem.symbol is None else f" of {problem.symbol}"
        if problem.rows:
            head = f"{problem.rows} rows{symbol}, "
        else:
            head = "" if problem.symbol is None else f"{problem.symbol}: "
        return f"{head}{problem.detail} (at {problem.where})"
    span = next(iter(problem.spans.values()))
    head = "" if span.symbol is None else f"{span.symbol}: "
    return f"{head}{span.detail} (at {problem.where})"
