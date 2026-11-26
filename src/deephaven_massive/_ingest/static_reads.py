"""A static `fetch()` or REST `history()` of symbols in code: one table, each failure its own.

A static table of values, passed where a dataset takes one value, is a fixed list too: its
values are read by value (`RestTransport.fetch_values`) and judged as symbols are
(`judge_failures`).

The read makes the requests a read by symbol makes and gives each request's rows to the
symbols it named (`symbol_reads.read_symbol_list`), so one symbol's failure
never takes another's rows: its read goes on, and the failed symbol is left out of the table
and listed in `m.problems()`:

- **Massive refused it** (`symbol_retry.is_symbol_refusal`): a `refused_symbols` problem, whose
  setting decides only whether the call raises.
- **Any other failure of its own** (a 5xx after the client's retries, a timeout, data cut
  short, a crypto pair whose lookup failed): a `symbol_read_failed` problem. A static call
  has no later read to try again, so the failure is final for that call.

Both are spans of the call that end when it returns. The call raises instead of returning:

- when every symbol it named failed (`every_symbol_failed`): no symbol got an answer. Each
  is listed first, since the error gives only the first symbol's reason;
- for a failure of the whole read (`symbol_reads.fails_followed_read`: a rejected key, a refusal
  of the feed such as a `DELAYED` label on the subscription the request is charged to, a closed
  session), which the reader raises at once, since no symbol escapes it;
- for a failure once the read's stop token is set: its table is gone, so the failure is the
  read's, not its symbol's, and no symbol is listed;
- for a failure under a cause set to `RAISE`: `refused_symbols`, `unavailable_symbols`
  (`UnavailableSymbolError`), and a withheld or unrequested row's cause (`RowDataError`), after
  every failed symbol is listed.

A read whose every row was withheld fails at its entry point (`ReadTally.check`).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from deephaven_massive._errors import every_symbol_failed, failure_text
from deephaven_massive._ingest.symbol_reads import read_symbol_list
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._tables.call_failure import fails_call, refusal_raised
from deephaven_massive._tables.problem_events import ProblemCause, ProblemSpans, SpanTracker
from deephaven_massive._tables.symbol_retry import is_symbol_refusal

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import pyarrow as pa

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._errors import MassiveError
    from deephaven_massive._ingest.rest import RestReader
    from deephaven_massive._ingest.symbol_reads import RestUnit
    from deephaven_massive._massive import Cancel, Patience
    from deephaven_massive._tables.read_tally import ReadTally

__all__ = ["judge_failures", "read_symbols", "report_failures"]

logger = logging.getLogger(__name__)


def read_symbols(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    symbols: Sequence[str],
    units: Sequence[RestUnit],
    failed: Mapping[str, MassiveError],
    window: HistoryWindow | None,
    current: bool,
    limit: int | None,
    cancel: Cancel | None,
    patience: Patience,
    tally: ReadTally,
) -> tuple[pa.Table, int]:
    """Make a static read's planned requests and return the rows of every symbol that did not fail.

    `limit` caps each symbol's rows where every request names one symbol; otherwise it caps
    the total, and the requests are sent one at a time, each asking only for the rows still
    wanted, until Massive has sent `limit` rows (`_send_capped`).

    Args:
        reader: The reader of the session's paths, with the call's policy and stop token.
        spec: The dataset.
        request: The normalized arguments.
        symbols: The canonical symbols the call named.
        units: The planned requests (`symbol_reads.fetch_units`, `history_units`).
        failed: The symbols that failed before any request, with their errors.
        window: The `history()` window whose rows to keep, or None for `fetch()`.
        current: True when the pages are checked for a `DELAYED` label.
        limit: `Request.limit` for `fetch()`, None for `history()`.
        cancel: The read's stop token, or None.
        patience: The patience of the whole read (`RestReader.patience`), shared by every
            request.
        tally: The whole read's count of its rows and of those withheld, which the read's
            entry point checks (`ReadTally.check`).

    Returns:
        The rows of every symbol that did not fail, an Arrow table of the dataset's schema in
        plan order and then page order, a failed symbol's rows from its other requests left
        out too, and the number of requests Massive answered.

    Raises:
        MassiveError: As the module docstring lists.
    """
    ordered = tuple(dict.fromkeys(symbols))
    read = read_symbol_list(
        reader,
        spec,
        request,
        symbols=ordered,
        units=units,
        failed=failed,
        window=window,
        current=current,
        limit=limit,
        cancel=cancel,
        patience=patience,
        tally=tally,
    )
    judge_failures(reader, spec, symbols=ordered, failures=read.failures, cancel=cancel)
    return read.rows, read.requests


def judge_failures(
    reader: RestReader,
    spec: DatasetSpec,
    *,
    symbols: Sequence[str],
    failures: Mapping[str, MassiveError],
    cancel: Cancel | None,
) -> None:
    """End a static read of a fixed list whose members failed alone: list each, raise as needed.

    The last step of every static read of a fixed list: symbols in code, and the values of a
    static table passed where a dataset takes one value, which are judged as symbols are.

    Args:
        reader: The reader, whose problem sink, policy, and call id the spans take.
        spec: The dataset.
        symbols: The call's symbols or values, each once, in the order the spans follow.
        failures: Each failed one's error.
        cancel: The read's stop token, or None.

    Raises:
        MassiveError: The first failure when the stop token is set: the read is over, not the
            symbol. Else, once each failure is listed, the failure `report_failures` raises,
            or the error of `every_symbol_failed` when every symbol failed.
    """
    if failures and cancel is not None and cancel.is_set():
        # A crypto pair's lookup the stop ended: the read is over, not the pair.
        stopped = next(iter(failures.values()))
        raise stopped
    report_failures(reader, spec, symbols=symbols, failures=failures)
    if symbols and all(symbol in failures for symbol in symbols):
        logger.debug("%s: every symbol failed: %s", spec.name, bounded_list(symbols))
        error = every_symbol_failed([failures[symbol] for symbol in symbols])
        raise error from failures[symbols[0]]


def report_failures(
    reader: RestReader,
    spec: DatasetSpec,
    *,
    symbols: Sequence[str],
    failures: Mapping[str, MassiveError],
    named: bool = True,
) -> None:
    """List each failed symbol in `m.problems()`, and raise a failure the call must not survive.

    A refused symbol is a `refused_symbols` span; any other failure of its own, but one that
    raises, a `symbol_read_failed` span. Each span starts and ends now: it is the call's, and
    the call is over. A symbol Massive has no data for was listed when its request was
    answered (`RestReader.read_each`), and a withheld or unrequested row when its page was.

    Args:
        reader: The reader, whose problem sink, policy, and call id the spans take.
        spec: The dataset.
        symbols: The call's symbols, in the order the spans follow.
        failures: Each failed symbol's error.
        named: Whether the call named `symbols`; False for the tickers of a read that named
            none (`dm.ALL`), which the call's refusal never tells the user to remove.

    Raises:
        MassiveError: Once every failed symbol is listed, the first symbol's failure, in the
            call's order, that the call must not survive: a refused symbol under
            `refused_symbols=RAISE`, an `UnavailableSymbolError` (only under
            `unavailable_symbols=RAISE`), or a `RowDataError` (only under its cause's `RAISE`).
            A refused symbol's, while another symbol did not fail, ends with the setting that
            reads the others (`refusal_raised`).
    """
    spans: dict[ProblemCause, dict[str, tuple[str | None, str | None, str, int]]] = {
        ProblemCause.REFUSED_SYMBOLS: {},
        ProblemCause.SYMBOL_READ_FAILED: {},
    }
    fatal: list[MassiveError] = []
    others = not named or not all(symbol in failures for symbol in symbols)
    for symbol in symbols:
        error = failures.get(symbol)
        if error is None:
            continue
        refused = is_symbol_refusal(error)
        if fails_call(error, reader.policy):
            raised = refused and others
            fatal.append(refusal_raised(error, symbol if named else None) if raised else error)
            if not refused:
                continue  # listed where it was found
        cause = ProblemCause.REFUSED_SYMBOLS if refused else ProblemCause.SYMBOL_READ_FAILED
        spans[cause][symbol] = (symbol, None, failure_text(error), 0)
    at = time.time_ns()
    found: list[ProblemSpans] = []
    for cause, held in spans.items():
        if not held:
            continue
        policy = reader.policy.refused_symbols if cause is ProblemCause.REFUSED_SYMBOLS else None
        tracker = SpanTracker(
            cause, dataset=spec.name, stream_id=reader.stream_id, where=spec.name, policy=policy
        )
        found.extend(
            snapshot
            for snapshot in (tracker.update(held, at=at), tracker.update({}, at=at))
            if snapshot is not None
        )
    if found:
        reader.problems(found)
    if fatal:
        logger.debug("%s: the read fails for %s", spec.name, fatal[0])
        raise fatal[0]
