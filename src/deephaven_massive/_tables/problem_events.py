"""A problem with the feed, as plain data: its cause, what the library did, where, and why.

Every part of the library that meets a problem with a table's data (a transport withholding
rows it cannot read whole, a stream's connection going down, a watchlist row that is not
Massive's ticker) describes it as plain data and hands it to its session's `ProblemSink`
(`SessionCore.problems`), which queues it for the diagnostics writer. The writer merges what
it receives into the rows of `m.problems()` (`problem_ledger.py`). Nothing here holds a
Deephaven object, a lock, or a secret, so any thread may build it and any thread may hand it on.

Two shapes cover every problem:

- `ProblemEvent`: something that happened, such as rows withheld from one payload. Events of
  one key (table, cause, symbol, field) add up in one row of `m.problems()`.
- `ProblemSpans`: the problems of one cause that hold now for one table, such as the
  watchlist values it skips or its connection's outage. Each is a span, named by its start;
  the writer opens a row for each new one and ends (`EndedAt`) each one the next snapshot no
  longer holds. Its emitter sends a new snapshot each time the set changes, never a diff.

`ProblemCause` names each cause, the `Cause` column, and `CAUSES` declares its traits: the
`dm.ErrorPolicy` field it follows (or none), whose problem it is (`ProblemSource`), what the
library does about it (`ProblemAction`), and whether it is a span. A new cause is a member and
its `CAUSES` entry; the docs table of causes is rendered from `CAUSES`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven_massive._enums import OnError
    from deephaven_massive._massive import DriftReport

__all__ = [
    "CAUSES",
    "EVENTS_PER_PAYLOAD",
    "CauseTraits",
    "Problem",
    "ProblemAction",
    "ProblemCause",
    "ProblemEvent",
    "ProblemSink",
    "ProblemSource",
    "ProblemSpans",
    "Span",
    "SpanTracker",
    "drift_events",
]

EVENTS_PER_PAYLOAD: Final = 1000
"""The most events one payload gives, one per cause, symbol, and field; the rest of each cause
is one `ProblemAction.OVERFLOW` event."""


class ProblemAction(StrEnum):
    """What the library did about a problem: the `Action` column of `m.problems()`."""

    WITHHELD = "WITHHELD"
    """Rows of Massive's data were left out of the table: the library could not read them
    whole."""
    SKIPPED = "SKIPPED"
    """A symbol, venue, or file line was left out, and the rest of the table goes on."""
    FAILED = "FAILED"
    """A read, a symbol's read, or a table failed."""
    GAP = "GAP"
    """Rows may be missing over the span: the data did not flow."""
    DELAYED = "DELAYED"
    """Rows arrive late, or not yet, and are not lost."""
    PUBLISHED = "PUBLISHED"
    """The rows were published, but Massive's data differed from its documentation."""
    OVERFLOW = "OVERFLOW"
    """More distinct problems of the cause than the table keeps for one table: this row counts
    the rest."""


class ProblemSource(StrEnum):
    """Whose problem it is: the `Source` column of `m.problems()`."""

    MASSIVE = "massive"
    """Massive's answer: what it sent, refused, or has no data for."""
    USER = "user"
    """The caller's input, such as a watchlist value that is not Massive's ticker."""
    LIBRARY = "library"
    """The library: what it could not read, reach, or keep up with."""


class ProblemCause(StrEnum):
    """Why there is a problem: the `Cause` column of `m.problems()`.

    A member named for a `dm.ErrorPolicy` field is the problem that field decides; the others
    have no setting. `CAUSES` holds each member's traits.
    """

    NONCANONICAL_SYMBOLS = "noncanonical_symbols"
    UNREADABLE_SYMBOLS = "unreadable_symbols"
    UNAVAILABLE_SYMBOLS = "unavailable_symbols"
    UNREQUESTED_SYMBOLS = "unrequested_symbols"
    UNREADABLE_KEYS = "unreadable_keys"
    UNCONVERTIBLE_VALUES = "unconvertible_values"
    MALFORMED_ROWS = "malformed_rows"
    REFUSED_VENUES = "refused_venues"
    UNLISTED_CONTRACTS = "unlisted_contracts"
    UNSCHEDULED_PRODUCTS = "unscheduled_products"
    UNDATED_ROWS = "undated_rows"
    STALE_SYMBOL_DATA = "stale_symbol_data"
    SCHEMA_DRIFT = "schema_drift"
    OUTAGE = "outage"
    QUIET_FEED = "quiet_feed"
    OPTIONS_CAP = "options_cap"
    UNACKNOWLEDGED_TOPICS = "unacknowledged_topics"
    RATE_LIMITED = "rate_limited"
    POLL_CYCLE_FAILED = "poll_cycle_failed"
    POLL_CYCLE_LATE = "poll_cycle_late"
    SYMBOL_READ_RETRYING = "symbol_read_retrying"
    SYMBOL_READ_FAILED = "symbol_read_failed"
    STREAM_FAILED = "stream_failed"

    @property
    def traits(self) -> CauseTraits:
        """The cause's policy field, source, action, and whether it is a span (`CAUSES`)."""
        return CAUSES[self]


@dataclass(frozen=True, kw_only=True, slots=True)
class CauseTraits:
    """What one `ProblemCause` is.

    Attributes:
        policy: The `dm.ErrorPolicy` field that decides it, or None when no setting does.
        source: Whose problem it is.
        action: What the library does about it.
        span: True when it lasts until its condition clears, with an `EndedAt`.
        summary: What happened, for the docs table of causes.
        fix: What the user does about it, for the same table.
    """

    policy: str | None
    source: ProblemSource
    action: ProblemAction
    span: bool
    summary: str
    fix: str


_C = ProblemCause
_MASSIVE, _USER, _LIBRARY = ProblemSource.MASSIVE, ProblemSource.USER, ProblemSource.LIBRARY

CAUSES: Final[Mapping[ProblemCause, CauseTraits]] = MappingProxyType(
    {
        _C.NONCANONICAL_SYMBOLS: CauseTraits(
            policy="noncanonical_symbols",
            source=_USER,
            action=ProblemAction.SKIPPED,
            span=True,
            summary="A value of a watchlist or other table you pass isn't Massive's exact ticker, "
            "or `dm.canonicalize_symbols` couldn't convert it, so its row is skipped. A "
            "`fetch()`, `history()`, or stream of a static table lists it once.",
            fix="Fix or delete the row. Massive's lists, such as `m.reference.tickers()`, show "
            "its spelling.",
        ),
        _C.UNREADABLE_SYMBOLS: CauseTraits(
            policy="unreadable_symbols",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive sent a row whose symbol the library can't read as Massive's ticker. "
            "The row is left out.",
            fix="The library withheld the row because Massive sent it in a form the library "
            "can't read. `Detail` gives what Massive sent and why, for a report to Massive. "
            "Set `unreadable_symbols` to `dm.OnError.IGNORE` to quiet it, or to "
            "`dm.OnError.RAISE` to fail the table.",
        ),
        _C.UNAVAILABLE_SYMBOLS: CauseTraits(
            policy="unavailable_symbols",
            source=_MASSIVE,
            action=ProblemAction.SKIPPED,
            span=False,
            summary="Massive has no data for the symbol. It answered HTTP 404, or the pair isn't "
            "on its crypto ticker list.",
            fix="Look the symbol up in Massive's lists, such as `m.reference.tickers()`.",
        ),
        _C.UNREQUESTED_SYMBOLS: CauseTraits(
            policy="unrequested_symbols",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive answered a call's request with a row of a symbol the call "
            "didn't ask for. The row is left out.",
            fix="Nothing, if the row is Massive's answer about another instrument. To read "
            "that symbol, name it in the call or add it to the watchlist. Set "
            "`unrequested_symbols` to "
            "`dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the table.",
        ),
        _C.UNREADABLE_KEYS: CauseTraits(
            policy="unreadable_keys",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive sent a row whose symbol, time, or other key the library can't read. "
            "The row is left out.",
            fix="The library withheld the row because Massive sent it in a form the library "
            "can't read. `Detail` gives what Massive sent and why, for a report to Massive. "
            "Set `unreadable_keys` to `dm.OnError.IGNORE` to quiet it, or to "
            "`dm.OnError.RAISE` to fail the table.",
        ),
        _C.UNCONVERTIBLE_VALUES: CauseTraits(
            policy="unconvertible_values",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive sent a row with a value the library can't read as its column's type. "
            "The row is left out.",
            fix="The library withheld the row because Massive sent it in a form the library "
            "can't read. `Detail` gives what Massive sent and why, for a report to Massive. "
            "Set `unconvertible_values` to `dm.OnError.IGNORE` to quiet it, or to "
            "`dm.OnError.RAISE` to fail the table.",
        ),
        _C.MALFORMED_ROWS: CauseTraits(
            policy="malformed_rows",
            source=_MASSIVE,
            action=ProblemAction.SKIPPED,
            span=False,
            summary="A line of a flat file isn't a row the library can read. The line is left out.",
            fix="The library skipped the line because Massive sent it in a form the library "
            "can't read. `Detail` names the file and its first such line, for a report to "
            "Massive. Set `malformed_rows` to `dm.OnError.IGNORE` to quiet it, or to "
            "`dm.OnError.RAISE` to fail the read.",
        ),
        _C.REFUSED_VENUES: CauseTraits(
            policy="refused_venues",
            source=_MASSIVE,
            action=ProblemAction.SKIPPED,
            span=False,
            summary="Massive refused the flat files of a futures exchange the call named "
            "(HTTP 403).",
            fix="Name in `venues` only the exchanges your plan includes.",
        ),
        _C.UNLISTED_CONTRACTS: CauseTraits(
            policy="unlisted_contracts",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive streamed rows of a futures contract missing from its snapshot and "
            "contracts list for 30 minutes. The library can't date them, so it leaves them out.",
            fix="Look the contract up with `m.futures.contracts()`. If Massive lists it, report "
            "a [bug](#report-a-bug) with the row's `Detail`.",
        ),
        _C.UNSCHEDULED_PRODUCTS: CauseTraits(
            policy="unscheduled_products",
            source=_MASSIVE,
            action=ProblemAction.WITHHELD,
            span=False,
            summary="Massive's futures schedules give no trading session for rows it streamed, "
            "so the library can't date them. The rows are left out.",
            fix="Report it as a [bug](#report-a-bug) with the row's `Detail`, so it can go to "
            "Massive.",
        ),
        _C.UNDATED_ROWS: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.DELAYED,
            span=True,
            summary="A futures contract's rows have waited over 15 seconds for its trading "
            "session from Massive's schedules. They publish, dated, once it loads.",
            fix="Nothing, unless it lasts. The table fails once the wait passes "
            "[`max_outage`](configuration.md#settings-reference).",
        ),
        _C.STALE_SYMBOL_DATA: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.GAP,
            span=True,
            summary="The last 100 rows of a symbol were all left out, so its latest values may be "
            "stale. It ends when a row of the symbol is published.",
            fix="The symbol's `WITHHELD` rows in this table say why its rows were left out. Do "
            "what their cause says.",
        ),
        _C.SCHEMA_DRIFT: CauseTraits(
            policy=None,
            source=_MASSIVE,
            action=ProblemAction.PUBLISHED,
            span=False,
            summary="Massive sent a field its documentation lacks. The rows are published without "
            "it.",
            fix="Report it as a [bug](#report-a-bug), so the library can add the field. The "
            "rows are fine.",
        ),
        _C.OUTAGE: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.GAP,
            span=True,
            summary="The connection was down, so rows may be missing.",
            fix="Nothing. The library reconnects, and the table fails only after "
            "[`max_outage`](configuration.md#settings-reference). "
            "If `Detail` says another program took the connection, see "
            "[Live tables](#live-table-messages).",
        ),
        _C.QUIET_FEED: CauseTraits(
            policy=None,
            source=_MASSIVE,
            action=ProblemAction.DELAYED,
            span=True,
            summary="The connection received nothing for a while during trading hours.",
            fix="Nothing, unless it lasts. `m.connections()` shows the connection.",
        ),
        _C.OPTIONS_CAP: CauseTraits(
            policy=None,
            source=_MASSIVE,
            action=ProblemAction.DELAYED,
            span=True,
            summary="The table has more contracts than Massive's cap of 1,000 per connection, "
            "counting a contract's trades and quotes as two. The contracts past the cap wait, "
            "and `Rows` counts them.",
            fix="Pass fewer contracts. The waiting ones subscribe as others leave.",
        ),
        _C.UNACKNOWLEDGED_TOPICS: CauseTraits(
            policy=None,
            source=_MASSIVE,
            action=ProblemAction.DELAYED,
            span=True,
            summary="Massive has not confirmed some of the table's subscriptions, so the library "
            "keeps sending them.",
            fix="Nothing, unless it lasts. Then report a [bug](#report-a-bug) with the row's "
            "`Detail`.",
        ),
        _C.RATE_LIMITED: CauseTraits(
            policy=None,
            source=_MASSIVE,
            action=ProblemAction.DELAYED,
            span=True,
            summary="Massive refused a request for coming too fast (HTTP 429), so the requests "
            "of that Massive subscription wait until the pause ends.",
            fix="Nothing: the requests go on after the pause. `m.rate_limits()` shows it.",
        ),
        _C.POLL_CYCLE_FAILED: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.GAP,
            span=True,
            summary="`poll()` cycles failed, so rows may be missing. It ends with the next good "
            "cycle.",
            fix="Fix what `Detail` names. The poll keeps trying, and the table fails once its "
            "cycles have failed for [`max_outage`](configuration.md#settings-reference).",
        ),
        _C.POLL_CYCLE_LATE: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.DELAYED,
            span=True,
            summary="`poll()` cycles take longer than the interval, so the table updates late.",
            fix="Poll fewer symbols, or pass `poll()` a longer interval.",
        ),
        _C.SYMBOL_READ_RETRYING: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.DELAYED,
            span=True,
            summary="A symbol's read failed in a way that may pass, so the library tries it "
            "again. A read that waits on Massive's crypto ticker list is one of these.",
            fix="Nothing, unless it lasts. Then fix what `Detail` names.",
        ),
        _C.SYMBOL_READ_FAILED: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.FAILED,
            span=True,
            summary="A watchlist symbol's read failed and won't be tried again.",
            fix="Fix what `Detail` names. Then delete the symbol's watchlist row and add it back "
            "to read it again.",
        ),
        _C.STREAM_FAILED: CauseTraits(
            policy=None,
            source=_LIBRARY,
            action=ProblemAction.FAILED,
            span=False,
            summary="The table failed. `Detail` and its `Error` in `m.streams()` say why.",
            fix="Fix what `Detail` names. Then run the line that made the table again.",
        ),
    }
)
"""Each cause's traits, in the order of the docs table."""


@dataclass(frozen=True, kw_only=True, slots=True)
class ProblemEvent:
    """Something that happened to one table or read: rows of one cause, symbol, and field.

    Attributes:
        cause: The cause.
        action: What the library did: the cause's own action, or `ProblemAction.OVERFLOW` for
            the rest of a payload's problems past `EVENTS_PER_PAYLOAD`.
        dataset: The dataset, such as `"stocks.trades"`.
        stream_id: The table's `StreamId` in `m.streams()`, or a `wait=True` read's call id;
            None for a problem of no table or read.
        where: The request, the WebSocket URL and channel, or the flat file, as the transport
            names it in its log lines, with no secret; for the log only.
        symbol: The symbol the problem concerns, or None: exactly as Massive sent it for a row
            the library could not read, as the table would name it for a row it read whole (an
            `unrequested_symbols` row), or as the caller gave it.
        field: The column the problem concerns, or None.
        rows: How many rows the event counts; 0 for a problem of no rows.
        detail: Why, in one line: the field, the expected type, and the value's first 200
            characters, the derivation that failed, or Massive's answer. The writer removes
            the session's secrets from it.
        at: When it happened, epoch nanoseconds.
        policy: The setting of the cause's `dm.ErrorPolicy` field that applied, or None for a
            cause without one.
    """

    cause: ProblemCause
    action: ProblemAction
    dataset: str
    stream_id: int | None
    where: str
    symbol: str | None
    field: str | None
    rows: int
    detail: str
    at: int
    policy: OnError | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class Span:
    """One span problem that holds now.

    Attributes:
        symbol: The symbol it concerns, as Massive sent it or the caller gave it, or None.
        field: The column it concerns, or None.
        detail: Why, in one line, as `ProblemEvent.detail`.
        since: When it began, epoch nanoseconds: the span's start, which names it.
        rows: How many rows or contracts it holds back now, such as the contracts the options
            cap defers; 0 when it counts none.
        policy: The setting that applied to this span when it differs from its snapshot's
            (`ProblemSpans.policy`), such as the `on_error` of `canonicalize_symbols` for a
            value it refused; None to take the snapshot's.
    """

    symbol: str | None = None
    field: str | None = None
    detail: str
    since: int
    rows: int = 0
    policy: OnError | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class ProblemSpans:
    """Every span problem of one cause that holds now for one table.

    It replaces the table's previous snapshot of the cause: a span it no longer holds has
    ended, at `at`. Each span has a name its emitter gives it, unique in the snapshot (the
    symbol, the bucket), which ties it to the same span in the next snapshot.

    Attributes:
        cause: The cause, a span cause (`CauseTraits.span`).
        dataset: The dataset, or None for a problem of the session (a rate-limit pause).
        stream_id: The table's `StreamId`, or None for a problem of the session.
        where: Where the problem was found, for the log only.
        spans: Each span that holds, by its name; empty when none does.
        at: When the snapshot was taken, epoch nanoseconds.
        policy: The setting of the cause's `dm.ErrorPolicy` field, or None.
    """

    cause: ProblemCause
    dataset: str | None
    stream_id: int | None
    where: str
    spans: Mapping[str, Span]
    at: int
    policy: OnError | None = None


Problem: TypeAlias = "ProblemEvent | ProblemSpans"
"""What a `ProblemSink` receives."""

ProblemSink: TypeAlias = "Callable[[Sequence[Problem]], None]"
"""Receives problems. It never blocks and calls no Java; it logs, so its caller holds no
`registry_lock` (`problem_reporter.ProblemReporter`)."""


class SpanTracker:
    """An emitter's spans of one cause for one table: each span's start, kept across changes.

    The emitter tells it what holds now, as often as that may have changed; it returns a
    snapshot only when something did, giving a span that still holds the start it had, and
    one that holds anew the time of the call. Not thread-safe: one emitter's thread (or lock)
    owns it.

    Example:
        ::

            tracker = SpanTracker(
                ProblemCause.NONCANONICAL_SYMBOLS,
                dataset="stocks.trades",
                stream_id=7,
                where="stream 7",
            )
            spans = tracker.update({"aapl": ("aapl", None, "not Massive's ticker", 0)})
            if spans is not None:
                core.problems([spans])
    """

    __slots__ = ("_cause", "_dataset", "_held", "_policy", "_stream_id", "_where")

    def __init__(
        self,
        cause: ProblemCause,
        *,
        dataset: str | None,
        stream_id: int | None,
        where: str,
        policy: OnError | None = None,
    ) -> None:
        """Start with no span.

        Args:
            cause: The cause, a span cause.
            dataset: The dataset, or None for a problem of the session.
            stream_id: The table, or None for a problem of the session.
            where: Where the spans are found, for the log only.
            policy: The setting of the cause's `dm.ErrorPolicy` field, or None.
        """
        self._cause = cause
        self._dataset = dataset
        self._stream_id = stream_id
        self._where = where
        self._policy = policy
        self._held: dict[str, Span] = {}

    @property
    def holds(self) -> bool:
        """Whether some span holds now."""
        return bool(self._held)

    def update(
        self,
        current: Mapping[str, tuple[str | None, str | None, str, int]],
        *,
        at: int | None = None,
        since: int | None = None,
        policies: Mapping[str, OnError] | None = None,
    ) -> ProblemSpans | None:
        """Take what holds now; return the snapshot when it changed.

        Args:
            current: Each span that holds, by its name: its symbol, field, detail, and rows.
            at: When, epoch nanoseconds; now when None.
            since: When a span that holds anew began, epoch nanoseconds, when the emitter
                knows it better than `at` (an outage's last frame); `at` when None.
            policies: The setting of each span, by name, that differs from the tracker's own.

        Returns:
            The snapshot, or None when every span and its detail and rows are as before.
        """
        now = time.time_ns() if at is None else at
        held: dict[str, Span] = {}
        for name, (symbol, field, detail, rows) in current.items():
            was = self._held.get(name)
            start = was.since if was is not None else (now if since is None else since)
            policy = None if policies is None else policies.get(name)
            held[name] = Span(
                symbol=symbol, field=field, detail=detail, since=start, rows=rows, policy=policy
            )
        if held == self._held:
            return None
        self._held = held
        return ProblemSpans(
            cause=self._cause,
            dataset=self._dataset,
            stream_id=self._stream_id,
            where=self._where,
            spans=dict(held),
            at=now,
            policy=self._policy,
        )


def drift_events(
    drift: DriftReport,
    *,
    dataset: str,
    stream_id: int | None,
    where: str,
    symbol: str | None = None,
) -> tuple[ProblemEvent, ...]:
    """Return a payload's `schema_drift` events: one per field Massive's documentation lacks.

    Only an extra field is a problem: its value is dropped from the rows. A documented field a
    payload lacks is Massive's null, and a value that does not fit withholds its row, which
    its own cause reports.

    Args:
        drift: The payload's drift.
        dataset: The dataset.
        stream_id: The table or read that received the payload, or None.
        where: Where the payload came from, for the log only.
        symbol: The payload's symbol when its samples cannot say (a REST request for one
            symbol), or None.

    Returns:
        The events, by field name; none when no field is extra.
    """
    at = time.time_ns()
    events = []
    for key in sorted(drift.extra):
        sample = drift.samples.get(key)
        seen = ""
        sent = symbol if sample is None or sample.symbol is None else sample.symbol
        if sent is not None:
            seen += f" It was first sent for {sent}."
        if sample is not None and sample.value is not None:
            seen += f" Its first value: {sample.value}."
        events.append(
            ProblemEvent(
                cause=ProblemCause.SCHEMA_DRIFT,
                action=ProblemAction.PUBLISHED,
                dataset=dataset,
                stream_id=stream_id,
                where=where,
                symbol=None,
                field=key,
                rows=0,
                detail=f"Massive sent the field {key!r}, which its documentation lacks, so the "
                f"rows are published without it.{seen}",
                at=at,
            )
        )
    return tuple(events)
