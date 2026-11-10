"""The timeline of two sessions on one key competing for a WebSocket slot, and its verdict.

The slot probe in `tests/ingest/test_ws_integration.py` streams `X:BTCUSD` in one session,
opens a second session that cannot share the first's connections, and samples both every few
seconds. Each sample holds, per session, its crypto WebSocket connection's `State`,
`Reconnects`, and `AckedTopics` from `m.connections()`, and its stream's `RowsPublished` from
`m.streams()`. Bitcoin trades many times a second, so a session whose rows stop growing has
lost its data.

The verdict names which of four readings the samples support:

- `FIRST_HELD`: the first session kept its connection and its rows never stalled, and the
  second never got a row. Massive refuses the newer connection, as the library assumes.
- `NEWER_EVICTED_OLDER`: the first session reconnected while the second got data. Massive
  keeps the newer connection and drops the older one.
- `PING_PONG`: both sessions reconnected at least twice. Each reconnect evicts the other, so
  two clients of one key take turns for as long as both run.
- `UNCLEAR`: none of these.

Every function is pure; the probe reads the tables and the log records and calls them.
"""

from __future__ import annotations

import dataclasses
import itertools
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    import logging
    from collections.abc import Sequence

FIRST_HELD: Final = "first held, second waited"
"""The verdict when the first session kept streaming and the second never got a row."""

NEWER_EVICTED_OLDER: Final = "newer evicted older"
"""The verdict when the first session reconnected while the second got data."""

PING_PONG: Final = "ping-pong"
"""The verdict when both sessions reconnected more than once."""

UNCLEAR: Final = "unclear"
"""The verdict when the samples fit none of the other readings."""

STALL: Final = 10.0
"""Seconds without a new row after which the first session's stream counts as stalled.
Bitcoin trades many times a second, so a gap this long means the data stopped."""

_NO_CONNECTION: Final = "NONE"
"""The state recorded while a session has no crypto WebSocket connection row."""

_WIRE_EVENTS: Final = ("status", "refused the connection", "authenticated with", "closed (code")
"""Text that marks a wire client's log record as a connection event the timeline keeps: a
status message, a `max_connections` refusal, an authentication, or a close."""


@dataclasses.dataclass(frozen=True)
class Side:
    """One session's connection and stream at one sample.

    Attributes:
        state: The crypto WebSocket connection's `State`, or `NONE` when it has no row.
        reconnects: The connection's `Reconnects`: authenticated sockets after its first.
        acked: The connection's `AckedTopics`.
        rows: The `RowsPublished` of the session's streams, summed.
    """

    state: str
    reconnects: int
    acked: int
    rows: int


@dataclasses.dataclass(frozen=True)
class Sample:
    """Both sessions at one moment of the window.

    Attributes:
        seconds: Seconds since the window opened.
        first: The session that streamed first.
        second: The session opened after it.
    """

    seconds: float
    first: Side
    second: Side


def side(connections: Sequence[dict[str, Any]], streams: Sequence[dict[str, Any]]) -> Side:
    """Return one session's side of a sample from its diagnostics rows.

    Args:
        connections: The rows of the session's `m.connections()`.
        streams: The rows of the session's `m.streams()`.

    Returns:
        The crypto WebSocket connection's state, reconnects, and acked topics (`NONE`, 0, 0
        without one), and the streams' published rows.
    """
    rows = sum(int(r["RowsPublished"]) for r in streams)
    for r in connections:
        if r["Transport"] == "WebSocket" and r["AssetClass"] == "crypto":
            return Side(str(r["State"]), int(r["Reconnects"]), int(r["AckedTopics"]), rows)
    return Side(_NO_CONNECTION, 0, 0, rows)


def changes(samples: Sequence[Sample]) -> list[str]:
    """Return a line for each sample that differs from the one before, and for the last.

    A sample differs when either session's state, reconnects, or acked count changed, or when
    its rows started or stopped growing; rows growing at a steady pace are not a change, which
    keeps the timeline short.

    Args:
        samples: The window's samples, in order.

    Returns:
        Lines such as `  12.0 s  first CONNECTED r=0 acked=1 rows=180+ | second ...`, where a
        `+` marks rows that grew since the sample before.
    """
    lines: list[str] = []
    before: Sample | None = None
    shown: tuple[object, ...] | None = None
    for index, sample in enumerate(samples):
        first_grew = before is not None and sample.first.rows > before.first.rows
        second_grew = before is not None and sample.second.rows > before.second.rows
        key = (
            _without_rows(sample.first),
            first_grew,
            _without_rows(sample.second),
            second_grew,
        )
        if key != shown or index == len(samples) - 1:
            lines.append(
                f"{sample.seconds:6.1f} s  first {_shown(sample.first, grew=first_grew)} "
                f"| second {_shown(sample.second, grew=second_grew)}"
            )
            shown = key
        before = sample
    return lines


def verdict(samples: Sequence[Sample]) -> str:
    """Return the reading the samples support: one of the four verdicts of this module.

    Reconnects count from the first sample, so a reconnect before the window does not count.
    The checks run from the most specific reading to the least: `PING_PONG`, then
    `NEWER_EVICTED_OLDER`, then `FIRST_HELD`.

    Args:
        samples: The window's samples, in order.

    Returns:
        `PING_PONG` when both sessions' reconnects grew by at least two;
        `NEWER_EVICTED_OLDER` when the first's grew and the second published a row;
        `FIRST_HELD` when the first never reconnected, its rows grew with no gap of `STALL`
        seconds, and the second published no row; and `UNCLEAR` otherwise, an empty window
        included.
    """
    if not samples:
        return UNCLEAR
    start, end = samples[0], samples[-1]
    first_reconnects = end.first.reconnects - start.first.reconnects
    second_reconnects = end.second.reconnects - start.second.reconnects
    second_got_data = any(s.second.rows > 0 for s in samples)
    if first_reconnects >= 2 and second_reconnects >= 2:
        return PING_PONG
    if first_reconnects > 0 and second_got_data:
        return NEWER_EVICTED_OLDER
    if first_reconnects == 0 and not second_got_data and _never_stalled(samples):
        return FIRST_HELD
    return UNCLEAR


def wire_events(records: Sequence[logging.LogRecord]) -> list[str]:
    """Return a line for each connection event among a wire client's log records.

    Args:
        records: Log records of the wire client's logger, in order.

    Returns:
        Lines such as `16:57:53.120Z massive group #2: ... status MAX_CONNECTIONS: ...`, one
        per status message, `max_connections` refusal, authentication, or close; the thread
        name before ` ws ` names the session's connection group, so the lines of the two
        sessions tell apart.
    """
    lines = []
    for record in records:
        message = record.getMessage()
        if any(marker in message for marker in _WIRE_EVENTS):
            at = datetime.fromtimestamp(record.created, UTC).strftime("%H:%M:%S.%f")[:-3]
            group = record.threadName.split(" ws ")[0] if record.threadName else "?"
            lines.append(f"{at}Z {group}: {message}")
    return lines


def _without_rows(each: Side) -> tuple[str, int, int]:
    """Return what of a side counts as a change by itself: everything but its rows."""
    return each.state, each.reconnects, each.acked


def _shown(each: Side, *, grew: bool) -> str:
    """Return a side as timeline text, its rows marked `+` when they grew."""
    mark = "+" if grew else ""
    return f"{each.state} r={each.reconnects} acked={each.acked} rows={each.rows}{mark}"


def _never_stalled(samples: Sequence[Sample]) -> bool:
    """Return whether the first session's rows grew, never going `STALL` seconds without one."""
    last_growth = samples[0].seconds
    for before, sample in itertools.pairwise(samples):
        if sample.first.rows > before.first.rows:
            last_growth = sample.seconds
        elif sample.seconds - last_growth > STALL:
            return False
    return samples[-1].first.rows > samples[0].first.rows
