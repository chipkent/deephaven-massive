"""The live run's fixed daily timetable: what runs when, on each kind of trading day.

The live-run program (`live_run`) runs every group of checks at a fixed New York time instead
of scheduling them, so a reader can check the plan by eye. Each trading day is one of three
kinds, decided from the run's state:

1. **A stream day**: a probe at 03:15, then `stream-day` from 03:45 to 20:30, and nothing else.
   The probe runs before 03:30, when Massive starts clearing and refilling its snapshots, during
   which the probe cannot tell the plans (`probe.SNAPSHOT_RESET`).
2. **The day after a stream day**: a probe at 08:00, `throughput` from 09:15 to 11:00, then the
   ordinary sequence from 11:30.
3. **An ordinary day**: a probe at 08:00, `luld` at 09:28, then the ordinary sequence from
   11:30, when the previous day's flat files are published.

The ordinary sequence runs each group in turn, when it has due checks and its latest start has
not passed (`ORDINARY_SEQUENCE`). A day that is not a trading day runs nothing. Stream days are
planned by `next_stream_day`. Trading days and half days come from the US exchanges' rules
(`calendar`). Every function is pure.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Final

from tests._live.calendar import NEW_YORK, is_half_day, is_trading_day, trading_day_before


class DayKind(enum.Enum):
    """The kind of a day in the timetable."""

    CLOSED = "closed"
    STREAM = "stream day"
    AFTER_STREAM = "day after a stream day"
    ORDINARY = "ordinary trading day"


@dataclass(frozen=True)
class Slot:
    """When one group may start on a day, and when it must stop.

    Attributes:
        group: The group.
        earliest: The earliest start, New York time.
        latest: The latest start; a group not started by then waits for the next day that has
            it.
        stop: When a running attempt is stopped (a hard stop), or None for the default 03:00
            of the next day.
    """

    group: str
    earliest: time
    latest: time
    stop: time | None = None


STREAM_PROBE: Final = time(3, 15)
"""The probe of a stream day, before Massive's snapshot reset."""

DAY_PROBE: Final = time(8, 0)
"""The probe of every other trading day."""

STREAM_DAY: Final = Slot("stream-day", time(3, 45), time(20, 0), stop=time(20, 30))
"""A stream day's one group; a child that died may restart until 20:00."""

THROUGHPUT: Final = Slot("throughput", time(9, 15), time(10, 0), stop=time(11, 0))
"""The throughput measurement, the morning after a stream day."""

LULD: Final = Slot("luld", time(9, 28), time(11, 0))
"""The LULD check, which needs the regular session's first bands."""

SEQUENCE_START: Final = time(11, 30)
"""When the ordinary sequence starts: the previous day's flat files are published by then."""

ORDINARY_SEQUENCE: Final = (
    Slot("stream-day-reconcile", SEQUENCE_START, time(18, 0)),
    Slot("half-day", SEQUENCE_START, time(18, 0)),
    Slot("evidence", SEQUENCE_START, time(18, 0)),
    Slot("smoke", SEQUENCE_START, time(20, 0)),
    Slot("responses", SEQUENCE_START, time(20, 0)),
    Slot("limits-ws", time(16, 15), time(21, 0)),
    Slot("limits-second-connection", SEQUENCE_START, time(22, 0)),
)
"""The ordinary sequence, in order; `half-day` runs only on the trading day after a half day."""

ANY_TIME_GROUPS: Final = ("evidence", "smoke", "responses", "stream-day-reconcile")
"""The groups `--any-time` runs at once: they need no market hours."""

HARD_STOP: Final = time(3, 0)
"""When any attempt without a stop of its own is stopped, the next day: before a stream day's
probe, so no attempt runs into one."""


def day_kind(day: date, *, stream_day: str, stream_days: list[str]) -> DayKind:
    """Return the kind of a day.

    Args:
        day: The New York date.
        stream_day: The planned stream day, ISO, or `""`.
        stream_days: The days a stream day ran, ISO.

    Returns:
        `CLOSED` on a weekend or holiday, `STREAM` on the planned stream day, `AFTER_STREAM`
        on the trading day after one that ran, else `ORDINARY`.
    """
    if not is_trading_day(day):
        return DayKind.CLOSED
    if day.isoformat() == stream_day:
        return DayKind.STREAM
    if trading_day_before(day).isoformat() in stream_days:
        return DayKind.AFTER_STREAM
    return DayKind.ORDINARY


def after_half_day(day: date) -> date | None:
    """Return the half day that a trading day follows, if it follows one.

    Args:
        day: A trading day.

    Returns:
        The trading day before it when that was a half day, else None.
    """
    before = trading_day_before(day)
    return before if is_half_day(before) else None


def probe_time(kind: DayKind) -> time | None:
    """Return when a day of a kind probes.

    Args:
        kind: The day's kind.

    Returns:
        03:15 on a stream day, 08:00 on another trading day, None on a closed day.
    """
    return {DayKind.STREAM: STREAM_PROBE, DayKind.CLOSED: None}.get(kind, DAY_PROBE)


def slots(day: date, kind: DayKind) -> tuple[Slot, ...]:
    """Return the groups a day may run, in order.

    Args:
        day: The New York date.
        kind: Its kind.

    Returns:
        The day's slots; `half-day` is left out unless the day follows a half day.
    """
    if kind is DayKind.CLOSED:
        return ()
    if kind is DayKind.STREAM:
        return (STREAM_DAY,)
    sequence = tuple(
        s for s in ORDINARY_SEQUENCE if s.group != "half-day" or after_half_day(day) is not None
    )
    first = THROUGHPUT if kind is DayKind.AFTER_STREAM else LULD
    return (first, *sequence)


def at(day: date, moment: time) -> datetime:
    """Return a New York time on a day as an aware datetime.

    Args:
        day: The date.
        moment: The time of day.

    Returns:
        The instant, in New York time.
    """
    return datetime.combine(day, moment, NEW_YORK)


def stop_of(day: date, slot: Slot) -> datetime:
    """Return when an attempt started in a slot on a day is stopped.

    Args:
        day: The day it started.
        slot: Its slot.

    Returns:
        The slot's own stop that day, or 03:00 of the next day.
    """
    if slot.stop is not None:
        return at(day, slot.stop)
    return at(day + timedelta(days=1), HARD_STOP)


def next_stream_day(today: date, stream_days: list[str]) -> date:
    """Plan a stream day: the first trading day after today that can be one.

    A stream day is never the trading day after another stream day, which measures
    throughput, nor the trading day after a half day, which reruns the half-day checks.

    Args:
        today: The New York date of the probe that found a unit due.
        stream_days: The days a stream day ran, ISO.

    Returns:
        The planned day.
    """
    day = today + timedelta(days=1)
    while (
        not is_trading_day(day)
        or trading_day_before(day).isoformat() in stream_days
        or after_half_day(day) is not None
    ):
        day += timedelta(days=1)
    return day
