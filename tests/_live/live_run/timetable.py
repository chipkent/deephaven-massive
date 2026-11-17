"""The live run's fixed daily timetable: what runs when, on each kind of trading day.

The live-run program (`live_run`) runs every group of checks at a fixed New York time instead
of scheduling them, so a reader can check the plan by eye. Each trading day is one of three
kinds, decided from the run's state:

1. **A stream day**: a probe at 03:15, then `stream-day` from 03:45 to 20:30, and nothing else.
2. **The day after a stream day**: a probe at 08:00, `throughput` from 09:15 to 11:00, then the
   ordinary sequence from 11:30.
3. **An ordinary day**: a probe at 08:00, `luld` at 09:28, then the ordinary sequence from
   11:30, when the previous day's flat files are published.

The ordinary sequence runs each group in turn, when it has due checks and its latest start has
not passed (`ORDINARY_SEQUENCE`). A day that is not a trading day runs nothing. Stream days are
planned by `next_stream_day`.

A **stream night** runs across two days: `stream-night` from 16:30 New York time on a planned
evening to 09:30 the next morning, the hours only crypto and futures trade. That evening no other
group starts at or after 16:15, and one still running is stopped then, so the night starts with
the key's connections free; the night's child may restart until 09:00. A night is planned by
`next_stream_night` for an evening before a full trading day: Sunday, or a full trading day
itself, so futures trade from the evening's 17:00 Chicago session open to the morning, and the
night never touches a stream day.

Trading days and half days come from the US exchanges' rules (`calendar`), together with the
closures Massive's market status lists as upcoming (`closures`, from the probe). Every function
is pure.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Final

from tests._live.calendar import NEW_YORK, is_half_day, is_trading_day, trading_day_before

if TYPE_CHECKING:
    from collections.abc import Mapping


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
"""The probe of a stream day, half an hour before its streams start."""

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
    Slot("stream-night-reconcile", SEQUENCE_START, time(18, 0)),
    Slot("half-day", SEQUENCE_START, time(18, 0)),
    Slot("rate-limits", SEQUENCE_START, time(18, 0)),
    Slot("evidence", SEQUENCE_START, time(18, 0)),
    Slot("smoke", SEQUENCE_START, time(20, 0)),
    Slot("responses", SEQUENCE_START, time(20, 0)),
    Slot("limits-ws", time(16, 15), time(21, 0)),
    Slot("limits-second-connection", SEQUENCE_START, time(22, 0)),
)
"""The ordinary sequence, in order; `half-day` runs only on the trading day after a half day."""

ANY_TIME_GROUPS: Final = (
    "rate-limits",
    "evidence",
    "smoke",
    "responses",
    "stream-day-reconcile",
    "stream-night-reconcile",
)
"""The groups `--any-time` runs at once: they need no market hours."""

NIGHT_START: Final = time(16, 30)
"""When a stream night starts on its evening, New York time: after the stock close, half an
hour before futures' daily break, so rows of the ending futures session are recorded too."""

NIGHT_END: Final = time(9, 30)
"""When a stream night ends, the next morning, New York time: the stock open."""

NIGHT_LAST_START: Final = time(9, 0)
"""The latest restart of a stream night's child that died, the next morning."""

NIGHT_CLEARANCE: Final = time(16, 15)
"""On a stream night's evening, no other group starts at or after this, and one still running
is stopped at it, so the night opens its streams with the key's connections free."""

SUNDAY: Final = 6
"""Sunday's `date.weekday()`: futures open for the week that evening at 17:00 Chicago time."""

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


def next_stream_day(today: date, stream_days: list[str], *, night: str) -> date:
    """Plan a stream day: the first trading day after today that can be one.

    A stream day is never the trading day after another stream day, which measures
    throughput, nor the trading day after a half day, which reruns the half-day checks, nor
    a day the planned stream night holds: its evening, or its morning.

    Args:
        today: The New York date of the probe that found a unit due.
        stream_days: The days a stream day ran, ISO.
        night: The planned stream night's evening, ISO, or `""`.

    Returns:
        The planned day.
    """
    held = set()
    if night:
        held = {night, (date.fromisoformat(night) + timedelta(days=1)).isoformat()}
    day = today + timedelta(days=1)
    while (
        not is_trading_day(day)
        or trading_day_before(day).isoformat() in stream_days
        or after_half_day(day) is not None
        or day.isoformat() in held
    ):
        day += timedelta(days=1)
    return day


def night_start(night: date) -> datetime:
    """Return when a stream night starts.

    Args:
        night: The night's evening, its New York date.

    Returns:
        16:30 New York time that evening.
    """
    return at(night, NIGHT_START)


def night_end(night: date) -> datetime:
    """Return when a stream night ends.

    Args:
        night: The night's evening, its New York date.

    Returns:
        09:30 New York time the next morning.
    """
    return at(night + timedelta(days=1), NIGHT_END)


def is_full_trading_day(day: date, closures: Mapping[str, str]) -> bool:
    """Tell whether the US exchanges trade a whole day on a day.

    Args:
        day: The New York date.
        closures: Massive's upcoming closures: each ISO date's status, `closed` or
            `early-close` (`run_probe.ProbeResult.closures`).

    Returns:
        True for a trading day that is no half day and no closure Massive lists.
    """
    return is_trading_day(day) and not is_half_day(day) and day.isoformat() not in closures


def can_be_night(evening: date, *, stream_day: str, closures: Mapping[str, str]) -> bool:
    """Tell whether an evening can be a stream night.

    Its morning must be a full trading day, so the futures session that opens at 17:00 Chicago
    time runs through the night; the evening itself must be a Sunday, when futures open for the
    week, or a full trading day, whose futures session ends normally at 16:00 Chicago time. So
    a night falls on a Sunday to Thursday evening, never around a holiday or a half day, and
    never on the planned stream day's evening or the evening before it, whose morning the
    stream day needs.

    Args:
        evening: The night's evening, its New York date.
        stream_day: The planned stream day, ISO, or `""`.
        closures: Massive's upcoming closures, by ISO date.

    Returns:
        True when the night can be planned.
    """
    morning = evening + timedelta(days=1)
    evening_trades = evening.weekday() == SUNDAY or is_full_trading_day(evening, closures)
    clear = stream_day not in {evening.isoformat(), morning.isoformat()}
    return evening_trades and is_full_trading_day(morning, closures) and clear


def next_stream_night(today: date, *, stream_day: str, closures: Mapping[str, str]) -> date:
    """Plan a stream night: the first evening after today that can be one (`can_be_night`).

    Args:
        today: The New York date of the probe that found a night unit due.
        stream_day: The planned stream day, ISO, or `""`.
        closures: Massive's upcoming closures, by ISO date.

    Returns:
        The night's evening.
    """
    evening = today + timedelta(days=1)
    while not can_be_night(evening, stream_day=stream_day, closures=closures):
        evening += timedelta(days=1)
    return evening
