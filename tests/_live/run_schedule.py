"""The live run's next step: which groups have due checks, and what to do now.

The live-run program (`live_run`) loops: probe, run a group, or sleep. `next_step` decides,
from the state, the run's memory, and the time:

1. Probe when the day's probe time (`timetable.probe_time`) has passed since the last probe, or
   when a probe that could not tell the tiers is due to be tried again.
2. Sleep through the pause after a probe, so the first group starts on an unspent minute.
3. Run the `--any-time` groups that still have due checks, at once, except on a stream day.
4. Run the first of the day's slots (`timetable.slots`) that has due checks, has not run today,
   and whose latest start has not passed; sleep until its earliest start if it is not open yet.
5. Otherwise sleep until the next trading day's probe.

A group has due checks when any of its checks is due (`run_state.is_due`); a pytest group
never collected counts as due, since its checks are not known yet. Every function is pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final

from tests._live import timetable
from tests._live.calendar import NEW_YORK
from tests._live.replies import EXAMPLES
from tests._live.run_state import check_of, is_due
from tests._live.stream_units import stream_units, throughput_units

if TYPE_CHECKING:
    from tests._live.run_context import RunMemory
    from tests._live.run_state import RunState

PYTEST_GROUPS: Final = {
    "evidence": ("-m", "integration", "-k", "not luld"),
    "luld": ("-m", "integration", "-k", "luld"),
    "smoke": ("-m", "integration_smoke"),
    "limits-ws": ("-m", "integration_limits"),
    "limits-second-connection": ("-m", "integration_limits"),
    "half-day": (
        "-m",
        "integration",
        "-k",
        "day_and_week_bar_stamps_from_rest or day_and_longer_bar_stamps_from_rest",
    ),
}
"""Each pytest group's selection, as pytest options."""

PYTEST_PATHS: Final = {
    "limits-ws": ("tests/massive/ws/test_client_integration.py",),
    "limits-second-connection": ("tests/ingest/test_ws_integration.py",),
}
"""The test files a group collects from, when not every test file: only its collection names
them, since the run itself names only the due tests, and a path beside them would run the
path's every test."""

PROBE_RETRIES: Final = 3
PROBE_RETRY_AFTER: Final = timedelta(minutes=5)
"""How many times, and how soon, a probe that could not tell the tiers is tried again."""

MAX_STREAM_STARTS: Final = 5
"""How many times a stream day's child may start in one day, after it died."""


@dataclass(frozen=True)
class Step:
    """What the run does next.

    Attributes:
        action: `"probe"`, `"run"`, or `"sleep"`.
        group: The group to run, or what the sleep waits for.
        until: The group's stop, or the sleep's end; None for a probe.
    """

    action: str
    group: str = ""
    until: datetime | None = None


def half_day_suffix(today: date) -> str:
    """Return the suffix of the half-day checks' names on a day.

    Args:
        today: The New York date.

    Returns:
        `" after <half day>"` on the trading day after a half day, else `""`.
    """
    after = timetable.after_half_day(today)
    return f" after {after.isoformat()}" if after is not None else ""


def known_names(state: RunState, group: str, today: date) -> list[str] | None:
    """Return the names of a group's checks, as far as they are known.

    Args:
        state: The run's state.
        group: The group.
        today: The New York date.

    Returns:
        The names, or None for a pytest group never collected.
    """
    tiers = state.entitlements[state.current]["tiers"] if state.current else {}
    if group in PYTEST_GROUPS:
        if group not in state.known:
            return None
        suffix = half_day_suffix(today) if group == "half-day" else ""
        return [name + suffix for name in state.known[group]]
    names = {
        "responses": [e.name for e in EXAMPLES],
        "stream-day": stream_units(tiers),
        "throughput": throughput_units(tiers),
        "stream-day-reconcile": _reconcile_units(state, today),
    }
    return names.get(group, [])


def _reconcile_units(state: RunState, today: date) -> list[str]:
    """Return the units to compare with history: those that streamed a whole earlier day.

    Args:
        state: The run's state.
        today: The New York date.

    Returns:
        The stream day's passed units, once its day is over; none before.
    """
    if not state.stream_days or state.stream_days[-1] >= today.isoformat():
        return []
    return [
        c.name for c in state.checks.values() if c.group == "stream-day" and c.status == "passed"
    ]


def due_names(
    state: RunState, group: str, today: date, *, any_time: bool = False
) -> list[str] | None:
    """Return the names of a group's due checks.

    Args:
        state: The run's state.
        group: The group.
        today: The New York date.
        any_time: Whether the attempt is an `--any-time` one, which leaves out the stream
            examples of `responses`.

    Returns:
        The due names, or None when the group must be collected first.
    """
    names = known_names(state, group, today)
    if names is None:
        return None
    if group == "responses" and any_time:
        streams = {e.name for e in EXAMPLES if e.stream}
        names = [n for n in names if n not in streams]
    return [n for n in names if is_due(state, check_of(state, group, n))]


def has_due(state: RunState, group: str, today: date, *, any_time: bool = False) -> bool:
    """Tell whether a group has work.

    Args:
        state: The run's state.
        group: The group.
        today: The New York date.
        any_time: Whether the attempt would be an `--any-time` one.

    Returns:
        True when it has due checks, or tests never collected.
    """
    names = due_names(state, group, today, any_time=any_time)
    return names is None or bool(names)


def ran_today(state: RunState, group: str, today: date) -> bool:
    """Tell whether a group is done for the day.

    Args:
        state: The run's state.
        group: The group.
        today: The New York date.

    Returns:
        True when it finished an attempt today (an interrupted one does not count); for a
        stream day, when its child started `MAX_STREAM_STARTS` times.
    """
    attempts = [a for a in state.attempts if a.group == group and a.day == today.isoformat()]
    if group == "stream-day":
        return len(attempts) >= MAX_STREAM_STARTS
    return any(a.status == "finished" for a in attempts)


def _probe_due(memory: RunMemory, now: datetime, probe_slot: datetime | None) -> bool:
    """Tell whether the run probes now.

    Args:
        memory: The run's memory.
        now: The current time.
        probe_slot: Today's probe time, or None on a closed day.

    Returns:
        True when today's probe time passed since the last probe, or a failed probe's retry
        is due.
    """
    if memory.probed_at is None:
        return True
    if probe_slot is not None and memory.probed_at < probe_slot <= now:
        return True
    retry = memory.probed_at + PROBE_RETRY_AFTER
    return 0 < memory.retries < PROBE_RETRIES and retry <= now


def next_step(state: RunState, memory: RunMemory, now: datetime) -> Step:
    """Decide the run's next step.

    Args:
        state: The run's state; `--any-time` groups with nothing due leave its list.
        memory: The run's memory.
        now: The current time, aware.

    Returns:
        The step.
    """
    today = now.astimezone(NEW_YORK).date()
    kind = timetable.day_kind(today, stream_day=state.stream_day, stream_days=state.stream_days)
    probe_at = timetable.probe_time(kind)
    if _probe_due(memory, now, timetable.at(today, probe_at) if probe_at else None):
        return Step("probe")
    if memory.not_before is not None and now < memory.not_before:
        return Step("sleep", "the pause after the probe", memory.not_before)
    for group in list(state.any_time) if kind is not timetable.DayKind.STREAM else []:
        if has_due(state, group, today, any_time=True):
            return Step("run", group, any_time_stop(state, today))
        state.any_time.remove(group)
    for slot in timetable.slots(today, kind):
        if ran_today(state, slot.group, today) or not has_due(state, slot.group, today):
            continue
        if now < timetable.at(today, slot.earliest):
            return Step("sleep", slot.group, timetable.at(today, slot.earliest))
        if now <= timetable.at(today, slot.latest):
            return Step("run", slot.group, timetable.stop_of(today, slot))
    return Step("sleep", "the next probe", next_probe(state, today))


def any_time_stop(state: RunState, today: date) -> datetime:
    """Return when an `--any-time` attempt started today is stopped.

    Args:
        state: The run's state, for its planned stream day.
        today: The New York date.

    Returns:
        03:00 of the next day, or the planned stream day's probe if that comes first.
    """
    stop = timetable.at(today + timedelta(days=1), timetable.HARD_STOP)
    if state.stream_day > today.isoformat():
        planned = date.fromisoformat(state.stream_day)
        stop = min(stop, timetable.at(planned, timetable.STREAM_PROBE))
    return stop


def next_probe(state: RunState, today: date) -> datetime:
    """Return the next trading day's probe time.

    Args:
        state: The run's state, for its stream day.
        today: The New York date.

    Returns:
        The probe time of the first trading day after today.
    """
    day = today + timedelta(days=1)
    kind = timetable.day_kind(day, stream_day=state.stream_day, stream_days=state.stream_days)
    while kind is timetable.DayKind.CLOSED:
        day += timedelta(days=1)
        kind = timetable.day_kind(day, stream_day=state.stream_day, stream_days=state.stream_days)
    return timetable.at(day, timetable.probe_time(kind) or timetable.DAY_PROBE)


def next_start_text(state: RunState, group: str, now: datetime) -> str:
    """Describe when a group can run next, for `status`.

    Args:
        state: The run's state.
        group: The group.
        now: The current time, aware.

    Returns:
        Such as `"Tue 2026-11-03 11:30 EST"` within the next 10 days, else `"not scheduled"`.
    """
    today = now.astimezone(NEW_YORK).date()
    for offset in range(10):
        day = today + timedelta(days=offset)
        kind = timetable.day_kind(day, stream_day=state.stream_day, stream_days=state.stream_days)
        for slot in timetable.slots(day, kind):
            if slot.group == group and timetable.at(day, slot.latest) >= now:
                start = max(timetable.at(day, slot.earliest), now.astimezone(NEW_YORK))
                return f"{start:%a %Y-%m-%d %H:%M %Z}"
    return "not scheduled"
