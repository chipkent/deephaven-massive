"""The live run's next step: which groups have due checks, and what to do now.

The live-run program (`live_run`) loops: probe, run a group, or sleep. `next_step` decides,
from the state, the run's memory, and the time:

1. Probe when the day's probe time (`timetable.probe_time`) has passed since the last probe, or
   when a probe that could not tell the tiers is due to be tried again.
2. Sleep through the pause after a probe, so the first group starts on an unspent minute.
3. Run the planned stream night (`stream-night`) while it is open: from its start until its
   latest restart the next morning, while its units are due and its child has started fewer
   than `MAX_STREAM_STARTS` times.
4. Run the `--any-time` groups that still have due checks, at once, except on a stream day.
5. Run the first of the day's slots (`timetable.slots`) that has due checks, has not run today,
   and whose latest start has not passed; sleep until its earliest start if it is not open yet.
6. Otherwise sleep until the next trading day's probe, or the planned night's start or latest
   restart if sooner.

On a stream night's evening nothing in 4 or 5 starts from `timetable.NIGHT_CLEARANCE`, and
whatever starts before is stopped then. Nothing in 4 or 5 starts from the night's start to its
latest restart the next morning either, even once the night's child may start no more: the run
sleeps until that restart or the next probe, whichever comes first. The child stops itself
before 09:30, so the morning's groups start as soon as it ends after the latest restart.

A group has due checks when any of its checks is due (`run_state.is_due`); a pytest group
never collected counts as due, since its checks are not known yet. Every function is pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final

from tests._live.calendar import NEW_YORK
from tests._live.live_run import timetable
from tests._live.live_run.replies import EXAMPLES
from tests._live.live_run.state import check_of, is_due
from tests._live.live_run.stream_units import night_units, stream_units, throughput_units

if TYPE_CHECKING:
    from tests._live.live_run.context import RunMemory
    from tests._live.live_run.state import RunState

PYTEST_GROUPS: Final = {
    "rate-limits": ("-m", "integration_rate_limits"),
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
"""How many times a stream day's child may start in one day, or a stream night's in one night,
after it died."""


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
        "stream-day-reconcile": _reconcile_units(state, "stream-day", today),
        "stream-night": night_units(tiers),
        "stream-night-reconcile": _reconcile_units(state, "stream-night", today),
    }
    return names.get(group, [])


def _reconcile_units(state: RunState, streamed: str, today: date) -> list[str]:
    """Return the units to compare with history: those that streamed a whole earlier day or night.

    Args:
        state: The run's state.
        streamed: The streaming group, `stream-day` or `stream-night`.
        today: The New York date.

    Returns:
        Its passed units, once its latest day is over (a night's, from the morning after its
        evening); none before.
    """
    days = state.stream_nights if streamed == "stream-night" else state.stream_days
    if not days or days[-1] >= today.isoformat():
        return []
    return [c.name for c in state.checks.values() if c.group == streamed and c.status == "passed"]


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
    night = _night_run(state, today, now)
    if night is not None:
        return night
    clearance = night_clearance(state, now)
    step = (
        None if clearance is not None and now >= clearance else _group_step(state, now, kind=kind)
    )
    if step is None or clearance is None:
        return step or _idle(state, today, now)
    assert step.until is not None  # every run has a stop and every sleep an end
    if step.action == "run":
        return Step("run", step.group, min(step.until, clearance))
    return step if step.until < clearance else _idle(state, today, now)


def _group_step(state: RunState, now: datetime, *, kind: timetable.DayKind) -> Step | None:
    """Return the next `--any-time` group or slot to run or wait for, if any.

    Args:
        state: The run's state; `--any-time` groups with nothing due leave its list.
        now: The current time, aware.
        kind: The kind of its New York day.

    Returns:
        The step (`next_step` 4 and 5), or None when no group runs today any more or the
        planned stream night holds the key (`_night_holds_key`).
    """
    if _night_holds_key(state, now):
        return None
    today = now.astimezone(NEW_YORK).date()
    quiet = kind is timetable.DayKind.STREAM
    for group in list(state.any_time) if not quiet else []:
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
    return None


def _idle(state: RunState, today: date, now: datetime) -> Step:
    """Return the sleep until the next probe, or the planned night's start or latest restart.

    Args:
        state: The run's state.
        today: The New York date.
        now: The current time, aware.

    Returns:
        The sleep, until whichever of them comes first.
    """
    wake = next_probe(state, today)
    if state.stream_night:
        evening = date.fromisoformat(state.stream_night)
        start, last = timetable.night_start(evening), _last_start(evening)
        if now < start < wake:
            return Step("sleep", "stream-night", start)
        if start <= now < last < wake:
            return Step("sleep", "the stream night's latest restart", last)
    return Step("sleep", "the next probe", wake)


def night_starts(state: RunState) -> int:
    """Count the starts of the planned stream night's child.

    Args:
        state: The run's state.

    Returns:
        How many attempts of the planned night there are.
    """
    return sum(a.group == "stream-night" and a.day == state.stream_night for a in state.attempts)


def _night_run(state: RunState, today: date, now: datetime) -> Step | None:
    """Return the stream night's run while it may start or restart.

    Args:
        state: The run's state.
        today: The New York date.
        now: The current time, aware.

    Returns:
        The run, stopped at the night's end, from its start to its latest restart the next
        morning, while its units are due and its child started fewer than `MAX_STREAM_STARTS`
        times; otherwise None.
    """
    if not night_may_start(state, now):
        return None
    evening = date.fromisoformat(state.stream_night)
    if now < timetable.night_start(evening) or not has_due(state, "stream-night", today):
        return None
    return Step("run", "stream-night", timetable.night_end(evening))


def night_may_start(state: RunState, now: datetime) -> bool:
    """Tell whether the planned stream night's child may still start, now or later.

    A night whose child started `MAX_STREAM_STARTS` times, or whose latest restart the next
    morning has passed, runs no more, even before its end, so a probe then plans the next one.

    Args:
        state: The run's state.
        now: The current time, aware.

    Returns:
        True with a planned night until its latest restart, while its child has started fewer
        than `MAX_STREAM_STARTS` times.
    """
    if not state.stream_night:
        return False
    last = _last_start(date.fromisoformat(state.stream_night))
    return now <= last and night_starts(state) < MAX_STREAM_STARTS


def _last_start(evening: date) -> datetime:
    """Return the latest restart of a stream night's child.

    Args:
        evening: The night's evening, its New York date.

    Returns:
        09:00 New York time the next morning (`timetable.NIGHT_LAST_START`).
    """
    return timetable.at(evening + timedelta(days=1), timetable.NIGHT_LAST_START)


def _night_holds_key(state: RunState, now: datetime) -> bool:
    """Tell whether the planned stream night holds the key, so no other group starts.

    Args:
        state: The run's state.
        now: The current time, aware.

    Returns:
        True from the night's start until its latest restart the next morning.
    """
    if not state.stream_night:
        return False
    evening = date.fromisoformat(state.stream_night)
    return timetable.night_start(evening) <= now < _last_start(evening)


def night_clearance(state: RunState, now: datetime) -> datetime | None:
    """Return when the planned stream night needs the key to itself, while that is ahead.

    Args:
        state: The run's state.
        now: The current time, aware.

    Returns:
        16:15 New York time on the night's evening (`timetable.NIGHT_CLEARANCE`) before the
        night starts; None without a planned night, or once it started.
    """
    if not state.stream_night:
        return None
    evening = date.fromisoformat(state.stream_night)
    if now >= timetable.night_start(evening):
        return None
    return timetable.at(evening, timetable.NIGHT_CLEARANCE)


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
    if group == "stream-night":
        return _night_start_text(state, now)
    today = now.astimezone(NEW_YORK).date()
    for offset in range(10):
        day = today + timedelta(days=offset)
        kind = timetable.day_kind(day, stream_day=state.stream_day, stream_days=state.stream_days)
        for slot in timetable.slots(day, kind):
            if slot.group == group and timetable.at(day, slot.latest) >= now:
                start = max(timetable.at(day, slot.earliest), now.astimezone(NEW_YORK))
                return f"{start:%a %Y-%m-%d %H:%M %Z}"
    return "not scheduled"


def _night_start_text(state: RunState, now: datetime) -> str:
    """Describe when the planned stream night starts, for `status`.

    Args:
        state: The run's state.
        now: The current time, aware.

    Returns:
        Such as `"Sun 2026-11-08 16:30 EST"`, or the current time during the night; `"not
        scheduled"` without a planned night or after its end.
    """
    if not state.stream_night:
        return "not scheduled"
    evening = date.fromisoformat(state.stream_night)
    if now >= timetable.night_end(evening):
        return "not scheduled"
    start = max(timetable.night_start(evening), now.astimezone(NEW_YORK))
    return f"{start:%a %Y-%m-%d %H:%M %Z}"
