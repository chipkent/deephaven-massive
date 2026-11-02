"""Which bucket a futures bar of a session or longer covers, judged from the bars inside it.

A simpler rule for the library's futures bar windows needs a model of how Massive buckets
these bars: each whole session goes into the bucket of the UTC calendar day of its first
minute; a bucket is a run of whole UTC days on a fixed grid (N days for N sessions, 7N days for
N weeks, N, 3N, or 12N calendar months), starting at its `window_start`, 00:00 UTC; and its
label (`session_end_date`) is its last session's. The futures bucket check reads one month of
one contract per product and tests every part of that model against Massive's own shorter
bars:

- `placements`: where each 1-session bar's `window_start` falls against its first minute bar;
- `bucket_check`: each longer bar against candidate contents of its bucket (the whole sessions
  that start in its UTC days, the next N sessions, the minute bars of its UTC days, and, when
  a read's `{from}` falls inside the bucket, only the sessions from `{from}` on);
- `grid_verdict`: whether two reads from different `{from}` days share their windows (a fixed
  grid) or each starts at its own `{from}`;
- `clipping_verdict`: whether one bucket read from two `{from}` values gives the same bar.

Every function is pure; the check in `tests/namespaces/` reads the bars and calls them.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

from tests._live.calendar import months_before

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone of the CME Group's trading calendar."""

MATCH: Final = 1e-9
"""The largest relative error at which two bars' prices and volumes are the same bar."""

_DAY_NS: Final = 86_400 * 1_000_000_000
"""Nanoseconds per day."""

_RESOLUTION: Final = re.compile(r"(?P<count>\d+)(?P<unit>session|week|month|quarter|year)")
"""A resolution of a session or longer, such as `2session` or `1month`."""

_MONTHS: Final = {"month": 1, "quarter": 3, "year": 12}
"""Calendar months per unit of the units counted in months."""

NO_MATCH: Final = "no match"
"""The verdict on a bar that no candidate content of its bucket matches."""

NOT_JUDGED: Final = "not judged"
"""The verdict on a bar whose bucket ends after the shorter bars read."""


@dataclass(frozen=True)
class Bar:
    """One futures bar as Massive sent it, the values the bucket checks compare.

    Attributes:
        start_ns: Its `window_start`, epoch nanoseconds.
        label: Its `session_end_date`, ISO text; None when absent.
        prices: Its open, high, low, and close.
        volume: Its volume.
    """

    start_ns: int
    label: str | None
    prices: tuple[float, float, float, float]
    volume: float


def bar_of(row: Mapping[str, Any]) -> Bar:
    """Read a raw futures aggregate row.

    Args:
        row: The row as Massive sent it (`window_start` in ns, `session_end_date`, `open`,
            `high`, `low`, `close`, `volume`).

    Returns:
        The bar; a missing volume counts as 0.
    """
    label = row.get("session_end_date")
    opened, high, low, closed = (float(row[key]) for key in ("open", "high", "low", "close"))
    return Bar(
        start_ns=int(row["window_start"]),
        label=None if label is None else str(label),
        prices=(opened, high, low, closed),
        volume=float(row.get("volume") or 0),
    )


def combined(bars: Sequence[Bar]) -> Bar | None:
    """Combine consecutive bars into one, as Massive would if they were its bucket.

    Args:
        bars: The bars, oldest first.

    Returns:
        The first start and open, the highest high, the lowest low, the last close and label,
        and the summed volume; None for no bars.
    """
    if not bars:
        return None
    prices = (
        bars[0].prices[0],
        max(bar.prices[1] for bar in bars),
        min(bar.prices[2] for bar in bars),
        bars[-1].prices[3],
    )
    return Bar(bars[0].start_ns, bars[-1].label, prices, sum(bar.volume for bar in bars))


def bar_error(left: Bar, right: Bar) -> float:
    """Return the largest relative error between two bars' prices and volumes.

    Args:
        left: One bar.
        right: The other.

    Returns:
        The largest of `|a - b| / max(|a|, |b|)` over open, high, low, close, and volume.
    """
    pairs = zip((*left.prices, left.volume), (*right.prices, right.volume), strict=True)
    return max(abs(a - b) / max(abs(a), abs(b), 1e-12) for a, b in pairs)


def utc_day(stamp_ns: int) -> date:
    """Return the UTC calendar day of an epoch-nanosecond stamp.

    Args:
        stamp_ns: The stamp.

    Returns:
        The day.
    """
    return datetime.fromtimestamp(stamp_ns // 1_000_000_000, UTC).date()


def utc_midnight_ns(day: date) -> int:
    """Return 00:00 UTC of a day as epoch nanoseconds.

    Args:
        day: The day.

    Returns:
        The stamp.
    """
    return (day - date(1970, 1, 1)).days * _DAY_NS


def stamp_text(stamp_ns: int) -> str:
    """Write an epoch-nanosecond stamp in UTC and Chicago time.

    Args:
        stamp_ns: The stamp.

    Returns:
        Such as `"2026-10-01T00:00Z (Wed 2026-09-30 19:00 Chicago)"`.
    """
    moment = datetime.fromtimestamp(stamp_ns // 1_000_000_000, UTC)
    return f"{moment:%Y-%m-%dT%H:%MZ} ({moment.astimezone(CHICAGO):%a %Y-%m-%d %H:%M} Chicago)"


def even_and_odd_days(day: date) -> tuple[date, date]:
    """Return `day` and the day after, the one with an even epoch-day number first.

    Massive could align 2-session bars to a fixed grid counted in days from 1970-01-01, or to
    each request's `{from}`; requests from an even and an odd epoch day tell the two apart.

    Args:
        day: The first day.

    Returns:
        The even epoch day and the odd one.
    """
    after = day + timedelta(days=1)
    return (day, after) if (day - date(1970, 1, 1)).days % 2 == 0 else (after, day)


def bucket_end(start_day: date, resolution: str) -> date:
    """Return the exclusive end day of a bucket on the model's grid.

    Args:
        start_day: The bucket's first UTC day, its `window_start`.
        resolution: A resolution of a session or longer, such as `"2session"` or `"1month"`.

    Returns:
        `start_day` plus N days for N sessions, 7N days for N weeks, and N, 3N, or 12N
        calendar months for months, quarters, and years.

    Raises:
        ValueError: For a resolution shorter than a session.
    """
    found = _RESOLUTION.fullmatch(resolution)
    if found is None:
        raise ValueError(f"{resolution!r} is not a resolution of a session or longer")
    count, unit = int(found["count"]), found["unit"]
    if unit == "session":
        return start_day + timedelta(days=count)
    if unit == "week":
        return start_day + timedelta(days=7 * count)
    first = months_before(start_day, -count * _MONTHS[unit])
    return first + timedelta(days=start_day.day - 1)


def placements(sessions: Sequence[Bar], minutes: Sequence[Bar]) -> tuple[list[str], str]:
    """Describe where each 1-session bar's `window_start` falls against its first minute bar.

    The model places a session in the UTC day of its first minute; the Chicago day of its first
    minute is the other reading for a session that opens in the Chicago evening.

    Args:
        sessions: The 1-session bars, oldest first.
        minutes: The minute bars over the same days, oldest first, each labelled with its
            session.

    Returns:
        One line per session (its `window_start`, label, first minute bar, and which day its
        `window_start` is midnight UTC of), and the verdict: for how many sessions it is the
        first minute's UTC day, its Chicago day, and how many had no minute bar.
    """
    first: dict[str | None, int] = {}
    for minute in minutes:
        first.setdefault(minute.label, minute.start_ns)
    lines, counts = [], Counter[str]()
    for session in sessions:
        opened = first.get(session.label)
        if opened is None:
            counts["no minute bar"] += 1
            lines.append(
                f"session {session.label}: start {stamp_text(session.start_ns)}; no minute"
            )
            continue
        chicago = datetime.fromtimestamp(opened // 1_000_000_000, UTC).astimezone(CHICAGO).date()
        days = [
            name
            for name, day in (("UTC", utc_day(opened)), ("Chicago", chicago))
            if session.start_ns == utc_midnight_ns(day)
        ]
        for name in days:
            counts[name] += 1
        lines.append(
            f"session {session.label}: start {stamp_text(session.start_ns)}; first minute "
            f"{stamp_text(opened)}; start is 00:00 UTC of the first minute's "
            f"{' and '.join(days) or 'neither'} day"
        )
    verdict = (
        f"window_start is 00:00 UTC of the first minute bar's UTC day for {counts['UTC']} of "
        f"{len(sessions)} sessions, of its Chicago day for {counts['Chicago']}; no minute bar "
        f"for {counts['no minute bar']}"
    )
    return lines, verdict


@dataclass(frozen=True)
class BucketInputs:
    """The shorter bars a longer bar is checked against, and the spans they cover.

    Attributes:
        sessions: The 1-session bars, oldest first.
        sessions_from_ns: The first `window_start` the 1-session read asked for.
        minutes: The minute bars, oldest first.
        minutes_from_ns: The first `window_start` the minute read asked for.
        until_ns: The end of both reads, or now if earlier: a bucket ending after it is
            incomplete in the shorter bars, and is not judged.
    """

    sessions: Sequence[Bar]
    sessions_from_ns: int
    minutes: Sequence[Bar]
    minutes_from_ns: int
    until_ns: int


def _inside(bars: Sequence[Bar], start_ns: int, end_ns: int) -> list[Bar]:
    """Return the bars that start in `[start_ns, end_ns)`.

    Returns:
        The bars, in their order.
    """
    return [bar for bar in bars if start_ns <= bar.start_ns < end_ns]


def bucket_check(
    resolution: str, bar: Bar, inputs: BucketInputs, *, read_from_ns: int
) -> tuple[str, str]:
    """Compare a longer bar with the candidate contents of its bucket.

    The candidates are the whole sessions whose `window_start` falls in the bucket's UTC days
    (`bucket_end`); for N sessions, the N sessions from its start, whatever their days; the
    minute bars of the bucket's UTC days (a slice that may cut a session); and, when the read's
    `{from}` falls after the bar's start, the whole sessions from `{from}` on (a bucket clipped
    at `{from}`). A candidate whose shorter bars were not all read is left out.

    Args:
        resolution: The bar's resolution, such as `"2session"`.
        bar: The bar.
        inputs: The shorter bars.
        read_from_ns: The `{from}` the bar's read asked for, epoch ns.

    Returns:
        One evidence line (the bar, its bucket, and each candidate's relative error and
        whether its last session is the bar's label), and the verdict: the name of the first
        candidate within `MATCH`, `NO_MATCH`, or `NOT_JUDGED` when the bucket ends after the
        reads.
    """
    first_day = utc_day(bar.start_ns)
    end_day = bucket_end(first_day, resolution)
    end_ns = utc_midnight_ns(end_day)
    head = (
        f"{resolution} bar {stamp_text(bar.start_ns)} label {bar.label} prices {bar.prices} "
        f"volume {bar.volume:g}; bucket {first_day} to {end_day} (exclusive)"
    )
    if end_ns > inputs.until_ns:
        return f"{head}: not judged, the bucket ends after the reads", NOT_JUDGED
    candidates: dict[str, list[Bar]] = {}
    if bar.start_ns >= inputs.sessions_from_ns:
        candidates["whole sessions in its UTC days"] = _inside(
            inputs.sessions, bar.start_ns, end_ns
        )
        count = int(resolution.removesuffix("session")) if resolution.endswith("session") else 0
        if count:
            later = [s for s in inputs.sessions if s.start_ns >= bar.start_ns]
            candidates[f"the {count} sessions from its start"] = later[:count]
    if bar.start_ns >= inputs.minutes_from_ns:
        candidates["minute bars of its UTC days"] = _inside(inputs.minutes, bar.start_ns, end_ns)
    if read_from_ns > bar.start_ns:
        candidates["whole sessions from {from}"] = _inside(inputs.sessions, read_from_ns, end_ns)
    results, best = [], NO_MATCH
    for name, bars in candidates.items():
        built = combined(bars)
        if built is None:
            results.append(f"{name}: none")
            continue
        error = bar_error(bar, built)
        label = (
            "label is its last session"
            if built.label == bar.label
            else (f"its last session is {built.label}")
        )
        mark = " MATCH" if error <= MATCH else ""
        results.append(f"{name} ({len(bars)} bars): {error:.2e}, {label}{mark}")
        if mark and best == NO_MATCH:
            best = name
    return f"{head}: {'; '.join(results) or 'no shorter bars read'}", best


def bucket_verdict(resolution: str, verdicts: Sequence[str]) -> str:
    """Sum one resolution's bucket checks into a verdict line.

    Args:
        resolution: The resolution.
        verdicts: Each bar's verdict (`bucket_check`).

    Returns:
        Such as `"2session: whole sessions in its UTC days 12, no match 1, of 13 bars"`; the
        bars not judged are named and left out of the count.
    """
    counts = Counter(verdict for verdict in verdicts if verdict != NOT_JUDGED)
    parts = ", ".join(f"{name} {n}" for name, n in counts.most_common())
    later = verdicts.count(NOT_JUDGED)
    left = f" ({later} not judged: they end after the reads)" if later else ""
    return f"{resolution}: {parts or 'no bar judged'}, of {counts.total()} bars{left}"


def grid_verdict(first: Sequence[Bar], second: Sequence[Bar], *, after_ns: int) -> str:
    """Say whether two reads of the same resolution from different `{from}` share their windows.

    Args:
        first: The bars read from one `{from}`.
        second: The bars read from another.
        after_ns: Compare only windows starting at or after this stamp, past both `{from}`
            values, so a first window clipped at its `{from}` does not count.

    Returns:
        `"fixed grid"` when both reads hold the same windows there, `"anchored to {from}"`
        when they share none, `"partly shared"` otherwise, or `"no windows to compare"`; each
        with the window counts.
    """
    left = {bar.start_ns for bar in first if bar.start_ns >= after_ns}
    right = {bar.start_ns for bar in second if bar.start_ns >= after_ns}
    counts = f"({len(left)} and {len(right)} windows, {len(left & right)} shared)"
    if not left or not right:
        return f"no windows to compare {counts}"
    if left == right:
        return f"fixed grid: both reads hold the same windows {counts}"
    if not left & right:
        return f"anchored to {{from}}: the reads share no window {counts}"
    return f"partly shared {counts}"


def clipping_verdict(first: Sequence[Bar], second: Sequence[Bar]) -> str:
    """Say whether one bucket read from two `{from}` values gives the same bar.

    Args:
        first: The bars read from the earlier `{from}`.
        second: The bars read from the later one, inside the same bucket.

    Returns:
        For the first window both reads hold: `"the same bar"` when its prices and volume
        agree, `"clipped at {from}"` when they differ; or `"no shared window"` with each read's
        first start, when the later read's bar starts elsewhere.
    """
    shared = sorted({bar.start_ns for bar in first} & {bar.start_ns for bar in second})
    if not shared:
        starts = [stamp_text(bars[0].start_ns) if bars else "none" for bars in (first, second)]
        return f"no shared window: the reads start at {starts[0]} and {starts[1]}"
    start = shared[0]
    left = next(bar for bar in first if bar.start_ns == start)
    right = next(bar for bar in second if bar.start_ns == start)
    error = bar_error(left, right)
    if error <= MATCH:
        return f"the same bar from both {{from}} values at {stamp_text(start)}"
    return f"clipped at {{from}}: the bar at {stamp_text(start)} differs by {error:.2e}"
