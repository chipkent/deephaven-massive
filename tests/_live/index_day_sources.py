"""Which day each index day source covers, and where the index flat files and REST disagree.

An index day bar comes from four sources: the aggregates range, `/prev`, the daily open and
close, and the `us_indices/day_aggs_v1` file. The library assumes they all cover the same
window, and the evidence so far (`I:SPX` and `I:AAVE100` on two days) cannot say which: the New
York calendar day, or the Chicago one, an hour later. For an index computed only during the
stock session the two windows hold the same minutes and agree; for one that updates around the
clock they differ, and only such an index can tell them apart.

The many-index check measures this broadly: for each index and day it rebuilds the day from
REST minute bars over both windows, says which window each source matches (`window_verdict`),
compares the `us_indices/minute_aggs_v1` file's minutes with REST's (`compare_minute_file`), and
sums each index's days (`IndexTally`) into one line per index and one verdict per question
(`summary_lines`). Every function is pure; the check in `tests/namespaces/` reads the data and
calls them.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

from tests._live.calendar import NEW_YORK

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone whose calendar day may be an index day's: midnight there is 01:00 New York time."""

WINDOWS: Final = (("New York", NEW_YORK), ("Chicago", CHICAGO))
"""The candidate day windows of an index day bar, by name and zone."""

MATCH: Final = 1e-6
"""The largest relative error at which two bars' prices are the same bar."""

FILE_SOURCE: Final = "day file"
"""The source name of the `us_indices/day_aggs_v1` row, which `IndexTally` compares with REST."""

RANGE_SOURCE: Final = "range"
"""The source name of the aggregates range's 1-day bar."""

ROUND_THE_CLOCK: Final = "I:AAVE100"
"""The index whose day file and `/prev` matched no REST window in an earlier run."""

Prices = tuple[float, float, float, float]
"""A bar's open, high, low, and close; an index has no volume."""

_NS_PER_MS: Final = 1_000_000
"""Nanoseconds per millisecond."""


def crypto_choices(found: Mapping[str, Sequence[Mapping[str, Any]]], *, each: int) -> list[str]:
    """Pick a few crypto indices of each family from Massive's index ticker list.

    Massive publishes several indices per coin (`I:AAVE100`, `I:AAVE400`, `I:AAVE10RP`, ...).
    Taking the `100` one first matches the shape of `I:AAVE100`, and its siblings show whether
    a mismatch belongs to one index or to the family.

    Args:
        found: The ticker-list rows of each family's search, by family (such as `"BTC"`), as
            Massive sent them.
        each: The most indices kept per family.

    Returns:
        The tickers chosen, family by family: those starting `I:<family>`, the `100` one
        first, then the rest in ticker order; a family with none adds nothing.
    """
    chosen: list[str] = []
    for family, rows in found.items():
        prefix = f"I:{family}"
        tickers = sorted({str(row["ticker"]) for row in rows if row.get("ticker")})
        own = [ticker for ticker in tickers if ticker.startswith(prefix)]
        first = [ticker for ticker in own if ticker == f"{prefix}100"]
        chosen.extend((first + [ticker for ticker in own if ticker not in first])[:each])
    return chosen


def minute_read_spans(days: Iterable[date]) -> list[tuple[date, date]]:
    """Return the fewest day ranges whose REST minute bars cover every day's two windows.

    A day's Chicago window ends at 01:00 New York time the next day, so each day needs its
    neighbours' minutes too: the day before to the day after. Overlapping or touching ranges
    are merged, so a run of recent days costs one request.

    Args:
        days: The days to rebuild.

    Returns:
        The first and last day of each range, oldest first.
    """
    spans: list[tuple[date, date]] = []
    for day in sorted(days):
        first, last = day - timedelta(days=1), day + timedelta(days=1)
        if spans and first <= spans[-1][1] + timedelta(days=1):
            spans[-1] = (spans[-1][0], max(spans[-1][1], last))
        else:
            spans.append((first, last))
    return spans


def day_window_ms(day: date, zone: tzinfo) -> tuple[int, int]:
    """Return a calendar day in a zone as epoch milliseconds, midnight to the next midnight.

    Midnight to midnight, not 24 hours, so a 23- or 25-hour day is right.

    Args:
        day: The day.
        zone: The zone.

    Returns:
        The window's start and its exclusive end.
    """
    after = day + timedelta(days=1)
    start = datetime(day.year, day.month, day.day, tzinfo=zone)
    end = datetime(after.year, after.month, after.day, tzinfo=zone)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def prices(row: Mapping[str, Any]) -> Prices | None:
    """Read a bar's prices from any index source.

    Args:
        row: A REST bar (`o`, `h`, `l`, `c`), a daily open and close, or a flat-file row as
            text (`open`, `high`, `low`, `close`).

    Returns:
        The open, high, low, and close, or None when one is missing or an empty field.
    """
    keys = ("o", "h", "l", "c") if "o" in row else ("open", "high", "low", "close")
    values = [row.get(key) for key in keys]
    if any(value is None or value == "" for value in values):
        return None
    opened, high, low, closed = (float(str(value)) for value in values)
    return opened, high, low, closed


def rebuilt(minutes: Sequence[Mapping[str, Any]], start_ms: int, end_ms: int) -> Prices | None:
    """Rebuild one bar from the REST minute bars stamped in `[start_ms, end_ms)`.

    Args:
        minutes: REST minute bars (`t` in epoch ms, `o`, `h`, `l`, `c`), oldest first.
        start_ms: The window's start.
        end_ms: The window's exclusive end.

    Returns:
        The first open, highest high, lowest low, and last close, or None when no minute bar
        falls inside.
    """
    inside = [m for m in minutes if start_ms <= int(m["t"]) < end_ms]
    if not inside:
        return None
    return (
        float(inside[0]["o"]),
        float(max(m["h"] for m in inside)),
        float(min(m["l"] for m in inside)),
        float(inside[-1]["c"]),
    )


def relative_error(left: Prices, right: Prices) -> float:
    """Return the largest relative error between two bars' prices.

    Args:
        left: One bar's prices.
        right: The other's.

    Returns:
        The largest of `|a - b| / max(|a|, |b|)` over open, high, low, and close.
    """
    return max(abs(a - b) / max(abs(a), abs(b), 1e-12) for a, b in zip(left, right, strict=True))


def window_verdict(
    values: Prices | None, rebuilds: Mapping[str, Prices | None]
) -> tuple[str, float | None]:
    """Say which day window a source's bar covers.

    Args:
        values: The source's prices, None when it sent no bar.
        rebuilds: The minute rebuild of each window, by window name (`WINDOWS`).

    Returns:
        The verdict, and the bar's relative error to the closest rebuild (None without one):
        `"New York day"` or `"Chicago day"` when only that rebuild matches; `"both days"` when
        both do (the windows hold the same minutes); `"neither day"`; `"no bar"` when the
        source sent none; `"no minute bars"` when neither window could be rebuilt.
    """
    if values is None:
        return "no bar", None
    errors = {
        name: relative_error(values, rebuilt)
        for name, rebuilt in rebuilds.items()
        if rebuilt is not None
    }
    if not errors:
        return "no minute bars", None
    matched = [name for name, error in errors.items() if error <= MATCH]
    closest = min(errors.values())
    if len(matched) == len(WINDOWS):
        return "both days", closest
    if matched:
        return f"{matched[0]} day", closest
    return "neither day", closest


def windows_differ(minutes: Sequence[Mapping[str, Any]], day: date) -> bool:
    """Tell whether an index's New York and Chicago windows of a day hold different minutes.

    They differ when a REST minute bar falls in an hour one window has and the other lacks:
    00:00 to 01:00 New York time on the day, or on the day after.

    Args:
        minutes: REST minute bars (`t` in epoch ms).
        day: The day.

    Returns:
        True when at least one minute bar falls in either hour.
    """
    new_york = day_window_ms(day, NEW_YORK)
    chicago = day_window_ms(day, CHICAGO)
    hours = ((new_york[0], chicago[0]), (new_york[1], chicago[1]))
    return any(start <= int(m["t"]) < end for m in minutes for start, end in hours)


@dataclass(frozen=True)
class MinuteFileComparison:
    """How one day's `us_indices/minute_aggs_v1` rows of an index compare with its REST minutes.

    Attributes:
        rows: How many rows the file holds for the index.
        first_ms: The earliest row's stamp, epoch ms; None without rows.
        last_ms: The latest row's stamp; None without rows.
        lacks: The REST minutes of the New York day the file lacks, oldest first.
        adds: The file's minutes REST does not have, oldest first.
        outside: The file's minutes outside the New York day, oldest first.
        differing: The minutes both hold whose prices differ by more than `MATCH`: the stamp,
            the file's prices, and REST's.
    """

    rows: int
    first_ms: int | None
    last_ms: int | None
    lacks: tuple[int, ...]
    adds: tuple[int, ...]
    outside: tuple[int, ...]
    differing: tuple[tuple[int, Prices, Prices], ...]

    @property
    def has_gaps(self) -> bool:
        """Whether the file's minutes are not exactly REST's minutes of the New York day.

        True when it holds no row, lacks a REST minute, adds one REST lacks, or holds one
        outside the New York day.
        """
        return not self.rows or bool(self.lacks or self.adds or self.outside)

    @property
    def values_differ(self) -> bool:
        """Whether a minute the file and REST both hold has other prices in the file."""
        return bool(self.differing)


def compare_minute_file(
    file_rows: Sequence[Mapping[str, Any]], minutes: Sequence[Mapping[str, Any]], day: date
) -> MinuteFileComparison:
    """Compare one day's minute-file rows of an index with its REST minute bars.

    Args:
        file_rows: The file's rows of the index, as text (`window_start` in epoch ns, `open`,
            `high`, `low`, `close`).
        minutes: The index's REST minute bars from the day before to the day after (`t` in
            epoch ms).
        day: The file's day.

    Returns:
        The comparison.
    """
    start, end = day_window_ms(day, NEW_YORK)
    rest = {int(m["t"]): m for m in minutes}
    filed: dict[int, Prices | None] = {
        int(str(row["window_start"])) // _NS_PER_MS: prices(row) for row in file_rows
    }
    stamps = sorted(filed)
    differing = []
    for stamp in stamps:
        own = filed[stamp]
        other = prices(rest[stamp]) if stamp in rest else None
        if own is not None and other is not None and relative_error(own, other) > MATCH:
            differing.append((stamp, own, other))
    return MinuteFileComparison(
        rows=len(stamps),
        first_ms=stamps[0] if stamps else None,
        last_ms=stamps[-1] if stamps else None,
        lacks=tuple(s for s in sorted(rest) if start <= s < end and s not in filed),
        adds=tuple(s for s in stamps if s not in rest),
        outside=tuple(s for s in stamps if not start <= s < end),
        differing=tuple(differing),
    )


WINDOW_VERDICTS: Final = frozenset({"New York day", "Chicago day"})
"""The verdicts that name one window (`window_verdict`)."""


@dataclass
class IndexTally:
    """One index's verdicts over the days read, summed for its summary line.

    The files can depart from REST in three ways, counted apart because they mean different
    things: a day-file row that covers another window than the range bar is a question of
    windows, not of data; a day-file row that matches neither window, or minute-file prices that
    differ from REST's, are wrong values; and minutes the file lacks or adds are gaps.

    Attributes:
        index: The index ticker.
        verdicts: How many days each source got each verdict (`window_verdict`), by source.
        largest: Each source's largest relative error to its closest window over the days.
        windows_differ_days: The days whose New York and Chicago windows held different
            minutes (`windows_differ`); an index with none is a regular-hours index.
        compared_days: The days on which at least one file was read and held the index.
        absent_days: The days on which no file read held a row of the index: the files lack
            it, which is neither a window question nor wrong values.
        other_window_days: The days whose day-file row matched one window and the range bar
            the other.
        no_window_days: The days whose day-file row matched neither window.
        minute_gap_days: The days whose minute file had gaps (`MinuteFileComparison.has_gaps`).
        minute_value_days: The days whose minute file had prices other than REST's.
    """

    index: str
    verdicts: dict[str, Counter[str]] = field(default_factory=dict)
    largest: dict[str, float] = field(default_factory=dict)
    windows_differ_days: set[date] = field(default_factory=set)
    compared_days: set[date] = field(default_factory=set)
    absent_days: set[date] = field(default_factory=set)
    other_window_days: set[date] = field(default_factory=set)
    no_window_days: set[date] = field(default_factory=set)
    minute_gap_days: set[date] = field(default_factory=set)
    minute_value_days: set[date] = field(default_factory=set)

    def add_verdict(self, source: str, verdict: str, error: float | None) -> None:
        """Count one day's verdict of one source and keep its largest error.

        Args:
            source: The source's name, such as `"range"` or `"/prev"`.
            verdict: The verdict (`window_verdict`).
            error: The relative error to the closest window; None without one.
        """
        self.verdicts.setdefault(source, Counter())[verdict] += 1
        if error is not None:
            self.largest[source] = max(error, self.largest.get(source, 0.0))

    def add_files(
        self,
        day: date,
        *,
        range_verdict: str,
        day_file: str | None,
        minute_file: MinuteFileComparison | None,
    ) -> None:
        """Count one day's file findings.

        Args:
            day: The day.
            range_verdict: The range bar's verdict that day (`window_verdict`).
            day_file: The day-file row's verdict (`window_verdict`; `"no bar"` when the file
                lacks the index); None when the file was not published.
            minute_file: The minute file's comparison; None when the file was not published.
        """
        if day_file is None and minute_file is None:
            return
        if day_file in (None, "no bar") and (minute_file is None or not minute_file.rows):
            self.absent_days.add(day)
            return
        self.compared_days.add(day)
        if day_file == "neither day":
            self.no_window_days.add(day)
        elif {day_file, range_verdict} <= WINDOW_VERDICTS and day_file != range_verdict:
            self.other_window_days.add(day)
        if minute_file is not None and minute_file.has_gaps:
            self.minute_gap_days.add(day)
        if minute_file is not None and minute_file.values_differ:
            self.minute_value_days.add(day)

    @property
    def wrong_value_days(self) -> set[date]:
        """The days a file held values no REST window has: no window, or other minute prices."""
        return self.no_window_days | self.minute_value_days

    @property
    def around_the_clock(self) -> bool:
        """Whether its New York and Chicago windows differed on any day read."""
        return bool(self.windows_differ_days)

    def range_window(self) -> str:
        """Say which window the range day bars covered on every day that tells them apart.

        Returns:
            `"Chicago day"` or `"New York day"` when every day that tells them apart got that
            verdict; `"mixed or neither"` otherwise; `"no day tells"` for an index whose
            windows never differed.
        """
        if not self.around_the_clock:
            return "no day tells"
        telling = set(self.verdicts.get(RANGE_SOURCE, Counter())) & {
            "New York day",
            "Chicago day",
            "neither day",
        }
        return (
            telling.pop() if telling in ({"New York day"}, {"Chicago day"}) else "mixed or neither"
        )

    def line(self) -> str:
        """Describe the index in one evidence line.

        Returns:
            Such as `"I:VIX (around the clock): range: Chicago day 5; ...; largest relative
            error to the closest window: range 0.00e+00, ...; of 5 days with files: day file
            in another window than the range 0, in no window 0, minute gaps 1, minute values
            differ 0; absent from the files on 0"`.
        """
        kind = "around the clock" if self.around_the_clock else "regular hours"
        sources = "; ".join(
            f"{source}: " + ", ".join(f"{v} {n}" for v, n in sorted(counts.items()))
            for source, counts in self.verdicts.items()
        )
        errors = ", ".join(f"{source} {error:.2e}" for source, error in self.largest.items())
        return (
            f"{self.index} ({kind}): {sources}; largest relative error to the closest window: "
            f"{errors or 'none'}; of {len(self.compared_days)} days with files: day file in "
            f"another window than the range {len(self.other_window_days)}, in no window "
            f"{len(self.no_window_days)}, minute gaps {len(self.minute_gap_days)}, minute values "
            f"differ {len(self.minute_value_days)}; absent from the files on "
            f"{len(self.absent_days)}"
        )


def _named(tallies: Sequence[IndexTally], days: Callable[[IndexTally], set[date]]) -> list[str]:
    """Return the indices whose tally has any day in one of its day sets.

    Returns:
        The index tickers, in order.
    """
    return [t.index for t in tallies if days(t)]


def summary_lines(tallies: Sequence[IndexTally]) -> list[str]:
    """Sum the indices into one line each and one verdict per question.

    Args:
        tallies: Every index's tally.

    Returns:
        A line per index, then three verdict lines: (a) whether REST day bars cover the Chicago
        day, among the indices whose windows differ; (b) for the regular-hours and the
        around-the-clock indices apart, which day files cover another window than the range,
        which match no window, which minute files have gaps, and which have other prices, then
        the indices the files lack; (c) whether `I:AAVE100`'s wrong values (a day file in no
        window, or other minute prices) are isolated.
    """
    lines = [tally.line() for tally in tallies]
    telling = [t for t in tallies if t.around_the_clock]
    regular = [t for t in tallies if not t.around_the_clock]
    windows = Counter(t.range_window() for t in telling)
    lines.append(
        f"(a) REST day bars cover the Chicago day for {windows['Chicago day']} of {len(telling)} "
        f"indices whose New York and Chicago days differ (New York day {windows['New York day']}, "
        f"mixed or neither {windows['mixed or neither']}); {len(regular)} regular-hours "
        "indices cannot tell the two apart"
    )
    kinds = (
        ("day file in another window than the range", lambda t: t.other_window_days),
        ("day file in no window", lambda t: t.no_window_days),
        ("minute-file gaps", lambda t: t.minute_gap_days),
        ("minute-file prices differ", lambda t: t.minute_value_days),
    )
    parts = [
        f"{name}: regular hours {_named(regular, days)} of {len(regular)}, around the clock "
        f"{_named(telling, days)} of {len(telling)}"
        for name, days in kinds
    ]
    lines.append(
        f"(b) {'; '.join(parts)}; absent from the files: {_named(tallies, lambda t: t.absent_days)}"
    )
    target = next((t for t in tallies if t.index == ROUND_THE_CLOCK), None)
    others = [t.index for t in telling if t.wrong_value_days and t.index != ROUND_THE_CLOCK]
    if target is None:
        verdict = f"not read: {ROUND_THE_CLOCK} was not among the indices"
    elif not target.wrong_value_days:
        verdict = "no wrong values seen on these days"
    elif others:
        verdict = f"not isolated: other around-the-clock indices have wrong values too {others}"
    else:
        verdict = "isolated: no other around-the-clock index has wrong values"
    gaps = (
        ""
        if target is None
        else f"; its minute files have gaps on {len(target.minute_gap_days)} days"
    )
    lines.append(f"(c) {ROUND_THE_CLOCK}'s mismatch: {verdict}{gaps}")
    return lines
