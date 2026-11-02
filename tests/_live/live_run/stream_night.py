"""The live run's `stream-night` group: crypto and futures streams through one night.

A stream day ends at 20:30 New York time, so it never holds a stream through the hours only
24-hour markets trade, when the library dates rows across two boundaries: crypto's `Date` is
the UTC date, which flips at 00:00 UTC, and a futures row's `Date` is its session, which the
library takes from Massive's futures schedules and which flips when the evening's session closes
at 16:00 Chicago time: a row between that close and the next session's 17:00 open belongs to
the next session. A stream night holds, from 16:30 New York time on a planned evening to
09:30 the next morning (`timetable.night_start`, `timetable.night_end`), through the library's
public verbs:

- **Crypto** (`crypto` unit, the Currencies subscription): trades, quotes, and minute bars of
  `X:BTCUSD` and `X:ETHUSD`, which trade around the clock.
- **Futures** (`futures` unit): minute bars of the front ES and NQ contracts of the morning's
  session, and from Futures Developer up their trades and quotes. Futures pause from 16:00 to
  17:00 Chicago time and, on a Sunday, open for the week at 17:00.

The rows, samples, and gaps are recorded as a stream day records them (`stream_capture`:
`rows/`, `minutes.csv`, `latency.csv`, `samples-*.csv`, `gaps.csv`), and a child that starts
again the same night appends to the same folder. At the night's end the last child writes:

- `hours.csv`: the rows each stream recorded per UTC hour of arrival;
- `boundaries.csv`: for each boundary, the rows within `BOUNDARY_WINDOW` before and after it
  by their own time, their `Date` values, the `Date` expected on that side, and how many rows
  carry another (a null `Date` included);
- `summary.json`: the night, its gaps, the most reconnects of each connection, each unit's
  quiet hours, the boundary rows, and any `rows/` file that could not be read.

A unit fails when one of its streams failed, the night's gaps passed 30 minutes, the recorder
could not write a batch, an hour its market was open went without a row (`quiet_hours`), or a
row near a boundary carries the wrong `Date`. The next day `stream-night-reconcile` compares
the night's minute bars with history (`reconcile.run_night_reconcile`).

Runs in a child process with an embedded Deephaven server and an open session, like the stream
day; the functions here run on its main thread, apart from the recorder's.
"""

from __future__ import annotations

import csv
import importlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

import deephaven_massive as dm
from tests._live.calendar import NEW_YORK
from tests._live.live_run import timetable
from tests._live.live_run.stream_capture import (
    CRYPTO,
    MAX_GAP,
    Clock,
    Recorder,
    Stream,
    append_csv,
    diagnostics_rows,
    failed_streams,
    held_streams,
    index_futures,
    record_gap,
    sample_until,
    total_gaps,
)
from tests._live.live_run.stream_units import delay_of

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession

CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone of the futures exchanges' sessions."""

FUTURES_PAUSE: Final = time(16, 0)
FUTURES_ROLL: Final = time(17, 0)
"""The daily pause of the index futures, Chicago time: the evening's session closes at 16:00
and the next one opens at 17:00. A row from the close on, the pause's pre-open included, gets
the next session's `Date` (`deephaven_massive._ingest.futures_calendar`)."""

BOUNDARY_WINDOW: Final = timedelta(minutes=90)
"""How far on each side of a boundary its rows are read: after the 16:00 Chicago close it covers
the pause and the next session's first half hour; before it, the ending session's last half hour
from the night's 16:30 New York start."""

FILE_SLACK: Final = timedelta(hours=2)
"""How far beyond a boundary's window the `rows/` files are read, whose hours are those of the
rows' arrival: covers a delayed unit's lag."""

MINUTE: Final = dm.Timespan.MINUTE

_HOUR: Final = timedelta(hours=1)


@dataclass(frozen=True)
class Boundary:
    """One instant of a night at which a unit's rows change `Date`.

    Attributes:
        name: What it is, such as `"16:00 Chicago close"` or `"00:00 UTC"`.
        at: The instant.
        before: The `Date` of the rows just before it, ISO; `""` when the market is closed
            just before (futures on a Sunday), so the side is recorded but not judged.
        after: The `Date` of the rows from it on, ISO.
    """

    name: str
    at: datetime
    before: str
    after: str


def futures_open(night: date) -> datetime:
    """Return when the night's next futures session opens.

    Args:
        night: The night's evening, its New York date.

    Returns:
        17:00 Chicago time that evening.
    """
    return datetime.combine(night, FUTURES_ROLL, CHICAGO)


def boundaries(unit: str, night: date) -> list[Boundary]:
    """Return the boundaries a unit's rows cross during a night.

    Args:
        unit: The unit, such as `"futures delayed"`.
        night: The night's evening.

    Returns:
        For futures, the 16:00 Chicago close, from the evening's session (none on a Sunday) to
        the morning's, which the pause from the close to the 17:00 open belongs to; for crypto,
        00:00 UTC, from the evening's UTC date to the next.
    """
    evening, morning = night.isoformat(), (night + timedelta(days=1)).isoformat()
    if unit.startswith("futures"):
        before = "" if night.weekday() == timetable.SUNDAY else evening
        close = datetime.combine(night, FUTURES_PAUSE, CHICAGO)
        return [Boundary("16:00 Chicago close", close, before, morning)]
    midnight = datetime.combine(night + timedelta(days=1), time(0), UTC)
    return [Boundary("00:00 UTC", midnight, evening, morning)]


def open_hours(unit: str, night: date) -> list[tuple[datetime, datetime]]:
    """Return when a unit's market is open during a night, by the rows' own time.

    Args:
        unit: The unit.
        night: The night's evening.

    Returns:
        Crypto: the whole night. Futures: the night from the 17:00 Chicago open, and before
        it, on an evening other than Sunday's, the night's start to the 16:00 Chicago pause.
    """
    start, end = timetable.night_start(night), timetable.night_end(night)
    if not unit.startswith("futures"):
        return [(start, end)]
    after = (futures_open(night), end)
    if night.weekday() == timetable.SUNDAY:
        return [after]
    return [(start, datetime.combine(night, FUTURES_PAUSE, CHICAGO)), after]


def night_streams(units: list[str], *, tiers: dict[str, str], night: date) -> list[Stream]:
    """Return a night's streams for its due units.

    Args:
        units: The due units, such as `["crypto real-time", "futures delayed"]`.
        tiers: Each class's tier: Futures Starter streams no trades or quotes.
        night: The night's evening; the futures contracts are the morning session's front ones.

    Returns:
        The streams, in the units' order.
    """
    contracts = index_futures(night + timedelta(days=1))
    streams: list[Stream] = []
    for unit in units:
        if unit.startswith("crypto"):
            streams += [
                _ticks("crypto-trades", unit, lambda m: m.crypto.trades(CRYPTO)),
                _ticks("crypto-quotes", unit, lambda m: m.crypto.quotes(CRYPTO)),
                Stream("crypto-minute-bars", unit, lambda m: m.crypto.bars(CRYPTO, MINUTE)),
            ]
            continue
        streams.append(
            Stream("futures-minute-bars", unit, lambda m: m.futures.bars(contracts, MINUTE))
        )
        if tiers["futures"] != "starter":
            streams += [
                _ticks("futures-trades", unit, lambda m: m.futures.trades(contracts)),
                _ticks("futures-quotes", unit, lambda m: m.futures.quotes(contracts)),
            ]
    return streams


def _ticks(name: str, unit: str, query: Callable[[MassiveSession], Any]) -> Stream:
    """Return a tick stream: dated by its `Timestamp`, held only as a `blink()` table."""
    return Stream(name, unit, query, event_column="Timestamp", held=False)


def run_stream_night(
    m: MassiveSession,
    *,
    units: list[str],
    tiers: dict[str, str],
    night: date,
    folder: Path,
    until: datetime,
    report: Callable[[str, str, str], None],
    clock: Clock | None = None,
) -> None:
    """Stream the due units until the night's end, then judge each unit.

    Args:
        m: The session.
        units: The due units.
        tiers: Each class's tier.
        night: The night's evening, its New York date.
        folder: The attempt's folder, shared by every child of the same night.
        until: When the night ends (09:30 New York time the next morning).
        report: Receives each unit's name, status, and reason at the end.
        clock: The time source and wait; the real clock by default.
    """
    clock = clock or Clock()
    record_gap(folder, clock.now(), day_start=timetable.night_start(night))
    streams = night_streams(units, tiers=tiers, night=night)
    recorder = Recorder(folder)
    with held_streams(m, streams, recorder):
        sample_until(m, folder, until=until, clock=clock, recorder=recorder)
        failures = failed_streams(diagnostics_rows(m, m.streams()))
    summary = summarize_night(folder, streams, units=units, night=night, end=clock.now())
    gaps = total_gaps(folder)
    for unit in units:
        status, reason = night_verdict(
            failure=failures.get("currencies" if unit.startswith("crypto") else "futures", ""),
            gaps=gaps,
            errors=recorder.errors,
            quiet=[datetime.fromisoformat(h) for h in summary["quiet_hours"][unit]],
            wrong=[b for b in summary["boundaries"] if b["unit"] == unit and b["wrong"]],
        )
        report(unit, status, reason)


def summarize_night(
    folder: Path, streams: list[Stream], *, units: list[str], night: date, end: datetime
) -> dict[str, Any]:
    """Write a night's `hours.csv`, `boundaries.csv`, and `summary.json`, and return the summary.

    Args:
        folder: The night's attempt folder, with every child's files.
        streams: The night's streams.
        units: The due units.
        night: The night's evening.
        end: When the night's recording ended; later hours are not judged.

    Returns:
        The summary, as written to `summary.json`.
    """
    hours = hourly_rows(folder)
    append_csv(
        folder / "hours.csv",
        [{"stream": s, "hour": h, "rows": n} for (s, h), n in sorted(hours.items())],
    )
    gaps = gap_intervals(folder)
    unreadable: list[str] = []
    found: list[dict[str, Any]] = []
    quiet: dict[str, list[str]] = {}
    for unit in units:
        own = [s for s in streams if s.unit == unit]
        rows = Counter[str]()
        for (name, hour), count in hours.items():
            if any(s.name == name for s in own):
                rows[hour] += count
        quiet[unit] = [
            h.isoformat() for h in quiet_hours(unit, night, rows=rows, gaps=gaps, end=end)
        ]
        for boundary in boundaries(unit, night):
            for stream in own:
                found += boundary_rows(folder, stream, boundary, unreadable=unreadable)
    append_csv(folder / "boundaries.csv", found)
    summary = {
        "night": night.isoformat(),
        "start": timetable.night_start(night).isoformat(),
        "end": timetable.night_end(night).isoformat(),
        "gaps_minutes": round(total_gaps(folder).total_seconds() / 60),
        "reconnects": reconnects(folder),
        "quiet_hours": quiet,
        "boundaries": found,
        "unreadable": sorted(set(unreadable)),
    }
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary


def hourly_rows(folder: Path) -> dict[tuple[str, str], int]:
    """Add up `minutes.csv` by stream and UTC hour of arrival.

    Args:
        folder: The night's attempt folder.

    Returns:
        The rows of each `(stream, hour)`, the hour as `"2026-11-04T22"`; empty without the file.
    """
    path = folder / "minutes.csv"
    if not path.exists():
        return {}
    found: Counter[tuple[str, str]] = Counter()
    with path.open(encoding="utf-8") as file:
        for row in csv.DictReader(file):
            found[row["stream"], row["minute"][:13]] += int(row["rows"])
    return dict(found)


def gap_intervals(folder: Path) -> list[tuple[datetime, datetime]]:
    """Read the gaps recorded in `gaps.csv`.

    Args:
        folder: The night's attempt folder.

    Returns:
        Each gap's start and end; empty without the file.
    """
    path = folder / "gaps.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        return [
            (datetime.fromisoformat(r["start"]), datetime.fromisoformat(r["end"]))
            for r in csv.DictReader(file)
        ]


def quiet_hours(
    unit: str,
    night: date,
    *,
    rows: Counter[str],
    gaps: list[tuple[datetime, datetime]],
    end: datetime,
) -> list[datetime]:
    """Return the hours a unit's market was open and its streams recorded no row.

    An hour is a whole UTC hour of arrival inside the night and before `end`. It is judged when
    the market was open through all of it, by the rows' own time (the hour moved back by the
    unit's delay, `open_hours`), and no recorded gap touches it.

    Args:
        unit: The unit.
        night: The night's evening.
        rows: The unit's rows per hour of arrival, such as `{"2026-11-04T22": 512}`.
        gaps: The night's gaps (`gap_intervals`).
        end: When the recording ended.

    Returns:
        The start of each quiet hour, in UTC.
    """
    start = timetable.night_start(night).astimezone(UTC)
    last = min(timetable.night_end(night), end)
    hour = start.replace(minute=0, second=0, microsecond=0)
    hour += _HOUR if hour < start else timedelta()
    delay = delay_of(unit)
    markets = open_hours(unit, night)
    quiet: list[datetime] = []
    while hour + _HOUR <= last:
        first, final = hour - delay, hour + _HOUR - delay
        trading = any(a <= first and final <= b for a, b in markets)
        gapped = any(a < hour + _HOUR and hour < b for a, b in gaps)
        if trading and not gapped and not rows[f"{hour:%Y-%m-%dT%H}"]:
            quiet.append(hour)
        hour += _HOUR
    return quiet


def _row_hour(stream: str, name: str) -> datetime:
    """Return the UTC hour of arrival a `rows/` file of a stream holds.

    Args:
        stream: The stream.
        name: The file's name, such as `"crypto-trades-2026-11-04T22.parquet"` or a restarted
            child's `"crypto-trades-2026-11-04T22-4121.parquet"`.

    Returns:
        The hour.
    """
    label = name[len(stream) + 1 :][:13]
    return datetime.strptime(label, "%Y-%m-%dT%H").replace(tzinfo=UTC)


def boundary_rows(
    folder: Path, stream: Stream, boundary: Boundary, *, unreadable: list[str]
) -> list[dict[str, Any]]:
    """Count a stream's rows on each side of a boundary, by their `Date`.

    A row's side is decided by its own time: `TimestampStart` for bars, `Timestamp` for ticks.
    A file a killed child left unfinished cannot be read: it is skipped and named.

    Args:
        folder: The night's attempt folder.
        stream: The stream.
        boundary: The boundary.
        unreadable: Receives the name of each file that could not be read.

    Returns:
        Two rows, `before` and `after`: the unit, stream, boundary, its instant, the side, the
        rows within `BOUNDARY_WINDOW` on it, their `Date` values with counts (such as
        `"2026-11-04=512"`), the `Date` expected there, and how many rows carry another.
    """
    parquet = importlib.import_module("pyarrow.parquet")
    first, last = boundary.at - BOUNDARY_WINDOW, boundary.at + BOUNDARY_WINDOW
    edge = int(boundary.at.timestamp() * 1e9)
    sides: dict[str, Counter[str]] = {"before": Counter(), "after": Counter()}
    for path in sorted((folder / "rows").glob(f"{stream.name}-*.parquet")):
        hour = _row_hour(stream.name, path.name)
        if not first - FILE_SLACK <= hour <= last + FILE_SLACK:
            continue
        try:
            table = parquet.read_table(path)
        except (OSError, ValueError):  # pyarrow's ArrowInvalid is a ValueError
            unreadable.append(path.name)
            continue
        column = "TimestampStart" if "TimestampStart" in table.column_names else "Timestamp"
        stamps = table.column(column).cast("int64").to_pylist()
        for stamp, day in zip(stamps, table.column("Date").to_pylist(), strict=True):
            if first.timestamp() * 1e9 <= stamp < last.timestamp() * 1e9:
                sides["before" if stamp < edge else "after"][day or "null"] += 1
    found = []
    for side, expected in (("before", boundary.before), ("after", boundary.after)):
        dates = sides[side]
        wrong = sum(n for day, n in dates.items() if day != expected) if expected else 0
        found.append(
            {
                "unit": stream.unit,
                "stream": stream.name,
                "boundary": boundary.name,
                "at": boundary.at.astimezone(UTC).isoformat(),
                "side": side,
                "rows": sum(dates.values()),
                "dates": " ".join(f"{day}={n}" for day, n in sorted(dates.items())),
                "expected": expected,
                "wrong": wrong,
            }
        )
    return found


def reconnects(folder: Path) -> dict[str, int]:
    """Return the most reconnects each connection reported in the night's samples.

    Args:
        folder: The night's attempt folder.

    Returns:
        By connection, such as `"crypto realtime"` (its `AssetClass` and `Recency`), the
        highest `Reconnects` sampled; a restarted child's connections count from 0 again.
    """
    path = folder / "samples-connections.csv"
    if not path.exists():
        return {}
    found: dict[str, int] = {}
    with path.open(encoding="utf-8") as file:
        for row in csv.DictReader(file):
            name = f"{row['AssetClass']} {row['Recency']}".strip()
            found[name] = max(found.get(name, 0), int(row["Reconnects"] or 0))
    return found


def night_verdict(
    *,
    failure: str,
    gaps: timedelta,
    errors: list[str],
    quiet: list[datetime],
    wrong: list[dict[str, Any]],
) -> tuple[str, str]:
    """Decide one unit's result at the end of a night.

    Args:
        failure: The error of its class's first failed stream, `""` for none.
        gaps: The night's gaps.
        errors: The recorder's errors.
        quiet: Its quiet hours (`quiet_hours`).
        wrong: Its boundary rows (`boundary_rows`) with rows of an unexpected `Date`.

    Returns:
        `("passed", "")` or `("passed", "gaps N")`, or `("failed", why)`.
    """
    minutes = round(gaps.total_seconds() / 60)
    if failure:
        return "failed", f"a stream failed: {failure}"
    if gaps > MAX_GAP:
        return "failed", f"gaps {minutes}"
    if errors:
        return "failed", errors[0]
    if quiet:
        first = quiet[0].astimezone(NEW_YORK)
        return "failed", (
            f"no rows in {len(quiet)} hour(s) its market was open, the first from "
            f"{first:%a %H:%M %Z}"
        )
    if wrong:
        w = wrong[0]
        return "failed", (
            f"{w['stream']}: {w['wrong']} rows {w['side']} {w['boundary']} dated "
            f"{w['dates']}, not {w['expected']}"
        )
    return "passed", f"gaps {minutes}" if minutes else ""
