"""The live run's reconciliations: a stream day's or night's minute bars against history.

On a trading day after a stream day, each unit's streamed minute bars (the stream day's
`rows/*-minute-bars-*.parquet`) are compared with the same symbols' minute bars of that day
from REST, `history(<day>, <day>, source=dm.HistorySource.REST)`; stock bars are fetched with
`adjusted=False`, since live bars are as traded. Per symbol it counts the bars on each side,
the minutes only one side has, and the share of bars present on both whose open, high, low,
close, and volume match exactly. The rows go to `reconciliation.csv`, the totals to
`summary.json`. The match share is a finding, not a verdict: a unit passes when its comparison
completed.

`stream-night-reconcile` does the same for a stream night (`run_night_reconcile`): the crypto
and futures minute bars streamed from 16:30 New York time on the night's evening to 09:30 the
next morning, against REST history of the evening's and the morning's dates, both sides kept to
the night's hours. It also compares each bar's `Date`, which the stream takes from the futures
schedules or the UTC date and REST from Massive's own field, and repeats the comparison within
`stream_night.BOUNDARY_WINDOW` on each side of each boundary the night crosses
(`stream_night.boundaries`). A bar on both sides with another `Date` fails its unit.

Runs in a child process with an embedded Deephaven server and an open session.
"""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

import deephaven_massive as dm
from tests._live.live_run import stream_night, timetable
from tests._live.live_run.stream_capture import append_csv, rows_of

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven.table import Table

    from deephaven_massive import MassiveSession

FIELDS: Final = ("Open", "High", "Low", "Close", "Volume")
"""The bar fields compared, of every stream but the indices', which have no volume."""

INDEX_FIELDS: Final = ("Open", "High", "Low", "Close")
"""The bar fields compared of index bars."""


def fields_of(stream: str) -> tuple[str, ...]:
    """Return the fields compared of a stream's bars.

    Args:
        stream: The stream, such as `"indices-minute-bars"`.

    Returns:
        `INDEX_FIELDS` for index bars, else `FIELDS`.
    """
    return INDEX_FIELDS if stream.startswith("indices") else FIELDS


UNIT_STREAMS: Final = {
    "stocks": ("stocks-minute-bars",),
    "options": ("options-minute-bars",),
    "currencies": ("forex-minute-bars", "crypto-minute-bars"),
    "indices": ("indices-minute-bars",),
    "futures": ("futures-minute-bars",),
}
"""The minute-bar streams of each class's unit."""


def _history(
    m: MassiveSession, stream: str, *, symbols: list[str], first: date, last: date
) -> Table:
    """Build the REST history query of a stream's symbols over some days.

    Args:
        m: The session.
        stream: The stream, such as `"stocks-minute-bars"`.
        symbols: Its symbols.
        first: The first day.
        last: The last day.

    Returns:
        The days' minute bars, as a static table.
    """
    rest = dm.HistorySource.REST
    queries: dict[str, Callable[[], Any]] = {
        "stocks-minute-bars": lambda: m.stocks.bars(symbols, dm.Timespan.MINUTE, adjusted=False),
        "options-minute-bars": lambda: m.options.bars(symbols, dm.Timespan.MINUTE),
        "forex-minute-bars": lambda: m.forex.bars(symbols, dm.Timespan.MINUTE),
        "crypto-minute-bars": lambda: m.crypto.bars(symbols, dm.Timespan.MINUTE),
        "indices-minute-bars": lambda: m.indices.bars(symbols, dm.Timespan.MINUTE),
        "futures-minute-bars": lambda: m.futures.bars(symbols, dm.Timespan.MINUTE),
    }
    table: Table = queries[stream]().history(first, last, source=rest)
    return table


def streamed_bars(
    folder: Path, stream: str, *, unreadable: list[str], fields: tuple[str, ...] = ()
) -> dict[tuple[str, int], tuple[Any, ...]]:
    """Read a stream's bars from its hourly Parquet files, the latest of each minute winning.

    A bar Massive corrects arrives again for the same minute; the one with the latest
    `TimestampInsert` wins, whatever file a restarted child wrote it to.

    A file a killed child left unfinished has no footer and cannot be read: it is skipped and
    named in `unreadable`, so the rest of the day still compares.

    Args:
        folder: The stream day's attempt folder.
        stream: The stream.
        unreadable: Receives the name of each file that could not be read.
        fields: The fields to read; `fields_of(stream)` by default.

    Returns:
        Each bar's fields by `(Symbol, TimestampStart in epoch ns)`.
    """
    parquet = importlib.import_module("pyarrow.parquet")
    bars: dict[tuple[str, int], tuple[Any, ...]] = {}
    arrived: dict[tuple[str, int], int] = {}
    for path in sorted((folder / "rows").glob(f"{stream}-*.parquet")):
        try:
            table = parquet.read_table(path)
        except (OSError, ValueError):  # pyarrow's ArrowInvalid is a ValueError
            unreadable.append(path.name)
            continue
        starts = table.column("TimestampStart").cast("int64").to_pylist()
        symbols = table.column("Symbol").to_pylist()
        inserts = table.column("TimestampInsert").cast("int64").to_pylist()
        columns = (table.column(f).to_pylist() for f in fields or fields_of(stream))
        values = list(zip(*columns, strict=True))
        for symbol, start, insert, value in zip(symbols, starts, inserts, values, strict=True):
            if insert >= arrived.get((symbol, start), insert):
                bars[symbol, start], arrived[symbol, start] = value, insert
    return bars


def compare(
    streamed: dict[tuple[str, int], tuple[Any, ...]],
    fetched: dict[tuple[str, int], tuple[Any, ...]],
) -> list[dict[str, Any]]:
    """Compare streamed and fetched bars per symbol.

    Args:
        streamed: The streamed bars, by `(Symbol, start)`.
        fetched: The REST bars, by `(Symbol, start)`.

    Returns:
        One row per symbol: the bars on each side, the minutes only one side has, the bars on
        both, and the share of those whose fields all match.
    """
    rows = []
    for symbol in sorted({s for s, _ in streamed} | {s for s, _ in fetched}):
        live = {k for k in streamed if k[0] == symbol}
        rest = {k for k in fetched if k[0] == symbol}
        both = live & rest
        same = sum(streamed[k] == fetched[k] for k in both)
        rows.append(
            {
                "symbol": symbol,
                "streamed": len(live),
                "fetched": len(rest),
                "only_streamed": len(live - rest),
                "only_fetched": len(rest - live),
                "both": len(both),
                "match_share": round(same / len(both), 4) if both else None,
            }
        )
    return rows


def run_reconcile(
    m: MassiveSession,
    *,
    units: list[str],
    stream_folder: Path,
    day: date,
    folder: Path,
    report: Callable[[str, str, str], None],
) -> None:
    """Compare each due unit's streamed minute bars with REST history, and write the results.

    Args:
        m: The session.
        units: The due units.
        stream_folder: The stream day's attempt folder.
        day: The stream day.
        folder: This attempt's folder.
        report: Receives each unit's name, status, and reason: passed when its bars were
            compared, failed when the stream day left none.
    """
    unreadable: list[str] = []
    summary: dict[str, Any] = {"day": day.isoformat(), "streams": {}, "unreadable": unreadable}
    for unit in units:
        compared = 0
        for stream in UNIT_STREAMS[unit.split()[0]]:
            streamed = streamed_bars(stream_folder, stream, unreadable=unreadable)
            symbols = sorted({symbol for symbol, _ in streamed})
            fetched: dict[tuple[str, int], tuple[Any, ...]] = {}
            if symbols:
                query = _history(m, stream, symbols=symbols, first=day, last=day)
                for row in rows_of(query):
                    start = int(row["TimestampStart"].timestamp() * 1e9)
                    fetched[row["Symbol"], start] = tuple(row[f] for f in fields_of(stream))
            rows = compare(streamed, fetched)
            compared += len(symbols)
            append_csv(folder / "reconciliation.csv", [{"stream": stream} | r for r in rows])
            both = sum(r["both"] for r in rows)
            matched = sum(round((r["match_share"] or 0) * r["both"]) for r in rows)
            summary["streams"][stream] = {
                "symbols": len(rows),
                "bars_on_both_sides": both,
                "match_share": round(matched / both, 4) if both else None,
            }
        if compared:
            report(unit, "passed", "")
        else:
            report(unit, "failed", "no streamed bars to compare")
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")


NIGHT_STREAMS: Final = {"crypto": "crypto-minute-bars", "futures": "futures-minute-bars"}
"""The minute-bar stream of each night unit, by the unit's first word."""

DATED: Final = (*FIELDS, "Date")
"""The fields read of a night's bars: the compared values, then the `Date`."""


def run_night_reconcile(
    m: MassiveSession,
    *,
    units: list[str],
    stream_folder: Path,
    night: date,
    folder: Path,
    report: Callable[[str, str, str], None],
) -> None:
    """Compare each due night unit's minute bars with REST history, overall and at its boundaries.

    Args:
        m: The session.
        units: The due units, such as `["crypto real-time"]`.
        stream_folder: The night's attempt folder.
        night: The night's evening.
        folder: This attempt's folder.
        report: Receives each unit's name, status, and reason: passed when its bars were
            compared and every bar on both sides has the same `Date`; failed when the night
            left none, or a `Date` differs.
    """
    start, end = timetable.night_start(night), timetable.night_end(night)
    unreadable: list[str] = []
    summary: dict[str, Any] = {"night": night.isoformat(), "streams": {}, "unreadable": unreadable}
    for unit in units:
        stream = NIGHT_STREAMS[unit.split()[0]]
        bars = streamed_bars(stream_folder, stream, unreadable=unreadable, fields=DATED)
        streamed = _within(bars, start, end)
        symbols = sorted({symbol for symbol, _ in streamed})
        fetched: dict[tuple[str, int], tuple[Any, ...]] = {}
        if symbols:
            query = _history(
                m, stream, symbols=symbols, first=night, last=night + timedelta(days=1)
            )
            for row in rows_of(query):
                bar_start = int(row["TimestampStart"].timestamp() * 1e9)
                fetched[row["Symbol"], bar_start] = tuple(row[f] for f in DATED)
        found = {
            name: _compare_window(
                folder,
                stream,
                window=name,
                streamed=_within(streamed, *span),
                fetched=_within(fetched, *span),
            )
            for name, span in night_windows(unit, night).items()
        }
        summary["streams"][stream] = found
        mismatches = found["night"]["date_mismatches"]
        if not symbols:
            report(unit, "failed", "no streamed bars to compare")
        elif mismatches:
            report(unit, "failed", f"{mismatches} bars dated other than in history")
        else:
            report(unit, "passed", "")
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")


def night_windows(unit: str, night: date) -> dict[str, tuple[datetime, datetime]]:
    """Return the spans a night unit's bars are compared over.

    Args:
        unit: The unit.
        night: The night's evening.

    Returns:
        `"night"`, the whole night; then for each of its boundaries (`stream_night.boundaries`)
        `"<boundary> before"` and `"<boundary> after"`, `stream_night.BOUNDARY_WINDOW` long.
    """
    windows = {"night": (timetable.night_start(night), timetable.night_end(night))}
    for boundary in stream_night.boundaries(unit, night):
        edge, reach = boundary.at, stream_night.BOUNDARY_WINDOW
        windows[f"{boundary.name} before"] = (edge - reach, edge)
        windows[f"{boundary.name} after"] = (edge, edge + reach)
    return windows


def _compare_window(
    folder: Path,
    stream: str,
    *,
    window: str,
    streamed: dict[tuple[str, int], tuple[Any, ...]],
    fetched: dict[tuple[str, int], tuple[Any, ...]],
) -> dict[str, Any]:
    """Compare a night's streamed and fetched bars of one span, and append its rows.

    Args:
        folder: This attempt's folder; its `reconciliation.csv` gets one row per symbol.
        stream: The stream.
        window: The span's name: `"night"`, or a boundary and a side.
        streamed: The streamed bars in the span, read with `DATED`.
        fetched: The REST bars in the span, read with `DATED`.

    Returns:
        The span's totals: symbols, bars on both sides, their match share, and how many of
        them have another `Date` on each side.
    """
    rows = compare(_values(streamed), _values(fetched))
    dated = [k for k in streamed.keys() & fetched.keys() if streamed[k][-1] != fetched[k][-1]]
    for row in rows:
        row["date_mismatches"] = sum(symbol == row["symbol"] for symbol, _ in dated)
    append_csv(
        folder / "reconciliation.csv", [{"stream": stream, "window": window} | r for r in rows]
    )
    both = sum(r["both"] for r in rows)
    matched = sum(round((r["match_share"] or 0) * r["both"]) for r in rows)
    return {
        "symbols": len(rows),
        "bars_on_both_sides": both,
        "match_share": round(matched / both, 4) if both else None,
        "date_mismatches": len(dated),
    }


def _within(
    bars: dict[tuple[str, int], tuple[Any, ...]], first: datetime, last: datetime
) -> dict[tuple[str, int], tuple[Any, ...]]:
    """Keep the bars that start in a span of time.

    Args:
        bars: Bars by `(Symbol, start in epoch ns)`.
        first: The span's start, inclusive.
        last: Its end, exclusive.

    Returns:
        The bars that start from `first` and before `last`.
    """
    low, high = first.timestamp() * 1e9, last.timestamp() * 1e9
    return {k: v for k, v in bars.items() if low <= k[1] < high}


def _values(bars: dict[tuple[str, int], tuple[Any, ...]]) -> dict[tuple[str, int], tuple[Any, ...]]:
    """Drop the `Date` from bars read with `DATED`.

    Args:
        bars: Bars whose last field is the `Date`.

    Returns:
        The bars with only the compared values.
    """
    return {k: v[:-1] for k, v in bars.items()}
