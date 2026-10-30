"""The live run's `stream-day-reconcile` group: a stream day's minute bars against history.

On a trading day after a stream day, each unit's streamed minute bars (the stream day's
`rows/*-minute-bars-*.parquet`) are compared with the same symbols' minute bars of that day
from REST, `history(<day>, <day>, source=dm.HistorySource.REST)`; stock bars are fetched with
`adjusted=False`, since live bars are as traded. Per symbol it counts the bars on each side,
the minutes only one side has, and the share of bars present on both whose open, high, low,
close, and volume match exactly. The rows go to `reconciliation.csv`, the totals to
`summary.json`. The match share is a finding, not a verdict: a unit passes when its comparison
completed.

Runs in a child process with an embedded Deephaven server and an open session.
"""

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING, Any, Final

import deephaven_massive as dm
from tests._live.stream_capture import append_csv, rows_of

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from pathlib import Path

    from deephaven.table import Table

    from deephaven_massive import MassiveSession

FIELDS: Final = ("Open", "High", "Low", "Close", "Volume")
"""The bar fields compared."""

UNIT_STREAMS: Final = {
    "stocks": ("stocks-minute-bars",),
    "options": ("options-minute-bars",),
    "currencies": ("forex-minute-bars", "crypto-minute-bars"),
    "indices": ("indices-minute-bars",),
    "futures": ("futures-minute-bars",),
}
"""The minute-bar streams of each class's unit."""


def _history(m: MassiveSession, stream: str, *, symbols: list[str], day: date) -> Table:
    """Build the REST history query of a stream's symbols on a day.

    Args:
        m: The session.
        stream: The stream, such as `"stocks-minute-bars"`.
        symbols: Its symbols.
        day: The stream day.

    Returns:
        The day's minute bars, as a static table.
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
    table: Table = queries[stream]().history(day, day, source=rest)
    return table


def streamed_bars(folder: Path, stream: str) -> dict[tuple[str, int], tuple[Any, ...]]:
    """Read a stream's bars from its hourly Parquet files, the last of each minute winning.

    Args:
        folder: The stream day's attempt folder.
        stream: The stream.

    Returns:
        Each bar's fields by `(Symbol, TimestampStart in epoch ns)`.
    """
    parquet = importlib.import_module("pyarrow.parquet")
    bars: dict[tuple[str, int], tuple[Any, ...]] = {}
    for path in sorted((folder / "rows").glob(f"{stream}-*.parquet")):
        table = parquet.read_table(path)
        starts = table.column("TimestampStart").cast("int64").to_pylist()
        symbols = table.column("Symbol").to_pylist()
        values = list(zip(*(table.column(f).to_pylist() for f in FIELDS), strict=True))
        for symbol, start, value in zip(symbols, starts, values, strict=True):
            bars[symbol, start] = value
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
        report: Receives each unit's name, status, and reason.
    """
    summary: dict[str, Any] = {"day": day.isoformat(), "streams": {}}
    for unit in units:
        for stream in UNIT_STREAMS[unit.split()[0]]:
            streamed = streamed_bars(stream_folder, stream)
            symbols = sorted({symbol for symbol, _ in streamed})
            fetched: dict[tuple[str, int], tuple[Any, ...]] = {}
            if symbols:
                for row in rows_of(_history(m, stream, symbols=symbols, day=day)):
                    start = int(row["TimestampStart"].timestamp() * 1e9)
                    fetched[row["Symbol"], start] = tuple(row[f] for f in FIELDS)
            rows = compare(streamed, fetched)
            append_csv(folder / "reconciliation.csv", [{"stream": stream} | r for r in rows])
            both = sum(r["both"] for r in rows)
            matched = sum(round((r["match_share"] or 0) * r["both"]) for r in rows)
            summary["streams"][stream] = {
                "symbols": len(rows),
                "bars_on_both_sides": both,
                "match_share": round(matched / both, 4) if both else None,
            }
        report(unit, "passed", "")
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
