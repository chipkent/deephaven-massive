"""Tests of the live run's `stream-day-reconcile` group (`tests/_live/live_run/reconcile.py`).

A stream day's bars are written as the stream day writes them, one Parquet file per hour, and
the REST history of the same day is scripted on the fake REST server; the tests read the
comparison the group writes.
"""

from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

import deephaven_massive as dm
from tests._fakes.rest import Reply, envelope
from tests._live.live_run import reconcile

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession
    from tests._fakes.rest import FakeRestServer

DAY = date(2026, 10, 28)
OPEN = datetime(2026, 10, 28, 14, 30, tzinfo=UTC)
MINUTE_NS = 60 * 10**9


def _ns(moment: datetime) -> int:
    return int(moment.timestamp()) * 10**9


def _write_streamed(
    folder: Path,
    stream: str,
    bars: list[tuple[str, int, float]],
    *,
    name: str = "2026-10-28T14",
    arrived: int = 0,
) -> None:
    """Write a stream's bars, `(symbol, start ns, close)`, as one hour's Parquet file.

    Each row arrives `arrived` nanoseconds after the epoch, plus its position in the list.
    """
    (folder / "rows").mkdir(parents=True, exist_ok=True)
    inserts = [arrived + i for i in range(len(bars))]
    table = pa.table(
        {
            "Symbol": [b[0] for b in bars],
            "TimestampStart": pa.array([b[1] for b in bars], pa.timestamp("ns", tz="UTC")),
            "TimestampInsert": pa.array(inserts, pa.timestamp("ns", tz="UTC")),
            "Open": [1.0] * len(bars),
            "High": [2.0] * len(bars),
            "Low": [0.5] * len(bars),
            "Close": [b[2] for b in bars],
            "Volume": [100.0] * len(bars),
        }
    )
    pq.write_table(table, folder / "rows" / f"{stream}-{name}.parquet")


def _rest_bar(start_ns: int, close: float) -> dict[str, object]:
    return {"t": start_ns // 10**6, "o": 1.0, "h": 2.0, "l": 0.5, "c": close, "v": 100}


def test_streamed_bars_are_compared_with_history_per_symbol_and_minute(
    fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    start = _ns(OPEN)
    streamed = tmp_path / "stream-day"
    _write_streamed(
        streamed,
        "stocks-minute-bars",
        [("AAPL", start, 1.5), ("AAPL", start + MINUTE_NS, 1.6), ("AAPL", start, 1.5)],
    )
    history = [_rest_bar(start, 1.5), _rest_bar(start + 2 * MINUTE_NS, 1.7)]
    fake_rest.reply(
        "/v2/aggs/ticker/AAPL/range/1/minute/1793160000000/1793246399999",
        # The library asks for the New York day in epoch milliseconds.
        Reply(envelope(history, ticker="AAPL", resultsCount=2, adjusted=False)),
    )
    seen: list[tuple[str, str, str]] = []
    folder = tmp_path / "reconcile"
    folder.mkdir()
    with dm.MassiveSession(dm.MassiveConfig(api_key="k", rest_base_url=fake_rest.url)) as m:
        reconcile.run_reconcile(
            m,
            units=["stocks", "indices"],
            stream_folder=streamed,
            day=DAY,
            folder=folder,
            report=lambda *result: seen.append(result),
        )
    assert seen == [
        ("stocks", "passed", ""),
        ("indices", "failed", "no streamed bars to compare"),  # nothing was compared
    ]
    assert "adjusted=false" in fake_rest.requests[-1].target
    with (folder / "reconciliation.csv").open(encoding="utf-8") as file:
        [row] = list(csv.DictReader(file))
    assert row == {
        "stream": "stocks-minute-bars",
        "symbol": "AAPL",
        "streamed": "2",
        "fetched": "2",
        "only_streamed": "1",
        "only_fetched": "1",
        "both": "1",
        "match_share": "1.0",
    }
    summary = json.loads((folder / "summary.json").read_text())
    assert summary == {
        "day": "2026-10-28",
        "streams": {
            "stocks-minute-bars": {"symbols": 1, "bars_on_both_sides": 1, "match_share": 1.0},
            "indices-minute-bars": {"symbols": 0, "bars_on_both_sides": 0, "match_share": None},
        },
        "unreadable": [],
    }


def test_an_unreadable_hour_is_skipped_and_named(tmp_path: Path) -> None:
    start = _ns(OPEN)
    streamed = tmp_path / "stream-day"
    _write_streamed(streamed, "stocks-minute-bars", [("AAPL", start, 1.5)])
    (streamed / "rows" / "stocks-minute-bars-2026-10-28T15.parquet").write_bytes(b"PAR1 cut")
    unreadable: list[str] = []
    bars = reconcile.streamed_bars(streamed, "stocks-minute-bars", unreadable=unreadable)
    assert list(bars) == [("AAPL", start)]
    assert unreadable == ["stocks-minute-bars-2026-10-28T15.parquet"]


def test_a_symbol_on_one_side_only_has_no_match_share() -> None:
    rows = reconcile.compare({("A", 1): (1.0,)}, {("B", 1): (1.0,), ("A", 1): (2.0,)})
    assert [(r["symbol"], r["both"], r["match_share"]) for r in rows] == [
        ("A", 1, 0.0),
        ("B", 0, None),
    ]


class _Session:
    """Stands in for a session: every attribute and call is recorded and returns itself."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def __getattr__(self, name: str) -> _Session:
        self._calls.append(name)
        return self

    def __call__(self, *args: object, **kwargs: object) -> _Session:
        self._calls.append(repr((args, kwargs)))
        return self


def test_each_stream_reads_its_own_class_history() -> None:
    expected: dict[str, Callable[[list[str]], bool]] = {
        "stocks-minute-bars": lambda calls: "stocks" in calls and "adjusted" in calls[2],
        "options-minute-bars": lambda calls: "options" in calls,
        "forex-minute-bars": lambda calls: "forex" in calls,
        "crypto-minute-bars": lambda calls: "crypto" in calls,
        "indices-minute-bars": lambda calls: "indices" in calls,
        "futures-minute-bars": lambda calls: "futures" in calls,
    }
    for stream, check in expected.items():
        calls: list[str] = []
        session: MassiveSession = _Session(calls)  # type: ignore[assignment]  # records the verbs
        reconcile._history(session, stream, symbols=["X"], first=DAY, last=DAY)
        assert check(calls), (stream, calls)
        assert calls[-2] == "history"


def test_index_bars_are_compared_without_a_volume(
    fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    start = _ns(OPEN)
    streamed = tmp_path / "stream-day"
    (streamed / "rows").mkdir(parents=True)
    bars = pa.table(
        {
            "Symbol": ["I:NDX"],
            "TimestampStart": pa.array([start], pa.timestamp("ns", tz="UTC")),
            "TimestampInsert": pa.array([start], pa.timestamp("ns", tz="UTC")),
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
        }
    )
    pq.write_table(bars, streamed / "rows" / "indices-minute-bars-2026-10-28T14.parquet")
    history = [{"t": start // 10**6, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5}]
    fake_rest.reply(
        "/v2/aggs/ticker/I%3ANDX/range/1/minute/1793160000000/1793246399999",
        Reply(envelope(history, ticker="I:NDX", resultsCount=1)),
    )
    seen: list[tuple[str, str, str]] = []
    folder = tmp_path / "reconcile"
    folder.mkdir()
    with dm.MassiveSession(dm.MassiveConfig(api_key="k", rest_base_url=fake_rest.url)) as m:
        reconcile.run_reconcile(
            m,
            units=["indices"],
            stream_folder=streamed,
            day=DAY,
            folder=folder,
            report=lambda *result: seen.append(result),
        )
    assert seen == [("indices", "passed", "")]
    summary = json.loads((folder / "summary.json").read_text())
    assert summary["streams"]["indices-minute-bars"]["match_share"] == 1.0


def test_the_latest_arrival_of_a_bar_wins_across_a_restart(tmp_path: Path) -> None:
    start = _ns(OPEN)
    # A restarted child's file sorts before the first child's, yet holds the later correction.
    _write_streamed(tmp_path, "stocks-minute-bars", [("AAPL", start, 1.5)], arrived=100)
    _write_streamed(
        tmp_path,
        "stocks-minute-bars",
        [("AAPL", start, 1.6)],
        name="2026-10-28T14-123",
        arrived=200,
    )
    bars = reconcile.streamed_bars(tmp_path, "stocks-minute-bars", unreadable=[])
    assert bars[("AAPL", start)][3] == 1.6


# --- the stream night -------------------------------------------------------------------------

NIGHT = date(2026, 10, 28)
"""A Wednesday on New York's summer time: the night runs 20:30 to 13:30 UTC."""

MIDNIGHT = datetime(2026, 10, 29, tzinfo=UTC)


def _night_bars(folder: Path, bars: list[tuple[datetime, str]]) -> None:
    """Write a night's streamed crypto bars, `(start, Date)`, all of `X:BTCUSD`."""
    (folder / "rows").mkdir(parents=True, exist_ok=True)
    starts = [_ns(start) for start, _ in bars]
    table = pa.table(
        {
            "Symbol": ["X:BTCUSD"] * len(bars),
            "TimestampStart": pa.array(starts, pa.timestamp("ns", tz="UTC")),
            "TimestampInsert": pa.array(starts, pa.timestamp("ns", tz="UTC")),
            "Open": [1.0] * len(bars),
            "High": [2.0] * len(bars),
            "Low": [0.5] * len(bars),
            "Close": [1.5] * len(bars),
            "Volume": [100.0] * len(bars),
            "Date": [day for _, day in bars],
        }
    )
    pq.write_table(table, folder / "rows" / "crypto-minute-bars-2026-10-28T23.parquet")


def _night_reconcile(
    fake_rest: FakeRestServer, streamed: Path, folder: Path
) -> list[tuple[str, str, str]]:
    history = [
        _rest_bar(_ns(at), 1.5)
        for at in (
            MIDNIGHT - timedelta(minutes=1),
            MIDNIGHT,
            MIDNIGHT + timedelta(minutes=1),
            MIDNIGHT + timedelta(hours=2),  # streamed nowhere
            MIDNIGHT + timedelta(hours=15),  # after the night's end: left out
        )
    ]
    fake_rest.reply(
        "/v2/aggs/ticker/X%3ABTCUSD/range/1/minute/1793145600000/1793318399999",
        Reply(envelope(history, ticker="X:BTCUSD", resultsCount=len(history))),
    )
    seen: list[tuple[str, str, str]] = []
    folder.mkdir()
    with dm.MassiveSession(dm.MassiveConfig(api_key="k", rest_base_url=fake_rest.url)) as m:
        reconcile.run_night_reconcile(
            m,
            units=["crypto", "futures"],
            stream_folder=streamed,
            night=NIGHT,
            folder=folder,
            report=lambda *result: seen.append(result),
        )
    return seen


def test_a_nights_bars_are_compared_over_the_night_and_at_its_boundaries(
    fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    streamed = tmp_path / "stream-night"
    _night_bars(
        streamed,
        [
            (MIDNIGHT - timedelta(hours=5), "2026-10-28"),  # before the night's start: left out
            (MIDNIGHT - timedelta(minutes=1), "2026-10-28"),
            (MIDNIGHT, "2026-10-29"),
            (MIDNIGHT + timedelta(minutes=1), "2026-10-29"),
        ],
    )
    seen = _night_reconcile(fake_rest, streamed, tmp_path / "reconcile")
    assert seen == [
        ("crypto", "passed", ""),
        ("futures", "failed", "no streamed bars to compare"),
    ]
    summary = json.loads((tmp_path / "reconcile" / "summary.json").read_text())
    crypto = summary["streams"]["crypto-minute-bars"]
    assert crypto["night"] == {
        "symbols": 1,
        "bars_on_both_sides": 3,
        "match_share": 1.0,
        "date_mismatches": 0,
    }
    assert crypto["00:00 UTC before"]["bars_on_both_sides"] == 1
    assert crypto["00:00 UTC after"]["bars_on_both_sides"] == 2
    assert set(summary["streams"]["futures-minute-bars"]) == {
        "night",
        "16:00 Chicago close before",
        "16:00 Chicago close after",
    }
    with (tmp_path / "reconcile" / "reconciliation.csv").open(encoding="utf-8") as file:
        night = next(r for r in csv.DictReader(file) if r["window"] == "night")
    assert (night["streamed"], night["fetched"], night["only_fetched"]) == ("3", "4", "1")


def test_a_crypto_bar_dated_other_than_its_utc_date_fails_its_unit(
    fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    streamed = tmp_path / "stream-night"
    _night_bars(
        streamed,
        [(MIDNIGHT, "2026-10-29"), (MIDNIGHT + timedelta(minutes=1), "2026-10-28")],
    )
    seen = _night_reconcile(fake_rest, streamed, tmp_path / "reconcile")
    assert seen[0] == ("crypto", "failed", "1 bars dated other than their UTC date")
    summary = json.loads((tmp_path / "reconcile" / "summary.json").read_text())
    assert summary["streams"]["crypto-minute-bars"]["00:00 UTC after"]["date_mismatches"] == 1
    with (tmp_path / "reconcile" / "reconciliation.csv").open(encoding="utf-8") as file:
        rows = [r for r in csv.DictReader(file) if r["window"] == "night"]
    assert rows[0]["date_mismatches"] == "1"


def test_a_futures_date_is_held_to_historys_and_a_crypto_date_to_its_start() -> None:
    start = _ns(MIDNIGHT)
    right, wrong = (1.0, "2026-10-29"), (1.0, "2026-10-28")
    bar = ("X:BTCUSD", start)
    # Futures: the sides must agree, whatever the date; a bar on one side only is not compared.
    assert reconcile.misdated({bar: wrong}, {bar: wrong}, utc_dated=False) == []
    assert reconcile.misdated({bar: right}, {bar: wrong}, utc_dated=False) == [bar]
    assert reconcile.misdated({bar: wrong}, {}, utc_dated=False) == []
    # Crypto: each side must carry the UTC date of the bar's start, even when they agree.
    assert reconcile.misdated({bar: right}, {bar: right}, utc_dated=True) == []
    assert reconcile.misdated({bar: wrong}, {bar: wrong}, utc_dated=True) == [bar, bar]
    assert reconcile.misdated({}, {bar: wrong}, utc_dated=True) == [bar]
    assert reconcile.utc_date(start - 1) == "2026-10-28"
