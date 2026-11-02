"""Tests of the live run's `stream-night` group (`tests/_live/live_run/stream_night.py`).

The night of Wednesday 4 November 2026 serves as the example: New York is on EST, so the night
runs from 21:30 UTC to 14:30 UTC the next morning, the futures pause from 22:00 to 23:00 UTC,
and their session rolls at 23:00 UTC. The boundaries, the quiet hours, and the verdict are
pure and tested on files written by hand, as the recorder writes them; one night runs end to
end in a real `MassiveSession` on the fake WebSocket server, its streams swapped for a stock
bars stream the fake server serves, so it records rows, samples, and its summary in a few steps.
"""

from __future__ import annotations

import csv
import json
import threading
import time
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import deephaven_massive as dm
from tests._live.live_run import stream_night
from tests._live.live_run.stream_capture import Clock, Stream
from tests._live.live_run.stream_night import Boundary

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession
    from tests._fakes.ws_session import WsSession

NIGHT = date(2026, 11, 4)
SUNDAY = date(2026, 11, 8)
START = datetime(2026, 11, 4, 21, 30, tzinfo=UTC)
END = datetime(2026, 11, 5, 14, 30, tzinfo=UTC)
ROLL = datetime(2026, 11, 4, 23, tzinfo=UTC)
MIDNIGHT = datetime(2026, 11, 5, tzinfo=UTC)


def _utc(hour: int, minute: int = 0, *, day: int = 4) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=UTC)


def _ns(moment: datetime) -> int:
    return int(moment.timestamp()) * 10**9


def _csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _write_rows(
    folder: Path, name: str, rows: list[tuple[datetime, str | None]], *, column: str = "Timestamp"
) -> None:
    """Write rows `(own time, Date)` as one `rows/` file named `name`."""
    (folder / "rows").mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "Symbol": ["ESZ6"] * len(rows),
            column: pa.array([_ns(t) for t, _ in rows], pa.timestamp("ns", tz="UTC")),
            "Date": pa.array([d for _, d in rows], pa.string()),
        }
    )
    pq.write_table(table, folder / "rows" / name)


# --- the night's times ----------------------------------------------------------------------


def test_each_unit_crosses_its_own_boundary() -> None:
    assert stream_night.boundaries("futures delayed", NIGHT) == [
        Boundary("17:00 Chicago", ROLL, "2026-11-04", "2026-11-05")
    ]
    # On a Sunday futures open at the roll: nothing trades before it to be judged.
    [sunday] = stream_night.boundaries("futures real-time", SUNDAY)
    assert (sunday.before, sunday.after) == ("", "2026-11-09")
    assert stream_night.boundaries("crypto real-time", NIGHT) == [
        Boundary("00:00 UTC", MIDNIGHT, "2026-11-04", "2026-11-05")
    ]


def test_futures_are_open_but_for_their_pause_and_crypto_all_night() -> None:
    assert stream_night.open_hours("crypto real-time", NIGHT) == [(START, END)]
    assert stream_night.open_hours("futures delayed", NIGHT) == [
        (START, _utc(22)),
        (ROLL, END),
    ]
    [(opens, _)] = stream_night.open_hours("futures delayed", SUNDAY)
    assert opens == datetime(2026, 11, 8, 23, tzinfo=UTC)


def test_each_unit_streams_trades_quotes_and_minute_bars(massive_session: MassiveSession) -> None:
    streams = stream_night.night_streams(
        ["crypto real-time", "futures delayed"], tiers={"futures": "developer"}, night=NIGHT
    )
    assert [(s.name, s.unit, s.event_column, s.held) for s in streams] == [
        ("crypto-trades", "crypto real-time", "Timestamp", False),
        ("crypto-quotes", "crypto real-time", "Timestamp", False),
        ("crypto-minute-bars", "crypto real-time", "TimestampEnd", True),
        ("futures-minute-bars", "futures delayed", "TimestampEnd", True),
        ("futures-trades", "futures delayed", "Timestamp", False),
        ("futures-quotes", "futures delayed", "Timestamp", False),
    ]
    symbols = {s.name: s.query(massive_session)._request.symbols for s in streams}
    assert set(symbols["crypto-quotes"]) == {"X:BTCUSD", "X:ETHUSD"}
    assert set(symbols["futures-trades"]) == {"ESZ6", "NQZ6"}
    starter = stream_night.night_streams(
        ["futures delayed"], tiers={"futures": "starter"}, night=NIGHT
    )
    assert [s.name for s in starter] == ["futures-minute-bars"]  # Starter streams bars only


# --- the quiet hours --------------------------------------------------------------------------


def test_an_open_hour_without_a_row_is_quiet_unless_a_gap_covers_it() -> None:
    rows: Counter[str] = Counter()
    for hour in range(22, 24):
        rows[f"2026-11-04T{hour:02d}"] = 5
    for hour in range(14):
        rows[f"2026-11-05T{hour:02d}"] = 5
    del rows["2026-11-05T03"]
    quiet = stream_night.quiet_hours("crypto real-time", NIGHT, rows=rows, gaps=[], end=END)
    assert quiet == [_utc(3, day=5)]
    gap = [(_utc(3, 10, day=5), _utc(3, 20, day=5))]
    assert stream_night.quiet_hours("crypto real-time", NIGHT, rows=rows, gaps=gap, end=END) == []
    # An hour after the recording ended is not judged.
    early = _utc(2, day=5)
    assert stream_night.quiet_hours("crypto real-time", NIGHT, rows=rows, gaps=[], end=early) == []


def test_futures_hours_touched_by_their_pause_or_delay_are_not_judged() -> None:
    nothing: Counter[str] = Counter()
    delayed = stream_night.quiet_hours("futures delayed", NIGHT, rows=nothing, gaps=[], end=END)
    # 22:00 and 23:00 UTC arrive rows of the pause, delayed 10 minutes: judged from 00:00.
    assert delayed == [_utc(h, day=5) for h in range(14)]
    real = stream_night.quiet_hours("futures real-time", NIGHT, rows=nothing, gaps=[], end=END)
    assert real[0] == ROLL


# --- the boundaries -------------------------------------------------------------------------


def test_rows_near_a_boundary_are_counted_by_side_and_date(tmp_path: Path) -> None:
    stream = Stream("futures-trades", "futures delayed", lambda _m: None, "Timestamp", held=False)
    _write_rows(
        tmp_path,
        "futures-trades-2026-11-04T21.parquet",
        [(_utc(21, 45), "2026-11-04"), (_utc(21, 50), "2026-11-04"), (_utc(20, 0), "2026-11-04")],
    )
    _write_rows(
        tmp_path,
        "futures-trades-2026-11-04T23-4121.parquet",  # a restarted child's file
        [(_utc(23, 1), "2026-11-05"), (_utc(23, 2), None), (_utc(23, 3), "2026-11-04")],
    )
    _write_rows(  # outside the files read: its hour is far from the boundary
        tmp_path, "futures-trades-2026-11-05T12.parquet", [(_utc(23, 5), "1999-01-01")]
    )
    (tmp_path / "rows" / "futures-trades-2026-11-05T00.parquet").write_bytes(b"PAR1 cut")
    unreadable: list[str] = []
    [boundary] = stream_night.boundaries("futures delayed", NIGHT)
    before, after = stream_night.boundary_rows(tmp_path, stream, boundary, unreadable=unreadable)
    assert unreadable == ["futures-trades-2026-11-05T00.parquet"]
    assert before == {
        "unit": "futures delayed",
        "stream": "futures-trades",
        "boundary": "17:00 Chicago",
        "at": "2026-11-04T23:00:00+00:00",
        "side": "before",
        "rows": 2,  # the 20:00 row is outside the 90 minutes
        "dates": "2026-11-04=2",
        "expected": "2026-11-04",
        "wrong": 0,
    }
    assert (after["rows"], after["dates"], after["wrong"]) == (
        3,
        "2026-11-04=1 2026-11-05=1 null=1",
        2,
    )


def test_bars_take_their_side_from_their_start_and_a_closed_side_is_not_judged(
    tmp_path: Path,
) -> None:
    stream = Stream("futures-minute-bars", "futures real-time", lambda _m: None)
    roll = datetime(2026, 11, 8, 23, tzinfo=UTC)
    _write_rows(
        tmp_path,
        "futures-minute-bars-2026-11-08T22.parquet",
        [(roll - timedelta(minutes=1), "2026-11-07"), (roll, "2026-11-09")],
        column="TimestampStart",
    )
    [boundary] = stream_night.boundaries("futures real-time", SUNDAY)
    before, after = stream_night.boundary_rows(tmp_path, stream, boundary, unreadable=[])
    assert (before["rows"], before["expected"], before["wrong"]) == (1, "", 0)
    assert (after["rows"], after["wrong"]) == (1, 0)


# --- the files and the verdict ----------------------------------------------------------------


def test_the_hours_gaps_and_reconnects_are_read_from_the_nights_files(tmp_path: Path) -> None:
    assert stream_night.hourly_rows(tmp_path) == {}
    assert stream_night.gap_intervals(tmp_path) == []
    assert stream_night.reconnects(tmp_path) == {}
    (tmp_path / "minutes.csv").write_text(
        "stream,minute,rows\n"
        "crypto-trades,2026-11-04T22:01,2\n"
        "crypto-trades,2026-11-04T22:02,3\n"
        "crypto-trades,2026-11-04T23:00,1\n"
    )
    assert stream_night.hourly_rows(tmp_path) == {
        ("crypto-trades", "2026-11-04T22"): 5,
        ("crypto-trades", "2026-11-04T23"): 1,
    }
    (tmp_path / "gaps.csv").write_text(
        f"start,end,cause\n{_utc(22).isoformat()},{_utc(22, 5).isoformat()},the child restarted\n"
    )
    assert stream_night.gap_intervals(tmp_path) == [(_utc(22), _utc(22, 5))]
    (tmp_path / "samples-connections.csv").write_text(
        "time,AssetClass,Recency,Reconnects\n"
        "t,crypto,realtime,1\n"
        "t,crypto,realtime,3\n"
        "t,futures,delayed,\n"
    )
    assert stream_night.reconnects(tmp_path) == {"crypto realtime": 3, "futures delayed": 0}


def test_a_units_verdict_names_the_first_thing_wrong() -> None:
    def verdict(
        *,
        failure: str = "",
        gaps: timedelta = timedelta(),
        errors: tuple[str, ...] = (),
        quiet: tuple[datetime, ...] = (),
        wrong: tuple[dict[str, object], ...] = (),
    ) -> tuple[str, str]:
        return stream_night.night_verdict(
            failure=failure, gaps=gaps, errors=list(errors), quiet=list(quiet), wrong=list(wrong)
        )

    wrong = {
        "stream": "futures-trades",
        "wrong": 2,
        "side": "after",
        "boundary": "17:00 Chicago",
        "dates": "2026-11-04=2",
        "expected": "2026-11-05",
    }
    assert verdict() == ("passed", "")
    assert verdict(gaps=timedelta(minutes=4)) == ("passed", "gaps 4")
    assert verdict(failure="HTTP 403") == ("failed", "a stream failed: HTTP 403")
    assert verdict(gaps=timedelta(minutes=31)) == ("failed", "gaps 31")
    assert verdict(errors=("crypto-trades: KeyError",)) == ("failed", "crypto-trades: KeyError")
    assert verdict(quiet=(_utc(3, day=5), _utc(4, day=5))) == (
        "failed",
        "no rows in 2 hour(s) its market was open, the first from Wed 22:00 EST",
    )
    assert verdict(wrong=(wrong,)) == (
        "failed",
        "futures-trades: 2 rows after 17:00 Chicago dated 2026-11-04=2, not 2026-11-05",
    )


def test_the_summary_gathers_the_nights_findings(tmp_path: Path) -> None:
    streams = stream_night.night_streams(["crypto real-time"], tiers={}, night=NIGHT)
    _write_rows(
        tmp_path,
        "crypto-trades-2026-11-04T23.parquet",
        [(_utc(23, 59), "2026-11-04"), (_utc(0, 1, day=5), "2026-11-04")],
    )
    (tmp_path / "minutes.csv").write_text(
        "stream,minute,rows\n"
        "crypto-trades,2026-11-04T23:59,2\n"
        "stocks-trades,2026-11-04T22:00,4\n"  # no stream of the night: its rows count for none
    )
    summary = stream_night.summarize_night(
        tmp_path, streams, units=["crypto real-time"], night=NIGHT, end=_utc(1, 30, day=5)
    )
    assert summary["quiet_hours"] == {
        "crypto real-time": [_utc(22).isoformat(), _utc(0, day=5).isoformat()]
    }
    wrong = [b for b in summary["boundaries"] if b["wrong"]]
    assert [(b["stream"], b["side"], b["wrong"]) for b in wrong] == [("crypto-trades", "after", 1)]
    assert len(summary["boundaries"]) == 6  # three streams, two sides
    assert json.loads((tmp_path / "summary.json").read_text()) == summary
    assert _csv(tmp_path / "hours.csv") == [
        {"stream": "crypto-trades", "hour": "2026-11-04T23", "rows": "2"},
        {"stream": "stocks-trades", "hour": "2026-11-04T22", "rows": "4"},
    ]
    assert len(_csv(tmp_path / "boundaries.csv")) == 6


# --- a whole night ------------------------------------------------------------------------------


def _wait_for(condition: Callable[[], bool], what: str) -> None:
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + 15.0
    while not condition():
        assert time.monotonic() < deadline, f"waited 15 s for {what}"
        pause.wait(0.02)


class _Night:
    """A fake clock from the night's start: its first wait runs the test's action."""

    def __init__(self, action: Callable[[], None] | None = None) -> None:
        self.now_at = START
        self.action = action

    def now(self) -> datetime:
        return self.now_at

    def wait(self, seconds: float) -> bool:
        if self.action is not None:
            action, self.action = self.action, None
            action()
        self.now_at += timedelta(seconds=seconds)
        return False


@pytest.mark.jvm_held
def test_a_night_records_its_rows_and_judges_its_units(
    make_ws_session: Callable[..., WsSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def bars(m: MassiveSession) -> object:
        return m.stocks.bars(["AAPL"], dm.Timespan.MINUTE)

    stream = Stream("crypto-minute-bars", "crypto real-time", bars)
    monkeypatch.setattr(stream_night, "night_streams", lambda *_a, **_k: [stream])
    harness = make_ws_session()
    now_ms = int(time.time() * 1000)

    def publish() -> None:
        harness.subscribed("AM.AAPL")
        harness.publish(
            {
                "ev": "AM",
                "sym": "AAPL",
                "v": 100,
                "av": 1000,
                "op": 1.0,
                "vw": 1.0,
                "o": 1.0,
                "c": 1.5,
                "h": 2.0,
                "l": 0.5,
                "a": 1.0,
                "z": 10,
                "s": now_ms - 60_000,
                "e": now_ms,
            }
        )
        _wait_for(
            lambda: bool(list((tmp_path / "rows").glob("crypto-minute-bars-*.parquet"))),
            "the bars' Parquet file",
        )

    clock = _Night(publish)
    seen: list[tuple[str, str, str]] = []
    stream_night.run_stream_night(
        harness.session,
        units=["crypto real-time"],
        tiers={"currencies": "starter"},
        night=NIGHT,
        folder=tmp_path,
        until=START + timedelta(seconds=25),
        report=lambda *result: seen.append(result),
        clock=Clock(now=clock.now, wait=clock.wait),
    )
    assert seen == [("crypto real-time", "passed", "")]
    assert [r["rows"] for r in _csv(tmp_path / "hours.csv")] == ["1"]
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert (summary["night"], summary["gaps_minutes"]) == ("2026-11-04", 0)
    assert summary["quiet_hours"] == {"crypto real-time": []}
    assert summary["reconnects"]
