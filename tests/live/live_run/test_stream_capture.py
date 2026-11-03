"""Tests of the live run's streaming groups (`tests/_live/live_run/stream_capture.py`).

A stream day and a throughput hour run in a real `MassiveSession` whose WebSocket connections
reach the fake Massive server (`WsSession`), and whose REST requests reach the fake REST
server. A fake clock drives the sampling loop, so a whole day takes a few steps: at its first
wait a test publishes frames, then waits until the recorder's thread has written them; then the
test reads the files the group wrote. The recorder is also driven directly with batches of
rows, for the hours, the restarts, and the failures that frames cannot reach.

The stream-day tests are marked `jvm_held`: Deephaven's wrapper of a table listener keeps the
listened table, and so the session's results, until the JVM collects the stopped listener.
"""

from __future__ import annotations

import csv
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pyarrow.parquet as pq
import pytest

from tests._fakes.rest import Reply, envelope
from tests._live.live_run import stream_capture
from tests._live.live_run.stream_capture import Clock, Recorder, Stream

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven.table_listener import TableUpdate

    from deephaven_massive import MassiveSession
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

START = datetime(2026, 11, 4, 14, 0, tzinfo=UTC)
"""09:00 New York time on a Wednesday."""


def _wait_for(condition: Callable[[], bool], what: str) -> None:
    """Poll a condition the recorder's thread makes true, failing after 15 s."""
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + 15.0
    while not condition():
        assert time.monotonic() < deadline, f"waited 15 s for {what}"
        pause.wait(0.02)


@dataclass
class _Day:
    """A fake clock: its first wait runs the test's action, and each wait moves time on."""

    now_at: datetime = START
    action: Callable[[], None] | None = None
    waits: list[float] = field(default_factory=list)
    stop_after: int | None = None

    def now(self) -> datetime:
        return self.now_at

    def wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        if self.action is not None:
            action, self.action = self.action, None
            action()
        self.now_at += timedelta(seconds=seconds)
        return self.stop_after is not None and len(self.waits) >= self.stop_after

    def clock(self) -> Clock:
        return Clock(now=self.now, wait=self.wait)


def _bar(ev: str, symbol: str, *, start_ms: int, seconds: int) -> dict[str, object]:
    return {
        "ev": ev,
        "sym": symbol,
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
        "s": start_ms,
        "e": start_ms + seconds * 1000,
    }


def _trade(symbol: str, t_ms: int) -> dict[str, object]:
    return {"ev": "T", "sym": symbol, "x": 4, "i": "1", "z": 3, "p": 10.0, "s": 5, "t": t_ms}


def _csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _reports() -> tuple[list[tuple[str, str, str]], Callable[[str, str, str], None]]:
    seen: list[tuple[str, str, str]] = []
    return seen, lambda *result: seen.append(result)


def _ns(moment: datetime) -> np.datetime64:
    return np.datetime64(int(moment.timestamp() * 1e9), "ns")


def _batch(*inserted: datetime, lag: timedelta = timedelta(seconds=2)) -> dict[str, Any]:
    """Return rows of a tick stream, each arriving `lag` after its own time.

    Returns:
        The rows, by column, as a listener hands them over.
    """
    return {
        "Symbol": np.array(["X:BTCUSD"] * len(inserted), dtype=object),
        "Timestamp": np.array([_ns(t - lag) for t in inserted]),
        "TimestampInsert": np.array([_ns(t) for t in inserted]),
    }


_TICKS = Stream("crypto-trades", "currencies real-time", lambda _m: None, "Timestamp", held=False)


# --- the stream day -------------------------------------------------------------------------


@pytest.mark.jvm_held
def test_a_stream_day_records_rows_counts_delays_and_samples(
    make_ws_session: Callable[..., WsSession], tmp_path: Path
) -> None:
    harness = make_ws_session()
    now_ms = int(time.time() * 1000)
    day = _Day()

    def publish() -> None:
        harness.subscribed("AM.AAPL", "A.AAPL", "T.AAPL")
        harness.publish(
            _bar("AM", "AAPL", start_ms=now_ms - 60_000, seconds=60),
            _bar("A", "AAPL", start_ms=now_ms - 1000, seconds=1),
            _trade("AAPL", now_ms),
        )
        names = ("stocks-minute-bars", "stocks-second-bars", "stocks-trades")
        _wait_for(
            lambda: all(list((tmp_path / "rows").glob(f"{n}-*.parquet")) for n in names),
            "every stream's Parquet file",
        )
        # The later samples must see the streams in the session's diagnostics.
        assert harness.session._core.diagnostics.wait_until_shown(15.0)

    day.action = publish
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["stocks delayed"],
        tiers={"stocks": "developer"},
        folder=tmp_path,
        until=START + timedelta(seconds=25),
        report=report,
        day_start=START,
        clock=day.clock(),
    )
    assert seen == [("stocks delayed", "passed", "")]
    assert day.waits == [10.0, 10.0, 5.0]
    [minute_file] = (tmp_path / "rows").glob("stocks-minute-bars-*.parquet")
    assert pq.read_table(minute_file).column("Symbol").to_pylist() == ["AAPL"]
    counts = {r["stream"]: int(r["rows"]) for r in _csv(tmp_path / "minutes.csv")}
    assert counts == {"stocks-minute-bars": 1, "stocks-second-bars": 1, "stocks-trades": 1}
    latency = {r["stream"]: r for r in _csv(tmp_path / "latency.csv")}
    assert set(latency) == set(counts)
    # The trade arrived within moments of its own time; the delayed unit's documented delay
    # of 15 minutes is taken off, so its delay reads about -15 minutes.
    assert -16 * 60_000 < float(latency["stocks-trades"]["max_ms"]) < -14 * 60_000
    samples = _csv(tmp_path / "samples-process.csv")
    assert len(samples) == 3
    assert float(samples[0]["heap_committed_mb"]) > 0
    assert {r["Dataset"] for r in _csv(tmp_path / "samples-streams.csv")} == {
        "stocks.bars",
        "stocks.trades",
    }
    assert _csv(tmp_path / "samples-connections.csv")
    assert not (tmp_path / "gaps.csv").exists()


@pytest.mark.jvm_held
def test_a_batch_the_recorder_cannot_write_fails_the_day(
    make_ws_session: Callable[..., WsSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = threading.Event()

    def broken(unit: str) -> timedelta:
        called.set()
        raise ValueError(f"no delay for {unit}")

    monkeypatch.setattr(stream_capture, "delay_of", broken)
    harness = make_ws_session()
    day = _Day()

    def publish() -> None:
        harness.subscribed("AM.AAPL")
        now_ms = int(time.time() * 1000)
        harness.publish(_bar("AM", "AAPL", start_ms=now_ms - 60_000, seconds=60))
        _wait_for(called.is_set, "the recorder's try at the batch")

    day.action = publish
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["stocks delayed"],
        tiers={"stocks": "starter"},
        folder=tmp_path,
        until=START + timedelta(seconds=5),
        report=report,
        day_start=START,
        clock=day.clock(),
    )
    [(unit, status, reason)] = seen
    assert (unit, status) == ("stocks delayed", "failed")
    assert reason == "stocks-minute-bars: ValueError: no delay for stocks delayed"


@pytest.mark.jvm_held
def test_a_restarted_day_records_its_gap_and_fails_past_thirty_minutes(
    make_ws_session: Callable[..., WsSession], tmp_path: Path
) -> None:
    harness = make_ws_session()
    runs: list[list[tuple[str, str, str]]] = []
    for minutes in (0, 5, 36):
        seen, report = _reports()
        start = START + timedelta(minutes=minutes)
        stream_capture.run_stream_day(
            harness.session,
            units=["indices delayed"],
            tiers={"indices": "starter"},
            folder=tmp_path,
            until=start + timedelta(seconds=1),
            report=report,
            clock=_Day(now_at=start).clock(),
            day_start=START,
        )
        runs.append(seen)
    # The second start is 5 minutes after the first's last sample: a gap, but a short one.
    assert runs == [
        [("indices delayed", "passed", "")],
        [("indices delayed", "passed", "gaps 5")],
        [("indices delayed", "failed", "gaps 36")],
    ]
    gaps = _csv(tmp_path / "gaps.csv")
    assert [g["cause"] for g in gaps] == ["the child restarted"] * 2


@pytest.mark.jvm_held
def test_a_unit_without_a_row_after_its_session_opened_fails(
    make_ws_session: Callable[..., WsSession], tmp_path: Path
) -> None:
    """An acknowledged stream that never delivers fails its unit once its session is open.

    The index session opens at 09:30 New York time, and the delayed feed lags 15 minutes, so
    from 09:50 a stream day whose index streams recorded no row fails the unit.
    """
    harness = make_ws_session()
    late = START + timedelta(minutes=50)  # 09:50 New York time
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["indices delayed"],
        tiers={"indices": "starter"},
        folder=tmp_path,
        until=late + timedelta(seconds=1),
        report=report,
        clock=_Day(now_at=late).clock(),
        day_start=late,
    )
    assert seen == [("indices delayed", "failed", "no rows during its session's hours")]


def test_unit_rows_add_up_every_childs_rows_of_the_units_streams(tmp_path: Path) -> None:
    (tmp_path / "minutes.csv").write_text(
        "stream,minute,rows\n"
        "crypto-trades,2026-11-04T14:00,2\n"
        "crypto-trades,2026-11-04T14:01,3\n"
        "gone-stream,2026-11-04T14:00,7\n"
    )
    assert stream_capture.unit_rows(tmp_path, [_TICKS]) == {"currencies real-time": 5}
    assert stream_capture.unit_rows(tmp_path / "missing", [_TICKS]) == {}


def test_a_units_rows_are_due_after_its_session_opens_and_its_delay() -> None:
    day = date(2026, 11, 4)
    due = stream_capture.rows_due_from("stocks delayed", day)
    assert due.astimezone(UTC) == datetime(2026, 11, 4, 9, 20, tzinfo=UTC)  # 04:20 New York
    futures = stream_capture.rows_due_from("futures real-time", day)
    assert futures.astimezone(UTC) == datetime(2026, 11, 4, 8, 50, tzinfo=UTC)  # 03:50


@pytest.mark.jvm_held
def test_a_stream_day_streams_the_option_contracts_nearest_the_money(
    make_ws_session: Callable[..., WsSession], fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    contracts = [("O:SPY261106C00600000", 600.0), ("O:SPY261106C00700000", 700.0)]
    rows = [
        {
            "details": {"ticker": ticker, "expiration_date": "2026-11-06", "strike_price": strike},
            "underlying_asset": {"price": 690.0, "ticker": "SPY"},
        }
        for ticker, strike in contracts
    ]
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(envelope(rows)))
    harness = make_ws_session(rest_base_url=fake_rest.url)

    def diagnostics_caught_up() -> None:
        # The first sample may run before the session's diagnostics show the new stream.
        assert harness.session._core.diagnostics.wait_until_shown(15.0)

    day = _Day(stop_after=2, action=diagnostics_caught_up)
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["options delayed"],
        tiers={"options": "starter"},
        folder=tmp_path,
        until=START + timedelta(hours=1),
        report=report,
        day_start=START,
        clock=day.clock(),
    )
    assert seen == [("options delayed", "passed", "")]
    assert day.waits == [10.0, 10.0]  # the wait that says stop ends the day early
    last = (START + timedelta(seconds=10)).isoformat()
    [symbols] = {r["Symbols"] for r in _csv(tmp_path / "samples-streams.csv") if r["time"] == last}
    assert {s.strip() for s in symbols.split(",")} == {ticker for ticker, _ in contracts}


def test_a_stream_day_already_over_only_reports(
    massive_session: MassiveSession, tmp_path: Path
) -> None:
    seen, report = _reports()
    stream_capture.run_stream_day(
        massive_session,
        units=[],
        tiers={},
        folder=tmp_path,
        until=datetime(2020, 1, 2, tzinfo=UTC),  # long past: the real clock is after it
        report=report,
    )
    assert seen == []
    assert not (tmp_path / "samples-process.csv").exists()


def test_the_default_clock_reads_the_real_time() -> None:
    before = datetime.now(UTC)
    assert before <= Clock().now() <= datetime.now(UTC)


# --- what the day streams -------------------------------------------------------------------


def _chain_row(
    symbol: str, expiry: str | None, *, strike: float, price: float | None
) -> dict[str, Any]:
    return {
        "Symbol": symbol,
        "DetailsExpirationDate": expiry,
        "DetailsStrikePrice": strike,
        "UnderlyingAssetPrice": price,
    }


def test_the_option_contracts_are_the_nearest_expiry_nearest_the_money() -> None:
    today = date(2026, 11, 4)
    rows = [
        _chain_row(f"O:SPY{n:02}", "2026-11-05", strike=600.0 + n, price=610.0) for n in range(20)
    ]
    rows += [
        _chain_row("O:OLD", "2026-11-03", strike=610.0, price=610.0),
        _chain_row("O:LATER", "2026-11-06", strike=610.0, price=610.0),
        _chain_row("O:NONE", None, strike=610.0, price=610.0),
    ]
    chosen = stream_capture.option_contracts(rows, today)
    assert chosen[0] == "O:SPY10"
    assert sorted(chosen) == [f"O:SPY{n:02}" for n in range(5, 15)]
    unpriced = [
        _chain_row("O:A", "2026-11-05", strike=1.0, price=None),
        _chain_row("O:B", "2026-11-05", strike=5.0, price=None),
        _chain_row("O:C", "2026-11-05", strike=9.0, price=None),
    ]
    assert stream_capture.option_contracts(unpriced, today)[0] == "O:B"  # the middle strike
    assert stream_capture.option_contracts([], today) == []


def test_each_unit_streams_its_watchlist(massive_session: MassiveSession) -> None:
    units = [
        "stocks real-time",
        "options delayed",
        "currencies real-time",
        "indices delayed",
        "futures delayed",
    ]
    streams = stream_capture.stream_day_streams(
        units,
        tiers={"stocks": "advanced", "options": "starter"},
        today=date(2026, 10, 29),
        options=["O:SPY261106C00600000"],
    )
    assert [(s.name, s.unit, s.event_column, s.held) for s in streams] == [
        ("stocks-minute-bars", "stocks real-time", "TimestampEnd", True),
        ("stocks-second-bars", "stocks real-time", "TimestampEnd", True),
        ("stocks-trades", "stocks real-time", "Timestamp", False),
        ("options-minute-bars", "options delayed", "TimestampEnd", True),
        ("forex-minute-bars", "currencies real-time", "TimestampEnd", True),
        ("crypto-minute-bars", "currencies real-time", "TimestampEnd", True),
        ("forex-quotes", "currencies real-time", "Timestamp", False),
        ("indices-minute-bars", "indices delayed", "TimestampEnd", True),
        ("futures-minute-bars", "futures delayed", "TimestampEnd", True),
    ]
    symbols = {s.name: s.query(massive_session)._request.symbols for s in streams}
    assert "ESZ6" in symbols["futures-minute-bars"]
    assert "NQZ6" in symbols["futures-minute-bars"]
    assert "KIMpL" in symbols["stocks-minute-bars"]
    starter = stream_capture.stream_day_streams(
        ["stocks delayed", "options delayed"],
        tiers={"stocks": "starter"},
        today=date(2026, 10, 29),
        options=[],
    )
    assert [s.name for s in starter] == ["stocks-minute-bars", "stocks-second-bars"]


def test_throughput_streams_every_symbol_of_stocks_and_currencies(
    massive_session: MassiveSession,
) -> None:
    units = ["stocks delayed", "currencies real-time"]
    streams = stream_capture.throughput_streams(units, tiers={"stocks": "starter"})
    assert [(s.name, s.event_column) for s in streams] == [
        ("stocks-all-second-bars", "TimestampEnd"),
        ("crypto-all-trades", "Timestamp"),
        ("forex-all-quotes", "Timestamp"),
    ]
    for stream in streams:
        assert stream.query(massive_session) is not None


# --- the recorder ---------------------------------------------------------------------------


def test_the_recorder_splits_rows_by_hour_and_keeps_a_restarted_childs_rows(
    tmp_path: Path,
) -> None:
    first = Recorder(tmp_path)
    first.put(_TICKS, _batch(START - timedelta(seconds=1), START, START + timedelta(minutes=1)))
    first.close()
    assert sorted(p.name for p in (tmp_path / "rows").iterdir()) == [
        "crypto-trades-2026-11-04T13.parquet",
        "crypto-trades-2026-11-04T14.parquet",
    ]
    latency = _csv(tmp_path / "latency.csv")
    assert [(r["hour"], r["rows"], r["max_ms"]) for r in latency] == [
        ("2026-11-04T13", "1", "2000.0"),
        ("2026-11-04T14", "2", "2000.0"),
    ]
    minutes = _csv(tmp_path / "minutes.csv")
    assert [(r["minute"], r["rows"]) for r in minutes] == [
        ("2026-11-04T13:59", "1"),
        ("2026-11-04T14:00", "1"),
        ("2026-11-04T14:01", "1"),
    ]
    second = Recorder(tmp_path)  # a child started again in the same hour
    second.put(_TICKS, _batch(START + timedelta(minutes=2)))
    second.close()
    names = {p.name for p in (tmp_path / "rows").glob("crypto-trades-2026-11-04T14*")}
    [restarted] = names - {"crypto-trades-2026-11-04T14.parquet"}
    assert pq.read_table(tmp_path / "rows" / restarted).num_rows == 1


def test_the_recorder_keeps_a_bad_batchs_error_and_goes_on(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path)
    recorder.put(_TICKS, {"Symbol": np.array(["X:BTCUSD"], dtype=object)})
    recorder.put(_TICKS, _batch(START))
    recorder.put(_TICKS, _batch(START + timedelta(seconds=1)))  # the same hour's file
    recorder.close()
    assert recorder.errors == ["crypto-trades: KeyError: 'TimestampInsert'"]
    assert pq.read_table(tmp_path / "rows" / "crypto-trades-2026-11-04T14.parquet").num_rows == 2


def test_a_recorder_with_nothing_writes_nothing(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path)
    recorder.flush_minutes()
    recorder.close()
    assert not (tmp_path / "minutes.csv").exists()
    assert not (tmp_path / "latency.csv").exists()


def test_a_csv_written_again_holds_only_the_new_rows(tmp_path: Path) -> None:
    path = tmp_path / "hours.csv"
    stream_capture.write_csv(path, [{"hour": "22", "rows": 1}])
    stream_capture.write_csv(path, [{"hour": "22", "rows": 1}, {"hour": "23", "rows": 2}])
    assert path.read_text() == "hour,rows\n22,1\n23,2\n"
    stream_capture.write_csv(path, [])
    assert not path.exists()


# --- gaps -----------------------------------------------------------------------------------


def test_gaps_count_only_pauses_past_a_minute(tmp_path: Path) -> None:
    assert stream_capture.record_gap(tmp_path, START, day_start=START) == timedelta()
    stream_capture.append_csv(tmp_path / "samples-process.csv", [{"time": START.isoformat()}])
    later = START + timedelta(seconds=60)
    assert stream_capture.record_gap(tmp_path, later, day_start=START) == timedelta()
    three = stream_capture.record_gap(tmp_path, START + timedelta(minutes=3), day_start=START)
    assert three == timedelta(minutes=3)


def test_a_stream_day_begun_late_counts_the_lost_hours_as_a_gap(tmp_path: Path) -> None:
    evening = START + timedelta(hours=15)
    assert stream_capture.record_gap(tmp_path, evening, day_start=START) == timedelta(hours=15)
    [gap] = _csv(tmp_path / "gaps.csv")
    assert gap["cause"] == "the day's first child started late"


def test_the_base_stocks_business_plan_streams_no_second_bars() -> None:
    day = stream_capture.stream_day_streams(
        ["stocks real-time"], tiers={"stocks": "business"}, today=START.date(), options=[]
    )
    assert [s.name for s in day] == ["stocks-minute-bars"]
    measured = stream_capture.throughput_streams(["stocks real-time"], tiers={"stocks": "business"})
    assert [s.name for s in measured] == ["stocks-all-minute-bars"]


# --- throughput -----------------------------------------------------------------------------


def test_the_throughput_hour_starts_later_for_delayed_stocks() -> None:
    day = date(2026, 11, 5)
    delayed = stream_capture.throughput_window(["stocks delayed"], day)
    live = stream_capture.throughput_window(["stocks real-time", "currencies real-time"], day)
    assert [t.isoformat() for t in delayed] == [
        "2026-11-05T09:45:00-05:00",
        "2026-11-05T10:45:00-05:00",
    ]
    assert live[0].isoformat() == "2026-11-05T09:30:00-05:00"


def test_a_sampled_throughput_hour_passes_and_an_unsampled_one_fails(
    ws_session: WsSession, tmp_path: Path
) -> None:
    seen, report = _reports()

    def diagnostics_caught_up() -> None:
        # The samples after the first wait must see the streams in the session's diagnostics.
        assert ws_session.session._core.diagnostics.wait_until_shown(15.0)

    stream_capture.run_throughput(
        ws_session.session,
        units=["stocks real-time", "currencies real-time"],
        tiers={"stocks": "advanced", "currencies": "starter"},
        folder=tmp_path / "covered",
        until=START + timedelta(seconds=50),
        report=report,
        clock=_Day(action=diagnostics_caught_up).clock(),
        window=(START, START + timedelta(seconds=40)),
    )
    assert seen == [("stocks real-time", "passed", ""), ("currencies real-time", "passed", "")]
    summary = json.loads((tmp_path / "covered" / "summary.json").read_text())
    assert summary["covered"] is True
    assert set(summary["streams"]) == {"stocks.bars", "crypto.trades", "forex.quotes"}
    seen, report = _reports()
    stream_capture.run_throughput(
        ws_session.session,
        units=["stocks delayed"],
        tiers={"stocks": "starter"},
        folder=tmp_path / "early",
        until=START + timedelta(seconds=10),
        report=report,
        clock=_Day().clock(),
    )
    assert seen == [("stocks delayed", "failed", "the measured hour was not sampled without a gap")]


def test_the_throughput_summary_gives_rates_peaks_and_drift(tmp_path: Path) -> None:
    def at(seconds: int) -> str:
        return (START + timedelta(seconds=seconds)).isoformat()

    stream_capture.append_csv(
        tmp_path / "samples-streams.csv",
        [
            {"time": at(0), "Dataset": "stocks.bars", "RowsPublished": "0", "SchemaDrift": ""},
            {"time": at(10), "Dataset": "stocks.bars", "RowsPublished": "100", "SchemaDrift": ""},
            {"time": at(20), "Dataset": "stocks.bars", "RowsPublished": "400", "SchemaDrift": "x"},
            {"time": at(10), "Dataset": "forex.quotes", "RowsPublished": "", "SchemaDrift": ""},
            {"time": at(999), "Dataset": "stocks.bars", "RowsPublished": "9", "SchemaDrift": ""},
        ],
    )
    stream_capture.append_csv(
        tmp_path / "samples-connections.csv",
        [
            {"time": at(0), "BufferedRows": "7", "Reconnects": "", "SchemaDrift": "y"},
            {"time": at(20), "BufferedRows": "", "Reconnects": "2", "SchemaDrift": ""},
        ],
    )
    stream_capture.append_csv(
        tmp_path / "samples-process.csv",
        [
            {"time": at(0), "cpu_seconds": "1.5", "heap_used_mb": "10"},
            {"time": at(20), "cpu_seconds": "4.0", "heap_used_mb": "30"},
        ],
    )
    summary = stream_capture.summarize_throughput(tmp_path, (START, START + timedelta(seconds=30)))
    assert summary["streams"] == {
        "stocks.bars": {"mean_rows_per_second": 20.0, "peak_rows_per_second": 30.0},
        "forex.quotes": {"mean_rows_per_second": 0.0, "peak_rows_per_second": 0.0},
    }
    assert summary["covered"] is True
    assert (summary["peak_buffered_rows"], summary["reconnects"]) == (7, 2)
    assert summary["schema_drift"] == ["x", "y"]
    assert (summary["peak_heap_used_mb"], summary["cpu_seconds"]) == (30.0, 2.5)


def test_a_throughput_summary_without_samples_is_not_covered(tmp_path: Path) -> None:
    summary = stream_capture.summarize_throughput(tmp_path, (START, START + timedelta(hours=1)))
    assert summary["covered"] is False
    assert summary["streams"] == {}
    assert (summary["cpu_seconds"], summary["peak_heap_used_mb"]) == (0.0, 0.0)


def test_a_gap_inside_the_measured_hour_leaves_it_uncovered(tmp_path: Path) -> None:
    times = [START, START + timedelta(seconds=30), START + timedelta(seconds=200)]
    stream_capture.append_csv(
        tmp_path / "samples-process.csv",
        [{"time": t.isoformat(), "cpu_seconds": "1", "heap_used_mb": "1"} for t in times],
    )
    window = (START, START + timedelta(seconds=210))
    assert stream_capture.summarize_throughput(tmp_path, window)["covered"] is False


class _Update:
    """A stand-in for Deephaven's table update: only its added rows."""

    def __init__(self, added: dict[str, np.ndarray[Any, Any]]) -> None:
        self._added = added

    def added(self) -> dict[str, np.ndarray[Any, Any]]:
        return self._added


def test_the_listener_queues_only_updates_that_added_rows(tmp_path: Path) -> None:
    queued: list[tuple[str, int]] = []

    class Queue:
        def put(
            self, stream: stream_capture.Stream, columns: dict[str, np.ndarray[Any, Any]]
        ) -> None:
            queued.append((stream.name, len(columns["Symbol"])))

    stream = stream_capture.Stream("stocks-minute-bars", "stocks delayed", lambda _m: None)
    copier = stream_capture.RowCopier(stream, cast("stream_capture.Recorder", Queue()))
    copier.on_update(
        cast("TableUpdate", _Update({"Symbol": np.array(["AAPL", "NVDA"])})), is_replay=False
    )
    copier.on_update(
        cast("TableUpdate", _Update({"Symbol": np.array([], dtype=object)})), is_replay=False
    )
    copier.on_update(cast("TableUpdate", _Update({})), is_replay=False)
    assert queued == [("stocks-minute-bars", 2)]


def test_a_class_whose_stream_failed_is_named_with_its_error() -> None:
    rows = [
        {"Dataset": "stocks.bars", "State": "STREAMING", "Error": ""},
        {"Dataset": "forex.quotes", "State": "FAILED", "Error": "HTTP 403 not entitled"},
        {"Dataset": "crypto.bars", "State": "FAILED", "Error": "a later failure"},
    ]
    assert stream_capture.failed_streams(rows) == {"currencies": "HTTP 403 not entitled"}


def test_a_stall_between_samples_is_recorded_as_a_gap(
    massive_session: MassiveSession, tmp_path: Path
) -> None:
    day = _Day()
    jumps = iter([timedelta(seconds=10), timedelta(hours=2)])

    def wait(seconds: float) -> bool:
        day.waits.append(seconds)
        day.now_at += next(jumps, timedelta(hours=9))  # the machine slept through the second
        return False

    recorder = Recorder(tmp_path)
    try:
        stream_capture.sample_until(
            massive_session,
            tmp_path,
            until=START + timedelta(hours=3),
            clock=Clock(now=day.now, wait=wait),
            recorder=recorder,
        )
    finally:
        recorder.close()
    [gap] = _csv(tmp_path / "gaps.csv")
    assert (gap["start"], gap["end"]) == (
        (START + timedelta(seconds=10)).isoformat(),
        (START + timedelta(seconds=10, hours=2)).isoformat(),
    )
    assert stream_capture.total_gaps(tmp_path) == timedelta(hours=2)


@pytest.mark.jvm_held
def test_a_stream_that_failed_fails_its_unit(
    make_ws_session: Callable[..., WsSession], fake_ws_server: FakeMassiveServer, tmp_path: Path
) -> None:
    fake_ws_server.refuse["AM.I:SPX"] = "not authorized"
    harness = make_ws_session()

    def refused() -> None:
        def failed() -> bool:
            harness.flush()
            assert harness.session._core.diagnostics.wait_until_shown(15.0)
            states = stream_capture.diagnostics_rows(harness.session, harness.session.streams())
            return any(r["State"] == "FAILED" for r in states)

        fake_ws_server.wait_for(lambda _: failed())

    day = _Day(stop_after=1, action=refused)
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["indices delayed"],
        tiers={"indices": "starter"},
        folder=tmp_path,
        until=START + timedelta(hours=1),
        report=report,
        day_start=START,
        clock=day.clock(),
    )
    [(unit, status, reason)] = seen
    assert (unit, status) == ("indices delayed", "failed")
    assert reason.startswith("a stream failed: ")
    assert "not authorized" in reason


@pytest.mark.jvm_held
def test_an_options_unit_without_contracts_fails(
    make_ws_session: Callable[..., WsSession], fake_rest: FakeRestServer, tmp_path: Path
) -> None:
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(envelope([])))
    harness = make_ws_session(rest_base_url=fake_rest.url)
    seen, report = _reports()
    stream_capture.run_stream_day(
        harness.session,
        units=["options delayed"],
        tiers={"options": "starter"},
        folder=tmp_path,
        until=START + timedelta(hours=1),
        report=report,
        clock=_Day(stop_after=1).clock(),
        day_start=START,
    )
    assert seen == [("options delayed", "failed", "the SPY chain gave no contract to stream")]


@pytest.mark.jvm_held
def test_a_throughput_stream_that_failed_fails_its_unit(
    make_ws_session: Callable[..., WsSession], fake_ws_server: FakeMassiveServer, tmp_path: Path
) -> None:
    fake_ws_server.refuse["XT.*"] = "not authorized"
    harness = make_ws_session()

    def refused() -> None:
        def failed() -> bool:
            harness.flush()
            assert harness.session._core.diagnostics.wait_until_shown(15.0)
            states = stream_capture.diagnostics_rows(harness.session, harness.session.streams())
            return any(r["State"] == "FAILED" for r in states)

        fake_ws_server.wait_for(lambda _: failed())

    seen, report = _reports()
    stream_capture.run_throughput(
        harness.session,
        units=["currencies real-time"],
        tiers={"currencies": "starter"},
        folder=tmp_path,
        until=START + timedelta(seconds=50),
        report=report,
        clock=_Day(action=refused).clock(),
        window=(START, START + timedelta(seconds=40)),
    )
    [(unit, status, reason)] = seen
    assert (unit, status) == ("currencies real-time", "failed")
    assert "not authorized" in reason
