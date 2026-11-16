"""Tests for the futures rows held until their session date is known (`_tables/held_rows.py`).

The hold reads its calendar through `FuturesSessionDates`, here a scripted fake whose states,
dates, and version the test sets, and the time from an injected clock. Batches are plain Arrow
tables in the shape a futures flush converts: `Symbol`, `Timestamp`, `Date`, and a value.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from deephaven_massive._datasets.row_times import RowTimes, TimeRule
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._errors import BrokenGuaranteeError, EntitlementError, ErrorSource
from deephaven_massive._tables import held_rows
from deephaven_massive._tables.futures_dates import LookupState, Readiness
from deephaven_massive._tables.held_rows import (
    DELAYED_AFTER,
    RECHECK_EVERY,
    Delayed,
    HeldRows,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

SCHEMA = pa.schema(
    [
        ("Symbol", pa.string()),
        ("Timestamp", pa.timestamp("ns", "UTC")),
        ("Date", pa.string()),
        ("Price", pa.float64()),
        ("TimestampInsert", pa.timestamp("ns", "UTC")),
    ]
)
RULE = TimeRule("futures.trades", RowTimes.TICK, AssetClass.FUTURES)
WAITING = LookupState(Readiness.WAITING, "its product is being looked up")
READY = LookupState(Readiness.READY)


class FakeCalendar:
    """A futures session calendar whose states and dates the test sets."""

    def __init__(self) -> None:
        self.version = 0
        self.found: dict[str, LookupState] = {}
        self.dates: dict[str, str | None] = {}  # what `correct` gives each ticker's rows
        self.asked: list[set[str]] = []
        self.placed_from: dict[str, int] = {}  # instants before it have no loaded session
        self.dead = False

    def alive(self) -> bool:
        return not self.dead

    def states(self, symbols: Iterable[str]) -> dict[str, LookupState]:
        wanted = set(symbols)
        self.asked.append(wanted)
        return {s: self.found.get(s, WAITING) for s in wanted}

    def unplaced(self, symbol: str, instant: int) -> str:
        return f"no session holds {symbol} at {instant}"

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        assert instants.type == pa.int64()
        assert len(dates) == len(symbols)
        return pa.array(
            [
                None if t < self.placed_from.get(s, 0) else self.dates.get(s)
                for s, t in zip(symbols.to_pylist(), instants.to_pylist(), strict=True)
            ],
            pa.string(),
        )


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def calendar() -> FakeCalendar:
    return FakeCalendar()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def held(calendar: FakeCalendar, clock: Clock) -> HeldRows[str]:
    return HeldRows(calendar, max_outage=300.0, clock=clock)


def batch(*rows: tuple[str | None, str | None, float]) -> pa.Table:
    # Rows of (Symbol, Date, Price), stamped one second apart.
    return pa.table(
        {
            "Symbol": [s for s, _, _ in rows],
            "Timestamp": [10**18 + n * 10**9 for n in range(len(rows))],
            "Date": [d for _, d, _ in rows],
            "Price": [p for _, _, p in rows],
            "TimestampInsert": [2 * 10**18] * len(rows),
        },
        schema=SCHEMA,
    )


def published(settled: held_rows.Settled) -> list[tuple[str | None, str | None, float]]:
    # The released rows, then the flush's, in the order the flush publishes them.
    tables = [rows for rows, _ in settled.released]
    if settled.rows is not None:
        tables.append(settled.rows)
    return [
        row
        for rows in tables
        for row in zip(
            rows.column("Symbol").to_pylist(),
            rows.column("Date").to_pylist(),
            rows.column("Price").to_pylist(),
            strict=True,
        )
    ]


def settle(held: HeldRows[str], rows: pa.Table | None, *, generation: int = 1) -> held_rows.Settled:
    return held.settle("trades", rows, generation=generation, times=RULE)


def test_dated_rows_pass_through_without_asking_the_calendar(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    rows = batch(("ESZ6", "2026-11-16", 1.0), ("NQZ6", "2026-11-16", 2.0))
    settled = settle(held, rows)
    assert settled.rows is rows
    assert calendar.asked == []
    assert held.holding() == ()


def test_waiting_rows_are_held_and_publish_dated_before_newer_rows(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    first = settle(
        held, batch(("ESZ6", None, 1.0), ("NQZ6", "2026-11-16", 2.0), ("ESZ6", None, 3.0))
    )
    assert published(first) == [("NQZ6", "2026-11-16", 2.0)]
    assert held.holding() == ("trades",)
    assert held.rows_of("trades") == 2
    assert held.total_bytes > 0
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    second = settle(held, batch(("ESZ6", None, 4.0)), generation=2)
    # The held rows first, in arrival order, then the newer row, all dated.
    assert published(second) == [
        ("ESZ6", "2026-11-17", 1.0),
        ("ESZ6", "2026-11-17", 3.0),
        ("ESZ6", "2026-11-17", 4.0),
    ]
    assert [generation for _, generation in second.released] == [1]  # the held rows' socket
    assert held.holding() == ()


def test_a_held_ticker_whose_new_rows_are_dated_releases_its_held_rows_first(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    # The schedule loaded between the flushes: the new rows came dated, and the held ones must
    # still publish before them.
    settle(held, batch(("ESZ6", None, 1.0)))
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    settled = settle(held, batch(("ESZ6", "2026-11-17", 2.0)))
    assert published(settled) == [("ESZ6", "2026-11-17", 1.0), ("ESZ6", "2026-11-17", 2.0)]


def test_a_row_no_loaded_session_holds_waits_for_a_reload_then_is_withheld(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    # Never published without a date: it waits for its product's schedule to load again, and
    # only a load that began after it was held, answering without a session for it, withholds
    # it, with the sessions nearest it.
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=clock.now - 60)
    first = settle(held, batch(("ESZ6", None, 1.0), ("ESZ6", None, 2.0)))
    assert first.rows is None
    assert held.rows_of("trades") == 2
    clock.now += RECHECK_EVERY
    assert settle(held, None).discarded == {}  # still the old load
    assert held.rows_of("trades") == 2
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=clock.now)
    calendar.version += 1
    settled = settle(held, None)
    instant = 10**18  # the first such row's
    assert settled.discarded == {
        Readiness.UNSCHEDULED: {"ESZ6": (2, f"no session holds ESZ6 at {instant}")}
    }
    assert held.holding() == ()


def test_a_reload_that_places_held_rows_publishes_them(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=clock.now - 60)
    settle(held, batch(("ESZ6", None, 1.0)))
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    assert published(settle(held, None)) == [("ESZ6", "2026-11-17", 1.0)]


def test_rows_of_a_product_with_no_schedule_are_withheld(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    none = LookupState(Readiness.UNSCHEDULED, "Massive's futures schedules hold no session of BZT")
    calendar.found["BZTZ6"] = none
    settled = settle(held, batch(("BZTZ6", None, 1.0), ("ESZ6", "2026-11-16", 2.0)))
    assert published(settled) == [("ESZ6", "2026-11-16", 2.0)]
    assert settled.discarded == {Readiness.UNSCHEDULED: {"BZTZ6": (1, none.reason)}}


def test_rows_without_a_symbol_are_never_held(held: HeldRows[str]) -> None:
    settled = settle(held, batch((None, None, 1.0)))
    assert published(settled) == [(None, None, 1.0)]


def test_held_tickers_are_checked_when_the_calendar_learns_something_or_a_second_passes(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)))
    calendar.asked.clear()
    settle(held, None)
    assert calendar.asked == []  # nothing changed, and no time passed
    calendar.version += 1
    settle(held, None)
    assert calendar.asked == [{"ESZ6"}]
    clock.now += RECHECK_EVERY
    settle(held, batch(("NQZ6", "2026-11-16", 2.0)))
    assert calendar.asked[-1] == {"ESZ6"}


def test_unlisted_rows_are_discarded_and_counted(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    settle(held, batch(("GONEZ6", None, 1.0), ("GONEZ6", None, 2.0)))
    gone = LookupState(Readiness.UNLISTED, "neither list holds it (asked over 30 min)")
    calendar.found["GONEZ6"] = gone
    calendar.version += 1
    settled = settle(held, batch(("GONEZ6", None, 3.0), ("ESZ6", "2026-11-16", 4.0)))
    assert published(settled) == [("ESZ6", "2026-11-16", 4.0)]
    assert settled.discarded == {Readiness.UNLISTED: {"GONEZ6": (3, gone.reason)}}
    assert held.holding() == ()


def test_a_refused_lookup_fails_its_tickers_streams_with_massives_answer(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    refusal = EntitlementError("HTTP 403: not entitled", status=403, request_id="r1")
    calendar.found["ESZ6"] = LookupState(Readiness.REFUSED, "refused", error=refusal)
    settled = settle(held, batch(("ESZ6", None, 1.0)))
    assert settled.rows is None
    error = settled.failed["ESZ6"]
    assert str(error) == "looking up the trading session of ESZ6 failed: HTTP 403: not entitled"
    assert (error.status, error.request_id, error.source) == (403, "r1", ErrorSource.MASSIVE)
    assert held.holding() == ()


def test_an_outage_fails_its_tickers_streams_past_max_outage_from_the_oldest_held_row(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    failing = LookupState(
        Readiness.WAITING, "looking up its product failed: HTTP 503", failing_since=0.0
    )
    calendar.found["ESZ6"] = failing
    settle(held, batch(("ESZ6", None, 1.0)))  # failing for long, but held only now
    clock.now += 300.0
    assert settle(held, None).failed == {}
    clock.now += 1.0
    settled = settle(held, None)
    assert str(settled.failed["ESZ6"]) == (
        "the rows of ESZ6 have waited 301 s for its trading session, longer than "
        "MassiveConfig.max_outage (300 s): looking up its product failed: HTTP 503"
    )
    assert held.holding() == ()


def test_a_long_wait_is_reported_once_and_its_release_logged(
    *,
    held: HeldRows[str],
    calendar: FakeCalendar,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)))
    clock.now += DELAYED_AFTER - 1
    assert settle(held, batch(("ESZ6", None, 2.0))).delayed == {}
    clock.now += 1
    assert settle(held, None).delayed == {"ESZ6": Delayed(2, WAITING.reason)}
    clock.now += RECHECK_EVERY
    assert settle(held, None).delayed == {}  # once per wait
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.held_rows"):
        settle(held, None)
    assert caplog.messages == ["the held futures rows of ESZ6 are settled after 16 s"]


def test_past_the_byte_limit_the_dataset_holding_the_most_fails(
    *,
    held: HeldRows[str],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    one = batch(("ESZ6", None, 1.0)).take([0])
    monkeypatch.setattr(held_rows, "HELD_BYTES_LIMIT", one.get_total_buffer_size() * 3)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.held_rows"):
        assert (
            held.settle("quotes", batch(("ESZ6", None, 1.0)), generation=1, times=RULE).overflow
            == ""
        )
        assert len(caplog.messages) == 1  # a tenth of the limit: the wait is named
        assert "a tenth of the" in caplog.messages[0]
        settled = held.settle(
            "trades",
            batch(("ESZ6", None, 1.0), ("ESZ6", None, 2.0), ("NQZ6", None, 3.0)),
            generation=1,
            times=RULE,
        )
    assert settled.overflow.startswith("3 streamed futures rows (0 MB) waited for their trading")
    assert held.settle("quotes", None, generation=1, times=RULE).overflow == ""


def test_keep_and_drop_forget_the_rows_of_datasets_no_stream_reads(
    held: HeldRows[str], caplog: pytest.LogCaptureFixture
) -> None:
    held.settle("trades", batch(("ESZ6", None, 1.0)), generation=1, times=RULE)
    held.settle(
        "quotes",
        batch(("ESZ6", None, 1.0), ("NQZ6", None, 2.0)),
        generation=1,
        times=RULE,
    )
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.held_rows"):
        held.keep(["trades"])
        assert held.holding() == ("trades",)
        held.drop("trades", why="its streams failed")
        assert held.holding() == ()
        held.drop("trades", why="again")  # nothing held: nothing logged
    assert caplog.messages == [
        "2 held futures rows of 2 tickers are dropped: no stream reads them any more",
        "1 held futures rows of 1 tickers are dropped: its streams failed",
    ]


def test_held_rows_are_copies_of_their_own_rows(held: HeldRows[str]) -> None:
    # A thin ticker's held row must not keep a whole flush's columns alive.
    rows = batch(*[("NQZ6", "2026-11-16", float(n)) for n in range(1000)], ("ESZ6", None, 1.0))
    settle(held, rows)
    assert held.total_bytes < rows.nbytes // 10


def test_a_bar_spanning_two_sessions_keeps_its_null_date_and_publishes(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    # Its `Date` is null by the dataset's own rule, not for want of a schedule: once its
    # ticker is ready, it publishes as it is, never dated by its start alone.
    instant = pa.timestamp("ns", "UTC")
    start = 1_741_388_370 * 10**9  # 2025-03-07 22:59:30Z: 16:59:30 Chicago, before the roll
    bars = pa.table(
        {
            "Symbol": ["ESZ6"],
            "TimestampStart": pa.array([start], instant),
            "TimestampEnd": pa.array([start + 60 * 10**9], instant),  # past 17:00
            "Date": pa.array([None], pa.string()),
            "TimestampInsert": pa.array([2 * 10**18], instant),
        }
    )
    rule = TimeRule(
        "futures.bars", RowTimes.BAR, AssetClass.FUTURES, timespan=Timespan.MINUTE, multiplier=1
    )
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=0.0)
    calendar.dates["ESZ6"] = "2025-03-07"
    settled = held.settle("bars", bars, generation=1, times=rule)
    assert settled.rows is not None
    assert settled.rows.column("Date").to_pylist() == [None]
    assert held.holding() == ()


def test_a_refused_state_without_its_refusal_is_a_library_defect(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    calendar.found["ESZ6"] = LookupState(Readiness.REFUSED, "refused")
    with pytest.raises(BrokenGuaranteeError, match="the refused lookup of ESZ6 has no refusal"):
        settle(held, batch(("ESZ6", None, 1.0)))


def test_a_long_wait_merges_its_held_tables(held: HeldRows[str], clock: Clock) -> None:
    for n in range(held_rows.MERGE_AT + 1):
        clock.now += 0.05
        settle(held, batch(("ESZ6", None, float(n))), generation=n)
    assert held.rows_of("trades") == held_rows.MERGE_AT + 1
    assert len(held._held["trades"]["ESZ6"].parts) == 1
    assert held._held["trades"]["ESZ6"].parts[0].generation == held_rows.MERGE_AT


def test_a_row_held_after_the_last_load_began_waits_for_the_next(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    # The load in flight when the second row arrived may not cover it: only the first, held
    # before that load began, is withheld.
    settle(held, batch(("ESZ6", None, 1.0)))
    started = clock.now + 0.5
    clock.now += 1
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=started)
    settled = settle(held, batch(("ESZ6", None, 2.0)))
    assert settled.discarded[Readiness.UNSCHEDULED]["ESZ6"][0] == 1
    assert held.rows_of("trades") == 1


def test_a_waiting_tickers_dated_row_never_overtakes_its_held_rows(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    # The calendar answered between the state read and the dating: the new row came dated,
    # but its ticker still waits in this step, so the row waits behind the held one.
    settle(held, batch(("ESZ6", None, 1.0)))
    settled = settle(held, batch(("ESZ6", "2026-11-17", 2.0)))
    assert settled.rows is None
    assert held.rows_of("trades") == 2


def test_the_end_of_a_reported_wait_is_resumed(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)))
    clock.now += DELAYED_AFTER
    settle(held, None)
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    assert settle(held, None).resumed == frozenset({"ESZ6"})


def test_a_held_dated_row_publishes_once_its_ticker_is_ready(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)))
    settle(held, batch(("ESZ6", "2026-11-17", 2.0)))  # held behind the first
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    assert published(settle(held, None)) == [
        ("ESZ6", "2026-11-17", 1.0),
        ("ESZ6", "2026-11-17", 2.0),
    ]


def test_a_held_tickers_refusal_fails_its_streams(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)))
    refusal = EntitlementError("HTTP 403", status=403)
    calendar.found["ESZ6"] = LookupState(Readiness.REFUSED, "refused", error=refusal)
    calendar.version += 1
    assert "HTTP 403" in str(settle(held, None).failed["ESZ6"])
    assert held.holding() == ()


def test_new_rows_of_a_ticker_failing_past_max_outage_are_not_held(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    calendar.found["ESZ6"] = LookupState(
        Readiness.WAITING, "looking up its product failed: HTTP 503", failing_since=0.0
    )
    settle(held, batch(("ESZ6", None, 1.0)))
    clock.now += 301.0
    settled = settle(held, batch(("ESZ6", None, 2.0)))
    assert "longer than MassiveConfig.max_outage (300 s)" in str(settled.failed["ESZ6"])
    assert held.rows_of("trades") == 0


@pytest.mark.parametrize("held_first", [True, False], ids=["held", "new"])
def test_a_state_no_case_handles_is_refused(
    held: HeldRows[str], calendar: FakeCalendar, *, held_first: bool
) -> None:
    if held_first:
        settle(held, batch(("ESZ6", None, 1.0)))
        calendar.version += 1
    calendar.found["ESZ6"] = LookupState("bogus")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(AssertionError):
        settle(held, batch(("ESZ6", None, 2.0)))


def test_a_row_left_waiting_keeps_every_later_row_of_its_ticker_behind_it(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    # The first row lies outside the loaded sessions, the second inside: the second must not
    # publish before the reload decides the first.
    settle(held, batch(("ESZ6", None, 0.5)))  # held while its lookup runs
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=0.0)
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.placed_from["ESZ6"] = 10**18 + 1  # the first row's instant is unplaced
    settled = settle(
        held, batch(("ESZ6", None, 1.0), ("ESZ6", None, 2.0), ("NQZ6", "2026-11-16", 3.0))
    )
    assert published(settled) == [("NQZ6", "2026-11-16", 3.0)]
    assert held.rows_of("trades") == 3
    calendar.version += 1
    assert published(settle(held, None)) == []  # still behind it
    assert held.rows_of("trades") == 3
    calendar.placed_from.clear()  # the reload placed it
    calendar.version += 1
    assert published(settle(held, None)) == [
        ("ESZ6", "2026-11-17", 0.5),
        ("ESZ6", "2026-11-17", 1.0),
        ("ESZ6", "2026-11-17", 2.0),
    ]


@pytest.mark.parametrize("refused", [True, False], ids=["refused", "outage"])
def test_a_failing_reload_fails_the_rows_waiting_on_it(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock, *, refused: bool
) -> None:
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=0.0)
    calendar.placed_from["ESZ6"] = 2 * 10**18  # no loaded session holds it
    settle(held, batch(("ESZ6", None, 1.0)))
    clock.now += 301.0
    calendar.found["ESZ6"] = LookupState(
        Readiness.READY,
        "loading the schedule of its product ES failed: HTTP 503",
        failing_since=None if refused else 0.0,
        error=EntitlementError("HTTP 403", status=403) if refused else None,
        loaded_at=0.0,
    )
    calendar.version += 1
    failure = str(settle(held, None).failed["ESZ6"])
    assert ("HTTP 403" if refused else "longer than MassiveConfig.max_outage") in failure
    assert held.holding() == ()


def test_released_rows_keep_their_own_socket_generation(
    held: HeldRows[str], calendar: FakeCalendar
) -> None:
    settle(held, batch(("ESZ6", None, 1.0)), generation=1)
    calendar.found["ESZ6"] = READY
    calendar.dates["ESZ6"] = "2026-11-17"
    calendar.version += 1
    settled = settle(held, batch(("NQZ6", None, 2.0)), generation=2)  # held: still waiting
    assert [generation for _, generation in settled.released] == [1]
    assert settled.rows is None


def test_merged_rows_keep_the_time_each_was_held(
    held: HeldRows[str], calendar: FakeCalendar, clock: Clock
) -> None:
    # A load that began after the first row was held, but before the merge, still answers
    # for that row: merging must not make it look newer.
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=0.0)
    calendar.placed_from["ESZ6"] = 2 * 10**18  # no loaded session holds any row
    first = clock.now
    for n in range(held_rows.MERGE_AT + 1):
        settle(held, batch(("ESZ6", None, float(n))))
        clock.now += 1
    assert len(held._held["trades"]["ESZ6"].parts) == 1
    calendar.found["ESZ6"] = LookupState(Readiness.READY, loaded_at=first)
    calendar.version += 1
    settled = settle(held, None)
    assert settled.discarded[Readiness.UNSCHEDULED]["ESZ6"][0] == 1  # the first row only
    assert held.rows_of("trades") == held_rows.MERGE_AT
