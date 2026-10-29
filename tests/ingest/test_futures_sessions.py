"""Tests for the futures session calendar of streamed rows (`_ingest/futures_sessions.py`).

The calendar's worker makes its requests through an injected `fetch`, here a scripted fake that
records each call and can hold the requests of one path open, and reads the time from an
injected clock, so every reload, spacing, and patience decision is deterministic. A test waits
only on observable state: a recorded request, or the calendar's `wait_idle`, which returns once
the worker has served every lookup queued so far.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from deephaven_massive._errors import MassiveError
from deephaven_massive._ingest import futures_sessions
from deephaven_massive._ingest.futures_sessions import (
    CONTRACTS_PATH,
    LOAD_PATIENCE,
    RELOAD_SPACING,
    SCHEDULES_PATH,
    FuturesSessions,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

TIMEOUT = 15.0
SPACING = RELOAD_SPACING.total_seconds()
PATIENCE = LOAD_PATIENCE.total_seconds()


def instant(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp()) * 10**9


def event(product: str, day: str, stamp: str, *, kind: str = "open") -> dict[str, object]:
    return {
        "event": kind,
        "product_code": product,
        "session_end_date": day,
        "timestamp": f"{stamp}+00:00",
        "trading_venue": "XCME",
    }


# ES around Good Friday 2024: the session of Monday 04-01 opened on Thursday 03-28 at 17:00 CT.
ES_SCHEDULE = [
    event("ES", "2024-03-28", "2024-03-27T22:00:00", kind="open"),
    event("ES", "2024-03-28", "2024-03-28T21:00:00", kind="close"),
    event("ES", "2024-04-01", "2024-03-28T22:00:00", kind="open"),
    event("ES", "2024-04-01", "2024-04-01T21:00:00", kind="close"),
]
# ZC opens its next session with a 16:45 CT pre-open, before the 17:00 roll.
ZC_SCHEDULE = [
    event("ZC", "2024-03-27", "2024-03-26T21:45:00", kind="pre_open"),
    event("ZC", "2024-03-27", "2024-03-27T19:20:00", kind="close"),
    event("ZC", "2024-03-28", "2024-03-27T21:45:00", kind="pre_open"),
    event("ZC", "2024-03-28", "2024-03-28T01:00:00", kind="open"),
    event("ZC", "2024-03-28", "2024-03-28T19:20:00", kind="close"),
]
CONTRACTS: list[dict[str, object]] = [
    {"ticker": "ESM4", "product_code": "ES"},
    {"ticker": "ESM4", "product_code": "NOT-ES"},  # a second row keeps the first product
    {"ticker": "ZCK4", "product_code": "ZC"},
    {"ticker": "UNASKED", "product_code": "XX"},  # not asked for: ignored
    {"ticker": "ESU4"},  # no product: ignored
]


class Clock:
    """The calendar's clock: seconds since the epoch, moved by the test."""

    def __init__(self) -> None:
        self.now = datetime(2024, 3, 28, 23, tzinfo=UTC).timestamp()

    def __call__(self) -> float:
        return self.now


class FakeFetch:
    """The REST transport's `fetch_rows`: scripted answers, recorded calls, held paths."""

    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.answers: dict[str, list[dict[str, object]] | Exception] = {
            CONTRACTS_PATH: list(CONTRACTS),
            SCHEDULES_PATH: [*ES_SCHEDULE, *ZC_SCHEDULE],
        }
        self.held: set[str] = set()  # the paths whose requests wait for `release`
        self.release = threading.Event()

    def __call__(self, path: str, query: Mapping[str, object]) -> list[dict[str, object]]:
        with self.cond:
            self.calls.append((path, dict(query)))
            self.cond.notify_all()
            held = path in self.held
        if held:
            assert self.release.wait(TIMEOUT)
        answer = self.answers[path]
        if isinstance(answer, Exception):
            raise answer
        return list(answer)

    def hold(self, *paths: str) -> None:
        with self.cond:
            self.held = set(paths)
        self.release.clear()

    def wait(self, count: int) -> None:
        with self.cond:
            assert self.cond.wait_for(lambda: len(self.calls) >= count, TIMEOUT)

    def paths(self) -> list[str]:
        with self.cond:
            return [path for path, _ in self.calls]


@pytest.fixture
def fetch() -> FakeFetch:
    return FakeFetch()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def calendar(fetch: FakeFetch, clock: Clock) -> Iterator[FuturesSessions]:
    sessions = FuturesSessions(fetch, label="test", clock=clock)
    yield sessions
    fetch.release.set()
    sessions.close()


def correct(
    calendar: FuturesSessions, rows: list[tuple[str | None, str | None, str | None]]
) -> tuple[list[str | None], list[tuple[str, str]]]:
    symbols = pa.array([s for s, _, _ in rows], pa.string())
    instants = pa.array([None if t is None else instant(t) for _, t, _ in rows], pa.int64())
    dates = pa.array([d for _, _, d in rows], pa.string())
    fixed, misses = calendar.correct(symbols, instants, dates)
    return fixed.to_pylist(), misses


def idle(calendar: FuturesSessions) -> None:
    assert calendar.wait_idle(TIMEOUT)


def watched(calendar: FuturesSessions, *symbols: str) -> None:
    calendar.watch(symbols)
    idle(calendar)


# Thursday 18:00 CT before Good Friday: the rule says 03-29, which has no session; ES's Monday
# session opened at 17:00. Placed only once ES's schedule is loaded.
FOLDED = ("ESM4", "2024-03-28T23:00:00", "2024-03-29")


def test_watch_looks_up_products_then_loads_their_schedules(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    watched(calendar, "ESM4", "ZCK4", "ESU4")
    contracts, schedules = fetch.calls[0], fetch.calls[1]
    assert contracts == (CONTRACTS_PATH, {"ticker.any_of": ("ESM4", "ESU4", "ZCK4"), "limit": 1000})
    assert schedules[0] == SCHEDULES_PATH
    # Today, in sessions, is Friday 03-29: a week back, ten days ahead.
    assert schedules[1] == {
        "product_code.any_of": ("ES", "ZC"),
        "session_end_date.gte": date(2024, 3, 22),
        "session_end_date.lte": date(2024, 4, 8),
        "limit": 1000,
    }
    # ESM4 takes ES (the first row naming it), whose Monday session opened on Thursday; ESU4
    # has no product in the answer, so its rows are misses.
    dates, misses = correct(calendar, [FOLDED, ("ESU4", "2024-03-28T15:00:00", "2024-03-28")])
    assert dates == ["2024-04-01", "2024-03-28"]
    assert [symbol for symbol, _ in misses] == ["ESU4"]
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


def test_a_watched_contract_is_asked_for_once(calendar: FuturesSessions, fetch: FakeFetch) -> None:
    watched(calendar, "ESM4")
    watched(calendar, "ESM4")  # known: no new request
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


@pytest.mark.parametrize(
    "spread", ["ZCK4-ZCN4", "BOB 02-01 U6", "BZ:BF F7-G7-H7", "6A:XF:AUDUSD:Q6"]
)
def test_a_spread_is_never_looked_up_and_keeps_the_rule_date(
    *,
    calendar: FuturesSessions,
    fetch: FakeFetch,
    caplog: pytest.LogCaptureFixture,
    spread: str,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        watched(calendar, spread)
        row = (spread, "2024-03-27T21:50:00", "2024-03-27")
        assert correct(calendar, [row, row]) == (["2024-03-27", "2024-03-27"], [])
        idle(calendar)
    assert fetch.calls == []
    # Logged once, at DEBUG: a spread is no error.
    assert [r.levelno for r in caplog.records if spread in r.getMessage()] == [logging.DEBUG]


def test_a_long_watchlist_is_looked_up_in_chunks(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    tickers = [f"C{n:03d}" for n in range(101)]
    watched(calendar, *tickers)
    sent = [query["ticker.any_of"] for path, query in fetch.calls if path == CONTRACTS_PATH]
    assert [len(chunk) for chunk in sent if isinstance(chunk, tuple)] == [100, 1]


def test_what_is_queued_while_a_request_runs_is_served_together(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    calendar.watch(["ZCK4"])
    calendar.watch(["ZCN4"])
    fetch.release.set()
    idle(calendar)
    contracts = [query for path, query in fetch.calls if path == CONTRACTS_PATH]
    assert contracts[1]["ticker.any_of"] == ("ZCK4", "ZCN4")


def test_rows_inside_their_session_keep_the_rule_date(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])


def test_a_holiday_fold_moves_the_row_to_the_next_session(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [FOLDED])
    assert (dates, misses) == (["2024-04-01"], [])


def test_a_pre_open_before_the_roll_moves_to_the_next_session(calendar: FuturesSessions) -> None:
    watched(calendar, "ZCK4")
    # 16:50 CT: before 17:00, so the rule keeps today; the 16:45 pre-open began tomorrow's.
    dates, _ = correct(calendar, [("ZCK4", "2024-03-27T21:50:00", "2024-03-27")])
    assert dates == ["2024-03-28"]


def test_a_row_between_sessions_keeps_the_rule_date(
    calendar: FuturesSessions, caplog: pytest.LogCaptureFixture
) -> None:
    watched(calendar, "ESM4")
    with caplog.at_level(logging.ERROR):
        dates, misses = correct(calendar, [("ESM4", "2024-03-28T21:30:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])
    assert not caplog.records


def test_rows_without_a_value_are_left_alone(calendar: FuturesSessions) -> None:
    dates, misses = correct(
        calendar,
        [
            (None, "2024-03-28T15:00:00", "2024-03-28"),
            ("ESM4", None, "2024-03-28"),
            ("ESM4", "2024-03-28T15:00:00", None),
        ],
    )
    assert (dates, misses) == (["2024-03-28", "2024-03-28", None], [])


def test_an_unknown_contract_keeps_the_rule_date_silently_and_is_looked_up(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    assert correct(calendar, [FOLDED]) == (["2024-03-29"], [])
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


@pytest.mark.parametrize("path", [CONTRACTS_PATH, SCHEDULES_PATH])
def test_a_lookup_still_loading_after_a_minute_makes_misses(
    *,
    calendar: FuturesSessions,
    fetch: FakeFetch,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
    path: str,
) -> None:
    # The request is in flight: at first the row keeps the rule's date silently.
    fetch.hold(path)
    calendar.watch(["ESM4"])
    fetch.wait(1 if path == CONTRACTS_PATH else 2)
    assert correct(calendar, [FOLDED]) == (["2024-03-29"], [])
    clock.now += PATIENCE - 1
    assert correct(calendar, [FOLDED]) == (["2024-03-29"], [])
    # A minute after it was asked for, its rows are misses, logged once.
    clock.now += 1
    with caplog.at_level(logging.ERROR):
        dates, misses = correct(calendar, [FOLDED, FOLDED])
    assert dates == ["2024-03-29", "2024-03-29"]
    assert misses == [
        (
            "ESM4",
            (
                "futures session of ESM4 not confirmed: its schedule was not loaded after 60 s; "
                "its rows keep the session the 17:00 Chicago rule gives them, 2024-03-29"
            ),
        )
    ]
    assert caplog.text.count("not loaded after 60 s") == 1
    # Once it loads, the rows place again.
    fetch.release.set()
    idle(calendar)
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_row_past_the_loaded_sessions_is_a_miss_logged_once(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    watched(calendar, "ESM4")
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")
    with caplog.at_level(logging.ERROR):
        dates, misses = correct(calendar, [row, row])
        again = correct(calendar, [row])
    assert dates == ["2024-04-02", "2024-04-02"]
    assert misses == [
        (
            "ESM4",
            (
                "futures session of ESM4 not confirmed: its loaded schedule (ES) ends with the "
                "session of 2024-04-01; its rows keep the session the 17:00 Chicago rule gives "
                "them, 2024-04-02"
            ),
        )
    ]
    assert again == (["2024-04-02"], misses)  # every batch shows it; the log once
    assert caplog.text.count("not confirmed") == 1
    # The miss asked for a reload, which the spacing allows once the schedule's own load aged:
    # misses within `RELOAD_SPACING` of the load make no request, one after it makes one.
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 1
    clock.now += SPACING - 1
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 1
    clock.now += 1
    correct(calendar, [row])
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2
    # And the reload's own time starts the next spacing.
    clock.now += SPACING - 1
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2


def test_a_row_at_the_last_close_is_past_the_loaded_sessions(calendar: FuturesSessions) -> None:
    # A session's close is its first instant without trading: a row stamped exactly at the
    # close of the last loaded session lies past every loaded session.
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [("ESM4", "2024-04-01T21:00:00", "2024-04-01")])
    assert dates == ["2024-04-01"]
    assert "its loaded schedule (ES) ends with the session of 2024-04-01" in misses[0][1]
    # A second earlier, the row is inside it.
    assert correct(calendar, [("ESM4", "2024-04-01T20:59:59", "2024-04-01")]) == (
        ["2024-04-01"],
        [],
    )


def test_a_row_before_the_loaded_sessions_is_a_miss(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    _, misses = correct(calendar, [("ESM4", "2024-03-20T15:00:00", "2024-03-20")])
    assert "its loaded schedule (ES) begins with the session of 2024-03-28" in misses[0][1]


def test_a_contract_massive_does_not_list_is_a_miss_and_asked_again(
    calendar: FuturesSessions, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "NOPE5")
    _, misses = correct(calendar, [("NOPE5", "2024-03-28T15:00:00", "2024-03-28")])
    assert misses == [
        (
            "NOPE5",
            (
                "futures session of NOPE5 not confirmed: Massive's futures contracts do not "
                "list it; its rows keep the session the 17:00 Chicago rule gives them, "
                "2024-03-28"
            ),
        )
    ]
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH]  # asked within the spacing: no request
    clock.now += SPACING
    correct(calendar, [("NOPE5", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, CONTRACTS_PATH]


def test_a_failed_lookup_is_logged_and_its_rows_are_misses(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[CONTRACTS_PATH] = MassiveError("HTTP 500")
    with caplog.at_level(logging.ERROR):
        watched(calendar, "ESM4")
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "futures schedules: looking up its product failed: HTTP 500 (ESM4)" in caplog.text
    assert "not confirmed: looking up its product failed: HTTP 500" in misses[0][1]
    # Asked again after the spacing; a second failure keeps what the calendar knew.
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-29")])
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, CONTRACTS_PATH]
    fetch.answers[CONTRACTS_PATH] = list(CONTRACTS)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-30")])
    idle(calendar)
    # Loaded at last: the failure is forgotten and the rows place.
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_failed_schedule_is_logged_its_rows_are_misses_and_it_is_retried(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 429")
    with caplog.at_level(logging.ERROR):
        watched(calendar, "ESM4")
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "futures schedules: loading its schedule failed: HTTP 429 (ES)" in caplog.text
    assert "not confirmed: loading its schedule failed: HTTP 429" in misses[0][1]
    fetch.answers[SCHEDULES_PATH] = list(ES_SCHEDULE)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_day_old_schedule_is_loaded_again(
    calendar: FuturesSessions, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 1  # fresh: not reloaded
    clock.now += timedelta(days=1).total_seconds()
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH, SCHEDULES_PATH]


def test_close_stops_the_worker_once_and_queues_nothing_after(
    fetch: FakeFetch, clock: Clock
) -> None:
    calendar = FuturesSessions(fetch, label="test", clock=clock)
    calendar.close()
    calendar.close()
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    calendar.watch(["ESM4"])
    assert correct(calendar, [FOLDED]) == (["2024-03-29"], [])
    assert calendar.wait_idle(0)
    assert fetch.calls == []


@pytest.mark.parametrize(
    "failure",
    [MassiveError("MassiveSession #1 is closed"), RuntimeError("cannot schedule new futures")],
    ids=["closed-client", "unexpected"],
)
def test_a_lookup_cut_short_by_close_is_not_logged(
    *, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture, failure: Exception
) -> None:
    # The session closes while a request is on the wire; the request then fails against the
    # closed client, in whatever way. That is no failure to report.
    calendar = FuturesSessions(fetch, label="test", clock=clock)
    fetch.answers[CONTRACTS_PATH] = failure
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    closing = threading.Thread(target=calendar.close)
    with caplog.at_level(logging.ERROR):
        closing.start()
        assert calendar.wait_idle(TIMEOUT)  # closed
        fetch.release.set()
        closing.join(TIMEOUT)
    assert not closing.is_alive()
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    assert caplog.records == []


def test_a_schedule_without_events_for_a_product_places_nothing(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    fetch.answers[SCHEDULES_PATH] = [
        event("XX", "2024-03-28", "2024-03-27T22:00:00"),  # another product's
        {"product_code": "ES", "session_end_date": None, "timestamp": "2024-03-27T22:00:00"},
    ]
    watched(calendar, "ESM4")
    _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "its loaded schedule (ES) holds no session;" in misses[0][1]


def test_the_module_names_its_paths() -> None:
    assert futures_sessions.CONTRACTS_PATH == "/futures/v1/contracts"
    assert futures_sessions.SCHEDULES_PATH == "/futures/v1/schedules"


def test_the_worker_outlives_an_unexpected_error(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    # An answer the calendar cannot read (here a defect's ValueError) is a failed lookup: its
    # rows are misses, and the worker keeps serving later lookups.
    fetch.answers[CONTRACTS_PATH] = ValueError("bad answer")
    with caplog.at_level(logging.ERROR):
        watched(calendar, "ESM4")
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "futures schedules: a lookup failed" in caplog.text
    assert "not confirmed: looking it up failed: ValueError('bad answer')" in misses[0][1]
    fetch.answers[CONTRACTS_PATH] = list(CONTRACTS)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])
