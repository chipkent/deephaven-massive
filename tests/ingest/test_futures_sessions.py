"""Tests for the futures session calendar of streamed rows (`_ingest/futures_sessions.py`).

The calendar's worker makes its requests through an injected `fetch`, here a scripted fake that
records each call and can hold a request open, and reads the time from an injected clock, so
every reload and spacing decision is deterministic. A test waits only on observable state: a
recorded request, or the dates `correct` returns.
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
    RELOAD_SPACING,
    SCHEDULES_PATH,
    FuturesSessions,
    Session,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

TIMEOUT = 15.0
SPACING = RELOAD_SPACING.total_seconds()


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
    """The REST transport's `fetch_rows`: scripted answers, recorded calls, an optional hold."""

    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.answers: dict[str, list[dict[str, object]] | Exception] = {
            CONTRACTS_PATH: list(CONTRACTS),
            SCHEDULES_PATH: [*ES_SCHEDULE, *ZC_SCHEDULE],
        }
        self.hold = threading.Event()
        self.hold.set()

    def __call__(self, path: str, query: Mapping[str, object]) -> list[dict[str, object]]:
        with self.cond:
            self.calls.append((path, dict(query)))
            self.cond.notify_all()
        assert self.hold.wait(TIMEOUT)
        answer = self.answers[path]
        if isinstance(answer, Exception):
            raise answer
        return list(answer)

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
    fetch.hold.set()
    sessions.close()


def correct(
    calendar: FuturesSessions, rows: list[tuple[str | None, str | None, str | None]]
) -> tuple[list[str | None], list[tuple[str, str]]]:
    symbols = pa.array([s for s, _, _ in rows], pa.string())
    instants = pa.array([None if t is None else instant(t) for _, t, _ in rows], pa.int64())
    dates = pa.array([d for _, _, d in rows], pa.string())
    fixed, misses = calendar.correct(symbols, instants, dates)
    return fixed.to_pylist(), misses


def until(condition: Callable[[], bool]) -> None:
    # A bounded wait for the worker to publish what it loaded.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def loaded(calendar: FuturesSessions, *products: str) -> Callable[[], bool]:
    def check() -> bool:
        with calendar._lock:
            return all(product in calendar._sessions for product in products)

    return check


def known(calendar: FuturesSessions, *contracts: str) -> Callable[[], bool]:
    def check() -> bool:
        with calendar._lock:
            return all(contract in calendar._products for contract in contracts)

    return check


def watched(calendar: FuturesSessions, *symbols: str) -> None:
    calendar.watch(symbols)
    until(known(calendar, *(s.partition("-")[0] for s in symbols)))


def test_watch_looks_up_products_then_loads_their_schedules(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    calendar.watch(["ESM4", "ZCK4-ZCN4", "ESU4"])
    until(loaded(calendar, "ES", "ZC"))
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
    with calendar._lock:
        assert calendar._products == {"ESM4": "ES", "ESU4": None, "ZCK4": "ZC"}
        es = calendar._sessions["ES"]
    assert es[1] == Session(
        date(2024, 4, 1),
        instant("2024-03-28T22:00:00"),
        instant("2024-04-01T21:00:00"),
    )


def test_a_watched_contract_is_asked_for_once(calendar: FuturesSessions, fetch: FakeFetch) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    calendar.watch(["ESM4"])  # known: no new request
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


def test_a_long_watchlist_is_looked_up_in_chunks(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    tickers = [f"C{n:03d}" for n in range(101)]
    calendar.watch(tickers)
    until(known(calendar, *tickers))
    sent = [query["ticker.any_of"] for path, query in fetch.calls if path == CONTRACTS_PATH]
    assert [len(chunk) for chunk in sent] == [100, 1]  # type: ignore[arg-type]  # tuples of str


def test_what_is_queued_while_a_request_runs_is_served_together(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    fetch.hold.clear()
    calendar.watch(["ESM4"])
    fetch.wait(1)
    calendar.watch(["ZCK4"])
    calendar.watch(["ZCN4"])
    fetch.hold.set()
    until(known(calendar, "ESM4", "ZCK4", "ZCN4"))
    contracts = [query for path, query in fetch.calls if path == CONTRACTS_PATH]
    assert contracts[1]["ticker.any_of"] == ("ZCK4", "ZCN4")


def test_rows_inside_their_session_keep_the_rule_date(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])


def test_a_holiday_fold_moves_the_row_to_the_next_session(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    # Thursday 18:00 CT: the rule says Good Friday, which has no session; Monday's opened.
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T23:00:00", "2024-03-29")])
    assert (dates, misses) == (["2024-04-01"], [])


def test_a_pre_open_before_the_roll_moves_to_the_next_session(calendar: FuturesSessions) -> None:
    watched(calendar, "ZCK4-ZCN4")
    until(loaded(calendar, "ZC"))
    # 16:50 CT: before 17:00, so the rule keeps today; the 16:45 pre-open began tomorrow's.
    dates, _ = correct(calendar, [("ZCK4-ZCN4", "2024-03-27T21:50:00", "2024-03-27")])
    assert dates == ["2024-03-28"]


def test_a_row_between_sessions_keeps_the_rule_date(
    calendar: FuturesSessions, caplog: pytest.LogCaptureFixture
) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
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
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])
    until(loaded(calendar, "ES"))
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


def test_a_product_not_loaded_yet_keeps_the_rule_date_silently(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    fetch.answers[SCHEDULES_PATH] = []
    fetch.hold.clear()
    calendar.watch(["ESM4"])
    fetch.wait(1)
    fetch.hold.set()
    until(known(calendar, "ESM4"))
    fetch.hold.clear()
    # The schedule request is in flight: the product is known but not loaded.
    with calendar._lock:
        calendar._sessions = {}
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])


def test_a_row_past_the_loaded_sessions_is_a_miss_reported_once(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")
    with caplog.at_level(logging.ERROR):
        dates, misses = correct(calendar, [row, row])
        again = correct(calendar, [row])
    assert dates == ["2024-04-02", "2024-04-02"]
    assert misses == [
        (
            "ESM4",
            (
                "futures session of ESM4 not confirmed: its schedule (ES) holds no session "
                "after 2024-04-01T22:00:00+00:00; its rows keep the session the 17:00 Chicago "
                "rule gives them, 2024-04-02"
            ),
        )
    ]
    assert again == (["2024-04-02"], [])
    assert caplog.text.count("not confirmed") == 1
    # The miss asked for a reload, which the spacing allows once the schedule's own load aged.
    assert fetch.paths().count(SCHEDULES_PATH) == 1
    clock.now += SPACING
    correct(calendar, [row])
    fetch.wait(3)
    assert fetch.paths()[-1] == SCHEDULES_PATH


def test_a_row_before_the_loaded_sessions_is_a_miss(calendar: FuturesSessions) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    _, misses = correct(calendar, [("ESM4", "2024-03-20T15:00:00", "2024-03-20")])
    assert "holds no session before 2024-03-20T15:00:00+00:00" in misses[0][1]


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
    assert fetch.paths() == [CONTRACTS_PATH]  # asked within the spacing: no request
    clock.now += SPACING
    correct(calendar, [("NOPE5", "2024-03-28T15:00:00", "2024-03-28")])
    fetch.wait(2)
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
    fetch.wait(2)
    fetch.answers[CONTRACTS_PATH] = list(CONTRACTS)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-30")])
    until(loaded(calendar, "ES"))
    with calendar._lock:
        assert calendar._products["ESM4"] == "ES"
        assert "ESM4" not in calendar._failed


def test_a_failed_schedule_is_logged_its_rows_are_misses_and_it_is_retried(
    *, calendar: FuturesSessions, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 429")
    with caplog.at_level(logging.ERROR):
        watched(calendar, "ESM4")
        fetch.wait(2)
        until(lambda: "loading its schedule failed" in caplog.text)
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "not confirmed: loading its schedule failed: HTTP 429" in misses[0][1]
    fetch.answers[SCHEDULES_PATH] = list(ES_SCHEDULE)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    until(loaded(calendar, "ES"))
    with calendar._lock:
        assert "ES" not in calendar._failed


def test_a_day_old_schedule_is_loaded_again(
    calendar: FuturesSessions, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert fetch.paths().count(SCHEDULES_PATH) == 1  # fresh: not reloaded
    clock.now += timedelta(days=1).total_seconds()
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    fetch.wait(3)
    assert fetch.paths()[-1] == SCHEDULES_PATH


def test_close_stops_the_worker_once_and_queues_nothing_after(
    fetch: FakeFetch, clock: Clock
) -> None:
    calendar = FuturesSessions(fetch, label="test", clock=clock)
    calendar.close()
    calendar.close()
    assert not calendar._thread.is_alive()
    calendar.watch(["ESM4"])
    assert calendar._queue.empty()
    assert fetch.calls == []


def test_a_schedule_without_events_for_a_product_places_nothing(
    calendar: FuturesSessions, fetch: FakeFetch
) -> None:
    fetch.answers[SCHEDULES_PATH] = [
        event("XX", "2024-03-28", "2024-03-27T22:00:00"),  # another product's
        {"product_code": "ES", "session_end_date": None, "timestamp": "2024-03-27T22:00:00"},
    ]
    watched(calendar, "ESM4")
    until(loaded(calendar, "ES"))
    _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "holds no session before" in misses[0][1]


def test_the_module_names_its_paths() -> None:
    assert futures_sessions.CONTRACTS_PATH == "/futures/v1/contracts"
    assert futures_sessions.SCHEDULES_PATH == "/futures/v1/schedules"
