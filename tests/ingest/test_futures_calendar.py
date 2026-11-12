"""Tests for the futures session calendar of streamed rows (`_ingest/futures_calendar.py`).

The calendar's worker makes its requests through an injected `fetch`, here a scripted fake that
records each call and can hold the requests of one path open, and reads the time from an
injected clock, so every reload, spacing, and patience decision is deterministic. A test waits
only on observable state: a recorded request, `wait_loaded`, or the calendar's work queue
(`_WatchedQueue`, put in place of the module's), which tells when the worker has served every
lookup queued so far and waits for more.
"""

from __future__ import annotations

import logging
import queue
import threading
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest
from typing_extensions import override

from deephaven_massive._errors import LIBRARY_BUG, ErrorCause, MassiveError
from deephaven_massive._ingest import futures_calendar
from deephaven_massive._ingest.futures_calendar import (
    RELOAD_SPACING,
    FuturesCalendar,
)
from deephaven_massive.namespaces import _futures as futures

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven_massive import _massive

TIMEOUT = 15.0
CONTRACTS_PATH = cast("str", futures.CONTRACTS.fetch_path)
SCHEDULES_PATH = cast("str", futures.SCHEDULES.fetch_path)
PATHS = {"contracts_path": CONTRACTS_PATH, "schedules_path": SCHEDULES_PATH}
"""The paths the session passes: those of the futures reference datasets."""
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


class _WatchedQueue:
    """The worker's queue, which tells when the worker waits on it with nothing queued.

    It stands in for `queue.SimpleQueue` with the three methods the calendar calls.
    """

    def __init__(self) -> None:
        self.items: queue.SimpleQueue[object] = queue.SimpleQueue()
        self.cond = threading.Condition()
        self.waiting = False  # the worker waits in `get`, and nothing was put since

    def put(self, item: object) -> None:
        with self.cond:
            self.waiting = False
        self.items.put(item)

    def get(self) -> object:
        with self.cond:
            self.waiting = True
            self.cond.notify_all()
        item = self.items.get()
        with self.cond:
            self.waiting = False
        return item

    def get_nowait(self) -> object:
        return self.items.get_nowait()

    def wait_idle(self) -> None:
        with self.cond:
            assert self.cond.wait_for(lambda: self.waiting and self.items.empty(), TIMEOUT)


@pytest.fixture(autouse=True)
def _watched_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every calendar of these tests gets a queue that tells when its worker is idle.
    module = SimpleNamespace(SimpleQueue=_WatchedQueue, Empty=queue.Empty)
    monkeypatch.setattr(futures_calendar, "queue", module)


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
        self.charged: list[tuple[str, _massive.Cancel]] = []  # each call's bucket and token

    def __call__(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: _massive.Cancel
    ) -> list[dict[str, object]]:
        path = template
        with self.cond:
            self.calls.append((path, dict(query)))
            self.charged.append((bucket, cancel))
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
def calendar(fetch: FakeFetch, clock: Clock) -> Iterator[FuturesCalendar]:
    sessions = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    yield sessions
    fetch.release.set()
    sessions.close()


class _Misses(logging.Handler):
    # Gathers the miss WARNINGs one `correct` call logs, as `(symbol, message)`.
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.found: list[tuple[str, str]] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage().removeprefix("test: ")
        if message.startswith("futures session of "):
            self.found.append((message.split()[3], message))


def correct(
    calendar: FuturesCalendar, rows: list[tuple[str | None, str | None, str | None]]
) -> tuple[list[str | None], list[tuple[str, str]]]:
    # The dates, and the misses this call logged (each is logged once per key and rule date).
    symbols = pa.array([s for s, _, _ in rows], pa.string())
    instants = pa.array([None if t is None else instant(t) for _, t, _ in rows], pa.int64())
    dates = pa.array([d for _, _, d in rows], pa.string())
    handler = _Misses()
    owner = logging.getLogger("deephaven_massive._ingest.futures_calendar")
    owner.addHandler(handler)
    try:
        fixed = calendar.correct(symbols, instants, dates)
    finally:
        owner.removeHandler(handler)
    return fixed.to_pylist(), handler.found


def null_date(symbol: str, why: str, day: str) -> str:
    return (
        f"futures session of {symbol} not confirmed: {why}; its rows get a null Date, since "
        f"the 17:00 Chicago rule's {day} may be wrong (a holiday or an early pre-open moves the "
        "session); their Timestamp and every other column are kept, and the stream keeps running"
    )


def idle(calendar: FuturesCalendar) -> None:
    cast("_WatchedQueue", calendar._queue).wait_idle()


def watched(calendar: FuturesCalendar, *symbols: str) -> None:
    calendar.watch(symbols)
    idle(calendar)


# Thursday 18:00 CT before Good Friday: the rule says 03-29, which has no session; ES's Monday
# session opened at 17:00. Placed only once ES's schedule is loaded.
FOLDED = ("ESM4", "2024-03-28T23:00:00", "2024-03-29")


def test_watch_looks_up_products_then_loads_their_schedules(
    calendar: FuturesCalendar, fetch: FakeFetch
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
    # has no product in the answer, so its rows are misses, with no date.
    dates, misses = correct(calendar, [FOLDED, ("ESU4", "2024-03-28T15:00:00", "2024-03-28")])
    assert dates == ["2024-04-01", None]
    assert [symbol for symbol, _ in misses] == ["ESU4"]
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


def test_a_watched_contract_is_asked_for_once(calendar: FuturesCalendar, fetch: FakeFetch) -> None:
    watched(calendar, "ESM4")
    watched(calendar, "ESM4")  # known: no new request
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]


@pytest.mark.parametrize(
    "spread",
    ["ZCK4-ZCN4", "BOB 02-01 U6", "BZ:BF F7-G7-H7", "6A:XF:AUDUSD:Q6", "ZZ:NEW COMBO Z6"],
)
def test_a_combo_is_looked_up_as_written_and_a_miss_has_no_date(
    *,
    calendar: FuturesCalendar,
    fetch: FakeFetch,
    caplog: pytest.LogCaptureFixture,
    spread: str,
) -> None:
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        watched(calendar, spread)
        assert caplog.records == []
        row = (spread, "2024-03-27T21:50:00", "2024-03-27")
        why = "Massive's futures contracts do not list it"  # this answer holds no combo
        assert correct(calendar, [row, row]) == (
            [None, None],
            [(spread.split(maxsplit=1)[0], null_date(spread, why, "2024-03-27"))],
        )
        assert correct(calendar, [row]) == ([None], [])
        idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH]  # looked up once, as a contract is
    # The rule's date can be wrong around a holiday, so its rows get none: one WARNING per
    # combo and rule date, however many batches it is in; watching it alone says nothing.
    assert [r.levelno for r in caplog.records if spread in r.getMessage()] == [logging.WARNING]


def test_a_long_watchlist_is_looked_up_in_chunks(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    tickers = [f"C{n:03d}Z5" for n in range(101)]
    watched(calendar, *tickers)
    sent = [query["ticker.any_of"] for path, query in fetch.calls if path == CONTRACTS_PATH]
    assert [len(chunk) for chunk in sent if isinstance(chunk, tuple)] == [100, 1]


def test_what_is_queued_while_a_request_runs_is_served_together(
    calendar: FuturesCalendar, fetch: FakeFetch
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


def test_rows_inside_their_session_keep_the_rule_date(calendar: FuturesCalendar) -> None:
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])


def test_a_holiday_fold_moves_the_row_to_the_next_session(calendar: FuturesCalendar) -> None:
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [FOLDED])
    assert (dates, misses) == (["2024-04-01"], [])


def test_a_pre_open_before_the_roll_moves_to_the_next_session(calendar: FuturesCalendar) -> None:
    watched(calendar, "ZCK4")
    # 16:50 CT: before 17:00, so the rule keeps today; the 16:45 pre-open began tomorrow's.
    dates, _ = correct(calendar, [("ZCK4", "2024-03-27T21:50:00", "2024-03-27")])
    assert dates == ["2024-03-28"]


def test_a_row_between_sessions_gets_the_next_sessions_date(
    calendar: FuturesCalendar, caplog: pytest.LogCaptureFixture
) -> None:
    # After Thursday's close, before the folded Monday session opens: never the rule's guess
    # (Thursday), but the next session the schedule knows.
    watched(calendar, "ESM4")
    with caplog.at_level(logging.WARNING):
        dates, misses = correct(calendar, [("ESM4", "2024-03-28T21:30:00", "2024-03-28")])
    assert (dates, misses) == (["2024-04-01"], [])
    assert not caplog.records


def test_rows_without_a_symbol_or_an_instant_get_no_date(calendar: FuturesCalendar) -> None:
    # Such a row (a symbol `unreadable_symbols` kept as null, say) cannot be placed, and the
    # 17:00 rule's date may be wrong, so it has none.
    dates, misses = correct(
        calendar,
        [
            (None, "2024-03-28T15:00:00", "2024-03-28"),
            ("ESM4", None, "2024-03-28"),
            ("ESM4", "2024-03-28T15:00:00", None),
        ],
    )
    assert (dates, misses) == ([None, None, None], [])


def test_an_unknown_contract_has_no_date_until_it_is_looked_up(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    dates, misses = correct(calendar, [FOLDED])
    assert (dates, misses) == ([None], [])  # still loading: no date, and only DEBUG
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, SCHEDULES_PATH]
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_wait_loaded_returns_once_the_schedules_load(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    fetch.hold(SCHEDULES_PATH)
    calendar.watch(["ESM4", "ZCK4-ZCN4"])  # the spread is never looked up, nor waited for
    assert not calendar.wait_loaded(["ESM4", "ZCK4-ZCN4"], 0.05, stop=threading.Event())
    fetch.release.set()
    assert calendar.wait_loaded(["ESM4", "ZCK4-ZCN4"], TIMEOUT, stop=threading.Event())
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_wait_loaded_returns_once_the_lookups_fail_or_find_nothing(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 429")
    calendar.watch(["ESM4", "NOPZ5"])
    assert calendar.wait_loaded(["ESM4", "NOPZ5"], TIMEOUT, stop=threading.Event())


def test_wake_ends_a_wait_whose_stop_is_set(calendar: FuturesCalendar, fetch: FakeFetch) -> None:
    # A closing session sets its stop event and wakes the calendar, which other sessions keep
    # open: the wait ends at once, reporting the schedules not loaded.
    fetch.hold(SCHEDULES_PATH)
    calendar.watch(["ESM4"])
    stop = threading.Event()
    waiting = threading.Event()
    results: list[bool] = []

    def wait() -> None:
        waiting.set()
        results.append(calendar.wait_loaded(["ESM4"], 3600.0, stop=stop))  # only a wake ends it

    waiter = threading.Thread(target=wait)
    waiter.start()
    assert waiting.wait(TIMEOUT)
    stop.set()
    calendar.wake()
    waiter.join(TIMEOUT)
    assert results == [False]
    fetch.release.set()


def test_wait_loaded_returns_once_closed(fetch: FakeFetch, clock: Clock) -> None:
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    calendar.close()
    assert calendar.wait_loaded(["ESM4"], 0, stop=threading.Event())


@pytest.mark.parametrize(
    ("path", "why"),
    [
        (CONTRACTS_PATH, "its product is still being looked up"),
        (SCHEDULES_PATH, "its schedule is still loading"),
    ],
)
def test_a_row_whose_lookup_is_still_loading_is_a_miss(
    *,
    calendar: FuturesCalendar,
    fetch: FakeFetch,
    caplog: pytest.LogCaptureFixture,
    path: str,
    why: str,
) -> None:
    # The request is in flight: the rule's date may be wrong without the schedule, so the rows
    # get none. A lookup still running is no news, so it logs at DEBUG only, not as a WARNING
    # at every stream's start.
    fetch.hold(path)
    calendar.watch(["ESM4"])
    fetch.wait(1 if path == CONTRACTS_PATH else 2)
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._ingest.futures_calendar"):
        dates, misses = correct(calendar, [FOLDED, FOLDED])
        again = correct(calendar, [FOLDED])
    assert (dates, misses, again) == ([None, None], [], ([None], []))
    # Once per contract (or product), rule date, and reason, however many batches.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.DEBUG, f"test: futures session of ESM4: {why}; its rows get a null Date")
    ]
    # Once it loads, the rows place again.
    fetch.release.set()
    idle(calendar)
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_row_past_the_loaded_sessions_is_a_miss_logged_once(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    watched(calendar, "ESM4")
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")
    with caplog.at_level(logging.WARNING):
        dates, misses = correct(calendar, [row, row])
        again = correct(calendar, [row])
    assert dates == [None, None]
    why = "its loaded schedule (ES) ends with the session of 2024-04-01"
    assert misses == [("ESM4", null_date("ESM4", why, "2024-04-02"))]
    assert again == ([None], [])  # every batch's rows have no date; the log is once
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


def test_a_row_at_the_last_close_is_past_the_loaded_sessions(calendar: FuturesCalendar) -> None:
    # A session's close is its first instant without trading: a row stamped exactly at the
    # close of the last loaded session lies past every loaded session.
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [("ESM4", "2024-04-01T21:00:00", "2024-04-01")])
    assert dates == [None]
    assert "its loaded schedule (ES) ends with the session of 2024-04-01" in misses[0][1]
    # A second earlier, the row is inside it.
    assert correct(calendar, [("ESM4", "2024-04-01T20:59:59", "2024-04-01")]) == (
        ["2024-04-01"],
        [],
    )


def test_a_row_before_the_loaded_sessions_is_a_miss(calendar: FuturesCalendar) -> None:
    watched(calendar, "ESM4")
    _, misses = correct(calendar, [("ESM4", "2024-03-20T15:00:00", "2024-03-20")])
    assert "its loaded schedule (ES) begins with the session of 2024-03-28" in misses[0][1]


def test_a_contract_massive_does_not_list_is_a_miss_and_asked_again(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "NOPZ5")
    _, misses = correct(calendar, [("NOPZ5", "2024-03-28T15:00:00", "2024-03-28")])
    why = "Massive's futures contracts do not list it"
    assert misses == [("NOPZ5", null_date("NOPZ5", why, "2024-03-28"))]
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH]  # asked within the spacing: no request
    clock.now += SPACING
    correct(calendar, [("NOPZ5", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert fetch.paths() == [CONTRACTS_PATH, CONTRACTS_PATH]


def test_a_failed_lookup_is_logged_and_its_rows_are_misses(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[CONTRACTS_PATH] = MassiveError("HTTP 500")
    with caplog.at_level(logging.WARNING):
        watched(calendar, "ESM4")
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "futures schedules: looking up its product failed: HTTP 500 (ESM4)" in caplog.text
    assert {r.levelno for r in caplog.records} == {logging.WARNING}
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


def test_an_unreadable_lookup_answer_is_logged_as_a_library_defect(
    *, calendar: FuturesCalendar, fetch: FakeFetch, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[CONTRACTS_PATH] = MassiveError("bad page", cause=ErrorCause.LIBRARY_DEFECT)
    with caplog.at_level(logging.WARNING):
        watched(calendar, "ESM4")
        correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    [record] = [r for r in caplog.records if "futures schedules: looking up" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"test: futures schedules: looking up its product failed: bad page (ESM4). {LIBRARY_BUG}"
    )


def test_a_failed_schedule_is_logged_its_rows_are_misses_and_it_is_retried(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 429")
    with caplog.at_level(logging.WARNING):
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
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
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
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    calendar.close()
    calendar.close()
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    calendar.watch(["ESM4"])
    assert correct(calendar, [FOLDED])[0] == [None]
    assert fetch.calls == []


def test_closing_wakes_a_caller_waiting_for_the_worker(fetch: FakeFetch, clock: Clock) -> None:
    # A lookup is on the wire, so nothing is loaded; closing the calendar must release a caller
    # already waiting in `wait_loaded` at once, not when the lookup finally returns.
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    waiting = threading.Event()
    results: list[bool] = []

    def wait() -> None:
        waiting.set()
        results.append(calendar.wait_loaded(["ESM4"], TIMEOUT, stop=threading.Event()))

    waiter = threading.Thread(target=wait)
    waiter.start()
    assert waiting.wait(TIMEOUT)
    closing = threading.Thread(target=calendar.close)
    closing.start()
    waiter.join(TIMEOUT / 3)
    assert results == [True]  # woken by the close, while the lookup is still held
    fetch.release.set()
    closing.join(TIMEOUT)
    assert not closing.is_alive()


def test_each_lookup_is_charged_to_futures_in_a_lane_of_its_own(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    watched(calendar, "ESM4")
    assert [path for path, _ in fetch.calls] == [CONTRACTS_PATH, SCHEDULES_PATH]
    (first_bucket, first), (second_bucket, second) = fetch.charged
    assert first_bucket == second_bucket == "futures"
    assert first is not second  # each lookup waits in its own lane
    assert not first.is_set()
    assert not second.is_set()


@pytest.mark.parametrize("held", [CONTRACTS_PATH, SCHEDULES_PATH])
def test_closing_ends_a_lookup_still_waiting_for_the_rate_governor(
    fetch: FakeFetch, clock: Clock, held: str
) -> None:
    # The lookup's token follows the calendar's: a lookup paced behind a Basic futures
    # subscription's limit stops waiting the moment the calendar closes.
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.hold(held)
    calendar.watch(["ESM4"])
    calls = 1 if held == CONTRACTS_PATH else 2  # the schedule follows the contract's product
    fetch.wait(calls)
    _, lookup = fetch.charged[calls - 1]
    assert not lookup.is_set()
    closing = threading.Thread(target=calendar.close, daemon=True)
    closing.start()
    assert lookup.wait(TIMEOUT)
    fetch.release.set()
    closing.join(TIMEOUT)
    assert not closing.is_alive()


def test_a_close_that_does_not_wait_leaves_the_worker_to_end_on_its_own(
    fetch: FakeFetch, clock: Clock
) -> None:
    # A thread holding the update graph's lock closes without waiting: the worker, busy with a
    # request, ends once the request returns.
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    calendar.close(wait=False)
    worker = next(t for t in threading.enumerate() if t.name == "test futures schedules")
    assert worker.is_alive()  # still in its request: the close did not wait for it
    fetch.release.set()
    worker.join(TIMEOUT)
    assert not worker.is_alive()


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
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.answers[CONTRACTS_PATH] = failure
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    closing = threading.Thread(target=calendar.close)
    with caplog.at_level(logging.WARNING):
        closing.start()
        assert calendar.wait_loaded(["ESM4"], TIMEOUT, stop=threading.Event())  # closed
        fetch.release.set()
        closing.join(TIMEOUT)
    assert not closing.is_alive()
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    assert caplog.records == []


def test_a_schedule_without_events_for_a_product_places_nothing(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    fetch.answers[SCHEDULES_PATH] = [
        event("XX", "2024-03-28", "2024-03-27T22:00:00"),  # another product's
        {"product_code": "ES", "session_end_date": None, "timestamp": "2024-03-27T22:00:00"},
    ]
    watched(calendar, "ESM4")
    _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "its loaded schedule (ES) holds no session;" in misses[0][1]
    # An empty schedule stays loaded like any other: a miss past the reload spacing asks for no
    # reload, and only the daily reload of every schedule looks again.
    clock.now += SPACING + 1
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 1


def test_the_worker_outlives_an_unexpected_error(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    # An answer the calendar cannot read (here a defect's ValueError) is a failed lookup: its
    # rows are misses, and the worker keeps serving later lookups.
    fetch.answers[CONTRACTS_PATH] = ValueError("bad answer")
    with caplog.at_level(logging.WARNING):
        watched(calendar, "ESM4")
        _, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert "futures schedules: a lookup failed. This is a bug in deephaven-massive." in caplog.text
    assert logging.ERROR in {r.levelno for r in caplog.records}
    assert "not confirmed: looking it up failed: ValueError('bad answer')" in misses[0][1]
    fetch.answers[CONTRACTS_PATH] = list(CONTRACTS)
    clock.now += SPACING
    correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    idle(calendar)
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])
