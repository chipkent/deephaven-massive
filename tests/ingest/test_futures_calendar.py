"""Tests for the futures session calendar of streamed rows (`_ingest/futures_calendar.py`).

The calendar's worker makes its requests through an injected `fetch`, here a scripted fake that
records each call, answers each path from a script, and can hold the requests of one path open;
it reads the time from an injected clock, so every retry, discard, and reload decision is
deterministic. A test waits only on observable state: a recorded request, or the calendar's
work queue (`_WatchedQueue`, put in place of the module's), which tells when the worker has
served every lookup queued so far and waits for more.
"""

from __future__ import annotations

import logging
import queue
import threading
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from typing_extensions import override

from deephaven_massive._errors import (
    LIBRARY_BUG,
    AuthenticationError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    session_closed,
)
from deephaven_massive._ingest import futures_calendar
from deephaven_massive._ingest.futures_calendar import (
    RELOAD_SPACING,
    RETRY_DELAYS,
    RETRY_EVERY,
    TICKERS_PER_REQUEST,
    FuturesCalendar,
)
from deephaven_massive._tables.futures_dates import UNLISTED_AFTER, LookupState, Readiness
from deephaven_massive.namespaces import _futures as futures

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from deephaven_massive import _massive
    from deephaven_massive._datasets.catalog import DatasetSpec

TIMEOUT = 15.0


def _fetch_path(spec: DatasetSpec) -> str:
    path = spec.fetch_path
    assert path is not None
    return path


SNAPSHOT_PATH = _fetch_path(futures.SNAPSHOTS)
CONTRACTS_PATH = _fetch_path(futures.CONTRACTS)
SCHEDULES_PATH = _fetch_path(futures.SCHEDULES)
PATHS = {
    "snapshot_path": SNAPSHOT_PATH,
    "contracts_path": CONTRACTS_PATH,
    "schedules_path": SCHEDULES_PATH,
}
"""The paths the session passes: those of the futures reference datasets."""
SPACING = RELOAD_SPACING.total_seconds()


def answered(path: str, status: int, text: str) -> str:
    """Return Massive's answer to a GET of `path`, as the REST client words it."""
    return f'Massive answered GET {path} on api.massive.com with HTTP {status}: "{text}".'


UNAVAILABLE = "Service Unavailable"
NOT_ENTITLED = "You are not entitled to this data"


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
SNAPSHOT: list[dict[str, object]] = [
    {"details": {"ticker": "ESM4", "product_code": "ES"}},
    {"details": {"ticker": "CLZ6-CLF7", "product_code": "ES"}},  # a combo, by Massive's code
    {"details": {"ticker": "ZCK4", "product_code": "ZC"}},
    {"details": {"ticker": "ESU4"}},  # no product: not found here
    {"last_trade": {}},  # no details: skipped
]
CONTRACTS: list[dict[str, object]] = [
    {"ticker": "NEWZ6", "product_code": "ES"},
    {"ticker": "NEWZ6", "product_code": "NOT-ES"},  # a second row keeps the first product
    {"ticker": "ESU4", "product_code": "ES"},
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


Answer = list[dict[str, object]] | Exception


def _ticker(row: dict[str, object]) -> object:
    details = row.get("details")
    return details.get("ticker") if isinstance(details, dict) else row.get("ticker")


def _asked(rows: list[dict[str, object]], query: Mapping[str, object]) -> list[dict[str, object]]:
    # Massive answers only the tickers a lookup names.
    wanted = query.get("ticker.any_of")
    if not isinstance(wanted, tuple):
        return rows
    return [row for row in rows if _ticker(row) is None or _ticker(row) in wanted]


class FakeFetch:
    """The REST transport's `fetch_rows`: scripted answers, recorded calls, held paths."""

    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.answers: dict[str, Answer | Callable[[Mapping[str, object]], Answer]] = {
            SNAPSHOT_PATH: list(SNAPSHOT),
            CONTRACTS_PATH: list(CONTRACTS),
            SCHEDULES_PATH: [*ES_SCHEDULE, *ZC_SCHEDULE],
        }
        self.held: set[str] = set()  # the paths whose requests wait for `release`
        self.release = threading.Event()
        self.charged: list[tuple[str, _massive.Cancel]] = []  # each call's bucket and token

    def __call__(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: _massive.Cancel
    ) -> list[dict[str, object]]:
        with self.cond:
            self.calls.append((template, dict(query)))
            self.charged.append((bucket, cancel))
            self.cond.notify_all()
            held = template in self.held
        if held:
            assert self.release.wait(TIMEOUT)
        answer = self.answers[template]
        if callable(answer):
            answer = answer(query)
        if isinstance(answer, Exception):
            raise answer
        return _asked(list(answer), query)

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

    def queries(self, path: str) -> list[dict[str, object]]:
        with self.cond:
            return [query for called, query in self.calls if called == path]


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
    # The dates, and the misses this call logged (each is logged once per product and date).
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


def idle(calendar: FuturesCalendar) -> None:
    watched = calendar._queue
    assert isinstance(watched, _WatchedQueue)
    watched.wait_idle()


def watched(calendar: FuturesCalendar, *symbols: str) -> None:
    calendar.watch(symbols)
    idle(calendar)


def state(calendar: FuturesCalendar, symbol: str) -> LookupState:
    # The state, after the worker served what asking for it queued.
    calendar.states([symbol])
    idle(calendar)
    return calendar.states([symbol])[symbol]


# Thursday 18:00 CT before Good Friday: the rule says 03-29, which has no session; ES's Monday
# session opened at 17:00. Placed only once ES's schedule is loaded.
FOLDED = ("ESM4", "2024-03-28T23:00:00", "2024-03-29")
TODAY = date(2024, 3, 29)  # the futures session of the clock's start, Thursday 18:00 CT


# --- lookups ------------------------------------------------------------------------------------


def test_the_snapshot_names_the_product_then_its_schedule_loads(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    watched(calendar, "ESM4", "ZCK4")
    assert fetch.calls == [
        (SNAPSHOT_PATH, {"ticker.any_of": ("ESM4", "ZCK4"), "limit": 50000}),
        (
            SCHEDULES_PATH,
            {
                "product_code.any_of": ("ES", "ZC"),
                "session_end_date.gte": date(2024, 3, 22),
                "session_end_date.lte": date(2024, 4, 8),
                "limit": 1000,
            },
        ),
    ]
    found = calendar.states(["ESM4", "ZCK4"])
    assert {t: s.readiness for t, s in found.items()} == {
        "ESM4": Readiness.READY,
        "ZCK4": Readiness.READY,
    }
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_combination_is_dated_by_the_product_massive_names(calendar: FuturesCalendar) -> None:
    # Never by its ticker's spelling: the snapshot's product code decides.
    watched(calendar, "CLZ6-CLF7")
    assert correct(calendar, [("CLZ6-CLF7", "2024-03-28T23:00:00", "2024-03-29")]) == (
        ["2024-04-01"],
        [],
    )


def test_a_ticker_the_snapshot_lacks_is_looked_up_in_the_contracts_list_by_date(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    watched(calendar, "ESU4", "NEWZ6")
    # The week up to today's session, so each ticker answers one row a day, not its history.
    assert fetch.queries(CONTRACTS_PATH) == [
        {
            "ticker.any_of": ("ESU4", "NEWZ6"),
            "date.gte": TODAY - timedelta(days=7),
            "date.lte": TODAY,
            "limit": 1000,
        }
    ]
    assert state(calendar, "NEWZ6").readiness is Readiness.READY  # the first row's product


def test_a_long_watchlist_is_looked_up_in_chunks(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    tickers = [f"C{n:03d}Z5" for n in range(TICKERS_PER_REQUEST + 1)]
    watched(calendar, *tickers)
    for path in (SNAPSHOT_PATH, CONTRACTS_PATH):
        sent = [query["ticker.any_of"] for query in fetch.queries(path)]
        assert [len(chunk) for chunk in sent if isinstance(chunk, tuple)] == [100, 1]


def test_a_watched_ticker_is_asked_for_once(calendar: FuturesCalendar, fetch: FakeFetch) -> None:
    watched(calendar, "ESM4")
    watched(calendar, "ESM4")  # known: no new request
    assert fetch.paths() == [SNAPSHOT_PATH, SCHEDULES_PATH]


def test_what_is_queued_while_a_request_runs_is_served_together(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    fetch.hold(SNAPSHOT_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(1)
    calendar.watch(["ZCK4"])
    calendar.states(["ESU4"])
    fetch.release.set()
    idle(calendar)
    assert [q["ticker.any_of"] for q in fetch.queries(SNAPSHOT_PATH)] == [
        ("ESM4",),
        ("ESU4", "ZCK4"),
    ]


def test_a_product_another_round_loads_is_not_loaded_twice(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    # ES's schedule is queued by a ticker already found; the round that finds ZCK4 loads only
    # its own product.
    watched(calendar, "ESM4")
    fetch.hold(SCHEDULES_PATH)
    correct(calendar, [("ESM4", "2024-04-01T22:00:00", "2024-04-02")])  # a miss: ES reloads
    fetch.wait(3)
    calendar.watch(["CLZ6-CLF7"])  # ES too, already loading
    fetch.release.set()
    idle(calendar)
    schedules = [q["product_code.any_of"] for q in fetch.queries(SCHEDULES_PATH)]
    assert schedules == [("ES",), ("ES",)]


def test_a_reload_already_running_is_not_asked_again(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")  # past the loaded sessions
    fetch.hold(SCHEDULES_PATH)
    correct(calendar, [row])
    fetch.wait(3)
    clock.now += RETRY_EVERY
    correct(calendar, [row])  # the reload is still on the wire
    fetch.release.set()
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2


# --- waiting, retries, and discards -------------------------------------------------------------


def test_a_ticker_neither_list_holds_waits_and_is_asked_again_on_the_retry_schedule(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    found = state(calendar, "GONEZ6")
    assert found.readiness is Readiness.WAITING
    assert found.reason == (
        "neither Massive's futures snapshot nor its contracts list holds it (asked over 0 min)"
    )
    for asked, delay in enumerate((*RETRY_DELAYS, RETRY_EVERY, RETRY_EVERY), start=1):
        clock.now += delay - 1
        state(calendar, "GONEZ6")
        assert len(fetch.queries(CONTRACTS_PATH)) == asked  # not due yet
        clock.now += 1
        state(calendar, "GONEZ6")
        assert len(fetch.queries(CONTRACTS_PATH)) == asked + 1


def test_an_unlisted_ticker_is_discarded_after_30_minutes_then_asked_every_ten(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    state(calendar, "GONEZ6")
    clock.now += UNLISTED_AFTER.total_seconds() - 1
    assert state(calendar, "GONEZ6").readiness is Readiness.WAITING
    clock.now += 1
    found = state(calendar, "GONEZ6")
    assert found.readiness is Readiness.UNLISTED
    assert found.reason.endswith("(asked over 30 min)")
    clock.now += RETRY_EVERY  # the ask already due, whose answer starts the spacing
    state(calendar, "GONEZ6")
    asked = len(fetch.queries(CONTRACTS_PATH))
    clock.now += RETRY_EVERY
    state(calendar, "GONEZ6")
    assert len(fetch.queries(CONTRACTS_PATH)) == asked  # every ten minutes now
    clock.now += SPACING
    state(calendar, "GONEZ6")
    assert len(fetch.queries(CONTRACTS_PATH)) == asked + 1
    # Once a list holds it, its rows are dated again.
    fetch.answers[CONTRACTS_PATH] = [{"ticker": "GONEZ6", "product_code": "ES"}]
    clock.now += SPACING
    assert state(calendar, "GONEZ6").readiness is Readiness.READY


@pytest.mark.parametrize("path", [SNAPSHOT_PATH, CONTRACTS_PATH])
def test_a_transient_failure_waits_and_retries(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, path: str
) -> None:
    # A failing snapshot sends its tickers to the contracts list; that failing too, they wait.
    fetch.answers[path] = MassiveError(answered(path, 503, UNAVAILABLE), status=503)
    fetch.answers[CONTRACTS_PATH] = MassiveError(
        answered(CONTRACTS_PATH, 503, UNAVAILABLE), status=503
    )
    start = clock.now
    assert state(calendar, "NEWZ6") == LookupState(
        Readiness.WAITING,
        "looking up its product failed: Massive answered GET /futures/v1/contracts on "
        'api.massive.com with HTTP 503: "Service Unavailable".',
        failing_since=start,
    )
    clock.now += RETRY_DELAYS[0]
    fetch.answers[SNAPSHOT_PATH] = list(SNAPSHOT)
    fetch.answers[CONTRACTS_PATH] = list(CONTRACTS)
    assert state(calendar, "NEWZ6").readiness is Readiness.READY


def test_a_transient_failure_is_logged_once_per_run(
    *,
    calendar: FuturesCalendar,
    fetch: FakeFetch,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fetch.answers[SNAPSHOT_PATH] = MassiveError(
        answered(SNAPSHOT_PATH, 503, UNAVAILABLE), status=503
    )
    fetch.answers[CONTRACTS_PATH] = MassiveError(
        answered(CONTRACTS_PATH, 503, UNAVAILABLE), status=503
    )
    with caplog.at_level(logging.WARNING):
        state(calendar, "ESM4")
        clock.now += RETRY_DELAYS[0]
        state(calendar, "ESM4")
    assert caplog.messages == [
        (
            "Looking up the futures schedules of ESM4 failed, so their rows wait until a later "
            "lookup succeeds. Massive answered GET /futures/v1/contracts on api.massive.com "
            'with HTTP 503: "Service Unavailable".'
        )
    ]


@pytest.mark.parametrize("refusal", [AuthenticationError, EntitlementError])
def test_a_refusal_of_the_key_twice_refuses_the_ticker(
    *,
    calendar: FuturesCalendar,
    fetch: FakeFetch,
    clock: Clock,
    refusal: type[MassiveError],
) -> None:
    error = refusal("HTTP 403: not entitled", status=403)
    fetch.answers[SNAPSHOT_PATH] = []
    fetch.answers[CONTRACTS_PATH] = error
    assert state(calendar, "ESM4").readiness is Readiness.WAITING  # asked again in 2 s
    clock.now += RETRY_DELAYS[0]
    assert state(calendar, "ESM4") == LookupState(
        Readiness.REFUSED, "Massive refused the lookup of its product", error=error
    )
    # Asked again after the spacing, in case the plan changed.
    clock.now += SPACING
    fetch.answers[SNAPSHOT_PATH] = list(SNAPSHOT)
    assert state(calendar, "ESM4").readiness is Readiness.READY


def test_a_refusal_of_many_tickers_is_split_until_it_names_one(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    # One ticker Massive refuses (here one whose spelling it cannot take) costs the others
    # nothing: the request is halved until the refusal is about that ticker alone, which then
    # waits like a ticker the lists lack, with Massive's text.
    def answer(query: Mapping[str, object]) -> Answer:
        tickers = query["ticker.any_of"]
        assert isinstance(tickers, tuple)
        if "BAD:1" in tickers:
            return MassiveError(answered(CONTRACTS_PATH, 400, "Invalid ticker: BAD:1"), status=400)
        return [{"ticker": t, "product_code": "ES"} for t in tickers]

    fetch.answers[SNAPSHOT_PATH] = []
    fetch.answers[CONTRACTS_PATH] = answer
    watched(calendar, "AZ6", "BAD:1", "CZ6", "DZ6")
    assert [q["ticker.any_of"] for q in fetch.queries(CONTRACTS_PATH)] == [
        ("AZ6", "BAD:1", "CZ6", "DZ6"),
        ("AZ6", "BAD:1"),
        ("AZ6",),
        ("BAD:1",),
        ("CZ6", "DZ6"),
    ]
    states = calendar.states(["AZ6", "BAD:1", "CZ6", "DZ6"])
    ready = {t for t, s in states.items() if s.readiness is Readiness.READY}
    assert ready == {"AZ6", "CZ6", "DZ6"}
    assert states["BAD:1"] == LookupState(
        Readiness.WAITING,
        "Massive refused its lookup: Massive answered GET /futures/v1/contracts on "
        'api.massive.com with HTTP 400: "Invalid ticker: BAD:1". (asked over 0 min)',
    )


def test_a_snapshot_refused_twice_is_given_up_for_the_contracts_list(
    *,
    calendar: FuturesCalendar,
    fetch: FakeFetch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fetch.answers[SNAPSHOT_PATH] = EntitlementError(
        answered(SNAPSHOT_PATH, 403, NOT_ENTITLED), status=403
    )
    with caplog.at_level(logging.WARNING):
        watched(calendar, "ESU4")
        watched(calendar, "NEWZ6")
        watched(calendar, "OTHERZ6")
    assert fetch.paths().count(SNAPSHOT_PATH) == 2
    assert caplog.messages == [
        (
            "Massive refused the futures snapshot twice, so the library looks up futures "
            "products in Massive's contracts list only. The latest refusal: Massive answered "
            'GET /futures/v1/snapshot on api.massive.com with HTTP 403: "You are not entitled '
            'to this data".'
        )
    ]
    assert state(calendar, "NEWZ6").readiness is Readiness.READY


def test_a_snapshot_failing_for_another_reason_is_kept(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    fetch.answers[SNAPSHOT_PATH] = MassiveError("HTTP 400", status=400)
    watched(calendar, "ESU4")
    watched(calendar, "NEWZ6")
    watched(calendar, "OTHERZ6")
    assert fetch.paths().count(SNAPSHOT_PATH) == 3


def test_a_schedule_load_waits_on_a_transient_failure_and_refuses_on_a_refusal(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError(
        answered(SCHEDULES_PATH, 500, "Internal Server Error"), status=500
    )
    assert state(calendar, "ESM4") == LookupState(
        Readiness.WAITING,
        "loading the schedule of its product ES failed: Massive answered GET "
        '/futures/v1/schedules on api.massive.com with HTTP 500: "Internal Server Error".',
        failing_since=clock.now,
    )
    refusal = MassiveError("HTTP 400", status=400)
    fetch.answers[SCHEDULES_PATH] = refusal
    clock.now += RETRY_DELAYS[0]
    state(calendar, "ESM4")
    clock.now += RETRY_DELAYS[0]
    assert state(calendar, "ESM4") == LookupState(
        Readiness.REFUSED, "Massive refused the schedule of its product ES", error=refusal
    )


def test_a_lookup_still_running_says_so(calendar: FuturesCalendar, fetch: FakeFetch) -> None:
    fetch.hold(SCHEDULES_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(2)
    assert calendar.states(["ESM4"])["ESM4"] == LookupState(
        Readiness.WAITING, "the schedule of its product ES is loading"
    )
    assert calendar.states(["GONEZ6"])["GONEZ6"] == LookupState(
        Readiness.WAITING, "its product is being looked up"
    )


def test_a_loaded_schedule_stays_ready_while_its_reload_fails(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 500", status=500)
    clock.now += timedelta(days=1).total_seconds()
    correct(calendar, [FOLDED])  # a day old: reloaded
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2
    assert state(calendar, "ESM4").readiness is Readiness.READY
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_the_version_grows_with_each_round(calendar: FuturesCalendar) -> None:
    before = calendar.version
    watched(calendar, "ESM4")
    assert calendar.version > before


# --- correction ---------------------------------------------------------------------------------


def test_rows_inside_their_session_keep_the_rule_date(calendar: FuturesCalendar) -> None:
    watched(calendar, "ESM4")
    dates, misses = correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")])
    assert (dates, misses) == (["2024-03-28"], [])


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
    dates, misses = correct(
        calendar,
        [
            (None, "2024-03-28T15:00:00", "2024-03-28"),
            ("ESM4", None, "2024-03-28"),
            ("ESM4", "2024-03-28T15:00:00", None),
        ],
    )
    assert (dates, misses) == ([None, None, None], [])


def test_an_unknown_ticker_has_no_date_until_it_is_looked_up(
    calendar: FuturesCalendar, fetch: FakeFetch, caplog: pytest.LogCaptureFixture
) -> None:
    # Its rows wait in the flush; nothing is logged for a lookup still running.
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._ingest.futures_calendar"):
        assert correct(calendar, [FOLDED]) == ([None], [])
        idle(calendar)
    assert caplog.records == []
    assert fetch.paths() == [SNAPSHOT_PATH, SCHEDULES_PATH]
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_row_whose_schedule_is_loading_has_no_date(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    fetch.hold(SCHEDULES_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(2)
    assert correct(calendar, [FOLDED]) == ([None], [])
    fetch.release.set()
    idle(calendar)
    assert fetch.paths() == [SNAPSHOT_PATH, SCHEDULES_PATH]  # not asked twice


def test_a_row_past_the_loaded_sessions_asks_for_a_reload_once_a_minute(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    # The flush holds such a row until a reload answers; nothing is published or logged here.
    watched(calendar, "ESM4")
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")
    with caplog.at_level(logging.WARNING):
        assert correct(calendar, [row, row]) == ([None, None], [])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2
    clock.now += RETRY_EVERY - 1
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 2
    clock.now += 1
    correct(calendar, [row])
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == 3
    assert caplog.records == []


def test_a_ready_ticker_reports_when_its_schedule_loaded(
    calendar: FuturesCalendar, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    assert state(calendar, "ESM4") == LookupState(Readiness.READY, loaded_at=clock.now)


@pytest.mark.parametrize(
    ("row", "nearest"),
    [
        (
            "2024-04-01T22:00:00",
            (
                "holds 2024-04-01T22:00:00Z; the nearest Massive's schedule "
                "gives are those of 2024-04-01 (before) and none (after)"
            ),
        ),
        (
            "2024-04-01T21:00:00",
            (
                "holds 2024-04-01T21:00:00Z; the nearest Massive's schedule "
                "gives are those of 2024-04-01 (before) and none (after)"
            ),
        ),
        (
            "2024-03-20T15:00:00",
            (
                "holds 2024-03-20T15:00:00Z; the nearest Massive's schedule "
                "gives are those of none (before) and 2024-03-28 (after)"
            ),
        ),
    ],
    ids=["after", "at-the-last-close", "before"],
)
def test_a_row_outside_the_loaded_sessions_has_no_date_and_is_explained(
    calendar: FuturesCalendar, row: str, nearest: str
) -> None:
    watched(calendar, "ESM4")
    assert correct(calendar, [("ESM4", row, row[:10])]) == ([None], [])
    assert calendar.unplaced("ESM4", instant(row)) == f"no session of its product ES {nearest}"


def test_a_row_a_second_before_the_last_close_is_inside_it(calendar: FuturesCalendar) -> None:
    watched(calendar, "ESM4")
    assert correct(calendar, [("ESM4", "2024-04-01T20:59:59", "2024-04-01")]) == (
        ["2024-04-01"],
        [],
    )


def test_a_product_massive_confirms_has_no_session_is_unscheduled_until_it_has_one(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    # The window holds no session of ES, and Massive's latest-session answer is empty too: its
    # rows are withheld, never published undated, and the schedule is asked again each day.
    def answer(query: Mapping[str, object]) -> Answer:
        return [] if "product_code" in query else [event("ZC", "2024-03-28", "2024-03-27T21:45:00")]

    fetch.answers[SCHEDULES_PATH] = answer
    found = state(calendar, "ESM4")
    assert found == LookupState(
        Readiness.UNSCHEDULED, "Massive's futures schedules hold no session of its product ES"
    )
    assert fetch.queries(SCHEDULES_PATH)[-1] == {
        "product_code": "ES",
        "sort": "session_end_date.desc",
        "limit": 1,
    }
    with caplog.at_level(logging.WARNING):
        assert correct(calendar, [FOLDED]) == ([None], [])  # withheld: no miss logged
    fetch.answers[SCHEDULES_PATH] = list(ES_SCHEDULE)
    clock.now += timedelta(days=1).total_seconds()
    correct(calendar, [FOLDED])  # a day later: loaded again
    idle(calendar)
    assert state(calendar, "ESM4").readiness is Readiness.READY
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_product_with_sessions_outside_the_window_is_ready_and_its_rows_unplaced(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    # Massive holds a session of ES, only not in the window: not unscheduled, but no loaded
    # session holds its rows, which the flush holds and then withholds.
    def answer(query: Mapping[str, object]) -> Answer:
        if "product_code" in query:
            return [event("ES", "2028-01-03", "2028-01-02T23:00:00")]
        return [{"product_code": "ES", "session_end_date": None, "timestamp": "x"}]

    fetch.answers[SCHEDULES_PATH] = answer
    assert state(calendar, "ESM4").readiness is Readiness.READY
    assert correct(calendar, [("ESM4", "2024-03-28T15:00:00", "2024-03-28")]) == ([None], [])
    assert calendar.unplaced("ESM4", instant("2024-03-28T15:00:00")).endswith(
        "those of none (before) and none (after)"
    )


def test_a_failed_confirmation_of_no_session_waits_and_retries(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    def answer(query: Mapping[str, object]) -> Answer:
        if "product_code" in query:
            return MassiveError(answered(SCHEDULES_PATH, 502, "Bad Gateway"), status=502)
        return []

    fetch.answers[SCHEDULES_PATH] = answer
    found = state(calendar, "ESM4")
    assert found.readiness is Readiness.WAITING
    assert found.reason == (
        "loading the schedule of its product ES failed: Massive answered GET "
        '/futures/v1/schedules on api.massive.com with HTTP 502: "Bad Gateway".'
    )
    fetch.answers[SCHEDULES_PATH] = list(ES_SCHEDULE)
    clock.now += RETRY_DELAYS[0]
    assert state(calendar, "ESM4").readiness is Readiness.READY


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
    assert fetch.paths() == [SNAPSHOT_PATH, SCHEDULES_PATH, SCHEDULES_PATH]


# --- the worker ---------------------------------------------------------------------------------


def test_close_stops_the_worker_once_and_queues_nothing_after(
    fetch: FakeFetch, clock: Clock
) -> None:
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    assert calendar.alive()
    calendar.close()
    calendar.close()
    assert calendar.alive()  # closed is not dead
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    calendar.watch(["ESM4"])
    assert correct(calendar, [FOLDED])[0] == [None]
    assert fetch.calls == []


def test_a_worker_that_ended_without_close_is_not_alive(fetch: FakeFetch, clock: Clock) -> None:
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    calendar._queue.put(futures_calendar._STOP)  # the worker ends; the calendar did not close
    calendar._thread.join(TIMEOUT)
    assert not calendar.alive()
    calendar.close()


def test_each_lookup_is_charged_to_futures_in_a_lane_of_its_own(
    calendar: FuturesCalendar, fetch: FakeFetch
) -> None:
    watched(calendar, "ESM4")
    (first_bucket, first), (second_bucket, second) = fetch.charged
    assert first_bucket == second_bucket == "futures"
    assert first is not second  # each lookup waits in its own lane
    assert not first.is_set()
    assert not second.is_set()


@pytest.mark.parametrize("held", [SNAPSHOT_PATH, SCHEDULES_PATH])
def test_closing_ends_a_lookup_still_waiting_for_the_rate_governor(
    fetch: FakeFetch, clock: Clock, held: str
) -> None:
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.hold(held)
    calendar.watch(["ESM4"])
    calls = 1 if held == SNAPSHOT_PATH else 2  # the schedule follows the ticker's product
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
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.hold(SNAPSHOT_PATH)
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
    [
        MassiveError(session_closed()),
        RuntimeError("cannot schedule new futures"),
    ],
    ids=["closed-client", "unexpected"],
)
def test_a_lookup_cut_short_by_close_is_not_logged(
    *, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture, failure: Exception
) -> None:
    calendar = FuturesCalendar(fetch, **PATHS, label="test", clock=clock)
    fetch.answers[SNAPSHOT_PATH] = []
    fetch.answers[CONTRACTS_PATH] = failure
    fetch.hold(CONTRACTS_PATH)
    calendar.watch(["ESM4"])
    fetch.wait(2)
    closing = threading.Thread(target=calendar.close)
    with caplog.at_level(logging.WARNING):
        closing.start()
        fetch.release.set()
        closing.join(TIMEOUT)
    assert not closing.is_alive()
    assert "test futures schedules" not in {t.name for t in threading.enumerate()}
    assert caplog.records == []


def test_a_lookup_answer_the_library_cannot_read_is_logged_as_a_defect(
    *, calendar: FuturesCalendar, fetch: FakeFetch, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[SNAPSHOT_PATH] = []
    fetch.answers[CONTRACTS_PATH] = MassiveError("bad page", source=ErrorSource.LIBRARY)
    with caplog.at_level(logging.WARNING):
        found = state(calendar, "ESM4")
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None  # a bug, with its traceback
    assert record.getMessage() == (
        "Looking up the futures schedules of ESM4 failed, so their rows wait until a later "
        f"lookup succeeds. bad page. {LIBRARY_BUG}"
    )
    assert found.readiness is Readiness.WAITING
    assert found.failing_since is not None


def test_the_worker_outlives_an_unexpected_error(
    *, calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.answers[SNAPSHOT_PATH] = ValueError("bad answer")
    with caplog.at_level(logging.WARNING):
        found = state(calendar, "ESM4")
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
    assert record.getMessage() == f"Looking up the futures schedules of ESM4 failed. {LIBRARY_BUG}"
    assert found.reason == "looking up its product failed: ValueError('bad answer')"
    fetch.answers[SNAPSHOT_PATH] = list(SNAPSHOT)
    clock.now += RETRY_DELAYS[0]
    assert state(calendar, "ESM4").readiness is Readiness.READY


def test_a_lookup_the_worker_has_no_rule_for_is_a_logged_defect(
    *, calendar: FuturesCalendar, fetch: FakeFetch, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.hold(SNAPSHOT_PATH)
    calendar.watch(["ZCK4"])
    fetch.wait(1)  # the worker is busy, so it takes the next two items together
    with caplog.at_level(logging.ERROR):
        calendar._queue.put(("prices", frozenset({"ESM4"})))
        calendar.watch(["ESM4"])
        fetch.release.set()
        idle(calendar)
    assert caplog.messages == [
        (
            "The futures schedule lookups were handed ('prices', frozenset({'ESM4'})), which is "
            "neither a ticker nor a product. The library skips it and looks up the others. "
            f"{LIBRARY_BUG}"
        )
    ]
    assert correct(calendar, [FOLDED]) == (["2024-04-01"], [])


def test_a_failure_after_an_answer_without_the_ticker_is_an_outage(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    # The lists lacked it, then stopped answering: the outage counts, not the old answer.
    state(calendar, "GONEZ6")
    fetch.answers[SNAPSHOT_PATH] = MassiveError("HTTP 503", status=503)
    fetch.answers[CONTRACTS_PATH] = MassiveError("HTTP 503", status=503)
    clock.now += RETRY_DELAYS[0]
    found = state(calendar, "GONEZ6")
    assert (found.readiness, found.failing_since) == (Readiness.WAITING, clock.now)
    clock.now += UNLISTED_AFTER.total_seconds()
    assert state(calendar, "GONEZ6").readiness is Readiness.WAITING  # never discarded


def test_a_failed_reload_waits_for_its_retry_and_shows_on_the_ready_state(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 500", status=500)
    clock.now += timedelta(days=1).total_seconds()
    correct(calendar, [FOLDED])  # a day old: reloaded, and it fails
    idle(calendar)
    found = state(calendar, "ESM4")
    assert (found.readiness, found.failing_since) == (Readiness.READY, clock.now)
    asked = fetch.paths().count(SCHEDULES_PATH)
    correct(calendar, [FOLDED])
    correct(calendar, [FOLDED])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == asked  # its retry is not due yet
    clock.now += RETRY_DELAYS[0]
    correct(calendar, [FOLDED])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == asked + 1


def test_a_failed_reload_for_a_row_no_session_holds_retries_on_time(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    watched(calendar, "ESM4")
    row = ("ESM4", "2024-04-01T22:00:00", "2024-04-02")  # past the loaded sessions
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 500", status=500)
    correct(calendar, [row])
    idle(calendar)
    asked = fetch.paths().count(SCHEDULES_PATH)
    clock.now += RETRY_DELAYS[0]  # its retry is due, well before a minute
    correct(calendar, [row])
    idle(calendar)
    assert fetch.paths().count(SCHEDULES_PATH) == asked + 1


@pytest.mark.parametrize("later", ["unlisted", "outage"])
def test_a_later_answer_replaces_an_old_refusal(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock, *, later: str
) -> None:
    refusal = EntitlementError("HTTP 403", status=403)
    fetch.answers[SNAPSHOT_PATH] = []
    fetch.answers[CONTRACTS_PATH] = refusal
    state(calendar, "GONEZ6")
    clock.now += RETRY_DELAYS[0]
    assert state(calendar, "GONEZ6").readiness is Readiness.REFUSED
    fetch.answers[CONTRACTS_PATH] = (
        [] if later == "unlisted" else MassiveError("HTTP 503", status=503)
    )
    clock.now += SPACING
    assert state(calendar, "GONEZ6").readiness is Readiness.WAITING


def test_a_new_ticker_of_a_product_whose_schedule_failed_waits_for_its_retry(
    calendar: FuturesCalendar, fetch: FakeFetch, clock: Clock
) -> None:
    fetch.answers[SCHEDULES_PATH] = MassiveError("HTTP 500", status=500)
    watched(calendar, "ESM4")
    asked = fetch.paths().count(SCHEDULES_PATH)
    watched(calendar, "CLZ6-CLF7")  # also ES: its schedule's retry is not due yet
    assert fetch.paths().count(SCHEDULES_PATH) == asked
