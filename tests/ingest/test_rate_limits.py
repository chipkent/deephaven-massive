"""Tests for the library side of the REST rate limits: shared governors, warnings, rows, notices."""

from __future__ import annotations

import contextlib
import functools
import logging
import threading
from typing import TYPE_CHECKING

import pytest
from deephaven.update_graph import exclusive_lock

from deephaven_massive import _massive
from deephaven_massive._enums import Subscription
from deephaven_massive._ingest import rate_limits
from deephaven_massive._ingest.rate_limits import (
    BUCKETS,
    CONSOLE_PATIENCE,
    LEARN_ONLY,
    LIVE_TABLE_ADVICE,
    OTHER,
    READ_PATIENCE,
    GovernorRegistry,
    PaceCheck,
    RateLimitNotices,
    ReadShape,
    Waiter,
    announce,
    bucket_of,
    console_advice,
    rate_limit_rows,
    read_patience,
)
from deephaven_massive._tables.diagnostics import LimitSource, RateLimitState
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext

LOGGER = rate_limits.__name__
URL = "https://api.massive.com"
PER_MINUTE = "You've exceeded the maximum requests per minute"


def _registry(clock: FakeClock) -> GovernorRegistry:
    return GovernorRegistry(
        functools.partial(_massive.RequestGovernor, BUCKETS, learn_only=LEARN_ONLY, clock=clock)
    )


def _lease(
    registry: GovernorRegistry,
    *,
    api_key: str = "key-1",
    base_url: str = URL,
    limits: dict[Subscription, int] | None = None,
    label: str = "MassiveSession #1",
    listener: object = None,
    on_pause: Callable[[_massive.PauseNotice], None] | None = None,
) -> rate_limits.GovernorLease:
    return registry.lease(
        api_key=api_key,
        base_url=base_url,
        limits=limits or {},
        label=label,
        listener=listener if callable(listener) else (lambda: None),
        on_pause=on_pause or _ignore_pause,
    )


def _ignore_pause(notice: _massive.PauseNotice) -> None:
    del notice


def _paced(governor: _massive.RequestGovernor) -> dict[str, int | None]:
    return {state.bucket: state.limit for state in governor.snapshot()}


def _refuse(
    governor: _massive.RequestGovernor, bucket: str, message: str, *, retry_after: float
) -> None:
    permit = governor.admission(bucket, lane=object()).acquire(_massive.Cancel())
    with contextlib.suppress(_massive.WireError):  # a pause past the patience gives up
        governor.refused(permit, retry_after=retry_after, message=message)


# --- Buckets -------------------------------------------------------------------------------------


def test_one_bucket_per_subscription_then_other() -> None:
    assert BUCKETS == ("stocks", "options", "indices", "currencies", "futures", "other")
    assert bucket_of(Subscription.CURRENCIES) == "currencies"
    assert bucket_of(None) == OTHER == "other"


# --- Sharing -------------------------------------------------------------------------------------


def test_sessions_of_one_key_and_origin_share_a_governor() -> None:
    registry = _registry(FakeClock())
    one = _lease(registry)
    same = _lease(registry, base_url="HTTPS://API.massive.com:443/", label="MassiveSession #2")
    other_key = _lease(registry, api_key="key-2")
    other_origin = _lease(registry, base_url="https://api.massive.com:8443")
    assert one.governor is same.governor
    assert one.governor is not other_key.governor
    assert one.governor is not other_origin.governor
    assert other_key.governor is not other_origin.governor
    assert len(registry) == 3
    assert one.governor.buckets == BUCKETS


def test_the_registry_keeps_a_digest_never_the_key() -> None:
    registry = _registry(FakeClock())
    _lease(registry, api_key="secret-key-123")
    [(digest, origin)] = list(registry._entries)
    assert digest == "dc87f94e8f44b5018e54a588eebeaae61eebdb6c256f8f2f61b7c6ba347bca63"  # SHA-256
    assert origin == ("https", "api.massive.com", 443)
    assert "secret-key-123" not in repr(registry._entries)


def test_the_stricter_declaration_wins_while_its_session_holds_the_governor(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = _registry(FakeClock())
    with caplog.at_level(logging.WARNING, LOGGER):
        first = _lease(registry, limits={Subscription.STOCKS: 5, Subscription.OPTIONS: 8})
        quiet = _lease(registry, label="MassiveSession #2")  # declares nothing: no warning
        second = _lease(registry, limits={Subscription.OPTIONS: 3}, label="MassiveSession #3")
    governor = first.governor
    assert _paced(governor) == {
        "stocks": 5,
        "options": 3,
        "indices": None,
        "currencies": None,
        "futures": None,
        "other": None,
    }
    assert caplog.messages == [
        (
            "This session declares rest_requests_per_minute {options: 3}, and another session "
            "that uses the same API key declares {options: 8, stocks: 5}. Massive counts their "
            "requests together, so both pace {options: 3, stocks: 5}, the lowest limit declared "
            "for each subscription."
        )
    ]
    first.release()
    assert _paced(governor)["stocks"] is None
    assert _paced(governor)["options"] == 3
    second.release()
    second.release()  # idempotent
    quiet.release()
    assert set(_paced(governor).values()) == {None}


def test_the_same_declaration_twice_is_no_conflict(caplog: pytest.LogCaptureFixture) -> None:
    registry = _registry(FakeClock())
    limits = {Subscription.FUTURES: 5}
    with caplog.at_level(logging.WARNING, LOGGER):
        _lease(registry, limits=limits)
        _lease(registry, limits=limits, label="MassiveSession #2")
    assert caplog.messages == []


def test_an_idle_entry_outlives_its_last_lease_until_its_window_passes() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    lease = _lease(registry, limits={Subscription.STOCKS: 5})
    governor = lease.governor
    governor.done(
        governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
    )
    lease.release()
    # A new session soon after (a notebook cell run again) still sees the slot held.
    again = _lease(registry, limits={Subscription.STOCKS: 5})
    assert again.governor is governor
    again.release()
    clock.advance(61.0)
    fresh = _lease(registry, api_key="key-2")  # any lease prunes idle entries first
    assert len(registry) == 1
    assert fresh.governor is not governor


def test_a_lease_listener_hears_changes_until_the_lease_is_released() -> None:
    registry = _registry(FakeClock())
    heard: list[int] = []
    lease = _lease(registry, listener=lambda: heard.append(1))
    governor = lease.governor
    governor.declare({})
    assert heard == [1]
    lease.release()
    governor.declare({})
    assert heard == [1]


def test_a_lease_hears_each_pause_from_the_one_open_until_the_lease_is_released() -> None:
    # A session that takes its lease mid-pause hears of that pause at once, then of its end,
    # and of nothing once it has released the lease.
    clock = FakeClock()
    registry = _registry(clock)
    first = _lease(registry, label="MassiveSession #1")
    governor = first.governor
    permit = governor.admission("stocks", lane=object()).acquire(_massive.Cancel())
    governor.refused(permit, retry_after=20.0, message="HTTP 429")
    heard: list[tuple[str, float, float, bool]] = []
    second = _lease(
        registry,
        label="MassiveSession #2",
        on_pause=lambda n: heard.append((n.bucket, n.started, n.ends, n.ended)),
    )
    start = clock.time()
    assert heard == [("stocks", start, start + 20.0, False)]
    clock.advance(20.0)
    governor.snapshot()  # finds the pause over
    assert heard[1:] == [("stocks", start, start + 20.0, True)]
    second.release()
    permit = governor.admission("stocks", lane=object()).acquire(_massive.Cancel())
    governor.refused(permit, retry_after=5.0, message="HTTP 429")
    assert len(heard) == 2
    first.release()


def test_concurrent_leases_share_one_governor() -> None:
    registry = _registry(FakeClock())
    leases: list[rate_limits.GovernorLease] = []
    lock = threading.Lock()

    def take() -> None:
        lease = _lease(registry)
        with lock:
            leases.append(lease)

    threads = [threading.Thread(target=take) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert len({id(lease.governor) for lease in leases}) == 1
    assert len(registry) == 1
    assert registry.leases() == 16


def test_the_registry_counts_the_leases_not_yet_released() -> None:
    registry = _registry(FakeClock())
    one = _lease(registry)
    other = _lease(registry, api_key="key-2")
    assert registry.leases() == 2
    one.release()
    one.release()  # idempotent: counted once
    assert registry.leases() == 1
    other.release()
    assert registry.leases() == 0


# --- Refusal notices -----------------------------------------------------------------------------


def _error(message: str) -> _massive.WireError:
    return _massive.WireError(
        "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429 (request_id r1): "
        f'"{message}".',
        kind=_massive.WireFailure.REFUSED,
        request_id="r1",
    )


def test_a_per_minute_refusal_is_explained_once_per_session(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitNotices("MassiveSession #1", {})
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused("currencies", _error(PER_MINUTE), per_minute=True)
        warnings.refused("currencies", _error(PER_MINUTE), per_minute=True)
    # Requests pause and resume by themselves: an INFO notice, not a WARNING.
    assert [r.levelno for r in caplog.records] == [logging.INFO]
    assert caplog.messages == [
        (
            "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429 (request_id "
            f'r1): "{PER_MINUTE}". Massive limits currencies requests per minute. Once the pause '
            "ends, currencies requests are paced at the number of them Massive answered of those "
            "sent in the refusal's clock minute, if it answered any. The next log line and "
            "m.rate_limits() give it. To pace them from the first request, pass "
            "rest_requests_per_minute={dm.Subscription.CURRENCIES: n} to dm.MassiveConfig, with "
            "the limit n Massive gives you."
        )
    ]


def test_a_declared_subscription_refused_points_at_other_users_of_the_account(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitNotices("MassiveSession #2", {Subscription.STOCKS: 5})
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused("stocks", _error(PER_MINUTE), per_minute=True)
        warnings.refused("options", _error("busy"), per_minute=False)  # another bucket: its own
    assert caplog.messages == [
        (
            "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429 (request_id "
            f'r1): "{PER_MINUTE}". Stocks requests are already paced at the 5 per minute '
            "declared, so either other sessions, processes, or API keys of the account share "
            "the limit, or 5 is above Massive's limit. They pause and resume by themselves."
        ),
        (
            "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429 (request_id "
            'r1): "busy". Options requests pause and resume by themselves. If this persists, '
            "send request_id r1 to Massive's support."
        ),
    ]


def test_a_refusal_without_a_request_id_names_none(caplog: pytest.LogCaptureFixture) -> None:
    warnings = RateLimitNotices("MassiveSession #1", {})
    answer = 'Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429: "busy".'
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused(
            "stocks",
            _massive.WireError(answer, kind=_massive.WireFailure.REFUSED),
            per_minute=False,
        )
    assert caplog.messages == [
        (
            f"{answer} Stocks requests pause and resume by themselves. If this persists, tell "
            "Massive's support."
        )
    ]


def test_a_refusal_outside_the_subscriptions_names_no_subscription(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitNotices("MassiveSession #1", {})
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused(OTHER, _error(PER_MINUTE), per_minute=True)
    # Massive counts reference data on its own: the bucket learns its limit, but no setting
    # declares it, so the notice offers none.
    assert caplog.messages == [
        (
            "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 429 (request_id "
            f'r1): "{PER_MINUTE}". That is Massive\'s limit on reference and other data outside '
            "the subscriptions, which it counts on their own. Once the pause ends, requests "
            "outside Massive's subscriptions are paced at the number of them Massive answered of "
            "those sent in the refusal's clock minute, if it answered any. The next log line and "
            "m.rate_limits() give it."
        )
    ]


# --- Rows ----------------------------------------------------------------------------------------


def test_rows_show_each_buckets_state() -> None:
    clock = FakeClock(manual=True, wall=1_800_000_000.0)
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=clock)
    governor.declare({"stocks": 5})
    for _ in range(5):
        governor.done(
            governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
        )
    waiting = threading.Thread(
        target=governor.admission("stocks", lane=object()).acquire,
        args=(_massive.Cancel(),),
        daemon=True,
    )
    waiting.start()
    clock.wait_for_sleepers(1)
    _refuse(governor, "options", PER_MINUTE, retry_after=30.0)
    rows = {row.bucket: row for row in rate_limit_rows(governor)}
    assert list(rows) == list(BUCKETS)
    stocks, options = rows["stocks"], rows["options"]
    assert (stocks.state, stocks.requests_per_minute, stocks.limit_source) == (
        RateLimitState.PACED,
        5,
        LimitSource.DECLARED,
    )
    assert (stocks.waiting, stocks.recent_requests, stocks.attempts) == (1, 5, 5)
    # Paused, and learning nothing until the pause ends.
    assert (options.state, options.limit_source) == (RateLimitState.PAUSED, None)
    assert options.paused_until == 1_800_000_030 * 10**9
    assert (options.refusals, options.last_refusal) == (1, PER_MINUTE)
    assert (rows["indices"].state, rows["indices"].limit_source) == (RateLimitState.READY, None)
    assert rows["indices"].paused_until is None
    # Stamped on the governor's clock, the one `PausedUntil` is read on.
    assert rows["currencies"].updated_at == round(clock.time() * 1e9)
    clock.advance(61.0)
    waiting.join(10)
    assert not waiting.is_alive()


@pytest.mark.parametrize("asked", [1e10, 1e12, float("inf")])
def test_a_huge_retry_after_leaves_paused_until_a_valid_timestamp(asked: float) -> None:
    # A Retry-After of 31,700 years, or of more digits than a float holds, would put the pause
    # end past what a nanosecond timestamp can hold; the ten-minute cap keeps it a real time.
    clock = FakeClock(manual=True, wall=1_800_000_000.0)
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=clock)
    _refuse(governor, "options", "HTTP 429: slow down", retry_after=asked)
    rows = {row.bucket: row for row in rate_limit_rows(governor)}
    assert rows["options"].state is RateLimitState.PAUSED
    assert rows["options"].paused_until == (1_800_000_000 + 600) * 10**9
    assert rows["options"].paused_until < 2**63
    assert governor.next_change() == 600.0


# --- Notices -------------------------------------------------------------------------------------


def test_a_paced_read_announces_its_least_duration(caplog: pytest.LogCaptureFixture) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    governor.declare({"stocks": 5})
    buckets = ["stocks"] * 250 + ["options"] * 3 + ["other"] * 40
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.trades fetch", buckets)
        announce(governor, "stocks.trades fetch", ["stocks"] * 5)  # all start at once: quiet
    assert caplog.messages == [
        (
            "stocks.trades fetch: 250 requests, paced at 5 requests a minute for stocks. At that "
            "pace they take at least 49 min."
        )
    ]


def test_a_paced_read_holding_the_update_graph_lock_warns_that_it_pauses_the_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # In a console command, which holds the update graph's lock, a paced read holds every live
    # table of the server still while it runs: the least duration becomes a WARNING.
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    governor.declare({"stocks": 5})
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.bars history", ["stocks"] * 250, pauses_live_tables=True)
        quick = ["stocks"] * 5  # every request starts at once: quiet
        announce(governor, "stocks.bars history", quick, pauses_live_tables=True)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            (
                "stocks.bars history: 250 requests, paced at 5 requests a minute for stocks. This "
                "command will pause every live table on the server for at least 49 min. Leave "
                "out wait=True to read in the background."
            ),
        )
    ]


def test_a_read_of_an_unpaced_subscription_announces_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Its limit, if any, is unknown until Massive refuses one of its requests.
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.bars history", ["stocks"] * 11, shape=ReadShape.PER_DAY)
        announce(governor, "economy.yields history", ["other"] * 40, shape=ReadShape.PER_DAY)
        announce(governor, "stocks.trades history", ["stocks"] * 40)
    assert caplog.messages == []


def test_a_learned_limit_shows_as_learned_and_paces_the_announcement(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    for _ in range(3):
        governor.done(
            governor.admission("options", lane=object()).acquire(_massive.Cancel()), answered=True
        )
    _refuse(governor, "options", PER_MINUTE, retry_after=70.0)
    governor.done(
        governor.admission("options", lane=object()).acquire(_massive.Cancel()), answered=True
    )
    options = {row.bucket: row for row in rate_limit_rows(governor)}["options"]
    assert (options.requests_per_minute, options.limit_source) == (3, LimitSource.LEARNED)
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "options.bars history", ["options"] * 7, shape=ReadShape.PER_DAY)
    assert caplog.messages == [
        (
            "options.bars history: 7 requests, one per day, paced at 3 requests a minute for "
            "options. At that pace they take at least 2 min."
        )
    ]


def test_a_paced_daily_download_states_its_pace(caplog: pytest.LogCaptureFixture) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    governor.declare({"stocks": 5})
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.bars history", ["stocks"] * 11, shape=ReadShape.PER_DAY)
    assert caplog.messages == [
        (
            "stocks.bars history: 11 requests, one per day, paced at 5 requests a minute for "
            "stocks. At that pace they take at least 2 min."
        )
    ]


def test_a_poll_whose_cycles_outlast_their_interval_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    check = PaceCheck("stocks.snapshots poll()", 30.0)
    with caplog.at_level(logging.WARNING, LOGGER):
        check.check(governor, {"stocks": 4, "other": 50})  # unpaced: fine
        governor.declare({"stocks": 5})
        check.check(governor, {"stocks": 2})  # 24 s of a 30 s interval: fine
        check.check(governor, {"other": 50, "stocks": 3})  # 36 s: too slow
        check.check(governor, {"stocks": 10})  # warned already
    assert caplog.messages == [
        (
            "stocks.snapshots poll(): each cycle makes 3 stocks requests, but they are paced at 5 "
            "per minute, so a cycle takes at least 37 s, longer than the 30 s interval, and cycles "
            "run late. To keep them on time, call poll(37) or a longer interval."
        )
    ]
    # The advice works: a poll at the interval the warning names keeps its cycles on time.
    caplog.clear()
    with caplog.at_level(logging.WARNING, LOGGER):
        PaceCheck("stocks.snapshots poll()", 37.0).check(governor, {"other": 50, "stocks": 3})
    assert caplog.messages == []


def test_a_paced_bucket_with_every_slot_held_shows_paced() -> None:
    clock = FakeClock()
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=clock)
    governor.declare({"stocks": 5})
    for _ in range(5):
        governor.done(
            governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
        )
    rows, change = rate_limits.rate_limit_pull(governor)
    stocks = next(row for row in rows if row.bucket == "stocks")
    # Nobody waits, but the next request would: it is not READY.
    assert (stocks.state, stocks.waiting, stocks.recent_requests) == (RateLimitState.PACED, 0, 5)
    assert change == 61.0  # the rows change when the first slot frees


def test_a_learned_limit_goes_with_its_governor() -> None:
    # Once the last session of the key has gone and its governor is idle, what it learned is
    # forgotten: a new governor starts unpaced and relearns from its first refusal.
    clock = FakeClock()
    registry = _registry(clock)
    lease = _lease(registry)
    governor = lease.governor
    for _ in range(5):
        governor.done(
            governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
        )
    _refuse(governor, "stocks", PER_MINUTE, retry_after=70.0)
    governor.done(
        governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
    )
    assert _paced(governor)["stocks"] == 5
    lease.release()
    clock.advance(61.0)
    _lease(registry, api_key="key-2").release()  # prunes the idle first governor
    again = _lease(registry)
    assert again.governor is not governor
    assert _paced(again.governor)["stocks"] is None
    assert [s.bucket for s in again.governor.snapshot() if s.learned is not None] == []


def test_the_default_registry_names_the_other_buckets_requests() -> None:
    lease = _lease(GovernorRegistry())
    governor = lease.governor
    permit = governor.admission(OTHER, lane=object()).acquire(_massive.Cancel())
    governor.refused(permit, retry_after=700.0, message="busy")
    # No setting declares the other bucket's limit, so the give-up names no setting to pace it.
    with pytest.raises(
        _massive.WireError,
        match=r"^busy The next request goes at .* Retry after that time, or stop the other "
        r"program\.$",
    ):
        governor.admission(OTHER, lane=object(), patience=_massive.Patience(0.0)).acquire(
            _massive.Cancel()
        )
    # The other bucket cannot be declared; it learns its limit from Massive's refusals.
    with pytest.raises(
        ValueError, match=r"^The buckets \['other'\] cannot take a declared limit\. "
    ):
        governor.declare({OTHER: 5})
    lease.release()


CONSOLE_ADVICE = "Leave out wait=True to read in the background."
"""The give-up advice of a `fetch()` or `history()` call in a console command."""


@pytest.mark.parametrize(
    ("waiter", "free", "console"),
    [
        (Waiter.CALL, (READ_PATIENCE, "", ""), (CONSOLE_PATIENCE, CONSOLE_ADVICE, "")),
        (Waiter.STREAM_OPEN, (READ_PATIENCE, "", ""), (CONSOLE_PATIENCE, "", "")),
        (
            Waiter.LIVE_TABLE,
            (300.0, LIVE_TABLE_ADVICE, "max_outage=300 s"),
            (CONSOLE_PATIENCE, "", ""),
        ),
    ],
)
def test_each_waiter_gets_its_patience_and_advice_with_and_without_the_lock(
    dh_exec_ctx: ExecutionContext,
    *,
    waiter: Waiter,
    free: tuple[float, str, str],
    console: tuple[float, str, str],
) -> None:
    # A caller waits ten minutes, a live table max_outage (300 s here). A console command holds
    # the update graph's exclusive lock, so every read gives up after 90 s; only a fetch() or
    # history() call takes wait=True, so only its error advises leaving it out. Only the live
    # table's limit is a setting of the user's, which its error names.
    patience = read_patience(dh_exec_ctx, max_outage=300.0, waiter=waiter)
    assert (patience.seconds, patience.advice, patience.limit) == free
    with exclusive_lock(dh_exec_ctx.update_graph):
        held = read_patience(dh_exec_ctx, max_outage=300.0, waiter=waiter)
    assert (held.seconds, held.advice, held.limit) == console


def test_a_waiter_no_case_names_is_refused(dh_exec_ctx: ExecutionContext) -> None:
    with pytest.raises(AssertionError):
        read_patience(
            dh_exec_ctx,
            max_outage=300.0,
            waiter="someone",  # type: ignore[arg-type]  # the point: a waiter no case names is refused
        )


def test_the_patience_constants_and_advice_read_as_documented() -> None:
    assert (READ_PATIENCE, CONSOLE_PATIENCE) == (600.0, 90.0)
    assert LIVE_TABLE_ADVICE == "Pass a larger max_outage to dm.MassiveConfig to wait longer."
    assert console_advice(300.0) == CONSOLE_ADVICE


def test_a_read_that_feeds_a_live_table_fails_at_its_first_pause_with_no_max_outage(
    dh_exec_ctx: ExecutionContext,
) -> None:
    assert read_patience(dh_exec_ctx, max_outage=0.0, waiter=Waiter.LIVE_TABLE).seconds == 0.0


@pytest.mark.parametrize("max_outage", [0.0, 90.0])
def test_the_console_advises_no_background_read_that_would_wait_no_longer(
    dh_exec_ctx: ExecutionContext, max_outage: float
) -> None:
    assert console_advice(max_outage) == ""
    with exclusive_lock(dh_exec_ctx.update_graph):
        console = read_patience(dh_exec_ctx, max_outage=max_outage, waiter=Waiter.CALL)
    assert (console.seconds, console.advice) == (CONSOLE_PATIENCE, "")


def test_each_read_gets_a_patience_of_its_own(dh_exec_ctx: ExecutionContext) -> None:
    first = read_patience(dh_exec_ctx, max_outage=300.0, waiter=Waiter.CALL)
    assert first is not read_patience(dh_exec_ctx, max_outage=300.0, waiter=Waiter.CALL)


def test_the_pace_warning_counts_each_slot_for_61_seconds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    governor.declare({"stocks": 5})
    with caplog.at_level(logging.WARNING, LOGGER):
        PaceCheck("fits", 61.0).check(governor, {"stocks": 5})  # 5 per 61 s: just fits
        PaceCheck("late", 60.0).check(governor, {"stocks": 5})  # a minute is too short
    assert [record.getMessage().split(":")[0] for record in caplog.records] == ["late"]


def test_every_shape_but_paged_says_how_it_counts() -> None:
    # A paged read's notice adds nothing; a new shape must say how it counts, never say nothing.
    assert set(rate_limits._SHAPE_WORDS) == set(ReadShape) - {ReadShape.PAGED}
