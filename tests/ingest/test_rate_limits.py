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
    BACKGROUND_ADVICE,
    BUCKETS,
    CONSOLE_PATIENCE,
    LEARN_ONLY,
    OTHER,
    READ_PATIENCE,
    GovernorRegistry,
    PaceCheck,
    RateLimitNotices,
    ReadShape,
    announce,
    bucket_of,
    console_advice,
    rate_limit_rows,
    read_patience,
)
from deephaven_massive._tables.diagnostics import LimitSource, RateLimitState
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
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
) -> rate_limits.GovernorLease:
    return registry.lease(
        api_key=api_key,
        base_url=base_url,
        limits=limits or {},
        label=label,
        listener=listener if callable(listener) else (lambda: None),
    )


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
            "MassiveSession #3 declares rest_requests_per_minute {options: 3}, but "
            "MassiveSession #1, which uses the same API key, declares {options: 8, stocks: 5}; "
            "Massive counts their requests together, so both pace {options: 3, stocks: 5}, the "
            "lowest limit declared for each subscription"
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
        f"HTTP 429 from GET /v3/trades/AAPL (request_id r1): {message}",
        kind=_massive.WireFailure.REFUSED,
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
            "MassiveSession #1: Massive refused a currencies request with HTTP 429 (HTTP 429 "
            f"from GET /v3/trades/AAPL (request_id r1): {PER_MINUTE}); Massive limits these "
            "requests per minute; once the pause ends, they are paced at the number of them "
            "Massive answered of those sent in the refusal's clock minute, if it answered any "
            "(the next log line and m.rate_limits() give it); declare "
            "MassiveConfig(rest_requests_per_minute={dm.Subscription.CURRENCIES: <limit>}) to "
            "pace them from the first request, with the limit Massive gives you"
        )
    ]


def test_a_declared_subscription_refused_points_at_other_users_of_the_account(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitNotices("MassiveSession #2", {Subscription.STOCKS: 5})
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused("stocks", _error(PER_MINUTE), per_minute=True)
        warnings.refused("options", _error("busy"), per_minute=False)  # another bucket: its own
    assert caplog.messages[0].endswith(
        "its requests are already paced at the 5 per minute declared, so either other sessions, "
        "processes, or API keys of the account share the limit, or 5 is above Massive's limit; "
        "requests pause and resume"
    )
    assert caplog.messages[1] == (
        "MassiveSession #2: Massive refused an options request with HTTP 429 (HTTP 429 from GET "
        "/v3/trades/AAPL (request_id r1): busy); requests pause and resume; if this persists, "
        "send the request id to Massive's support"
    )


def test_a_refusal_outside_the_subscriptions_names_no_subscription(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitNotices("MassiveSession #1", {})
    with caplog.at_level(logging.INFO, LOGGER):
        warnings.refused(OTHER, _error(PER_MINUTE), per_minute=True)
    assert caplog.messages[0].startswith(
        "MassiveSession #1: Massive refused a request outside Massive's subscriptions with "
    )
    # Massive counts reference data on its own: the bucket learns its limit, but no setting
    # declares it, so the notice offers none.
    assert caplog.messages[0].endswith(
        "that is Massive's limit on reference and other data outside the subscriptions, which "
        "it counts on their own; once the pause ends, they are paced at the number of them "
        "Massive answered of those sent in the refusal's clock minute, if it answered any (the "
        "next log line and m.rate_limits() give it)"
    )
    assert "MassiveConfig" not in caplog.messages[0]


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
            "stocks.trades fetch: 250 requests; paced at 5 requests a minute (stocks); at that "
            "pace they take at least 49 min"
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
                "stocks.bars history: 250 requests; paced at 5 requests a minute (stocks); this "
                "command will pause every live table on the server for at least 49 min: leave "
                "out wait=True to read in the background"
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
            "options.bars history: 7 requests, one per day; paced at 3 requests a minute "
            "(options); at that pace they take at least 2 min"
        )
    ]


def test_a_paced_daily_download_states_its_pace(caplog: pytest.LogCaptureFixture) -> None:
    governor = _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, clock=FakeClock())
    governor.declare({"stocks": 5})
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.bars history", ["stocks"] * 11, shape=ReadShape.PER_DAY)
    assert caplog.messages == [
        (
            "stocks.bars history: 11 requests, one per day; paced at 5 requests a minute (stocks); "
            "at that pace they take at least 2 min"
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
            "per minute, so a cycle takes at least 37 s, longer than the 30 s interval; cycles run "
            "late"
        )
    ]


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
    with pytest.raises(_massive.WireError, match="refusing requests outside Massive's"):
        governor.admission(OTHER, lane=object(), patience=_massive.Patience(0.0)).acquire(
            _massive.Cancel()
        )
    # The other bucket cannot be declared; it learns its limit from Massive's refusals.
    with pytest.raises(ValueError, match=r"\['other'\] cannot be declared"):
        governor.declare({OTHER: 5})
    lease.release()


def test_a_waited_read_waits_out_ten_minutes_of_pauses_or_ninety_seconds_under_the_lock(
    dh_exec_ctx: ExecutionContext,
) -> None:
    patience = read_patience(dh_exec_ctx, max_outage=300.0, background=False)
    assert (patience.seconds, patience.advice) == (READ_PATIENCE, "")
    assert READ_PATIENCE == 600.0
    # A console command holds the update graph's exclusive lock: the read gives up sooner, and
    # its give-up error says how to read without the lock.
    with exclusive_lock(dh_exec_ctx.update_graph):
        console = read_patience(dh_exec_ctx, max_outage=300.0, background=False)
    assert console.seconds == CONSOLE_PATIENCE == 90.0
    assert console.advice == (
        "this read ran inside a Deephaven console command, which waits out only 90 s of "
        "pauses; leave out wait=True to let fetch() or history() read in the background, "
        "which waits out MassiveConfig.max_outage (300 s)"
    )


def test_a_read_no_caller_waits_for_waits_out_max_outage(dh_exec_ctx: ExecutionContext) -> None:
    patience = read_patience(dh_exec_ctx, max_outage=300.0, background=True)
    assert (patience.seconds, patience.advice) == (300.0, BACKGROUND_ADVICE)
    assert BACKGROUND_ADVICE == (
        "raise MassiveConfig.max_outage to let a background read wait longer"
    )
    assert read_patience(dh_exec_ctx, max_outage=0.0, background=True).seconds == 0.0


@pytest.mark.parametrize("max_outage", [0.0, 90.0])
def test_the_console_advises_no_background_read_that_would_wait_no_longer(
    dh_exec_ctx: ExecutionContext, max_outage: float
) -> None:
    assert console_advice(max_outage) == ""
    with exclusive_lock(dh_exec_ctx.update_graph):
        console = read_patience(dh_exec_ctx, max_outage=max_outage, background=False)
    assert (console.seconds, console.advice) == (CONSOLE_PATIENCE, "")


def test_each_read_gets_a_patience_of_its_own(dh_exec_ctx: ExecutionContext) -> None:
    first = read_patience(dh_exec_ctx, max_outage=300.0, background=False)
    assert first is not read_patience(dh_exec_ctx, max_outage=300.0, background=False)


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
