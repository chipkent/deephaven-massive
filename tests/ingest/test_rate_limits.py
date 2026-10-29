"""Tests for the library side of the REST rate limits: shared governors, warnings, rows, notices."""

from __future__ import annotations

import contextlib
import functools
import logging
import threading

import pytest

from deephaven_massive import _massive
from deephaven_massive._enums import Subscription
from deephaven_massive._ingest import rate_limits
from deephaven_massive._ingest.rate_limits import (
    BUCKETS,
    OTHER,
    UNPACED,
    GovernorRegistry,
    PaceCheck,
    RateLimitWarnings,
    ReadShape,
    announce,
    bucket_of,
    rate_limit_rows,
)
from deephaven_massive._tables.diagnostics import LimitSource, RateLimitState
from tests._fakes.clock import FakeClock

LOGGER = rate_limits.__name__
URL = "https://api.massive.com"
BASIC = "You've exceeded the maximum requests per minute"


def _registry(clock: FakeClock) -> GovernorRegistry:
    return GovernorRegistry(
        functools.partial(_massive.RequestGovernor, BUCKETS, unpaced=UNPACED, clock=clock)
    )


def _lease(
    registry: GovernorRegistry,
    *,
    api_key: str = "key-1",
    base_url: str = URL,
    basic: frozenset[Subscription] = frozenset(),
    label: str = "MassiveSession #1",
    listener: object = None,
) -> rate_limits.GovernorLease:
    return registry.lease(
        api_key=api_key,
        base_url=base_url,
        basic=basic,
        label=label,
        listener=listener if callable(listener) else (lambda: None),
    )


def _paced(governor: _massive.RequestGovernor) -> dict[str, int | None]:
    return {state.bucket: state.limit for state in governor.snapshot()}


def _refuse(
    governor: _massive.RequestGovernor, bucket: str, message: str, *, retry_after: float
) -> None:
    permit = governor.acquire(bucket, _massive.Cancel(), lane=object())
    with contextlib.suppress(_massive.WireError):  # a pause past the patience gives up
        governor.refused(permit, retry_after=retry_after, message=message, request_id="r1")


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
        first = _lease(registry, basic=frozenset({Subscription.STOCKS}))
        quiet = _lease(registry, label="MassiveSession #2")  # declares nothing: no warning
        second = _lease(
            registry, basic=frozenset({Subscription.OPTIONS}), label="MassiveSession #3"
        )
    governor = first.governor
    assert _paced(governor) == {
        "stocks": 5,
        "options": 5,
        "indices": None,
        "currencies": None,
        "futures": None,
        "other": None,
    }
    assert caplog.messages == [
        (
            "MassiveSession #3 declares rest_basic_subscriptions {options}, but MassiveSession #1, "
            "which uses the same API key, declares {stocks}; Massive counts their requests "
            "together, so both pace {options, stocks}"
        )
    ]
    first.release()
    assert _paced(governor)["stocks"] is None
    assert _paced(governor)["options"] == 5
    second.release()
    second.release()  # idempotent
    quiet.release()
    assert set(_paced(governor).values()) == {None}


def test_the_same_declaration_twice_is_no_conflict(caplog: pytest.LogCaptureFixture) -> None:
    registry = _registry(FakeClock())
    basic = frozenset({Subscription.FUTURES})
    with caplog.at_level(logging.WARNING, LOGGER):
        _lease(registry, basic=basic)
        _lease(registry, basic=basic, label="MassiveSession #2")
    assert caplog.messages == []


def test_an_idle_entry_outlives_its_last_lease_until_its_window_passes() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    lease = _lease(registry, basic=frozenset({Subscription.STOCKS}))
    governor = lease.governor
    governor.done(governor.acquire("stocks", _massive.Cancel(), lane=object()), answered=True)
    lease.release()
    # A new session soon after (a notebook cell run again) still sees the slot held.
    again = _lease(registry, basic=frozenset({Subscription.STOCKS}))
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
    governor.declare(set())
    assert heard == [1]
    lease.release()
    governor.declare(set())
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


# --- Warnings ------------------------------------------------------------------------------------


def _error(message: str) -> _massive.WireError:
    return _massive.WireError(f"HTTP 429 from GET /v3/trades/AAPL (request_id r1): {message}")


def test_a_learned_basic_subscription_is_explained_once_per_session(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitWarnings("MassiveSession #1", frozenset())
    with caplog.at_level(logging.WARNING, LOGGER):
        warnings.refused("currencies", _error(BASIC), basic=True)
        warnings.refused("currencies", _error(BASIC), basic=True)
    assert caplog.messages == [
        (
            "MassiveSession #1: Massive refused a currencies request with HTTP 429 (HTTP 429 "
            f"from GET /v3/trades/AAPL (request_id r1): {BASIC}); that is the Basic tier's "
            "limit of 5 requests per minute, so these requests are paced at that rate from now "
            "on; declare MassiveConfig(rest_basic_subscriptions={dm.Subscription.CURRENCIES}) "
            "to pace them from the first request"
        )
    ]


def test_a_declared_subscription_refused_points_at_other_users_of_the_account(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitWarnings("MassiveSession #2", frozenset({Subscription.STOCKS}))
    with caplog.at_level(logging.WARNING, LOGGER):
        warnings.refused("stocks", _error(BASIC), basic=True)
        warnings.refused("options", _error("busy"), basic=False)  # another bucket: its own
    assert caplog.messages[0].endswith(
        "its requests are already paced at 5 per minute, so other sessions, processes, or API "
        "keys of the account are using the same limit; requests pause and resume"
    )
    assert caplog.messages[1] == (
        "MassiveSession #2: Massive refused an options request with HTTP 429 (HTTP 429 from GET "
        "/v3/trades/AAPL (request_id r1): busy); requests pause and resume; a paid "
        "subscription has no request limit, so if this persists, send the request id to "
        "Massive's support"
    )


def test_a_refusal_outside_the_subscriptions_names_no_subscription(
    caplog: pytest.LogCaptureFixture,
) -> None:
    warnings = RateLimitWarnings("MassiveSession #1", frozenset())
    with caplog.at_level(logging.WARNING, LOGGER):
        warnings.refused(OTHER, _error(BASIC), basic=True)
    assert caplog.messages[0].startswith(
        "MassiveSession #1: Massive refused a request outside Massive's subscriptions with "
    )
    # Data of no subscription belongs to no tier: the refusal pauses it but never paces it.
    assert caplog.messages[0].endswith(
        "requests outside Massive's subscriptions belong to no tier, so they pause and resume "
        "and are not paced"
    )
    assert "MassiveConfig" not in caplog.messages[0]


# --- Rows ----------------------------------------------------------------------------------------


def test_rows_show_each_buckets_state() -> None:
    clock = FakeClock(manual=True, wall=1_800_000_000.0)
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=clock)
    governor.declare({"stocks"})
    for _ in range(5):
        governor.done(governor.acquire("stocks", _massive.Cancel(), lane=object()), answered=True)
    waiting = threading.Thread(
        target=governor.acquire,
        args=("stocks", _massive.Cancel()),
        kwargs={"lane": object()},
        daemon=True,
    )
    waiting.start()
    clock.wait_for_sleepers(1)
    _refuse(governor, "options", BASIC, retry_after=30.0)
    _refuse(governor, "futures", "HTTP 429: busy", retry_after=700.0)
    rows = {row.bucket: row for row in rate_limit_rows(governor)}
    assert list(rows) == list(BUCKETS)
    stocks, options, futures = rows["stocks"], rows["options"], rows["futures"]
    assert (stocks.state, stocks.requests_per_minute, stocks.limit_source) == (
        RateLimitState.PACED,
        5,
        LimitSource.DECLARED,
    )
    assert (stocks.waiting, stocks.recent_requests, stocks.attempts) == (1, 5, 5)
    assert (options.state, options.limit_source) == (RateLimitState.PAUSED, LimitSource.LEARNED)
    assert options.paused_until == 1_800_000_030 * 10**9
    assert (options.refusals, options.last_refusal) == (1, BASIC)
    assert futures.state is RateLimitState.REFUSED
    assert (rows["indices"].state, rows["indices"].limit_source) == (RateLimitState.READY, None)
    assert rows["indices"].paused_until is None
    # Stamped on the governor's clock, the one `PausedUntil` is read on.
    assert rows["currencies"].updated_at == round(clock.time() * 1e9)
    clock.advance(61.0)
    waiting.join(10)
    assert not waiting.is_alive()


# --- Notices -------------------------------------------------------------------------------------


def test_a_paced_read_announces_its_least_duration(caplog: pytest.LogCaptureFixture) -> None:
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=FakeClock())
    governor.declare({"stocks"})
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


def test_a_daily_download_of_an_unpaced_subscription_names_the_basic_cost(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=FakeClock())
    with caplog.at_level(logging.INFO, LOGGER):
        announce(governor, "stocks.bars history", ["stocks"] * 11, shape=ReadShape.PER_DAY)
        announce(governor, "stocks.bars history", ["stocks"] * 5, shape=ReadShape.PER_DAY)
        announce(governor, "economy.yields history", ["other"] * 40, shape=ReadShape.PER_DAY)
        announce(governor, "stocks.trades history", ["stocks"] * 40)  # paged: quiet
    assert caplog.messages == [
        (
            "stocks.bars history: 11 requests, one per day; if the stocks subscription is on the "
            "Basic tier (5 requests a minute), they take at least 2 min"
        )
    ]


def test_a_paced_daily_download_states_its_pace(caplog: pytest.LogCaptureFixture) -> None:
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=FakeClock())
    governor.declare({"stocks"})
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
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=FakeClock())
    check = PaceCheck("stocks.snapshots poll()", 30.0)
    with caplog.at_level(logging.WARNING, LOGGER):
        check.check(governor, {"stocks": 4, "other": 50})  # unpaced: fine
        governor.declare({"stocks"})
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
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=clock)
    governor.declare({"stocks"})
    for _ in range(5):
        governor.done(governor.acquire("stocks", _massive.Cancel(), lane=object()), answered=True)
    rows, change = rate_limits.rate_limit_pull(governor)
    stocks = next(row for row in rows if row.bucket == "stocks")
    # Nobody waits, but the next request would: it is not READY.
    assert (stocks.state, stocks.waiting, stocks.recent_requests) == (RateLimitState.PACED, 0, 5)
    assert change == 61.0  # the rows change when the first slot frees


def test_a_learned_basic_outlives_its_pruned_governor() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    lease = _lease(registry)
    governor = lease.governor
    _refuse(governor, "stocks", BASIC, retry_after=5.0)
    lease.release()
    clock.advance(61.0)
    _lease(registry, api_key="key-2").release()  # prunes the idle first governor
    again = _lease(registry)
    assert again.governor is not governor
    assert _paced(again.governor)["stocks"] == 5
    assert [s.bucket for s in again.governor.snapshot() if s.learned] == ["stocks"]
    assert _paced(_lease(registry, api_key="key-3").governor)["stocks"] is None  # per key


def test_the_default_registry_names_the_other_buckets_requests() -> None:
    lease = _lease(GovernorRegistry())
    governor = lease.governor
    permit = governor.acquire(OTHER, _massive.Cancel(), lane=object())
    with pytest.raises(_massive.WireError, match="refused requests outside Massive's"):
        governor.refused(permit, retry_after=700.0, message="busy", request_id=None)
    lease.release()


def test_the_pace_warning_counts_each_slot_for_61_seconds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _massive.RequestGovernor(BUCKETS, unpaced=UNPACED, clock=FakeClock())
    governor.declare({"stocks"})
    with caplog.at_level(logging.WARNING, LOGGER):
        PaceCheck("fits", 61.0).check(governor, {"stocks": 5})  # 5 per 61 s: just fits
        PaceCheck("late", 60.0).check(governor, {"stocks": 5})  # a minute is too short
    assert [record.getMessage().split(":")[0] for record in caplog.records] == ["late"]
