"""Tests of the live run's attended upgrade check (`tests/_live/live_run/upgrade_check.py`).

Sessions reach the fake REST server, which answers the free plan's 429 until the scripted
upgrade; the test's governors wait on a fake clock (`rest_clock`), so no pause sleeps. The
check's own clock, sleeps, and Enter are stand-ins, and `tier_of` plays the probe.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from tests._fakes.rest import Reply, envelope
from tests._live.live_run import upgrade_check
from tests._live.live_run.upgrade_check import Waits

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession
    from tests._fakes.clock import FakeClock
    from tests._fakes.rest import FakeRestServer

PREV = "/v2/aggs/ticker/AAPL/prev"

LIMITED = Reply(
    envelope(
        status="ERROR",
        message="You've exceeded the maximum requests per minute, please wait or upgrade your "
        "subscription to continue.",
    ),
    status=429,
)

BAR = Reply(
    envelope(
        [{"T": "AAPL", "c": 1.0, "h": 1.0, "l": 1.0, "o": 1.0, "t": 1605042000000, "v": 1}],
        ticker="AAPL",
        resultsCount=1,
        adjusted=True,
    )
)


@dataclass
class _Time:
    """The check's clock: each sleep moves it, and the governors' clock, on.

    Each call takes `call_seconds`. Moving the governors' clock lets a closed session's
    governor go idle, so the next session starts with a fresh one, as in real time.
    """

    rest: FakeClock
    at: datetime = datetime(2026, 11, 4, 15, tzinfo=UTC)
    ticks: float = 0.0
    call_seconds: float = 0.5
    sleeps: list[float] = field(default_factory=list)
    entered: int = 0

    def now(self) -> datetime:
        return self.at

    def monotonic(self) -> float:
        self.ticks += self.call_seconds  # read once before and once after each call
        return self.ticks

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.at += timedelta(seconds=seconds)
        self.rest.advance(seconds)

    def read_line(self) -> str:
        self.entered += 1
        return ""

    def waits(self) -> Waits:
        return Waits(
            now=self.now, monotonic=self.monotonic, sleep=self.sleep, read_line=self.read_line
        )


@pytest.fixture
def open_session(fake_rest: FakeRestServer) -> Callable[[], MassiveSession]:
    def make() -> MassiveSession:
        return dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))

    return make


def test_an_upgrade_that_lifts_the_limit_passes(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    rest_clock: FakeClock,
) -> None:
    fake_rest.reply(PREV, LIMITED, BAR)
    tiers = ["basic", None, "starter"]
    clock = _Time(rest_clock)
    record = upgrade_check.run_upgrade_check(
        "stocks",
        open_session=open_session,
        tier_of=lambda: tiers.pop(0) if len(tiers) > 1 else tiers[0],
        folder=tmp_path,
        waits=clock.waits(),
    )
    assert record.verdict == "passed"
    assert record.learned_calls == 1
    assert record.row_before["LimitSource"] == "LEARNED"
    assert clock.entered == 1
    assert clock.sleeps == [60.0, 60.0, upgrade_check.FRESH_MINUTE]
    [round_] = record.rounds
    assert len(round_["calls"]) == upgrade_check.ROUND_CALLS
    assert not round_["refused"]
    assert "Now upgrade stocks on the dashboard, then press Enter" in capsys.readouterr().out
    stored = json.loads((tmp_path / "upgrade-check.json").read_text())
    assert stored["verdict"] == "passed"


def test_a_slow_round_without_refusals_blames_the_library(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    fake_rest.reply(PREV, LIMITED, BAR)
    clock = _Time(rest_clock, call_seconds=31.0)
    record = upgrade_check.run_upgrade_check(
        "stocks",
        open_session=open_session,
        tier_of=lambda: "starter",
        folder=tmp_path,
        waits=clock.waits(),
    )
    assert record.verdict == "failed: library paced"


def test_refusals_until_the_wait_ends_blame_massive(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    fake_rest.reply(PREV, *[LIMITED, BAR] * 100)  # every session B is refused again
    record = upgrade_check.run_upgrade_check(
        "stocks",
        open_session=open_session,
        tier_of=lambda: "starter",
        folder=tmp_path,
        waits=_Time(rest_clock).waits(),
    )
    assert record.verdict == "failed: Massive kept the free limit"
    assert all(r["refused"] for r in record.rounds)
    assert len(record.rounds) == 8  # every 125 s (65 s, a round, 60 s) within 15 minutes


def test_no_upgrade_is_a_rehearsal(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    fake_rest.reply(PREV, LIMITED, BAR)
    record = upgrade_check.run_upgrade_check(
        "stocks",
        open_session=open_session,
        tier_of=lambda: "basic",
        folder=tmp_path,
        waits=_Time(rest_clock).waits(),
    )
    assert record.verdict == "failed: tier stayed Basic"
    assert record.rounds == []


def test_a_limit_never_learned_stops_the_check(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession]
) -> None:
    fake_rest.reply(PREV, BAR)
    with open_session() as m, pytest.raises(RuntimeError, match="no limit was learned after 2"):
        upgrade_check.learn_limit(m, "stocks", limit=2)


def test_a_failed_call_is_recorded_with_its_error(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], rest_clock: FakeClock
) -> None:
    fake_rest.reply(PREV, Reply(envelope(status="ERROR", message="broken"), status=500))
    result = upgrade_check.round_of_calls(open_session, "stocks", _Time(rest_clock).waits())
    assert all(c["error"].startswith("MassiveError") for c in result["calls"])


def test_each_subscription_has_a_one_request_call() -> None:
    assert set(upgrade_check.CALLS) == set(upgrade_check.SUBSCRIPTIONS)


@pytest.mark.parametrize("subscription", ["options", "indices", "currencies", "futures"])
def test_each_call_asks_its_own_subscription(
    subscription: str, open_session: Callable[[], MassiveSession], fake_rest: FakeRestServer
) -> None:
    # The fake answers 404: only the request counts.
    with open_session() as m, contextlib.suppress(dm.MassiveError):
        upgrade_check.CALLS[subscription](m)
    assert fake_rest.requests


def test_the_default_waits_are_real() -> None:
    waits = Waits()
    before = datetime.now(UTC)
    assert before <= waits.now()


def test_the_options_call_reads_an_endpoint_basic_includes(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession]
) -> None:
    # A contract's previous-day bar: Options Basic has aggregates, but not the chain snapshot.
    with open_session() as m:
        upgrade_check.CALLS["options"](m)
    [request] = fake_rest.requests
    assert request.target.startswith("/v2/aggs/ticker/O%3ASPY261218C00600000/prev")
