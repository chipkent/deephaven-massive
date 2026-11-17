"""Tests of the live run's attended upgrade check (`tests/_live/live_run/upgrade_check.py`).

Sessions reach the fake REST server, which answers a limited subscription's 429 until the
scripted upgrade; the test's governors wait on a fake clock (`rest_clock`), so no pause sleeps.
The check's own clock, sleeps, and Enter are stand-ins.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from tests._fakes.rest import Reply, envelope
from tests._live.live_run import upgrade_check
from tests._live.live_run.stream_units import SUBSCRIPTIONS
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

    Each call takes `call_seconds`, or after the first sleep `slept_call_seconds` when set.
    Moving the governors' clock lets a closed session's governor go idle, so the next session
    starts with a fresh one, as in real time.
    """

    rest: FakeClock
    at: datetime = datetime(2026, 11, 4, 15, tzinfo=UTC)
    ticks: float = 0.0
    call_seconds: float = 0.5
    slept_call_seconds: float | None = None
    sleeps: list[float] = field(default_factory=list)
    entered: int = 0

    def now(self) -> datetime:
        return self.at

    def monotonic(self) -> float:
        self.ticks += self.call_seconds  # read once before and once after each call
        return self.ticks

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if self.slept_call_seconds is not None:
            self.call_seconds = self.slept_call_seconds
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
    # Session A learns the limit; the first minute's six calls draw a 429; the next minute's
    # are all answered, so the upgrade took effect.
    fake_rest.reply(PREV, BAR, LIMITED, BAR, LIMITED, BAR)
    clock = _Time(rest_clock)
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=clock.waits()
    )
    assert record.verdict == "passed"
    assert record.learned_calls == 2
    assert record.row_before["LimitSource"] == "LEARNED"
    assert clock.entered == 1
    assert clock.sleeps == [60.0, upgrade_check.FRESH_MINUTE]
    assert [a["refused"] for a in record.answered] == [True, False]
    assert len(record.answered[1]["calls"]) == upgrade_check.ANSWERED_CALLS
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
    # The six calls are answered at once; session B's calls, after the fresh minute, are slow.
    fake_rest.reply(PREV, BAR, LIMITED, BAR)
    clock = _Time(rest_clock, slept_call_seconds=31.0)
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=clock.waits()
    )
    assert record.verdict == "failed: library paced"


def test_six_calls_answered_over_more_than_a_minute_are_no_upgrade(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    """Six calls spread over more than a minute do not show the upgrade.

    Calls that a limit the library still holds paces draw no 429, though Massive may still
    limit the subscription.
    """
    fake_rest.reply(PREV, BAR, LIMITED, BAR)
    clock = _Time(rest_clock, call_seconds=11.0)  # six calls take 132 s
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=clock.waits()
    )
    assert record.verdict == "failed: Massive kept limiting for 15 minutes"
    assert record.rounds == []
    assert record.answered
    assert not any(a["refused"] for a in record.answered)
    assert all(a["seconds"] > upgrade_check.ANSWERED_WITHIN for a in record.answered)


def test_refusals_until_the_wait_ends_blame_massive(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    # Six calls answered, then every session B is refused again.
    fake_rest.reply(PREV, BAR, LIMITED, *[BAR] * 7, *[LIMITED, BAR] * 400)
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=_Time(rest_clock).waits()
    )
    assert record.verdict == "failed: Massive refused a round of ten"
    assert all(r["refused"] for r in record.rounds)
    assert len(record.rounds) == 14  # from 65 s, then every 60 s, within 15 minutes


def test_no_upgrade_is_a_rehearsal(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    fake_rest.reply(PREV, BAR, *[LIMITED, BAR] * 200)  # every minute's six calls draw a 429
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=_Time(rest_clock).waits()
    )
    assert record.verdict == "failed: Massive kept limiting for 15 minutes"
    assert record.rounds == []
    assert record.answered
    assert all(a["refused"] for a in record.answered)


def test_a_limit_never_learned_stops_the_check(
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    monkeypatch: pytest.MonkeyPatch,
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


FORBIDDEN = Reply(envelope(status="ERROR", message="not entitled"), status=403)


def test_six_failed_calls_are_no_upgrade(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    """Calls that fail fast draw no 429, but Massive answered none of them."""
    fake_rest.reply(PREV, BAR, LIMITED, BAR, FORBIDDEN)  # session A learns the limit, then 403s
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=_Time(rest_clock).waits()
    )
    assert record.verdict == "failed: Massive kept limiting for 15 minutes"
    assert record.rounds == []
    assert record.answered
    assert all(a["failed"] and not a["refused"] for a in record.answered)


def test_a_failed_call_in_a_round_of_ten_fails_the_check(
    *,
    fake_rest: FakeRestServer,
    open_session: Callable[[], MassiveSession],
    tmp_path: Path,
    rest_clock: FakeClock,
) -> None:
    # Six calls answered, then session B's second call is refused the data.
    fake_rest.reply(PREV, BAR, LIMITED, *[BAR] * 7, FORBIDDEN)
    record = upgrade_check.run_upgrade_check(
        "stocks", open_session=open_session, folder=tmp_path, waits=_Time(rest_clock).waits()
    )
    [round_] = record.rounds
    assert not round_["refused"]
    assert record.verdict.startswith("failed: a call of a round of ten failed: EntitlementError")


def test_a_404_is_an_answer_and_other_errors_are_failed_calls(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], rest_clock: FakeClock
) -> None:
    # The fake answers 404 to the options call's contract, as Massive does an expired one: the
    # library leaves the contract out and raises nothing.
    result = upgrade_check.round_of_calls(open_session, "options", _Time(rest_clock).waits())
    assert [c["error"] for c in result["calls"]] == [""] * upgrade_check.ROUND_CALLS
    assert result["failed"] == []
    fake_rest.reply(PREV, FORBIDDEN)
    result = upgrade_check.round_of_calls(open_session, "stocks", _Time(rest_clock).waits())
    assert len(result["failed"]) == upgrade_check.ROUND_CALLS


def test_each_subscription_has_a_one_request_call() -> None:
    assert set(upgrade_check.CALLS) == set(SUBSCRIPTIONS)


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


def test_the_options_call_reads_a_contract_bar_not_the_chain(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession]
) -> None:
    with open_session() as m:
        upgrade_check.CALLS["options"](m)
    [request] = fake_rest.requests
    assert request.target.startswith("/v2/aggs/ticker/O%3ASPY261218C00600000/prev")


def test_each_call_s_effect_is_shown_before_its_row_is_read(
    massive_session: MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A call's effect reaches m.rate_limits() only on a later update cycle: learn_limit waits
    # for it after each call and then reads the row once, so the second call, whose effect
    # is the learned limit, is the last one made.
    steps: list[str] = []
    calls = shown = 0

    def call(_m: MassiveSession) -> None:
        nonlocal calls
        calls += 1
        steps.append("call")

    def wait(_m: MassiveSession) -> None:
        nonlocal shown
        shown = calls
        steps.append("wait")

    def row(_m: MassiveSession, _subscription: str) -> dict[str, Any]:
        steps.append("read")
        return {"LimitSource": "LEARNED"} if shown >= 2 else {}

    monkeypatch.setitem(upgrade_check.CALLS, "stocks", call)
    monkeypatch.setattr(upgrade_check, "wait_for_diagnostics", wait)
    monkeypatch.setattr(upgrade_check, "_row", row)
    assert upgrade_check.learn_limit(massive_session, "stocks") == 2  # the calls never read it
    assert steps == ["call", "wait", "read"] * 2
