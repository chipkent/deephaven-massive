"""Tests of the live run's `responses` group (`tests/_live/live_run/replies.py`).

Each example runs in a real `MassiveSession` against the fake REST server, or the fake
WebSocket server for streams; the tests read the replies files and the index it writes.
"""

from __future__ import annotations

import json
import time
from datetime import date
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from tests._fakes.rest import Reply, envelope
from tests._live.live_run import replies
from tests._live.plans import TIERS, LivePlans

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven.table import Table

    from deephaven_massive import MassiveSession
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

TODAY = date(2026, 11, 4)

PLANTED_KEY = "PLANTED-replies-key-77c1"


def _plans(**tiers: str) -> LivePlans:
    return LivePlans({n: tiers.get(n, "basic") for n in TIERS}, s3=False, flat_files=(), source="t")


def _example(name: str) -> replies.Example:
    return next(e for e in replies.EXAMPLES if e.name == name)


@pytest.fixture
def open_session(fake_rest: FakeRestServer) -> Callable[[], MassiveSession]:
    def make() -> MassiveSession:
        return dm.MassiveSession(dm.MassiveConfig(api_key=PLANTED_KEY, rest_base_url=fake_rest.url))

    return make


def _report() -> tuple[list[tuple[str, str, str]], Callable[[str, str, str], None]]:
    seen: list[tuple[str, str, str]] = []
    return seen, lambda *result: seen.append(result)


def _never(_session: MassiveSession) -> None:
    pytest.fail("a REST example is never held")


def _lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_an_example_records_every_reply_cleaned_and_passes_with_data(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    body = {"market": "open", "serverTime": "2026-11-04T10:00:00-05:00", "echo": PLANTED_KEY}
    fake_rest.reply("/v1/marketstatus/now", Reply(body))
    seen, report = _report()
    replies.run_responses(
        [_example("market-status")],
        plans=_plans(),
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        entitlement="e1",
        report=report,
    )
    assert seen == [("market-status", "passed", "")]
    [reply] = _lines(tmp_path / "market-status.jsonl")
    assert (reply["transport"], reply["path"], reply["status"]) == (
        "rest",
        "/v1/marketstatus/now",
        200,
    )
    assert PLANTED_KEY not in (tmp_path / "market-status.jsonl").read_text()
    index = json.loads((tmp_path / "index.json").read_text())
    assert index == [
        {"name": "market-status", "status": "passed", "reason": "", "entitlement": "e1"}
    ]


def test_a_refusal_example_passes_when_massive_refuses(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    refusal = envelope(status="NOT_AUTHORIZED", message="You are not entitled to this data.")
    fake_rest.reply("/v2/last/trade/AAPL", Reply(refusal, status=403))
    seen, report = _report()
    replies.run_responses(
        [_example("refusal-stocks-developer")],
        plans=_plans(stocks="starter"),
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        entitlement="e1",
        report=report,
    )
    assert seen == [("refusal-stocks-developer", "passed", "")]
    [reply] = _lines(tmp_path / "refusal-stocks-developer.jsonl")
    assert reply["status"] == 403


def test_a_refusal_example_fails_when_massive_answers(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    fake_rest.reply("/v3/snapshot/indices", Reply(envelope([{"ticker": "I:SPX", "value": 1.0}])))
    seen, report = _report()
    replies.run_responses(
        [_example("refusal-indices-starter")],
        plans=_plans(),
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        entitlement="e1",
        report=report,
    )
    assert seen == [("refusal-indices-starter", "failed", "expected a refusal")]


def test_an_example_without_a_reply_fails_naming_what_it_raised(
    open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    def broken(m: MassiveSession, today: date) -> None:
        raise ValueError("no request made")

    example = replies.Example("broken", broken)
    status, reason = replies.run_example(
        example, open_session=open_session, folder=tmp_path, today=TODAY, hold=_never
    )
    assert (status, reason) == ("failed", "no reply was captured: ValueError: no request made")


def test_a_data_example_that_raises_after_a_reply_fails_with_the_error(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    fake_rest.reply("/v1/marketstatus/now", Reply({"market": "open"}))

    def fetch_then_fail(m: MassiveSession, today: date) -> None:
        m.reference.market_status().fetch(wait=True)
        raise dm.MassiveError("after the reply")

    status, reason = replies.run_example(
        replies.Example("late", fetch_then_fail),
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        hold=_never,
    )
    assert (status, reason) == ("failed", "MassiveError: after the reply")


def test_an_example_expecting_any_reply_passes_on_an_unknown_ticker(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    fake_rest.reply(
        "/v3/reference/tickers/ZZZZZQ",
        Reply(envelope(status="NOT_FOUND", message="Ticker not found."), status=404),
    )
    status, _ = replies.run_example(
        _example("unknown-ticker-details"),
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        hold=_never,
    )
    assert status == "passed"


def test_a_stream_example_records_frames_and_a_refused_stream(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    tmp_path: Path,
) -> None:
    harness: list[WsSession] = []
    fake_ws_server.refuse["XA.BTC-USD"] = "not authorized"

    def open_ws() -> MassiveSession:
        harness.append(make_ws_session())
        return harness[-1].session

    def wait_failed(m: MassiveSession) -> None:
        def failed() -> bool:
            harness[-1].flush()
            assert m._core.diagnostics.wait_until_shown(15.0)
            return any(r["State"] == "FAILED" for r in dh_to_pylist(m.streams()))

        fake_ws_server.wait_for(lambda _: failed())

    example = replies.Example(
        "refused-bars",
        lambda m, _: m.crypto.bars("X:BTCUSD", dm.Timespan.MINUTE).append_only(),
        expects=replies.REFUSAL,
        stream=True,
    )
    status, reason = replies.run_example(
        example, open_session=open_ws, folder=tmp_path, today=TODAY, hold=wait_failed
    )
    assert (status, reason) == ("passed", "")
    lines = _lines(tmp_path / "refused-bars.jsonl")
    assert lines
    assert all(line["transport"] == "ws" for line in lines)
    assert any("not authorized" in str(line["text"]) for line in lines)


@pytest.mark.parametrize(
    ("name", "tiers", "reason"),
    [
        ("market-status", {}, None),
        ("refusal-stocks-developer", {}, "not this tier: records the refusal on stocks starter"),
        ("stream-forex-quotes", {}, "needs Currencies Starter ($49/month)"),
        ("stream-forex-quotes", {"currencies": "starter"}, None),
        (
            "nvda-daily-2024-06-adjusted",
            {},
            "needs Stocks Starter ($29/month) (history back to 2024-06-03)",
        ),
        ("nvda-daily-2024-06-adjusted", {"stocks": "starter"}, None),
        ("nvda-daily-2024-06-adjusted", {"stocks": "advanced"}, None),
    ],
)
def test_an_example_is_skipped_with_what_it_needs(
    name: str, tiers: dict[str, str], reason: str | None
) -> None:
    assert replies.skip_reason(_example(name), _plans(**tiers), TODAY) == reason


def test_history_beyond_every_window_but_the_last_names_the_tier_that_keeps_all() -> None:
    reason = replies.history_need("stocks", date(2000, 1, 3), TODAY)
    assert reason == "needs Stocks Advanced ($199/month) (history back to 2000-01-03)"


def test_a_skipped_example_is_reported_and_indexed(tmp_path: Path) -> None:
    seen, report = _report()
    replies.run_responses(
        [_example("stream-forex-quotes")],
        plans=_plans(),
        open_session=lambda: pytest.fail("no session for a skipped example"),
        folder=tmp_path,
        today=TODAY,
        entitlement="e3",
        report=report,
    )
    assert seen == [("stream-forex-quotes", "skipped", "needs Currencies Starter ($49/month)")]
    assert json.loads((tmp_path / "index.json").read_text())[0]["entitlement"] == "e3"


class _Recorder:
    """Stands in for a session: every attribute and call is recorded and returns itself."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def __getattr__(self, name: str) -> _Recorder:
        self._calls.append(name)
        return self

    def __call__(self, *args: object, **kwargs: object) -> _Recorder:
        self._calls.append(repr((args, kwargs)))
        return self


def test_every_example_asks_through_the_public_verbs() -> None:
    for example in replies.EXAMPLES:
        calls: list[str] = []
        session: MassiveSession = _Recorder(calls)  # type: ignore[assignment]  # records the verbs
        example.run(session, TODAY)
        verb = "append_only" if example.stream else ("history", "fetch")
        assert any(name in calls for name in ((verb,) if isinstance(verb, str) else verb))
    calls = []
    daily = next(e for e in replies.EXAMPLES if e.name == "daily-bars-last-year")
    recorder: MassiveSession = _Recorder(calls)  # type: ignore[assignment]  # records the verbs
    daily.run(recorder, TODAY)
    year = "datetime.date(2025, 11, 4), datetime.date(2026, 11, 3)"
    assert any(year in call and "HistorySource.REST" in call for call in calls)


def test_the_default_hold_waits_its_seconds() -> None:
    started = time.monotonic()
    unused: MassiveSession = None  # type: ignore[assignment]  # the hold never reads it
    replies.hold_for(0.01)(unused)
    assert time.monotonic() - started >= 0.01
