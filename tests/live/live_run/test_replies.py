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
from deephaven_massive.queries import StreamHistoryQuery
from tests._fakes.rest import Reply, envelope
from tests._live.live_run import replies, reply_examples

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


def _example(name: str) -> reply_examples.Example:
    return next(e for e in reply_examples.EXAMPLES if e.name == name)


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
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
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
    assert index == [{"name": "market-status", "status": "passed", "reason": ""}]


def test_an_example_massive_refuses_is_refused_quoting_massive_with_its_reply_kept(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    refusal = envelope(status="NOT_AUTHORIZED", message="You are not entitled to this data.")
    fake_rest.reply("/v2/last/trade/AAPL", Reply(refusal, status=403))
    seen, report = _report()
    replies.run_responses(
        [_example("stocks-last-trade")],
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        report=report,
    )
    [(name, status, reason)] = seen
    assert (name, status) == ("stocks-last-trade", "refused")
    assert reason.startswith("refused: ")
    assert "not entitled" in reason
    [reply] = _lines(tmp_path / "stocks-last-trade.jsonl")
    assert reply["status"] == 403


def test_an_example_massive_answers_passes(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    fake_rest.reply("/v3/snapshot/indices", Reply(envelope([{"ticker": "I:SPX", "value": 1.0}])))
    seen, report = _report()
    replies.run_responses(
        [_example("indices-snapshot")],
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        report=report,
    )
    assert seen == [("indices-snapshot", "passed", "")]


def test_an_example_without_a_reply_fails_naming_what_it_raised(
    open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    def broken(m: MassiveSession, today: date) -> None:
        raise ValueError("no request made")

    status, reason = replies.run_example(
        reply_examples.Example("broken"),
        broken,
        open_session=open_session,
        folder=tmp_path,
        today=TODAY,
        hold=_never,
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
        reply_examples.Example("late"),
        fetch_then_fail,
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
        replies.CALLS["unknown-ticker-details"],
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

        harness[-1].wait(lambda _: failed(), "the refused stream to fail")

    def minute_bars(m: MassiveSession, _: date) -> Table:
        bars = m.crypto.bars("X:BTCUSD", dm.Timespan.MINUTE)
        assert isinstance(bars, StreamHistoryQuery)  # 1-minute bars stream
        return bars.append_only()

    status, reason = replies.run_example(
        reply_examples.Example("refused-bars", stream=True),
        minute_bars,
        open_session=open_ws,
        folder=tmp_path,
        today=TODAY,
        hold=wait_failed,
    )
    assert status == "refused"
    assert reason.startswith("refused: ")
    lines = _lines(tmp_path / "refused-bars.jsonl")
    assert lines
    assert all(line["transport"] == "ws" for line in lines)
    assert any("not authorized" in str(line["text"]) for line in lines)


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


def test_an_unlisted_delay_skips_as_not_listed_and_massive_refusal_stays_refused() -> None:
    """The settings refusing delayed data is the owner's to fix, not Massive's refusal."""
    example = _example("market-status")
    delayed = dm.EntitlementError("stocks data would be delayed", source=dm.ErrorSource.USER)
    refused = dm.EntitlementError("HTTP 403: not entitled", status=403)
    assert replies._verdict(example, error=delayed, stream_error="", count=1) == (
        "skipped",
        "not listed as delayed: stocks data would be delayed",
    )
    assert replies._verdict(example, error=refused, stream_error="", count=1) == (
        "refused",
        "refused: HTTP 403: not entitled",
    )


def test_an_entitlement_error_the_library_built_fails_not_refused() -> None:
    """A refusal the library built, with no status of Massive's, is no reply."""
    example = _example("market-status")
    built = dm.EntitlementError("not entitled")
    assert replies._verdict(example, error=built, stream_error="", count=1) == (
        "failed",
        "EntitlementError: not entitled",
    )


def test_a_stream_the_settings_refused_later_skips_as_not_listed() -> None:
    """The settings' refusal can fail a stream after it opened, in `m.streams()` alone."""
    example = _example("market-status")
    error = (
        "stocks streams would be delayed (Massive reports timeframe=DELAYED), and "
        "dm.Subscription.STOCKS is not in MassiveConfig.delayed_subscriptions"
    )
    assert replies._verdict(example, error=None, stream_error=error, count=1) == (
        "skipped",
        f"not listed as delayed: {error}",
    )


def test_every_example_has_its_code_and_no_code_lacks_an_example() -> None:
    assert list(replies.CALLS) == [e.name for e in reply_examples.EXAMPLES]


def test_every_example_asks_through_the_public_verbs(monkeypatch: pytest.MonkeyPatch) -> None:
    # An example that checks its bars query streams before it streams takes the recorder as one.
    monkeypatch.setattr(replies, "StreamHistoryQuery", _Recorder)
    for example in reply_examples.EXAMPLES:
        calls: list[str] = []
        session: MassiveSession = _Recorder(calls)  # type: ignore[assignment]  # records the verbs
        replies.CALLS[example.name](session, TODAY)
        verb = "append_only" if example.stream else ("history", "fetch")
        assert any(name in calls for name in ((verb,) if isinstance(verb, str) else verb))
    calls = []
    recorder: MassiveSession = _Recorder(calls)  # type: ignore[assignment]  # records the verbs
    replies.CALLS["daily-bars-last-year"](recorder, TODAY)
    year = "datetime.date(2025, 11, 4), datetime.date(2026, 11, 3)"
    assert any(year in call and "HistorySource.REST" in call for call in calls)


def test_the_default_hold_waits_its_seconds(massive_session: MassiveSession) -> None:
    started = time.monotonic()
    replies.hold_for(0.01)(massive_session)  # never read
    assert time.monotonic() - started >= 0.01
