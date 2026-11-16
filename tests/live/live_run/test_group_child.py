"""Tests of the child side of the built-in groups (`tests/_live/live_run/group_child.py`).

Each test writes the `request.json` the program would, runs the group with sessions on the
fake REST server, and reads the child's `results.jsonl`. The streaming groups get an end long
past, so they open their session, sample nothing, and report at once; the upgrade check's own
run is replaced by a stand-in, since its waits are its own module's to test.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from tests._fakes.rest import Reply
from tests._live.live_run import group_child, upgrade_check
from tests._live.live_run.outcomes import read_outcomes
from tests._live.live_run.upgrade_check import Record

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession
    from tests._fakes.rest import FakeRestServer

PAST = "2020-01-02T00:00:00+00:00"


@pytest.fixture
def open_session(fake_rest: FakeRestServer) -> Callable[[], MassiveSession]:
    def make() -> MassiveSession:
        return dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))

    return make


def _request(tmp_path: Path, group: str, due: list[str], **extra: object) -> str:
    folder = tmp_path / group / "2026-11-04-1"
    folder.mkdir(parents=True)
    request = {"group": group, "due": due, "until": PAST}
    (folder / "request.json").write_text(json.dumps(request | extra), encoding="utf-8")
    return f"{group}/2026-11-04-1"


def _results(tmp_path: Path, attempt: str) -> list[tuple[str, str, str]]:
    lines = read_outcomes(tmp_path / attempt / "results.jsonl")
    return [(r["name"], r["status"], r["reason"]) for r in lines]


NOTHING_DELAYED: frozenset[str] = frozenset()


def test_the_responses_group_runs_its_due_examples(
    fake_rest: FakeRestServer, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    fake_rest.reply("/v1/marketstatus/now", Reply({"market": "open"}))
    attempt = _request(tmp_path, "responses", ["market-status"])
    group_child.run_group(tmp_path, attempt, open_session=open_session, delayed=NOTHING_DELAYED)
    assert _results(tmp_path, attempt) == [("market-status", "passed", "")]
    assert (tmp_path / attempt / "market-status.jsonl").exists()


def test_the_upgrade_check_reports_its_verdict(
    open_session: Callable[[], MassiveSession], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def check(subscription: str, **_options: object) -> Record:
        return Record(subscription, verdict="failed: Massive kept limiting for 15 minutes")

    monkeypatch.setattr(upgrade_check, "run_upgrade_check", check)
    attempt = _request(tmp_path, "upgrade-check", ["indices"])
    group_child.run_group(tmp_path, attempt, open_session=open_session, delayed=NOTHING_DELAYED)
    assert _results(tmp_path, attempt) == [
        ("indices", "failed", "Massive kept limiting for 15 minutes")
    ]


@pytest.mark.parametrize("group", ["stream-day", "throughput", "stream-night"])
def test_a_streaming_group_past_its_end_reports_its_units(
    group: str, open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    attempt = _request(tmp_path, group, [], stream_day="2026-11-03")
    group_child.run_group(tmp_path, attempt, open_session=open_session, delayed=NOTHING_DELAYED)
    assert _results(tmp_path, attempt) == []


def test_the_night_reconciliation_compares_the_latest_night(
    open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    attempt = _request(
        tmp_path,
        "stream-night-reconcile",
        ["crypto"],
        stream_folder=str(tmp_path / "stream-night" / "2026-11-03-1"),
        stream_day="2026-11-03",
    )
    group_child.run_group(tmp_path, attempt, open_session=open_session, delayed=NOTHING_DELAYED)
    assert _results(tmp_path, attempt) == [("crypto", "failed", "no streamed bars to compare")]
    summary = json.loads((tmp_path / attempt / "summary.json").read_text())
    assert summary["night"] == "2026-11-03"


def test_the_reconciliation_compares_the_latest_stream_day(
    open_session: Callable[[], MassiveSession], tmp_path: Path
) -> None:
    attempt = _request(
        tmp_path,
        "stream-day-reconcile",
        ["indices"],
        stream_folder=str(tmp_path / "stream-day" / "2026-11-03-1"),
        stream_day="2026-11-03",
    )
    group_child.run_group(tmp_path, attempt, open_session=open_session, delayed=NOTHING_DELAYED)
    assert _results(tmp_path, attempt) == [("indices", "failed", "no streamed bars to compare")]
    summary = json.loads((tmp_path / attempt / "summary.json").read_text())
    assert summary["day"] == "2026-11-03"
