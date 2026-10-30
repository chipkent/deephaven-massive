"""Tests of the live run's attempts (`tests/_live/attempts.py`).

The program's interpreter is a wrapper around the fake child (`tests/_fakes/live_child.py`),
scripted per test through `FAKE_LIVE_CHILD`, so no live test ever runs and nothing reaches
Massive. The tests read the state, the attempt folders, and the logs, and plant the API key in
the child's output to see it cleaned.
"""

from __future__ import annotations

import _thread
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests._live import attempts, run_state
from tests._live.calendar import NEW_YORK
from tests._live.run_context import RunContext, RunMemory
from tests._live.run_state import Attempt, RunState, check_of, entitlement_id, record
from tests._live.secret_scrub import SecretFilter

if TYPE_CHECKING:
    from tests._live.machine_checks import Item
    from tests._live.run_probe import ProbeResult

ROOT = Path(__file__).resolve().parents[2]

PLANTED_KEY = "PLANTED-attempts-key-90d2"

NOW = datetime(2026, 11, 4, 12, tzinfo=NEW_YORK)

S3_PAIR = ("PLANTED-access-id", "PLANTED-s3-value")

BASIC = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "basic")


def _no_probe() -> ProbeResult:
    pytest.fail("no probe expected")


def _no_machine(_folder: Path, _offset: float | None) -> tuple[list[Item], dict[str, object]]:
    pytest.fail("no machine check expected")


class Harness:
    """A context on the fake child, its echoed lines, and a state on Basic."""

    def __init__(self, tmp_path: Path) -> None:
        wrapper = tmp_path / "python"
        child = ROOT / "tests" / "_fakes" / "live_child.py"
        wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{child}" "$@"\n')
        wrapper.chmod(0o755)
        self.root = tmp_path / "root"
        self.root.mkdir()
        self.lines: list[str] = []
        self.environ: dict[str, str] = {"MASSIVE_API_KEY": PLANTED_KEY, "FAKE_LIVE_CHILD": "{}"}
        self.ctx = RunContext(
            out=tmp_path / "out",
            root=self.root,
            environ=self.environ,
            secrets=SecretFilter((PLANTED_KEY,)),
            commit="c",
            probe=_no_probe,
            machine=_no_machine,
            now=lambda: NOW,
            python=str(wrapper),
            script=Path("live_run.py"),
            echo=self.lines.append,
            stop_grace=1.0,
        )
        self.ctx.out.mkdir()
        self.state = RunState(commit="c")
        self.state.current = entitlement_id(self.state, BASIC, [])
        self.memory = RunMemory()

    def script(self, **behavior: object) -> None:
        self.environ["FAKE_LIVE_CHILD"] = json.dumps(behavior)

    def check(self, group: str, name: str) -> run_state.Check:
        return check_of(self.state, group, name)


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


A, B, C = (
    "tests/x_integration.py::test_a",
    "tests/x_integration.py::test_b",
    "tests/x_integration.py::test_c",
)

PASS = [{"phase": "setup", "outcome": "passed"}, {"phase": "call", "outcome": "passed"}]


def _phase(phase: str, outcome: str, *, reason: str = "", error_type: str = "") -> dict[str, str]:
    return {"phase": phase, "outcome": outcome, "reason": reason, "error_type": error_type}


def _run(harness: Harness, group: str = "evidence", *, until: datetime | None = None) -> list[str]:
    probes: list[str] = []
    attempts.run_pytest_group(
        harness.ctx,
        harness.state,
        harness.memory,
        group=group,
        until=until or NOW + timedelta(hours=1),
        reprobe=lambda: probes.append("probed"),
    )
    return probes


def test_a_pytest_group_runs_only_its_due_tests_and_records_each(harness: Harness) -> None:
    record(
        harness.state, harness.check("evidence", C), status="passed", reason="", day="2026-11-03"
    )
    harness.script(
        print=[f"collecting with {PLANTED_KEY}"],
        evidence="evidence line\n",
        collection={
            A: {"items": ["21"], "requests": 12},
            B: {"items": [], "requests": 10},
            C: {"items": ["22"], "requests": 10},
        },
        outcomes={
            A: PASS,
            B: [_phase("setup", "skipped", reason="needs Stocks Starter ($29/month)")],
        },
    )
    (harness.root / "live-evidence.log").write_text("left by a run before\n")
    assert _run(harness) == []
    folder = harness.ctx.out / "evidence" / "2026-11-04-1"
    argv = json.loads((folder / "argv.json").read_text())
    assert argv[:4] == ["-m", "pytest", "-p", "tests._live.outcomes"]
    assert argv[-2:] == [A, B]  # the passed test is not due
    assert argv[-6:-2] == ["-m", "integration", "-k", "not luld"]
    assert harness.check("evidence", A).status == "passed"
    assert harness.check("evidence", A).items == ["21"]
    assert (harness.check("evidence", B).status, harness.check("evidence", B).reason) == (
        "skipped",
        "needs Stocks Starter ($29/month)",
    )
    assert harness.state.known["evidence"] == [A, B, C]
    assert (
        harness.ctx.out / "orphans" / "live-evidence.log"
    ).read_text() == "left by a run before\n"
    assert (folder / "live-evidence.log").read_text() == "evidence line\n"
    log = (folder / "group.log").read_text()
    assert "collecting with ***" in log
    assert PLANTED_KEY not in log
    assert all(PLANTED_KEY not in line for line in harness.lines)
    assert any(
        "evidence: 2 checks; on Basic, up to 4 minutes of requests" in line
        for line in harness.lines
    )
    assert harness.state.attempts[-1].status == "finished"
    assert harness.memory.finished_since_pack
    assert run_state.load(harness.ctx.out) == harness.state


def test_a_test_that_left_no_record_is_an_error_when_the_child_ends_by_itself(
    harness: Harness,
) -> None:
    harness.script(collection={A: {"items": [], "requests": 10}}, exit=1)
    _run(harness)
    check = harness.check("evidence", A)
    assert (check.status, check.reason, check.failures) == ("error", "the child ended first", 1)


def test_a_group_with_nothing_due_runs_no_tests(harness: Harness) -> None:
    record(harness.state, harness.check("smoke", A), status="passed", reason="", day="")
    harness.script(collection={A: {"items": [], "requests": 10}})
    _run(harness, "smoke")
    folder = harness.ctx.out / "smoke" / "2026-11-04-1"
    assert not (folder / "argv.json").exists()
    assert harness.state.attempts[-1].status == "finished"


def test_a_failed_collection_leaves_the_known_tests_unchanged(harness: Harness) -> None:
    harness.state.known["smoke"] = [A]
    harness.script(exit=4)
    _run(harness, "smoke")
    assert harness.state.known["smoke"] == [A]


def test_a_refusal_of_an_allowed_test_reprobes_and_is_recorded_refused(harness: Harness) -> None:
    refused = [_phase("setup", "passed"), _phase("call", "failed", error_type="EntitlementError")]
    harness.script(collection={A: {"items": [], "requests": 1}}, outcomes={A: refused})
    assert _run(harness) == ["probed"]
    assert harness.check("evidence", A).status == "refused"


def test_a_refusal_after_which_a_tier_fell_stays_due(harness: Harness) -> None:
    refused = [_phase("call", "failed", error_type="WireEntitlementError")]
    harness.script(collection={A: {"items": [], "requests": 1}}, outcomes={A: refused})
    record(harness.state, harness.check("evidence", A), status="passed", reason="", day="")
    harness.state.current = entitlement_id(harness.state, BASIC | {"stocks": "starter"}, [])

    def fall() -> None:
        harness.state.current = entitlement_id(harness.state, BASIC, [])

    attempts.run_pytest_group(
        harness.ctx,
        harness.state,
        harness.memory,
        group="evidence",
        until=NOW + timedelta(hours=1),
        reprobe=fall,
    )
    check = harness.check("evidence", A)
    assert check.status == "passed"  # not recorded: it runs again under the lower tier
    assert harness.state.current == "e1"


def test_half_day_checks_carry_their_half_day(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monday = datetime(2026, 11, 30, 12, tzinfo=NEW_YORK)
    monkeypatch.setattr(harness.ctx, "now", lambda: monday)
    harness.script(collection={A: {"items": [], "requests": 1}}, outcomes={A: PASS})
    _run(harness, "half-day", until=monday + timedelta(hours=1))
    assert harness.check("half-day", f"{A} after 2026-11-27").status == "passed"


def test_a_child_running_past_its_stop_is_stopped_and_its_tests_stay_due(harness: Harness) -> None:
    harness.script(collection={A: {"items": [], "requests": 1}}, hang=True)
    _run(harness, until=NOW + timedelta(seconds=1))
    check = harness.check("evidence", A)
    assert (check.status, check.failures) == ("not run", 0)
    assert harness.state.attempts[-1].status == "finished"


def test_a_child_that_ignores_the_interrupt_is_killed(harness: Harness) -> None:
    harness.script(collection={A: {"items": [], "requests": 1}}, hang=True, ignore_interrupt=True)
    _run(harness, until=NOW + timedelta(seconds=1))
    assert harness.state.attempts[-1].exit_code == -9


def test_control_c_stops_the_child_and_records_the_attempt_interrupted(harness: Harness) -> None:
    def interrupt(line: str) -> None:
        if line.endswith("hanging"):
            _thread.interrupt_main()

    harness.ctx.echo = interrupt
    harness.script(collection={A: {"items": [], "requests": 1}}, hang=True)
    with pytest.raises(KeyboardInterrupt):
        _run(harness)
    assert harness.state.attempts[-1].status == "interrupted"
    assert harness.check("evidence", A).status == "not run"


def test_a_built_in_group_reports_through_its_results_file(harness: Harness) -> None:
    harness.ctx.script = Path("fake.py")
    harness.environ["MASSIVE_S3_ACCESS_KEY_ID"], harness.environ["MASSIVE_S3_SECRET_ACCESS_KEY"] = (
        S3_PAIR
    )
    harness.script(results=[{"name": "market-status", "status": "passed", "reason": ""}])
    harness.memory.any_time = ["responses"]
    attempts.run_built_in_group(
        harness.ctx,
        harness.state,
        harness.memory,
        group="responses",
        until=NOW + timedelta(hours=1),
    )
    folder = harness.ctx.out / "responses" / "2026-11-04-1"
    request = json.loads((folder / "request.json").read_text())
    assert "stream-forex-quotes" not in request["due"]  # --any-time leaves out the streams
    assert request["tiers"] == BASIC
    assert json.loads((folder / "argv.json").read_text()) == [
        "fake.py", "_group", "--out", str(harness.ctx.out), "--attempt", "responses/2026-11-04-1",
        "--token", harness.state.attempts[-1].token,
    ]  # fmt: skip
    assert harness.check("responses", "market-status").status == "passed"
    assert harness.check("responses", "ticker-details").status == "error"


def test_the_responses_child_gets_no_s3_pair(harness: Harness) -> None:
    harness.environ["MASSIVE_S3_ACCESS_KEY_ID"] = "PLANTED-access-id"
    folder = harness.ctx.out
    env = attempts.environment(harness.ctx, harness.state, folder=folder, extra={}, with_s3=False)
    assert "MASSIVE_S3_ACCESS_KEY_ID" not in env
    assert env["MASSIVE_LIVE_PLANS"].startswith("stocks=basic,options=basic")
    assert env["DEEPHAVEN_MASSIVE_LOG_FILE"] == str((folder / "debug.log").resolve())


def test_a_stream_day_restarts_in_the_same_folder_and_is_remembered(harness: Harness) -> None:
    harness.state.current = entitlement_id(harness.state, BASIC | {"stocks": "starter"}, [])
    harness.script(hang=True)
    for _ in range(2):
        attempts.run_built_in_group(
            harness.ctx,
            harness.state,
            harness.memory,
            group="stream-day",
            until=NOW - attempts.STREAM_GRACE + timedelta(seconds=1),
        )
    folders = [a.folder for a in harness.state.attempts]
    assert folders == ["stream-day/2026-11-04-1", "stream-day/2026-11-04-1"]
    assert harness.state.stream_days == ["2026-11-04"]
    assert (
        harness.check("stream-day", "stocks delayed").status == "not run"
    )  # stopped, so still due
    request = json.loads((harness.ctx.out / folders[0] / "request.json").read_text())
    assert request["stream_folder"] == str(harness.ctx.out / "stream-day/2026-11-04-1")


def test_attempts_are_numbered_per_group_and_day(harness: Harness) -> None:
    first = attempts.new_attempt(harness.ctx, harness.state, "smoke")
    second = attempts.new_attempt(harness.ctx, harness.state, "smoke")
    assert (first.folder, second.folder) == ("smoke/2026-11-04-1", "smoke/2026-11-04-2")
    assert first.token != second.token
    assert (harness.ctx.out / second.folder).is_dir()


def test_attempts_whose_child_is_gone_are_marked_interrupted() -> None:
    state = RunState(commit="c")
    gone = Attempt("evidence", "e", "2026-11-04", "", pid=0)
    done = Attempt("evidence", "e", "2026-11-04", "", status="finished")
    state.attempts = [gone, done]
    assert attempts.mark_stale_attempts(state) == [gone]
    assert (gone.status, done.status) == ("interrupted", "finished")


def test_evidence_never_overwrites_evidence(tmp_path: Path) -> None:
    folder = tmp_path / "attempt"
    for text in ("first", "second", "third"):
        (tmp_path / "live-evidence.log").write_text(text)
        attempts.move_evidence(tmp_path / "live-evidence.log", folder)
    names = sorted(p.name for p in folder.iterdir())
    assert names == ["live-evidence-2.log", "live-evidence-3.log", "live-evidence.log"]
    attempts.move_evidence(tmp_path / "missing.log", folder)
