"""Tests of the live-run program's commands (`tests/_live/live_run.py`).

Each command runs with a context of fakes: a scripted probe, a scripted machine check, a clock
that jumps to the end of every sleep, and an interpreter that runs the fake child
(`tests/_fakes/live_child.py`), so no live test runs and nothing reaches Massive. Every test
plants the secrets in the environment, the probe's answer, and the children's output, and the
end-to-end test proves that none of them reaches a file or the terminal.
"""

from __future__ import annotations

import _thread
import json
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests._live import live_run, results, run_state
from tests._live.calendar import NEW_YORK
from tests._live.machine_checks import FAIL, OK, Item
from tests._live.run_context import RunContext, RunMemory, RunRefusedError
from tests._live.run_probe import ProbeResult
from tests._live.run_state import RunState, check_of, entitlement_id, record
from tests._live.secret_scrub import SecretFilter, fingerprints

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

ROOT = Path(__file__).resolve().parents[2]

KEY = "PLANTED-run-key-4c1d"
DELAYED_KEY = "PLANTED-delayed-key-8e2a"
S3_PAIR = ("PLANTEDACCESSID7Q", "PLANTED/s3+value=91")

NOW = datetime(2026, 11, 4, 12, tzinfo=NEW_YORK)

BASIC = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "basic")

NODE = "tests/x_integration.py::test_a"


class Clock:
    """A clock that jumps to the end of each sleep, and stops the run after a few sleeps."""

    def __init__(self, sleeps: int) -> None:
        self.now = NOW
        self.sleeps: list[datetime] = []
        self.limit = sleeps

    def sleep_until(self, moment: datetime) -> None:
        self.sleeps.append(moment)
        if len(self.sleeps) >= self.limit:
            raise KeyboardInterrupt
        self.now = max(self.now, moment)


class Harness:
    """A context of fakes, what it printed, and the probes it will answer."""

    def __init__(self, tmp_path: Path, *, sleeps: int = 3) -> None:
        wrapper = tmp_path / "python"
        child = ROOT / "tests" / "_fakes" / "live_child.py"
        wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{child}" "$@"\n')
        wrapper.chmod(0o755)
        self.clock = Clock(sleeps)
        self.lines: list[str] = []
        self.probes: list[ProbeResult] = []
        self.environ = {
            "MASSIVE_API_KEY": KEY,
            "MASSIVE_DELAYED_API_KEY": DELAYED_KEY,
            "MASSIVE_S3_ACCESS_KEY_ID": S3_PAIR[0],
            "MASSIVE_S3_SECRET_ACCESS_KEY": S3_PAIR[1],
            "FAKE_LIVE_CHILD": "{}",
        }
        self.awake: list[str] = []
        root = tmp_path / "root"
        root.mkdir()
        self.ctx = RunContext(
            out=tmp_path / "out",
            root=root,
            environ=self.environ,
            secrets=SecretFilter.from_environment(self.environ),
            commit="c1",
            probe=self.probe,
            machine=self.machine,
            keep_awake=self.keep_awake,
            now=lambda: self.clock.now,
            sleep_until=self.sleep_until,
            python=str(wrapper),
            script=Path("live_run.py"),
            echo=self.lines.append,
            read_enter=lambda: "",
            stop_grace=1.0,
        )

    def sleep_until(self, moment: datetime) -> None:
        self.clock.sleep_until(moment)  # the clock a test sets now, not the first one

    def probe(self) -> ProbeResult:
        if not self.probes:
            return probed(BASIC)
        return self.probes.pop(0)

    def machine(self, folder: Path, offset: float | None) -> tuple[list[Item], dict[str, object]]:
        items = [Item("clock", OK, f"{offset} s"), Item("disk", OK, str(folder))]
        return items, {"commit": "c1", "items": [vars(i) for i in items], "echo": KEY}

    def keep_awake(self) -> Callable[[], None]:
        self.awake.append("on")
        return lambda: self.awake.append("off")

    def script(self, **behavior: object) -> None:
        self.environ["FAKE_LIVE_CHILD"] = json.dumps(behavior)

    def state(self) -> RunState:
        state = run_state.load(self.ctx.out)
        assert state is not None
        return state


def probed(
    tiers: dict[str, str] | None,
    *,
    flat_files: list[str] | None = None,
    error: str = "",
    key_rejected: bool = False,
    clock_offset: float | None = None,
) -> ProbeResult:
    return ProbeResult(
        when="2026-11-04T17:00:00Z",
        tiers=tiers,
        flat_files=flat_files or [],
        history_start=dict.fromkeys(tiers or {}, "2024-11-04"),
        clock_offset=clock_offset,
        key_rejected=key_rejected,
        error=error,
    )


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


def _every_file(folder: Path) -> Iterator[tuple[Path, bytes]]:
    for path in sorted(folder.rglob("*")):
        if path.is_file():
            yield path, path.read_bytes()


# --- the lock ---------------------------------------------------------------------------------


def test_the_lock_refuses_a_second_holder(tmp_path: Path) -> None:
    assert not live_run.is_locked(tmp_path)
    with live_run.held_lock(tmp_path):
        assert live_run.is_locked(tmp_path)
        with pytest.raises(RunRefusedError, match="stop it first"), live_run.held_lock(tmp_path):
            pass
    assert not live_run.is_locked(tmp_path)


# --- probing ----------------------------------------------------------------------------------


def test_a_probe_is_recorded_and_its_subscriptions_adopted(harness: Harness) -> None:
    state, memory = RunState(commit="c1"), RunMemory()
    harness.ctx.out.mkdir()
    result = probed(
        BASIC | {"stocks": "starter"}, flat_files=["us_stocks_sip/day_aggs_v1"], error=f"echo {KEY}"
    )
    live_run.apply_probe(harness.ctx, state, memory, result=result)
    assert state.current == "e1"
    assert state.stream_day == "2026-11-05"
    assert memory.not_before == NOW + live_run.BASIC_PAUSE
    assert memory.latest_probe is not None
    [stored] = (harness.ctx.out / "probes").glob("*.json")
    assert KEY not in stored.read_text()
    csv_text = (harness.ctx.out / "probes" / "entitlements.csv").read_text()
    assert "stocks=starter" in csv_text
    assert "us_stocks_sip/day_aggs_v1" in csv_text
    assert any("stream day planned for 2026-11-05" in line for line in harness.lines)


def test_a_probe_that_cannot_tell_keeps_basic_and_counts_a_retry(harness: Harness) -> None:
    state, memory = RunState(commit="c1"), RunMemory()
    live_run.apply_probe(harness.ctx, state, memory, result=probed(None, error="HTTP 503"))
    assert state.entitlements[state.current]["tiers"] == BASIC
    assert memory.retries == 1
    assert state.stream_day == ""
    live_run.apply_probe(harness.ctx, state, memory, result=probed(None, error="HTTP 503"))
    assert (state.current, memory.retries) == ("e1", 2)


def test_a_rejected_key_stops_the_program(harness: Harness) -> None:
    with pytest.raises(RunRefusedError, match="the API key was rejected"):
        live_run.apply_probe(
            harness.ctx, RunState(commit="c1"), RunMemory(), result=probed(None, key_rejected=True)
        )


def test_flat_files_without_the_s3_pair_are_reminded_once_a_day(harness: Harness) -> None:
    del harness.environ["MASSIVE_S3_ACCESS_KEY_ID"]
    del harness.environ["MASSIVE_S3_SECRET_ACCESS_KEY"]
    state, memory = RunState(commit="c1"), RunMemory()
    for _ in range(2):
        live_run.apply_probe(
            harness.ctx, state, memory, result=probed(BASIC | {"options": "starter"})
        )
    reminders = [line for line in harness.lines if "the S3 pair is not set" in line]
    assert len(reminders) == 1
    assert "Options starter includes flat files" in reminders[0]
    live_run.apply_probe(harness.ctx, state, RunMemory(), result=probed(BASIC))
    assert len([line for line in harness.lines if "the S3 pair is not set" in line]) == 1


# --- check ------------------------------------------------------------------------------------


def test_check_prints_the_machine_and_the_probe_and_writes_nothing(harness: Harness) -> None:
    harness.probes = [probed(BASIC, clock_offset=0.4)]
    assert live_run.command_check(harness.ctx) == 0
    assert harness.lines[0] == "OK   clock: 0.4 s"
    assert "     stocks: basic, history from 2024-11-04" in harness.lines
    assert harness.lines[-1] == "ready: yes"
    assert not harness.ctx.out.exists()


def test_check_says_not_ready_for_a_failed_item_or_a_rejected_key(harness: Harness) -> None:
    def failing(_folder: Path, _offset: float | None) -> tuple[list[Item], dict[str, object]]:
        return [Item("java", FAIL, "missing")], {}

    harness.ctx.machine = failing
    harness.probes = [probed(None, error="HTTP 429")]
    assert live_run.command_check(harness.ctx) == 1
    assert "WARN probe: could not tell the subscriptions (HTTP 429)" in harness.lines
    harness.lines.clear()
    harness.probes = [probed(None, key_rejected=True)]
    assert live_run.command_check(harness.ctx) == 1
    assert "FAIL key: the API key was rejected: copy it again from Massive's dashboard" in (
        harness.lines
    )


# --- run --------------------------------------------------------------------------------------

PASS = [{"phase": "setup", "outcome": "passed"}, {"phase": "call", "outcome": "passed"}]


def test_a_run_any_time_runs_every_group_it_can_then_idles_and_packs(harness: Harness) -> None:
    harness.script(
        print=[
            f"key {KEY} delayed {DELAYED_KEY} id {S3_PAIR[0]} secret {S3_PAIR[1]}",
            f"GET /v3?apiKey={KEY}",
        ],
        collection={NODE: {"items": ["21"], "requests": 3}},
        outcomes={NODE: PASS},
        results=[{"name": "market-status", "status": "passed", "reason": ""}],
    )
    assert live_run.command_run(harness.ctx, any_time=True) == live_run.INTERRUPTED
    state = harness.state()
    groups = [a.group for a in state.attempts]
    assert groups == ["evidence", "smoke", "responses", "limits-ws", "limits-second-connection"]
    assert check_of(state, "evidence", NODE).status == "passed"
    assert check_of(state, "responses", "market-status").status == "passed"
    assert harness.awake == ["on", "off"]
    assert (harness.ctx.out / "preflight" / "environment.json").exists()
    assert list(harness.ctx.out.glob("live-results-2026-11-04.zip"))
    command = f"uv run python scripts/live_run.py run --out {harness.ctx.out}"
    resume = f"interrupted; to resume, run: {command}"
    assert harness.lines[-1].endswith(resume)  # without --any-time, which ran once
    state = harness.state()
    assert state.any_time_used
    assert set(state.fingerprints) == {
        "MASSIVE_API_KEY",
        "MASSIVE_DELAYED_API_KEY",
        "MASSIVE_S3_ACCESS_KEY_ID",
        "MASSIVE_S3_SECRET_ACCESS_KEY",
    }
    assert any(
        "idle: nothing more to run until Wed 16:15 EST (limits-ws)" in line
        for line in harness.lines
    )
    secrets = [s.encode() for s in (KEY, DELAYED_KEY, *S3_PAIR)]
    for path, data in _every_file(harness.ctx.out):
        assert not any(secret in data for secret in secrets), path
    printed = "\n".join(harness.lines)
    assert not any(secret.decode() in printed for secret in secrets)


def test_a_run_resumes_and_refuses_another_commit(harness: Harness) -> None:
    harness.script(collection={})
    live_run.command_run(harness.ctx)
    harness.clock = Clock(1)
    assert live_run.command_run(harness.ctx) == live_run.INTERRUPTED
    assert any(line.endswith("resuming") for line in harness.lines)
    harness.ctx.commit = "c2"
    with pytest.raises(
        RunRefusedError, match=r"started on commit c1: to resume, run `git -C .* checkout c1`"
    ):
        live_run.command_run(harness.ctx)


def test_redo_makes_a_groups_checks_due_again(harness: Harness) -> None:
    harness.ctx.out.mkdir()
    state = RunState(commit="c1")
    state.current = entitlement_id(state, BASIC, [])
    check = check_of(state, "smoke", NODE)
    for _ in range(2):
        record(state, check, status="failed", reason="call: AssertionError", day="2026-11-03")
    run_state.save(harness.ctx.out, state)
    harness.clock = Clock(1)
    live_run.command_run(harness.ctx, redo=("smoke",))
    assert (check_of(harness.state(), "smoke", NODE).failures) == 0


def test_a_probe_step_reprobes_during_a_run(harness: Harness) -> None:
    harness.clock = Clock(3)
    harness.clock.now = datetime(2026, 11, 7, 12, tzinfo=NEW_YORK)  # a Saturday: nothing runs
    harness.probes = [probed(BASIC), probed(BASIC)]
    live_run.command_run(harness.ctx)
    assert harness.clock.sleeps[1] == datetime(2026, 11, 9, 8, tzinfo=NEW_YORK)
    assert not harness.probes  # the Monday probe ran after the first sleep


def test_an_idle_pack_that_finds_a_secret_says_so(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness.script(collection={NODE: {"items": [], "requests": 1}}, outcomes={NODE: PASS})
    harness.clock = Clock(2)

    def refuse(*_args: object, **_options: object) -> tuple[None, list[str]]:
        return None, ["run.log: line 1"]

    monkeypatch.setattr(results, "pack", refuse)
    live_run.command_run(harness.ctx, any_time=True)
    assert any(line.endswith(f"{live_run.PACK_REFUSED} run.log: line 1") for line in harness.lines)


# --- status and pack --------------------------------------------------------------------------


def test_status_without_a_run(harness: Harness) -> None:
    assert live_run.command_status(harness.ctx) == 1
    assert harness.lines == [f"no run in {harness.ctx.out}"]


def test_status_shows_the_latest_probe_that_told_the_tiers(harness: Harness) -> None:
    state, memory = RunState(commit="c1"), RunMemory()
    live_run.apply_probe(harness.ctx, state, memory, result=probed(BASIC))
    unknown = probed(None, error="503")
    unknown.when = "2026-11-04T18:00:00Z"
    live_run.apply_probe(harness.ctx, state, memory, result=unknown)
    harness.lines.clear()
    assert live_run.command_status(harness.ctx) == 0
    assert harness.lines[0] == f"latest probe: {probed(BASIC).when}"


def test_status_before_any_probe_says_so(harness: Harness) -> None:
    harness.ctx.out.mkdir()
    run_state.save(harness.ctx.out, RunState(commit="c1"))
    assert live_run.command_status(harness.ctx) == 0
    assert harness.lines[0] == "latest probe: none yet"


def test_pack_refuses_while_a_run_holds_the_folder(harness: Harness) -> None:
    with live_run.held_lock(harness.ctx.out):
        assert live_run.command_pack(harness.ctx) == 1
    assert "stop it with Control-C first" in harness.lines[0]


def test_pack_refuses_without_a_run(harness: Harness) -> None:
    assert live_run.command_pack(harness.ctx) == 1
    assert harness.lines[-1] == f"no run in {harness.ctx.out}"


def _saved_run(harness: Harness) -> RunState:
    harness.ctx.out.mkdir()
    state = RunState(commit="c1", salt="0f" * 16)
    state.fingerprints = fingerprints(harness.environ, state.salt)
    run_state.save(harness.ctx.out, state)
    return state


def test_pack_writes_the_zip_or_names_every_secret_found(harness: Harness) -> None:
    _saved_run(harness)
    assert live_run.command_pack(harness.ctx) == 0
    assert harness.lines[-1] == f"wrote {harness.ctx.out / 'live-results-2026-11-04.zip'}"
    (harness.ctx.out / "leak.txt").write_text(f"{KEY}\n")
    assert live_run.command_pack(harness.ctx) == 1
    assert harness.lines[-2:] == [live_run.PACK_REFUSED, "  leak.txt: line 1"]


@pytest.mark.parametrize(
    ("change", "detail"),
    [
        ({"MASSIVE_API_KEY": ""}, "not set: MASSIVE_API_KEY"),
        (
            {"MASSIVE_DELAYED_API_KEY": "PLANTED-another-delayed"},
            "a different value: MASSIVE_DELAYED_API_KEY",
        ),
    ],
)
def test_pack_needs_exactly_the_secrets_the_run_used(
    *, harness: Harness, change: dict[str, str], detail: str
) -> None:
    _saved_run(harness)
    harness.environ.update(change)
    assert live_run.command_pack(harness.ctx) == 1
    assert harness.lines[-1].startswith("pack: set exactly the secrets the run used")
    assert detail in harness.lines[-1]
    assert not list(harness.ctx.out.glob("*.zip"))


def test_pack_refuses_a_secret_the_run_did_not_have(harness: Harness) -> None:
    state = _saved_run(harness)
    del state.fingerprints["MASSIVE_DELAYED_API_KEY"]
    run_state.save(harness.ctx.out, state)
    assert live_run.command_pack(harness.ctx) == 1
    assert "not used by the run: MASSIVE_DELAYED_API_KEY" in harness.lines[-1]


def test_pack_refuses_a_folder_whose_run_recorded_no_secrets(harness: Harness) -> None:
    harness.ctx.out.mkdir()
    run_state.save(harness.ctx.out, RunState(commit="c1"))
    assert live_run.command_pack(harness.ctx) == 1
    assert harness.lines[-1].endswith("no run recorded them")


def test_pack_waits_for_disk_space(harness: Harness) -> None:
    _saved_run(harness)
    harness.ctx.free_disk = lambda _folder: 0
    assert live_run.command_pack(harness.ctx) == 1
    assert harness.lines[-1].startswith("pack: the zip needs about 0 MB, and 0 MB is free")


def test_an_idle_pack_waits_for_disk_space(harness: Harness) -> None:
    harness.script(collection={NODE: {"items": [], "requests": 1}}, outcomes={NODE: PASS})
    harness.clock = Clock(2)
    harness.ctx.free_disk = lambda _folder: 0
    live_run.command_run(harness.ctx, any_time=True)
    assert any("pack: the zip needs about" in line for line in harness.lines)
    assert not list(harness.ctx.out.glob("*.zip"))


# --- robustness -------------------------------------------------------------------------------


def test_every_command_that_probes_needs_the_api_key_first(harness: Harness) -> None:
    harness.environ["MASSIVE_API_KEY"] = " "
    commands = (
        live_run.command_check,
        live_run.command_run,
        lambda ctx: live_run.command_upgrade_check(ctx, "stocks"),
    )
    for command in commands:
        with pytest.raises(RunRefusedError, match="MASSIVE_API_KEY is not set in this terminal"):
            command(harness.ctx)
    assert not harness.ctx.out.exists()


def test_a_probe_that_raises_is_an_unknown_answer(harness: Harness) -> None:
    def broken() -> ProbeResult:
        raise RuntimeError("the network is down")

    harness.ctx.probe = broken
    result = live_run.safe_probe(harness.ctx)
    assert (result.tiers, result.error) == (
        None,
        "the probe failed: RuntimeError: the network is down",
    )


def test_a_step_that_raises_is_logged_and_retried(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    failures = [OSError(28, f"No space left on device near {KEY}")]
    real_step = live_run._step

    def step(ctx: RunContext, state: RunState, memory: RunMemory) -> None:
        if failures:
            raise failures.pop()
        real_step(ctx, state, memory)

    monkeypatch.setattr(live_run, "_step", step)
    harness.clock = Clock(2)
    assert live_run.command_run(harness.ctx) == live_run.INTERRUPTED
    assert harness.clock.sleeps[0] == NOW + live_run.ERROR_BACKOFF
    log = (harness.ctx.out / "run.log").read_text()
    assert "a step failed; trying again in 5 minutes" in log
    assert "No space left on device near ***" in log
    assert KEY not in log


def test_a_state_that_cannot_be_saved_is_reported_not_raised(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def full(*_args: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(run_state, "save", full)
    live_run._save_quietly(harness.ctx, RunState(commit="c1"))
    assert harness.lines == ["could not save the state: [Errno 28] No space left on device"]


@pytest.mark.parametrize("number", [signal.SIGHUP, signal.SIGTERM])
def test_a_hang_up_or_termination_stops_the_run_like_control_c(
    harness: Harness, number: signal.Signals
) -> None:
    def hang_up(_moment: datetime) -> None:
        signal.raise_signal(number)

    before = signal.getsignal(number)
    harness.ctx.sleep_until = hang_up
    assert live_run.command_run(harness.ctx) == live_run.INTERRUPTED
    assert signal.getsignal(number) is before  # restored once the run ends


def test_any_time_is_used_once_per_folder(harness: Harness) -> None:
    harness.script(collection={})
    harness.clock = Clock(1)
    live_run.command_run(harness.ctx, any_time=True)
    state = harness.state()
    state.any_time = []
    run_state.save(harness.ctx.out, state)
    harness.clock = Clock(1)
    live_run.command_run(harness.ctx, any_time=True)
    assert harness.state().any_time == []
    assert any("--any-time was used before for this folder" in line for line in harness.lines)


# --- the upgrade check ------------------------------------------------------------------------


def test_the_upgrade_check_refuses_a_subscription_not_on_basic(harness: Harness) -> None:
    harness.probes = [probed(BASIC | {"stocks": "starter"})]
    with pytest.raises(RunRefusedError, match="stocks is on starter, not Basic"):
        live_run.command_upgrade_check(harness.ctx, "stocks")


@pytest.mark.parametrize(
    ("result", "status", "code"),
    [
        ({"name": "stocks", "status": "passed", "reason": ""}, "passed", 0),
        ({"name": "stocks", "status": "failed", "reason": "library paced"}, "failed", 1),
        ({"name": "stocks", "status": "failed", "reason": live_run.REHEARSAL}, "not run", 0),
    ],
)
def test_the_upgrade_check_passes_enter_to_its_child_and_records_the_verdict(
    *, harness: Harness, result: dict[str, str], status: str, code: int
) -> None:
    entered: list[str] = []

    def enter() -> str:
        entered.append("enter")
        return ""

    harness.ctx.read_enter = enter
    harness.script(prompt="Now upgrade stocks on the dashboard, then press Enter", results=[result])
    assert live_run.command_upgrade_check(harness.ctx, "stocks") == code
    assert entered == ["enter"]
    assert check_of(harness.state(), "upgrade-check", "stocks").status == status
    request = json.loads(
        (harness.ctx.out / "upgrade-check" / "2026-11-04-1" / "request.json").read_text()
    )
    assert request["due"] == ["stocks"]


def test_an_upgrade_check_that_reports_nothing_is_an_error(harness: Harness) -> None:
    harness.script(exit=1)
    assert live_run.command_upgrade_check(harness.ctx, "indices") == 1
    check = check_of(harness.state(), "upgrade-check", "indices")
    assert (check.status, check.reason) == ("error", "the upgrade check reported nothing")


def test_control_c_during_the_upgrade_check_records_nothing(harness: Harness) -> None:
    def interrupt(line: str) -> None:
        harness.lines.append(line)
        if line.endswith("hanging"):
            _thread.interrupt_main()

    harness.ctx.echo = interrupt
    harness.script(hang=True)
    assert live_run.command_upgrade_check(harness.ctx, "stocks") == live_run.INTERRUPTED
    state = harness.state()
    assert state.attempts[-1].status == "interrupted"
    assert check_of(state, "upgrade-check", "stocks").status == "not run"
    assert harness.lines[-1] == "upgrade check interrupted; nothing recorded"


def test_the_pause_after_a_basic_probe_holds_the_first_group(harness: Harness) -> None:
    harness.clock = Clock(1)
    live_run.command_run(harness.ctx)
    assert harness.clock.sleeps == [NOW + live_run.BASIC_PAUSE]
    assert NOW + timedelta(seconds=65) == harness.clock.sleeps[0]


def test_no_pause_follows_a_probe_when_nothing_is_on_basic(harness: Harness) -> None:
    memory = RunMemory()
    paid = dict.fromkeys(BASIC, "starter")
    live_run.apply_probe(harness.ctx, RunState(commit="c1"), memory, result=probed(paid))
    assert memory.not_before is None
