"""Tests of the live run's context (`tests/_live/live_run/context.py`): its log and defaults."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from tests._live.calendar import NEW_YORK
from tests._live.live_run.context import RunContext, RunMemory
from tests._live.live_run.secret_scrub import SecretFilter

if TYPE_CHECKING:
    from pathlib import Path

    from tests._live.live_run.machine_checks import Item
    from tests._live.live_run.run_probe import ProbeResult

KEY = "PLANTED-context-key-0a7f"


def _probe() -> ProbeResult:
    pytest.fail("no probe expected")


def _machine(_folder: Path, _offset: float | None) -> tuple[list[Item], dict[str, object]]:
    pytest.fail("no machine check expected")


def _context(out: Path) -> RunContext:
    return RunContext(
        out=out,
        root=out,
        environ={},
        secrets=SecretFilter((KEY,)),
        commit="c",
        probe=_probe,
        machine=_machine,
        now=lambda: datetime(2026, 11, 4, 9, 5, tzinfo=NEW_YORK),
    )


def test_say_stamps_cleans_and_appends_each_line_to_the_run_log(tmp_path: Path) -> None:
    printed: list[str] = []
    ctx = _context(tmp_path / "out")
    ctx.echo = printed.append
    ctx.say(f"probing with {KEY}")
    ctx.say("done")
    assert printed == ["09:05 EST probing with ***", "09:05 EST done"]
    assert (tmp_path / "out" / "run.log").read_text() == "\n".join(printed) + "\n"


def test_the_defaults_print_sleep_and_keep_awake_nothing(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    ctx.echo("not shown")
    ctx.sleep_until(ctx.now())
    ctx.keep_awake()()
    assert list(tmp_path.iterdir()) == []


def test_a_runs_memory_starts_empty() -> None:
    memory = RunMemory()
    assert (memory.probed_at, memory.retries, memory.not_before) == (None, 0, None)
