"""Tests of the live run's child commands (`tests/_live/live_run/child_commands.py`).

They run in this test process, whose JVM stands in for the server the script starts; the group
is a stand-in, and the probe a fake, so nothing reaches Massive.
"""

from __future__ import annotations

import json
import os
import threading
from typing import TYPE_CHECKING, Any

from tests._live.live_run import child_commands, group_child
from tests._live.live_run.probe_result import ProbeResult, read_result
from tests._live.live_run.run_probe import Credentials

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from pathlib import Path

    import pytest

    from deephaven_massive import MassiveSession

KEY = "PLANTED-child-key-2d7a"


def test_the_group_child_watches_its_parent_then_runs_its_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "request.json").write_text(
        json.dumps({"group": "responses", "tiers": {"stocks": "basic"}})
    )
    events: list[str] = []
    monkeypatch.setattr(
        group_child, "run_group", lambda _out, attempt, **_options: events.append(attempt)
    )
    code = child_commands.main(
        ["_group", "--out", str(tmp_path), "--attempt", "a", "--token", "t"],
        environ={},
        parent=4321,
        watch=lambda parent: events.append(f"watching {parent}"),
    )
    assert code == 0
    assert events == ["watching 4321", "a"]


def test_the_group_childs_sessions_declare_the_basic_and_delayed_subscriptions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    request = {"group": "upgrade-check", "tiers": {"stocks": "basic", "options": "starter"}}
    (tmp_path / "a" / "request.json").write_text(json.dumps(request))
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    seen: dict[str, Callable[..., Any]] = {}

    def run_group(
        _out: Path,
        _attempt: str,
        *,
        open_session: Callable[[], MassiveSession],
        tier_of: Callable[[str], str | None],
    ) -> None:
        seen["open_session"], seen["tier_of"] = open_session, tier_of

    tiers: list[dict[str, str] | None] = [{"stocks": "starter"}, None]

    def probe_key(_credentials: Credentials, *, now: datetime) -> ProbeResult:
        return ProbeResult(when="", tiers=tiers.pop(0))

    monkeypatch.setattr(group_child, "run_group", run_group)
    monkeypatch.setattr(child_commands, "probe_key", probe_key)
    child_commands.run_group_child(
        tmp_path,
        "a",
        environ={"MASSIVE_API_KEY": KEY},
    )
    open_session = seen["open_session"]
    with open_session() as session:
        assert {str(s) for s in session._core.config.rest_basic_subscriptions} == {"stocks"}
        assert {str(s) for s in session._core.config.delayed_subscriptions} == {"options"}
    assert seen["tier_of"]("stocks") == "starter"
    assert seen["tier_of"]("stocks") is None


def test_a_group_child_ends_once_its_parent_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    parents = [100, 100, 1]
    monkeypatch.setattr(os, "getppid", lambda: parents.pop(0))
    waits: list[float] = []
    ended: list[str] = []

    def wait(seconds: float) -> bool:
        waits.append(seconds)
        return False

    child_commands.watch_parent(100, orphaned=lambda: ended.append("ended"), wait=wait)
    assert (waits, ended) == ([child_commands.PARENT_CHECK, child_commands.PARENT_CHECK], ["ended"])


def test_the_parent_watch_runs_on_a_thread_of_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[int] = []
    done = threading.Event()

    def watch_parent(parent: int, **_options: object) -> None:
        seen.append(parent)
        done.set()

    monkeypatch.setattr(child_commands, "watch_parent", watch_parent)
    child_commands.start_parent_watch(4321)
    assert done.wait(10.0)
    assert seen == [4321]  # the pid the script read, not the parent the child has now


def test_the_probe_child_writes_its_probe_for_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[Credentials] = []

    def probe_key(credentials: Credentials, *, now: datetime) -> ProbeResult:
        seen.append(credentials)
        return ProbeResult(when="2026-11-04T17:00:00Z", tiers={"stocks": "starter"})

    monkeypatch.setattr(child_commands, "probe_key", probe_key)
    result = tmp_path / "probe.json"
    watched: list[int] = []
    code = child_commands.main(
        ["_probe", "--result", str(result)],
        environ={"MASSIVE_API_KEY": KEY},
        parent=4321,
        watch=watched.append,
    )
    assert code == 0
    assert watched == [4321]  # a probe child watches its parent too
    assert seen == [Credentials(api_key=KEY, s3_pair=None)]
    assert read_result(result).tiers == {"stocks": "starter"}


def test_a_refused_child_command_line_returns_its_status(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The script exits through os._exit, so argparse's SystemExit must come back as a status.
    assert child_commands.main(["_probe"], parent=1, environ={}) == 2
    assert "--result" in capsys.readouterr().err
