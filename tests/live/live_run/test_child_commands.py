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
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "request.json").write_text(json.dumps({"group": "responses"}))
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


SETTINGS = """\
delayed_subscriptions: [options]
rest_requests_per_minute: {stocks: 5}
max_outage: 7
"""


def test_a_child_without_a_settings_file_takes_the_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    config = child_commands.session_config({}, declare_limits=True)
    assert (config.delayed_subscriptions, config.rest_requests_per_minute) == (set(), {})


def test_a_childs_sessions_take_the_owners_settings_as_the_live_tests_do(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The owner's delayed listing, limits, and hosts pass through; every other setting not."""
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    settings = tmp_path / "settings.yaml"
    settings.write_text(SETTINGS)
    environ = {"DEEPHAVEN_MASSIVE_CONFIG_FILE": str(settings)}
    config = child_commands.session_config(environ, declare_limits=True)
    assert {s.value for s in config.delayed_subscriptions} == {"options"}
    assert {s.value: n for s, n in config.rest_requests_per_minute.items()} == {"stocks": 5}
    assert config.max_outage != 7
    upgrade = child_commands.session_config(environ, declare_limits=False)
    assert ({s.value for s in upgrade.delayed_subscriptions}, upgrade.rest_requests_per_minute) == (
        {"options"},
        {},
    )


def test_the_group_child_opens_its_sessions_and_names_the_delayed_subscriptions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "request.json").write_text(json.dumps({"group": "upgrade-check"}))
    settings = tmp_path / "settings.yaml"
    settings.write_text(SETTINGS)
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    seen: dict[str, Any] = {}

    def run_group(
        _out: Path,
        _attempt: str,
        *,
        open_session: Callable[[], MassiveSession],
        delayed: frozenset[str],
    ) -> None:
        seen["open_session"], seen["delayed"] = open_session, delayed

    monkeypatch.setattr(group_child, "run_group", run_group)
    child_commands.run_group_child(
        tmp_path, "a", environ={"DEEPHAVEN_MASSIVE_CONFIG_FILE": str(settings)}
    )
    assert seen["delayed"] == frozenset({"options"})
    with seen["open_session"]() as session:
        assert not session._core.config.rest_requests_per_minute  # the upgrade check learns


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
        return ProbeResult(when="2026-11-04T17:00:00Z", clock_offset=0.5)

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
    assert seen == [Credentials(api_key=KEY)]
    assert read_result(result).clock_offset == 0.5


def test_a_refused_child_command_line_returns_its_status(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The script exits through os._exit, so argparse's SystemExit must come back as a status.
    assert child_commands.main(["_probe"], parent=1, environ={}) == 2
    assert "--result" in capsys.readouterr().err
