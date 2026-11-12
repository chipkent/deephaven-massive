"""Tests of the live run's child server (`tests/_live/live_run/server.py`)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, ClassVar

import pytest

from tests._live.live_run import machine_checks, server

if TYPE_CHECKING:
    from pathlib import Path


def test_the_probe_child_gets_the_probe_heap() -> None:
    assert server.jvm_args(["_probe", "--result", "r.json"]) == [f"-Xmx{server.PROBE_HEAP >> 20}m"]


@pytest.mark.parametrize(
    ("group", "memory", "heap"),
    [
        ("responses", 64 << 30, server.SMALL_HEAP),
        ("stream-day", 64 << 30, server.STREAM_HEAP_LIMIT),
        ("throughput", 12 << 30, 6 << 30),
        ("stream-night", 8 << 30, 4 << 30),
        ("stream-day", None, server.SMALL_HEAP),
    ],
)
def test_a_group_child_gets_a_heap_sized_to_its_group_and_its_crash_log_in_the_attempt(
    *, group: str, memory: int | None, heap: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "request.json").write_text(json.dumps({"group": group, "tiers": {}}))
    monkeypatch.setattr(machine_checks, "memory_total", lambda: memory)
    argv = ["_group", "--out", str(tmp_path), "--attempt", "a", "--token", "t"]
    assert server.jvm_args(argv) == [
        f"-Xmx{heap >> 20}m",
        f"-XX:ErrorFile={tmp_path.resolve() / 'a'}/hs_err_pid%p.log",
    ]


@pytest.mark.parametrize(
    ("printed", "problem"),
    [
        ('openjdk version "21.0.2" 2024-01-16', None),
        ("", "FAIL java: Java not found: install JDK 17 or later"),
        ('java version "1.8.0_391"', "FAIL java: Java 8: install JDK 17 or later"),
    ],
)
def test_a_missing_or_old_java_is_reported_before_the_server_starts(
    *, printed: str, problem: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[list[str]] = []

    def run(argv: list[str]) -> str:
        asked.append(argv)
        return printed

    monkeypatch.setattr(server, "run_command", run)
    assert server.java_problem({"JAVA_HOME": "/jdk"}) == problem
    assert asked == [["/jdk/bin/java", "-version"]]


class _Server:
    """Stands in for `deephaven_server.Server`: this process already runs a JVM."""

    started: ClassVar[list[dict[str, object]]] = []

    def __init__(self, **options: object) -> None:
        self.options = options

    def start(self) -> None:
        _Server.started.append(self.options)


@pytest.mark.parametrize(("printed", "starts"), [('openjdk version "21.0.2"', True), ("", False)])
def test_a_child_starts_its_sized_server_only_with_java(
    *, printed: str, starts: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_Server, "started", [])
    monkeypatch.setattr(server, "Server", _Server)
    monkeypatch.setattr(server, "run_command", lambda _argv: printed)
    problem = server.start_server(["_probe", "--result", "r.json"], environ={})
    if starts:
        assert problem is None
        assert _Server.started == [{"port": 0, "jvm_args": [f"-Xmx{server.PROBE_HEAP >> 20}m"]}]
    else:
        assert problem == "FAIL java: Java not found: install JDK 17 or later"
        assert _Server.started == []
