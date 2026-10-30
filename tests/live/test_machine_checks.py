"""Tests of the live run's machine checks (`tests/_live/machine_checks.py`).

Each item's verdict is checked on the values that decide it; the whole check runs once on this
machine and once with every command missing, and its environment record must name only which
secrets are set, never a value.
"""

from __future__ import annotations

import json
import os
import shutil
from importlib import metadata
from pathlib import Path

import pytest

from tests._live import machine_checks
from tests._live.machine_checks import FAIL, GIB, OK, WARN

ROOT = Path(__file__).resolve().parent.parent.parent
PLANTED = "PLANTED-machine-key-8d2e"


@pytest.mark.parametrize(
    ("text", "verdict", "detail"),
    [
        ('openjdk version "17.0.2" 2022-01-18', OK, "Java 17"),
        ('openjdk version "21" 2023-09-19', OK, "Java 21"),
        ('java version "1.8.0_391"', FAIL, "Java 8: install JDK 17 or later"),
        ('openjdk version "11.0.1"', FAIL, "Java 11: install JDK 17 or later"),
        ("", FAIL, "Java not found: install JDK 17 or later"),
    ],
)
def test_java(text: str, verdict: str, detail: str) -> None:
    assert machine_checks.java_item(text) == machine_checks.Item("java", verdict, detail)


@pytest.mark.parametrize(
    ("environ", "verdict"),
    [
        ({"MASSIVE_API_KEY": PLANTED}, OK),
        ({"MASSIVE_API_KEY": " "}, FAIL),
        ({"MASSIVE_API_KEY": PLANTED, "MASSIVE_S3_ACCESS_KEY_ID": "a"}, FAIL),
        (
            {
                "MASSIVE_API_KEY": PLANTED,
                "MASSIVE_S3_ACCESS_KEY_ID": "a",
                "MASSIVE_S3_SECRET_ACCESS_KEY": "b",
            },
            OK,
        ),
    ],
)
def test_secrets_are_reported_only_as_set(environ: dict[str, str], verdict: str) -> None:
    item = machine_checks.secrets_item(environ)
    assert item.verdict == verdict
    assert PLANTED not in item.detail


@pytest.mark.parametrize(
    ("offset", "verdict"), [(None, WARN), (0.5, OK), (-5.0, WARN), (31.0, FAIL)]
)
def test_clock(offset: float | None, verdict: str) -> None:
    assert machine_checks.clock_item(offset).verdict == verdict


def test_disk_memory_and_keep_awake() -> None:
    assert machine_checks.disk_item(25 * GIB).verdict == OK
    assert machine_checks.disk_item(5 * GIB).verdict == FAIL
    assert machine_checks.memory_item(None).verdict == WARN
    assert machine_checks.memory_item(4 * GIB).verdict == WARN
    assert machine_checks.memory_item(16 * GIB).detail == "16 GB"
    assert machine_checks.keep_awake_item("Darwin", lambda name: f"/usr/bin/{name}").verdict == OK
    missing = machine_checks.keep_awake_item("Linux", lambda _name: None)
    assert (missing.verdict, missing.detail.split()[0]) == (WARN, "systemd-inhibit")


def test_memory_total_is_none_when_the_system_cannot_tell(monkeypatch: pytest.MonkeyPatch) -> None:
    assert (machine_checks.memory_total() or 0) > 0

    def refuse(_name: str) -> int:
        raise ValueError(_name)

    monkeypatch.setattr(os, "sysconf", refuse)
    assert machine_checks.memory_total() is None


def test_dependency_versions_skip_extras_and_name_missing_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def requires(_name: str) -> list[str]:
        return ["numpy>=2", "pytest; extra == 'test'", "not-installed-package-x>=1"]

    monkeypatch.setattr(metadata, "requires", requires)
    found = machine_checks.dependency_versions()
    assert list(found) == ["deephaven-massive", "numpy", "not-installed-package-x"]
    assert found["not-installed-package-x"] == "not installed"
    monkeypatch.setattr(metadata, "requires", lambda _name: None)
    assert list(machine_checks.dependency_versions()) == ["deephaven-massive"]


def test_the_machine_check_names_secrets_only_as_set(tmp_path: Path) -> None:
    environ = {"MASSIVE_API_KEY": PLANTED, "HTTPS_PROXY": "http://proxy:1"}
    items, environment = machine_checks.check_machine(
        root=ROOT, disk_path=tmp_path, environ=environ, clock_offset=0.1
    )
    assert [i.name for i in items] == [
        "java",
        "uv",
        "commit",
        "secrets",
        "clock",
        "disk",
        "memory",
        "keep-awake",
    ]
    assert environment["set"] == {
        "MASSIVE_API_KEY": True,
        "MASSIVE_DELAYED_API_KEY": False,
        "MASSIVE_S3_ACCESS_KEY_ID": False,
        "MASSIVE_S3_SECRET_ACCESS_KEY": False,
        "HTTPS_PROXY": True,
        "SSL_CERT_FILE": False,
    }
    assert PLANTED not in json.dumps(environment, default=str)
    assert len(str(environment["commit"])) == 40


def test_a_machine_without_the_tools_is_not_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(machine_checks, "run_command", lambda _argv: "")
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    items, environment = machine_checks.check_machine(
        root=ROOT, disk_path=tmp_path, environ={}, clock_offset=None
    )
    verdicts = {i.name: i.verdict for i in items}
    assert verdicts["java"] == verdicts["uv"] == verdicts["commit"] == FAIL
    assert environment["java"] == ""
    assert not machine_checks.ready(items)
    assert machine_checks.ready([machine_checks.Item("x", WARN, "")])
