"""Tests of the tester's setup check (`tests/_live/tester/setup_check.py`).

The commands the check runs are answered by a stand-in for `run_command`, except where a test
runs the inspection with this interpreter for real, and the probe is a made-up result, so
nothing reaches Massive or Docker.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from tests._live.live_run.children import run_command
from tests._live.live_run.machine_checks import FAIL, OK, WARN, Item
from tests._live.live_run.run_probe import ProbeResult
from tests._live.tester import setup_check

ROOT = Path(__file__).resolve().parents[3]
COMMIT = "a" * 40
ACCEPTED = ProbeResult(
    when="2026-10-30T15:00:00Z",
    tiers={"stocks": "basic", "options": "basic"},
    history_start={"stocks": "2024-10-30", "options": "2024-10-30"},
    clock_offset=0.2,
)


def _console(commit: str = COMMIT) -> str:
    found = {
        "deephaven-massive": "0.1.0",
        "deephaven-server": "42.7",
        "deephaven-core": "42.7",
        "commit": commit,
    }
    return json.dumps(found)


class Commands:
    """Answers the commands the check runs, and records them."""

    def __init__(self, answers: dict[str, str]) -> None:
        self.answers = answers
        self.ran: list[list[str]] = []

    def __call__(self, argv: list[str]) -> str:
        self.ran.append(argv)
        for word, answer in self.answers.items():
            if word in argv:
                return answer
        return ""


def test_the_inspection_reports_this_interpreters_install() -> None:
    found = json.loads(run_command([sys.executable, "-c", setup_check.INSPECT_CONSOLE]))
    assert found["deephaven-massive"]
    assert found["deephaven-core"]
    assert found["commit"] == ""  # an editable install from the checkout records no commit


def test_tag_item_names_the_tag_or_warns_without_one() -> None:
    on_tag = setup_check.tag_item(ROOT, Commands({"describe": "v0.1.0rc1\n"}))
    assert on_tag == Item("version", OK, "the checkout is on v0.1.0rc1")
    assert setup_check.tag_item(ROOT, Commands({})).verdict == WARN


def test_console_command_runs_the_console_python_or_the_image() -> None:
    python = setup_check.console_command(Path("/v/bin/python"), "")
    assert python == ["/v/bin/python", "-c", setup_check.INSPECT_CONSOLE]
    docker = setup_check.console_command(None, "massive-test")
    assert docker[-4:] == ["python", "massive-test", "-c", setup_check.INSPECT_CONSOLE]
    assert docker[1:4] == ["run", "--rm", "--entrypoint"]


def test_image_commit_reads_the_label_or_nothing() -> None:
    labelled = Commands({"inspect": f"{COMMIT}\n"})
    assert setup_check.image_commit("img", labelled) == COMMIT
    assert setup_check.COMMIT_LABEL in labelled.ran[0][-2]
    assert setup_check.image_commit("img", Commands({"inspect": "<no value>\n"})) == ""


def test_console_items_compare_the_commits() -> None:
    found = json.loads(_console())
    same = setup_check.console_items(found, checkout_commit=COMMIT, where="venv")
    assert [i.verdict for i in same] == [OK, OK]
    assert same[0].detail == "deephaven-massive 0.1.0, deephaven-core 42.7, in venv"
    other = setup_check.console_items(found, checkout_commit="b" * 40, where="venv")
    assert other[1].verdict == FAIL
    assert "install the same version in both" in other[1].detail
    found["commit"] = ""
    unknown = setup_check.console_items(found, checkout_commit=COMMIT, where="venv")
    assert unknown[1].verdict == WARN


def test_console_items_fail_without_the_install_or_its_parts() -> None:
    missing = setup_check.console_items(None, checkout_commit=COMMIT, where="/v/bin/python")
    assert missing == [
        Item(
            "console",
            FAIL,
            "could not run /v/bin/python: install the console setup as the guide says",
        )
    ]
    found = json.loads(_console())
    found["deephaven-massive"] = None
    no_library = setup_check.console_items(found, checkout_commit=COMMIT, where="v")
    assert no_library[0].detail == "v lacks deephaven-massive: install it as the guide says"
    found = json.loads(_console())
    found["deephaven-server"] = found["deephaven-core"] = None
    no_server = setup_check.console_items(found, checkout_commit=COMMIT, where="v")
    assert no_server[0].detail == "v lacks the Deephaven server: install it as the guide says"


def test_probe_items_describe_the_key_and_its_plans() -> None:
    items = setup_check.probe_items(ACCEPTED)
    assert [i.name for i in items] == ["key", "plan stocks", "plan options", "flat files"]
    assert items[1].detail == "basic, history from 2024-10-30"
    assert items[3].detail == "none"
    rejected = ProbeResult(when="t", key_rejected=True)
    assert setup_check.probe_items(rejected)[0].verdict == FAIL
    unknown = ProbeResult(when="t", error="HTTP 503")
    assert setup_check.probe_items(unknown) == [
        Item("key", WARN, "could not tell the plans (HTTP 503); try again soon")
    ]


def test_check_setup_checks_the_machine_both_installs_and_the_key(tmp_path: Path) -> None:
    checkout = run_command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()
    commands = Commands({"-c": f"noise\n{_console(checkout)}\n", "describe": "v1\n"})
    items = setup_check.check_setup(
        root=ROOT,
        folder=tmp_path,
        environ={"MASSIVE_API_KEY": "k"},
        console_python=Path("/v/bin/python"),
        docker_image="",
        run=commands,
        probe=lambda: ACCEPTED,
    )
    by_name = {item.name: item for item in items}
    assert by_name["java"].name == "java"
    assert by_name["version"].detail == "the checkout is on v1"
    assert by_name["same commit"] == Item("same commit", OK, f"both are {checkout}")
    assert by_name["key"].verdict == OK


def test_check_setup_reads_a_docker_images_commit_from_its_label(tmp_path: Path) -> None:
    commands = Commands({"-c": _console(""), "inspect": f"{COMMIT}\n"})
    items = setup_check.check_setup(
        root=ROOT,
        folder=tmp_path,
        environ={"MASSIVE_API_KEY": "k"},
        console_python=None,
        docker_image="massive-test",
        run=commands,
        probe=lambda: ACCEPTED,
    )
    same = next(i for i in items if i.name == "same commit")
    assert COMMIT in same.detail
    assert next(i for i in items if i.name == "console").detail.endswith("in massive-test")


def test_check_setup_fails_when_the_console_cannot_run(tmp_path: Path) -> None:
    items = setup_check.check_setup(
        root=ROOT,
        folder=tmp_path,
        environ={"MASSIVE_API_KEY": "k"},
        console_python=Path("/nowhere/python"),
        docker_image="",
        run=Commands({}),
        probe=lambda: ACCEPTED,
    )
    console = next(i for i in items if i.name == "console")
    assert console.verdict == FAIL
