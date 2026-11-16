"""Tests of the live run's outcomes plugin (`tests/_live/live_run/outcomes.py`).

The hooks are called directly with stand-in items and reports, and the plugin is also loaded
into a real pytest child on a small test file outside the repository, so neither the
repository's `conftest.py` nor a Massive key is ever involved: the child writes the collection
and one line per phase, and the program's readers turn those into check statuses.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from tests._live.live_run import outcomes

ROOT = Path(__file__).resolve().parents[3]


@dataclass
class _Config:
    options: dict[str, str] = field(default_factory=dict)

    def getoption(self, name: str) -> str:
        return self.options.get(name, "")


@dataclass
class _Item:
    nodeid: str
    marks: list[pytest.Mark] = field(default_factory=list)
    config: _Config = field(default_factory=_Config)

    def iter_markers(self, name: str) -> list[pytest.Mark]:
        return [m for m in self.marks if m.name == name]

    def get_closest_marker(self, name: str) -> pytest.Mark | None:
        found = self.iter_markers(name)
        return found[0] if found else None


@dataclass
class _Report:
    when: str
    outcome: str
    longrepr: object = None

    @property
    def failed(self) -> bool:
        return self.outcome == "failed"

    @property
    def skipped(self) -> bool:
        return self.outcome == "skipped"


@dataclass
class _ExcInfo:
    typename: str


@dataclass
class _Call:
    excinfo: _ExcInfo | None


def _report_through_hook(item: _Item, report: _Report, call: _Call) -> object:
    hook: Any = outcomes.pytest_runtest_makereport
    wrapper = hook(item, call)
    next(wrapper)
    with pytest.raises(StopIteration) as stopped:
        wrapper.send(report)
    return stopped.value.value


def test_collection_writes_items(tmp_path: Path) -> None:
    target = tmp_path / "collection.json"
    config = _Config({outcomes.COLLECTION_OPTION: str(target)})
    items = [
        _Item("a.py::marked", [pytest.mark.settles("21").mark, pytest.mark.verifies("x").mark]),
        _Item("a.py::plain"),
    ]
    hook: Any = outcomes.pytest_collection_modifyitems
    hook(config, items)
    assert json.loads(target.read_text()) == {
        "a.py::marked": {"items": ["21", "x"]},
        "a.py::plain": {"items": []},
    }


def test_collection_writes_nothing_without_the_option(tmp_path: Path) -> None:
    hook: Any = outcomes.pytest_collection_modifyitems
    hook(_Config(), [_Item("b.py::t")])
    assert list(tmp_path.iterdir()) == []


def test_each_phase_appends_a_line(tmp_path: Path) -> None:
    target = tmp_path / "outcomes.jsonl"
    item = _Item("c.py::t", config=_Config({outcomes.OUTCOMES_OPTION: str(target)}))
    failed = _Report("call", "failed")
    assert _report_through_hook(item, failed, _Call(_ExcInfo("AssertionError"))) is failed
    skipped = _Report("setup", "skipped", ("f.py", 1, "Skipped: refused: HTTP 403: no"))
    _report_through_hook(item, skipped, _Call(None))
    _report_through_hook(item, _Report("teardown", "failed"), _Call(None))
    lines = outcomes.read_outcomes(target)
    assert [(r["phase"], r["outcome"], r["error_type"], r["reason"]) for r in lines] == [
        ("call", "failed", "AssertionError", ""),
        ("setup", "skipped", "", "refused: HTTP 403: no"),
        ("teardown", "failed", "", ""),
    ]


def test_a_phase_writes_nothing_without_the_option() -> None:
    report = _Report("call", "passed")
    assert _report_through_hook(_Item("d.py::t"), report, _Call(None)) is report


def test_a_skip_reason_given_as_text_loses_its_prefix() -> None:
    assert outcomes.skip_reason("Skipped: closed") == "closed"


def test_reading_skips_a_line_cut_short_and_a_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    assert outcomes.read_outcomes(path) == []
    path.write_text('{"nodeid": "x"}\n{"nodeid": ')
    assert outcomes.read_outcomes(path) == [{"nodeid": "x"}]


def _phase(phase: str, outcome: str, *, error_type: str = "", reason: str = "") -> dict[str, str]:
    return {"phase": phase, "outcome": outcome, "error_type": error_type, "reason": reason}


@pytest.mark.parametrize(
    ("phases", "expected"),
    [
        ([_phase("setup", "passed"), _phase("call", "passed")], ("passed", "")),
        ([_phase("setup", "skipped", reason="needs S3")], ("skipped", "needs S3")),
        (
            [_phase("call", "skipped", reason="refused: HTTP 403: not entitled")],
            ("refused", "refused: HTTP 403: not entitled"),
        ),
        (
            [_phase("call", "failed", error_type="EntitlementError")],
            ("failed", "call: EntitlementError"),
        ),
        (
            [_phase("call", "failed", error_type="AssertionError"), _phase("teardown", "failed")],
            ("failed", "call: AssertionError, teardown: failed"),
        ),
        ([_phase("setup", "passed")], ("error", "did not finish")),
    ],
)
def test_phases_make_one_check_status(
    phases: list[dict[str, str]], expected: tuple[str, str]
) -> None:
    assert outcomes.check_outcome(phases) == expected


def test_a_real_pytest_child_records_its_collection_and_phases(tmp_path: Path) -> None:
    (tmp_path / "test_tiny.py").write_text(
        textwrap.dedent(
            """
            import pytest

            @pytest.mark.settles("7")
            def test_passes():
                pass

            def test_skips():
                pytest.skip("refused: HTTP 403: not entitled")

            def test_fails():
                raise KeyError("x")
            """
        )
    )
    collection = tmp_path / "collection.json"
    lines = tmp_path / "outcomes.jsonl"
    env = {k: v for k, v in os.environ.items() if not k.startswith("MASSIVE_")}
    env |= {"PYTHONPATH": str(ROOT)}
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests._live.live_run.outcomes",
            "-p",
            "no:cacheprovider",
            "--live-run-token",
            "abc",
            "--live-collection",
            "collection.json",
            "--live-outcomes",
            "outcomes.jsonl",
            "-q",
            "test_tiny.py",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 1, done.stdout + done.stderr
    assert json.loads(collection.read_text())["test_tiny.py::test_passes"] == {"items": ["7"]}
    by_test: dict[str, list[dict[str, Any]]] = {}
    for line in outcomes.read_outcomes(lines):
        by_test.setdefault(line["nodeid"], []).append(line)
    assert {n: outcomes.check_outcome(p) for n, p in by_test.items()} == {
        "test_tiny.py::test_passes": ("passed", ""),
        "test_tiny.py::test_skips": ("refused", "refused: HTTP 403: not entitled"),
        "test_tiny.py::test_fails": ("failed", "call: KeyError"),
    }
    assert by_test["test_tiny.py::test_passes"][0]["items"] == ["7"]


def test_the_plugin_adds_the_token_and_file_options() -> None:
    added: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class _Parser:
        def addoption(self, *names: object, **options: object) -> None:
            added.append((names, options))

    hook: Any = outcomes.pytest_addoption
    hook(_Parser())
    assert [names for names, _ in added] == [
        ("--live-run-token",),
        (outcomes.OUTCOMES_OPTION,),
        (outcomes.COLLECTION_OPTION,),
    ]
    assert all(options["default"] == "" for _, options in added)
