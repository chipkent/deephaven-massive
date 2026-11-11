from __future__ import annotations

import datetime as dt
import os
import runpy
import sys
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.refresh import __main__ as command
from deephaven_massive._massive.refresh import drift, fetch, generate, render, snapshot
from deephaven_massive._massive.refresh.gates import Gate, Problem, RefreshError

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.skipif(
    not snapshot.DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)
def test_the_plain_run_reports_an_up_to_date_schema(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The run regenerates copies, never the committed files, so this test cannot hide a
    # parity failure by rewriting them.
    committed = {path.name: path.read_bytes() for path in render.SPEC_DIRECTORY.glob("*.json")}
    for name, data in committed.items():
        (tmp_path / name).write_bytes(data)
    monkeypatch.setattr(render, "SPEC_DIRECTORY", tmp_path)
    assert command.main([]) == 0
    assert capsys.readouterr().out == f"{tmp_path} is up to date\n"
    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == committed


def test_the_plain_run_reports_an_update(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # `generate.refresh` writing a changed file is tested with the generator; here only the
    # command's report of it is.
    written = [render.REST_FILE, render.DESCRIPTIONS_FILE]
    monkeypatch.setattr(generate, "refresh", lambda: written)
    assert command.main([]) == 0
    assert capsys.readouterr().out.splitlines() == [f"{path} updated" for path in written]


def test_a_stopped_refresh_prints_the_problems_and_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    problem = Problem(gate=Gate.SHORT_KEY, source="/v1/x", key="p", message="short key")

    def stopped() -> list[Path]:
        raise RefreshError([problem])

    monkeypatch.setattr(generate, "refresh", stopped)
    assert command.main([]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "the refresh stopped on 1 problem(s):\nshort-key /v1/x p: short key\n"


def test_fetch_replaces_the_snapshot(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[Path] = []

    def fetched(directory: Path) -> list[str]:
        calls.append(directory)
        return ["openapi.json", "manifest.json"]

    monkeypatch.setattr(fetch, "fetch", fetched)
    assert command.main(["--fetch"]) == 0
    assert calls == [snapshot.DIRECTORY]
    assert capsys.readouterr().out.splitlines() == [
        f"stored 2 files in {snapshot.DIRECTORY}",
        "now run `python scripts/refresh.py` to regenerate the schemas",
    ]


def test_a_failed_fetch_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def failed(directory: Path) -> list[str]:
        raise fetch.FetchError("GET https://api.massive.com/openapi answered HTTP 503")

    monkeypatch.setattr(fetch, "fetch", failed)
    assert command.main(["--fetch"]) == 1
    assert capsys.readouterr().err == "GET https://api.massive.com/openapi answered HTTP 503\n"


@pytest.mark.parametrize("found", [False, True], ids=["no-drift", "drift"])
def test_check_writes_the_report_to_its_file_and_exits_one_on_drift(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    tmp_path: Path,
    found: bool,
) -> None:
    items = (drift.Drift(section=drift.Section.CHANGES, area="REST", item="/v1/x", detail="new"),)
    report = drift.Report(
        items=items if found else (), fetched=dt.date(2026, 10, 27), bucket_listed=True
    )
    monkeypatch.setattr(drift, "check", lambda: report)
    report_file = tmp_path / "drift.md"
    report_file.write_text("an older report\n")
    assert command.main(["--check", "--report", str(report_file)]) == int(found)
    assert report_file.read_text(encoding="utf-8") == report.text()
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "error",
    [
        fetch.FetchError("GET https://api.massive.com/openapi answered HTTP 503"),
        RefreshError([Problem(gate=Gate.SNAPSHOT, source="", key="x", message="edited")]),
        WireError("HTTP 403 for s3://flatfiles/ at https://files.massive.com", status=403),
        RecursionError("maximum recursion depth exceeded"),
    ],
    ids=["download", "snapshot", "bucket", "recursion"],
)
def test_a_check_that_cannot_be_made_exits_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    tmp_path: Path,
    error: Exception,
) -> None:
    def failed() -> drift.Report:
        raise error

    monkeypatch.setattr(drift, "check", failed)
    report_file = tmp_path / "drift.md"
    assert command.main(["--check", "--report", str(report_file)]) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == f"the drift check could not be made: {error}\n"
    assert not report_file.exists()


@pytest.mark.parametrize("damage", ["missing", "not-json"])
def test_a_check_against_a_damaged_snapshot_exits_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    tmp_path: Path,
    damage: str,
) -> None:
    # The vendored snapshot is read before anything is downloaded, so no network is needed.
    (tmp_path / snapshot.MANIFEST).write_text("{not json" if damage == "not-json" else "{}")
    monkeypatch.setattr(snapshot, "DIRECTORY", tmp_path)
    report_file = tmp_path / "drift.md"
    assert command.main(["--check", "--report", str(report_file)]) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err.startswith("the drift check could not be made: ")
    assert not report_file.exists()


@pytest.mark.parametrize(
    "argv", [["--check"], ["--report", "drift.md"], ["--fetch", "--report", "drift.md"]]
)
def test_check_and_report_go_together(capsys: pytest.CaptureFixture[str], argv: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        command.main(argv)
    assert caught.value.code == 2
    assert "--check and --report go together" in capsys.readouterr().err


def test_check_and_fetch_exclude_each_other(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        command.main(["--check", "--fetch"])
    assert caught.value.code == 2
    assert "not allowed with argument" in capsys.readouterr().err


def test_the_help_names_the_vendored_snapshot(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        command.main(["--help"])
    assert caught.value.code == 0
    assert "vendor/massive/" in " ".join(capsys.readouterr().out.split())


def test_running_the_module_runs_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["refresh", "--help"])
    with pytest.raises(SystemExit) as caught:
        runpy.run_path(command.__file__, run_name="__main__")
    assert caught.value.code == 0
