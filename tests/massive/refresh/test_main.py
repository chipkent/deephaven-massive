from __future__ import annotations

import os
import runpy
import sys
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import __main__ as command
from deephaven_massive._massive.refresh import fetch, generate, render, snapshot
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
    # The run regenerates a copy, never the committed file, so this test cannot hide a parity
    # failure by rewriting it.
    committed = render.REST_FILE.read_bytes()
    copy = tmp_path / "rest.json"
    copy.write_bytes(committed)
    monkeypatch.setattr(render, "REST_FILE", copy)
    assert command.main([]) == 0
    assert capsys.readouterr().out == f"{copy} is up to date\n"
    assert copy.read_bytes() == committed


def test_the_plain_run_reports_an_update(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # `generate.refresh` writing a changed file is tested with the generator; here only the
    # command's report of it is.
    monkeypatch.setattr(generate, "refresh", lambda: True)
    assert command.main([]) == 0
    assert capsys.readouterr().out == f"{render.REST_FILE} updated\n"


def test_a_stopped_refresh_prints_the_problems_and_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    problem = Problem(gate=Gate.SHORT_KEY, source="/v1/x", key="p", message="short key")

    def stopped() -> bool:
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
        "now run `python -m deephaven_massive._massive.refresh` to regenerate the schemas",
    ]


def test_a_failed_fetch_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def failed(directory: Path) -> list[str]:
        raise fetch.FetchError("GET https://api.massive.com/openapi answered HTTP 503")

    monkeypatch.setattr(fetch, "fetch", failed)
    assert command.main(["--fetch"]) == 1
    assert capsys.readouterr().err == "GET https://api.massive.com/openapi answered HTTP 503\n"


def test_check_is_not_an_option_yet(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        command.main(["--check"])
    assert caught.value.code == 2
    assert "unrecognized arguments: --check" in capsys.readouterr().err


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
