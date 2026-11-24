"""Tests of the live run's results for people (`tests/_live/live_run/results.py`).

A state is built by hand; the tests read the `status` lines, `checks.csv`, `REPORT.md`, and the
zip, and plant secrets to see `pack` refuse.
"""

from __future__ import annotations

import csv
import json
import zipfile
from typing import TYPE_CHECKING

from tests._live.live_run import results
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.live_run.state import Attempt, RunState, check_of, record

if TYPE_CHECKING:
    from pathlib import Path

PLANTED_KEY = "PLANTED-results-key-31f0"

REFUSAL = "refused: HTTP 403 from GET /v3/quotes/AAPL on h (request_id 1): not entitled"


def _state() -> RunState:
    state = RunState(commit="abc123")
    passed = check_of(state, "evidence", "t.py::test_a")
    passed.items = ["21", "verified thing"]
    record(state, passed, status="passed", reason="", day="2026-11-04")
    refused = check_of(state, "evidence", "t.py::test_b")
    refused.items = ["22"]
    record(state, refused, status="refused", reason=REFUSAL, day="2026-11-04")
    failed = check_of(state, "smoke", "t.py::test_c")
    failed.items = ["23", "13"]
    record(state, failed, status="failed", reason="call: AssertionError", day="2026-11-04")
    check_of(state, "responses", "market-status").items = ["24"]
    check_of(state, "upgrade-check", "stocks")
    return state


def test_why_a_check_is_unverified() -> None:
    state = _state()
    unlisted = check_of(state, "smoke", "t.py::test_d")
    record(state, unlisted, status="skipped", reason="not listed as delayed: x", day="2026-11-04")
    unnamed = check_of(state, "smoke", "t.py::test_e")
    record(state, unnamed, status="refused", reason="refused: NOT_AUTHORIZED", day="2026-11-04")
    assert results.why_unverified(state.checks["evidence t.py::test_b"]) == (
        "refused by Massive (stocks)"
    )
    assert results.why_unverified(unlisted) == "not listed as delayed"
    assert results.why_unverified(unnamed) == "refused by Massive"
    assert results.why_unverified(state.checks["smoke t.py::test_c"]) == "failed"


def test_the_status_view_lists_the_probe_every_check_and_a_summary() -> None:
    state = _state()
    lines = results.status_lines(
        state,
        latest_probe={"when": "2026-11-04T13:00:00Z"},
        next_start=lambda group: f"soon ({group})",
    )
    assert lines[0] == "latest probe: 2026-11-04T13:00:00Z"
    assert "next stream day: none planned" in lines
    assert "evidence (next: soon (evidence))" in lines
    assert f"  t.py::test_b: {REFUSAL}" in lines  # Massive's refusal, its word said once
    assert "  stocks: not started (optional)" in lines
    header = "group | passed | failed | error | refused | skipped | problem present | problem fixed"
    assert f"{header} | not run" in lines
    assert "evidence | 1 | 0 | 0 | 1 | 0 | 0 | 0 | 0" in lines
    assert "upgrade-check | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1" in lines


def test_the_status_view_before_any_probe() -> None:
    lines = results.status_lines(RunState(commit="c"), latest_probe=None, next_start=str)
    assert lines[:2] == ["latest probe: none yet", "next stream day: none planned"]


def test_checks_csv_has_one_line_per_check(tmp_path: Path) -> None:
    results.write_checks_csv(tmp_path, _state())
    with (tmp_path / "checks.csv").open() as file:
        rows = list(csv.DictReader(file))
    assert [r["name"] for r in rows] == [
        "t.py::test_a",
        "t.py::test_b",
        "t.py::test_c",
        "market-status",
        "stocks",
    ]
    assert rows[1]["reason"] == REFUSAL
    assert rows[1]["refused_subscription"] == "stocks"
    assert rows[3]["refused_subscription"] == ""


def test_an_items_status_follows_its_checks() -> None:
    state = _state()
    checks = state.checks
    passed, refused = checks["evidence t.py::test_a"], checks["evidence t.py::test_b"]
    failed, never = checks["smoke t.py::test_c"], checks["responses market-status"]
    assert results.item_status([passed]) == "evidence recorded"
    assert results.item_status([passed, never]) == "partly run"
    assert results.item_status([passed, refused]) == "refused by Massive (stocks)"
    assert results.item_status([refused, failed]) == "failed"


def _problem_checks(state: RunState) -> None:
    present = check_of(state, "evidence", "t.py::test_shows")
    present.issue = 797
    record(
        state, present, status="problem present", reason="volume 12 against 13", day="2026-11-04"
    )
    fixed = check_of(state, "evidence", "t.py::test_gone")
    fixed.issue = 803
    record(state, fixed, status="problem fixed", reason="close it", day="2026-11-04")
    half = check_of(state, "smoke", "t.py::test_docs")
    half.issue = 779
    record(state, half, status="problem fixed", reason="close it", day="2026-11-04")
    other_half = check_of(state, "smoke", "t.py::test_data")
    other_half.issue = 779
    record(state, other_half, status="problem present", reason="Chicago", day="2026-11-04")
    refused = check_of(state, "smoke", "t.py::test_refused")
    refused.issue = 778
    record(state, refused, status="refused", reason=REFUSAL, day="2026-11-04")
    check_of(state, "smoke", "t.py::test_never").issue = 775


def test_the_report_lists_the_massive_problems_by_issue(tmp_path: Path) -> None:
    state = _state()
    _problem_checks(state)
    results.write_report(tmp_path, state)
    report = (tmp_path / "REPORT.md").read_text()
    section = report.split("## Massive problems, by GitHub issue\n\n", 1)[1]
    block, table = section.split("\n\n", 2)[:2]
    assert block.splitlines() == [
        "```text",
        "Massive problems FIXED, close the issue, 1 of 4:",
        "    #803: evidence: t.py::test_gone",
        "Massive problems partly fixed, read each check, 1 of 4:",
        "    #779: smoke: t.py::test_data, smoke: t.py::test_docs",
        "Massive problems still present, 1 of 4:",
        "    #797: evidence: t.py::test_shows",
        "Massive problems not judged (a check failed, was skipped, or was refused), 1 of 4:",
        f"    #778: smoke: t.py::test_refused [refused: {REFUSAL}]",
        "```",
    ]
    assert table.splitlines()[2:] == [
        "| #803 | evidence: t.py::test_gone | problem fixed | close it |",
        "| #797 | evidence: t.py::test_shows | problem present | volume 12 against 13 |",
        "| #779 | smoke: t.py::test_data | problem present | Chicago |",
        "| #779 | smoke: t.py::test_docs | problem fixed | close it |",
        f"| #778 | smoke: t.py::test_refused | refused | {REFUSAL} |",
    ]
    assert "test_shows" not in report.split("## Still unverified", 1)[1]


def test_the_report_has_every_section(tmp_path: Path) -> None:
    (tmp_path / "preflight").mkdir()
    environment = {"items": [{"name": "java", "verdict": "OK", "detail": "Java 21"}], "issue": None}
    (tmp_path / "preflight" / "environment.json").write_text(json.dumps(environment))
    (tmp_path / "probes").mkdir()
    (tmp_path / "probes" / "probes.csv").write_text(
        "when,clock_offset_s\n2026-11-04T13:00:00Z,0.2\n"
    )
    results.write_report(tmp_path, _state())
    report = (tmp_path / "REPORT.md").read_text()
    assert "Commit: `abc123`" in report
    assert "| java | OK | Java 21 |" in report
    assert "| 2026-11-04T13:00:00Z | 0.2 |" in report
    assert "## What Massive refused, by day" in report
    assert f"| 2026-11-04 | stocks | evidence: t.py::test_b | {REFUSAL} |" in report
    assert "## smoke" in report
    assert "| 21 | evidence recorded |" in report
    assert "| 22 | refused by Massive (stocks) |" in report
    assert "| 13 |" not in report  # checked by CI, not by a live test
    assert "- **refused by Massive (stocks)**: evidence: t.py::test_b" in report
    assert "- **failed**: smoke: t.py::test_c" in report


def test_the_report_without_an_environment_or_a_probe(tmp_path: Path) -> None:
    results.write_report(tmp_path, RunState(commit="c"))
    report = (tmp_path / "REPORT.md").read_text()
    assert "No environment was recorded." in report
    assert "No probe was recorded." in report
    assert "Massive refused nothing." in report
    assert "No check of an open Massive problem ran." in report


def test_pack_zips_every_file_but_the_lock_and_earlier_zips(tmp_path: Path) -> None:
    (tmp_path / "evidence" / "2026-11-04-1").mkdir(parents=True)
    (tmp_path / "evidence" / "2026-11-04-1" / "group.log").write_text("ok\n")
    (tmp_path / "run.lock").write_text("")
    (tmp_path / "live-results-2026-11-03.zip").write_bytes(b"old")
    target, finds = results.pack(tmp_path, _state(), SecretFilter((PLANTED_KEY,)), day="2026-11-04")
    assert finds == []
    assert target == tmp_path / "live-results-2026-11-04.zip"
    with zipfile.ZipFile(target) as archive:
        names = sorted(archive.namelist())
    assert names == ["REPORT.md", "checks.csv", "evidence/2026-11-04-1/group.log"]


def test_pack_refuses_when_a_secret_is_anywhere(tmp_path: Path) -> None:
    (tmp_path / "debug.log").write_text(f"one\nkey {PLANTED_KEY}\n")
    target, finds = results.pack(tmp_path, _state(), SecretFilter((PLANTED_KEY,)), day="2026-11-04")
    assert target is None
    assert finds == ["debug.log: line 2"]
    assert not list(tmp_path.glob("*.zip"))


def test_pack_stores_parquet_as_it_is_and_keeps_only_the_newest_zip(tmp_path: Path) -> None:
    (tmp_path / "rows").mkdir()
    (tmp_path / "rows" / "bars.parquet").write_bytes(b"PAR1" * 100)
    (tmp_path / "group.log").write_text("line\n" * 100)
    (tmp_path / "live-results-2026-11-03.zip").write_bytes(b"older")
    target, _ = results.pack(tmp_path, _state(), SecretFilter((PLANTED_KEY,)), day="2026-11-04")
    assert target is not None
    with zipfile.ZipFile(target) as archive:
        kinds = {info.filename: info.compress_type for info in archive.infolist()}
    assert kinds["rows/bars.parquet"] == zipfile.ZIP_STORED
    assert kinds["group.log"] == zipfile.ZIP_DEFLATED
    assert sorted(p.name for p in tmp_path.glob("*.zip")) == ["live-results-2026-11-04.zip"]


def test_the_status_view_names_the_planned_night() -> None:
    state = _state()
    assert "next stream night: none planned" in results.status_lines(
        state, latest_probe=None, next_start=str
    )
    state.stream_night = "2026-11-04"
    lines = results.status_lines(state, latest_probe=None, next_start=str)
    assert "next stream night: Wed 2026-11-04 16:30 EST to Thu 09:30 EST" in lines


def test_the_report_gives_each_nights_findings(tmp_path: Path) -> None:
    state = _state()
    results.write_report(tmp_path, state)
    assert "## Stream nights\n\nNo stream night has run." in (tmp_path / "REPORT.md").read_text()
    night = "stream-night/2026-11-04-1"
    state.attempts = [
        Attempt("stream-night", night, "2026-11-04", ""),
        Attempt("stream-night", night, "2026-11-04", ""),  # a restart shares the folder
        Attempt("stream-night", "stream-night/2026-11-09-1", "2026-11-09", ""),  # no summary
    ]
    (tmp_path / night).mkdir(parents=True)
    boundary = {
        "unit": "futures",
        "stream": "futures-minute-bars",
        "boundary": "16:00 Chicago close",
        "at": "2026-11-04T23:00:00+00:00",
        "side": "after",
        "rows": 90,
        "dates": "2026-11-04=1 2026-11-05=89",
        "expected": "2026-11-05",
        "wrong": 1,
    }
    summary = {
        "night": "2026-11-04",
        "gaps_minutes": 3,
        "reconnects": {"futures": 1},
        "quiet_hours": {"futures": [], "crypto": ["2026-11-05T03:00:00+00:00"]},
        "boundaries": [boundary],
    }
    (tmp_path / night / "summary.json").write_text(json.dumps(summary))
    results.write_report(tmp_path, state)
    report = (tmp_path / "REPORT.md").read_text()
    assert report.count("### 2026-11-04") == 1
    assert "- Gaps: 3 minutes. Reconnects: futures 1." in report
    assert "- futures: quiet hours (UTC): none." in report
    assert "- crypto: quiet hours (UTC): 2026-11-05T03:00:00+00:00." in report
    assert (
        "| futures-minute-bars | 16:00 Chicago close | after | 90 | 2026-11-04=1 2026-11-05=89 "
        "| 2026-11-05 | 1 |" in report
    )
    summary["reconnects"] = {}
    (tmp_path / night / "summary.json").write_text(json.dumps(summary))
    results.write_report(tmp_path, state)
    assert "Reconnects: none." in (tmp_path / "REPORT.md").read_text()
