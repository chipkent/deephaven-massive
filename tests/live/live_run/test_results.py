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
from tests._live.live_run.state import RunState, check_of, entitlement_id, record

if TYPE_CHECKING:
    from pathlib import Path

BASIC = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "basic")

PLANTED_KEY = "PLANTED-results-key-31f0"


def _state() -> RunState:
    state = RunState(commit="abc123")
    state.current = entitlement_id(state, BASIC, [])
    passed = check_of(state, "evidence", "t.py::test_a")
    passed.items = ["21", "verified thing"]
    record(state, passed, status="passed", reason="", day="2026-11-04")
    skipped = check_of(state, "evidence", "t.py::test_b")
    skipped.items = ["22"]
    record(
        state,
        skipped,
        status="skipped",
        reason="needs Stocks Starter ($29/month)",
        day="2026-11-04",
    )
    failed = check_of(state, "smoke", "t.py::test_c")
    failed.items = ["23", "13"]
    record(state, failed, status="failed", reason="call: AssertionError", day="2026-11-04")
    check_of(state, "responses", "market-status").items = ["24"]
    check_of(state, "upgrade-check", "stocks")
    return state


def test_a_reason_loses_the_plans_prices() -> None:
    assert results.shown_reason(
        "needs Stocks Starter ($29/month); Options Business (price on request)"
    ) == ("needs Stocks Starter; Options Business")
    assert (
        results.shown_reason("needs at least one class on Basic (free)")
        == "needs at least one class on Basic"
    )


def test_the_status_view_lists_the_probe_every_check_and_a_summary() -> None:
    state = _state()
    lines = results.status_lines(
        state,
        latest_probe={"when": "2026-11-04T13:00:00Z"},
        next_start=lambda group: f"soon ({group})",
    )
    assert lines[0] == "latest probe: 2026-11-04T13:00:00Z"
    assert lines[1].startswith("  subscriptions: stocks=basic")
    assert "next stream day: none planned" in lines
    assert "evidence (next: soon (evidence))" in lines
    assert any(
        line.startswith("  t.py::test_b: skipped: needs Stocks Starter [stocks=basic")
        for line in lines
    )
    assert "  stocks: not started (optional)" in lines
    assert "group | passed | failed | error | refused | skipped | not run" in lines
    assert "evidence | 1 | 0 | 0 | 0 | 1 | 0" in lines
    assert "upgrade-check | 0 | 0 | 0 | 0 | 0 | 1" in lines


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
    assert rows[1]["reason"] == "needs Stocks Starter"
    assert rows[1]["subscriptions"].startswith("stocks=basic, options=basic")
    assert rows[3]["subscriptions"] == ""


def test_an_items_status_follows_its_checks() -> None:
    state = _state()
    checks = state.checks
    passed, skipped = checks["evidence t.py::test_a"], checks["evidence t.py::test_b"]
    failed, never = checks["smoke t.py::test_c"], checks["responses market-status"]
    assert results.item_status([passed]) == "evidence recorded"
    assert results.item_status([passed, never]) == "partly run"
    assert results.item_status([passed, skipped]) == "needs Stocks Starter"
    assert results.item_status([skipped, failed]) == "failed"


def test_the_report_has_every_section(tmp_path: Path) -> None:
    (tmp_path / "preflight").mkdir()
    environment = {"items": [{"name": "java", "verdict": "OK", "detail": "Java 21"}]}
    (tmp_path / "preflight" / "environment.json").write_text(json.dumps(environment))
    (tmp_path / "probes").mkdir()
    (tmp_path / "probes" / "entitlements.csv").write_text(
        "when,entitlement\n2026-11-04T13:00:00Z,e1\n"
    )
    results.write_report(tmp_path, _state())
    report = (tmp_path / "REPORT.md").read_text()
    assert "Commit: `abc123`" in report
    assert "| java | OK | Java 21 |" in report
    assert "| 2026-11-04T13:00:00Z | e1 |" in report
    assert "## smoke" in report
    assert "| 21 | evidence recorded |" in report
    assert "| 22 | needs Stocks Starter |" in report
    assert "| 13 |" not in report  # checked by CI, not by a live test
    assert "- **needs Stocks Starter**: evidence: t.py::test_b" in report
    assert "- **failed**: smoke: t.py::test_c" in report


def test_the_report_without_an_environment_or_a_probe(tmp_path: Path) -> None:
    results.write_report(tmp_path, RunState(commit="c"))
    report = (tmp_path / "REPORT.md").read_text()
    assert "No environment was recorded." in report
    assert "No probe was recorded." in report


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
