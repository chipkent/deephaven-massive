"""Tests of the probe's result as the parent gets it (`tests/_live/live_run/probe_result.py`).

The probe's child is the fake child (`tests/_fakes/live_child.py`) behind a wrapper that stands
in for the interpreter, so no server starts and nothing reaches Massive.
"""

from __future__ import annotations

import dataclasses
import json
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from tests._live import plans
from tests._live.live_run import probe_result
from tests._live.live_run.probe_result import ProbeChildError, ProbeResult
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.plans import TIERS

ROOT = Path(__file__).resolve().parents[3]

KEY = "PLANTED-probe-child-key-3f9d"

RESULT = ProbeResult(
    when="2026-11-04T17:00:00Z",
    tiers={"stocks": "starter"},
    flat_files=["us_stocks_sip/day_aggs_v1"],
    history_start={"stocks": "2021-11-04"},
    clock_offset=-0.25,
    closures={"2026-11-26": "closed"},
    requests=[{"path": "/v1/marketstatus/now", "query": {}, "status": 200, "request_id": "r"}],
)


def _probe(tmp_path: Path, script: dict[str, object], **options: timedelta) -> ProbeResult:
    wrapper = tmp_path / "python"
    child = ROOT / "tests" / "_fakes" / "live_child.py"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{child}" "$@"\n')
    wrapper.chmod(0o755)
    environ = {"MASSIVE_API_KEY": KEY, "FAKE_LIVE_CHILD": json.dumps(script)}
    return probe_result.probe_in_child(
        python=str(wrapper),
        script=ROOT / "scripts" / "live_run.py",
        root=tmp_path,
        environ=environ,
        secrets=SecretFilter.from_environment(environ),
        **options,
    )


def test_a_result_written_by_the_child_reads_back_whole(tmp_path: Path) -> None:
    probe_result.write_result(tmp_path / "r.json", RESULT)
    assert probe_result.read_result(tmp_path / "r.json") == RESULT


def test_the_probe_runs_in_a_child_and_its_result_is_read(tmp_path: Path) -> None:
    assert _probe(tmp_path, {"probe": dataclasses.asdict(RESULT)}) == RESULT


def test_a_child_that_writes_no_result_fails_showing_its_output_cleaned(tmp_path: Path) -> None:
    script = {"print": ["FAIL java: Java not found", f"key {KEY}"], "exit": 1}
    with pytest.raises(ProbeChildError) as raised:
        _probe(tmp_path, script)
    message = str(raised.value)
    assert message.startswith("the probe's child exited with status 1: FAIL java: Java not found")
    assert KEY not in message


def test_a_child_that_exits_cleanly_without_a_result_fails(tmp_path: Path) -> None:
    with pytest.raises(ProbeChildError, match="exited with status 0"):
        _probe(tmp_path, {})


def test_a_child_that_does_not_end_in_time_is_stopped_showing_its_output(
    tmp_path: Path,
) -> None:
    # The limit leaves the child time to print before it is stopped, however busy the machine.
    script = {"print": ["starting the server", f"key {KEY}"], "hang": True}
    with pytest.raises(ProbeChildError) as raised:
        _probe(tmp_path, script, limit=timedelta(seconds=5))
    message = str(raised.value)
    assert message.startswith("the probe's child did not end within 0:00:05: starting the server")
    assert KEY not in message


def test_plans_take_the_live_tests_grammar() -> None:
    tiers = dict.fromkeys(TIERS, "basic") | {"stocks": "starter", "currencies": "business"}
    text = "stocks=starter,options=basic,indices=basic,currencies=business,futures=basic"
    assert probe_result.plans_variable(tiers, []) == text
    with_files = probe_result.plans_variable(tiers, ["us_stocks_sip/day_aggs_v1", "a/b"])
    assert with_files == text + ",s3=a/b,s3=us_stocks_sip/day_aggs_v1"
    read = plans.declared_plans(with_files)  # the live tests read it back exactly
    assert (read.tiers, read.s3, read.flat_files) == (
        tiers,
        True,
        ("a/b", "us_stocks_sip/day_aggs_v1"),
    )
    assert plans.reaches(read, "a/b")
    assert not plans.reaches(read, "global_crypto/day_aggs_v1")
