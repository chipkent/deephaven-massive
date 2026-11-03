"""Tests of the live tests' own debug log (`tests/_live/debug_log_file.py`).

The log is pointed at a folder under `tmp_path` instead of the repository root. One test lets
`turn_on` set the variable in the process's environment, opens a session with a planted key and
S3 pair, which reads the variable as a live run's first session does, makes a request to the
fake REST server, and scans the file: no secret may be in it.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import deephaven_massive as dm
from deephaven_massive import _debug_log
from tests._fakes.rest import Reply
from tests._live import debug_log_file

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from tests._fakes.rest import FakeRestServer

NOW = datetime(2026, 11, 4, 14, 3, 7, tzinfo=UTC)

PLANTED = {
    "MASSIVE_API_KEY": "PLANTED-debug-log-key-7c2e",
    "MASSIVE_S3_ACCESS_KEY_ID": "PLANTED-debug-log-id-19aa",
    "MASSIVE_S3_SECRET_ACCESS_KEY": "PLANTED-debug-log-secret-5d0f",
}


def test_a_run_gets_a_new_folder_and_the_variable_names_its_log(tmp_path: Path) -> None:
    environ: dict[str, str] = {}
    path = debug_log_file.turn_on(environ, root=tmp_path, now=NOW)
    assert path == tmp_path / "live-debug-logs" / f"20261104T140307Z-{os.getpid()}" / "debug.log"
    assert path.parent.is_dir()
    assert path.is_absolute()
    assert environ == {"DEEPHAVEN_MASSIVE_LOG_FILE": str(path)}


def test_a_log_file_the_environment_names_is_left_as_it_is(tmp_path: Path) -> None:
    for preset in ("/elsewhere/debug.log", ""):
        environ = {"DEEPHAVEN_MASSIVE_LOG_FILE": preset}
        assert debug_log_file.turn_on(environ, root=tmp_path, now=NOW) is None
        assert environ == {"DEEPHAVEN_MASSIVE_LOG_FILE": preset}
    assert not (tmp_path / "live-debug-logs").exists()


def test_the_summary_names_the_log_or_says_none_was_written(tmp_path: Path) -> None:
    path = tmp_path / "run" / "debug.log"
    path.parent.mkdir()
    assert debug_log_file.summary_lines(path, PLANTED) == [
        f"debug log: none was written at {path} (no session was opened)"
    ]
    assert not path.parent.exists()  # a run that opened no session leaves no empty folder
    assert debug_log_file.summary_lines(path, PLANTED)[0].startswith("debug log: none")
    path.parent.mkdir()
    (path.parent / "other.txt").write_text("kept\n", encoding="utf-8")
    debug_log_file.summary_lines(path, PLANTED)
    assert (path.parent / "other.txt").exists()  # a folder holding anything else stays
    path.write_text("a clean line\n", encoding="utf-8")
    assert debug_log_file.summary_lines(path, PLANTED) == [
        f"debug log: {path}",
        "send this file with a report of any failure above",
    ]


def test_a_log_holding_a_secret_is_named_as_not_to_be_sent(tmp_path: Path) -> None:
    path = tmp_path / "debug.log"
    path.write_text("one\nkey " + PLANTED["MASSIVE_S3_SECRET_ACCESS_KEY"] + "\n", encoding="utf-8")
    assert debug_log_file.summary_lines(path, PLANTED) == [
        f"debug log: {path}",
        "a secret was found in it: do not send it; send the owner these places instead:",
        "  debug.log: line 2",
    ]


def test_a_sessions_debug_log_holds_no_secret(
    fake_rest: FakeRestServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A fresh switch, so the session reads the variables as a run's first session does.
    monkeypatch.setattr(_debug_log, "_switch", _debug_log._Switch())
    for name in (debug_log_file.FILE_VARIABLE, "DEEPHAVEN_MASSIVE_LOG"):
        monkeypatch.setenv(name, "")  # restored, or removed, when the test ends
        monkeypatch.delenv(name)
    path = debug_log_file.turn_on(os.environ, root=tmp_path, now=NOW)
    assert path is not None
    assert os.environ[debug_log_file.FILE_VARIABLE] == str(path)
    fake_rest.reply("/v1/marketstatus/now", Reply({"market": "open"}))
    config = dm.MassiveConfig(
        api_key=PLANTED["MASSIVE_API_KEY"],
        rest_base_url=fake_rest.url,
        s3_access_key_id=PLANTED["MASSIVE_S3_ACCESS_KEY_ID"],
        s3_secret_access_key=PLANTED["MASSIVE_S3_SECRET_ACCESS_KEY"],
    )
    try:
        with dm.MassiveSession(config) as m:
            m.reference.market_status().fetch()
    finally:
        dm.disable_debug_logging()
    assert "GET" in path.read_text(encoding="utf-8")  # the requests were logged at DEBUG
    assert debug_log_file.summary_lines(path, PLANTED)[1].startswith("send this file")
