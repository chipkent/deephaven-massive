"""Tests of the tester's bug report bundle (`tests/_live/tester/bug_report.py`).

The bundle is built from a tester's folder made up in a temporary directory, with planted
secrets, so the tests see what the zip holds and that no secret reaches it.
"""

from __future__ import annotations

import base64
import zipfile
from typing import TYPE_CHECKING

from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.tester import bug_report

if TYPE_CHECKING:
    from pathlib import Path

KEY = "PLANTED-bundle-key-93fe"


def _folder(tmp_path: Path) -> Path:
    folder = tmp_path / "massive-test"
    (folder / "diagnostics").mkdir(parents=True)
    (folder / "examples-output" / "tables" / "03_daily_bars").mkdir(parents=True)
    (folder / "bug-report.md").write_text("What I did\n", encoding="utf-8")
    (folder / "setup-check.txt").write_text("OK   same commit: both are 1a2b\n", "utf-8")
    (folder / "massive-debug.log").write_text(f"GET /v3?apiKey={KEY}\nline two\n", "utf-8")
    (folder / "diagnostics" / "streams.csv").write_text("Id,State\n1,FAILED\n", "utf-8")
    (folder / "examples-output" / "summary.txt").write_text("01: passed\n", "utf-8")
    tables = folder / "examples-output" / "tables" / "03_daily_bars"
    (tables / "daily.csv").write_text("Symbol\nAAPL\n", "utf-8")
    (folder / "growth.parquet").write_bytes(b"PAR1")
    (folder / "notes.txt").write_text("not collected\n", "utf-8")
    (folder / "diagnostics" / "picture.png").write_bytes(b"\x89PNG")
    return folder


def test_the_bundle_holds_the_collected_text_files_cleaned_and_the_environment(
    tmp_path: Path,
) -> None:
    folder = _folder(tmp_path)
    target, finds = bug_report.build_bundle(
        folder, environment={"os": "macOS", "key": KEY}, secrets=SecretFilter((KEY,)), stamp="T1"
    )
    assert finds == []
    assert target == folder / "bug-report-T1.zip"
    with zipfile.ZipFile(target) as archive:
        names = sorted(archive.namelist())
        texts = {name: archive.read(name).decode() for name in names}
    assert names == [
        "bug-report.md",
        "diagnostics/streams.csv",
        "environment.json",
        "examples-output/summary.txt",
        "examples-output/tables/03_daily_bars/daily.csv",
        "massive-debug.log",
        "setup-check.txt",
    ]
    assert texts["massive-debug.log"] == "GET /v3?apiKey=***\nline two\n"
    assert '"key": "***"' in texts["environment.json"]
    assert KEY not in "".join(texts.values())
    assert not (folder / bug_report.STAGING).exists()


def test_a_secret_the_cleaning_misses_blocks_the_zip(tmp_path: Path) -> None:
    folder = _folder(tmp_path)
    hidden = base64.b64encode(f"user:{KEY}".encode()).decode()
    (folder / "server.log").write_text(f"token {hidden}\n", encoding="utf-8")
    target, finds = bug_report.build_bundle(
        folder, environment={}, secrets=SecretFilter((KEY,)), stamp="T2"
    )
    assert target is None
    assert finds == ["server.log: line 1"]
    assert not list(folder.glob("bug-report-*.zip"))
    assert not (folder / bug_report.STAGING).exists()


def test_an_empty_folder_still_bundles_the_environment(tmp_path: Path) -> None:
    target, _ = bug_report.build_bundle(
        tmp_path, environment={"os": "Linux"}, secrets=SecretFilter((KEY,)), stamp="T3"
    )
    assert target is not None
    with zipfile.ZipFile(target) as archive:
        assert archive.namelist() == ["environment.json"]


def test_tail_text_keeps_the_end_of_a_long_file(tmp_path: Path) -> None:
    path = tmp_path / "long.log"
    path.write_bytes(b"0123456789\xff")
    assert bug_report.tail_text(path, limit=100) == "0123456789�"
    assert bug_report.tail_text(path, limit=4) == "[the first 7 bytes were left out]\n789�"
