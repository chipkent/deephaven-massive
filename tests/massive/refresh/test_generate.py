"""Tests for the refresh: the parity of `spec/rest.json` with the snapshot, and when it writes.

Every test reads the vendored snapshot, so the module skips where there is none (an sdist
leaves it out).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import generate, render
from deephaven_massive._massive.refresh.snapshot import DIRECTORY

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)


def test_rest_json_matches_the_snapshot_byte_for_byte(tmp_path: Path) -> None:
    # A fresh rendering must be the committed file exactly: the file changes only through the
    # refresh.
    target = tmp_path / "rest.json"
    assert generate.refresh(spec_file=target) is True
    assert target.read_bytes() == render.REST_FILE.read_bytes()


def test_refresh_writes_only_a_changed_file(tmp_path: Path) -> None:
    committed = render.REST_FILE.read_bytes()
    target = tmp_path / "rest.json"
    target.write_text("stale\n", encoding="utf-8")
    assert generate.refresh(spec_file=target) is True
    assert target.read_bytes() == committed
    assert generate.refresh(spec_file=target) is False
    assert render.REST_FILE.read_bytes() == committed


def test_refresh_reads_the_snapshot_it_is_given(tmp_path: Path) -> None:
    # The default snapshot is read only when no directory is passed.
    target = tmp_path / "rest.json"
    assert generate.refresh(DIRECTORY, target) is True
    assert target.read_bytes() == render.REST_FILE.read_bytes()
