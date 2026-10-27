"""Tests for the refresh: the parity of `spec/*.json` with the snapshot, and when it writes.

Every test reads the vendored snapshot, so the module skips where there is none (an sdist
leaves it out).
"""

from __future__ import annotations

import datetime as dt
import json
import os
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import generate, render, snapshot
from deephaven_massive._massive.refresh.snapshot import DIRECTORY, MANIFEST, OPENAPI, VendorFile

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)

_COMMITTED = (render.REST_FILE, render.DESCRIPTIONS_FILE)


def test_the_generated_files_match_the_snapshot_byte_for_byte(tmp_path: Path) -> None:
    # A fresh rendering must be each committed file exactly: they change only through the
    # refresh.
    written = generate.refresh(spec_directory=tmp_path)
    assert written == [tmp_path / "rest.json", tmp_path / "descriptions.json"]
    for committed in _COMMITTED:
        assert (tmp_path / committed.name).read_bytes() == committed.read_bytes(), committed.name


def test_refresh_writes_only_a_changed_file(tmp_path: Path) -> None:
    committed = [path.read_bytes() for path in _COMMITTED]
    (tmp_path / "rest.json").write_bytes(render.REST_FILE.read_bytes())
    (tmp_path / "descriptions.json").write_text("stale\n", encoding="utf-8")
    assert generate.refresh(spec_directory=tmp_path) == [tmp_path / "descriptions.json"]
    assert (tmp_path / "descriptions.json").read_bytes() == committed[1]
    assert generate.refresh(spec_directory=tmp_path) == []
    assert [path.read_bytes() for path in _COMMITTED] == committed


def test_refresh_reads_the_snapshot_it_is_given(tmp_path: Path) -> None:
    # The default snapshot is read only when no directory is passed.
    assert len(generate.refresh(DIRECTORY, tmp_path)) == 2
    assert (tmp_path / "rest.json").read_bytes() == render.REST_FILE.read_bytes()


def _copy_snapshot(target: Path, openapi: bytes) -> None:
    """Store a copy of the vendored snapshot with another specification, re-hashed.

    Args:
        target: Where to store it.
        openapi: The stored bytes of the replacement specification.
    """
    manifest = json.loads((DIRECTORY / MANIFEST).read_text(encoding="utf-8"))
    files = []
    for name, entry in manifest["files"].items():
        stored = openapi if name == OPENAPI else (DIRECTORY / name).read_bytes()
        files.append(VendorFile(name=name, url=entry["url"], raw=stored, stored=stored))
    snapshot.store(target, files, fetched=dt.date.fromisoformat(manifest["fetched"]))


def test_a_reworded_description_changes_only_the_descriptions_file(tmp_path: Path) -> None:
    # The only change is one field's description: rest.json must stay byte for byte as it is,
    # so a reviewer sees that no schema moved.
    original = (DIRECTORY / OPENAPI).read_bytes()
    reworded = original.replace(b"The exchange symbol that this item is traded under.", b"Moved.")
    assert reworded != original
    _copy_snapshot(tmp_path / "snapshot", reworded)
    (tmp_path / "spec").mkdir()
    for committed in _COMMITTED:
        (tmp_path / "spec" / committed.name).write_bytes(committed.read_bytes())
    written = generate.refresh(tmp_path / "snapshot", tmp_path / "spec")
    assert written == [tmp_path / "spec" / "descriptions.json"]
    assert (tmp_path / "spec" / "rest.json").read_bytes() == render.REST_FILE.read_bytes()
    texts = json.loads((tmp_path / "spec" / "descriptions.json").read_text(encoding="utf-8"))
    assert "Moved." in {
        text for by_key in texts["descriptions"]["REST"].values() for text in by_key.values()
    }
