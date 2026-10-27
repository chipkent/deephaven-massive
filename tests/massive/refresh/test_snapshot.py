"""Tests for the vendored snapshot: its location, storing with a manifest, and verified loading.

Synthetic snapshots are written under `tmp_path`; the one test of the real snapshot skips
where there is none (an sdist leaves it out).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import snapshot
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.snapshot import Snapshot, VendorFile

if TYPE_CHECKING:
    from pathlib import Path


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- The location ---------------------------------------------------------------------------


def test_the_snapshot_lives_outside_the_package() -> None:
    # `vendor/massive/` at the top of the checkout, beside `src/`: no distribution reaches it.
    root = snapshot.DIRECTORY.parent.parent
    assert snapshot.DIRECTORY.relative_to(root).as_posix() == "vendor/massive"
    assert (root / "src" / "deephaven_massive" / "_massive" / "refresh").is_dir()


@pytest.mark.skipif(
    not snapshot.DIRECTORY.is_dir(),
    reason="the vendored snapshot (vendor/massive/) is in a source checkout only",
)
def test_the_real_snapshot_matches_its_manifest() -> None:
    manifest = json.loads((snapshot.DIRECTORY / "manifest.json").read_text("utf-8"))
    assert Snapshot.load().openapi_sha256 == manifest["files"]["openapi.json"]["stored_sha256"]


def test_a_missing_snapshot_says_to_run_from_a_source_checkout(tmp_path: Path) -> None:
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "absent")
    (problem,) = caught.value.problems
    assert (problem.gate, problem.key) == ("snapshot", str(tmp_path / "absent"))
    assert "source checkout" in problem.message


# --- Storing --------------------------------------------------------------------------------


def _file(name: str, stored: bytes, raw: bytes | None = None) -> VendorFile:
    return VendorFile(
        name=name, url=f"https://example.com/{name}", raw=raw or stored, stored=stored
    )


def test_store_writes_every_file_and_a_manifest_of_both_hashes(tmp_path: Path) -> None:
    target = tmp_path / "massive"
    files = [_file("openapi.json", b"{}\n", raw=b"{}"), _file("rest/a/b.md", b"page\n")]
    names = snapshot.store(target, files, fetched=dt.date(2026, 10, 26))
    assert names == ["openapi.json", "rest/a/b.md", "manifest.json"]
    assert (target / "rest" / "a" / "b.md").read_bytes() == b"page\n"
    text = (target / "manifest.json").read_text(encoding="utf-8")
    assert text.endswith("}\n")
    assert json.loads(text) == {
        "fetched": "2026-10-26",
        "files": {
            "openapi.json": {
                "url": "https://example.com/openapi.json",
                "fetched_sha256": _sha(b"{}"),
                "stored_sha256": _sha(b"{}\n"),
            },
            "rest/a/b.md": {
                "url": "https://example.com/rest/a/b.md",
                "fetched_sha256": _sha(b"page\n"),
                "stored_sha256": _sha(b"page\n"),
            },
        },
    }


def test_store_replaces_the_old_snapshot_entirely(tmp_path: Path) -> None:
    target = tmp_path / "massive"
    (target / "rest" / "gone").mkdir(parents=True)
    (target / "rest" / "gone" / "page.md").write_text("removed by Massive\n", encoding="utf-8")
    (tmp_path / "massive-incoming").mkdir()  # a staging directory left by an interrupted run
    (tmp_path / "massive-previous").mkdir()  # a backup left by an interrupted swap
    snapshot.store(target, [_file("openapi.json", b"{}\n")], fetched=dt.date(2026, 1, 1))
    assert sorted(p.name for p in target.iterdir()) == ["manifest.json", "openapi.json"]
    assert not (tmp_path / "massive-incoming").exists()
    assert not (tmp_path / "massive-previous").exists()


# --- Loading --------------------------------------------------------------------------------


def _write_snapshot(directory: Path, *, llms: str, pages: dict[str, str]) -> None:
    """Write a snapshot whose manifest lists every file with its true hash."""
    files = [
        _file("openapi.json", json.dumps({"paths": {}}).encode()),
        _file("llms.txt", llms.encode()),
    ]
    files += [_file(f"rest/{name}", text.encode()) for name, text in pages.items()]
    snapshot.store(directory, files, fetched=dt.date(2026, 1, 1))


_INDEX = "## REST API\n- [Trades](https://massive.com/docs/rest/stocks/trades.md): Trades\n"


def test_a_stored_snapshot_loads(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    loaded = Snapshot.load(tmp_path / "s")
    assert loaded.pages == {"stocks/trades.md": "page"}
    assert loaded.openapi == {"paths": {}}
    assert loaded.llms == _INDEX
    assert loaded.openapi_sha256 == _sha((tmp_path / "s" / "openapi.json").read_bytes())


def test_gate_snapshot_an_edited_vendor_file(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    page = tmp_path / "s" / "rest" / "stocks" / "trades.md"
    page.write_text("pagf", encoding="utf-8")  # one byte changed
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key) for p in caught.value.problems] == [
        ("snapshot", "rest/stocks/trades.md")
    ]


def test_gate_snapshot_files_must_match_the_manifest_exactly(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / "stray.md").write_text("added by hand", encoding="utf-8")
    (tmp_path / "s" / "rest" / "stocks" / "trades.md").rename(tmp_path / "trades.md")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key, p.message) for p in caught.value.problems if p.gate == "snapshot"] == [
        ("snapshot", "rest/stocks/trades.md", "listed but missing"),
        ("snapshot", "stray.md", "not in the manifest"),
    ]


def test_gate_docs_a_linked_page_missing_from_the_snapshot(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key) for p in caught.value.problems] == [("docs", "stocks/trades.md")]


def test_a_snapshot_that_is_not_json_objects_is_reported(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / "openapi.json").write_text("[]", encoding="utf-8")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [p.gate for p in caught.value.problems] == ["snapshot"]
