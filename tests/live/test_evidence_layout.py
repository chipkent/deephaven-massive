"""Tests of the evidence layout (`tests/_live/evidence_layout.py`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests._live import evidence_layout

if TYPE_CHECKING:
    from pathlib import Path


def test_a_multi_line_message_stays_indented_under_its_test(tmp_path: Path) -> None:
    # Massive's refusal text may hold line breaks. Every line of it stays indented, so the
    # next lines never read as a new test's id or as output of their own.
    path = tmp_path / "live-evidence-20261124T120000Z.log"
    message = 'Massive answered GET /v3/x with HTTP 400: "bad value\nsee the docs"\r\nlast'
    evidence_layout.append(path, "2026-11-24T12:00:00Z tests/live/test_x.py::test_a", "first")
    evidence_layout.append(path, None, message)
    evidence_layout.append(path, "2026-11-24T12:00:01Z tests/live/test_x.py::test_b", "")
    assert path.read_text(encoding="utf-8").splitlines() == [
        "2026-11-24T12:00:00Z tests/live/test_x.py::test_a",
        "    first",
        '    Massive answered GET /v3/x with HTTP 400: "bad value',
        '    see the docs"',
        "    last",
        "2026-11-24T12:00:01Z tests/live/test_x.py::test_b",
        "    ",
    ]


def test_the_summary_indents_every_line_of_an_entry() -> None:
    assert evidence_layout.indented_lines("one\ntwo") == ["    one", "    two"]
