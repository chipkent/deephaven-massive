"""The layout of a live run's evidence: each test's id, then its recorded lines indented below.

A live test records what it observed through the `evidence` fixture, one entry at a time. An
entry may hold line breaks, such as Massive's refusal text, so every line of it is indented,
and a reader sees where each entry of a test ends. `tests/conftest.py` appends the entries to
the run's evidence file with `append` and prints them again in the terminal summary with
`indented_lines`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pathlib import Path

INDENT: Final = "    "
"""What starts every line of an entry, below its test's id."""


def indented_lines(entry: str) -> list[str]:
    """Return the lines of one recorded entry, each indented under its test's id.

    Args:
        entry: What a test recorded, which may hold line breaks.

    Returns:
        One indented line for each line of the entry, and one for an empty entry.
    """
    return [f"{INDENT}{line}" for line in entry.splitlines() or [""]]


def append(path: Path, heading: str | None, entry: str) -> None:
    """Append one entry to an evidence file and flush it, so a run cut short keeps it.

    Args:
        path: The evidence file, created by the first append.
        heading: The line that opens a new test's entries, or None when the entry follows
            another of the same test.
        entry: What the test recorded, which may hold line breaks.
    """
    with path.open("a", encoding="utf-8") as file:
        if heading is not None:
            file.write(f"{heading}\n")
        file.writelines(f"{line}\n" for line in indented_lines(entry))
        file.flush()
