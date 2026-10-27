"""Regenerate the committed wire schemas from the vendored snapshot: the refresh's one entry.

`refresh()` is what `python -m deephaven_massive._massive.refresh` runs. It composes the
other modules and adds only the rule for writing:

1. `snapshot.Snapshot.load` reads the vendored snapshot and verifies it against its manifest;
2. `rest.generate_rest` turns its specification into one `RestPath` per generated path,
   checking every gate (`gates`) along the way;
3. `render.render_rest` and `render.render_descriptions` write those paths as the canonical
   text of `spec/rest.json` and `spec/descriptions.json`; each file is replaced only when
   every gate passed and its text changed, so a refresh that only rewords descriptions
   leaves `rest.json` untouched.

A stop at any step raises `gates.RefreshError` with every problem found, and nothing is
written. Nothing here touches the network. The refresh runs from a source checkout only:
neither the refresh package nor the snapshot ships in a distribution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._massive.refresh import render
from deephaven_massive._massive.refresh.rest import generate_rest
from deephaven_massive._massive.refresh.snapshot import Snapshot
from deephaven_massive._massive.spec.types import Transport

if TYPE_CHECKING:
    from pathlib import Path

    from deephaven_massive._massive.refresh.hand_input import HandInput

__all__ = ["refresh"]


def refresh(
    snapshot_directory: Path | None = None,
    spec_directory: Path | None = None,
    hand: HandInput | None = None,
) -> list[Path]:
    """Regenerate `spec/rest.json` and `spec/descriptions.json` from the snapshot.

    Each file is written only when every gate passes and its text changes. A `RefreshError`
    from loading the snapshot or from a gate propagates, and nothing is written.

    Each REST source's URLs in `descriptions.json` are those of every documentation page
    that names its path, as the snapshot's manifest records them; the generator guarantees
    every generated path has at least one.

    Args:
        snapshot_directory: The snapshot to read; `snapshot.DIRECTORY` when omitted.
        spec_directory: The directory to write the files to; `render.SPEC_DIRECTORY` when
            omitted. Both defaults are read when the function runs, so a test can point them
            elsewhere.
        hand: The hand input; the real tables when omitted.

    Returns:
        The files written (those whose text changed), `rest.json` first; empty when both were
        already current.
    """
    snapshot = Snapshot.load(snapshot_directory)
    directory = spec_directory or render.SPEC_DIRECTORY
    paths, coverage = generate_rest(snapshot, hand)
    urls = {
        path.key: [snapshot.page_urls[page] for page in coverage.pages[path.key]] for path in paths
    }
    texts = {
        render.REST_FILE.name: render.render_rest(paths),
        render.DESCRIPTIONS_FILE.name: render.render_descriptions(
            {Transport.REST: paths}, fetched=snapshot.fetched, urls={Transport.REST: urls}
        ),
    }
    written: list[Path] = []
    for name, text in texts.items():
        target = directory / name
        if target.is_file() and target.read_text(encoding="utf-8") == text:
            continue
        target.write_text(text, encoding="utf-8")
        written.append(target)
    return written
