"""Regenerate the committed wire schemas from the vendored snapshot: the refresh's one entry.

`refresh()` is what `python -m deephaven_massive._massive.refresh` runs. It composes the
other modules and adds only the rule for writing:

1. `snapshot.Snapshot.load` reads the vendored snapshot and verifies it against its manifest;
2. `rest.generate_rest` turns its specification into one `RestPath` per generated path,
   checking every gate (`gates`) along the way;
3. `render.render_rest` writes those paths as the canonical text of `spec/rest.json`, which
   replaces the file only when every gate passed and the text changed.

A stop at any step raises `gates.RefreshError` with every problem found, and nothing is
written. Nothing here touches the network. The refresh runs from a source checkout only:
neither the refresh package nor the snapshot ships in a distribution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._massive.refresh import render
from deephaven_massive._massive.refresh.rest import generate_rest
from deephaven_massive._massive.refresh.snapshot import Snapshot

if TYPE_CHECKING:
    from pathlib import Path

    from deephaven_massive._massive.refresh.hand import HandInput

__all__ = ["refresh"]


def refresh(
    snapshot_directory: Path | None = None,
    spec_file: Path | None = None,
    hand: HandInput | None = None,
) -> bool:
    """Regenerate `spec/rest.json` from the snapshot.

    The file is written only when every gate passes and its text changes. A `RefreshError`
    from loading the snapshot or from a gate propagates, and nothing is written.

    Args:
        snapshot_directory: The snapshot to read; `snapshot.DIRECTORY` when omitted.
        spec_file: The file to write; `render.REST_FILE` when omitted. Both defaults are read
            when the function runs, so a test can point them elsewhere.
        hand: The hand input; the real tables when omitted.

    Returns:
        True when the file was written (its text changed), False when it was already current.
    """
    snapshot = Snapshot.load(snapshot_directory)
    spec_file = spec_file or render.REST_FILE
    paths, _ = generate_rest(snapshot, hand)
    text = render.render_rest(paths, openapi_sha256=snapshot.openapi_sha256)
    if spec_file.is_file() and spec_file.read_text(encoding="utf-8") == text:
        return False
    spec_file.write_text(text, encoding="utf-8")
    return True
