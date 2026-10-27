"""The refresh command: `python -m deephaven_massive._massive.refresh [--fetch]`.

Run it from a source checkout with `uv run`; neither the package nor the vendored snapshot it
reads (`vendor/massive/`) is in a distribution.

- With no option, it regenerates `spec/rest.json` and `spec/descriptions.json` from the
  vendored snapshot, checking every refresh gate first, and prints each file it changed. It
  reads no network. When a gate stops it, it prints every problem (the gate, the path, the
  key, and the vendor's description) and exits with status 1, having written nothing.
- With `--fetch`, it replaces the snapshot with the live vendor sources (the OpenAPI
  specification, the documentation index, and the REST pages the index links) and stops; run
  it again without the option to regenerate from the new snapshot. A failed download leaves
  the old snapshot in place and exits with status 1.

Review the snapshot diff and the `spec/*.json` diff together in one pull request.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from deephaven_massive._massive.refresh import fetch, generate, render, snapshot
from deephaven_massive._massive.refresh.gates import RefreshError

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["main"]

_PROG = "python -m deephaven_massive._massive.refresh"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the refresh command.

    Args:
        argv: The command-line arguments after the program name; `sys.argv[1:]` when omitted.

    Returns:
        The exit status: 0 on success, 1 when the refresh or the fetch stopped (the reason is
        printed to standard error). An unknown option makes `argparse` exit with status 2.
    """
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description="Regenerate the wire schemas from the vendored snapshot of Massive's "
        "published specifications (vendor/massive/ in the source checkout).",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="replace the vendored snapshot with the live vendor sources (reads public "
        "Massive URLs)",
    )
    options = parser.parse_args(argv)
    try:
        if options.fetch:
            names = fetch.fetch(snapshot.DIRECTORY)
            sys.stdout.write(f"stored {len(names)} files in {snapshot.DIRECTORY}\n")
            sys.stdout.write(f"now run `{_PROG}` to regenerate the schemas\n")
        else:
            written = generate.refresh()
            for path in written:
                sys.stdout.write(f"{path} updated\n")
            if not written:
                sys.stdout.write(f"{render.SPEC_DIRECTORY} is up to date\n")
    except (RefreshError, fetch.FetchError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
