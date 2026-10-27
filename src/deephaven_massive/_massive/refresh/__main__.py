"""The refresh command: `python -m deephaven_massive._massive.refresh [--check | --fetch]`.

Run it from a source checkout with `uv run`; neither the package nor the vendored snapshot it
reads (`vendor/massive/`) is in a distribution.

- With no option, it regenerates `spec/rest.json`, `spec/ws.json`, `spec/flatfiles.json`,
  and `spec/descriptions.json` from the vendored snapshot, checking every refresh gate first,
  and prints each file it changed. It reads no network. When a gate stops it, it prints every
  problem (the gate, the source, the key, and the vendor's description) and exits with status
  1, having written nothing.
- With `--fetch`, it replaces the snapshot with the live vendor sources (the OpenAPI and
  WebSocket specifications, the documentation index, and the REST, WebSocket, and flat-file
  pages the index links) and stops; run it again without the option to regenerate from the
  new snapshot. A failed download leaves the old snapshot in place and exits with status 1.
- With `--check`, it downloads the same live sources into a scratch directory, compares them
  with the vendored snapshot and the committed schemas (`drift`), prints the drift report as
  Markdown to standard output, and changes nothing. It exits with status 0 when there is no
  drift, 1 when there is, and 2 when the check could not be made (a download failed, the
  vendored snapshot does not match its manifest, or the flat-file bucket could not be
  listed), with the reason on standard error. With `MASSIVE_S3_ACCESS_KEY_ID` and
  `MASSIVE_S3_SECRET_ACCESS_KEY` set, it also lists the flat-file bucket's top-level prefixes.
  The weekly drift workflow runs this same command.

Review the snapshot diff and the `spec/*.json` diff together in one pull request.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.refresh import drift, fetch, generate, render, snapshot
from deephaven_massive._massive.refresh.gates import RefreshError

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["main"]

_PROG = "python -m deephaven_massive._massive.refresh"
# The exit status of a check that found drift, and of one that could not be made.
_DRIFT = 1
_CHECK_FAILED = 2


def main(argv: Sequence[str] | None = None) -> int:
    """Run the refresh command.

    Args:
        argv: The command-line arguments after the program name; `sys.argv[1:]` when omitted.

    Returns:
        The exit status. The plain run and `--fetch`: 0 on success, 1 when the refresh or the
        fetch stopped (the reason is printed to standard error). `--check`: 0 without drift,
        1 with drift, 2 when the check could not be made. A usage error makes `argparse` exit
        with status 2.
    """
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description="Regenerate the wire schemas from the vendored snapshot of Massive's "
        "published specifications (vendor/massive/ in the source checkout).",
    )
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument(
        "--check",
        action="store_true",
        help="compare the live vendor sources with the vendored snapshot and print the drift "
        "report; changes nothing; exits 1 on drift, 2 when the check fails",
    )
    choice.add_argument(
        "--fetch",
        action="store_true",
        help="replace the vendored snapshot with the live vendor sources (reads public "
        "Massive URLs)",
    )
    options = parser.parse_args(argv)
    if options.check:
        return _check()
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


def _check() -> int:
    """Run the drift check and print its report.

    Returns:
        0 without drift, 1 with drift, 2 when the check could not be made (the reason is
        printed to standard error and no report to standard output).
    """
    try:
        report = drift.check()
    except (RefreshError, fetch.FetchError, WireError) as error:
        sys.stderr.write(f"the drift check could not be made: {error}\n")
        return _CHECK_FAILED
    sys.stdout.write(report.text())
    return _DRIFT if report.items else 0


if __name__ == "__main__":
    raise SystemExit(main())
