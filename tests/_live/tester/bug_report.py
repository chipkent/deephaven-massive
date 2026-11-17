"""The tester's bug report bundle: the logs, the diagnostics, and the machine, in one clean zip.

When something goes wrong, `docs/tester-guide.md` asks the tester to save the session's
diagnostics tables (the example `24_save_diagnostics.py`), write the report from the guide's
template into `bug-report.md`, and run `uv run python scripts/tester.py bug-report`. That
command calls `build_bundle`, which collects from the tester's folder (`~/massive-test` by
default):

- `bug-report.md`, the tester's own report;
- `setup-check.txt`, what `scripts/tester.py check` printed last: the console's versions,
  whether both setups hold the same commit, and the plan of each subscription;
- `massive-debug.log`, the library's debug log, and `server.log`, the Deephaven server's
  output;
- `diagnostics/`, the CSV files of `m.problems()`, `m.streams()`, `m.connections()`,
  `m.rate_limits()`, and `m.symbol_reads()`;
- `examples-output/`, what `scripts/tester.py examples` saved;
- `environment.json`, which it writes: the operating system, Python, Java, the commit, the
  library's and its dependencies' versions, which secret variables are set (never their
  values), and the machine checks of the live-run program (`machine_checks.check_machine`).

**The secret rules.** Only text files are collected (`TEXT_SUFFIXES`); a file larger than
`MAX_FILE_BYTES` keeps only its end. Each passes through the live-run program's secret
filter (`SecretFilter.clean`): the exact values of `MASSIVE_API_KEY`,
`MASSIVE_DELAYED_API_KEY`, `MASSIVE_S3_ACCESS_KEY_ID`, and `MASSIVE_S3_SECRET_ACCESS_KEY`
(plain and URL-encoded) and a proxy URL's password become `***`, as do the values of `apiKey=`
parameters and of `Authorization` and `X-Amz-` headers. Then every collected file is scanned
for each secret, plain, URL-encoded, and base64-encoded (`secret_scrub.scan`), and when one is
found, no zip is written and the finds are returned instead. The command refuses to run
without `MASSIVE_API_KEY`, since a scan for no secret would prove nothing.

The zip is `bug-report-<UTC time>.zip` in the tester's folder; the staging folder it is built
from is removed either way.
"""

from __future__ import annotations

import json
import shutil
import zipfile
from typing import TYPE_CHECKING, Final

from tests._live.live_run.secret_scrub import scan

if TYPE_CHECKING:
    from pathlib import Path

    from tests._live.live_run.secret_scrub import SecretFilter

COLLECTED: Final = (
    "bug-report.md",
    "setup-check.txt",
    "massive-debug.log",
    "server.log",
    "diagnostics",
    "examples-output",
)
"""What the bundle collects from the tester's folder: files, and folders taken whole."""

TEXT_SUFFIXES: Final = frozenset({".md", ".log", ".txt", ".csv", ".json"})
"""The kinds of file collected; anything else (a Parquet file, a zip) is left out."""

MAX_FILE_BYTES: Final = 20 << 20
"""The most of one file collected: a longer file keeps only its last bytes."""

STAGING: Final = ".bug-report-staging"
"""The folder, inside the tester's folder, where the bundle is assembled."""


def collected_files(folder: Path) -> list[Path]:
    """List the files the bundle takes from the tester's folder.

    Args:
        folder: The tester's folder.

    Returns:
        The text files among `COLLECTED`, those inside a collected folder included, in order.
    """
    found: list[Path] = []
    for name in COLLECTED:
        path = folder / name
        candidates = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
        found.extend(p for p in candidates if p.is_file() and p.suffix in TEXT_SUFFIXES)
    return found


def tail_text(path: Path, limit: int = MAX_FILE_BYTES) -> str:
    """Read a text file, keeping only its end when it is long.

    Args:
        path: The file.
        limit: The most bytes kept.

    Returns:
        The text, undecodable bytes replaced; a long file's last `limit` bytes after a line
        that says how much was left out.
    """
    data = path.read_bytes()
    if len(data) <= limit:
        return data.decode("utf-8", errors="replace")
    kept = data[-limit:].decode("utf-8", errors="replace")
    return f"[the first {len(data) - limit} bytes were left out]\n{kept}"


def build_bundle(
    folder: Path, *, environment: dict[str, object], secrets: SecretFilter, stamp: str
) -> tuple[Path | None, list[str]]:
    """Collect, clean, scan, and zip the tester's bug report.

    Args:
        folder: The tester's folder.
        environment: The machine's description, written as `environment.json`.
        secrets: The secrets to remove and then look for.
        stamp: The UTC time that names the zip, such as `20261030T141500Z`.

    Returns:
        The zip and no finds; or None and every find, when a secret survived the cleaning and
        nothing was zipped.
    """
    staging = folder / STAGING
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        for path in collected_files(folder):
            target = staging / path.relative_to(folder)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(secrets.clean(tail_text(path)), encoding="utf-8")
        text = json.dumps(environment, indent=1, default=str)
        (staging / "environment.json").write_text(secrets.clean(text) + "\n", encoding="utf-8")
        finds = scan(staging, secrets)
        if finds:
            return None, finds
        target = folder / f"bug-report-{stamp}.zip"
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(p for p in staging.rglob("*") if p.is_file()):
                archive.write(path, path.relative_to(staging).as_posix())
        return target, []
    finally:
        shutil.rmtree(staging, ignore_errors=True)
