"""The vendored snapshot of Massive's published specifications: layout, manifest, and loading.

The refresh never reads the network to generate. Its vendor input is a snapshot directory,
`vendor/massive/` at the top of a source checkout (`DIRECTORY`), which `fetch` replaces and
`Snapshot.load` reads. The files there are Massive's published content, vendored only as
generation input: they are not covered by the repository's license and go in neither the
wheel nor the sdist, so the refresh runs from a source checkout only. The layout is:

- `openapi.json`: the REST specification;
- `llms.txt`: the documentation index;
- `rest/<area>/.../<page>.md`: every REST documentation page the index links;
- `manifest.json`: the fetch date and, for each file above, its URL, the SHA-256 of the bytes
  as fetched, and the SHA-256 of the file as stored.

This module is the one owner of that format. `store` writes a snapshot with its manifest and
swaps it into place atomically; `Snapshot.load` reads one back, verifying every file against
the manifest, so a hand edit of a vendor file (to get a gate past, say) stops the refresh
instead of reaching the generated schemas. How a download is turned into a stored file (the
canonical forms) is `fetch`'s business; what a page says is `docs`'s.

It imports the standard library, `docs`, and `gates` only.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import docs
from deephaven_massive._massive.refresh.gates import Gate, Problem, RefreshError

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json, JsonObject

__all__ = [
    "DIRECTORY",
    "LLMS",
    "MANIFEST",
    "OPENAPI",
    "REST_PAGES",
    "Snapshot",
    "VendorFile",
    "page_file",
    "store",
]

DIRECTORY: Final = Path(__file__).parents[4] / "vendor" / "massive"
"""The vendored snapshot: `vendor/massive/` at the top of the source checkout."""

OPENAPI: Final = "openapi.json"
"""The snapshot file of the REST specification."""

LLMS: Final = "llms.txt"
"""The snapshot file of the documentation index."""

REST_PAGES: Final = "rest"
"""The snapshot directory of the REST documentation pages."""

MANIFEST: Final = "manifest.json"
"""The snapshot file that records where every other file came from."""


# The files an operating system's file browser leaves in a folder it shows.
_OS_LITTER: Final = frozenset({".DS_Store", "Thumbs.db", "desktop.ini"})


def page_file(url: str) -> str:
    """Return where the snapshot stores a REST documentation page.

    `fetch` writes each page here and `Snapshot.load` reads it back, so the layout has one
    owner. A URL that is not a REST documentation page raises `docs.DocsError`
    (`docs.page_file` checks it).

    Args:
        url: A page URL the documentation index links.

    Returns:
        The page's name relative to the snapshot directory, `rest/<area>/.../<page>.md`.
    """
    return f"{REST_PAGES}/{docs.page_file(url)}"


@dataclass(frozen=True, kw_only=True)
class VendorFile:
    """One snapshot file, downloaded and ready to store.

    Attributes:
        name: The path relative to the snapshot directory, with `/` separators.
        url: Where it was downloaded from.
        raw: The bytes as downloaded.
        stored: The canonical bytes to write.
    """

    name: str
    url: str
    raw: bytes
    stored: bytes


def _sha256(data: bytes) -> str:
    """Return the hexadecimal SHA-256 digest of `data`.

    Args:
        data: The bytes to hash.

    Returns:
        64 lower-case hexadecimal digits.
    """
    return hashlib.sha256(data).hexdigest()


def _manifest(files: Sequence[VendorFile], fetched: dt.date) -> bytes:
    """Return the manifest of a snapshot, in canonical JSON.

    Args:
        files: The snapshot's files.
        fetched: The day of the fetch.

    Returns:
        `{"fetched": "<YYYY-MM-DD>", "files": {<name>: {"url", "fetched_sha256",
        "stored_sha256"}}}`, keys sorted, with a one-space indent and a final newline.
    """
    document: Mapping[str, object] = {
        "fetched": fetched.isoformat(),
        "files": {
            file.name: {
                "url": file.url,
                "fetched_sha256": _sha256(file.raw),
                "stored_sha256": _sha256(file.stored),
            }
            for file in files
        },
    }
    return (json.dumps(document, sort_keys=True, indent=1) + "\n").encode()


def store(directory: Path, files: Sequence[VendorFile], *, fetched: dt.date) -> list[str]:
    """Replace the snapshot in `directory` with `files` and their manifest.

    The new snapshot is written to a sibling directory (`<name>-incoming`); the old one is
    renamed aside (`<name>-previous`), the new one is renamed into place, and only then is the
    old one removed. At no point is the snapshot directory half written. A staging directory an
    interrupted run left behind is removed first, and so is its backup, unless the run stopped
    mid-swap with no snapshot in place: then the backup is restored first. Files of the old
    snapshot that `files` lacks (a page Massive removed) are gone afterwards.

    Args:
        directory: The snapshot directory, normally `DIRECTORY`. It need not exist yet.
        files: Every file of the new snapshot but the manifest, with unique names.
        fetched: The fetch date written to the manifest.

    Returns:
        The names of the stored files, relative to `directory`, manifest last.
    """
    staging = directory.with_name(f"{directory.name}-incoming")
    backup = directory.with_name(f"{directory.name}-previous")
    if backup.exists() and not directory.exists():
        # An earlier run stopped between moving the old snapshot aside and moving the new one
        # in: the backup is the only copy, so it goes back before anything is cleaned up.
        backup.rename(directory)
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(backup, ignore_errors=True)
    for file in files:
        target = staging / file.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(file.stored)
    staging.mkdir(parents=True, exist_ok=True)
    (staging / MANIFEST).write_bytes(_manifest(files, fetched))
    if directory.exists():
        directory.rename(backup)
    staging.rename(directory)
    shutil.rmtree(backup, ignore_errors=True)
    return [file.name for file in files] + [MANIFEST]


@dataclass(frozen=True, kw_only=True)
class Snapshot:
    """The vendor sources the generators read, already parsed and verified.

    Created by `load` from a snapshot directory, or by a test directly. Immutable.

    Example:
        ::

            from deephaven_massive._massive.refresh.snapshot import Snapshot

            snapshot = Snapshot.load()
            print(len(snapshot.pages), snapshot.fetched)

    Attributes:
        openapi: The OpenAPI specification.
        llms: The text of the documentation index.
        pages: Each REST page the index links, by its name below the snapshot's `rest/`
            directory (`"stocks/trades-quotes/trades.md"`), as text.
        fetched: The day the snapshot was downloaded, from the manifest. It is written into
            `descriptions.json`, so the generated files name the snapshot they came from.
        urls: Each stored file, by its name relative to the snapshot directory
            (`"rest/stocks/trades-quotes/trades.md"`), mapped to the URL it was downloaded
            from, from the manifest.
    """

    openapi: JsonObject
    llms: str
    pages: Mapping[str, str]
    fetched: dt.date
    urls: Mapping[str, str]

    @classmethod
    def load(cls, directory: Path | None = None) -> Snapshot:
        """Read a snapshot directory written by `store`, verifying it against its manifest.

        Every file is hashed again and compared with the SHA-256 the manifest recorded when it
        was stored, and the files on disk must be exactly the files the manifest lists.

        Args:
            directory: The snapshot directory; `DIRECTORY` when omitted, read when the method
                runs.

        Returns:
            The parsed snapshot.

        Raises:
            RefreshError: If the directory does not exist (the refresh runs from a source
                checkout only), the manifest or the specification is not a JSON object, the
                manifest's fetch date is not an ISO date, a file's hash differs from the
                manifest's or the manifest records no URL for it, a file is on disk but not in
                the manifest or the reverse, or a REST page the index links is missing.
        """
        directory = directory or DIRECTORY
        if not directory.is_dir():
            raise RefreshError(
                [
                    Problem(
                        gate=Gate.SNAPSHOT,
                        source="",
                        key=str(directory),
                        message="no vendored snapshot here: run the refresh from a source "
                        "checkout, which has vendor/massive/",
                    )
                ]
            )
        manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
        openapi = json.loads((directory / OPENAPI).read_text(encoding="utf-8"))
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if not isinstance(files, dict) or not isinstance(openapi, dict):
            raise RefreshError(
                [Problem(gate=Gate.SNAPSHOT, source="", key="", message="not a JSON object")]
            )
        problems = list(_verify(directory, files))
        try:
            fetched = dt.date.fromisoformat(manifest.get("fetched"))
        except (TypeError, ValueError):
            fetched = None
            problems.append(
                Problem(
                    gate=Gate.SNAPSHOT,
                    source="",
                    key=MANIFEST,
                    message=f"the fetch date {manifest.get('fetched')!r} is not an ISO date",
                )
            )
        llms = (directory / LLMS).read_text(encoding="utf-8")
        pages: dict[str, str] = {}
        for url in docs.rest_page_urls(llms):
            stored = page_file(url)
            name = stored.removeprefix(f"{REST_PAGES}/")
            path = directory / stored
            if path.is_file():
                pages[name] = path.read_text(encoding="utf-8")
            else:
                problems.append(
                    Problem(
                        gate=Gate.DOCS,
                        source="",
                        key=name,
                        message=f"the index links {url} but the snapshot lacks the page",
                    )
                )
        if problems or fetched is None:
            raise RefreshError(problems)
        urls = {
            name: link
            for name, entry in files.items()
            if isinstance(entry, dict) and isinstance(link := entry.get("url"), str)
        }
        return cls(
            openapi=openapi,
            llms=llms,
            pages=pages,
            fetched=fetched,
            urls=urls,
        )


def _os_litter(name: str) -> bool:
    """Return whether a file name is one an operating system's file browser leaves behind.

    Finder writes `.DS_Store` and `._*` (AppleDouble) files, and Windows Explorer `Thumbs.db`
    and `desktop.ini`, merely when a person views a folder. No vendor file can have such a
    name (every stored name is fixed or a lower-case `.md` page below `rest/`), so ignoring
    them hides no edit; any other stray file still stops the refresh.

    Args:
        name: A file's name, without its directory.

    Returns:
        True for those names.
    """
    return name in _OS_LITTER or name.startswith("._")


def _verify(directory: Path, files: Mapping[str, Json]) -> Iterator[Problem]:
    """Compare a snapshot's files with its manifest, ignoring operating-system litter.

    Args:
        directory: The snapshot directory.
        files: The manifest's `files` object: name to `{"stored_sha256", ...}`.

    Yields:
        A `snapshot` problem for each file whose hash differs from the manifest's or whose URL
        the manifest does not record, each file on disk the manifest does not list, and each
        listed file that is missing.
    """
    on_disk = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*")
        if path.is_file() and path.name != MANIFEST and not _os_litter(path.name)
    }
    for name in sorted(on_disk - set(files)):
        yield Problem(gate=Gate.SNAPSHOT, source="", key=name, message="not in the manifest")
    for name, listed in sorted(files.items()):
        entry = listed if isinstance(listed, dict) else {}
        recorded = entry.get("stored_sha256")
        if name not in on_disk:
            yield Problem(gate=Gate.SNAPSHOT, source="", key=name, message="listed but missing")
        elif _sha256((directory / name).read_bytes()) != recorded:
            yield Problem(
                gate=Gate.SNAPSHOT,
                source="",
                key=name,
                message="its SHA-256 differs from the manifest's: a vendor file was edited; "
                "refetch instead",
            )
        elif not isinstance(entry.get("url"), str):
            yield Problem(
                gate=Gate.SNAPSHOT, source="", key=name, message="the manifest records no URL"
            )
