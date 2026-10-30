"""The column reference: the generated `docs/datasets/<namespace>.md` pages, and their sync.

Every dataset's columns are documented in three places, all read from the same dataset schema
(`Catalog.dataset_schema`), so they never disagree: the column descriptions every result
carries (`_tables/columns.py`), the static table of the queries' `columns()`, and these pages,
which readers browse on GitHub before they install anything. This module owns the pages:

- `render` turns a catalog and the provenance of the generated sources into one Markdown page per
  namespace (`stocks.md`, ..., `partners.md`), one section per dataset in declaration order, and one
  page for each dataset the session serves itself (`snapshots.md`). It is pure: it reads only its
  arguments and the schemas the catalog merges, imports no Deephaven (a type cell is the
  `ColumnType` value), and writes nothing.
- `column_changes` compares two renderings and says, per dataset, which columns a change
  added or removed, so a refresh or a catalog change shows what it does to each table.
- `sync` makes a directory hold exactly the rendered pages: it deletes every other file and
  subdirectory, and rewrites only the pages whose bytes differ. It is the only function of the
  dataset layer that touches the file system; it makes no network or Deephaven call.

The one command that writes the pages, `uv run python scripts/render_reference.py`, starts the
embedded Deephaven server (the catalog lives in `_session.py`, which imports Deephaven), then
calls `render` on the process's catalog and `sync` on `docs/datasets/`. Since the command also
empties the directory of everything it did not render, a renamed or removed dataset or
namespace never leaves an orphaned page. The parity test, `tests/datasets/test_column_reference.py`,
fails on any page that differs from what `render` gives, so a catalog change or a refresh that
moves a description lands with its regenerated pages.

Each section starts with an explicit `<a id="<dataset>"></a>` anchor, the dataset's name below
its namespace with dots written as hyphens (`trades`, `financials-ratios`, `benzinga-news`), or
the whole name of a dataset with no namespace (`snapshots`), because GitHub drops the dots of a
heading's own anchor. Each dataset method's docstring links
its section by that anchor rather than listing columns.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import PurePath
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.schema import RAW_SYMBOL, TRANSPORT_LABELS
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._massive import Transport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import Column
    from deephaven_massive._massive import Provenance

__all__ = [
    "COMMAND",
    "PAGES_URL",
    "SyncResult",
    "anchor",
    "call_path",
    "column_changes",
    "render",
    "section_url",
    "sync",
]

COMMAND: Final = "uv run python scripts/render_reference.py"
"""The command that regenerates the column reference, run from the repository root."""

PAGES_URL: Final = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
"""Where the committed pages are read on GitHub; a page's URL is this and its file name."""

_TRANSPORT_ORDER: Final = (Transport.REST, Transport.FLAT_FILES, Transport.WS)
"""The order of the transports in a **Supplied by** cell and of the wire-field columns, the
order of `Column.supplied_by`."""

_WIRE_HEADINGS: Final[Mapping[Transport, str]] = {
    Transport.REST: "REST field",
    Transport.FLAT_FILES: "Flat-file field",
    Transport.WS: "WebSocket field",
}
"""The heading of each transport's wire-field column."""

_FROM_REQUEST: Final = "request"
"""The wire-field cell of a key the library fills from the request rather than from a field."""

_GUIDE: Final = (
    "Each section lists one dataset's columns in the order every verb returns them. "
    "**Supplied by** names the transports that fill a column; a column the serving transport "
    "does not fill is null. The wire-field columns name the Massive fields behind each column "
    "(`request` for a value the library fills from the request). The same descriptions appear "
    "in the web UI's column tooltips and in a query's `columns()`."
)
"""The paragraph under every page's title."""

_MERGE: Final = (
    "A dataset has only the columns it can fill, so two asset classes' tables of one kind "
    "(stock and forex quotes, say) can differ in their columns, while the columns they share "
    "have one name and type. To merge them, keep the shared columns: "
    "`common = [c for c in a.column_names if c in b.column_names]`, then "
    "`merge([t.view(common) for t in (a, b)])`. To keep every column instead, add each "
    'missing one to the other table as a typed null first, such as `b.update("Tape = '
    'NULL_LONG")`.'
)
"""The paragraph after the guide on a page with a dataset whose kind other datasets share: how to
merge tables of different classes."""

_RAW_SYMBOL_NOTE: Final = (
    "{datasets} {have} a `RawSymbol` String column, which holds Massive's spelling of a "
    "symbol the library cannot read; that row's `Symbol` is null, and `RawSymbol` is null on "
    "every other row. It is the one column whose presence depends on a setting: a session "
    "with `unreadable_symbols=dm.OnError.RAISE` (or `DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS=raise`) "
    "fails on such a row instead and has no `RawSymbol`. The tables below show the default "
    "session, with it."
)
"""The paragraph under the guide of a page whose datasets read their symbol from rows,
formatted with those datasets' call paths and the verb that agrees with their number."""

_PROVENANCE: Final = (
    "Generated by `{command}` from Massive's documentation as fetched on {fetched}. The "
    "descriptions are Massive's, corrected where they are clearly wrong; the library writes "
    "its own for the key columns and the columns it computes."
)
"""Every page's footer, formatted with the command and the fetch date."""

_LINKS_HEADING: Final = "Massive's documentation of this dataset:"
"""The line that introduces the list of a dataset's vendor pages under its column table."""


@dataclass(frozen=True, slots=True)
class SyncResult:
    """What one `sync` changed in its directory.

    Created by `sync` and returned to its caller, which prints it; immutable.

    Attributes:
        written: The file names of the pages written, because they were missing or their bytes
            differed, sorted.
        deleted: The names of the entries removed from the directory (files, symbolic links,
            and whole subdirectories) because no page has their name, or because a page's
            name was taken by something other than a regular file, sorted.
    """

    written: tuple[str, ...]
    deleted: tuple[str, ...]


def anchor(spec: DatasetSpec) -> str:
    """Return the id of a dataset's section on its namespace page.

    Dataset docstrings link the section as `docs/datasets/<namespace>.md#<anchor>`.

    Args:
        spec: The dataset.

    Returns:
        The dataset's name below its namespace with dots written as hyphens: `"trades"` for
        `stocks.trades`, `"financials-ratios"` for `stocks.financials.ratios`, and
        `"benzinga-news"` for `partners.benzinga.news`; the whole name of a dataset the session
        serves itself, which has no namespace: `"snapshots"`.
    """
    return (spec.name.partition(".")[2] or spec.name).replace(".", "-")


def section_url(spec: DatasetSpec) -> str:
    """Return the web address of a dataset's section of the column reference.

    Args:
        spec: The dataset.

    Returns:
        `PAGES_URL`, the namespace's page, and the section's `anchor`, such as
        `"https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#trades"`.
    """
    return f"{PAGES_URL}{spec.name.partition('.')[0]}.md#{anchor(spec)}"


def call_path(spec: DatasetSpec) -> str:
    """Return how a user reaches a dataset from a session `m`, which its section's heading shows.

    Args:
        spec: The dataset.

    Returns:
        `m.` and the dataset's name, such as `"m.stocks.trades"`, or `"m.snapshots"` for a
        dataset the session serves itself.
    """
    return f"m.{spec.name}"


def render(catalog: Catalog, provenance: Provenance) -> dict[str, str]:
    """Render the column reference: one Markdown page per namespace, and per session dataset.

    Call it to regenerate the pages (the command passes them to `sync`) or to compare them with
    the committed ones (the parity test). A dataset with no namespace, which the session serves
    itself (`m.snapshots`), has a page of its own, named after it. A page starts with a title, a
    short guide, and, where a dataset's kind has other members, the recipe that merges tables of
    different asset classes on their shared columns, then, when some of its datasets read their
    symbol from rows, a note that names them and their `RawSymbol` column, which a session with
    `unreadable_symbols=dm.OnError.RAISE` lacks (the tables show the default session, with it),
    then holds one section per dataset of the namespace, in declaration order: the anchor
    (`anchor`), a heading with the dataset's call path (`call_path`), its
    `DatasetSpec.summary`, and a table with one row per column in result order. The table's
    columns are **Column**, **Deephaven type** (the `ColumnType` value, such as `Instant`),
    **Description**, **Supplied by** (the transports that fill the column, as `REST`,
    `flat files`, and `WebSocket`), and one wire-field column per transport the dataset reads
    (`REST field`, `Flat-file field`, `WebSocket field`), each listing the distinct wire keys
    behind the column in source order, or `request` for a key filled from the request. Under
    the table comes the sorted, distinct list of Massive's pages documenting the dataset's own
    sources (`_dataset_urls`), omitted when they have none. A footer gives the generator command
    and the date Massive's documentation was fetched.

    A `|` in a cell is escaped with a backslash, no line has trailing whitespace, and every
    page ends in exactly one newline, so the pages pass the repository's commit hooks
    unchanged.

    Pure: no I/O beyond the wire package's one-time load of the generated files that the
    catalog's schema merge may trigger, no Deephaven call, and the same output for the same
    arguments.

    Args:
        catalog: Every dataset to document, in declaration order; the process's catalog for
            the real pages, or a synthetic one in tests.
        provenance: The fetch date and the vendor URLs of the generated sources, as
            `_massive.provenance()` returns them; a source without URLs adds none.

    Returns:
        The page text by file name, `"<namespace>.md"` (or `"<dataset>.md"` for a dataset
        with no namespace), in the order the pages' datasets first appear in the catalog;
        empty for an empty catalog.

    The schema merge's `ValueError` or `KeyError` (`Catalog.dataset_schema`) propagates for a
    catalog whose datasets do not merge.
    """
    by_namespace: dict[str, list[DatasetSpec]] = {}
    for spec in catalog.by_name.values():
        by_namespace.setdefault(spec.name.partition(".")[0], []).append(spec)
    return {
        f"{namespace}.md": _page(namespace, specs, catalog=catalog, provenance=provenance)
        for namespace, specs in by_namespace.items()
    }


def _page(
    namespace: str, specs: Sequence[DatasetSpec], *, catalog: Catalog, provenance: Provenance
) -> str:
    """Render one namespace's page, or the page of one dataset with no namespace.

    Args:
        namespace: The namespace, such as `"stocks"`, or the name of the dataset with no
            namespace, such as `"snapshots"`.
        specs: Its datasets, in declaration order; at least one.
        catalog: The catalog that merges their schemas.
        provenance: The fetch date and the sources' URLs.

    Returns:
        The page text, ending in one newline.
    """
    title = f"the `m.{namespace}` datasets" if "." in specs[0].name else f"`m.{namespace}`"
    lines = [
        f"# Columns of {title}",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        _GUIDE,
    ]
    # Only a dataset of a kind shared with other datasets has tables to merge across classes.
    if any(spec.kind is not None and len(catalog.members(spec.kind)) > 1 for spec in specs):
        lines.extend(("", _MERGE))
    kept = [
        f"`{call_path(spec)}`"
        for spec in specs
        if RAW_SYMBOL in catalog.dataset_schema(spec).column_names
    ]
    if kept:
        named = " and ".join(kept[-2:])
        if kept[:-2]:
            named = ", ".join([*kept[:-2], named.replace(" and ", ", and ")])
        have = "have" if kept[1:] else "has"
        lines.extend(("", _RAW_SYMBOL_NOTE.format(datasets=named, have=have)))
    for spec in specs:
        lines.extend(("", *_section(spec, catalog, provenance)))
    fetched = provenance.fetched.isoformat()
    lines.extend(("", "---", "", _PROVENANCE.format(command=COMMAND, fetched=fetched)))
    return "\n".join(lines) + "\n"


def _section(spec: DatasetSpec, catalog: Catalog, provenance: Provenance) -> list[str]:
    """Render one dataset's section: anchor, heading, summary, column table, and vendor pages.

    Args:
        spec: The dataset.
        catalog: The catalog that merges its schema.
        provenance: The URLs of each source, filtered by `_dataset_urls`.

    Returns:
        The section's lines, without a trailing blank line.
    """
    used = {transport for transport, _ in spec.sources}
    wire = [transport for transport in _TRANSPORT_ORDER if transport in used]
    header = [
        "Column",
        "Deephaven type",
        "Description",
        "Supplied by",
        *(_WIRE_HEADINGS[t] for t in wire),
    ]
    lines = [
        f'<a id="{anchor(spec)}"></a>',
        "",
        f"## `{call_path(spec)}`",
        "",
        spec.summary,
        "",
        _row(header),
        _row(["---"] * len(header)),
    ]
    schema = catalog.dataset_schema(spec)  # the default session's columns, RawSymbol included
    lines.extend(_row(_cells(column, wire)) for column in schema.columns)
    urls = _dataset_urls(spec, provenance)
    if urls:
        lines.extend(("", _LINKS_HEADING, ""))
        lines.extend(f"- <{url}>" for url in urls)
    return lines


def _cells(column: Column, wire: Sequence[Transport]) -> list[str]:
    """Return the cells of one column's row, before escaping.

    Args:
        column: The schema column.
        wire: The transports the dataset reads, in wire-column order.

    Returns:
        The column's name, type, description, transports, and one wire-field cell per
        transport of `wire`.
    """
    supplied = ", ".join(TRANSPORT_LABELS[t] for t in column.supplied_by)
    fields = [
        ", ".join(
            _FROM_REQUEST if key is None else f"`{key}`" for key in column.wire_keys.get(t, ())
        )
        for t in wire
    ]
    return [f"`{column.name}`", column.type.value, column.description, supplied, *fields]


def _row(cells: Sequence[str]) -> str:
    """Return one Markdown table row, with every `|` inside a cell escaped.

    Args:
        cells: The cell texts.

    Returns:
        `| a | b |`. The cells are joined by `" | "`, so an empty cell leaves two spaces
        between its pipes (`|  |`) and the row never ends in whitespace.
    """
    escaped = [cell.replace("|", "\\|") for cell in cells]
    return "| " + " | ".join(escaped) + " |"


def _dataset_urls(spec: DatasetSpec, provenance: Provenance) -> list[str]:
    """Return the vendor pages that document one dataset's sources, sorted and distinct.

    Massive documents a source several asset classes share (the unified snapshot
    `/v3/snapshot`, `/v3/reference/tickers`) once per class, and its URLs name the class as a
    path segment (`https://massive.com/docs/rest/options/snapshots/unified-snapshot.md`). A
    dataset of one asset class keeps only the pages of a source whose path has a segment equal
    to its class's value, so the options snapshot links the options page alone. When none of
    a source's pages names the class, all of them are kept: a change in the vendor's URL
    layout then lists more pages, never none. A dataset without an asset class, which spans
    classes, keeps every page.

    Args:
        spec: The dataset.
        provenance: Each source's URLs; a source without an entry adds none.

    Returns:
        The URLs, sorted and distinct; empty when no source of the dataset has any.
    """
    urls: set[str] = set()
    for source in spec.sources:
        found = provenance.urls.get(source, ())
        if spec.asset_class is not ClassFromPrefix.ANY:
            own = [url for url in found if spec.asset_class.value in _segments(url)]
            found = own or found
        urls.update(found)
    return sorted(urls)


def _segments(url: str) -> list[str]:
    """Return the slash-separated parts of a URL.

    Args:
        url: A URL, such as `"https://massive.com/docs/rest/options/trades.md"`.

    Returns:
        Its parts, such as `["https:", "", "massive.com", "docs", "rest", "options",
        "trades.md"]`. The scheme and host are parts too; no asset class's value equals
        either, so they never match one.
    """
    return url.split("/")


_HEADING: Final = re.compile(r"^## `(m\.[^`]+)`$")
"""A section's heading, which names its dataset's call path."""

_COLUMN_ROW: Final = re.compile(r"^\| `([^`]+)` \| ([^|]+) \|")
"""A row of a section's column table: its first cell names the column, its second the type."""


def _columns_by_dataset(pages: Mapping[str, str]) -> dict[str, dict[str, str]]:
    """Read each dataset's columns and their Deephaven types back from rendered pages.

    The pages are the only record of what the reference said before a change, so the report
    reads them back in the format `_section` writes: a `## `m.…`` heading per dataset, then a
    table whose rows start with the column name in backticks and its type.

    Args:
        pages: Page text by file name, as `render` returns them or as a directory holds them.

    Returns:
        Each section's columns, in order, with their types, by the call path its heading names.
    """
    found: dict[str, dict[str, str]] = {}
    current: dict[str, str] = {}
    for text in pages.values():
        for line in text.split("\n"):
            heading = _HEADING.match(line)
            if heading:
                current = found.setdefault(heading[1], {})
                continue
            row = _COLUMN_ROW.match(line)
            if row:
                current[row[1]] = row[2]
    return found


def column_changes(before: Mapping[str, str], after: Mapping[str, str]) -> list[str]:
    """Say which columns each dataset gained or lost between two renderings of the reference.

    Call it with the committed pages and the newly rendered ones (`render`), before `sync`
    replaces the first with the second: the command prints its lines, so a refresh of
    Massive's specifications or a catalog change shows what it does to each dataset's table.
    A change of a column's place or description is not reported; the pages' own diff shows it.
    Pure: reads only its arguments.

    Args:
        before: Page text by file name, as the directory held it; empty for a first rendering.
        after: Page text by file name, as `render` returns it.

    Returns:
        One line per dataset whose columns or column types changed, in the order of `after`
        and then of the datasets only `before` has: `m.forex.quotes: added Exchange; removed
        Tape; retyped Conditions int[] to long[]`, or
        `m.snapshots: new dataset with 74 columns`, or `m.reference.snapshots: dataset
        removed`. Empty when no dataset's columns changed.
    """
    old = _columns_by_dataset(before)
    new = _columns_by_dataset(after)
    lines = []
    for path, columns in new.items():
        if path not in old:
            lines.append(f"{path}: new dataset with {len(columns)} columns")
            continue
        was = old[path]
        added = [c for c in columns if c not in was]
        removed = [c for c in was if c not in columns]
        retyped = [f"{c} {was[c]} to {t}" for c, t in columns.items() if was.get(c, t) != t]
        parts = [
            f"{verb} {', '.join(names)}"
            for verb, names in (("added", added), ("removed", removed), ("retyped", retyped))
            if names
        ]
        if parts:
            lines.append(f"{path}: {'; '.join(parts)}")
    lines.extend(f"{path}: dataset removed" for path in old if path not in new)
    return lines


def sync(directory: Path, pages: Mapping[str, str]) -> SyncResult:
    """Make `directory` hold exactly `pages`, rewriting only what differs.

    The column reference command calls it with `render`'s pages and `docs/datasets/`; the
    directory belongs to it. Every entry of `directory` that `pages` does not name is deleted:
    a file, a symbolic link (never followed), or a whole subdirectory. An entry named by a page
    but that is not a regular file (a subdirectory, or a symbolic link, which could point
    outside the directory) is deleted too, and the page written in its place. Then each page
    whose file is missing or whose bytes differ from the page's UTF-8 text is written; a page
    whose bytes already match is not touched, so its modification time stays.

    A missing `directory` counts as empty. It is created, with its parents, only when a page
    must be written, so syncing no pages to a missing directory succeeds and creates nothing.
    Apart from creating those missing parents, nothing outside `directory` is read, written,
    or deleted. `directory` itself must not be a symbolic link, since syncing through one would
    empty the link's target; a symbolic link among its parents is the caller's own layout and
    is followed as usual.

    Side effects: deletes and writes files under `directory`. No network or Deephaven call;
    not safe to run twice at once on one directory.

    Args:
        directory: The directory the pages live in, such as `docs/datasets/`.
        pages: The page text by file name, as `render` returns it. Each name is a plain file
            name, such as `"stocks.md"`.

    Returns:
        The names written and deleted. Both are empty when the directory already held exactly
        the pages.

    Raises:
        ValueError: If `directory` is a symbolic link, or a page name is not a plain file
            name (empty, `.`, `..`, or containing a path separator); both are checked before
            anything is touched.

    An `OSError` from the file system propagates, for example a `NotADirectoryError` when
    `directory` is a file; the changes made before it stay.
    """
    if directory.is_symlink():
        raise ValueError(f"{directory} is a symbolic link; sync the directory it points to")
    for name in pages:
        if name in {"", ".", ".."} or PurePath(name).name != name:
            raise ValueError(f"page name {name!r} is not a plain file name")
    try:
        entries = sorted(directory.iterdir())
    except FileNotFoundError:
        entries = []
    deleted = []
    for entry in entries:
        if entry.name in pages and entry.is_file() and not entry.is_symlink():
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()
        deleted.append(entry.name)
    written = []
    for name, text in pages.items():
        path = directory / name
        data = text.encode()
        if path.is_file() and path.read_bytes() == data:
            continue
        directory.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        written.append(name)
    return SyncResult(written=tuple(sorted(written)), deleted=tuple(deleted))
