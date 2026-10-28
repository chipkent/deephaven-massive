"""The column reference: the generated `docs/datasets/<namespace>.md` pages, and their sync.

Every dataset's columns are documented in three places, all read from the same dataset schema
(`Catalog.dataset_schema`), so they never disagree: the column descriptions every result
carries (`_tables/columns.py`), the static table of the queries' `columns()`, and these pages,
which readers browse on GitHub before they install anything. This module owns the pages:

- `render` turns a catalog and the provenance of the generated sources into one Markdown page
  per namespace module (`stocks.md`, ..., `partners.md`), one section per dataset in
  declaration order. It is pure: it reads only its arguments and the schemas the catalog
  merges, imports no Deephaven (a type cell is the `ColumnType` value), and writes nothing.
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
its namespace with dots written as hyphens (`trades`, `financials-ratios`, `benzinga-news`),
because GitHub drops the dots of a heading's own anchor. Each dataset method's docstring links
its section by that anchor rather than listing columns.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import PurePath
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.schema import RAW_SYMBOL, TRANSPORT_LABELS
from deephaven_massive._massive import Transport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import Column
    from deephaven_massive._massive import Provenance

__all__ = ["COMMAND", "SyncResult", "anchor", "call_path", "render", "sync"]

COMMAND: Final = "uv run python scripts/render_reference.py"
"""The command that regenerates the column reference, run from the repository root."""

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

_SESSION_METHODS: Final[Mapping[str, str]] = {"reference.snapshots": "m.snapshots"}
"""The datasets called as a method of the session itself rather than of their namespace. The
unified snapshot spans asset classes, so it is `m.snapshots(...)`; its record is
`reference.snapshots`, and its section is on the reference page."""

_SESSION_METHOD_NOTE: Final = (
    "Called as `{call}`, a method of the session itself rather than of `m.{namespace}`; its "
    "catalog record is `{name}`, so its columns are documented on this page."
)
"""The line under the summary of a dataset that is a method of the session itself, which says
why its section is on its namespace's page."""

_GUIDE: Final = (
    "Each section lists one dataset's columns in the order every verb returns them. "
    "**Supplied by** names the transports that fill a column; a column the serving transport "
    "does not fill is null. The wire-field columns name the Massive fields behind each column "
    "(`request` for a value the library fills from the request). The same descriptions appear "
    "in the web UI's column tooltips and in a query's `columns()`."
)
"""The paragraph under every page's title."""

_RAW_SYMBOL_NOTE: Final = (
    "In a session with `unknown_symbols=dm.UnknownSymbols.WARN` (or "
    "`MASSIVE_UNKNOWN_SYMBOLS=warn`), {datasets} also have a `RawSymbol` String column, which "
    "holds Massive's spelling of a symbol the library cannot read; that row's `Symbol` is "
    "null, and `RawSymbol` is null on every other row. The tables below show the default "
    "session, without it."
)
"""The paragraph under the guide of a page whose datasets read their symbol from rows,
formatted with those datasets' call paths."""

_PROVENANCE: Final = (
    "Generated by `{command}` from Massive's documentation as fetched on {fetched}. The "
    "descriptions are Massive's, corrected where they are clearly wrong; the library writes "
    "its own for the key columns and for columns that only other datasets of the same kind "
    "fill."
)
"""The first sentence of every page's footer, formatted with the command and the fetch date."""


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
        `"benzinga-news"` for `partners.benzinga.news`.
    """
    return spec.name.partition(".")[2].replace(".", "-")


def call_path(spec: DatasetSpec) -> str:
    """Return how a user reaches a dataset from a session `m`, which its section's heading shows.

    Args:
        spec: The dataset.

    Returns:
        `m.` and the dataset's name, such as `"m.stocks.trades"`; for a dataset that is a
        method of the session itself, that method, such as `"m.snapshots"` for
        `reference.snapshots`.
    """
    return _SESSION_METHODS.get(spec.name, f"m.{spec.name}")


def render(catalog: Catalog, provenance: Provenance) -> dict[str, str]:
    """Render the column reference: one Markdown page per namespace module with datasets.

    Call it to regenerate the pages (the command passes them to `sync`) or to compare them
    with the committed ones (the parity test). A page starts with a title and a short guide,
    then, when some of its datasets read their symbol from rows, a note that names them and
    the `RawSymbol` column they gain in a session with `UnknownSymbols.WARN` (the tables show
    the default session), then holds one section per dataset of the namespace, in declaration
    order: the anchor
    (`anchor`), a heading with the dataset's call path (`call_path`), its
    `DatasetSpec.summary`, and a table with one row per column in result order. The table's
    columns are **Column**, **Deephaven type** (the `ColumnType` value, such as `Instant`),
    **Description**, **Supplied by** (the transports that fill the column, as `REST`,
    `flat files`, and `WebSocket`, empty for a column only other datasets of its kind fill),
    and one wire-field column per transport the dataset reads (`REST field`,
    `Flat-file field`, `WebSocket field`), each listing the distinct wire keys behind the
    column in source order, or `request` for a key filled from the request. A footer gives
    the date Massive's documentation was fetched and the sorted, distinct URLs of the
    documentation of every source the page's datasets read.

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
        The page text by file name, `"<namespace>.md"`, in the order the namespaces first
        appear in the catalog; empty for an empty catalog.

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
    """Render one namespace's page.

    Args:
        namespace: The namespace, such as `"stocks"`.
        specs: Its datasets, in declaration order; at least one.
        catalog: The catalog that merges their schemas.
        provenance: The fetch date and the sources' URLs.

    Returns:
        The page text, ending in one newline.
    """
    lines = [
        f"# Columns of the `m.{namespace}` datasets",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        _GUIDE,
    ]
    kept = [
        f"`{call_path(spec)}`"
        for spec in specs
        if RAW_SYMBOL in catalog.dataset_schema(spec, raw_symbol=True).column_names
    ]
    if kept:
        named = " and ".join(kept[-2:])
        if kept[:-2]:
            named = ", ".join([*kept[:-2], named.replace(" and ", ", and ")])
        lines.extend(("", _RAW_SYMBOL_NOTE.format(datasets=named)))
    for spec in specs:
        lines.extend(("", *_section(spec, catalog)))
    lines.extend(("", *_footer(specs, provenance)))
    return "\n".join(lines) + "\n"


def _section(spec: DatasetSpec, catalog: Catalog) -> list[str]:
    """Render one dataset's section: its anchor, heading, summary, and column table.

    Args:
        spec: The dataset.
        catalog: The catalog that merges its schema.

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
        *_session_note(spec),
        _row(header),
        _row(["---"] * len(header)),
    ]
    lines.extend(_row(_cells(column, wire)) for column in catalog.dataset_schema(spec).columns)
    return lines


def _session_note(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the lines that explain why a session method's section is on its namespace page.

    Args:
        spec: The dataset.

    Returns:
        For a dataset that is a method of the session itself (`m.snapshots`), the note
        (`_SESSION_METHOD_NOTE`) and a blank line; for every other dataset, nothing.
    """
    if spec.name not in _SESSION_METHODS:
        return ()
    namespace = spec.name.partition(".")[0]
    note = _SESSION_METHOD_NOTE.format(call=call_path(spec), namespace=namespace, name=spec.name)
    return (note, "")


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


def _footer(specs: Sequence[DatasetSpec], provenance: Provenance) -> list[str]:
    """Render a page's footer: the snapshot's fetch date and its sources' documentation URLs.

    Args:
        specs: The page's datasets.
        provenance: The fetch date and each source's URLs.

    Returns:
        The footer's lines: a rule, the provenance sentence, and, when any source has URLs,
        the sorted, distinct URLs as a list.
    """
    urls = sorted(
        {
            url
            for spec in specs
            for source in spec.sources
            for url in provenance.urls.get(source, ())
        }
    )
    fetched = provenance.fetched.isoformat()
    lines = ["---", "", _PROVENANCE.format(command=COMMAND, fetched=fetched)]
    if urls:
        lines.extend(("", "Massive's documentation of these datasets:", ""))
        lines.extend(f"- <{url}>" for url in urls)
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
