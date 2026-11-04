"""The dataset reference: the generated `docs/datasets/` pages, and their sync.

Every dataset's columns are documented in three places, all read from the same dataset schema
(`Catalog.dataset_schema`), so they never disagree: the column descriptions every result
carries (`_tables/columns.py`), the static table of the queries' `columns()`, and these pages,
which readers browse on GitHub before they install anything. This module owns the pages:

- `render` turns a catalog, the provenance of the generated sources, and each dataset's call
  (`DatasetCall`, which the API layer supplies) into one Markdown page per dataset, named after
  its dotted name (`stocks.trades.md`, `stocks.financials.ratios.md`, `snapshots.md`), and an
  index, `README.md`, which GitHub shows when a reader opens the directory: a grid of asset
  classes by dataset and a list of every dataset by namespace, with the methods of the session
  itself (`m.snapshots`) under a heading of their own. A dataset's page says how to
  call it, its verbs, its time range, its `poll()` mode, the `sort` values and `filters` keys
  it accepts, which of Massive's plans include it (from the plan tables of Massive's own pages,
  so they follow every refresh), and its columns, with the Massive field behind each column
  folded away. It is pure: it reads only its arguments and the schemas the catalog merges,
  imports no Deephaven (a type cell is the `ColumnType` value), and writes nothing.
- `column_changes` compares two renderings and says, per dataset, which columns a change
  added or removed, so a refresh or a catalog change shows what it does to each table.
- `sync` makes a directory hold exactly the rendered pages: it deletes every other file and
  subdirectory, and rewrites only the pages whose bytes differ. It is the only function of the
  dataset layer that touches the file system; it makes no network or Deephaven call.

The one command that writes the pages, `uv run python scripts/render_reference.py`, starts the
embedded Deephaven server (the catalog lives in `_session.py`, which imports Deephaven), then
calls `render` on the process's catalog and `sync` on `docs/datasets/`. Since the command also
empties the directory of everything it did not render, a renamed or removed dataset never
leaves an orphaned page. The parity test, `tests/datasets/test_column_reference.py`, fails on
any page that differs from what `render` gives, so a catalog change or a refresh that moves a
description or a plan table lands with its regenerated pages. Each dataset method's docstring
names its page by its web address, `PAGES_URL` and `<dataset>.md`, rather than listing columns.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import PurePath
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.column_descriptions import (
    TRANSPORT_LABELS,
    TRANSPORT_ORDER,
    joined_with_and,
)
from deephaven_massive._datasets.filters import filter_keys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import AssetClass, ClassFromPrefix, PollMode
from deephaven_massive._massive import ParamKind, Transport

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from pathlib import Path

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import Column
    from deephaven_massive._massive import Provenance

__all__ = [
    "COMMAND",
    "INDEX",
    "PAGES_URL",
    "DatasetCall",
    "SyncResult",
    "call_path",
    "column_changes",
    "page_name",
    "page_url",
    "render",
    "sync",
]

COMMAND: Final = "uv run python scripts/render_reference.py"
"""The command that regenerates the dataset reference, run from the repository root."""

PAGES_URL: Final = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
"""Where the committed pages are read on GitHub; a page's URL is this and its file name."""

INDEX: Final = "README.md"
"""The index page's file name, which GitHub shows when a reader opens the directory."""

_WIRE_HEADINGS: Final[Mapping[Transport, str]] = {
    Transport.REST: "REST field",
    Transport.FLAT_FILES: "Flat-file field",
    Transport.WS: "WebSocket field",
}
"""The heading of each transport's wire-field column."""

_PAGE_AREAS: Final = (
    ("https://massive.com/docs/rest/", "REST"),
    ("https://massive.com/docs/websocket/", "WebSocket"),
    ("https://massive.com/docs/flat-files/", "flat files"),
)
"""The URL prefix of each area of Massive's documentation, and how a plan column names it."""

_FROM_REQUEST: Final = "request"
"""The wire-field cell of a key the library fills from the request rather than from a field."""

_NOT_INCLUDED: Final = "Not included"
"""Massive's word for a plan without the data, which a plan cell shows alone."""

_INDEX_GUIDE: Final = (
    "Every dataset the library serves, one page each. A page shows how to call the dataset, "
    "its verbs, the `sort` values and `filters` keys it accepts, which of Massive's plans "
    "include it, and every column of its tables, in the order every verb returns them. The "
    "same column descriptions appear in the Deephaven IDE's column tooltips and in a query's "
    "`columns()`, and `dm.datasets()` lists these datasets as a table."
)
"""The paragraph under the index's title."""

_GRID_GUIDE: Final = (
    "The datasets that several asset classes have, one row each; a mark links that class's "
    "page. Every other dataset is listed under its namespace below, and a method of the "
    "session itself, such as `m.snapshots`, under Session methods."
)
"""The paragraph above the index's grid."""

_SESSION_HEADING: Final = "Session methods"
"""The index's heading over the datasets that are methods of the session, such as `m.snapshots`."""

_MERGE: Final = (
    "Tables of one kind from two asset classes (stock and forex quotes, say) can differ in "
    "their columns. To merge them, keep the shared columns: `common = [c for c in "
    "a.column_names if c in b.column_names]`, then `merge([t.view(common) for t in (a, b)])`. "
    "To keep every column instead, add each missing one to the other table as a typed null "
    'first, such as `b.update("Tape = NULL_LONG")`.'
)
"""The paragraph of the index on merging tables of different classes."""

_TYPE_DIFFERENCES: Final = (
    "A column such tables share has one name, and one type in every class except those listed "
    "below, where Massive declares a different type for some classes than for others. Cast one "
    "table's column to the other's type before a merge, such as "
    '`a.update("LastTradeSize = (double) LastTradeSize")`.'
)
"""The paragraph of the index over the shared columns whose type differs between classes."""

_TYPE_NAMES: Final[Mapping[ColumnType, str]] = {
    ColumnType.DOUBLE: "a 64-bit decimal number",
    ColumnType.LONG: "a 64-bit whole number",
    ColumnType.INT: "a 32-bit whole number",
    ColumnType.BOOLEAN: "true or false",
    ColumnType.STRING: "text",
    ColumnType.INSTANT: "an exact moment in time",
    ColumnType.LONG_ARRAY: "an array of `long`",
    ColumnType.STRING_ARRAY: "an array of `String`",
}
"""What each Deephaven type a page names holds, as the index's type legend says it."""

_WIRE_GUIDE: Final = (
    "The Massive field behind each column, per transport (`request` for a value the library "
    "fills from the request; empty for a column the library computes)."
)
"""The paragraph inside the folded wire-field table."""

_PROVENANCE: Final = (
    "Generated by `{command}` from Massive's documentation as fetched on {fetched}. The "
    "descriptions and plan tables are Massive's, descriptions corrected where they are clearly "
    "wrong; the library writes its own for the key columns and the columns it computes."
)
"""Every page's footer, formatted with the command and the fetch date."""

_LINKS_HEADING: Final = "Massive's documentation of this dataset:"
"""The line that introduces the list of a dataset's vendor pages."""

_PLANS_GUIDE: Final = (
    "Which of Massive's plans include this dataset, how recent its data is on each, and how far "
    "back its history goes, as Massive's pages state them."
)
"""The paragraph under a page's plan heading."""

_POLL_MODES: Final[Mapping[PollMode, str]] = {
    PollMode.REPLACE: "holds exactly the latest cycle's rows",
    PollMode.UPSERT: "adds and updates rows, never deletes",
    PollMode.APPEND: "appends only rows it has not seen",
}
"""What each default `poll()` mode does to the table, as a page says it."""


@dataclass(frozen=True, slots=True)
class DatasetCall:
    """How a user calls one dataset: its method's signature and the verbs its queries have.

    The API layer builds one per dataset (`dataset_calls` in `_dataset_list.py`), because the
    dataset methods and verbs live there and this layer may not import them; `render` writes
    them on the dataset's page. Immutable.

    Attributes:
        signature: The call as a user writes it from a session `m`, with each argument's
            default, such as `"m.stocks.bars(symbols, timespan, *, multiplier=1,
            adjusted=True)"`.
        verbs: The verbs its queries have, in the library's order, such as `("fetch",
            "poll")`.
        arguments: The method's argument names, in order, such as `("symbols", "start",
            "end", "sort", "order", "limit", "filters")`; a page lists the `sort` values and
            `filters` keys only of a method that takes `sort` or `filters`.
        typed: The wire parameter each argument sets, whose `filters` forms the method
            refuses, such as `"type"` for an argument `ticker_type`; an argument that sets
            none appears under its own name, which is harmless.
    """

    signature: str
    verbs: tuple[str, ...]
    arguments: tuple[str, ...] = ()
    typed: tuple[str, ...] = ()


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


def page_name(spec: DatasetSpec) -> str:
    """Return the file name of a dataset's page.

    Dataset docstrings name the page by its web address, `page_url`.

    Args:
        spec: The dataset.

    Returns:
        Its dotted name and `.md`: `"stocks.trades.md"`, `"stocks.financials.ratios.md"`, or,
        for a dataset the session serves itself, `"snapshots.md"`.
    """
    return f"{spec.name}.md"


def page_url(spec: DatasetSpec) -> str:
    """Return the web address of a dataset's page of the reference.

    Args:
        spec: The dataset.

    Returns:
        `PAGES_URL` and the page's name, such as
        `"https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.trades.md"`.
    """
    return f"{PAGES_URL}{page_name(spec)}"


def call_path(spec: DatasetSpec) -> str:
    """Return how a user reaches a dataset from a session `m`, which its page's heading shows.

    Args:
        spec: The dataset.

    Returns:
        `m.` and the dataset's name, such as `"m.stocks.trades"`, or `"m.snapshots"` for a
        dataset the session serves itself.
    """
    return f"m.{spec.name}"


def render(
    catalog: Catalog, provenance: Provenance, calls: Mapping[str, DatasetCall]
) -> dict[str, str]:
    """Render the dataset reference: an index and one Markdown page per dataset.

    Call it to regenerate the pages (the command passes them to `sync`) or to compare them with
    the committed ones (the parity test).

    The index (`INDEX`) has a title, a short guide, a grid with one row per dataset name that
    two or more of the six asset-class namespaces have (`trades`, `bars`, `snapshots`, ...)
    and one column per asset class, each mark linking that class's page, the recipe that
    merges tables of different classes, and one table per namespace listing each dataset's
    page, summary, and verbs, in declaration order.

    A dataset's page has its call path as heading (`call_path`), its `DatasetSpec.summary`, its
    call (`DatasetCall.signature`) in a code block, and a table of facts: its verbs, the
    transports that serve it, the wire parameters its time range is sent as (when it has
    one), its default `poll()` mode and keys (for a dataset with `fetch()`), the `sort` values
    it takes (an enum's values, or any field of the rows for a free-text `sort`), and the
    `filters` keys it accepts (`filters.filter_keys`). Then come the plan tables of Massive's
    pages that document its sources (`_dataset_urls`, from `Provenance.plans`): one row per
    plan, one column per page, each cell the plan's recency and history (or "Not included"),
    and the summary of a page that lists no plan. Then the columns, one row per column in
    result order (**Column**, **Deephaven type**, the `ColumnType` value such as `Instant`,
    **Description**, **Supplied by**, the transports that fill it), and, folded in
    `<details>`, the Massive field behind each column per
    transport the dataset reads (`request` for a key filled from the request). Last come the
    sorted, distinct vendor pages, each a link to the web page (the fetched URL without its
    `.md`) labeled with the page's title and documentation area (`Provenance.titles`), and a
    footer with the generator command and the date Massive's documentation was fetched.

    A `|` in a table cell is escaped with a backslash, no line has trailing whitespace, and
    every page ends in exactly one newline, so the pages pass the repository's commit hooks
    unchanged.

    Pure: no I/O beyond the wire package's one-time load of the generated files that the
    catalog's schema merge may trigger, no Deephaven call, and the same output for the same
    arguments.

    Args:
        catalog: Every dataset to document, in declaration order; the process's catalog for
            the real pages, or a synthetic one in tests.
        provenance: The fetch date, the vendor URLs of the generated sources, and their pages'
            plan tables and titles, as `_massive.provenance()` returns them; a source without
            URLs adds none, and a page without plan tables adds no column.
        calls: Each dataset's call, by dataset name; it must name every dataset of `catalog`.

    Returns:
        The page text by file name: `INDEX` first, then each dataset's page (`page_name`), in
        catalog order; only the index for an empty catalog.

    Raises:
        KeyError: If `calls` lacks a dataset of the catalog.

    The schema merge's `ValueError` or `KeyError` (`Catalog.dataset_schema`) propagates for a
    catalog whose datasets do not merge.
    """
    specs = tuple(catalog.by_name.values())
    missing = [spec.name for spec in specs if spec.name not in calls]
    if missing:
        raise KeyError(f"no call given for the datasets {missing}")
    fetched = provenance.fetched.isoformat()
    footer = ["---", "", _PROVENANCE.format(command=COMMAND, fetched=fetched)]
    pages = {INDEX: _index(specs, calls, footer, catalog=catalog)}
    for spec in specs:
        pages[page_name(spec)] = _page(
            spec, calls[spec.name], catalog=catalog, provenance=provenance, footer=footer
        )
    return pages


def _index(
    specs: Sequence[DatasetSpec],
    calls: Mapping[str, DatasetCall],
    footer: list[str],
    *,
    catalog: Catalog,
) -> str:
    """Render the index page: the grid of asset classes, then every dataset by namespace.

    Under the title come what a page shows and what each type a page names holds; under the
    grid, how to merge tables of one kind from two classes, and the columns they share whose
    type differs between classes (`_type_differences`).

    Args:
        specs: Every dataset, in catalog order.
        calls: Each dataset's call, by name.
        footer: The footer's lines.
        catalog: The catalog that merges each dataset's schema, for the columns' types.

    Returns:
        The page text, ending in one newline.
    """
    types = "; ".join(f"`{name.value}`, {meaning}" for name, meaning in _TYPE_NAMES.items())
    lines = [
        "# Dataset reference",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        _INDEX_GUIDE,
        "",
        f"A column's Deephaven type is one of: {types}.",
    ]
    classes = [c.value for c in AssetClass]
    grid: dict[str, dict[str, DatasetSpec]] = {}
    for spec in specs:
        namespace, _, rest = spec.name.partition(".")
        if namespace in classes:
            grid.setdefault(rest, {})[namespace] = spec
    grid = {name: by_class for name, by_class in grid.items() if len(by_class) > 1}
    if grid:
        lines.extend(("", "## By asset class", "", _GRID_GUIDE, ""))
        lines.append(_row(["Dataset", *(f"`m.{c}`" for c in classes)]))
        lines.append(_row(["---"] * (len(classes) + 1)))
        for name, by_class in grid.items():
            cells = [f"[✓]({page_name(by_class[c])})" if c in by_class else "" for c in classes]
            lines.append(_row([f"`{name}`", *cells]))
        lines.extend(("", _MERGE))
        differences = _type_differences(grid, catalog)
        if differences:
            lines.extend(("", _TYPE_DIFFERENCES, "", *differences))
    # A dataset named without a dot, such as `snapshots`, is a method of the session rather than
    # of a namespace, so it goes under a heading that names no namespace.
    by_heading: dict[str, list[DatasetSpec]] = {}
    for spec in specs:
        namespace, dot, _ = spec.name.partition(".")
        heading = f"`m.{namespace}`" if dot else _SESSION_HEADING
        by_heading.setdefault(heading, []).append(spec)
    for heading, members in by_heading.items():
        lines.extend(("", f"## {heading}", ""))
        lines.append(_row(["Dataset", "Summary", "Verbs"]))
        lines.append(_row(["---"] * 3))
        lines.extend(
            _row(
                [
                    f"[`{call_path(spec)}`]({page_name(spec)})",
                    spec.summary,
                    ", ".join(calls[spec.name].verbs),
                ]
            )
            for spec in members
        )
    lines.extend(("", *footer))
    return "\n".join(lines) + "\n"


def _type_differences(grid: Mapping[str, Mapping[str, DatasetSpec]], catalog: Catalog) -> list[str]:
    """List the columns that tables of one kind share with a different type in some classes.

    Massive declares some fields with one type for one asset class and another for the next
    (a snapshot's `LastTradeSize` is a whole number for stocks and a decimal for crypto), and
    the library keeps each declared type, so such a column must be cast before a merge.

    Args:
        grid: Each dataset name that several classes have, mapped to its dataset per class.
        catalog: The catalog that merges each dataset's schema.

    Returns:
        One list item per dataset name with such a column, in grid order, such as
        ``- `movers`: `LastTradeSize` is long for stocks, double for crypto``; empty when
        every shared column has one type.
    """
    items = []
    for name, by_class in grid.items():
        types: dict[str, dict[str, list[str]]] = {}
        for asset_class, spec in by_class.items():
            for column in catalog.dataset_schema(spec).columns:
                by_type = types.setdefault(column.name, {})
                by_type.setdefault(column.type.value, []).append(asset_class)
        differing = [
            f"`{column}` is "
            + ", ".join(f"{t} for {joined_with_and(found)}" for t, found in by_type.items())
            for column, by_type in types.items()
            if len(by_type) > 1
        ]
        if differing:
            items.append(f"- `{name}`: " + "; ".join(differing))
    return items


def _page(
    spec: DatasetSpec,
    call: DatasetCall,
    *,
    catalog: Catalog,
    provenance: Provenance,
    footer: list[str],
) -> str:
    """Render one dataset's page.

    Args:
        spec: The dataset.
        call: Its call.
        catalog: The catalog that merges its schema and holds its REST paths.
        provenance: The vendor URLs of its sources and their plan tables and titles.
        footer: The footer's lines.

    Returns:
        The page text, ending in one newline.
    """
    lines = [
        f"# `{call_path(spec)}`",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        spec.summary,
        "",
        "```python",
        call.signature,
        "```",
        "",
        "[All datasets](README.md)",
        "",
        _row(["", ""]),
        _row(["---", "---"]),
        *(_row(fact) for fact in _facts(spec, call, catalog)),
    ]
    urls = _dataset_urls(spec, provenance)
    lines.extend(_plans(urls, provenance))
    lines.extend(_columns(spec, catalog))
    if urls:
        lines.extend(("", _LINKS_HEADING, ""))
        lines.extend(_links(urls, provenance))
    lines.extend(("", *footer))
    return "\n".join(lines) + "\n"


def _facts(spec: DatasetSpec, call: DatasetCall, catalog: Catalog) -> list[list[str]]:
    """Return the rows of a dataset page's table of facts.

    Args:
        spec: The dataset.
        call: Its call, for its verbs and its arguments' wire names.
        catalog: The catalog that holds its REST paths.

    Returns:
        `[label, text]` rows: verbs, transports, time range (for a dataset with one), `poll()`
        (for a dataset with `fetch()`), and `sort` and `filters` (for a method that takes
        them).
    """
    used = {transport for transport, _ in spec.sources}
    facts = [
        ["Verbs", ", ".join(f"`{verb}()`" for verb in call.verbs)],
        ["Served by", ", ".join(TRANSPORT_LABELS[t] for t in TRANSPORT_ORDER if t in used)],
    ]
    if spec.time_axis is not None:
        low, high = spec.time_axis
        facts.append(["Time range", f"`start` and `end` are sent as `{low}` and `{high}`"])
    if spec.has_fetch:
        facts.append(["`poll()`", _poll_text(spec)])
    if "sort" in call.arguments:
        facts.append(["`sort`", _sort_text(spec, catalog)])
    if "filters" in call.arguments:
        keys = filter_keys(catalog, spec, typed=call.typed)
        facts.append(["`filters`", ", ".join(f"`{key}`" for key in keys) or "none"])
    return facts


def _poll_text(spec: DatasetSpec) -> str:
    """Say what a dataset's `poll()` does by default.

    Args:
        spec: A dataset with `fetch()`.

    Returns:
        The mode, then its effect and keys, such as "`dm.PollMode.REPLACE`: holds exactly the
        latest cycle's rows, keyed by `Symbol`".
    """
    text = f"`dm.PollMode.{spec.poll_mode.name}`: {_POLL_MODES[spec.poll_mode]}"
    if spec.poll_keys:
        text = f"{text}, keyed by {_column_list(spec.poll_keys)}"
    if spec.revision_keys:
        text = f"{text}; `mode=dm.PollMode.APPEND` also keys by {_column_list(spec.revision_keys)}"
    return text


def _column_list(names: Sequence[str]) -> str:
    """Join column names for a sentence, each in backticks.

    Args:
        names: The names; at least one.

    Returns:
        The names joined by `joined_with_and`, such as "`Symbol` and `IssuerName`".
    """
    return joined_with_and([f"`{name}`" for name in names])


def _sort_text(spec: DatasetSpec, catalog: Catalog) -> str:
    """Say which `sort` values a dataset takes, from its REST paths' `sort` parameters.

    Args:
        spec: The dataset.
        catalog: The catalog that holds its REST paths.

    Returns:
        The enum values, in order and distinct, of every path whose `sort` is an enum; "any
        field of the rows" for a path whose `sort` is free text (comma-separated fields, each
        with an optional `.asc` or `.desc`); "not taken" when no path has `sort`.
    """
    values: list[str] = []
    free = False
    for path in spec.rest_paths:
        param = next((p for p in catalog.rest_path(path).params if p.name == "sort"), None)
        if param is None:
            continue
        if param.kind is ParamKind.ENUM:
            values.extend(v for v in param.values if v not in values)
        else:
            free = True
    parts = [", ".join(f"`{value}`" for value in values)] if values else []
    if free:
        parts.append(
            "any REST field of the rows (see Massive fields below), or several separated by "
            "commas, each with an optional `.asc` or `.desc`"
        )
    return "; ".join(parts) or "not taken"


def _plans(urls: Iterable[str], provenance: Provenance) -> list[str]:
    """Render the plan tables of a dataset's vendor pages as one table.

    Args:
        urls: The dataset's vendor pages.
        provenance: Their plan tables and titles, by URL.

    Returns:
        The section's lines, starting with a blank line; empty when no page has plan tables.
    """
    pages = [(url, provenance.plans[url]) for url in urls if url in provenance.plans]
    if not pages:
        return []
    listed = [(url, page) for url, page in pages if page.plans]
    lines = [
        "",
        "## Plan access",
        "",
        _PLANS_GUIDE,
    ]
    if listed:
        plans = list(dict.fromkeys(terms.plan for _, page in listed for terms in page.plans))
        # Pages of one title and area, such as one per asset class, share one column.
        titles = [_label(url, provenance) for url, _ in listed]
        columns = {
            title: [page for (_, page), own in zip(listed, titles, strict=True) if own == title]
            for title in titles
        }
        header = ["Plan", *columns]
        lines.extend(("", _row(header), _row(["---"] * len(header))))
        for plan in plans:
            cells = []
            for column in columns.values():
                terms = [t for page in column for t in page.plans if t.plan == plan]
                texts = (_plan_cell(t.access, t.recency, t.history) for t in terms)
                cells.append("; ".join(dict.fromkeys(texts)))
            lines.append(_row([plan, *cells]))
    unlisted = [(url, page) for url, page in pages if not page.plans]
    if unlisted:
        lines.append("")
        lines.extend(
            f"- {_label(url, provenance)}: "
            + "; ".join(text for text in (page.access, page.recency, page.history) if text)
            for url, page in unlisted
        )
    return lines


def _plan_cell(access: str, recency: str | None, history: str | None) -> str:
    """Condense one plan's terms into a table cell.

    Args:
        access: Whether the plan includes the data, as Massive writes it.
        recency: How recent the data is on the plan, or None when the page does not say.
        history: How far back the plan reaches, or None when the page does not say.

    Returns:
        "Not included" for a plan without the data; otherwise its recency and history, such as
        "15-minute delayed, 5 years", or its access text when the page gives neither.
    """
    if access == _NOT_INCLUDED:
        return _NOT_INCLUDED
    shown = [part for part in (recency, history) if part and part != _NOT_INCLUDED]
    return ", ".join(shown) or access


def _label(url: str, provenance: Provenance) -> str:
    """Name a vendor page as the reference shows it: its title, then its documentation area.

    Args:
        url: The page's URL.
        provenance: The titles of the vendor pages, by URL, which include this page's.

    Returns:
        Such as `"Custom Bars (OHLC) (REST)"`.
    """
    return f"{provenance.titles[url]} ({_area(url)})"


def _links(urls: Sequence[str], provenance: Provenance) -> list[str]:
    """Render a page's list of vendor pages: one link per page, labeled with its name.

    Massive serves each documentation page twice: as Markdown at the URL the snapshot fetched,
    which ends in `.md`, and as the web page people read at the same URL without it. Each link
    goes to the web page. Massive documents a source several classes share once per class
    under one title (`Unified Snapshot`), so a label that two pages of the list share names
    the asset class its URL names, as a path segment, after the area.

    Args:
        urls: The dataset's vendor pages, as the snapshot fetched them.
        provenance: The titles of the vendor pages, by URL, which include these.

    Returns:
        One list item per URL, in order, such as
        `"- [Unified Snapshot (REST, crypto)](https://massive.com/docs/rest/crypto/snapshots/unified-snapshot)"`.
    """
    labels = [_label(url, provenance) for url in urls]
    items = []
    for url, label in zip(urls, labels, strict=True):
        named = next((c.value for c in AssetClass if c.value in _segments(url)), None)
        shown = label
        if labels.count(label) > 1 and named is not None:
            shown = f"{provenance.titles[url]} ({_area(url)}, {named})"
        items.append(f"- [{shown}]({url.removesuffix('.md')})")
    return items


def _area(url: str) -> str:
    """Name the area of Massive's documentation a page belongs to.

    Args:
        url: The page's URL.

    Returns:
        `"REST"`, `"WebSocket"`, or `"flat files"`; `"Massive"` for a URL of no known area.
    """
    return next((name for prefix, name in _PAGE_AREAS if url.startswith(prefix)), "Massive")


def _columns(spec: DatasetSpec, catalog: Catalog) -> list[str]:
    """Render a dataset's columns, then the Massive field behind each, folded away.

    Args:
        spec: The dataset.
        catalog: The catalog that merges its schema.

    Returns:
        The section's lines, starting with a blank line.
    """
    used = {transport for transport, _ in spec.sources}
    wire = [transport for transport in TRANSPORT_ORDER if transport in used]
    schema = catalog.dataset_schema(spec)  # the default session's columns, RawSymbol included
    header = ["Column", "Deephaven type", "Description", "Supplied by"]
    lines = ["", "## Columns", "", _row(header), _row(["---"] * len(header))]
    lines.extend(_row(_cells(column)) for column in schema.columns)
    fields = ["Column", *(_WIRE_HEADINGS[t] for t in wire)]
    lines.extend(
        (
            "",
            "<details><summary>Massive fields</summary>",
            "",
            _WIRE_GUIDE,
            "",
            _row(fields),
            _row(["---"] * len(fields)),
        )
    )
    lines.extend(_row(_wire_cells(column, wire)) for column in schema.columns)
    lines.extend(("", "</details>"))
    return lines


def _cells(column: Column) -> list[str]:
    """Return the cells of one column's row of the columns table, before escaping.

    Args:
        column: The schema column.

    Returns:
        The column's name, type, description, and transports.
    """
    supplied = ", ".join(TRANSPORT_LABELS[t] for t in column.supplied_by)
    return [f"`{column.name}`", column.type.value, column.description, supplied]


def _wire_cells(column: Column, wire: Sequence[Transport]) -> list[str]:
    """Return the cells of one column's row of the folded wire-field table, before escaping.

    Args:
        column: The schema column.
        wire: The transports the dataset reads, in wire-column order.

    Returns:
        The column's name and one cell per transport of `wire`: the distinct wire keys behind
        the column in source order, or `request` for a key filled from the request.
    """
    fields = [
        ", ".join(
            _FROM_REQUEST if key is None else f"`{key}`" for key in column.wire_keys.get(t, ())
        )
        for t in wire
    ]
    return [f"`{column.name}`", *fields]


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


_HEADING: Final = re.compile(r"^# `(m\.[^`]+)`$")
"""A dataset page's title, which names its dataset's call path."""

_COLUMNS_HEADING: Final = "## Columns"
"""The heading of a dataset page's columns table."""

_COLUMN_ROW: Final = re.compile(r"^\| `([^`]+)` \| ([^|]+) \|")
"""A row of a page's columns table: its first cell names the column, its second the type."""


def _columns_by_dataset(pages: Mapping[str, str]) -> dict[str, dict[str, str]]:
    """Read each dataset's columns and their Deephaven types back from rendered pages.

    The pages are the only record of what the reference said before a change, so the report
    reads them back in the format `_page` writes: a `# `m.…`` title, then, under the
    `## Columns` heading and before the next heading or the folded wire-field table, rows that
    start with the column name in backticks and its type. Every other line is ignored, the
    index's included.

    Args:
        pages: Page text by file name, as `render` returns them or as a directory holds them.

    Returns:
        Each page's columns, in order, with their types, by the call path its title names.
    """
    found: dict[str, dict[str, str]] = {}
    for text in pages.values():
        current: dict[str, str] | None = None
        reading = False
        for line in text.split("\n"):
            heading = _HEADING.match(line)
            if heading:
                current = found.setdefault(heading[1], {})
            elif line.startswith(("#", "<details>")):
                reading = line == _COLUMNS_HEADING and current is not None
            elif reading and current is not None and (row := _COLUMN_ROW.match(line)):
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
