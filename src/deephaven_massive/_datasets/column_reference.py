"""The dataset reference: the generated `docs/datasets/` pages, and their sync.

Every dataset's columns are documented in three places, all read from the same dataset schema
(`Catalog.dataset_schema`), so they never disagree: the column descriptions every result
carries (`_tables/columns.py`), the static table of the queries' `columns()`, and these pages,
which readers browse on GitHub before they install anything. This module owns the pages:

- `render` turns a catalog, the provenance of the generated sources, and each dataset's call
  (`DatasetCall`, which the API layer supplies) into one Markdown page per dataset, named after
  its dotted name (`stocks.trades.md`, `stocks.financials.ratios.md`, `snapshots.md`), and an
  index, `README.md`, which GitHub shows when a reader opens the directory: the datasets
  several asset classes have, every dataset by namespace, with the methods of the session
  itself (`m.snapshots`) under a heading of their own, and the rule of each argument many
  datasets share (`argument_rules.py`), printed once. A dataset's page says how to call it,
  its query methods, its `poll()` mode, each argument with its docstring line, the `sort`
  fields and `filters` keys it accepts, and a link to its shared rule, which of Massive's
  plans include it (from the plan tables of Massive's own pages, so they follow every
  refresh), and its columns, with the Massive field behind each column folded away. It is
  pure: it reads only its arguments and the schemas the catalog merges,
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

from deephaven_massive._datasets.argument_rules import ARGUMENT_RULES
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
    "Each dataset has a page with how to call it, its arguments, the plans that include it, and "
    "its columns. In the Deephaven IDE, the same column descriptions show as column tooltips, and "
    "`dm.datasets()` lists every dataset as a table."
)
"""The paragraph under the index's title."""

_GRID_HEADING: Final = "Datasets by asset class"
"""The heading of the index's table of the datasets several asset classes have."""

_GRID_GUIDE: Final = "The datasets that several asset classes have, linked to each class's page."
"""The paragraph above the index's table of the datasets several asset classes have."""

_NAMESPACE_HEADINGS: Final[Mapping[str, str]] = {
    "reference": "Reference data",
    "indicators": "Technical indicators",
    "partners": "Partner data",
}
"""The index's heading over a namespace whose capitalized name would not say what it holds;
every other namespace's heading is its capitalized name, such as `Stocks`."""

_SESSION_HEADING: Final = "Session methods"
"""The index's heading over the datasets that are methods of the session, such as `m.snapshots`."""

_SHARED_HEADING: Final = "Shared arguments"
"""The index's heading over the rules of the arguments many datasets take (`ARGUMENT_RULES`)."""

_SHARED_ANCHOR: Final = "shared-arguments"
"""The anchor of the index's shared-argument section, which every page's Arguments links."""

_SHARED_GUIDE: Final = (
    "These arguments follow the same rules on every dataset that takes them. A dataset's page "
    "gives what is particular to that dataset and links here."
)
"""The paragraph under the index's shared-argument heading."""

_MERGE_HEADING: Final = "Merge tables from two asset classes"
"""The heading of the index's section on merging tables of one kind from two classes."""

_MERGE: Final = (
    "Tables of one kind from two asset classes, such as stock and forex quotes, can have "
    "different columns. To merge two such tables, `a` and `b`, keep the columns they share:\n"
    "\n"
    "```python\n"
    "from deephaven import merge\n"
    "\n"
    "common = [c for c in a.column_names if c in b.column_names]\n"
    "merged = merge([t.view(common) for t in (a, b)])\n"
    "```\n"
    "\n"
    "To keep every column instead, first add each missing column to the other table as a "
    'typed null, such as `b.update("Tape = NULL_LONG")`.'
)
"""The Markdown of the index's section on merging tables of different classes."""

_TYPE_DIFFERENCES: Final = (
    "A few shared columns have a different type in some asset classes, because Massive declares "
    "them that way. Cast one table's column to the other's type before the merge, such as "
    '`a.update("LastTradeSize = (double) LastTradeSize")`:'
)
"""The paragraph of the index over the shared columns whose type differs between classes."""

_TYPES_HEADING: Final = "Column types"
"""The heading of the index's table of the Deephaven types the pages name."""

_TYPES_GUIDE: Final = "Each column has one of these Deephaven types:"
"""The paragraph above the index's table of types."""

_TYPE_NAMES: Final[Mapping[ColumnType, str]] = {
    ColumnType.DOUBLE: "A 64-bit floating-point number, Python's `float`",
    ColumnType.LONG: "A 64-bit integer",
    ColumnType.INT: "A 32-bit integer",
    ColumnType.BOOLEAN: "True or false",
    ColumnType.STRING: "Text",
    ColumnType.INSTANT: "An exact moment in time",
    ColumnType.LONG_ARRAY: "An array of `long`",
    ColumnType.STRING_ARRAY: "An array of `String`",
}
"""What each Deephaven type a page names holds, as the index's table of types says it."""

_INDEX_LINK: Final = f"[Dataset index]({INDEX})"
"""The link from every dataset page back to the index."""

_CALL_GUIDE: Final = "Call it from a session `m`:"
"""The sentence that introduces a dataset page's call."""

_WIRE_ANCHOR: Final = "massive-fields"
"""The anchor of a page's folded table of Massive fields, which a free-text `sort` links."""

_WIRE_GUIDE: Final = (
    "The Massive field behind each column, by source. `request` marks a value the library fills "
    "from the request, and an empty cell a column the library computes."
)
"""The paragraph inside the folded wire-field table."""

_PROVENANCE: Final = (
    "The plan tables and most column descriptions are Massive's, from its documentation as "
    "fetched on {fetched}, with a few corrected where Massive's are wrong. The library writes "
    "its own descriptions of the key columns and the columns it computes, such as `Symbol` and "
    "`Date`."
)
"""Every page's footer, formatted with the fetch date."""

_LINKS_HEADING: Final = "Massive's documentation"
"""The heading over the list of a dataset's vendor pages."""

_PLANS_HEADING: Final = "Plans"
"""The heading of a page's plan table."""

_PLANS_GUIDE: Final = (
    "How recent the data is on each of Massive's plans, and how far back its history goes, as "
    "Massive's pages state them."
)
"""The paragraph under a page's plan heading."""

_POLL_MODES: Final[Mapping[PollMode, str]] = {
    PollMode.REPLACE: "holds only the latest poll's rows",
    PollMode.UPSERT: "adds and updates rows and never deletes them",
    PollMode.APPEND: "appends each row it hasn't seen",
}
"""What each default `poll()` mode does to the table, as a page says it."""

_ARGUMENTS_GUIDE: Final = (
    f"A linked argument follows the [shared rules]({INDEX}#{_SHARED_ANCHOR}) on the dataset index."
)
"""The paragraph under a page's Arguments heading, when an argument links a shared rule."""


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
        descriptions: Each argument's line from the method's docstring, in the order of
            `arguments`, such as `"The first ex-dividend date."`; the page's Arguments table
            shows it beside the argument's name, which links its shared rule
            (`ARGUMENT_RULES`) on the index. An argument past the end of the tuple has an
            empty line.
    """

    signature: str
    verbs: tuple[str, ...]
    arguments: tuple[str, ...] = ()
    typed: tuple[str, ...] = ()
    descriptions: tuple[str, ...] = ()


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

    The index (`INDEX`) has a title, a short guide, a table of the dataset names that two or
    more of the six asset-class namespaces have (`trades`, `bars`, `snapshots`, ...), each
    linking every class's page, one table per namespace listing each dataset's page, summary,
    and query methods, in declaration order, the rule of every shared argument
    (`ARGUMENT_RULES`) under its name and anchor, the recipe that merges tables of different
    classes with the shared columns whose type differs, and a table of the column types.

    A dataset's page has its call path as heading (`call_path`), its `DatasetSpec.summary`, its
    call (`DatasetCall.signature`) in a code block, and a list of facts: its query methods, the
    transports that serve it, and, for a dataset with `fetch()`, its default `poll()` mode and
    keys. Then comes an Arguments table, when the method takes arguments: each argument, linked
    to its shared rule on the index when it has one, and its line from the method's docstring
    (`DatasetCall.descriptions`), followed for `sort` by the fields it takes and for `filters`
    by the keys it accepts (`filters.filter_keys`). Then come the plan tables of Massive's pages
    that document its sources (`_dataset_urls`, from `Provenance.plans`): one row per plan, one
    column per page, each cell the plan's recency and history (or "Not included"), and the
    summary of a page that lists no plan. Then the columns, one row per column in result order
    (**Column**, **Type**, the `ColumnType` value such as `Instant`, **Description**, and, for
    a dataset read from more than one transport, **Read from**), and, folded in `<details>`,
    the Massive field behind each column per transport the dataset reads (`request` for a key
    filled from the request). Last come the sorted, distinct vendor pages, each a link to the
    web page (the fetched URL without its `.md`) labeled with the page's title and
    documentation area (`Provenance.titles`), and a footer with the date Massive's
    documentation was fetched.

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
    footer = ["---", "", _PROVENANCE.format(fetched=provenance.fetched.isoformat())]
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
    """Render the index page.

    Under the title and its guide come the datasets several asset classes have, every dataset
    by namespace, the shared arguments' rules, how to merge tables of one kind from two classes
    with the columns whose type differs between classes (`_type_differences`), and what each
    column type holds.

    Args:
        specs: Every dataset, in catalog order.
        calls: Each dataset's call, by name.
        footer: The footer's lines.
        catalog: The catalog that merges each dataset's schema, for the columns' types.

    Returns:
        The page text, ending in one newline.
    """
    lines = [
        "# Dataset index",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        _INDEX_GUIDE,
    ]
    classes = [c.value for c in AssetClass]
    grid: dict[str, dict[str, DatasetSpec]] = {}
    for spec in specs:
        namespace, _, rest = spec.name.partition(".")
        if namespace in classes:
            grid.setdefault(rest, {})[namespace] = spec
    grid = {name: by_class for name, by_class in grid.items() if len(by_class) > 1}
    if grid:
        lines.extend(("", f"## {_GRID_HEADING}", "", _GRID_GUIDE, ""))
        lines.append(_row(["Dataset", "Asset classes"]))
        lines.append(_row(["---"] * 2))
        for name, by_class in grid.items():
            links = (
                f"[{c.capitalize()}]({page_name(by_class[c])})" for c in classes if c in by_class
            )
            lines.append(_row([f"`{name}`", ", ".join(links)]))
    # A dataset named without a dot, such as `snapshots`, is a method of the session rather than
    # of a namespace, so it goes under a heading that names no namespace, after every namespace.
    by_heading: dict[str, list[DatasetSpec]] = {}
    for spec in specs:
        namespace, dot, _ = spec.name.partition(".")
        heading = _NAMESPACE_HEADINGS.get(namespace, namespace.capitalize())
        by_heading.setdefault(heading if dot else _SESSION_HEADING, []).append(spec)
    session = by_heading.pop(_SESSION_HEADING, None)
    if session:
        by_heading[_SESSION_HEADING] = session
    for heading, members in by_heading.items():
        lines.extend(("", f"## {heading}", ""))
        lines.append(_row(["Dataset", "Summary", "Query methods"]))
        lines.append(_row(["---"] * 3))
        lines.extend(
            _row(
                [
                    f"[`{call_path(spec)}`]({page_name(spec)})",
                    spec.summary,
                    _methods(calls[spec.name]),
                ]
            )
            for spec in members
        )
    lines.extend(("", f'<a id="{_SHARED_ANCHOR}"></a>', "", f"## {_SHARED_HEADING}", ""))
    lines.append(_SHARED_GUIDE)
    for name, rule in ARGUMENT_RULES.items():
        lines.extend(("", f'<a id="{name}"></a>', "", f"### {name}", "", rule))
    if grid:
        lines.extend(("", f"## {_MERGE_HEADING}", "", _MERGE))
        differences = _type_differences(grid, catalog)
        if differences:
            lines.extend(("", _TYPE_DIFFERENCES, ""))
            lines.append(_row(["Dataset", "Column", "Types"]))
            lines.append(_row(["---"] * 3))
            lines.extend(_row(cells) for cells in differences)
    lines.extend(("", f"## {_TYPES_HEADING}", "", _TYPES_GUIDE, ""))
    lines.append(_row(["Type", "Holds"]))
    lines.append(_row(["---"] * 2))
    lines.extend(_row([f"`{t.value}`", meaning]) for t, meaning in _TYPE_NAMES.items())
    lines.extend(("", *footer))
    return "\n".join(lines) + "\n"


def _methods(call: DatasetCall) -> str:
    """Name a dataset's query methods, as its page and the index show them.

    Args:
        call: The dataset's call.

    Returns:
        Each verb as a call, in the library's order, such as "`fetch()`, `poll()`".
    """
    return ", ".join(f"`{verb}()`" for verb in call.verbs)


def _type_differences(
    grid: Mapping[str, Mapping[str, DatasetSpec]], catalog: Catalog
) -> list[list[str]]:
    """List the columns that tables of one kind share with a different type in some classes.

    Massive declares some fields with one type for one asset class and another for the next
    (a snapshot's `LastTradeSize` is a whole number for stocks and a decimal for crypto), and
    the library keeps each declared type, so such a column must be cast before a merge.

    Args:
        grid: Each dataset name that several classes have, mapped to its dataset per class.
        catalog: The catalog that merges each dataset's schema.

    Returns:
        One row of cells per such column, in grid order and then column order: the dataset
        name, the column, and its types by class, such as
        ``["`movers`", "`LastTradeSize`", "long for stocks, double for crypto"]``; empty when
        every shared column has one type.
    """
    rows: list[list[str]] = []
    for name, by_class in grid.items():
        types: dict[str, dict[str, list[str]]] = {}
        for asset_class, spec in by_class.items():
            for column in catalog.dataset_schema(spec).columns:
                by_type = types.setdefault(column.name, {})
                by_type.setdefault(column.type.value, []).append(asset_class)
        rows.extend(
            [
                f"`{name}`",
                f"`{column}`",
                ", ".join(f"{t} for {joined_with_and(found)}" for t, found in by_type.items()),
            ]
            for column, by_type in types.items()
            if len(by_type) > 1
        )
    return rows


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
        f"# {call_path(spec)}",
        "",
        f"<!-- Generated by `{COMMAND}`; do not edit by hand. -->",
        "",
        spec.summary,
        "",
        _CALL_GUIDE,
        "",
        "```python",
        call.signature,
        "```",
        "",
        *(f"- {fact}" for fact in _facts(spec, call)),
        "",
        _INDEX_LINK,
        *_arguments(spec, call, catalog),
    ]
    urls = _dataset_urls(spec, provenance)
    lines.extend(_plans(urls, provenance))
    lines.extend(_columns(spec, catalog))
    if urls:
        lines.extend(("", f"## {_LINKS_HEADING}", ""))
        lines.extend(_links(urls, provenance))
    lines.extend(("", *footer))
    return "\n".join(lines) + "\n"


def _transports(spec: DatasetSpec) -> list[Transport]:
    """Return the transports a dataset reads, in the reference's order.

    Args:
        spec: The dataset.

    Returns:
        Each transport of its sources once, in `TRANSPORT_ORDER`.
    """
    used = {transport for transport, _ in spec.sources}
    return [transport for transport in TRANSPORT_ORDER if transport in used]


def _facts(spec: DatasetSpec, call: DatasetCall) -> list[str]:
    """Return the items of a dataset page's list of facts.

    Args:
        spec: The dataset.
        call: Its call, for its query methods.

    Returns:
        "Query methods: ..." and "Read from: ...", then, for a dataset with `fetch()`, what
        `poll()` does by default (`_poll_text`) and, for one whose `APPEND` mode also keys by
        `revision_keys`, what that mode does.
    """
    facts = [
        f"Query methods: {_methods(call)}",
        "Read from: " + ", ".join(TRANSPORT_LABELS[t] for t in _transports(spec)),
    ]
    if spec.has_fetch:
        facts.append(f"`poll()` by default: {_poll_text(spec)}")
        if spec.revision_keys:
            keys = _column_list((*spec.poll_keys, *spec.revision_keys))
            facts.append(
                f"`poll(mode=dm.PollMode.APPEND)`: appends each new state of a row, "
                f"identified by {keys}"
            )
    return facts


def _arguments(spec: DatasetSpec, call: DatasetCall, catalog: Catalog) -> list[str]:
    """Render a dataset page's Arguments section: one row per argument of its method.

    Args:
        spec: The dataset, for the `sort` fields and `filters` keys it takes.
        call: The dataset's call, for its arguments, their docstring lines, and the wire
            parameters they set.
        catalog: The catalog that holds its REST paths.

    Returns:
        The heading, a guide that links the index's shared rules when an argument has one,
        and a table of **Argument** (linked to its rule in `ARGUMENT_RULES` on the index, when
        it has one) and **Description** (the docstring's line, then, for `sort`, the fields it
        takes (`_sort_text`) and, for `filters`, the keys it accepts), each line preceded by a
        blank line; nothing for a method that takes no argument.
    """
    if not call.arguments:
        return []
    described = dict(zip(call.arguments, call.descriptions, strict=False))
    lines = ["", "## Arguments", ""]
    if any(name in ARGUMENT_RULES for name in call.arguments):
        lines.extend((_ARGUMENTS_GUIDE, ""))
    lines.append(_row(["Argument", "Description"]))
    lines.append(_row(["---"] * 2))
    for name in call.arguments:
        text = described.get(name, "")
        added = ""
        if name == "sort":
            added = _sort_text(spec, catalog, text)
        elif name == "filters":
            keys = filter_keys(catalog, spec, typed=call.typed)
            added = "Keys: " + (", ".join(f"`{key}`" for key in keys) or "none") + "."
        shown = f"[`{name}`]({INDEX}#{name})" if name in ARGUMENT_RULES else f"`{name}`"
        lines.append(_row([shown, " ".join(part for part in (text, added) if part)]))
    return lines


def _poll_text(spec: DatasetSpec) -> str:
    """Say what a dataset's `poll()` does by default.

    Args:
        spec: A dataset with `fetch()`.

    Returns:
        The mode, then its effect and keys, such as "`dm.PollMode.REPLACE`, which holds only
        the latest poll's rows, one row per `Symbol`"; an `APPEND` dataset's rows are
        "identified by" its keys.
    """
    text = f"`dm.PollMode.{spec.poll_mode.name}`, which {_POLL_MODES[spec.poll_mode]}"
    if spec.poll_keys:
        keyed = "identified by" if spec.poll_mode is PollMode.APPEND else "one row per"
        text = f"{text}, {keyed} {_column_list(spec.poll_keys)}"
    return text


def _column_list(names: Sequence[str]) -> str:
    """Join column names for a sentence, each in backticks.

    Args:
        names: The names; at least one.

    Returns:
        The names joined by `joined_with_and`, such as "`Symbol` and `IssuerName`".
    """
    return joined_with_and([f"`{name}`" for name in names])


def _sort_text(spec: DatasetSpec, catalog: Catalog, line: str) -> str:
    """Say which `sort` fields a dataset takes, from its REST paths' `sort` parameters.

    Args:
        spec: The dataset.
        catalog: The catalog that holds its REST paths.
        line: The `sort` line of the method's docstring; a list of fields that it already
            quotes in full is not repeated.

    Returns:
        "Fields: " and the enum values, in order and distinct, of every path whose `sort` is an
        enum, and "any REST field under Massive fields", linked to the page's folded table, for
        a path whose `sort` is free text; "Fields: none." when no path has `sort`; empty when
        `line` quotes every value of an enum `sort` and no path's `sort` is free text.
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
    if values and not free and all(f'"{value}"' in line for value in values):
        return ""
    parts = [", ".join(f"`{value}`" for value in values)] if values else []
    if free:
        parts.append(f"any REST field under [Massive fields](#{_WIRE_ANCHOR})")
    return "Fields: " + (" or ".join(parts) or "none") + "."


def _plans(urls: Iterable[str], provenance: Provenance) -> list[str]:
    """Render the plan tables of a dataset's vendor pages as one table.

    Pages of one title and area, such as one per asset class, share one column. Where they
    state different terms for one plan, each term names the class its page's URL names, such
    as "crypto: Updated as needed, 2 years; forex: Updated hourly, 2 years".

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
    lines = ["", f"## {_PLANS_HEADING}", "", _PLANS_GUIDE]
    if listed:
        plans = list(dict.fromkeys(terms.plan for _, page in listed for terms in page.plans))
        titles = [_label(url, provenance) for url, _ in listed]
        columns = {
            title: [pair for pair, own in zip(listed, titles, strict=True) if own == title]
            for title in titles
        }
        header = ["Plan", *columns]
        lines.extend(("", _row(header), _row(["---"] * len(header))))
        for plan in plans:
            cells = []
            for column in columns.values():
                texts = {
                    url: _plan_cell(t.access, t.recency, t.history)
                    for url, page in column
                    for t in page.plans
                    if t.plan == plan
                }
                if len(set(texts.values())) > 1:
                    texts = {url: _named(url, text) for url, text in texts.items()}
                cells.append("; ".join(dict.fromkeys(texts.values())))
            lines.append(_row([plan, *cells]))
    unlisted = [(url, page) for url, page in pages if not page.plans]
    if unlisted:
        lines.append("")
        lines.extend(
            f"- {_label(url, provenance)}: "
            + " ".join(text for text in (page.access, page.recency, page.history) if text)
            for url, page in unlisted
        )
    return lines


def _named(url: str, text: str) -> str:
    """Prefix a plan cell's text with the asset class its page's URL names.

    Args:
        url: The vendor page the text comes from.
        text: The plan's terms on that page.

    Returns:
        Such as "crypto: Updated as needed, 2 years"; the text alone when the URL names no
        class.
    """
    named = _class_named(url)
    return text if named is None else f"{named}: {text}"


def _class_named(url: str) -> str | None:
    """Return the asset class a vendor page's URL names as a path segment, or None.

    Args:
        url: The page's URL, such as `"https://massive.com/docs/rest/crypto/tickers.md"`.

    Returns:
        The class's value, such as `"crypto"`; None for a URL that names none.
    """
    return next((c.value for c in AssetClass if c.value in _segments(url)), None)


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
        named = _class_named(url)
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
        The section's lines, starting with a blank line. The columns table has a **Read from**
        column only for a dataset read from more than one transport; the page's list of facts
        names the one transport of any other.
    """
    wire = _transports(spec)
    several = len(wire) > 1
    schema = catalog.dataset_schema(spec)  # the default session's columns, RawSymbol included
    header = ["Column", "Type", "Description", *(["Read from"] if several else [])]
    lines = ["", "## Columns", "", _row(header), _row(["---"] * len(header))]
    lines.extend(_row(_cells(column, several=several)) for column in schema.columns)
    fields = ["Column", *(_WIRE_HEADINGS[t] for t in wire)]
    lines.extend(
        (
            "",
            f'<a id="{_WIRE_ANCHOR}"></a>',
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


def _cells(column: Column, *, several: bool) -> list[str]:
    """Return the cells of one column's row of the columns table, before escaping.

    Args:
        column: The schema column.
        several: Whether the dataset reads more than one transport, so the row names the
            transports that fill the column.

    Returns:
        The column's name, type, and description, then its transports when `several`.
    """
    cells = [f"`{column.name}`", column.type.value, column.description]
    if several:
        cells.append(", ".join(TRANSPORT_LABELS[t] for t in column.supplied_by))
    return cells


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


_HEADING: Final = re.compile(r"^# (m\.\S+)$")
"""A dataset page's title, which names its dataset's call path."""

_COLUMNS_HEADING: Final = "## Columns"
"""The heading of a dataset page's columns table."""

_COLUMN_ROW: Final = re.compile(r"^\| `([^`]+)` \| ([^|]+) \|")
"""A row of a page's columns table: its first cell names the column, its second the type."""


def _columns_by_dataset(pages: Mapping[str, str]) -> dict[str, dict[str, str]]:
    """Read each dataset's columns and their Deephaven types back from rendered pages.

    The pages are the only record of what the reference said before a change, so the report
    reads them back in the format `_page` writes: a `# m.…` title, then, under the
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
