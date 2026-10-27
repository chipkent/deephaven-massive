"""Generate the flat-file sources from the snapshot: every documented dataset prefix to a `Source`.

`generate_flat_files` reads the snapshot's flat-file documentation pages and returns one
`Source` per generated prefix, with an account of every documented prefix
(`FlatFileCoverage`), or raises `RefreshError` with every problem found. Nothing here touches
the network or a file.

**Which prefixes are generated.** Each asset class's flat-file overview page lists its
datasets in a table: one row per dataset page, with the dataset's bucket prefix
(`S3 us_stocks_sip/trades_v1`). That row's prefix is the source key, so no hand-written
page-to-prefix map exists. Every documented prefix is generated unless an
`exclusions.EXCLUSIONS` entry of transport `FLAT_FILES` names it, with a reason. A dataset
page that no overview row names is not generated; the live drift check reports it.

**Venues.** Massive publishes each futures dataset once per venue (`us_futures_cme/...`,
`us_futures_cbot/...`), each with its own page. A prefix whose first segment ends in
`_<venue>` for a venue of `overrides.VENUES` is folded into one source keyed by the template
`us_futures_{venue}/trades_v1`, which the flat-file client fills per venue. The folded pages
must list identical columns, or the `venue-agreement` gate stops the refresh, and each folded
dataset must have a page for every venue.

**Fields** come from the dataset page's "Columns" table: each row's name is the CSV header and
wire key, its type is `integer` (`long`), `number` (`double`), `string`, or
`timestamp - integer` (an integer the vendor documents as a timestamp, which then needs its
`UNITS` entry), and its description is the field's. The table lists the columns
alphabetically; the files carry them in the order the page's "Example Data" header shows, so
that header gives the fields' order, and it must name exactly the table's columns (ignoring
case: some pages capitalize `Ticker`). The sample rows are not read. A flat file marks
nothing required. Names, types, and units are decided by the rules every transport shares
(`fields`).

It imports the standard library and other `_massive` modules only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import docs
from deephaven_massive._massive.refresh.fields import RawField, SourceFields
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem, RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi
from deephaven_massive._massive.spec.types import Source, Transport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import JsonObject
    from deephaven_massive._massive.refresh.snapshot import Snapshot

__all__ = ["FlatFileCoverage", "generate_flat_files", "venue_template"]

# Each documented column type, as the schema the field rules read, and whether the vendor
# documents it as a timestamp.
_COLUMN_TYPES: Final[Mapping[str, tuple[str, bool]]] = {
    "integer": ("integer", False),
    "number": ("number", False),
    "string": ("string", False),
    "timestamp - integer": ("integer", True),
}
# The placeholder a venue's name is folded into.
_VENUE: Final = "{venue}"


@dataclass(frozen=True, kw_only=True)
class FlatFileCoverage:
    """How every documented flat-file prefix is accounted for.

    Attributes:
        specified: Every prefix an overview row names, venues folded into the template
            (`"us_futures_{venue}/trades_v1"`).
        generated: The keys of the generated sources.
        excluded: The prefixes named by an exclusion.
        pages: Each specified prefix mapped to the names of its dataset pages (one per
            venue for a folded prefix), sorted.
        uses: What generating each attempted source consumed, by source key.
    """

    specified: frozenset[str]
    generated: frozenset[str]
    excluded: frozenset[str]
    pages: Mapping[str, tuple[str, ...]]
    uses: Mapping[str, PathUse]


def venue_template(prefix: str, venues: Sequence[str]) -> tuple[str, str | None]:
    """Fold a venue's name out of a prefix.

    Args:
        prefix: A documented prefix, such as `"us_futures_cme/trades_v1"`.
        venues: The venue names (`overrides.VENUES`).

    Returns:
        The template and the venue (`("us_futures_{venue}/trades_v1", "cme")`), or the prefix
        unchanged and `None` when its first segment ends in no venue.
    """
    head, _, rest = prefix.partition("/")
    for venue in venues:
        if head.endswith(f"_{venue}"):
            return f"{head.removesuffix(venue)}{_VENUE}/{rest}", venue
    return prefix, None


class _FileGenerator(SourceFields):
    """Generate one prefix's `Source` from its dataset page, collecting its problems."""

    def generate(self, page: str) -> Source | None:
        """Build the prefix's source from its dataset page.

        Args:
            page: The Markdown text of the dataset page.

        Returns:
            The source, or `None` when a problem prevents building it.
        """
        try:
            table = docs.columns(page)
            header = docs.example_header(page)
        except docs.DocsError as error:
            self.problem(Gate.DOCS, "", str(error))
            return None
        by_name = {column.name.lower(): column for column in table}
        if not table or sorted(by_name) != sorted(cell.lower() for cell in header):
            self.problem(
                Gate.DOCS,
                "",
                f"the Example Data header {header} must name exactly the Columns table's "
                f"columns {[c.name for c in table]}",
            )
            return None
        fields = []
        for cell in header:
            column = by_name[cell.lower()]
            raw = self.raw(column)
            if raw is not None and (item := self.field(raw)) is not None:
                fields.append(item)
        self.check_columns(fields)
        if self.problems:
            return None
        return Source(key=self.source, transport=Transport.FLAT_FILES, fields=tuple(fields))

    def raw(self, column: docs.DocColumn) -> RawField | None:
        """Return a "Columns" row as a field the shared rules read.

        Args:
            column: The row.

        Returns:
            The field, or `None` (with a problem) for a type the pages do not use.
        """
        found = _COLUMN_TYPES.get(column.type)
        if found is None:
            self.problem(Gate.SHAPE, column.name, f"unsupported column type {column.type!r}")
            return None
        kind, timestamp = found
        schema: JsonObject = {"type": kind, "description": column.description}
        return RawField(column.name, schema, required=False, timestamp=timestamp)


def _documented_prefixes(
    snapshot: Snapshot, venues: Sequence[str], problems: list[Problem]
) -> dict[str, dict[str, str]]:
    """Map each prefix the overview pages name to its dataset pages, venues folded.

    Args:
        snapshot: The snapshot.
        venues: The venue names.
        problems: Where an overview row naming a page outside the snapshot is reported
            (`docs`).

    Returns:
        Prefix or template to `{venue or "": page name}`, in overview order.
    """
    prefixes: dict[str, dict[str, str]] = {}
    for name, page in sorted(snapshot.flat_file_pages.items()):
        for url, prefix in docs.datasets(page):
            try:
                target = str(docs.page_file(url, docs.FLAT_FILES))
            except docs.DocsError as error:
                problems.append(
                    Problem(gate=Gate.DOCS, source=prefix, key=name, message=str(error))
                )
                continue
            if target not in snapshot.flat_file_pages:
                problems.append(
                    Problem(
                        gate=Gate.DOCS,
                        source=prefix,
                        key=name,
                        message=f"the overview names {url}, which the index does not link",
                    )
                )
                continue
            template, venue = venue_template(prefix, venues)
            prefixes.setdefault(template, {})[venue or ""] = target
    return prefixes


def _check_venues(
    template: str, pages: Mapping[str, str], *, snapshot: Snapshot, venues: Sequence[str]
) -> list[Problem]:
    """Require a folded dataset to have every venue's page, each listing identical columns.

    Two pages list identical columns when their "Columns" tables hold the same names and
    types (the tables' own order differs between venues) and their "Example Data" headers
    name them in the same order.

    Args:
        template: The folded prefix.
        pages: Venue to its dataset page's name, the generated venue's first.
        snapshot: The snapshot.
        venues: The venue names.

    Returns:
        A `venue-agreement` problem for a missing venue and for each page whose columns
        differ from the first venue's, and a `docs` problem for another venue's page that
        cannot be read (the first page's own problems are the generator's).
    """
    problems = [
        Problem(
            gate=Gate.VENUE_AGREEMENT,
            source=template,
            key=venue,
            message="no overview row names this venue's page",
        )
        for venue in venues
        if venue not in pages
    ]
    generated = next(iter(pages))
    shapes: dict[str, tuple[list[tuple[str, str]], list[str]]] = {}
    for venue, name in pages.items():
        page = snapshot.flat_file_pages[name]
        try:
            table = sorted((c.name, c.type) for c in docs.columns(page))
            shapes[venue] = (table, [cell.lower() for cell in docs.example_header(page)])
        except docs.DocsError as error:
            if venue != generated:
                problems.append(
                    Problem(gate=Gate.DOCS, source=template, key=name, message=str(error))
                )
    first = shapes.get(generated)
    problems += [
        Problem(
            gate=Gate.VENUE_AGREEMENT,
            source=template,
            key=venue,
            message=f"{pages[venue]} lists other columns than {pages[generated]}",
        )
        for venue, shape in shapes.items()
        if first is not None and shape != first
    ]
    return problems


def generate_flat_files(
    snapshot: Snapshot, hand: HandInput | None = None
) -> tuple[tuple[Source, ...], FlatFileCoverage]:
    """Generate every flat-file source of the snapshot and check the gates met per prefix.

    The checks that span sources (stale hand entries, the dataset kinds) run afterwards, over
    every transport, in `generate`.

    Args:
        snapshot: The vendored sources.
        hand: The hand input; the real tables when omitted.

    Returns:
        The generated sources in overview order (the overview pages sorted by name), and how
        every documented prefix is accounted for.

    Raises:
        RefreshError: With every problem found, if there is any.
    """
    hand = hand or HandInput()
    venues = list(hand.venues)
    problems: list[Problem] = []
    prefixes = _documented_prefixes(snapshot, venues, problems)
    excluded: set[str] = set()
    for entry in (e for e in hand.exclusions if e.kind is Transport.FLAT_FILES):
        if entry.item in prefixes:
            excluded.add(entry.item)
        else:
            problems.append(
                Problem(
                    gate=Gate.STALE_ENTRY,
                    source=entry.item,
                    key="",
                    message="EXCLUSIONS entry for a prefix no overview names",
                )
            )
    used_venues = {v for pages in prefixes.values() for v in pages if v}
    problems += [
        Problem(
            gate=Gate.STALE_ENTRY,
            source="",
            key=venue,
            message="VENUES entry for a venue no documented prefix names",
        )
        for venue in venues
        if venue not in used_venues
    ]
    uses: dict[str, PathUse] = {}
    generated: list[Source] = []
    for template, pages in prefixes.items():
        if template in excluded:
            continue
        if _VENUE in template:
            problems += _check_venues(template, pages, snapshot=snapshot, venues=venues)
        first = next(iter(pages.values()))
        generator = _FileGenerator(OpenApi({}), hand, template, origin=snapshot.origin)
        value = generator.generate(snapshot.flat_file_pages[first])
        uses[template] = generator.use()
        problems += generator.problems
        if value is not None:
            generated.append(value)
    if problems:
        raise RefreshError(problems)
    coverage = FlatFileCoverage(
        specified=frozenset(prefixes),
        generated=frozenset(s.key for s in generated),
        excluded=frozenset(excluded),
        pages={key: tuple(sorted(pages.values())) for key, pages in prefixes.items()},
        uses=uses,
    )
    return tuple(generated), coverage
