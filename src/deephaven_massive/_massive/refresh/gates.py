"""The refresh gates: what stops a refresh, how a stop is reported, and the cross-source checks.

A **gate** is a rule the generated schemas and the hand input must pass before anything is
written. Every generator collects its problems instead of stopping at the first, and the
refresh then stops with all of them at once (`RefreshError`), each naming its gate, the
source, the key, and the vendor's description for the reviewer. The description is shown to
help a person choose a name; it never chooses one. `Gate` lists every gate and what it
rejects.

This module owns the vocabulary every other refresh module reports in (`Gate`, `Problem`,
`RefreshError`) and the checks that span sources or read the hand input as a whole: stale hand
entries (`stale_entries`, `unneeded_type_fixes`, `unused_description_fixes`,
`check_exclusions`) and one concept per column within a dataset kind (`concept_collisions`).
The checks that need one field or parameter at a time run inside the generator that meets it
(`rest_sources`, `ws_sources`, `flat_file_sources`); the checks here read what each source consumed
through `PathUse`, so this module depends on no generator. The checks that read the generated
sources of a kind across transports (one type and two transports per column, the column order) are
`kinds`'s.

It imports the standard library, `naming`, and `spec.types` at run time, and the hand-input
types for type checking only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from deephaven_massive._massive.refresh import naming
from deephaven_massive._massive.spec.types import Transport

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.hand_input import HandInput
    from deephaven_massive._massive.refresh.overrides import TypeFix

__all__ = [
    "Gate",
    "PathUse",
    "Problem",
    "RefreshError",
    "check_exclusions",
    "concept_collisions",
    "stale_entries",
    "unneeded_type_fixes",
    "unused_description_fixes",
]


class Gate(StrEnum):
    """A refresh gate. Its value is the name a problem is printed with."""

    SHORT_KEY = "short-key"
    """A short wire key (three characters or fewer, no underscore) with no lexicon, identity,
    or `NAMES` entry: a column name would be guessed."""
    COLUMN_COLLISION = "column-collision"
    """Two fields of one source that give one column, two keys a projection gives one column,
    or a `NAMES` entry that coins a dataset key column (`Symbol`, `Timestamp`, `Underlying`),
    which only the dataset key rename makes."""
    TIMESTAMP_UNIT = "timestamp-unit"
    """A timestamp-like field left to a default: a `long` or `double` with no `UNITS` entry, or
    text with no `format` and neither a `UNITS` nor a `TYPE_FIXES` entry; or a timestamp-like
    string parameter with no `PARAM_UNITS` entry. A field is timestamp-like when the
    specification tags it with a unit, or the snake case of its long name is `timestamp`,
    `*_timestamp`, `updated`, `*_updated`, `window_start`, `window_end`, or `*_time`."""
    STALE_ENTRY = "stale-entry"
    """A hand entry (a lexicon shape, identity, `NAMES`, `UNITS`, tag-conflict, `TYPE_FIXES`,
    `KEY_FIXES`, `DESCRIPTION_FIXES`, `RESULT_KEYS`, `PARAM_UNITS`, `EXCLUSIONS`, or
    `SAMPLE_ERRORS` entry) naming a path, field, parameter, or sample the snapshot no longer
    has, or claiming what the snapshot contradicts (a `TYPE_FIXES` entry no documented sample
    needs any more, a `KEY_FIXES` entry whose key the example no longer sends, or a
    `DESCRIPTION_FIXES` entry quoting a description the vendor no longer gives, among them)."""
    TYPE_AGREEMENT = "type-agreement"
    """Within one of `overrides.KINDS`, one column whose `WireType`s do not join: the type
    lattice joins `long` with `double` to `double`, `long` with `int-array` to `int-array`,
    and an `epoch` with an `epoch` (each source keeping its unit), and nothing else. A
    conflict is first a possible naming error (one name for two fields), so the problem
    prints both fields' descriptions."""
    TRANSPORT_AGREEMENT = "transport-agreement"
    """Within one group of `overrides.KINDS` whose sources span two or more transports, a
    column only one transport supplies and `TRANSPORT_ONLY` does not name: often a one-sided
    lexicon slip (a trade's `s` named `Shares` on one transport while another says `size`)."""
    VENUE_AGREEMENT = "venue-agreement"
    """A futures flat-file dataset whose venue pages list different columns, or lack a venue
    of `VENUES`."""
    COLUMN_ORDER = "column-order"
    """A source whose column order cannot be taken from its dataset kind: its group has no
    flat-file or WebSocket member keeping all its fields to give the order, or two of its
    groups order its columns differently."""
    CONCEPT_COLLISION = "concept-collision"
    """Within one of `overrides.KINDS`, one column holding two concepts (a concept is a field's
    full long-name path, in snake case)."""
    EXAMPLE_TYPE = "example-type"
    """A documented example value (of the response's example, or of a field's own `example`)
    that the generated field refuses: the generated type and the documented wire disagree, so
    real data shaped like the sample would fail at run time. A `TYPE_FIXES` entry citing the
    sample decides the type, or a `SAMPLE_ERRORS` entry records that the sample is wrong. A
    response example with no value where the result key points fails it too, since nothing
    would be checked."""
    UNITS = "units"
    """A `UNITS` unit that differs from the specification's `x-polygon-go-type` tag without a
    tag-conflict entry."""
    ENVELOPE = "envelope"
    """An envelope key of a single-object body whose type is not the envelope's."""
    DOCS = "docs"
    """A documented endpoint the specification lacks, a generated path no page names, a page
    that is not an overview yet names no endpoint, a page the index links that the snapshot
    lacks, or an exclusion whose `undocumented` claim the documentation contradicts."""
    SHAPE = "shape"
    """A response the generator cannot read (no rows under the result key, an untyped field, a
    parameter outside the path or query, a parameter choice that is not of date formats), or an
    unknown `x-polygon-*` extension on a path or its operation."""
    SNAPSHOT = "snapshot"
    """A vendor file whose SHA-256 differs from the manifest's, a file set that differs from the
    manifest's, or no snapshot at all."""


@dataclass(frozen=True, kw_only=True)
class Problem:
    """One reason the refresh stops.

    Attributes:
        gate: The gate that found it.
        source: The source concerned (a REST path template, a WebSocket channel or
            specification path, a flat-file prefix, or a dataset kind), or `""` when none is.
        key: The wire key, parameter, or hand-input item concerned; `""` when none.
        message: What is wrong and what would fix it.
        description: The vendor's description of the field or parameter, when it has one.
    """

    gate: Gate
    source: str
    key: str
    message: str
    description: str = ""

    def text(self) -> str:
        """Return the problem as the refresh prints it.

        Returns:
            `"<gate> <source> <key>: <message>"` (an empty source or key left out), followed
            by an indented line with the vendor's description when there is one.
        """
        text = " ".join(part for part in (self.gate, self.source, self.key) if part)
        text += f": {self.message}"
        if self.description:
            text += f"\n    description: {' '.join(self.description.split())}"
        return text


class RefreshError(Exception):
    """The refresh found problems and wrote nothing.

    Attributes:
        problems: Every problem found, sorted by gate, path, and key.
    """

    def __init__(self, problems: Iterable[Problem]) -> None:
        """Collect the problems into one error.

        Args:
            problems: The problems found; at least one.
        """
        self.problems: tuple[Problem, ...] = tuple(
            sorted(problems, key=lambda p: (p.gate, p.source, p.key, p.message))
        )
        lines = [f"the refresh stopped on {len(self.problems)} problem(s):"]
        lines += [problem.text() for problem in self.problems]
        super().__init__("\n".join(lines))


@dataclass(frozen=True, kw_only=True)
class PathUse:
    """What generating one source consumed, for the checks that span sources.

    A generator fills one per source it attempts, even one that stopped on a problem, so a
    hand entry for that source is not reported stale as well.

    Attributes:
        wire_keys: The dotted wire key of every leaf field.
        objects: The dotted wire key of every nested object, and `""` for the row itself.
        unit_bases: The base names (without a filter suffix) of the string parameters a
            `PARAM_UNITS` entry decided.
        identities: The `IDENTITY` keys the path used.
        concepts: Each generated column to its concept: the field's full long-name path, each
            segment in snake case (`last_trade.price`).
        columns: Each generated field's wire key to its column.
        converted: The wire keys of the generated fields that have a conversion.
        needed_conversions: The wire keys of the fields with a conversion that some
            documented example value needs (a value the declared type alone refuses).
        example_keys: The keys a documented example sends that the source does not declare,
            each excused by a `SAMPLE_ERRORS` entry.
        described_keys: The wire keys of the generated fields, and of the nested objects
            whose notes they carry, whose `DESCRIPTION_FIXES` entry the generator looked up:
            only these entries can take effect.
    """

    wire_keys: frozenset[str] = frozenset()
    objects: frozenset[str] = frozenset({""})
    unit_bases: frozenset[str] = frozenset()
    identities: frozenset[str] = frozenset()
    concepts: Mapping[str, str] = field(default_factory=dict)
    columns: Mapping[str, str] = field(default_factory=dict)
    converted: frozenset[str] = frozenset()
    needed_conversions: frozenset[str] = frozenset()
    example_keys: frozenset[str] = frozenset()
    described_keys: frozenset[str] = frozenset()


def concept_collisions(hand: HandInput, uses: Mapping[str, PathUse]) -> Iterator[Problem]:
    """Require one concept per column, and one column per concept, within each dataset kind.

    A field's concept is its full long-name path, each segment in snake case (`day.close`,
    `last_trade.sip_timestamp`), so a snapshot's `DayClose`, `LastMinuteClose`, and
    `PrevDayClose` are three concepts, and the v2 snapshot's `lastTrade.p` and the unified
    snapshot's `last_trade.price` are one. Within the sources of one dataset kind, which merge
    into one table, a column must always hold the same concept, or two quantities would land
    in one column: two paths can read alike once PascalCased (`last.trade_price` and
    `last_trade.price` both give `LastTradePrice`). The reverse, one concept in two columns,
    cannot happen while a column is the PascalCase of its concept, but a projection renames a
    member's fields: its column holds the concept of its long name, and the field's own
    concept must not also sit in another column of the kind.

    Args:
        hand: The hand input, whose `kinds` lists each kind's sources and whose `projections`
            rename some of them.
        uses: What each generated source consumed; a kind source with no entry adds nothing.

    Yields:
        One `concept-collision` problem per column that holds two concepts within a kind, and
        one per concept that sits in two columns.
    """
    for kind, groups in sorted(hand.kinds.items()):
        concepts: dict[str, set[str]] = {}
        columns: dict[str, set[str]] = {}
        for source in (s for group in groups.values() for s in group):
            use = uses.get(source) or PathUse()
            projection = hand.projections.get((kind, source))
            if projection is None:
                found = [(column, concept, concept) for column, concept in use.concepts.items()]
            else:
                renamed = [
                    (
                        naming.column_name([long_name]),
                        naming.snake(long_name),
                        use.concepts[use.columns[wire_key]],
                    )
                    for wire_key, long_name in projection.named.items()
                    if wire_key in use.columns
                ]
                kept = [
                    (column, use.concepts[column], use.concepts[column])
                    for wire_key, column in use.columns.items()
                    if projection.renames is not None and wire_key not in projection.renames
                ]
                found = [*renamed, *kept]
            for column, concept, own in found:
                concepts.setdefault(column, set()).add(concept)
                columns.setdefault(own, set()).add(column)
        for column, names in sorted(concepts.items()):
            if len(names) > 1:
                yield Problem(
                    gate=Gate.CONCEPT_COLLISION,
                    source="",
                    key=column,
                    message=f"kind {kind!r}: one column holds the concepts {sorted(names)}",
                )
        for concept, named in sorted(columns.items()):
            if len(named) > 1:
                yield Problem(
                    gate=Gate.CONCEPT_COLLISION,
                    source="",
                    key=concept,
                    message=f"kind {kind!r}: one concept sits in the columns {sorted(named)}",
                )


def stale_entries(hand: HandInput, uses: Mapping[str, PathUse]) -> Iterator[Problem]:
    """Report the hand entries that name a source, field, object, or parameter not generated.

    The entries checked here are those whose staleness shows only once every source of every
    transport has run: `NAMES`, `UNITS`, `TAG_CONFLICTS`, `TYPE_FIXES`, `KEY_FIXES` (by the
    declared key), `SAMPLE_ERRORS`, lexicon `SHAPES`, `PARAM_UNITS`, `IDENTITY`, and
    `RESULT_KEYS`; `unused_description_fixes` checks `DESCRIPTION_FIXES`. An entry the
    snapshot contradicts on a field it does generate is reported by the generator that meets
    the field.

    Args:
        hand: The hand input.
        uses: What each generated path consumed.

    Yields:
        One `stale-entry` problem per stale entry.
    """
    field_tables: list[tuple[str, Iterable[tuple[str, str]]]] = [
        ("NAMES", hand.names),
        ("UNITS", hand.units),
        ("TAG_CONFLICTS", hand.tag_conflicts),
        ("TYPE_FIXES", hand.type_fixes),
        ("KEY_FIXES", hand.key_fixes),
        ("SAMPLE_ERRORS", [(e.path, e.wire_key) for e in hand.sample_errors]),
    ]
    for table, keys in field_tables:
        for path, wire_key in keys:
            use = uses.get(path)
            if use is None or wire_key not in use.wire_keys | use.example_keys:
                yield Problem(
                    gate=Gate.STALE_ENTRY,
                    source=path,
                    key=wire_key,
                    message=f"{table} entry for a field the snapshot does not generate",
                )
    for path, prefix in hand.shapes:
        use = uses.get(path)
        if use is None or prefix not in use.objects:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=path,
                key=prefix,
                message="lexicon shape for an object the snapshot does not generate",
            )
    for path, base in hand.param_units:
        use = uses.get(path)
        if use is None or base not in use.unit_bases:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=path,
                key=base,
                message="PARAM_UNITS entry for no string parameter the snapshot generates",
            )
    used = set().union(*(use.identities for use in uses.values()))
    for key in sorted(set(hand.identity) - used):
        yield Problem(
            gate=Gate.STALE_ENTRY,
            source="",
            key=key,
            message="IDENTITY entry for a key no generated object has",
        )
    for path in hand.result_keys:
        if path not in uses:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=path,
                key="",
                message="RESULT_KEYS entry for a path the snapshot does not generate",
            )


def unused_description_fixes(hand: HandInput, uses: Mapping[str, PathUse]) -> Iterator[Problem]:
    """Report the `DESCRIPTION_FIXES` entries no generated field or object note looked up.

    An entry takes effect only through the field it is keyed by, after any `KEY_FIXES` entry,
    or through the nested object it is keyed by, whose note every field below the object
    carries. So it is checked against the keys the generators looked up
    (`PathUse.described_keys`), not every key a source declares or an example sends: an entry
    under a declared key that a key fix replaces, under a key only an example sends, or under
    an object no generated field sits in, would otherwise be ignored without a word. An entry
    whose quote no longer matches is reported by the generator that meets its field or
    object.

    Args:
        hand: The hand input.
        uses: What each generated path consumed.

    Yields:
        One `stale-entry` problem per entry no field or object note looked up.
    """
    for path, wire_key in hand.description_fixes:
        use = uses.get(path)
        if use is None or wire_key not in use.described_keys:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=path,
                key=wire_key,
                message="DESCRIPTION_FIXES entry for no field or nested object the snapshot "
                "generates under that wire key",
            )


def unneeded_type_fixes(hand: HandInput, uses: Mapping[str, PathUse]) -> Iterator[Problem]:
    """Report the `TYPE_FIXES` entries no documented example needs any more.

    A widening conversion (`Conversion.widens`) exists because a documented sample is in a form
    the declared type refuses. Once no example value of any field the fix names is such a value
    (Massive corrected its sample), the fix is stale. One `TypeFix` object is one decision: the
    entries it fixes, across sources and transports, are needed while any of them is, while
    two fixes written separately are judged separately, however alike. A conversion that only
    retypes values its declared type accepts (`iso-text-instant`) is exempt: it is needed for
    as long as the specification declares the type, and the declared-type check retires it
    once that changes. A field that was not generated with its conversion (it stopped on
    another problem, or the entry is already stale) is left to the check that reported it.

    Args:
        hand: The hand input.
        uses: What each generated path consumed.

    Yields:
        One `stale-entry` problem per generated field of each unneeded fix.
    """
    decisions: dict[int, tuple[TypeFix, list[tuple[str, str]]]] = {}
    for key, fix in hand.type_fixes.items():
        decisions.setdefault(id(fix), (fix, []))[1].append(key)
    for fix, keys in decisions.values():
        if not fix.conversion.widens:
            continue
        generated = [(p, k) for p, k in keys if p in uses and k in uses[p].converted]
        if any(k in uses[p].needed_conversions for p, k in generated):
            continue
        for path, wire_key in generated:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=path,
                key=wire_key,
                message=f"TYPE_FIXES entry ({fix.conversion}) that no documented example "
                "needs: the declared type now accepts every sample",
            )


def check_exclusions(
    hand: HandInput,
    statuses: Mapping[str, str],
    pages: Mapping[str, Sequence[str]],
) -> Iterator[Problem]:
    """Check each REST exclusion against the specification and the documentation.

    Args:
        hand: The hand input.
        statuses: Each specification path's status: `"deprecated"`, `"draft"`, or `""` for a
            path the vendor publishes as current.
        pages: Each documented path's page names.

    Yields:
        A `stale-entry` problem for an exclusion naming a path the specification lacks or
        already marks deprecated or draft, and a `docs` problem for an `undocumented` claim the
        documentation contradicts, in either direction.
    """
    for entry in hand.exclusions:
        if entry.kind is not Transport.REST:
            continue
        if entry.item not in statuses:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=entry.item,
                key="",
                message="EXCLUSIONS entry for a path the specification lacks",
            )
        elif statuses[entry.item]:
            yield Problem(
                gate=Gate.STALE_ENTRY,
                source=entry.item,
                key="",
                message=f"EXCLUSIONS entry for a path the specification marks "
                f"{statuses[entry.item]}, which already accounts for it",
            )
        elif entry.undocumented and entry.item in pages:
            yield Problem(
                gate=Gate.DOCS,
                source=entry.item,
                key="",
                message=f"excluded as undocumented, but {', '.join(pages[entry.item])} "
                "documents it",
            )
        elif not entry.undocumented and entry.item not in pages:
            yield Problem(
                gate=Gate.DOCS,
                source=entry.item,
                key="",
                message="no documentation page names it; mark the exclusion undocumented",
            )
