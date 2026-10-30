"""The dataset kinds across transports: memberships, their gates, and the kind column order.

A **kind** (`overrides.KINDS`) is a set of sources, across transports and asset classes,
whose rows merge into one table schema at run time: the trades of every asset class from REST,
the WebSocket, and the flat files, say. `KINDS` groups each kind's sources by asset class, in
source order. Once every transport is generated, this module:

1. resolves `KINDS` and `PROJECTIONS` into `Member`s, one per source and group, reporting an
   entry that names a source the snapshot does not generate (`stale-entry`);
2. checks that one column joins to one `WireType` within a kind (`type-agreement`), by
   `WireType.common_type`, the rule the run-time schema merge uses too: `long` with
   `double` gives `double`, `long` with `long-array` gives `long-array`, two `epoch`s give an
   `epoch` (each source keeping its unit), and any other pair is a conflict, printed with
   both fields' descriptions since it is first a possible naming error;
3. checks that, within a group whose sources span two or more transports, every column is on
   at least two transports or is in `TRANSPORT_ONLY` for that group (`transport-agreement`),
   which catches a
   one-sided lexicon slip;
4. orders every member's columns from its kind (`column-order`): within each group, the
   flat-file sources and then the WebSocket sources, in `KINDS` order, give the order, which
   are the vendor's own (a file's columns, a message's keys); the group's other sources add
   their remaining columns after, in their own order. A member takes the group's order, so a
   REST member of a kind is reordered too, while a REST source in no kind keeps the
   specification's (alphabetical) order. Last, `naming.lead_order` moves each nested object's
   price, size, and bar columns to its start, as it does for every REST source as generated.
   A group with no flat-file or WebSocket source, or a source two of whose groups order its
   columns differently, fails the refresh;
5. writes each source's memberships (`spec.types.Membership`), which the generated files
   record, so the run-time merge never reads `refresh/`.

A projected membership contributes only its projection's fields, under the kind's names, and
the source keeps its full fields everywhere else. A projection that keeps every field only
renames (the forex last quote's `ask` as `AskPrice`), so its member is ordered, and gives the
order, like an unprojected one; a projection to a few fields does neither. It imports the
standard library and other `_massive` modules only.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.refresh import naming
from deephaven_massive._massive.refresh.gates import Gate, Problem
from deephaven_massive._massive.spec.types import Membership, Source, Transport, WireType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.gates import PathUse
    from deephaven_massive._massive.refresh.hand_input import HandInput
    from deephaven_massive._massive.spec.types import Field

__all__ = [
    "Member",
    "apply",
    "column_orders",
    "members",
    "transport_agreement",
    "type_agreement",
]

# A source of any transport: `apply` returns REST paths as `RestPath`s.
_S = TypeVar("_S", bound=Source)
# The transports whose sources give a kind's column order, in order of precedence.
_ORDER_TRANSPORTS = (Transport.FLAT_FILES, Transport.WS)
# The number of transports from which a group's columns must agree.
_MULTI_TRANSPORT = 2


@dataclass(frozen=True, kw_only=True)
class Member:
    """One source's place in one kind, with the source itself.

    Attributes:
        kind: The dataset kind.
        group: The asset-class group within the kind.
        position: The source's zero-based order within the group.
        source: The generated source.
        projection: The wire keys the membership keeps, each mapped to its PascalCase column
            in the kind; `None` keeps every field under its own column.
        renames_only: Whether the projection keeps every field and only renames some (it
            came from `Projection.renames`), so the member orders like an unprojected one.
    """

    kind: str
    group: str
    position: int
    source: Source
    projection: Mapping[str, str] | None = None
    renames_only: bool = False

    def columns(self) -> list[tuple[str, Field]]:
        """Return the columns the member contributes to its kind, with the field behind each.

        Returns:
            `(column, field)` in field order: every field under its own column, or only the
            projected fields under the projection's columns.
        """
        if self.projection is None:
            return [(f.column, f) for f in self.source.fields]
        return [
            (self.projection[f.wire_key], f)
            for f in self.source.fields
            if f.wire_key in self.projection
        ]


def members(hand: HandInput, sources: Mapping[str, Source]) -> tuple[list[Member], list[Problem]]:
    """Resolve `KINDS` and `PROJECTIONS` into members of the generated sources.

    Args:
        hand: The hand input.
        sources: Every generated source of every transport, by key.

    Returns:
        The members, kind by kind and group by group in `KINDS` order, and a `stale-entry`
        problem for each `KINDS` entry naming a source the snapshot does not generate, each
        `PROJECTIONS` entry of a `(kind, source)` `KINDS` does not list, and each projected
        wire key the source lacks.
    """
    found: list[Member] = []
    problems: list[Problem] = []
    listed: set[tuple[str, str]] = set()
    for kind, groups in hand.kinds.items():
        for group, keys in groups.items():
            for position, key in enumerate(keys):
                listed.add((kind, key))
                source = sources.get(key)
                if source is None:
                    problems.append(
                        Problem(
                            gate=Gate.STALE_ENTRY,
                            source=key,
                            key=kind,
                            message="KINDS entry for a source the snapshot does not generate",
                        )
                    )
                    continue
                projection = _projection(hand, kind, source, problems=problems)
                entry = hand.projections.get((kind, key))
                found.append(
                    Member(
                        kind=kind,
                        group=group,
                        position=position,
                        source=source,
                        projection=projection,
                        renames_only=entry is not None and entry.renames is not None,
                    )
                )
    problems += [
        Problem(
            gate=Gate.STALE_ENTRY,
            source=key,
            key=kind,
            message="PROJECTIONS entry for a source its kind does not list in KINDS",
        )
        for kind, key in hand.projections
        if (kind, key) not in listed
    ]
    return found, problems


def _projection(
    hand: HandInput, kind: str, source: Source, *, problems: list[Problem]
) -> Mapping[str, str] | None:
    """Return a source's projection in one kind, as wire key to PascalCase column.

    Args:
        hand: The hand input.
        kind: The kind.
        source: The generated source.
        problems: Where a projected wire key the source lacks is reported (`stale-entry`), and
            two projected keys that take one column (`column-collision`).

    Returns:
        The projection, its long names through the naming rule: the kept fields of a
        projection that keeps some, or every field (renamed or under its own column) of one
        that renames; `None` when `PROJECTIONS` has no entry for the source in the kind.
    """
    entry = hand.projections.get((kind, source.key))
    if entry is None:
        return None
    wire_keys = {f.wire_key for f in source.fields}
    problems.extend(
        Problem(
            gate=Gate.STALE_ENTRY,
            source=source.key,
            key=wire_key,
            message=f"PROJECTIONS entry of kind {kind!r} for a field the source lacks",
        )
        for wire_key in sorted(set(entry.named) - wire_keys)
    )
    if entry.renames is not None:
        projection = {
            f.wire_key: naming.column_name([entry.renames[f.wire_key]])
            if f.wire_key in entry.renames
            else f.column
            for f in source.fields
        }
    else:
        named = entry.named
        projection = {k: naming.column_name([v]) for k, v in named.items() if k in wire_keys}
    taken: dict[str, str] = {}
    for wire_key, column in projection.items():
        if column in taken:
            problems.append(
                Problem(
                    gate=Gate.COLUMN_COLLISION,
                    source=source.key,
                    key=wire_key,
                    message=f"PROJECTIONS of kind {kind!r} gives it the column {column!r} of "
                    f"{taken[column]!r}",
                )
            )
        taken.setdefault(column, wire_key)
    return projection


def type_agreement(found: Sequence[Member]) -> Iterator[Problem]:
    """Require one `WireType` per column within each kind.

    Args:
        found: The members.

    Yields:
        One `type-agreement` problem per column whose types do not join, naming the first
        pair of fields that conflict, with both descriptions.
    """
    seen: dict[tuple[str, str], tuple[WireType, list[tuple[Member, Field]]]] = {}
    reported: set[tuple[str, str]] = set()
    for member in found:
        for column, item in member.columns():
            key = (member.kind, column)
            if key not in seen:
                seen[key] = (item.wire_type, [(member, item)])
                continue
            joined, contributors = seen[key]
            merged = joined.common_type(item.wire_type)
            if merged is not None:
                seen[key] = (merged, [*contributors, (member, item)])
            elif key not in reported:
                reported.add(key)
                other, field = next(
                    (m, f)
                    for m, f in contributors
                    if f.wire_type.common_type(item.wire_type) is None
                )
                yield Problem(
                    gate=Gate.TYPE_AGREEMENT,
                    source=member.kind,
                    key=column,
                    message=f"{_cite(other, field)} and {_cite(member, item)} do not join; "
                    "first make sure they are one field, then fix the name or add a "
                    "TYPE_FIXES entry",
                )


def _cite(member: Member, item: Field) -> str:
    """Return a field's type, source, wire key, and description, for a problem.

    Args:
        member: The member the field belongs to.
        item: The field.

    Returns:
        Such as `long stocks/T 's' ("The trade size.")`.
    """
    return f"{item.wire_type} {member.source.key} {item.wire_key!r} ({item.description!r})"


def transport_agreement(hand: HandInput, found: Sequence[Member]) -> Iterator[Problem]:
    """Require every column of a multi-transport group on two transports, or `TRANSPORT_ONLY`.

    Args:
        hand: The hand input.
        found: The members.

    Yields:
        One `transport-agreement` problem per column one transport alone supplies within a
        group whose sources span two or more transports, unless `TRANSPORT_ONLY` names the
        kind, group, and column; and a `stale-entry` problem for each `TRANSPORT_ONLY` entry
        no such column needs.
    """
    groups: dict[tuple[str, str], list[Member]] = {}
    for member in found:
        groups.setdefault((member.kind, member.group), []).append(member)
    needed: set[tuple[str, str, str]] = set()
    for (kind, group), grouped in groups.items():
        if len({m.source.transport for m in grouped}) < _MULTI_TRANSPORT:
            continue
        transports: dict[str, set[Transport]] = {}
        for member in grouped:
            for column, _ in member.columns():
                transports.setdefault(column, set()).add(member.source.transport)
        for column, served in transports.items():
            if len(served) > 1:
                continue
            needed.add((kind, group, column))
            if (kind, group, column) not in hand.transport_only:
                (only,) = served
                yield Problem(
                    gate=Gate.TRANSPORT_AGREEMENT,
                    source=kind,
                    key=column,
                    message=f"group {group!r}: only {only} supplies it; check the names of "
                    "the other transports' fields, or add a TRANSPORT_ONLY entry",
                )
    for kind, group, column in sorted(set(hand.transport_only) - needed):
        yield Problem(
            gate=Gate.STALE_ENTRY,
            source=kind,
            key=column,
            message=f"TRANSPORT_ONLY entry for a column no single transport alone supplies in "
            f"group {group!r}",
        )


def column_orders(
    found: Sequence[Member], uses: Mapping[str, PathUse]
) -> tuple[dict[str, tuple[str, ...]], list[Problem]]:
    """Order the fields of every member that keeps all its fields from its group.

    Within each `(kind, group)`, the group's order is the columns of its flat-file sources,
    then of its WebSocket sources, then of its other sources, each in `KINDS` order and each
    column at its first appearance, then put in lead order (`naming.lead_order`, by the
    concept of the field behind each column's first appearance). A member that keeps all its
    fields (unprojected, or with a projection that only renames, `Projection.renames`) takes
    that order and may give it; a member projected to a few of its fields does neither, and
    its source keeps its own order.

    Args:
        found: The members.
        uses: What generating each source consumed, by key; every member's source has an
            entry, whose concepts give each field's nested-object group and own name.

    Returns:
        Each reordered source's key mapped to its wire keys in order, and a `column-order`
        problem for each member of a group with no flat-file or WebSocket member that keeps
        all its fields to give the order, and for each source whose groups order its fields
        differently.
    """
    groups: dict[tuple[str, str], list[Member]] = {}
    for member in found:
        groups.setdefault((member.kind, member.group), []).append(member)
    orders: dict[str, tuple[str, ...]] = {}
    problems: list[Problem] = []
    for (kind, group), grouped in groups.items():
        whole = [m for m in grouped if _keeps_every_field(m)]
        ordered = [m for t in _ORDER_TRANSPORTS for m in whole if m.source.transport is t]
        if not ordered:
            problems += [
                Problem(
                    gate=Gate.COLUMN_ORDER,
                    source=m.source.key,
                    key=kind,
                    message=f"group {group!r} has no flat-file or WebSocket source that keeps "
                    "all its fields to give its column order",
                )
                for m in grouped
            ]
            continue
        concepts: dict[str, str] = {}
        for member in [*ordered, *(m for m in grouped if all(m is not o for o in ordered))]:
            own = uses[member.source.key].concepts
            for column, item in member.columns():
                concepts.setdefault(column, own[item.column])
        leading = naming.lead_order(list(concepts), concepts.__getitem__)
        rank = {column: index for index, column in enumerate(leading)}
        for member in whole:
            wire_keys = tuple(
                f.wire_key for column, f in sorted(member.columns(), key=lambda c: rank[c[0]])
            )
            previous = orders.setdefault(member.source.key, wire_keys)
            if previous != wire_keys:
                problems.append(
                    Problem(
                        gate=Gate.COLUMN_ORDER,
                        source=member.source.key,
                        key=kind,
                        message=f"group {group!r} orders its fields {list(wire_keys)}, another "
                        f"group {list(previous)}",
                    )
                )
    return orders, problems


def _keeps_every_field(member: Member) -> bool:
    """Return whether a member contributes every field of its source to its kind.

    Args:
        member: The member.

    Returns:
        True for an unprojected member, and for one whose projection only renames.
    """
    return member.projection is None or member.renames_only


def apply(
    sources: Iterable[_S], found: Sequence[Member], orders: Mapping[str, Sequence[str]]
) -> list[_S]:
    """Return the sources with their memberships, and their fields in their kind's order.

    Args:
        sources: The generated sources of one transport, in their generated order.
        found: Every member, of every transport.
        orders: Each reordered source's wire keys in order (`column_orders`).

    Returns:
        Each source, its fields reordered when `orders` names it, with one `Membership` per
        member of it, in `KINDS` order.
    """
    memberships: dict[str, list[Membership]] = {}
    for member in found:
        memberships.setdefault(member.source.key, []).append(
            Membership(
                kind=member.kind,
                group=member.group,
                position=member.position,
                projection=member.projection,
            )
        )
    out: list[_S] = []
    for source in sources:
        fields = source.fields
        if source.key in orders:
            rank = {wire_key: index for index, wire_key in enumerate(orders[source.key])}
            fields = tuple(sorted(fields, key=lambda f: rank[f.wire_key]))
        out.append(
            dataclasses.replace(
                source, fields=fields, memberships=tuple(memberships.get(source.key, ()))
            )
        )
    return out
