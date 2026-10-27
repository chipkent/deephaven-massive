"""The dataset kinds across transports: memberships, their gates, and the kind column order.

A **kind** (`overrides.KINDS`) is a set of sources, across transports and asset classes,
whose rows merge into one table schema at run time: the trades of every asset class from REST,
the WebSocket, and the flat files, say. `KINDS` groups each kind's sources by asset class, in
source order. Once every transport is generated, this module:

1. resolves `KINDS` and `PROJECTIONS` into `Member`s, one per source and group, reporting an
   entry that names a source the snapshot does not generate (`stale-entry`);
2. checks that one column joins to one `WireType` within a kind (`type-agreement`), by the
   type lattice: `long` with `double` gives `double`, `long` with `int-array` gives
   `int-array`, two `epoch`s give an `epoch` (each source keeping its unit), and any other
   pair is a conflict, printed with both fields' descriptions since it is first a possible
   naming error;
3. checks that, within a group whose sources span two or more transports, every column is on
   at least two transports or is in `TRANSPORT_ONLY` (`transport-agreement`), which catches a
   one-sided lexicon slip;
4. orders every member's columns from its kind (`column-order`): within each group, the
   flat-file sources and then the WebSocket sources, in `KINDS` order, give the order, which
   are the vendor's own (a file's columns, a message's keys); the group's other sources add
   their remaining columns after, in their own order. A member takes the group's order, so a
   REST member of a kind is reordered too, while a REST source in no kind keeps the
   specification's (alphabetical) order. A group with no flat-file or WebSocket source, or a
   source two of whose groups order its columns differently, fails the refresh;
5. writes each source's memberships (`spec.types.Membership`), which the generated files
   record, so the run-time merge never reads `refresh/`.

A projected membership contributes only its projection's fields, under the kind's names; it
is neither reordered nor an order reference, and the source keeps its full fields everywhere
else. It imports the standard library and other `_massive` modules only.
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

    from deephaven_massive._massive.refresh.hand_input import HandInput
    from deephaven_massive._massive.spec.types import Field

__all__ = [
    "Member",
    "apply",
    "column_orders",
    "join",
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
    """

    kind: str
    group: str
    position: int
    source: Source
    projection: Mapping[str, str] | None = None

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
                found.append(
                    Member(
                        kind=kind,
                        group=group,
                        position=position,
                        source=source,
                        projection=projection,
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
        The projection, its long names through the naming rule; `None` when `PROJECTIONS`
        has no entry for the source in the kind.
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
        for wire_key in sorted(set(entry.fields) - wire_keys)
    )
    projection = {k: naming.column_name([v]) for k, v in entry.fields.items() if k in wire_keys}
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


def join(first: WireType, second: WireType) -> WireType | None:
    """Join two types of one column by the type lattice.

    Args:
        first: One type.
        second: The other.

    Returns:
        The joined type: either type when both are the same (two `epoch`s keep their units);
        `double` for `long` and `double`; `int-array` for `long` and `int-array` (a scalar
        becomes a one-element list); `None` for any other pair, a conflict.
    """
    if first is second:
        return first
    pair = {first, second}
    if pair == {WireType.LONG, WireType.DOUBLE}:
        return WireType.DOUBLE
    if pair == {WireType.LONG, WireType.INT_ARRAY}:
        return WireType.INT_ARRAY
    return None


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
            merged = join(joined, item.wire_type)
            if merged is not None:
                seen[key] = (merged, [*contributors, (member, item)])
            elif key not in reported:
                reported.add(key)
                other, field = next(
                    (m, f) for m, f in contributors if join(f.wire_type, item.wire_type) is None
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
        kind and column; and a `stale-entry` problem for each `TRANSPORT_ONLY` entry no such
        column needs.
    """
    groups: dict[tuple[str, str], list[Member]] = {}
    for member in found:
        groups.setdefault((member.kind, member.group), []).append(member)
    needed: set[tuple[str, str]] = set()
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
            needed.add((kind, column))
            if (kind, column) not in hand.transport_only:
                (only,) = served
                yield Problem(
                    gate=Gate.TRANSPORT_AGREEMENT,
                    source=kind,
                    key=column,
                    message=f"group {group!r}: only {only} supplies it; check the names of "
                    "the other transports' fields, or add a TRANSPORT_ONLY entry",
                )
    for kind, column in sorted(set(hand.transport_only) - needed):
        yield Problem(
            gate=Gate.STALE_ENTRY,
            source=kind,
            key=column,
            message="TRANSPORT_ONLY entry for a column no single transport alone supplies",
        )


def column_orders(found: Sequence[Member]) -> tuple[dict[str, tuple[str, ...]], list[Problem]]:
    """Order the columns of every unprojected member from its group.

    Within each `(kind, group)`, the group's order is the columns of its flat-file sources,
    then of its WebSocket sources, then of its other sources, each in `KINDS` order and each
    column at its first appearance. Every unprojected member takes that order for its own
    columns.

    Args:
        found: The members.

    Returns:
        Each reordered source's key mapped to its columns in order, and a `column-order`
        problem for each source whose group has no flat-file or WebSocket source to give the
        order, or whose groups order its columns differently.
    """
    groups: dict[tuple[str, str], list[Member]] = {}
    for member in found:
        groups.setdefault((member.kind, member.group), []).append(member)
    orders: dict[str, tuple[str, ...]] = {}
    problems: list[Problem] = []
    for (kind, group), grouped in groups.items():
        ordered = [
            m
            for t in _ORDER_TRANSPORTS
            for m in grouped
            if m.source.transport is t and m.projection is None
        ]
        if not ordered:
            problems += [
                Problem(
                    gate=Gate.COLUMN_ORDER,
                    source=m.source.key,
                    key=kind,
                    message=f"group {group!r} has no flat-file or WebSocket source to give its "
                    "column order",
                )
                for m in grouped
                if m.projection is None
            ]
            continue
        rank: dict[str, int] = {}
        for member in [*ordered, *(m for m in grouped if all(m is not o for o in ordered))]:
            for column, _ in member.columns():
                rank.setdefault(column, len(rank))
        for member in grouped:
            if member.projection is not None:
                continue
            columns = tuple(sorted((f.column for f in member.source.fields), key=rank.__getitem__))
            previous = orders.setdefault(member.source.key, columns)
            if previous != columns:
                problems.append(
                    Problem(
                        gate=Gate.COLUMN_ORDER,
                        source=member.source.key,
                        key=kind,
                        message=f"group {group!r} orders its columns {list(columns)}, another "
                        f"group {list(previous)}",
                    )
                )
    return orders, problems


def apply(
    sources: Iterable[_S], found: Sequence[Member], orders: Mapping[str, Sequence[str]]
) -> list[_S]:
    """Return the sources with their memberships, and their fields in their kind's order.

    Args:
        sources: The generated sources of one transport, in their generated order.
        found: Every member, of every transport.
        orders: Each reordered source's columns in order (`column_orders`).

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
            rank = {column: index for index, column in enumerate(orders[source.key])}
            fields = tuple(sorted(fields, key=lambda f: rank[f.column]))
        out.append(
            dataclasses.replace(
                source, fields=fields, memberships=tuple(memberships.get(source.key, ()))
            )
        )
    return out
