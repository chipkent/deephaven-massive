"""The naming policy: how a field's long names become its column, and which names need care.

A field's column is its wire key split into segments, each segment translated to Massive's
long form (the generator decides each long name from the lexicons, identity keys, and `NAMES`
entries), and the long names joined in PascalCase by `column_name`: `["prevDay", "vwap"]`
becomes `PrevDayVwap`. Nothing else renames a field: a vendor `symbol` is generated as
`Symbol`, and the library's dataset key rename and time columns, not the generator, make the
key columns (`KEY_COLUMNS`, `is_key_column`).

The module also holds the two name tests the gates apply: whether a key is short
(`is_short`, the `short-key` gate) and whether a long name is timestamp-like
(`is_timestamp_like`, the `timestamp-unit` gate). Every transport's generator shares them, so
one field gets one column whichever transport serves it.

Last, it holds the order rule that runs after the vendor's order (`lead_order`): within each
nested object, the price, size, and bar fields lead, so every table opens on its prices
whichever asset class or transport it comes from.

It imports only the standard library.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

__all__ = [
    "KEY_COLUMNS",
    "LEAD_FIELDS",
    "column_name",
    "is_key_column",
    "is_short",
    "is_timestamp_like",
    "lead_order",
    "snake",
]

_T = TypeVar("_T")

KEY_COLUMNS: Final = frozenset(
    {
        "Symbol",
        "Underlying",
        "Date",
        "Timestamp",
        "TimestampStart",
        "TimestampEnd",
        "TimestampInsert",
    }
)
"""The columns the library's dataset key rename and time columns produce: `Symbol`,
`Underlying`, a row's trading `Date`, `Timestamp`, a bar's window, and the arrival time. A
`NAMES` entry never coins one, nor any other `Timestamp<Qualifier>` (`is_key_column`)."""

_LIBRARY_TIMESTAMP: Final = re.compile(r"Timestamp(?:[A-Z][A-Za-z0-9]*)?")
"""`Timestamp`, alone or followed by a qualifier: the names of the library's time columns."""

LEAD_FIELDS: Final = (
    ("price",),
    ("size",),
    ("bid_price", "bid"),
    ("bid_size",),
    ("ask_price", "ask"),
    ("ask_size",),
    ("open",),
    ("high",),
    ("low",),
    ("close",),
    ("volume",),
    ("vwap",),
    ("value",),
)
"""The fields that lead their group, in order, each a field's own long name in snake case.
Names in one tuple share a place: Massive spells a bid price `bid_price` on most sources and
`bid` on the forex and crypto last quotes."""

_LEAD_RANK: Final = {name: rank for rank, names in enumerate(LEAD_FIELDS) for name in names}
# The prefixes that pair a bid field with its ask field (`bid_exchange`, `ask_exchange`).
_BID: Final = "bid_"
_ASK: Final = "ask_"

# The boundaries between the words of a camel-case or PascalCase name.
_WORD_BREAK: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
# The longest key the `short-key` gate treats as short.
_SHORT_KEY_LENGTH: Final = 3
# A long name, in snake case, that makes a field or a string parameter timestamp-like.
_TIMESTAMP_LIKE: Final = re.compile(
    r"timestamp|.+_timestamp|updated|.+_updated|window_start|window_end|.+_time"
)


def is_key_column(column: str) -> bool:
    """Return whether a column name belongs to the library, so no `NAMES` entry may coin it.

    Args:
        column: A PascalCase column name.

    Returns:
        True for a name of `KEY_COLUMNS` and for `Timestamp` followed by any qualifier
        (`TimestampStart`, `TimestampFoo`): the library's time columns take that form, so a
        vendor field coined into it would be mistaken for one.
    """
    return column in KEY_COLUMNS or bool(_LIBRARY_TIMESTAMP.fullmatch(column))


def column_name(long_names: Iterable[str]) -> str:
    """Join the long names of a wire key's segments into a PascalCase column name.

    Each long name is split into words at underscores and at camel-case boundaries, and each
    word is written with its first character upper-case and the rest lower-case, so
    `["prevDay", "vwap"]` gives `"PrevDayVwap"`, `["sip_mapping", "CTA"]` gives
    `"SipMappingCta"`, and `["aff_10b5_one"]` gives `"Aff10b5One"`.

    Args:
        long_names: The long name of each segment, outermost first.

    Returns:
        The column name.
    """
    words = [word for name in long_names for word in _WORD_BREAK.sub("_", name).split("_") if word]
    return "".join(word[0].upper() + word[1:].lower() for word in words)


def snake(name: str) -> str:
    """Return a long name in snake case, so the name rules match camel-case names too.

    A field's concept, for the `concept-collision` gate, is the snake case of each of its long
    names, joined with dots.

    Args:
        name: A long name, such as `"serverTime"` or `"last_updated"`.

    Returns:
        The name with camel-case boundaries turned into underscores, lower-cased
        (`"server_time"`).
    """
    return _WORD_BREAK.sub("_", name).lower()


def is_short(key: str) -> bool:
    """Return whether a wire key segment is short, so that it needs a name from hand input.

    Massive's short keys (`p`, `av`, `dav`) say nothing a reader can trust, so the `short-key`
    gate stops on one that no lexicon, identity, or `NAMES` entry translates.

    Args:
        key: One segment of a wire key, as the vendor spells it.

    Returns:
        True for three characters or fewer and no underscore.
    """
    return len(key) <= _SHORT_KEY_LENGTH and "_" not in key


def is_timestamp_like(long_name: str) -> bool:
    """Return whether a long name marks a timestamp, so its unit must never be left to a default.

    Args:
        long_name: The long name of a field's last segment, or a parameter's base name (its
            wire name without a filter suffix), in any case style.

    Returns:
        True when its snake case is `timestamp`, `*_timestamp`, `updated`, `*_updated`,
        `window_start`, `window_end`, or `*_time` (so `serverTime` counts).
    """
    return bool(_TIMESTAMP_LIKE.fullmatch(snake(long_name)))


def lead_order(fields: Sequence[_T], concept: Callable[[_T], str]) -> list[_T]:
    """Return fields in lead order: each nested object's price, size, and bar fields first.

    The refresh calls it last on every column order it writes, so one rule orders every
    dataset of every asset class: a table opens on its prices, and the feeds of one kind put
    them in the same place. The rule:

    1. A key column (`is_key_column`, such as a vendor `symbol` or `timestamp`) keeps its
       place; the library moves the key columns first at run time.
    2. A field's group is its nested-object parent: everything in its concept before the last
       dot (`day` for `day.open`, the top level for `price`). Each group's fields refill the
       places the group held, so no field moves into another group's place.
    3. Within a group, the fields whose own name is one of `LEAD_FIELDS` come first, in that
       order. The match is exact: `day.decimal_volume` and `previous_close` do not lead.
    4. Every other `bid_<x>` moves to just before its `ask_<x>` in the same group. Nothing
       else moves, so the rest keep their order.

    Args:
        fields: The fields in their vendor order, of any type.
        concept: Returns a field's concept: its full long-name path, each segment in snake
            case and joined with dots (`last_quote.bid_exchange`), as `gates.PathUse.concepts`
            records it.

    Returns:
        The same fields, reordered. For example, concepts `ticker, ask_exchange, ask_price,
        bid_exchange, bid_price` come back as `bid_price, ask_price, ticker, bid_exchange,
        ask_exchange`.
    """
    groups: dict[str, list[int]] = {}
    for index, item in enumerate(fields):
        path = concept(item)
        if "." not in path and is_key_column(column_name([path])):
            continue
        groups.setdefault(path.rpartition(".")[0], []).append(index)
    ordered = list(fields)
    for places in groups.values():
        names = [concept(fields[i]).rpartition(".")[2] for i in places]
        present = set(names)
        # By place in the order, then by vendor order among fields sharing a place.
        ranked = sorted(
            (_LEAD_RANK[name], index)
            for index, name in zip(places, names, strict=True)
            if name in _LEAD_RANK
        )
        leads = [index for _, index in ranked]
        # Each paired bid field, under the ask field it moves before.
        bids: dict[str, list[int]] = {}
        for index, name in zip(places, names, strict=True):
            partner = _ASK + name[len(_BID) :]
            if name not in _LEAD_RANK and name.startswith(_BID) and partner in present:
                bids.setdefault(partner, []).append(index)
        paired = {i for moved in bids.values() for i in moved}
        rest: list[int] = []
        for index, name in zip(places, names, strict=True):
            if name not in _LEAD_RANK and index not in paired:
                rest += bids.pop(name, [])
                rest.append(index)
        for place, index in zip(places, [*leads, *rest], strict=True):
            ordered[place] = fields[index]
    return ordered
