"""The naming policy: how a field's long names become its column, and which names need care.

A field's column is its wire key split into segments, each segment translated to Massive's
long form (the generator decides each long name from the lexicons, identity keys, and `NAMES`
entries), and the long names joined in PascalCase by `column_name`: `["prevDay", "vwap"]`
becomes `PrevDayVwap`. Nothing else renames a field: a vendor `symbol` is generated as
`Symbol`, and the library's dataset key rename, not the generator, makes the key columns
(`KEY_COLUMNS`).

The module also holds the two name tests the gates apply: whether a key is short
(`is_short`, the `short-key` gate) and whether a long name is timestamp-like
(`is_timestamp_like`, the `timestamp-unit` gate). Every transport's generator shares them, so
one field gets one column whichever transport serves it.

It imports only the standard library.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["KEY_COLUMNS", "column_name", "is_short", "is_timestamp_like", "snake"]

KEY_COLUMNS: Final = frozenset({"Symbol", "Timestamp", "Underlying"})
"""The dataset key columns the library's key rename produces; a `NAMES` entry never coins one."""

# The boundaries between the words of a camel-case or PascalCase name.
_WORD_BREAK: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
# The longest key the `short-key` gate treats as short.
_SHORT_KEY_LENGTH: Final = 3
# A long name, in snake case, that makes a field or a string parameter timestamp-like.
_TIMESTAMP_LIKE: Final = re.compile(
    r"timestamp|.+_timestamp|updated|.+_updated|window_start|window_end|.+_time"
)


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
