"""The hand input of the refresh, gathered into one value the generators and gates read.

Everything the refresh does not read from the vendor's own files is hand input: the lexicons
and identity keys (`lexicon`), the names, units, conversions, result keys, parameter units, and
dataset kinds (`overrides`), and the excluded vendor items and disbelieved vendor samples
(`exclusions`). Those modules hold the tables, each entry with its reason; this one only
bundles them as `HandInput`, so a test can pass small tables of its own instead of the real
ones.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deephaven_massive._massive.refresh import exclusions, lexicon, overrides

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._massive.refresh.exclusions import SampleError


__all__ = ["HandInput"]


@dataclass(frozen=True, kw_only=True)
class HandInput:
    """Every hand-written input of the generators, so tests can substitute their own.

    `HandInput()` holds the real tables of `lexicon`, `overrides`, and `exclusions`; a keyword
    replaces one table. Instances are immutable and safe to share.

    Example:
        ::

            from deephaven_massive._massive.refresh.hand_input import HandInput

            real = HandInput()
            without_exclusions = HandInput(exclusions=())

    Attributes:
        lexicons: Lexicon name to lexicon.
        shapes: `(path, object prefix)` to the lexicon name of that object's keys.
        identity: Short keys that are already Massive's long form, each with its reason.
        names: `(path, wire key)` to the long name of the last segment of a key Massive names
            nowhere, with the description it is derived from.
        units: `(path, wire key)` to the unit of an integer field, or to "not a timestamp" for
            a timestamp-like field that is not an epoch.
        tag_conflicts: `(path, wire key)` of a `units` entry that differs from the vendor's
            unit tag, with the reason the tag is wrong.
        type_fixes: `(path, wire key)` to the named conversion of a field whose documented
            samples its declared type does not fit.
        result_keys: Path to where its rows sit, when that is not `results`.
        param_units: `(path, base parameter)` to the unit of a timestamp parameter, or to
            "not an epoch".
        exclusions: The excluded vendor items.
        kinds: Kind to asset-class group to the sources of that group, the dataset kinds
            within which the `concept-collision` gate requires one concept per column.
        sample_errors: The documented example values the `example-type` gate does not
            believe, each with its reason.
    """

    lexicons: Mapping[str, lexicon.Lexicon] = field(default_factory=lambda: lexicon.LEXICONS)
    shapes: Mapping[tuple[str, str], str] = field(default_factory=lambda: lexicon.SHAPES)
    identity: Mapping[str, str] = field(default_factory=lambda: lexicon.IDENTITY)
    names: Mapping[tuple[str, str], overrides.Name] = field(default_factory=lambda: overrides.NAMES)
    units: Mapping[tuple[str, str], overrides.Unit] = field(default_factory=lambda: overrides.UNITS)
    tag_conflicts: Mapping[tuple[str, str], str] = field(
        default_factory=lambda: overrides.TAG_CONFLICTS
    )
    type_fixes: Mapping[tuple[str, str], overrides.TypeFix] = field(
        default_factory=lambda: overrides.TYPE_FIXES
    )
    result_keys: Mapping[str, overrides.ResultKey] = field(
        default_factory=lambda: overrides.RESULT_KEYS
    )
    param_units: Mapping[tuple[str, str], overrides.Unit] = field(
        default_factory=lambda: overrides.PARAM_UNITS
    )
    exclusions: Sequence[exclusions.Exclusion] = field(
        default_factory=lambda: exclusions.EXCLUSIONS
    )
    kinds: Mapping[str, Mapping[str, Sequence[str]]] = field(
        default_factory=lambda: overrides.KINDS
    )
    sample_errors: Sequence[SampleError] = field(default_factory=lambda: exclusions.SAMPLE_ERRORS)
