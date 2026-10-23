"""Declared timestamp units per `(dataset, transport, field)` and their conversion to nanoseconds.

Massive sends epoch timestamps in milliseconds on some transports and nanoseconds on others,
and the unit of a field is not always the one its documentation states. The previous code base
guessed units from magnitudes, which hid unit bugs. Here every timestamp field is *declared*:
each dataset's `DatasetSpec.time_units` maps `(transport, field)` to a `TimeUnit`, and
`_catalog.time_units()` composes those declarations into one `TimeUnitTable`. The table refuses
to convert a field nobody registered (DESIGN § 12).

Two rules guard the declarations that could not be verified from the documentation:

- **The FMV magnitude rule.** The WebSocket `FMV` `t` field is documented as nanoseconds but
  the documentation sample is in milliseconds, so it is declared `TimeUnit.FMV_MAGNITUDE`: a
  value `>= 10**17` is nanoseconds, anything smaller is milliseconds. Every nanosecond value
  after 1973-03-03 is at least 10**17 and a millisecond value reaches 10**17 only in the year
  3,170,000, so the rule cannot misclassify. The detected unit is logged once at INFO.
- **The defensive range check.** A rule marked `unverified` (for example the REST forex
  `/v1/last_quote` and crypto `/v1/last` `timestamp`) must convert to an instant within the
  years 1990 to 2100; otherwise conversion raises
  `MassiveError("timestamp unit mismatch for <dataset>.<field>")`. A wrong unit fails loudly.

The module is pure: it knows nothing about datasets beyond the names it is given, so the
catalog can import it without a cycle. `Transport` lives here because it is one axis of the
lookup; the schema and diagnostics modules use it too.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final, NamedTuple

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._errors import MassiveError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "FMV_NS_THRESHOLD",
    "TimeField",
    "TimeUnit",
    "TimeUnitTable",
    "Transport",
    "UnitRule",
    "is_timestamp_like",
]

logger = logging.getLogger(__name__)

FMV_NS_THRESHOLD: Final = 10**17
"""FMV `t` values at or above this are nanoseconds; smaller values are milliseconds."""

_NS_PER_MS: Final = 1_000_000
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MIN_CHECKED_NS: Final = int((datetime(1990, 1, 1, tzinfo=UTC) - _EPOCH).total_seconds()) * 10**9
_MAX_CHECKED_NS: Final = int((datetime(2101, 1, 1, tzinfo=UTC) - _EPOCH).total_seconds()) * 10**9

# Field names that hold a timestamp: `t`, `timestamp`, `*_timestamp`, `updated`, `*_updated`,
# `window_start`, `window_end`, and the single-letter bar bounds `s` and `e` are not included
# because they are ambiguous across models; datasets register those explicitly.
_TIMESTAMP_LIKE: Final = re.compile(r"^(t|timestamp|.*_timestamp|updated|.*_updated|window_.*)$")


class Transport(StrEnum):
    """The transport a row arrived on.

    The values are the spellings of the diagnostics tables' `Transport` column.
    """

    WS = "WS"
    REST = "REST"
    FLAT_FILES = "FLAT_FILES"


class TimeUnit(StrEnum):
    """The declared unit of an epoch timestamp field."""

    NANOSECONDS = "ns"
    MILLISECONDS = "ms"
    FMV_MAGNITUDE = "ms|ns"
    """Milliseconds below `FMV_NS_THRESHOLD`, nanoseconds at or above it (FMV `t` only)."""


@dataclass(frozen=True, slots=True)
class UnitRule:
    """The declared unit of one field, and whether the declaration is verified.

    Attributes:
        unit: The epoch unit the field's integers are in.
        unverified: True when the unit is a conservative guess (DESIGN § 12 marks it
            UNVERIFIED); conversions then check that the instant lies within 1990 to 2100.
    """

    unit: TimeUnit
    unverified: bool = False


_fmv_log_lock = threading.Lock()
_fmv_logged = threading.Event()


def _log_fmv_unit_once(unit: TimeUnit) -> None:
    """Log, once per process, which unit the FMV magnitude rule detected.

    Args:
        unit: The unit the first classified FMV value was found to be in.
    """
    with _fmv_log_lock:
        if _fmv_logged.is_set():
            return
        _fmv_logged.set()
    logger.info("FMV timestamps detected in %s (magnitude rule, DESIGN § 12)", unit.value)


def is_timestamp_like(field: str) -> bool:
    """Tell whether a field name looks like an epoch timestamp and so needs a declared unit.

    The catalog test uses this predicate to assert that every timestamp-like field of every
    registered SDK model and flat-file header has an entry in the unit table.

    Args:
        field: An SDK model field name, a WebSocket key, or a flat-file header, in snake case.

    Returns:
        True for `t`, `timestamp`, `*_timestamp`, `updated`, `*_updated`, and `window_*`.
    """
    return _TIMESTAMP_LIKE.match(field) is not None


class TimeField(NamedTuple):
    """The identity of one timestamp field: which dataset, which transport, which key."""

    dataset: str
    """The dataset name, such as `"stocks.trades"`."""
    transport: Transport
    """The transport the value arrives on."""
    field: str
    """The wire key, SDK field name, or flat-file header, such as `"t"` or `"sip_timestamp"`."""

    @property
    def label(self) -> str:
        """The `<dataset>.<field>` label used in error messages, such as `stocks.trades.t`."""
        return f"{self.dataset}.{self.field}"


class TimeUnitTable:
    """The `(dataset, transport, field) -> UnitRule` lookup, with conversions to nanoseconds.

    One instance is built by `_catalog.time_units()` from every `DatasetSpec`'s declarations
    and shared, read-only, by every transport. Instances are immutable after construction and
    safe to use from any thread. Converting a field that has no declared unit raises
    `LookupError`, because guessing a unit is exactly the bug this table exists to prevent.

    Example:
        ::

            trades_t = TimeField("stocks.trades", Transport.WS, "t")
            table = TimeUnitTable({trades_t: UnitRule(TimeUnit.MILLISECONDS)})
            table.to_epoch_ns(trades_t, 1_700_000_000_000)  # 1_700_000_000_000_000_000
    """

    __slots__ = ("_rules",)

    def __init__(self, rules: Mapping[TimeField, UnitRule]) -> None:
        """Freeze a copy of the declared rules.

        Args:
            rules: The unit of each declared field.
        """
        self._rules: Mapping[TimeField, UnitRule] = dict(rules)

    def __len__(self) -> int:
        """Return the number of declared fields.

        Returns:
            How many fields the table holds a unit for.
        """
        return len(self._rules)

    def __contains__(self, key: object) -> bool:
        """Tell whether a field has a declared unit.

        Args:
            key: A `TimeField`.

        Returns:
            True if the table holds a rule for `key`.
        """
        return key in self._rules

    def rule(self, key: TimeField) -> UnitRule:
        """Return the declared unit of one field.

        Args:
            key: The field to look up.

        Returns:
            The field's `UnitRule`.

        Raises:
            LookupError: If no unit is registered for the field. This is a library defect:
                every timestamp field must be declared in its dataset's `DatasetSpec`.
        """
        try:
            return self._rules[key]
        except KeyError:
            raise LookupError(
                f"no time unit registered for {key.label} on {key.transport.value}"
            ) from None

    def to_epoch_ns(self, key: TimeField, value: int) -> int:
        """Convert one epoch value of a registered field to nanoseconds.

        An unregistered field raises `LookupError` (see `rule`).

        Args:
            key: The registered field.
            value: The epoch integer as Massive sent it.

        Returns:
            Nanoseconds since the Unix epoch.

        Raises:
            MassiveError: If the rule is unverified and the result lies outside 1990 to 2100.
        """
        rule = self.rule(key)
        nanos = value
        if rule.unit is TimeUnit.MILLISECONDS:
            nanos = value * _NS_PER_MS
        elif rule.unit is TimeUnit.FMV_MAGNITUDE:
            is_ns = value >= FMV_NS_THRESHOLD
            _log_fmv_unit_once(TimeUnit.NANOSECONDS if is_ns else TimeUnit.MILLISECONDS)
            nanos = value if is_ns else value * _NS_PER_MS
        if rule.unverified and not _MIN_CHECKED_NS <= nanos < _MAX_CHECKED_NS:
            raise MassiveError(f"timestamp unit mismatch for {key.label}")
        return nanos

    def to_epoch_ns_array(self, key: TimeField, values: pa.Array) -> pa.Array:
        """Convert a column of epoch values of a registered field to nanoseconds, in Arrow.

        The conversion is vectorized: no value becomes a Python object, so a flat-file chunk or
        a WebSocket batch converts in one pass. Nulls stay null. An unregistered field raises
        `LookupError` (see `rule`).

        Args:
            key: The registered field.
            values: An integer Arrow array of epoch values as Massive sent them.

        Returns:
            An `int64` Arrow array of nanoseconds since the Unix epoch.

        Raises:
            MassiveError: If a converted value overflows 64-bit nanoseconds, or the rule is
                unverified and any non-null result lies outside 1990 to 2100.
        """
        rule = self.rule(key)
        nanos = pc.cast(values, pa.int64())
        if rule.unit is TimeUnit.MILLISECONDS:
            try:
                nanos = pc.multiply_checked(nanos, _NS_PER_MS)
            except pa.ArrowInvalid:
                raise MassiveError(f"timestamp unit mismatch for {key.label}") from None
        elif rule.unit is TimeUnit.FMV_MAGNITUDE:
            is_ns = pc.greater_equal(nanos, FMV_NS_THRESHOLD)
            any_ns = pc.any(is_ns).as_py()
            _log_fmv_unit_once(TimeUnit.NANOSECONDS if any_ns else TimeUnit.MILLISECONDS)
            # Unchecked: the products of nanosecond values may wrap, but if_else discards them.
            nanos = pc.if_else(is_ns, nanos, pc.multiply(nanos, _NS_PER_MS))
        if rule.unverified:
            bounds = pc.min_max(nanos).as_py()
            low, high = bounds["min"], bounds["max"]
            if low is not None and (low < _MIN_CHECKED_NS or high >= _MAX_CHECKED_NS):
                raise MassiveError(f"timestamp unit mismatch for {key.label}")
        return nanos
