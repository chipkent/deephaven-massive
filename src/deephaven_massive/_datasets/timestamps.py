"""Declared timestamp units per `(dataset, transport, field)`, and the timestamp routine.

Massive sends epoch timestamps in milliseconds on some transports and nanoseconds on others, and
the unit of a field is not always the one its documentation states. Every timestamp field is
therefore *declared*: each dataset's `DatasetSpec.time_units` maps `(transport, field)` to a
`TimeUnit`, and `catalog.declared_units` collects those declarations into a `TimeUnitTable`.
The table refuses to convert a field nobody declared, because guessing a unit hides unit bugs.

**The timestamp routine** (`TimeUnitTable.instants`) turns one column of epoch integers into
epoch nanoseconds, in this order:

1. **Epoch 0 means absent.** In every `Instant` column except the dataset's time column, a `0`
   becomes null: Massive's flat files write `0` where a timestamp has no value (`trf_timestamp`
   of a trade without a TRF). The time column keeps its `0`, which then fails step 3. Only
   timestamps are affected; a `0` price or size is data.
2. **Checked multiply.** The integer is scaled to nanoseconds from its unit (`s`, `ms`, `us`,
   or `ns`); a result that does not fit a signed 64-bit integer raises.
3. **Bound.** Every result must lie in the years 1990 to 2100.

Steps 2 and 3 raise `UnitMismatchError("timestamp unit mismatch for <dataset>.<field>: ...")`,
naming the field, its declared unit, and the first offending value as Massive sent it. Together
they catch every unit confusion: milliseconds declared as nanoseconds land in January 1970, and
nanoseconds declared as milliseconds overflow. `TimeUnitTable.to_epoch_ns_array` is steps 2 and
3 alone, for converting a time column to filter on it.

**The FMV magnitude rule.** The WebSocket `FMV` `t` field is documented as nanoseconds but the
documentation sample is in milliseconds, so it is declared `TimeUnit.FMV_MAGNITUDE`: a value
`>= 10**17` is nanoseconds, anything smaller is milliseconds. Every nanosecond value after
1973-03-03 is at least 10**17 and a millisecond value reaches 10**17 only in the year 3,170,000,
so the rule cannot misclassify. The unit is picked per value before step 2, and the detected
unit is logged once at INFO.

The bound applies to epoch integers only, never to ISO-8601 text, so a pre-1990 date written as
text stays valid. The module is pure: it knows nothing about datasets beyond the names it is
given, so the catalog can import it without a cycle.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final, NamedTuple

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._errors import MassiveError, log_once

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Transport

__all__ = [
    "FMV_NS_THRESHOLD",
    "MAX_INSTANT_NS",
    "MIN_INSTANT_NS",
    "TimeField",
    "TimeUnit",
    "TimeUnitTable",
    "UnitMismatchError",
]

logger = logging.getLogger(__name__)

FMV_NS_THRESHOLD: Final = 10**17
"""FMV `t` values at or above this are nanoseconds; smaller values are milliseconds."""

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
MIN_INSTANT_NS: Final = int((datetime(1990, 1, 1, tzinfo=UTC) - _EPOCH).total_seconds()) * 10**9
"""The earliest instant a timestamp may convert to: 1990-01-01T00:00Z, in epoch nanoseconds."""
MAX_INSTANT_NS: Final = int((datetime(2101, 1, 1, tzinfo=UTC) - _EPOCH).total_seconds()) * 10**9
"""The first instant past the bound: 2101-01-01T00:00Z, so the whole of 2100 is inside."""

_INT64_MAX: Final = 2**63 - 1


class UnitMismatchError(MassiveError):
    """A timestamp that overflows 64-bit nanoseconds or converts outside 1990 to 2100.

    A `MassiveError`, so a caller that does nothing special reports it as one; a transport
    catches it by this type to add where the value came from (the flat-file transport names
    the file) and raises a plain `MassiveError` with the same text.
    """


class TimeUnit(StrEnum):
    """The declared unit of an epoch timestamp field."""

    SECONDS = "s"
    MILLISECONDS = "ms"
    MICROSECONDS = "us"
    NANOSECONDS = "ns"
    FMV_MAGNITUDE = "ms|ns"
    """Milliseconds below `FMV_NS_THRESHOLD`, nanoseconds at or above it (FMV `t` only)."""


_NS_PER: Final[Mapping[TimeUnit, int]] = {
    TimeUnit.SECONDS: 10**9,
    TimeUnit.MILLISECONDS: 10**6,
    TimeUnit.MICROSECONDS: 10**3,
    TimeUnit.NANOSECONDS: 1,
}
"""Nanoseconds per unit, for every fixed unit."""


def _log_fmv_unit(unit: TimeUnit) -> None:
    """Log, once per process, which unit the FMV magnitude rule detected.

    Args:
        unit: The unit the first classified FMV value was found to be in.
    """
    log_once(logger, "fmv-unit", "FMV timestamps detected in %s (magnitude rule)", unit.value)


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
    """The `(dataset, transport, field) -> TimeUnit` lookup, and the timestamp routine.

    A transport builds one from the declarations of the datasets it reads
    (`catalog.declared_units`); the flat-file transport builds one per `history()` call.
    Instances are immutable after construction and safe to use from any thread. Converting a
    field that has no declared unit raises `LookupError`.

    Example:
        ::

            import pyarrow as pa

            from deephaven_massive._datasets.timestamps import TimeField, TimeUnit, TimeUnitTable
            from deephaven_massive._enums import Transport

            trf = TimeField("stocks.trades", Transport.FLAT_FILES, "trf_timestamp")
            table = TimeUnitTable({trf: TimeUnit.NANOSECONDS})
            values = pa.array([1_741_012_200_000_000_000, 0])
            table.instants(trf, values, time_column=False)  # [1741012200000000000, null]
    """

    __slots__ = ("_units",)

    def __init__(self, units: Mapping[TimeField, TimeUnit]) -> None:
        """Freeze a copy of the declared units.

        Args:
            units: The unit of each declared field.
        """
        self._units: Mapping[TimeField, TimeUnit] = dict(units)

    def __contains__(self, key: object) -> bool:
        """Tell whether a field has a declared unit.

        Args:
            key: A `TimeField`.

        Returns:
            True if the table declares a unit for `key`.
        """
        return key in self._units

    def unit(self, key: TimeField) -> TimeUnit:
        """Return the declared unit of one field.

        Args:
            key: The field to look up.

        Returns:
            The field's unit.

        Raises:
            LookupError: If no unit is declared for the field. This is a library defect:
                every timestamp field must be declared in its dataset's `DatasetSpec`.
        """
        try:
            return self._units[key]
        except KeyError:
            raise LookupError(
                f"no time unit registered for {key.label} on {key.transport.value}"
            ) from None

    def to_epoch_ns(self, key: TimeField, value: int) -> int:
        """Convert one epoch value of a declared field to nanoseconds.

        The scalar form of `to_epoch_ns_array`, which it calls, so both forms share one checked
        conversion and one bound.

        Args:
            key: The declared field.
            value: The epoch integer as Massive sent it.

        Returns:
            Nanoseconds since the Unix epoch.

        Raises:
            UnitMismatchError: If `value` does not fit a 64-bit integer. A value that overflows
                64-bit nanoseconds or lies outside 1990 to 2100 raises the `UnitMismatchError` of
                `to_epoch_ns_array`, and an undeclared field its `LookupError`.
        """
        try:
            values = pa.array([value], pa.int64())
        except (OverflowError, pa.ArrowInvalid):
            raise UnitMismatchError(
                f"timestamp unit mismatch for {key.label}: {value} does not fit 64 bits"
            ) from None
        nanos: int = self.to_epoch_ns_array(key, values)[0].as_py()
        return nanos

    def to_epoch_ns_array(self, key: TimeField, values: pa.Array) -> pa.Array:
        """Convert a column of a declared field to epoch nanoseconds, checked and bounded.

        Steps 2 and 3 of the timestamp routine: the checked multiply and the 1990 to 2100
        bound, vectorized in Arrow, so no value becomes a Python object. Nulls stay null. The
        flat-file reader uses it on the time column to filter rows by the requested range, so
        a wrongly declared unit fails there already.

        Args:
            key: The declared field.
            values: An integer Arrow array of epoch values as Massive sent them.

        Returns:
            An `int64` Arrow array of nanoseconds since the Unix epoch.

        An undeclared field raises the `LookupError` of `unit`. A value that overflows 64-bit
        nanoseconds, or a converted value outside 1990 to 2100, raises
        `UnitMismatchError("timestamp unit mismatch for <dataset>.<field>: ...")`, naming the unit
        and the first offending value as Massive sent it.
        """
        unit = self.unit(key)
        sent = pc.cast(values, pa.int64())
        if unit is TimeUnit.FMV_MAGNITUDE:
            is_ns = pc.call_function("greater_equal", [sent, FMV_NS_THRESHOLD])
            any_ns = pc.call_function("any", [is_ns]).as_py()
            if any_ns is not None:  # None: every value was null, so nothing was classified
                _log_fmv_unit(TimeUnit.NANOSECONDS if any_ns else TimeUnit.MILLISECONDS)
            # Only the millisecond values are scaled; the nanosecond ones are masked to null.
            millis = pc.call_function("if_else", [is_ns, None, sent])
            scaled = _scale(key, TimeUnit.MILLISECONDS, millis)
            nanos = pc.call_function("if_else", [is_ns, sent, scaled])
        else:
            nanos = _scale(key, unit, sent)
        _bound(key, unit, sent=sent, nanos=nanos)
        return nanos

    def instants(self, key: TimeField, values: pa.Array, *, time_column: bool) -> pa.Array:
        """Run the timestamp routine on one column: epoch 0 to null, checked multiply, bound.

        Every epoch-integer `Instant` column a transport delivers goes through here.

        Args:
            key: The declared field.
            values: An integer Arrow array of epoch values as Massive sent them.
            time_column: True for the dataset's time column (`Timestamp`), whose `0` is kept
                and so fails the bound; False for every other `Instant` column, whose `0`
                becomes null.

        Returns:
            An `int64` Arrow array of nanoseconds since the Unix epoch.

        Raises the `LookupError` and `UnitMismatchError` of `to_epoch_ns_array`.
        """
        if not time_column:
            values = pc.cast(values, pa.int64())
            values = pc.if_else(pc.equal(values, 0), pa.scalar(None, pa.int64()), values)
        return self.to_epoch_ns_array(key, values)


def _scale(key: TimeField, unit: TimeUnit, values: pa.Array) -> pa.Array:
    """Scale epoch integers of a fixed unit to nanoseconds, refusing to overflow.

    Args:
        key: The field, for the error message.
        unit: The values' unit; not `FMV_MAGNITUDE`.
        values: An `int64` array; nulls stay null.

    Returns:
        The `int64` nanoseconds.

    Raises:
        UnitMismatchError: If any value overflows 64-bit nanoseconds; the message names the first.
    """
    factor = _NS_PER[unit]
    if factor == 1:
        return values
    try:
        return pc.call_function("multiply_checked", [values, factor])
    except pa.ArrowInvalid:
        limit = _INT64_MAX // factor
        too_big = pc.or_(pc.greater(values, limit), pc.less(values, -limit))
        first = pc.filter(values, too_big)[0].as_py()
        raise UnitMismatchError(
            f"timestamp unit mismatch for {key.label}: {first} {unit.value} overflows "
            "64-bit nanoseconds"
        ) from None


def _bound(key: TimeField, unit: TimeUnit, *, sent: pa.Array, nanos: pa.Array) -> None:
    """Require every converted instant to lie in 1990 to 2100.

    Args:
        key: The field, for the error message.
        unit: Its declared unit, for the error message.
        sent: The values as Massive sent them, aligned with `nanos`.
        nanos: The converted epoch nanoseconds; nulls are skipped.

    Raises:
        UnitMismatchError: If any instant lies outside the bound; the message names the first
            such value as Massive sent it.
    """
    bounds = pc.call_function("min_max", [nanos]).as_py()
    low, high = bounds["min"], bounds["max"]
    if low is None or (low >= MIN_INSTANT_NS and high < MAX_INSTANT_NS):
        return
    outside = pc.or_(pc.less(nanos, MIN_INSTANT_NS), pc.greater_equal(nanos, MAX_INSTANT_NS))
    first = pc.filter(sent, outside)[0].as_py()
    raise UnitMismatchError(
        f"timestamp unit mismatch for {key.label}: {first} {unit.value} is outside 1990 to 2100"
    )
