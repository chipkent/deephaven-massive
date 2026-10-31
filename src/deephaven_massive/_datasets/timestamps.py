"""The one timestamp routine: epoch integers to checked nanoseconds, on every transport.

Massive sends epoch timestamps in seconds, milliseconds, microseconds, or nanoseconds. The unit
of a field is a fact about its wire format, verified by hand and written onto each generated
field (`Field.unit` in the wire package's generated schemas); `time_unit` reads it for one
field of one source, applying the FMV magnitude rule below, and the routine converts values by
it. No unit is declared, guessed, or inferred here.

**The timestamp routine** (`masked_instants`) turns one column of epoch integers into epoch
nanoseconds, in this order:

1. **Epoch 0 means absent.** In every `Instant` column except the dataset's time column, a `0`
   becomes null: Massive's flat files write `0` where a timestamp has no value (`trf_timestamp`
   of a trade without a TRF). The time column keeps its `0`, which then fails step 3. Only
   timestamps are affected; a `0` price or size is data.
2. **Checked multiply.** The integer is scaled to nanoseconds from its unit (`s`, `ms`, `us`,
   or `ns`); a result that does not fit a signed 64-bit integer fails.
3. **Bound.** Every result must lie in the years 1990 to 2100, or it fails.

A failing value becomes null, and the routine returns a mask of the rows that failed, so each
transport decides what a failure costs: a row whose time column fails is dropped, since it
cannot be placed, and any other failing value is null and reported as schema drift. Together
steps 2 and 3 catch every unit confusion: milliseconds read as nanoseconds land in January
1970, and nanoseconds read as milliseconds overflow. `first_failure` picks the first failing
value as Massive sent it, and `mismatch_message` names it, the field, and its unit.

**The FMV magnitude rule.** The WebSocket `FMV` channel's `t` field is documented as
nanoseconds but the documentation sample is in milliseconds, so `time_unit` gives it
`TimeUnit.FMV_MAGNITUDE`: a value `>= 10**17` is nanoseconds, anything smaller is milliseconds.
Every nanosecond value after 1973-03-03 is at least 10**17 and a millisecond value reaches
10**17 only in the year 3,170,000, so the rule cannot misclassify. The unit is picked per value
before step 2, and the detected unit is logged once at INFO.

The bound applies to epoch integers only, never to ISO-8601 text, so a pre-1990 date written as
text stays valid. The module is pure: it makes no Deephaven or network call, and its only state
is the once-only log line.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._log_limits import log_once
from deephaven_massive._massive import Transport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._massive import Field, Source

__all__ = [
    "FMV_NS_THRESHOLD",
    "MAX_INSTANT_NS",
    "MIN_INSTANT_NS",
    "TimeUnit",
    "first_failure",
    "masked_instants",
    "mismatch_message",
    "time_unit",
]

logger = logging.getLogger(__name__)

FMV_NS_THRESHOLD: Final = 10**17
"""FMV `t` values at or above this are nanoseconds; smaller values are milliseconds."""

MIN_INSTANT_NS: Final = epoch_ns(datetime(1990, 1, 1, tzinfo=UTC))
"""The earliest instant a timestamp may convert to: 1990-01-01T00:00Z, in epoch nanoseconds."""
MAX_INSTANT_NS: Final = epoch_ns(datetime(2101, 1, 1, tzinfo=UTC))
"""The first instant past the bound: 2101-01-01T00:00Z, so the whole of 2100 is inside."""

_INT64_MAX: Final = 2**63 - 1

_FMV_CHANNEL: Final = "FMV"
"""The WebSocket channel, on every cluster, whose time field takes the FMV magnitude rule."""


class TimeUnit(StrEnum):
    """The unit an epoch timestamp field counts in.

    The four fixed units share the values of the wire package's `WireUnit`, so
    `TimeUnit(field.unit.value)` converts one; `time_unit` does that for a field, applying the
    FMV rule.
    """

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


def time_unit(source: Source, field: Field) -> TimeUnit:
    """Return the unit one epoch field of one source counts in.

    The field's generated unit, except on the WebSocket `FMV` channel of every cluster, whose
    time field takes the FMV magnitude rule (see the module docstring).

    Args:
        source: The generated source the field belongs to.
        field: An epoch field of `source`, whose `unit` is set.

    Returns:
        The unit to convert the field's values by.

    Raises:
        ValueError: If the field is not an epoch (it has no unit); the message names the source
            and the field.
    """
    if field.unit is None:
        raise ValueError(f"{source.key} field {field.wire_key!r} is not an epoch timestamp")
    if source.transport is Transport.WS and source.key.rpartition("/")[2] == _FMV_CHANNEL:
        return TimeUnit.FMV_MAGNITUDE
    return TimeUnit(field.unit.value)


def first_failure(values: pa.Array, failed: pa.Array) -> int | None:
    """Return the first value of a column that failed the timestamp routine, as Massive sent it.

    Args:
        values: The integer Arrow array of epoch values given to `masked_instants`.
        failed: The mask `masked_instants` returned with it.

    Returns:
        The first failing value, for `mismatch_message` or a drift sample; None when no value
        failed.
    """
    if not pc.call_function("any", [failed]).as_py():
        return None
    first: int = pc.call_function("filter", [values, failed])[0].as_py()
    return first


def masked_instants(
    unit: TimeUnit, values: pa.Array, *, time_column: bool
) -> tuple[pa.Array, pa.Array]:
    """Run the timestamp routine on one column: failing values become null and are masked.

    Every epoch-integer `Instant` column a transport delivers goes through here: epoch 0 to
    null outside the time column, the scale to nanoseconds, and the 1990 to 2100 bound,
    vectorized in Arrow, so no value becomes a Python object. A value that overflows or lies
    outside the bound is null and marked in the returned mask, so the caller can drop exactly
    the rows whose time column failed and report every other failure as drift. A null stays
    null and is not a failure.

    Args:
        unit: The values' unit (`time_unit`).
        values: An integer Arrow array of epoch values as Massive sent them.
        time_column: True for the dataset's time column, whose `0` is kept and so fails the
            bound; False for every other `Instant` column, whose `0` becomes null.

    Returns:
        `(nanos, failed)`: an `int64` array of nanoseconds since the Unix epoch, null where
        the value was null or failed; and a boolean array, true exactly where a value failed.
    """
    sent = pc.cast(values, pa.int64())
    if not time_column:
        sent = pc.call_function(
            "if_else", [pc.call_function("equal", [sent, 0]), pa.scalar(None, pa.int64()), sent]
        )
    if unit is TimeUnit.FMV_MAGNITUDE:
        is_ns = pc.call_function("greater_equal", [sent, FMV_NS_THRESHOLD])
        any_ns = pc.call_function("any", [is_ns]).as_py()
        if any_ns is not None:  # None: every value was null, so nothing was classified
            _log_fmv_unit(TimeUnit.NANOSECONDS if any_ns else TimeUnit.MILLISECONDS)
        millis = _NS_PER[TimeUnit.MILLISECONDS]
        factor = pc.call_function("if_else", [is_ns, 1, millis])
        limit = pc.call_function("if_else", [is_ns, _INT64_MAX, _INT64_MAX // millis])
    else:
        factor = pa.scalar(_NS_PER[unit], pa.int64())
        limit = pa.scalar(_INT64_MAX // _NS_PER[unit], pa.int64())
    fits = pc.call_function(
        "and",
        [
            pc.call_function("less_equal", [sent, limit]),
            pc.call_function("greater_equal", [sent, pc.call_function("negate", [limit])]),
        ],
    )
    nanos = pc.call_function(
        "multiply", [pc.call_function("if_else", [fits, sent, pa.scalar(None, pa.int64())]), factor]
    )
    inside = pc.call_function(
        "and",
        [
            pc.call_function("greater_equal", [nanos, MIN_INSTANT_NS]),
            pc.call_function("less", [nanos, MAX_INSTANT_NS]),
        ],
    )
    failed = pc.call_function(
        "and",
        [
            pc.call_function("is_valid", [sent]),
            pc.call_function("invert", [pc.fill_null(inside, fill_value=False)]),
        ],
    )
    return pc.call_function("if_else", [failed, pa.scalar(None, pa.int64()), nanos]), failed


def mismatch_message(label: str, unit: TimeUnit, value: int) -> str:
    """Return the message the timestamp routine gives one failing epoch value.

    For a value `masked_instants` reported as failed, so a failure reads the same whether the
    routine raised or masked it.

    Args:
        label: The field, as `<dataset>.<column>`.
        unit: The value's unit.
        value: The epoch integer as Massive sent it, which failed the routine.

    Returns:
        `timestamp unit mismatch for <label>: <value> <unit> overflows 64-bit nanoseconds`, or
        `... is outside 1990 to 2100`.
    """
    if _overflows(unit, value):  # named in the unit it was scaled from
        return (
            f"timestamp unit mismatch for {label}: {value} {_fixed(unit, value).value} "
            "overflows 64-bit nanoseconds"
        )
    return f"timestamp unit mismatch for {label}: {value} {unit.value} is outside 1990 to 2100"


def _fixed(unit: TimeUnit, value: int) -> TimeUnit:
    """Return the fixed unit one value counts in: the FMV magnitude rule picks one per value.

    Args:
        unit: The field's unit.
        value: The epoch integer.

    Returns:
        `unit` itself, or for `FMV_MAGNITUDE` nanoseconds at or above `FMV_NS_THRESHOLD` and
        milliseconds below it.
    """
    if unit is not TimeUnit.FMV_MAGNITUDE:
        return unit
    return TimeUnit.NANOSECONDS if value >= FMV_NS_THRESHOLD else TimeUnit.MILLISECONDS


def _overflows(unit: TimeUnit, value: int) -> bool:
    """Tell whether one epoch value overflows 64-bit nanoseconds in its unit.

    Args:
        unit: The value's unit; for `FMV_MAGNITUDE`, the magnitude picks nanoseconds or
            milliseconds as `masked_instants` does.
        value: The epoch integer.

    Returns:
        True when its nanoseconds do not fit a signed 64-bit integer.
    """
    return abs(value * _NS_PER[_fixed(unit, value)]) > _INT64_MAX
