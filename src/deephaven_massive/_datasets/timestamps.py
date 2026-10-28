"""The one timestamp routine: epoch integers to checked nanoseconds, on every transport.

Massive sends epoch timestamps in seconds, milliseconds, microseconds, or nanoseconds. The unit
of a field is a fact about its wire format, verified by hand and written onto each generated
field (`Field.unit` in the wire package's generated schemas); `time_unit` reads it for one
field of one source, applying the FMV magnitude rule below, and the routine converts values by
it. No unit is declared, guessed, or inferred here.

**The timestamp routine** (`instants`) turns one column of epoch integers into epoch
nanoseconds, in this order:

1. **Epoch 0 means absent.** In every `Instant` column except the dataset's time column, a `0`
   becomes null: Massive's flat files write `0` where a timestamp has no value (`trf_timestamp`
   of a trade without a TRF). The time column keeps its `0`, which then fails step 3. Only
   timestamps are affected; a `0` price or size is data.
2. **Checked multiply.** The integer is scaled to nanoseconds from its unit (`s`, `ms`, `us`,
   or `ns`); a result that does not fit a signed 64-bit integer raises.
3. **Bound.** Every result must lie in the years 1990 to 2100.

Steps 2 and 3 raise `UnitMismatchError("timestamp unit mismatch for <label>: ...")`, naming
the field, its unit, and the first offending value as Massive sent it. Together they catch
every unit confusion: milliseconds read as nanoseconds land in January 1970, and nanoseconds
read as milliseconds overflow. `to_epoch_ns_array` is steps 2 and 3 alone, for converting a
time column to filter on it.

**The masked form** (`masked_instants`) runs the same three steps without raising: it returns
the converted column with every failing value null, and a mask of the rows that failed. The
WebSocket flush uses it, because a bad timestamp there fails only the streams that would have
received its row, and the rest of the batch is still published; `mismatch_message` then gives
each failing value the exact message the raising form would have.

**The FMV magnitude rule.** The WebSocket `FMV` channel's `t` field is documented as
nanoseconds but the documentation sample is in milliseconds, so `time_unit` gives it
`TimeUnit.FMV_MAGNITUDE`: a value `>= 10**17` is nanoseconds, anything smaller is milliseconds.
Every nanosecond value after 1973-03-03 is at least 10**17 and a millisecond value reaches
10**17 only in the year 3,170,000, so the rule cannot misclassify. The unit is picked per value
before step 2, and the detected unit is logged once at INFO.

**Bars of a day or longer** (`day_starts`) are stamped at 00:00 of the day they start on, in
the class day zone, whichever source serves them. Massive stamps the same bar differently by
source (the day's start on the aggregates range, the 16:00 New York session close on the stock
grouped daily and `/prev`, the day's last millisecond in the forex and crypto day files), so
each stamp at a time of day Massive is known to use moves to that day's 00:00, and any other
raises `MassiveError`, so a bar of another day cut is never returned under a label it may not
match. Which times each class accepts is decided by `history_source.stamp_rule`.

The bound applies to epoch integers only, never to ISO-8601 text, so a pre-1990 date written as
text stays valid. The module is pure: it makes no Deephaven or network call, and its only state
is the once-only log line.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, time, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._errors import MassiveError, log_once
from deephaven_massive._massive import Transport

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from datetime import tzinfo

    from deephaven_massive._massive import Field, Source

__all__ = [
    "FMV_NS_THRESHOLD",
    "MAX_INSTANT_NS",
    "MIN_INSTANT_NS",
    "TimeUnit",
    "UnitMismatchError",
    "day_starts",
    "instants",
    "masked_instants",
    "mismatch_message",
    "time_unit",
    "to_epoch_ns",
    "to_epoch_ns_array",
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

_FMV_CHANNEL: Final = "FMV"
"""The WebSocket channel, on every cluster, whose time field takes the FMV magnitude rule."""


class UnitMismatchError(MassiveError):
    """A timestamp that overflows 64-bit nanoseconds or converts outside 1990 to 2100.

    A `MassiveError`, so a caller that does nothing special reports it as one; a transport
    catches it by this type to add where the value came from (the flat-file transport names
    the file) and raises a plain `MassiveError` with the same text.
    """


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


def to_epoch_ns(label: str, unit: TimeUnit, value: int) -> int:
    """Convert one epoch value to nanoseconds.

    The scalar form of `to_epoch_ns_array`, which it calls, so both forms share one checked
    conversion and one bound.

    Args:
        label: The field, as `<dataset>.<column>`, for the error message.
        unit: The value's unit.
        value: The epoch integer as Massive sent it.

    Returns:
        Nanoseconds since the Unix epoch.

    Raises:
        UnitMismatchError: If `value` does not fit a 64-bit integer, overflows 64-bit
            nanoseconds, or lies outside 1990 to 2100.
    """
    try:
        values = pa.array([value], pa.int64())
    except (OverflowError, pa.ArrowInvalid):
        raise UnitMismatchError(
            f"timestamp unit mismatch for {label}: {value} does not fit 64 bits"
        ) from None
    nanos: int = to_epoch_ns_array(label, unit, values)[0].as_py()
    return nanos


def to_epoch_ns_array(label: str, unit: TimeUnit, values: pa.Array) -> pa.Array:
    """Convert a column of epoch integers to epoch nanoseconds, checked and bounded.

    Steps 2 and 3 of the timestamp routine: the checked multiply and the 1990 to 2100 bound,
    vectorized in Arrow (`masked_instants`), so no value becomes a Python object. Nulls stay
    null. The flat-file reader uses it on the time column to filter rows by the requested
    range, so a wrong unit fails there already.

    Args:
        label: The field, as `<dataset>.<column>`, for the error message.
        unit: The values' unit.
        values: An integer Arrow array of epoch values as Massive sent them.

    Returns:
        An `int64` Arrow array of nanoseconds since the Unix epoch.

    A value that overflows 64-bit nanoseconds, or a converted value outside 1990 to 2100,
    raises the `UnitMismatchError` of `instants`.
    """
    return instants(label, unit, values, time_column=True)


def instants(label: str, unit: TimeUnit, values: pa.Array, *, time_column: bool) -> pa.Array:
    """Run the timestamp routine on one column: epoch 0 to null, checked multiply, bound.

    Every epoch-integer `Instant` column a transport delivers goes through here, or through
    `masked_instants`, which this calls: the two forms are one implementation, and this one
    raises for the column's first failure instead of masking it.

    Args:
        label: The field, as `<dataset>.<column>`, for the error message.
        unit: The values' unit (`time_unit`).
        values: An integer Arrow array of epoch values as Massive sent them.
        time_column: True for the dataset's time column (`Timestamp`), whose `0` is kept and
            so fails the bound; False for every other `Instant` column, whose `0` becomes null.

    Returns:
        An `int64` Arrow array of nanoseconds since the Unix epoch.

    Raises:
        UnitMismatchError: If a value overflows 64-bit nanoseconds or lies outside 1990 to
            2100: `"timestamp unit mismatch for <label>: ..."`, naming the unit and, as Massive
            sent it, the first value that overflows, or else the first value out of bounds.
    """
    nanos, failed = masked_instants(unit, values, time_column=time_column)
    if pc.call_function("any", [failed]).as_py():
        sent = pc.call_function("filter", [pc.cast(values, pa.int64()), failed]).to_pylist()
        overflow = [v for v in sent if _overflows(unit, v)]
        raise UnitMismatchError(mismatch_message(label, unit, (overflow or sent)[0]))
    return nanos


def masked_instants(
    unit: TimeUnit, values: pa.Array, *, time_column: bool
) -> tuple[pa.Array, pa.Array]:
    """Run the timestamp routine on one column without raising: failing values become null.

    The same steps as `instants` (epoch 0 to null outside the time column, the scale to
    nanoseconds, the 1990 to 2100 bound), vectorized in Arrow, but a value that overflows or
    lies outside the bound is reported in the returned mask instead of raised, so the caller
    can drop exactly those rows and fail exactly the readers of those rows. A null stays null
    and is not a failure.

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
        The `UnitMismatchError` text: `timestamp unit mismatch for <label>: <value> <unit>
        overflows 64-bit nanoseconds`, or `... is outside 1990 to 2100`.
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


_MS_NS: Final = 10**6


def day_starts(
    label: str,
    values: pa.Array,
    *,
    zone: tzinfo,
    known: Collection[time],
    source: str,
    what: str,
    remedy: str,
) -> pa.Array:
    """Stamp each bar at 00:00 of the day it falls on, refusing a stamp of an unknown form.

    Massive stamps the same bar of a day or longer differently by source: at the start of the
    day on the aggregates range, at the session close (16:00 New York time) on the stock
    grouped daily and `/prev`, at the day's last millisecond in the forex day files. Each
    stamp whose wall-clock time of day in `zone`, to the millisecond, is 00:00 or one of
    `known` moves to 00:00 of its date there, so every source labels a bar alike. Any other
    stamp raises: the bar may cover other days than the one it would be labelled with, so it
    is never relabelled.

    Args:
        label: The dataset, for the message, such as `"stocks.bars"`.
        values: The bars' `Timestamp` column, an Arrow `timestamp("ns", "UTC")` array.
        zone: The class day zone: New York for stocks, options, and indices; UTC for forex and
            crypto. Its midnights start the bars' days, daylight saving time included.
        known: The other wall-clock times of day, in `zone`, at which Massive is known to
            stamp these bars; empty when only 00:00 is accepted.
        source: Where the bars came from, for the message, such as a REST path template or a
            flat-file prefix.
        what: The kind of bar, for the message, such as `"1-day bar"`.
        remedy: What the caller can do instead, for the message, such as reading the other
            source.

    Returns:
        The stamps at 00:00 of their day in `zone`, in the type of `values`; nulls stay null.

    A stamp at any other time of day raises the `MassiveError` of `_refuse`, naming the
    dataset, the source, the stamp in UTC and in `zone`, the accepted times, and `remedy`.
    """
    name = str(zone)
    local = pc.local_timestamp(values.cast(pa.timestamp("ns", name)))
    midnight = pc.floor_temporal(local, unit="day")
    # Both are wall-clock nanoseconds of the same day, so the difference is positive and the
    # integer division floors it to the millisecond.
    offset = pc.divide(pc.subtract(local.cast(pa.int64()), midnight.cast(pa.int64())), _MS_NS)
    accepted = pa.array(sorted({0, *(_millisecond_of_day(moment) for moment in known)}), pa.int64())
    odd = pc.and_(pc.is_valid(offset), pc.invert(pc.is_in(offset, value_set=accepted)))
    _refuse(label, values, odd, zone=zone, known=known, source=source, what=what, remedy=remedy)
    return pc.assume_timezone(midnight, timezone=name).cast(values.type)


def _millisecond_of_day(moment: time) -> int:
    """Return a wall-clock time of day as whole milliseconds since midnight.

    Args:
        moment: The time of day, naive.

    Returns:
        The milliseconds; a fraction of a millisecond is dropped.
    """
    return ((moment.hour * 60 + moment.minute) * 60 + moment.second) * 1000 + (
        moment.microsecond // 1000
    )


def _refuse(
    label: str,
    values: pa.Array,
    odd: pa.Array,
    *,
    zone: tzinfo,
    known: Collection[time],
    source: str,
    what: str,
    remedy: str,
) -> None:
    """Raise for the first bar a mask marks as stamped at an unknown time of day, if any.

    Args:
        label: The dataset, for the message.
        values: The stamps, an Arrow `timestamp("ns", "UTC")` array.
        odd: True for each refused stamp; False for an accepted or null one.
        zone: The class day zone.
        known: The accepted times of day besides 00:00.
        source: Where the bars came from, for the message.
        what: The kind of bar, for the message, such as `"1-day bar"`.
        remedy: What the caller can do instead, for the message.

    Raises:
        MassiveError: If `odd` holds a True, naming the first such stamp.
    """
    if not pc.any(odd).as_py():
        return
    bad_ns = pc.filter(values.cast(pa.int64()), odd)[0].as_py()
    bad = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=bad_ns // 1000)
    times = ", ".join(t.isoformat("milliseconds") for t in sorted({time(0), *known}))
    raise MassiveError(
        f"{label}: a {what} from {source} is stamped {bad.isoformat()} "
        f"({bad.astimezone(zone).isoformat()} in {zone}), which is not a time of day Massive is "
        f"known to stamp such a bar at ({times} in {zone}), so it may cover other days than the "
        f"one it would be labelled with; {remedy}, and report the stamp so the day cut can be "
        "settled"
    )
