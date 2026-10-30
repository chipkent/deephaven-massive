"""Epoch nanoseconds: the conversions between aware datetimes and nanoseconds since 1970.

Every time column the library builds holds `Instant`s, which Arrow carries as `int64`
nanoseconds since 1970-01-01T00:00Z, while the day and bar rules reason in aware `datetime`s.
This module owns the bridge between the two, once: the epoch itself, the length of a day in
nanoseconds, and the two conversions. A `datetime` holds microseconds, so a conversion from
nanoseconds floors to the microsecond; every instant the rules build is a whole microsecond, so
a conversion to nanoseconds is exact.

The module is pure: it imports nothing of the library and makes no Deephaven or network call.
`timestamps.py` converts Massive's epoch integers of any unit into checked nanoseconds; this
module converts the library's own instants.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Final

__all__ = ["DAY_NS", "EPOCH", "epoch_instant", "epoch_ns"]

EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
"""The Unix epoch, 1970-01-01T00:00Z, as an aware `datetime`."""

DAY_NS: Final = 86_400 * 10**9
"""The nanoseconds of one UTC day (86,400 seconds; epoch time has no leap seconds)."""

_ONE_MICROSECOND: Final = timedelta(microseconds=1)


def epoch_ns(moment: datetime) -> int:
    """Return an aware `datetime` as nanoseconds since the epoch.

    Args:
        moment: The instant, aware, in any zone.

    Returns:
        The nanoseconds, negative before 1970.

    Example:
        ::

            from datetime import UTC, datetime

            assert epoch_ns(datetime(1970, 1, 2, tzinfo=UTC)) == DAY_NS
    """
    return (moment - EPOCH) // _ONE_MICROSECOND * 1000


def epoch_instant(ns: int) -> datetime:
    """Return nanoseconds since the epoch as an aware UTC `datetime`, floored to the microsecond.

    Args:
        ns: The instant, in nanoseconds since the epoch.

    Returns:
        The instant, in UTC.

    Example:
        ::

            from datetime import UTC, datetime

            assert epoch_instant(DAY_NS + 999) == datetime(1970, 1, 2, tzinfo=UTC)
    """
    return EPOCH + timedelta(microseconds=ns // 1000)
