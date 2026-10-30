"""Deephaven's own date and time text, read by Deephaven's parser for the date arguments.

A date argument takes ISO-8601 text, which the dataset layer reads itself
(`_datasets/dates.py`, `parse_date_like`), and also any text Deephaven's own time functions
read, such as `"2025-03-03T09:30 America/New_York"` or `"2025-03-03T09:30:00 ET"`. Only
Deephaven's parser knows that grammar, and the dataset layer makes no Deephaven call, so the
API layer passes each text argument through `deephaven_time_text` first: ISO-8601 text is left
as it is, and other text becomes the Java `LocalDate` or `ZonedDateTime` Deephaven reads it as,
which the dataset layer then takes like any Java value. One grammar per form, and no second
hand-written one.

Text Deephaven reads only as a count since the epoch (`"20250303"`, `"1741012200"`) is not
taken: `deephaven.time.to_j_instant` reads bare digits as epoch seconds, milliseconds, or
nanoseconds, which would silently misread a date written without dashes. Only Deephaven's date
and zoned date-time parsers are used, and neither reads bare digits.

The function runs on the caller's thread, needs a running Deephaven JVM (as every verb does),
takes no lock, and is thread-safe.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from deephaven.dherror import DHError
from deephaven.time import to_j_local_date, to_j_zdt

from deephaven_massive._datasets.dates import TimeTextError, parse_date_like

if TYPE_CHECKING:
    from deephaven_massive._types import DateLike

__all__ = ["deephaven_time_text"]

logger = logging.getLogger(__name__)


def deephaven_time_text(value: DateLike, name: str) -> DateLike:
    """Turn Deephaven date or time text into its Java value; leave every other bound alone.

    Call it on each date argument at the API layer, before the dataset layer parses it. ISO-8601
    text is returned unchanged, so it parses exactly as before. Other text is read by
    Deephaven's parser: first as a date, so date-only text stays a calendar date
    (`to_j_local_date`), then as a date and time with a zone or offset (`to_j_zdt`), such as
    `"2025-03-03T09:30 America/New_York"`. Each conversion logs at DEBUG.

    Args:
        value: The argument as the caller passed it.
        name: The parameter's name, for messages (`"start"`, `"end"`, `"date"`).

    Returns:
        `value` itself, unless it is text only Deephaven reads; then the Java `LocalDate` or
        `ZonedDateTime` it names.

    Raises:
        ValueError: If `value` is text that neither ISO-8601 nor Deephaven's date and time
            text reads; the message names both forms. ISO-8601 text that is refused for
            another reason (a time without an offset, digits below the microsecond) raises
            that error, as before.
    """
    if not isinstance(value, str):
        return value
    try:
        parse_date_like(value, name)
    except TimeTextError as error:
        unread = error
    else:
        return value
    text = value.strip()
    for read in (to_j_local_date, to_j_zdt):
        try:
            java = read(text)
        except DHError:
            continue
        logger.debug("%s=%r read as Deephaven text: %s", name, value, java)
        return cast("DateLike", java)
    raise ValueError(
        f"{name}={value!r} is neither ISO-8601 (such as '2025-03-03' or "
        "'2025-03-03T09:30:00-05:00') nor Deephaven date or time text (such as "
        "'2025-03-03T09:30 America/New_York')"
    ) from unread
