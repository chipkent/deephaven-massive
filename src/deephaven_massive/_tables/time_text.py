"""Deephaven's own date and time text, read by Deephaven's parser for the date arguments.

A date argument takes ISO-8601 text, which the dataset layer reads itself
(`_datasets/dates.py`, `parse_date_like`), and also any text Deephaven's own time functions
read, such as `"2025-03-03T09:30 America/New_York"` or `"2025-03-03T09:30:00 ET"`. Only
Deephaven's parser knows that grammar, and the dataset layer makes no Deephaven call, so the
API layer passes each text argument through `deephaven_time_text` first: ISO-8601 text is left
as it is, and other text becomes the Java `LocalDate` or `ZonedDateTime` Deephaven reads it as,
which the dataset layer then takes like any Java value. One grammar per form, and no second
hand-written one.

Only Deephaven's date and zoned date-time parsers are used. Its instant parser,
`deephaven.time.to_j_instant`, reads bare digits as epoch seconds, milliseconds, or nanoseconds,
which would silently misread a date written without dashes; `"20250303"` is an ISO-8601 date
anyway, and `"1741012200"` is refused. Deephaven text that names a day and a zone but no time,
such as `"2025-03-03 ET"`, is refused too: Deephaven reads it as that midnight, so as an `end` it
would drop the day.

The function runs on the caller's thread, needs a running Deephaven JVM (as every verb does),
takes no lock, and is thread-safe.
"""

from __future__ import annotations

import logging
import re
from typing import Final

from deephaven.dherror import DHError
from deephaven.time import to_j_local_date, to_j_zdt

from deephaven_massive._datasets.dates import TimeTextError, parse_date_like

__all__ = ["deephaven_time_text"]

logger = logging.getLogger(__name__)

_JAVA_CLASS: Final = re.compile(r"^[\w.$]+(?:Exception|Error): ")
"""The Java exception class that starts Deephaven's message, such as
`java.lang.RuntimeException: `."""


def deephaven_time_text(value: object, name: str) -> object:
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
            text reads (the message names both forms, gives Deephaven's own reason, such as
            `No matching time zone: EST`, and names zones it reads), or Deephaven text that
            names a day and a zone but no time, such as `"2025-03-03 ET"`, which Deephaven
            reads as that midnight: as an `end` it would drop the day.

    ISO-8601 text refused for another reason (a time without an offset) raises the
    `ValueError` of `parse_date_like`, as before.
    """
    if not isinstance(value, str):
        return value
    try:
        parse_date_like(value, name)
    except TimeTextError as error:
        unread = error
    else:
        return value
    reason = ""
    for read in (to_j_local_date, to_j_zdt):
        try:
            java = read(value)
        except DHError as error:
            reason = _reason(error)
            continue
        if read is to_j_zdt and "T" not in value.split()[0]:
            raise ValueError(
                f"{name}={value!r} names a day and a zone but no time. Pass '2025-03-03' for "
                "the whole day, or a time such as '2025-03-03T00:00 ET'."
            )
        logger.debug("%s=%r read as Deephaven text: %s", name, value, java)
        return java
    raise ValueError(
        f"{name}={value!r} is neither ISO-8601 text nor Deephaven date or time text, and "
        f"Deephaven says: {reason.rstrip('.')}. Pass text such as '2025-03-03', "
        "'2025-03-03T09:30:00-05:00', or '2025-03-03T09:30 America/New_York', in a zone "
        "Deephaven reads, such as ET, CT, UTC, or America/New_York."
    ) from unread


def _reason(error: DHError) -> str:
    """Return Deephaven's own reason for refusing text, without its Java exception class.

    Args:
        error: What `deephaven.time` raised, whose first line ends in the Java message, such as
            `" : java.lang.RuntimeException: No matching time zone: EST"`.

    Returns:
        The message alone, such as `"No matching time zone: EST"`.
    """
    first = str(error).strip().splitlines()[0].removeprefix(":").strip()
    return _JAVA_CLASS.sub("", first, count=1)
