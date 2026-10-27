"""The key layout of Massive's flat-file bucket: where each day's file of a dataset lives.

Massive publishes one gzip CSV file per dataset and day in its S3 bucket, under keys
`<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz`, for example
`us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`. Futures files are published per venue, so
a futures prefix holds the placeholder `{venue}` (`us_futures_{venue}/trades_v1`), which a key
fills with the venue's lowercase name (`us_futures_cme/trades_v1`).

This module knows only the layout. Which prefix a dataset has, which venues a request reads,
and which dates a range needs are the library's decisions (`_flat_files.py` and
`_history_source.py`). It imports only the standard library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

__all__ = ["object_key"]


def object_key(prefix: str, day: date, venue: str | None = None) -> str:
    """Return the key of a dataset's file for one day.

    Args:
        prefix: The dataset's prefix in the bucket, such as `"us_stocks_sip/trades_v1"`; a
            futures prefix holds the placeholder `{venue}`.
        day: The file's partition date (for futures, its trading session).
        venue: The lowercase venue (`"cme"`, `"cbot"`, `"nymex"`, or `"comex"`) that fills
            `{venue}`; None for a prefix without the placeholder.

    Returns:
        The key, such as `"us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"`.

    Example:
        ::

            from datetime import date

            from deephaven_massive._massive import object_key

            key = object_key("us_futures_{venue}/trades_v1", date(2025, 3, 4), "cme")
            assert key == "us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz"
    """
    base = prefix if venue is None else prefix.format(venue=venue)
    return f"{base}/{day:%Y}/{day:%m}/{day.isoformat()}.csv.gz"
