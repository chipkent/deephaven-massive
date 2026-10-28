"""Live checks of Massive's S3 bucket through the wire flat-file client: publication and 404s.

They run only with the S3 credentials (`MASSIVE_S3_ACCESS_KEY_ID` and
`MASSIVE_S3_SECRET_ACCESS_KEY`) as well as `MASSIVE_API_KEY`. The publication check reads each
object's `LastModified` through the client's own S3 client, so it measures when a file was
published rather than whether it exists now. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
from botocore.exceptions import ClientError

from deephaven_massive._massive import FlatFileClient, WireEntitlementError, object_key

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"

_DAY_FILE = "us_stocks_sip/day_aggs_v1"
_DAY = date(2026, 6, 1)
"""A past Monday whose stock file is published."""

_WEEKEND = date(2026, 5, 31)
"""The Sunday before it, which has no stock file."""

_DAYS = 10
"""How many weekdays back the publication check reads."""

_NEW_YORK = ZoneInfo("America/New_York")


def _weekdays_before(day: date, count: int) -> list[date]:
    """Return the `count` weekdays before `day`, newest first.

    Returns:
        The days.
    """
    found: list[date] = []
    while len(found) < count:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            found.append(day)
    return found


def test_when_each_days_file_was_published(
    live_flat_files: FlatFileClient, *, live_s3_config: MassiveConfig, evidence: Evidence
) -> None:
    """When Massive publishes each day's stock file, against the documented ~11:00 New York.

    The library treats a day's file as unpublished before `flat_file_publish_cutoff` (11:30
    New York time the next day). Records the `LastModified` of `us_stocks_sip/day_aggs_v1`
    for each of the last ten weekdays, in New York time, or why it has none (a holiday has no
    file). Reading it: every file modified before 11:30 the next weekday confirms the cutoff;
    later ones show how far to move it.
    """
    s3, _proxy = live_flat_files._client_for(_DAY_FILE)
    today = datetime.now(_NEW_YORK).date()
    seen = 0
    for day in _weekdays_before(today, _DAYS):
        key = object_key(_DAY_FILE, day)
        try:
            head = s3.head_object(Bucket=live_s3_config.s3_bucket, Key=key)
        except ClientError as error:
            evidence(f"{day} {day:%a}: no LastModified ({error.response.get('Error', {})})")
            continue
        seen += 1
        published = head["LastModified"].astimezone(_NEW_YORK)
        evidence(f"{day} {day:%a}: published {published:%Y-%m-%d %H:%M:%S %Z}")
    assert seen


def test_a_weekend_key_is_missing_not_refused(
    live_flat_files: FlatFileClient, *, evidence: Evidence
) -> None:
    """Whether a key with no file answers 404 (missing) rather than 403 (refused).

    S3 answers 403 for a missing key when the credentials lack `ListBucket`, which would turn
    every weekend into an entitlement error. First opens the existing 2026-06-01 stock file,
    so the credentials are known to reach the dataset, then the Sunday before. Passes when the
    Sunday's key is missing.

    Raises:
        WireEntitlementError: When the Sunday's key is refused (HTTP 403); it is recorded
            first, and fails the test.
    """
    control = live_flat_files.open(object_key(_DAY_FILE, _DAY))
    evidence(f"control {_DAY}: {'exists' if control is not None else 'missing'}")
    assert control is not None, f"the {_DAY} stock file must exist for the check to mean anything"
    control.close()
    key = object_key(_DAY_FILE, _WEEKEND)
    try:
        file = live_flat_files.open(key)
    except WireEntitlementError as error:
        evidence(f"{key}: refused (HTTP {error.status}): {error}")
        raise
    evidence(f"{key}: {'missing (HTTP 404)' if file is None else 'exists'}")
    if file is not None:
        file.close()
    assert file is None
