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

from deephaven_massive._massive import FlatFileClient, WireError, object_key

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"

_DAY_FILE = "us_stocks_sip/day_aggs_v1"
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


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.settles("14")
def test_when_each_days_file_was_published(
    live_flat_files: FlatFileClient,
    *,
    live_s3_config: MassiveConfig,
    flat_files_reached: Callable[[str], bool],
    evidence: Evidence,
) -> None:
    """When Massive publishes each day's stock file, against the documented ~11:00 New York.

    The library treats a day's file as unpublished before `flat_file_publish_cutoff` (11:30
    New York time the next day), or 3 hours later when that day is a Saturday or Sunday, as
    Friday's is. Records the `LastModified` of `us_stocks_sip/day_aggs_v1` for each of the
    last ten weekdays, in New York time, or why it has none (a holiday has no file). Reading
    it: every weekday's file modified before 11:30 the next day, and every Friday's before
    14:30 on Saturday, confirms the rule; later ones show how far to move it. Skips when the
    S3 credentials do not reach the stock day files.
    """
    if not flat_files_reached(_DAY_FILE):
        pytest.skip(f"needs the {_DAY_FILE} flat files: the S3 credentials do not reach them")
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


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.verifies("16")
def test_a_weekend_key_is_missing_not_refused(
    live_flat_files: FlatFileClient,
    *,
    recent_monday: date,
    live_s3_config: MassiveConfig,
    flat_files_reached: Callable[[str], bool],
    evidence: Evidence,
) -> None:
    """Whether a key with no file answers 404 `NoSuchKey` (missing), not 403 or another 404.

    S3 answers 403 for a missing key when the credentials lack `ListBucket`, which would turn
    every weekend into an entitlement error; and a front end that rewrites 404 bodies (Massive
    serves an HTML "404 Not Found" page for a path it does not route) would hide S3's
    `NoSuchKey`, which the client requires before it calls a key missing, so every range that
    holds a weekend would raise. First opens the existing stock file of the most recent published
    Monday, so the credentials are known to reach the dataset. Then requests the Sunday before
    directly, to record the status, S3's error code, and the content type, and opens it through the
    client. Passes when the Sunday's key is missing.

    Raises:
        WireError: When the Sunday's key is refused (HTTP 403, `WireEntitlementError`) or
            answers a 404 that is not `NoSuchKey`; the answer is recorded first, and fails
            the test.
    """
    if not flat_files_reached(_DAY_FILE):
        pytest.skip(f"needs the {_DAY_FILE} flat files: the S3 credentials do not reach them")
    control = live_flat_files.open(object_key(_DAY_FILE, recent_monday))
    evidence(f"control {recent_monday}: {'exists' if control is not None else 'missing'}")
    assert control is not None, (
        f"the {recent_monday} stock file must exist for the check to mean anything"
    )
    control.close()
    key = object_key(_DAY_FILE, (recent_monday - timedelta(days=1)))
    s3, _proxy = live_flat_files._client_for(key)
    answer, code = "exists", None
    try:
        s3.get_object(Bucket=live_s3_config.s3_bucket, Key=key)["Body"].close()
    except ClientError as error:
        metadata = error.response.get("ResponseMetadata", {})
        code = error.response.get("Error", {}).get("Code")
        answer = (
            f"HTTP {metadata.get('HTTPStatusCode')}, code {code}, "
            f"content type {metadata.get('HTTPHeaders', {}).get('content-type')}"
        )
    evidence(f"{key}: {answer}")
    try:
        file = live_flat_files.open(key)
    except WireError as error:
        evidence(f"{key}: the client raised {type(error).__name__}: {error}")
        raise
    if file is not None:
        file.close()
    assert file is None, f"{key} exists ({answer})"
    assert code == "NoSuchKey", answer
