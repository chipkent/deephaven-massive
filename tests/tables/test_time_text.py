"""Tests for `deephaven_time_text`: Deephaven's date and time text in date arguments."""

import logging
from datetime import UTC, date, datetime, timedelta

import pytest

from deephaven_massive._datasets.dates import parse_date_like
from deephaven_massive._tables.time_text import deephaven_time_text


def _bound(text: str) -> date | datetime:
    return parse_date_like(deephaven_time_text(text, "start"), "start")


@pytest.mark.parametrize(
    "text",
    ["2025-03-03", "2025-03-03T09:30:00-05:00", "2025-03-03T14:30:00Z", " 2025-03-03 "],
)
def test_iso_text_is_left_for_the_dataset_layer(text: str) -> None:
    # The same object comes back, so ISO-8601 parses exactly as before.
    assert deephaven_time_text(text, "start") is text


@pytest.mark.parametrize(
    "text",
    [
        "2025-03-03T09:30 America/New_York",  # a zone name
        "2025-03-03T09:30:00 ET",  # Deephaven's short zone name
        "2025-03-03T09:30:00 -05:00",  # an offset after a space, which ISO-8601 does not read
    ],
)
def test_deephaven_zone_text_is_an_instant(text: str) -> None:
    moment = _bound(text)
    assert isinstance(moment, datetime)
    assert moment.astimezone(UTC) == datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    assert moment.utcoffset() == timedelta(hours=-5)


def test_deephaven_text_with_nanoseconds_rounds_outward() -> None:
    text = "2025-03-03T09:30:00.123456789 America/New_York"
    start = datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    assert _bound(text) == start
    end = parse_date_like(deephaven_time_text(text, "end"), "end", upper=True)
    assert end == start + timedelta(microseconds=1)


def test_a_conversion_logs_what_deephaven_read(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive._tables.time_text")
    deephaven_time_text("2025-03-03T09:30 America/New_York", "start")
    read = "2025-03-03T09:30-05:00[America/New_York]"
    assert caplog.messages == [
        f"start='2025-03-03T09:30 America/New_York' read as Deephaven text: {read}"
    ]


@pytest.mark.parametrize("text", ["tomorrow", "1741012200", ""])
def test_text_neither_reads_names_both_forms(text: str) -> None:
    # Bare digits are refused: Deephaven reads them only as a count since the epoch.
    with pytest.raises(ValueError, match=r"is neither ISO-8601 .* nor Deephaven date or time"):
        deephaven_time_text(text, "start")


def test_a_date_without_dashes_is_never_read_as_an_epoch_count() -> None:
    # ISO-8601's basic form reads it as a time without a zone, which is refused; Deephaven's
    # instant parser, which would read it as seconds since 1970, is never asked.
    with pytest.raises(ValueError, match=r"needs a timezone"):
        deephaven_time_text("20250303", "start")


def test_iso_text_refused_for_another_reason_keeps_its_error() -> None:
    with pytest.raises(ValueError, match=r"needs a timezone: pass an aware datetime or a date"):
        deephaven_time_text("2025-03-03T09:30", "start")
    with pytest.raises(ValueError, match=r"more than six fractional digits"):
        deephaven_time_text("2025-03-03T09:30:00.123456789Z", "start")


@pytest.mark.parametrize("value", [date(2025, 3, 3), 20250303])
def test_other_values_pass_through(value: object) -> None:
    # Integers stay refused by the parser, never read here.
    assert deephaven_time_text(value, "start") is value  # type: ignore[arg-type]  # any value passes
